#!/usr/bin/env bash
# Green Power use case: start one peer per partition file, run grouped report
# queries from the client and check them against the generator's ground truth.
#
# Usage: experiments/gp/run_local.sh <datadir> [settle_seconds_after_load] [policy.yaml]
#   <datadir>  output of server/generate_cim_data.py (contains peers/ and ground_truth.json)
#
# Peers forward to every routing-table peer (IDSS_MAX_FORWARD=0) so that results
# can be compared exactly; set IDSS_MAX_FORWARD yourself to test bounded fan-out.
# KILL="dso04 dso05" stops those partitions' peers before querying (peer-loss
# test); the client then expects every partition in the dataset's manifest.json
# and the checker compares against the ground truth without the lost DSOs.
# SCENARIO=S1_coordinator_only|S2_mixed applies per-partition policies from
# experiments/gp/policies/<scenario>/; REQUESTER=coordinator|researcher|anonymous
# selects the signed identity the client queries with (default coordinator).
# Metrics: WARMUP=<n> REPEATS=<n> rounds of all queries; OUT_CSV=<file> appends
# one row per query execution (experiments/gp/collect_metrics.py); TAGS="k=v ..."
# adds columns; KEEP_RUN=0 deletes the run directory afterwards.
set -euo pipefail
DATA=$(readlink -f "${1:?datadir}")
SETTLE=${2:-10}
REPO=$(cd "$(dirname "$0")/../.." && pwd)
POLICY=$(readlink -f "${3:-$REPO/server/policy.greenpower.yaml}")
RUN=$(mktemp -d "${TMPDIR:-/tmp}/idss_gp_run.XXXXXX")
mkdir -p "$RUN/logs" "$RUN/db" "$RUN/res"
export IDSS_MAX_FORWARD=${IDSS_MAX_FORWARD:-0}

(cd "$REPO" && go build -o "$RUN/idss_server" ./server && go build -o "$RUN/idss_client" ./client)

PIDS=()
cleanup() { kill "${PIDS[@]}" 2>/dev/null || true; wait 2>/dev/null || true; }
trap cleanup EXIT

# Requester identities (stable keys -> peer IDs that policies can list).
declare -A REQ_ID
for r in coordinator researcher anonymous; do
  REQ_ID[$r]=$("$RUN/idss_client" -key "$RUN/$r.key" -print-id)
done
REQUESTER=${REQUESTER:-coordinator}
[[ -n "${REQ_ID[$REQUESTER]:-}" ]] || { echo "unknown REQUESTER=$REQUESTER"; exit 1; }

# Policy for a partition: scenario-specific file, else the default policy.
policy_for() {
  local name=$1 dir="$REPO/experiments/gp/policies/${SCENARIO:-}" src="$POLICY"
  if [[ -n "${SCENARIO:-}" ]]; then
    [[ -d "$dir" ]] || { echo "unknown SCENARIO=$SCENARIO" >&2; exit 1; }
    if [[ -f "$dir/$name.yaml" ]]; then src="$dir/$name.yaml"
    elif [[ "$name" == dso* && -f "$dir/dso.yaml" ]]; then src="$dir/dso.yaml"
    elif [[ "$name" == public_* && -f "$dir/public.yaml" ]]; then src="$dir/public.yaml"
    fi
  fi
  mkdir -p "$RUN/policies"
  sed -e "s/__COORDINATOR__/${REQ_ID[coordinator]}/g" -e "s/__RESEARCHER__/${REQ_ID[researcher]}/g" "$src" > "$RUN/policies/$name.yaml"
  echo "$RUN/policies/$name.yaml"
}

start_peer() {
  local name=$1; shift
  cp "$DATA/peers/$name.json" "$RUN/$name.json"   # the peer keeps its file; copy protects the dataset
  local pol; pol=$(policy_for "$name")
  IDSS_LISTEN_ADDR=/ip4/127.0.0.1/tcp/0 IDSS_DISABLE_MDNS=1 IDSS_DB_PATH="$RUN/db" GOMAXPROCS=1 \
    "$RUN/idss_server" -usecase gp -f "$RUN/$name.json" -partition "$name" -policy "$pol" "$@" > "$RUN/logs/$name.log" 2>&1 &
  PIDS+=($!)
}

mapfile -t NAMES < <(ls "$DATA/peers" | sed 's/\.json$//' | sort)
start_peer "${NAMES[0]}"
SEED=""
for _ in $(seq 1 120); do
  SEED=$(grep -o "/ip4/127.0.0.1/tcp/[0-9]*/p2p/[A-Za-z0-9]*" "$RUN/logs/${NAMES[0]}.log" | head -1 || true)
  [[ -n "$SEED" ]] && break; sleep 0.5
done
[[ -n "$SEED" ]] || { echo "seed peer did not start"; exit 1; }
for name in "${NAMES[@]:1}"; do start_peer "$name" -peer "$SEED"; done
echo "Started ${#NAMES[@]} peers; seed ${NAMES[0]}; scenario ${SCENARIO:-default}; requester $REQUESTER (${REQ_ID[$REQUESTER]})"
echo "Waiting until every peer has loaded its partition"
for _ in $(seq 1 1200); do   # up to 10 minutes (large partitions load slowly)
  ready=$( (grep -l "Serving IDSS stream protocol" "$RUN"/logs/*.log 2>/dev/null || true) | wc -l)
  [[ "$ready" -eq "${#NAMES[@]}" ]] && break; sleep 0.5
done
[[ "$ready" -eq "${#NAMES[@]}" ]] || { echo "only $ready/${#NAMES[@]} peers ready"; exit 1; }
echo "All peers serving; settling ${SETTLE}s for routing tables"
sleep "$SETTLE"
declare -A PID_OF
for i in "${!NAMES[@]}"; do PID_OF[${NAMES[$i]}]=${PIDS[$i]}; done
for name in ${KILL:-}; do
  [[ "$name" == "${NAMES[0]}" ]] && { echo "cannot kill the seed/entry peer ${NAMES[0]}"; exit 1; }
  echo "Stopping $name (peer loss)"; kill "${PID_OF[$name]}"
done
[[ -n "${KILL:-}" ]] && sleep 2

# Report queries from experiments/gp/queries.tsv (Q4 = Q4n / Q4d). The client
# runs them one after another; WARMUP rounds (default 0) are run first and
# reported as round 0, then REPEATS rounds (default 1).
QFILE=${QUERIES:-$REPO/experiments/gp/queries.tsv}
mapfile -t QLINES < <(grep -v '^#' "$QFILE" | grep -F $'\t' | cut -f2-)
WARMUP=${WARMUP:-0}; REPEATS=${REPEATS:-1}
ROUNDS=$((WARMUP + REPEATS)); NQ=$((${#QLINES[@]} * ROUNDS))
TTL=${TTL:-10}
{
  sleep 2
  for _ in $(seq 1 "$ROUNDS"); do for q in "${QLINES[@]}"; do echo "$q, $TTL"; done; done
  echo exit
} | IDSS_CLIENT_RESULTS_DIR="$RUN/res" timeout $((NQ * (TTL + 2) + 30)) \
    "$RUN/idss_client" -s "$SEED" -key "$RUN/$REQUESTER.key" -expected "$DATA/manifest.json" > "$RUN/client.log" 2>&1 || true
grep -E "Got|Partitions|Time spent|rror" "$RUN/client.log" | tail -n $((3 * ${#QLINES[@]})) || true

NRES=$(ls "$RUN"/res/*.json 2>/dev/null | wc -l)
[[ "$NRES" -eq "$NQ" ]] || echo "WARNING: expected $NQ results, got $NRES (see $RUN/client.log)"
ALLOW=""; [[ -n "${KILL:-}${SCENARIO:-}" || "${IDSS_MAX_FORWARD}" != "0" ]] && ALLOW="--allow-missing"
TAGARGS=(--tag "dataset=$(basename "$DATA")" --tag "peers=${#NAMES[@]}" --tag "scenario=${SCENARIO:-default}"
         --tag "requester=$REQUESTER" --tag "kill=${KILL:-}" --tag "max_forward=$IDSS_MAX_FORWARD" --tag "ttl=$TTL")
for t in ${TAGS:-}; do TAGARGS+=(--tag "$t"); done   # e.g. TAGS="exp=E1 scale=0.1"
status=0
python3 "$REPO/experiments/gp/collect_metrics.py" --run "$RUN" --data "$DATA" --queries "$QFILE" \
  ${OUT_CSV:+--csv "$OUT_CSV"} "${TAGARGS[@]}" $ALLOW || status=1
for f in "$RUN"/res/*.json; do   # partitions that did not answer (last round only), with reasons
  python3 -c "import json,sys; d=json.load(open(sys.argv[1]))
for m in d.get('manifest') or []:
    if m['status'] != 'ANSWERED': print(f\"  {d['query'][:40]}...: {m['partition']} {m['status']} ({m.get('detail', '')})\")" "$f"
done | sort | uniq -c | sort -rn | head -n 20
echo "Run directory (logs, results): $RUN"
[[ "${KEEP_RUN:-1}" == "1" ]] || rm -rf "$RUN"
exit $status
