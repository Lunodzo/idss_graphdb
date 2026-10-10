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

# Report queries (see .github/copilot-instructions.md). Q4 = Q4n / Q4d.
TRAV="traverse failure:AFFECTS:asset:ACLineSegment traverse segment:CONTAINS:container:AssetContainer traverse asset:OWNS:owner:Organisation end end end"
declare -a QIDS=(Q1 Q3 Q4n Q4d Q5 Q6)
declare -A QTEXT=(
  [Q1]="@group(keys=0; aggs=count) get FailureEvent show failureYear"
  [Q3]="@group(keys=0,1; aggs=count) get FailureEvent $TRAV show 4:n:name, 1:n:faultCause"
  [Q4n]="@group(keys=0,decade(1); aggs=count) get FailureEvent traverse failure:AFFECTS:asset:ACLineSegment end show 2:n:insulation, 2:n:inServiceYear"
  [Q4d]="@group(keys=0,decade(1); aggs=exposure(2,1,2013,2023)) get ACLineSegment show insulation, inServiceYear, lengthKm"
  [Q5]="@join(type=digging; window=30; cols=1,2,3,4,5) @group(keys=0; aggs=count) get FailureEvent $TRAV show 4:n:name, 1:n:failureDay, 2:n:minLat, 2:n:maxLat, 2:n:minLon, 2:n:maxLon"
  [Q6]="@join(type=weather; window=3; cols=1,2,3) @group(keys=0; aggs=count) get FailureEvent $TRAV show 4:n:name, 1:n:failureDay, 2:n:lat, 2:n:lon"
)
TTL=${TTL:-10}
{
  sleep 5
  for q in "${QIDS[@]}"; do echo "${QTEXT[$q]}, $TTL"; sleep $((TTL + 2)); done
  echo exit
} | IDSS_CLIENT_RESULTS_DIR="$RUN/res" timeout $((${#QIDS[@]} * (TTL + 2) + 30)) \
    "$RUN/idss_client" -s "$SEED" -key "$RUN/$REQUESTER.key" -expected "$DATA/manifest.json" 2>&1 |
  grep -E "Got|Partitions|Time spent|rror" || true

mapfile -t RES < <(ls -tr "$RUN"/res/*.json)
if [[ ${#RES[@]} -ne ${#QIDS[@]} ]]; then
  echo "expected ${#QIDS[@]} results, got ${#RES[@]}"; exit 1
fi
declare -A FILE
for i in "${!QIDS[@]}"; do FILE[${QIDS[$i]}]=${RES[$i]}; done
status=0
ALLOW=""; [[ -n "${KILL:-}${SCENARIO:-}" ]] && ALLOW="--allow-missing"
CHECK="python3 $REPO/experiments/gp/check_ground_truth.py $DATA/ground_truth.json"
for q in Q1 Q3 Q5 Q6; do $CHECK $q "${FILE[$q]}" $ALLOW || status=1; done
$CHECK Q4 "${FILE[Q4n]}" "${FILE[Q4d]}" $ALLOW || status=1
for q in "${QIDS[@]}"; do   # partitions that did not answer, with reasons
  python3 -c "import json,sys; d=json.load(open(sys.argv[1]))
for m in d.get('manifest', []):
    if m['status'] != 'ANSWERED': print(f\"  {sys.argv[2]}: {m['partition']} {m['status']} ({m.get('detail', '')})\")" "${FILE[$q]}" "$q"
done
echo "Run directory (logs, results): $RUN"
exit $status
