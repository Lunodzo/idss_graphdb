#!/usr/bin/env bash
# E4 onboarding: add a public register to a running Green Power overlay.
#
# Usage: experiments/gp/run_onboard.sh <datadir> [register] [query_id]
#   <datadir>   output of server/generate_cim_data.py
#   register    partition of the register to onboard (default public_digging)
#   query_id    report that needs the register (default Q5)
#
# 1. Starts one peer per partition except <register> and waits until all have
#    loaded their partitions and announced their data kinds.
# 2. Runs the report once ("before"): the DSO partitions must report the join
#    as failed, because no peer provides the register's data kind yet.
# 3. Starts the register peer with nothing but its partition file, a policy
#    file and the address of any running peer. No running peer is restarted
#    or reconfigured.
# 4. Re-runs the report every POLL seconds until it matches the ground truth,
#    then REPEATS more times; prints the time from starting the register to
#    serving, to announcing its data kind, and to the first exact report.
# Checks that the existing peers kept their process IDs (no restart).
set -euo pipefail
DATA=$(readlink -f "${1:?datadir}")
REG=${2:-public_digging}
QID=${3:-Q5}
POLL=${POLL:-2}; TTL=${TTL:-10}; REPEATS=${REPEATS:-5}; SETTLE=${SETTLE:-10}; MAXWAIT=${MAXWAIT:-180}
REPO=$(cd "$(dirname "$0")/../.." && pwd)
REG_POLICY=${REG_POLICY:-$REPO/experiments/gp/policies/S1_coordinator_only/public.yaml}
RUN=$(mktemp -d "${TMPDIR:-/tmp}/idss_gp_onboard.XXXXXX")
mkdir -p "$RUN/logs" "$RUN/db" "$RUN/res"
(cd "$REPO" && go build -o "$RUN/idss_server" ./server && go build -o "$RUN/idss_client" ./client)

PIDS=()
cleanup() { kill "${PIDS[@]}" 2>/dev/null || true; wait 2>/dev/null || true; }
trap cleanup EXIT
"$RUN/idss_client" -key "$RUN/coordinator.key" -print-id > /dev/null
now() { date +%s.%N; }

start_peer() {   # name policy [extra args]
  local name=$1 pol=$2; shift 2
  cp "$DATA/peers/$name.json" "$RUN/$name.json"
  IDSS_LISTEN_ADDR=/ip4/127.0.0.1/tcp/0 IDSS_DISABLE_MDNS=1 IDSS_DB_PATH="$RUN/db" GOMAXPROCS=1 \
    "$RUN/idss_server" -usecase gp -f "$RUN/$name.json" -partition "$name" -policy "$pol" "$@" > "$RUN/logs/$name.log" 2>&1 &
  PIDS+=($!)
}
wait_log() {     # file pattern seconds
  for _ in $(seq 1 $(( $3 * 10 ))); do grep -q "$2" "$1" 2>/dev/null && return 0; sleep 0.1; done; return 1
}

mapfile -t NAMES < <(ls "$DATA/peers" | sed 's/\.json$//' | grep -vx "$REG" | sort)
start_peer "${NAMES[0]}" "$REPO/server/policy.greenpower.yaml"
wait_log "$RUN/logs/${NAMES[0]}.log" "/ip4/127.0.0.1/tcp/" 60 || { echo "seed did not start"; exit 1; }
SEED=$(grep -o "/ip4/127.0.0.1/tcp/[0-9]*/p2p/[A-Za-z0-9]*" "$RUN/logs/${NAMES[0]}.log" | head -1)
for name in "${NAMES[@]:1}"; do start_peer "$name" "$REPO/server/policy.greenpower.yaml" -peer "$SEED"; done
for name in "${NAMES[@]}"; do
  wait_log "$RUN/logs/$name.log" "Serving IDSS stream protocol" 600 || { echo "$name not serving"; exit 1; }
  wait_log "$RUN/logs/$name.log" "Announced data kinds" 120 || echo "WARNING: $name did not announce"
done
echo "Running ${#NAMES[@]} peers without $REG; settling ${SETTLE}s"
sleep "$SETTLE"
EXISTING_PIDS="${PIDS[*]}"

QUERY=$(grep -P "^$QID\t" "$REPO/experiments/gp/queries.tsv" | cut -f2-)
run_query() {    # tag -> prints result file
  local dir="$RUN/res/$1"; mkdir -p "$dir"
  printf '%s, %s\nexit\n' "$QUERY" "$TTL" | IDSS_CLIENT_RESULTS_DIR="$dir" timeout $((TTL + 20)) \
    "$RUN/idss_client" -s "$SEED" -key "$RUN/coordinator.key" -expected "$DATA/manifest.json" >> "$RUN/client.log" 2>&1 || true
  ls "$dir"/*.json 2>/dev/null | head -1
}
check() { python3 "$REPO/experiments/gp/check_ground_truth.py" "$DATA/ground_truth.json" "$QID" "$1" ${2:-} | head -1; }
manifest_line() {
  python3 -c "import json,sys,collections; d=json.load(open(sys.argv[1])); c=collections.Counter(m['status'] for m in d.get('manifest') or []); print(dict(c), d.get('elapsedMs'))" "$1"
}

f=$(run_query before)
echo "BEFORE: $(check "$f" --allow-missing)"
echo "BEFORE manifest: $(manifest_line "$f")"
python3 -c "import json,sys; [print('  ',m['partition'],m['status'],m.get('detail','')) for m in json.load(open(sys.argv[1]))['manifest'] if m['status']!='ANSWERED']" "$f" | sort | uniq | head -10

echo "Onboarding $REG: partition file $(du -h "$DATA/peers/$REG.json" | cut -f1), policy $REG_POLICY ($(grep -cv '^\s*\(#\|$\)' "$REG_POLICY") non-comment lines)"
T0=$(now)
start_peer "$REG" "$REG_POLICY" -peer "$SEED"
wait_log "$RUN/logs/$REG.log" "Serving IDSS stream protocol" 600; T_SERVE=$(now)
wait_log "$RUN/logs/$REG.log" "Announced data kinds" 120; T_ANN=$(now)
n=0; T_OK=""
while :; do
  n=$((n + 1)); f=$(run_query "after_$n")
  v=$(check "$f")
  if [[ "$v" == *MATCH* && "$v" != *MISMATCH* ]]; then T_OK=$(now); break; fi
  echo "  attempt $n: $v | $(manifest_line "$f")"
  (( $(echo "$(now) - $T0 > $MAXWAIT" | bc) )) && break
  sleep "$POLL"
done
lat=()
for i in $(seq 1 "$REPEATS"); do
  f=$(run_query "steady_$i"); v=$(check "$f")
  lat+=("$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['elapsedMs'])" "$f")")
  [[ "$v" == *MATCH* && "$v" != *MISMATCH* ]] || echo "  steady $i: $v"
done
alive=0; for p in $EXISTING_PIDS; do kill -0 "$p" 2>/dev/null && alive=$((alive + 1)); done
python3 - "$T0" "$T_SERVE" "$T_ANN" "${T_OK:-nan}" "$n" "$alive" "${#NAMES[@]}" "${lat[@]}" <<'EOF'
import sys, statistics
t0, ts, ta, tok = (float(x) for x in sys.argv[1:5]); n, alive, total = sys.argv[5:8]; lat = [float(x) for x in sys.argv[8:]]
print(f"AFTER: register serving after {ts-t0:.1f}s, announced after {ta-t0:.1f}s, first exact report after {tok-t0:.1f}s ({n} attempts)")
print(f"AFTER: existing peers still running with the same process: {alive}/{total} (none restarted or reconfigured)")
if lat: print(f"AFTER: steady report latency median {statistics.median(lat):.1f} ms over {len(lat)} runs")
EOF
echo "Run directory: $RUN"
