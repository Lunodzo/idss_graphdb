#!/usr/bin/env bash
# Green Power use case: start one peer per partition file, run grouped report
# queries from the client and check them against the generator's ground truth.
#
# Usage: experiments/gp/run_local.sh <datadir> [settle_seconds_after_load] [policy.yaml]
#   <datadir>  output of server/generate_cim_data.py (contains peers/ and ground_truth.json)
#
# Peers forward to every routing-table peer (IDSS_MAX_FORWARD=0) so that results
# can be compared exactly; set IDSS_MAX_FORWARD yourself to test bounded fan-out.
set -euo pipefail
DATA=$(readlink -f "${1:?datadir}")
SETTLE=${2:-10}
REPO=$(cd "$(dirname "$0")/../.." && pwd)
POLICY=$(readlink -f "${3:-$REPO/server/policy.permissive.yaml}")
RUN=$(mktemp -d "${TMPDIR:-/tmp}/idss_gp_run.XXXXXX")
mkdir -p "$RUN/logs" "$RUN/db" "$RUN/res"
export IDSS_MAX_FORWARD=${IDSS_MAX_FORWARD:-0}

(cd "$REPO" && go build -o "$RUN/idss_server" ./server && go build -o "$RUN/idss_client" ./client)

PIDS=()
cleanup() { kill "${PIDS[@]}" 2>/dev/null || true; wait 2>/dev/null || true; }
trap cleanup EXIT

start_peer() {
  local name=$1; shift
  cp "$DATA/peers/$name.json" "$RUN/$name.json"   # the peer keeps its file; copy protects the dataset
  IDSS_LISTEN_ADDR=/ip4/127.0.0.1/tcp/0 IDSS_DISABLE_MDNS=1 IDSS_DB_PATH="$RUN/db" GOMAXPROCS=1 \
    "$RUN/idss_server" -f "$RUN/$name.json" -partition "$name" -policy "$POLICY" "$@" > "$RUN/logs/$name.log" 2>&1 &
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
echo "Started ${#NAMES[@]} peers; seed ${NAMES[0]}; waiting until every peer has loaded its partition"
for _ in $(seq 1 1200); do   # up to 10 minutes (large partitions load slowly)
  ready=$( (grep -l "Serving IDSS stream protocol" "$RUN"/logs/*.log 2>/dev/null || true) | wc -l)
  [[ "$ready" -eq "${#NAMES[@]}" ]] && break; sleep 0.5
done
[[ "$ready" -eq "${#NAMES[@]}" ]] || { echo "only $ready/${#NAMES[@]} peers ready"; exit 1; }
echo "All peers serving; settling ${SETTLE}s for routing tables"
sleep "$SETTLE"

Q1='@group(keys=0; aggs=count) get FailureEvent show failureYear'
Q3='@group(keys=0,1; aggs=count) get FailureEvent traverse failure:AFFECTS:asset:ACLineSegment traverse segment:CONTAINS:container:AssetContainer traverse asset:OWNS:owner:Organisation end end end show 4:n:name, 1:n:faultCause'
TTL=${TTL:-10}
(sleep 5; echo "$Q1, $TTL"; sleep $((TTL + 2)); echo "$Q3, $TTL"; sleep $((TTL + 2)); echo exit) |
  IDSS_CLIENT_RESULTS_DIR="$RUN/res" timeout $((3 * TTL + 30)) "$RUN/idss_client" -s "$SEED" -role observer 2>&1 |
  grep -E "Got|Partitions|Time spent|rror" || true

mapfile -t RES < <(ls -tr "$RUN"/res/*.json)
status=0
python3 "$REPO/experiments/gp/check_ground_truth.py" "$DATA/ground_truth.json" Q1 "${RES[0]}" || status=1
python3 "$REPO/experiments/gp/check_ground_truth.py" "$DATA/ground_truth.json" Q3 "${RES[1]}" || status=1
echo "Run directory (logs, results): $RUN"
exit $status
