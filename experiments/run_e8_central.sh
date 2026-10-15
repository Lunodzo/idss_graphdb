#!/bin/bash
# E8: centralised baseline on the same engine and hardware.
#
# One IDSS peer holds the data of a whole community (equivalent_peers x 10
# customers, same generator, same 30-day horizon) and answers Q1-Q6 alone.
# Comparing its times with the peer-to-peer runs at the same community size
# isolates the cost of distribution, while the data a central server must
# first ingest (its store size and the raw-block volume) shows what
# centralisation moves off the members' premises. Resilience is not measured
# here: a central server is a single point of failure by construction.
#
# Usage: ./experiments/run_e8_central.sh <equivalent_peers> [repeats]
#   E8_CUSTOMERS_PER_PEER (default 10), E8_DAYS (default 30), E8_TTL (default 20)

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <equivalent_peers> [repeats]" >&2
    exit 1
fi
EQUIV=$1
REPEATS=${2:-3}
E8_CUSTOMERS_PER_PEER=${E8_CUSTOMERS_PER_PEER:-10}
E8_DAYS=${E8_DAYS:-30}
E8_TTL=${E8_TTL:-20}
CUSTOMERS=$((EQUIV * E8_CUSTOMERS_PER_PEER))
# Generating and loading a whole community into one store takes minutes.
export LAUNCH_TIMEOUT_SECONDS=${LAUNCH_TIMEOUT_SECONDS:-7200}
# start_peers.sh stops waiting for the data load after START_TIMEOUT_SECONDS
# and would let queries run against a half-loaded store.
export START_TIMEOUT_SECONDS=${START_TIMEOUT_SECONDS:-7200}
# A central server gets the whole allocation (peers run with GOMAXPROCS=1
# each). nproc can report 1 under LSF CPU binding, so prefer the scheduler's count.
export PEER_GOMAXPROCS=${E8_GOMAXPROCS:-${LSB_DJOB_NUMPROC:-${SLURM_CPUS_ON_NODE:-$(nproc --all)}}}

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=experiments/lib.sh
source "${ROOT_DIR}/lib.sh"
harness_require_commands go python3 timeout

RESULT_DIR="${ROOT_DIR}/results"
harness_new_run_dir "${RESULT_DIR}" "e8-central${EQUIV}-repeats${REPEATS}"
RUN_DIR="${HARNESS_RUN_DIR}"
RUN_ID="${HARNESS_RUN_ID}"
CSV_FILE="${RUN_DIR}/e8-central.csv"
ALL_CSV_FILE="${RESULT_DIR}/e8-all-results.csv"
CLIENT_RESULTS_DIR="${RUN_DIR}/client-live"
mkdir -p "${CLIENT_RESULTS_DIR}" "${RUN_DIR}/client-output"

CSV_HEADER="run_id,equivalent_peers,customers,days,query_label,repeat,ttl,elapsed_seconds,rows_returned,wire_bytes"
[[ -f "${ALL_CSV_FILE}" ]] || echo "${CSV_HEADER}" > "${ALL_CSV_FILE}"
echo "${CSV_HEADER#run_id,}" > "${CSV_FILE}"

QUERIES=(
    "q1_customers|get Customer"
    "q2_open_offers|get Offer where status = \"open\""
    "q3_meter_readings_raw|get MeterReading where readingType = \"activePower\""
    "q4_active_power_sum|get MeterReading where readingType = \"activePower\" show @sum(value)"
    "q5_customer_traversal|get Customer traverse owner:owns:asset:UsagePoint traverse point:records:reading:MeterReading"
    "q6_prosumer_energy_sum|get Customer where role = \"prosumer\" traverse owner:owns:asset:UsagePoint traverse point:records:reading:MeterReading where readingType = \"activePower\" end end show @sum(value)"
)

cleanup() {
    harness_stop_cluster
}
trap cleanup EXIT

LOG_DIR="${RUN_DIR}/peer-logs"
if ! harness_start_cluster 1 "${LOG_DIR}" "${RUN_DIR}/launch.log" \
    --customers "${CUSTOMERS}" --days "${E8_DAYS}" --interval-minutes 15 -policy "${E8_POLICY:-policy.permissive.yaml}"; then
    exit 1
fi
PEER_ID=${HARNESS_PEER_ADDRESS##*/p2p/}
PEER_LOG="${LOG_DIR}/${PEER_ID}.log"
# Never query a store that is still loading.
for _ in $(seq 1 "${START_TIMEOUT_SECONDS}"); do
    grep -q "Data loaded into the graph database" "${PEER_LOG}" 2>/dev/null && break
    sleep 1
done
if ! grep -q "Data loaded into the graph database" "${PEER_LOG}" 2>/dev/null; then
    echo "Central store did not finish loading within ${START_TIMEOUT_SECONDS} s" >&2
    exit 1
fi

# Load time (generation finished -> data loaded) and store size.
load_seconds=$(python3 - "${PEER_LOG}" <<'PY'
import re, sys
from datetime import datetime
start = end = None
for line in open(sys.argv[1], errors="replace"):
    m = re.match(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+)", line)
    if not m:
        continue
    t = datetime.fromisoformat(m.group(1))
    if "now loading data" in line and start is None:
        start = t
    if "Data loaded into the graph database" in line:
        end = t
print(f"{(end - start).total_seconds():.1f}" if start and end else "")
PY
)
db_root=${DB_PATH_ROOT:-${HARNESS_SERVER_DIR}/idss_graph_db}
db_bytes=$(du -sb "${db_root}/${PEER_ID}" 2>/dev/null | awk '{print $1}')

cat > "${RUN_DIR}/metadata.txt" <<EOF
run_id=${RUN_ID}
experiment=E8-centralised-baseline
started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
equivalent_peers=${EQUIV}
customers=${CUSTOMERS}
days=${E8_DAYS}
repeats=${REPEATS}
ttl_seconds=${E8_TTL}
load_seconds=${load_seconds}
store_bytes=${db_bytes:-}
host=$(hostname)
gomaxprocs=${PEER_GOMAXPROCS}
EOF

for repeat in $(seq 1 "${REPEATS}"); do
    for query_spec in "${QUERIES[@]}"; do
        label=${query_spec%%|*}
        query=${query_spec#*|}
        client_output="${RUN_DIR}/client-output/${label}-${repeat}.log"
        harness_run_query "${CLIENT_RESULTS_DIR}" "${client_output}" "${HARNESS_PEER_ADDRESS}" manager "${query}" "${E8_TTL}"
        row="${EQUIV},${CUSTOMERS},${E8_DAYS},${label},${repeat},${E8_TTL},${HARNESS_ELAPSED},${HARNESS_ROWS},${HARNESS_WIRE_BYTES:-}"
        echo "${row}" >> "${CSV_FILE}"
        echo "${RUN_ID},${row}" >> "${ALL_CSV_FILE}"
    done
done
trap - EXIT
harness_stop_cluster
rm -rf "${db_root:?}/${PEER_ID}"

echo "Wrote E8 results to ${CSV_FILE} (load ${load_seconds} s, store ${db_bytes:-?} bytes)"
cat "${CSV_FILE}"
