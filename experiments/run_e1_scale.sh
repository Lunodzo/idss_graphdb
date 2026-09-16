#!/bin/bash
# E1: how does query cost scale with community size?
#
# Sweeps peer count N over a fixed per-peer configuration (C customers, D days,
# quarter-hourly readings by default) and runs Q1-Q5 once per peer count at a
# single generous fixed TTL. Reports elapsed time, responding peers, and rows
# returned against N, repeated REPEATS times (default 5) with the mean taken
# downstream by experiments/summarize_csv.py.
#
# Uses the permissive policy so Q3/Q5 measure real record volume rather than
# the access-control decision; RBAC effects are the subject of run_e4_governance.sh.
#
# Usage: ./experiments/run_e1_scale.sh <max_peers> [repeats]

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <max_peers> [repeats]" >&2
    exit 1
fi

MAX_PEERS=$1
REPEATS=${2:-5}
START_PEERS=${START_PEERS:-2}
PEER_STEP=${PEER_STEP:-1}
TTL_SECONDS=${TTL_SECONDS:-20}
QUERY_ROLE=${QUERY_ROLE:-member}
E1_CUSTOMERS=${E1_CUSTOMERS:-10}
E1_DAYS=${E1_DAYS:-1}
E1_INTERVAL_MINUTES=${E1_INTERVAL_MINUTES:-15}
POLICY_FILE=${POLICY_FILE:-policy.permissive.yaml}

if ! [[ "${MAX_PEERS}" =~ ^[0-9]+$ ]] || (( MAX_PEERS < START_PEERS )); then
    echo "max_peers must be an integer >= START_PEERS (${START_PEERS})" >&2
    exit 1
fi
if ! [[ "${REPEATS}" =~ ^[0-9]+$ ]] || (( REPEATS < 1 )); then
    echo "repeats must be a positive integer" >&2
    exit 1
fi

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=experiments/lib.sh
source "${ROOT_DIR}/lib.sh"
harness_require_commands go python3 timeout

RESULT_DIR="${ROOT_DIR}/results"
harness_new_run_dir "${RESULT_DIR}" "e1-peers${MAX_PEERS}-repeats${REPEATS}"
RUN_DIR="${HARNESS_RUN_DIR}"
RUN_ID="${HARNESS_RUN_ID}"
CSV_FILE="${RUN_DIR}/e1-scale.csv"
ALL_CSV_FILE="${RESULT_DIR}/e1-all-results.csv"
LOG_DIR="${RUN_DIR}/peer-logs"
CLIENT_RESULTS_DIR="${RUN_DIR}/client-live"

mkdir -p "${LOG_DIR}" "${CLIENT_RESULTS_DIR}" "${RUN_DIR}/client-output"
if [[ ! -f "${ALL_CSV_FILE}" ]]; then
    echo "run_id,peer_count,query_label,repeat,ttl,elapsed_seconds,peers_responded,rows_returned" > "${ALL_CSV_FILE}"
fi
echo "peer_count,query_label,repeat,ttl,elapsed_seconds,peers_responded,rows_returned" > "${CSV_FILE}"
cat > "${RUN_DIR}/metadata.txt" <<EOF
run_id=${RUN_ID}
experiment=E1-scale-out
started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
max_peers=${MAX_PEERS}
start_peers=${START_PEERS}
peer_step=${PEER_STEP}
repeats=${REPEATS}
ttl_seconds=${TTL_SECONDS}
role=${QUERY_ROLE}
policy=${POLICY_FILE}
customers=${E1_CUSTOMERS}
days=${E1_DAYS}
interval_minutes=${E1_INTERVAL_MINUTES}
EOF

QUERIES=(
    "q1_customers|get Customer"
    "q2_open_offers|get Offer where status = \"open\""
    "q3_meter_readings_raw|get MeterReading where readingType = \"activePower\""
    "q4_active_power_sum|get MeterReading where readingType = \"activePower\" show @sum(value)"
    "q5_customer_traversal|get Customer traverse owner:owns:asset:UsagePoint traverse point:records:reading:MeterReading"
)

# Cleanup function to stop the cluster on exit
cleanup() {
    harness_stop_cluster
}
trap cleanup EXIT


# Main loop to run the experiment for different peer counts and repeats
for peer_count in $(seq "${START_PEERS}" "${PEER_STEP}" "${MAX_PEERS}"); do
    cluster_log_dir="${LOG_DIR}/${peer_count}"
    if ! harness_start_cluster "${peer_count}" "${cluster_log_dir}" "${RUN_DIR}/launch-${peer_count}.log" \
        --customers "${E1_CUSTOMERS}" --days "${E1_DAYS}" --interval-minutes "${E1_INTERVAL_MINUTES}" -policy "${POLICY_FILE}"; then
        exit 1
    fi

    for repeat in $(seq 1 "${REPEATS}"); do
        for query_spec in "${QUERIES[@]}"; do
            label=${query_spec%%|*}
            query=${query_spec#*|}
            client_output="${RUN_DIR}/client-output/${peer_count}-${label}-${repeat}.log"
            if ! harness_run_query "${CLIENT_RESULTS_DIR}" "${client_output}" "${HARNESS_PEER_ADDRESS}" "${QUERY_ROLE}" "${query}" "${TTL_SECONDS}"; then
                harness_stop_cluster
                exit 1
            fi
            echo "${peer_count},${label},${repeat},${TTL_SECONDS},${HARNESS_ELAPSED},${HARNESS_RESPONDERS},${HARNESS_ROWS}" >> "${CSV_FILE}"
            echo "${RUN_ID},${peer_count},${label},${repeat},${TTL_SECONDS},${HARNESS_ELAPSED},${HARNESS_RESPONDERS},${HARNESS_ROWS}" >> "${ALL_CSV_FILE}"
        done
    done

    harness_stop_cluster
done
trap - EXIT

echo "Wrote E1 results to ${CSV_FILE}"
echo "Summary (mean elapsed by peer_count, query_label):"
python3 "${ROOT_DIR}/summarize_csv.py" "${CSV_FILE}" --group peer_count,query_label --value elapsed_seconds
