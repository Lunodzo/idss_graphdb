#!/bin/bash
# E2: what does the time budget buy?
#
# Fixes the peer count N and sweeps TTL across an explicit list, reporting the
# fraction of peers that responded and rows returned for Q1-Q5. Characterizes
# the completeness-latency trade-off and the TTL budget needed for a
# community-wide query to become effectively complete.
#
# Uses the permissive policy for the same reason as run_e1_scale.sh: this
# experiment characterizes platform behaviour, not the governance layer.
#
# Usage: ./experiments/run_e2_ttl.sh <peer_count> [repeats]

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <peer_count> [repeats]" >&2
    exit 1
fi

PEER_COUNT=$1
REPEATS=${2:-5}
TTL_LIST=${TTL_LIST:-"0.5 1 2 3 5 8 13 20"}
QUERY_ROLE=${QUERY_ROLE:-member}
E2_CUSTOMERS=${E2_CUSTOMERS:-10}
E2_DAYS=${E2_DAYS:-1}
E2_INTERVAL_MINUTES=${E2_INTERVAL_MINUTES:-15}
POLICY_FILE=${POLICY_FILE:-policy.permissive.yaml}

if ! [[ "${PEER_COUNT}" =~ ^[0-9]+$ ]] || (( PEER_COUNT < 2 )); then
    echo "peer_count must be an integer >= 2" >&2
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
harness_new_run_dir "${RESULT_DIR}" "e2-peers${PEER_COUNT}-repeats${REPEATS}"
RUN_DIR="${HARNESS_RUN_DIR}"
RUN_ID="${HARNESS_RUN_ID}"
CSV_FILE="${RUN_DIR}/e2-ttl.csv"
ALL_CSV_FILE="${RESULT_DIR}/e2-all-results.csv"
LOG_DIR="${RUN_DIR}/peer-logs"
CLIENT_RESULTS_DIR="${RUN_DIR}/client-live"

mkdir -p "${LOG_DIR}" "${CLIENT_RESULTS_DIR}" "${RUN_DIR}/client-output"
if [[ ! -f "${ALL_CSV_FILE}" ]]; then
    echo "run_id,peer_count,query_label,repeat,ttl,elapsed_seconds,peers_responded,responded_fraction,rows_returned" > "${ALL_CSV_FILE}"
fi
echo "peer_count,query_label,repeat,ttl,elapsed_seconds,peers_responded,responded_fraction,rows_returned" > "${CSV_FILE}"
cat > "${RUN_DIR}/metadata.txt" <<EOF
run_id=${RUN_ID}
experiment=E2-ttl-sweep
started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
peer_count=${PEER_COUNT}
repeats=${REPEATS}
ttl_list=${TTL_LIST}
role=${QUERY_ROLE}
policy=${POLICY_FILE}
customers=${E2_CUSTOMERS}
days=${E2_DAYS}
interval_minutes=${E2_INTERVAL_MINUTES}
EOF

QUERIES=(
    "q1_customers|get Customer"
    "q2_open_offers|get Offer where status = \"open\""
    "q3_meter_readings_raw|get MeterReading where readingType = \"activePower\""
    "q4_active_power_sum|get MeterReading where readingType = \"activePower\" show @sum(value)"
    "q5_customer_traversal|get Customer traverse owner:owns:asset:UsagePoint traverse point:records:reading:MeterReading"
)

cleanup() {
    harness_stop_cluster
}
trap cleanup EXIT

if ! harness_start_cluster "${PEER_COUNT}" "${LOG_DIR}" "${RUN_DIR}/launch.log" \
    --customers "${E2_CUSTOMERS}" --days "${E2_DAYS}" --interval-minutes "${E2_INTERVAL_MINUTES}" -policy "${POLICY_FILE}"; then
    exit 1
fi

for repeat in $(seq 1 "${REPEATS}"); do
    for query_spec in "${QUERIES[@]}"; do
        label=${query_spec%%|*}
        query=${query_spec#*|}
        for ttl in ${TTL_LIST}; do
            client_output="${RUN_DIR}/client-output/${label}-ttl${ttl}-${repeat}.log"
            if ! harness_run_query "${CLIENT_RESULTS_DIR}" "${client_output}" "${HARNESS_PEER_ADDRESS}" "${QUERY_ROLE}" "${query}" "${ttl}"; then
                harness_stop_cluster
                exit 1
            fi
            responded_fraction=$(awk -v r="${HARNESS_RESPONDERS}" -v n="${PEER_COUNT}" 'BEGIN { printf "%.6f", r / n }')
            echo "${PEER_COUNT},${label},${repeat},${ttl},${HARNESS_ELAPSED},${HARNESS_RESPONDERS},${responded_fraction},${HARNESS_ROWS}" >> "${CSV_FILE}"
            echo "${RUN_ID},${PEER_COUNT},${label},${repeat},${ttl},${HARNESS_ELAPSED},${HARNESS_RESPONDERS},${responded_fraction},${HARNESS_ROWS}" >> "${ALL_CSV_FILE}"
        done
    done
done

harness_stop_cluster
trap - EXIT

echo "Wrote E2 results to ${CSV_FILE}"
echo "Summary (mean responded_fraction by query_label, ttl):"
python3 "${ROOT_DIR}/summarize_csv.py" "${CSV_FILE}" --group query_label,ttl --value responded_fraction
