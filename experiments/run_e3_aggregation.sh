#!/bin/bash
# E3: what do aggregation and result handling save?
#
# Compares Q3 (raw MeterReading records) against Q4 (the @sum aggregate of the
# same predicate) over one community configuration, reporting elapsed time and
# wire bytes returned (post-compression frame size, see
# client/idss_client.go readDelimitedMessage). Run once per configuration
# (e.g. medium and large) by passing different customers/days values.
#
# Uses the permissive policy so Q3 returns raw records rather than a
# policy-restricted count; run_e4_governance.sh exercises the RBAC dimension.
#
# Usage: ./experiments/run_e3_aggregation.sh <peer_count> <customers> <days> [interval_minutes] [repeats]

set -euo pipefail

if [[ $# -lt 3 ]]; then
    echo "Usage: $0 <peer_count> <customers> <days> [interval_minutes] [repeats]" >&2
    exit 1
fi

PEER_COUNT=$1
CUSTOMERS=$2
DAYS=$3
INTERVAL_MINUTES=${4:-15}
REPEATS=${5:-5}
TTL_SECONDS=${TTL_SECONDS:-30}
QUERY_ROLE=${QUERY_ROLE:-member}
POLICY_FILE=${POLICY_FILE:-policy.permissive.yaml}

for value in "${PEER_COUNT}" "${CUSTOMERS}" "${DAYS}" "${INTERVAL_MINUTES}" "${REPEATS}"; do
    if ! [[ "${value}" =~ ^[0-9]+$ ]] || (( value < 1 )); then
        echo "peer_count, customers, days, interval_minutes, and repeats must all be positive integers" >&2
        exit 1
    fi
done

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=experiments/lib.sh
source "${ROOT_DIR}/lib.sh"
harness_require_commands go python3 timeout

RESULT_DIR="${ROOT_DIR}/results"
harness_new_run_dir "${RESULT_DIR}" "e3-peers${PEER_COUNT}-c${CUSTOMERS}-d${DAYS}"
RUN_DIR="${HARNESS_RUN_DIR}"
RUN_ID="${HARNESS_RUN_ID}"
CSV_FILE="${RUN_DIR}/e3-aggregation.csv"
ALL_CSV_FILE="${RESULT_DIR}/e3-all-results.csv"
LOG_DIR="${RUN_DIR}/peer-logs"
CLIENT_RESULTS_DIR="${RUN_DIR}/client-live"

mkdir -p "${LOG_DIR}" "${CLIENT_RESULTS_DIR}" "${RUN_DIR}/client-output"
if [[ ! -f "${ALL_CSV_FILE}" ]]; then
    echo "run_id,peer_count,customers,days,interval_minutes,query_label,repeat,ttl,elapsed_seconds,peers_responded,rows_returned,wire_bytes" > "${ALL_CSV_FILE}"
fi
echo "peer_count,customers,days,interval_minutes,query_label,repeat,ttl,elapsed_seconds,peers_responded,rows_returned,wire_bytes" > "${CSV_FILE}"
cat > "${RUN_DIR}/metadata.txt" <<EOF
run_id=${RUN_ID}
experiment=E3-aggregation-savings
started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
peer_count=${PEER_COUNT}
customers=${CUSTOMERS}
days=${DAYS}
interval_minutes=${INTERVAL_MINUTES}
repeats=${REPEATS}
ttl_seconds=${TTL_SECONDS}
role=${QUERY_ROLE}
policy=${POLICY_FILE}
EOF

QUERIES=(
    "q3_meter_readings_raw|get MeterReading where readingType = \"activePower\""
    "q4_active_power_sum|get MeterReading where readingType = \"activePower\" show @sum(value)"
)

cleanup() {
    harness_stop_cluster
}
trap cleanup EXIT

if ! harness_start_cluster "${PEER_COUNT}" "${LOG_DIR}" "${RUN_DIR}/launch.log" \
    --customers "${CUSTOMERS}" --days "${DAYS}" --interval-minutes "${INTERVAL_MINUTES}" -policy "${POLICY_FILE}"; then
    exit 1
fi

for repeat in $(seq 1 "${REPEATS}"); do
    for query_spec in "${QUERIES[@]}"; do
        label=${query_spec%%|*}
        query=${query_spec#*|}
        client_output="${RUN_DIR}/client-output/${label}-${repeat}.log"
        if ! harness_run_query "${CLIENT_RESULTS_DIR}" "${client_output}" "${HARNESS_PEER_ADDRESS}" "${QUERY_ROLE}" "${query}" "${TTL_SECONDS}"; then
            harness_stop_cluster
            exit 1
        fi
        echo "${PEER_COUNT},${CUSTOMERS},${DAYS},${INTERVAL_MINUTES},${label},${repeat},${TTL_SECONDS},${HARNESS_ELAPSED},${HARNESS_RESPONDERS},${HARNESS_ROWS},${HARNESS_WIRE_BYTES}" >> "${CSV_FILE}"
        echo "${RUN_ID},${PEER_COUNT},${CUSTOMERS},${DAYS},${INTERVAL_MINUTES},${label},${repeat},${TTL_SECONDS},${HARNESS_ELAPSED},${HARNESS_RESPONDERS},${HARNESS_ROWS},${HARNESS_WIRE_BYTES}" >> "${ALL_CSV_FILE}"
    done
done

harness_stop_cluster
trap - EXIT

echo "Wrote E3 results to ${CSV_FILE}"
echo "Summary (mean elapsed_seconds, wire_bytes by query_label):"
python3 "${ROOT_DIR}/summarize_csv.py" "${CSV_FILE}" --group query_label --value elapsed_seconds
python3 "${ROOT_DIR}/summarize_csv.py" "${CSV_FILE}" --group query_label --value wire_bytes
