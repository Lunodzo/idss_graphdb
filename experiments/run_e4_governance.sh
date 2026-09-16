#!/bin/bash
# E4: does the governance layer behave as specified, and at what cost?
#
# Correctness: runs Q1-Q4 under each requester role against the default
# policy, extracts the access decision the receiving peer logged for that
# exact client, and checks it against the expected decision from
# server/policy.default.yaml (see check_e4_decisions.py). Also confirms that
# a "deny" decision does not suppress propagation, by diffing the
# "Query sent to peer" log count across the query.
#
# Cost: the same queries are then run against the permissive policy so the
# overhead of policy evaluation can be isolated from query execution cost by
# comparing elapsed times between the two policy runs for the same role/query.
#
# Usage: ./experiments/run_e4_governance.sh <peer_count> [repeats]

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <peer_count> [repeats]" >&2
    exit 1
fi

PEER_COUNT=$1
REPEATS=${2:-5}
TTL_SECONDS=${TTL_SECONDS:-15}
E4_CUSTOMERS=${E4_CUSTOMERS:-10}
E4_DAYS=${E4_DAYS:-1}
E4_INTERVAL_MINUTES=${E4_INTERVAL_MINUTES:-15}

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
harness_new_run_dir "${RESULT_DIR}" "e4-peers${PEER_COUNT}-repeats${REPEATS}"
RUN_DIR="${HARNESS_RUN_DIR}"
RUN_ID="${HARNESS_RUN_ID}"
CSV_FILE="${RUN_DIR}/e4-governance.csv"
ALL_CSV_FILE="${RESULT_DIR}/e4-all-results.csv"
CLIENT_RESULTS_DIR="${RUN_DIR}/client-live"

mkdir -p "${CLIENT_RESULTS_DIR}" "${RUN_DIR}/client-output"
CSV_HEADER="run_id,policy,role,query_label,repeat,ttl,elapsed_seconds,peers_responded,rows_returned,decision,forwarded_delta"
if [[ ! -f "${ALL_CSV_FILE}" ]]; then
    echo "${CSV_HEADER}" > "${ALL_CSV_FILE}"
fi
echo "${CSV_HEADER#run_id,}" > "${CSV_FILE}"
cat > "${RUN_DIR}/metadata.txt" <<EOF
run_id=${RUN_ID}
experiment=E4-governance
started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
peer_count=${PEER_COUNT}
repeats=${REPEATS}
ttl_seconds=${TTL_SECONDS}
customers=${E4_CUSTOMERS}
days=${E4_DAYS}
interval_minutes=${E4_INTERVAL_MINUTES}
EOF

QUERIES=(
    "q1_customers|get Customer"
    "q2_open_offers|get Offer where status = \"open\""
    "q3_meter_readings_raw|get MeterReading where readingType = \"activePower\""
    "q4_active_power_sum|get MeterReading where readingType = \"activePower\" show @sum(value)"
)
ROLES=(member manager observer)
POLICIES=(policy.default.yaml policy.permissive.yaml)

cleanup() {
    harness_stop_cluster
}
trap cleanup EXIT

for policy in "${POLICIES[@]}"; do
    log_dir="${RUN_DIR}/peer-logs-${policy%.yaml}"
    if ! harness_start_cluster "${PEER_COUNT}" "${log_dir}" "${RUN_DIR}/launch-${policy%.yaml}.log" \
        --customers "${E4_CUSTOMERS}" --days "${E4_DAYS}" --interval-minutes "${E4_INTERVAL_MINUTES}" -policy "${policy}"; then
        exit 1
    fi

    for repeat in $(seq 1 "${REPEATS}"); do
        for role in "${ROLES[@]}"; do
            for query_spec in "${QUERIES[@]}"; do
                label=${query_spec%%|*}
                query=${query_spec#*|}
                client_output="${RUN_DIR}/client-output/${policy%.yaml}-${role}-${label}-${repeat}.log"

                forwarded_before=$(harness_count_server_logs "${log_dir}" "Query sent to peer")
                if ! harness_run_query "${CLIENT_RESULTS_DIR}" "${client_output}" "${HARNESS_PEER_ADDRESS}" "${role}" "${query}" "${TTL_SECONDS}"; then
                    harness_stop_cluster
                    exit 1
                fi
                forwarded_after=$(harness_count_server_logs "${log_dir}" "Query sent to peer")
                forwarded_delta=$((forwarded_after - forwarded_before))

                client_peer_id=$(harness_extract_client_peer_id "${client_output}")
                decision=$(harness_decision_for_client "${log_dir}" "${client_peer_id}")
                decision=${decision:-unknown}

                echo "${policy},${role},${label},${repeat},${TTL_SECONDS},${HARNESS_ELAPSED},${HARNESS_RESPONDERS},${HARNESS_ROWS},${decision},${forwarded_delta}" >> "${CSV_FILE}"
                echo "${RUN_ID},${policy},${role},${label},${repeat},${TTL_SECONDS},${HARNESS_ELAPSED},${HARNESS_RESPONDERS},${HARNESS_ROWS},${decision},${forwarded_delta}" >> "${ALL_CSV_FILE}"
            done
        done
    done

    harness_stop_cluster
done
trap - EXIT

echo "Wrote E4 results to ${CSV_FILE}"
echo "Correctness check against server/policy.default.yaml:"
python3 "${ROOT_DIR}/check_e4_decisions.py" "${CSV_FILE}"
echo "Cost summary (mean elapsed_seconds by policy, role, query_label):"
python3 "${ROOT_DIR}/summarize_csv.py" "${CSV_FILE}" --group policy,role,query_label --value elapsed_seconds
