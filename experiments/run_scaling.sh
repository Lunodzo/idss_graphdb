#!/bin/bash

# Run repeatable IDSS scaling experiments and write CSV measurements.
#
# Usage: ./experiments/run_scaling.sh <max_peers> <repeats>

set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "Usage: $0 <max_peers> <repeats>"
    exit 1
fi

MAX_PEERS=$1
REPEATS=$2
if ! [[ "${MAX_PEERS}" =~ ^[0-9]+$ ]] || ! [[ "${REPEATS}" =~ ^[0-9]+$ ]] || (( MAX_PEERS < 2 || REPEATS < 1 )); then
    echo "max_peers must be at least 2 and repeats must be at least 1" >&2
    exit 1
fi


# 
START_PEERS=${START_PEERS:-2}
if ! [[ "${START_PEERS}" =~ ^[0-9]+$ ]] || (( START_PEERS < 2 || START_PEERS > MAX_PEERS )); then
    echo "START_PEERS must be between 2 and max_peers" >&2
    exit 1
fi

PEER_STEP=${PEER_STEP:-1}
if ! [[ "${PEER_STEP}" =~ ^[1-9][0-9]*$ ]]; then
    echo "PEER_STEP must be a positive integer" >&2
    exit 1
fi

for command in go python3 curl; do
    if ! command -v "${command}" >/dev/null 2>&1; then
        echo "Required command not found: ${command}" >&2
        echo "Install Go 1.23+, Python 3, and curl, then rerun this script." >&2
        exit 1
    fi
done

QUERY_TIMEOUT_SECONDS=${QUERY_TIMEOUT_SECONDS:-120}
LAUNCH_TIMEOUT_SECONDS=${LAUNCH_TIMEOUT_SECONDS:-600}
DISCOVERY_TIMEOUT_SECONDS=${DISCOVERY_TIMEOUT_SECONDS:-600}
SCALING_CUSTOMERS=${SCALING_CUSTOMERS:-4}
SCALING_DAYS=${SCALING_DAYS:-1}
SCALING_INTERVAL_MINUTES=${SCALING_INTERVAL_MINUTES:-15}
if ! [[ "${SCALING_CUSTOMERS}" =~ ^[1-9][0-9]*$ && "${SCALING_DAYS}" =~ ^[1-9][0-9]*$ && "${SCALING_INTERVAL_MINUTES}" =~ ^[1-9][0-9]*$ ]]; then
    echo "SCALING_CUSTOMERS, SCALING_DAYS, and SCALING_INTERVAL_MINUTES must be positive integers" >&2
    exit 1
fi
if ! [[ "${QUERY_TIMEOUT_SECONDS}" =~ ^[0-9]+$ ]] || (( QUERY_TIMEOUT_SECONDS < 1 )); then
    echo "QUERY_TIMEOUT_SECONDS must be a positive integer" >&2
    exit 1
fi
if ! command -v timeout >/dev/null 2>&1; then
    echo "Required command not found: timeout" >&2
    exit 1
fi

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
SERVER_DIR="${ROOT_DIR}/server"
CLIENT_DIR="${ROOT_DIR}/client"
RESULT_DIR="${ROOT_DIR}/experiments/results"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-peers${MAX_PEERS}-repeats${REPEATS}"
RUN_DIR="${RESULT_DIR}/${RUN_ID}"
SCALING_RESULTS_DIR="${RUN_DIR}/client-live"
SCALING_LOG_DIR="${RUN_DIR}/live-peer-logs"
run_suffix=1
while [[ -e "${RUN_DIR}" ]]; do
    run_suffix=$((run_suffix + 1))
    RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-peers${MAX_PEERS}-repeats${REPEATS}-${run_suffix}"
    RUN_DIR="${RESULT_DIR}/${RUN_ID}"
done
CSV_FILE="${RUN_DIR}/scaling.csv"
FORWARDING_CSV_FILE="${RUN_DIR}/forwarding.csv"
ALL_CSV_FILE="${RESULT_DIR}/all-results.csv"
QUERIES=(
    "customers|get Customer"
    "open_offers|get Offer where status = \"open\""
    "active_power_sum|get MeterReading where readingType = \"activePower\" show @sum(value)"
)

mkdir -p "${RUN_DIR}/client-results" "${RUN_DIR}/peer-logs" "${RUN_DIR}/client-output" "${SCALING_RESULTS_DIR}" "${SCALING_LOG_DIR}"
if [[ ! -f "${ALL_CSV_FILE}" ]]; then
    echo "run_id,peer_count,query_label,ttl,elapsed_seconds,peers_responded,rows_returned" > "${ALL_CSV_FILE}"
fi
echo "peer_count,query_label,ttl,elapsed_seconds,peers_responded,rows_returned" > "${CSV_FILE}"
echo "peer_count,query_label,ttl,forwarded_queries,intermediate_results,stream_closed" > "${FORWARDING_CSV_FILE}"
cat > "${RUN_DIR}/metadata.txt" <<EOF
run_id=${RUN_ID}
started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
max_peers=${MAX_PEERS}
start_peers=${START_PEERS}
peer_step=${PEER_STEP}
repeats=${REPEATS}
query_timeout_seconds=${QUERY_TIMEOUT_SECONDS}
launch_timeout_seconds=${LAUNCH_TIMEOUT_SECONDS}
discovery_timeout_seconds=${DISCOVERY_TIMEOUT_SECONDS}
customers=${SCALING_CUSTOMERS}
days=${SCALING_DAYS}
interval_minutes=${SCALING_INTERVAL_MINUTES}
ttl_policy=starts at 1 and increases until all current-run peers respond or the peer-count bound is reached
EOF

launcher_pid=""
current_peer_count=""
current_repeat=""
preserve_peer_logs() {
    if [[ -n "${current_peer_count}" && -d "${SCALING_LOG_DIR}" ]]; then
        mkdir -p "${RUN_DIR}/peer-logs/${current_peer_count}-${current_repeat}"
        cp -a "${SCALING_LOG_DIR}/." "${RUN_DIR}/peer-logs/${current_peer_count}-${current_repeat}/" 2>/dev/null || true
    fi
}

cleanup() {
    preserve_peer_logs
    if [[ -n "${launcher_pid}" ]]; then
        kill "${launcher_pid}" 2>/dev/null || true
        wait "${launcher_pid}" 2>/dev/null || true
    fi
}
trap cleanup EXIT

monotonic_seconds() {
    python3 -c 'import time; print(f"{time.monotonic():.9f}")'
}

count_server_logs() {
    local pattern=$1
    grep -hFic "${pattern}" "${SCALING_LOG_DIR}"/*.log 2>/dev/null | awk -F: '{ total += $NF } END { print total + 0 }' || true
}

for peer_count in $(seq "${PEER_STEP}" "${PEER_STEP}" "${MAX_PEERS}"); do
    if (( peer_count < START_PEERS )); then
        continue
    fi
    for repeat in $(seq 1 "${REPEATS}"); do
        current_peer_count="${peer_count}"
        current_repeat="${repeat}"
        pushd "${SERVER_DIR}" >/dev/null
        peer_address=""
        LOG_DIR="${SCALING_LOG_DIR}" LAUNCH_TIMEOUT_SECONDS="${LAUNCH_TIMEOUT_SECONDS}" DISCOVERY_TIMEOUT_SECONDS="${DISCOVERY_TIMEOUT_SECONDS}" START_BATCH_SIZE="${START_BATCH_SIZE:-5}" ./start_peers.sh "${peer_count}" --customers "${SCALING_CUSTOMERS}" --days "${SCALING_DAYS}" --interval-minutes "${SCALING_INTERVAL_MINUTES}" > "${RUN_DIR}/peers-${peer_count}-${repeat}.log" 2>&1 &
        launcher_pid=$!

        for attempt in $(seq 1 "${LAUNCH_TIMEOUT_SECONDS}"); do
            peer_address=$(grep "First peer address:" "${RUN_DIR}/peers-${peer_count}-${repeat}.log" | awk '{print $NF}' || true)
            launched_count=$(grep -c 'Peer [0-9][0-9]* launched with ID ' "${RUN_DIR}/peers-${peer_count}-${repeat}.log" 2>/dev/null || true)
            if [[ -n "${peer_address}" ]] && (( launched_count == peer_count )) && grep -q "All peers have joined the overlay." "${RUN_DIR}/peers-${peer_count}-${repeat}.log"; then
                break
            fi
            if ! kill -0 "${launcher_pid}" 2>/dev/null; then
                echo "Peer launcher failed for ${peer_count} peers:" >&2
                cat "${RUN_DIR}/peers-${peer_count}-${repeat}.log" >&2
                popd >/dev/null
                exit 1
            fi
            sleep 1
        done
        if [[ -z "${peer_address:-}" ]] || (( launched_count != peer_count )) || ! grep -q "All peers have joined the overlay." "${RUN_DIR}/peers-${peer_count}-${repeat}.log"; then
            echo "Peers did not become ready: expected ${peer_count}, launched ${launched_count:-0}" >&2
            cat "${RUN_DIR}/peers-${peer_count}-${repeat}.log" >&2
            kill "${launcher_pid}" 2>/dev/null || true
            wait "${launcher_pid}" 2>/dev/null || true
            popd >/dev/null
            exit 1
        fi
        mapfile -t peer_ids < <(grep 'Peer [0-9][0-9]* launched with ID ' "${RUN_DIR}/peers-${peer_count}-${repeat}.log" | awk '{print $NF}')
        ttl_limit=1
        ttl_levels=0
        while (( ttl_limit < peer_count )); do
            ttl_limit=$((ttl_limit * 2))
            ttl_levels=$((ttl_levels + 1))
        done
        ttl_limit=$((ttl_levels + 2))
        popd >/dev/null

        for query_spec in "${QUERIES[@]}"; do
            label=${query_spec%%|*}
            query=${query_spec#*|}
            for ttl in $(seq 1 "${ttl_limit}"); do
                rm -rf "${SCALING_RESULTS_DIR}"
                mkdir -p "${SCALING_RESULTS_DIR}"
                query_artifact_dir="${RUN_DIR}/client-results/${peer_count}-${repeat}-${label}-ttl${ttl}"
                mkdir -p "${query_artifact_dir}"
                client_output=$(mktemp)
                started=$(monotonic_seconds)
                pushd "${CLIENT_DIR}" >/dev/null
                if ! printf '%s, %s\nexit\n' "${query}" "${ttl}" | IDSS_CLIENT_RESULTS_DIR="${SCALING_RESULTS_DIR}" timeout --kill-after=10 "${QUERY_TIMEOUT_SECONDS}s" go run . -role manager -s "${peer_address}" >"${client_output}" 2>&1; then
                    cp "${client_output}" "${RUN_DIR}/client-output/${peer_count}-${repeat}-${label}-ttl${ttl}.log" 2>/dev/null || true
                    echo "Client query failed for ${label} with TTL ${ttl}" >&2
                    cat "${client_output}" >&2
                    popd >/dev/null
                    rm -f "${client_output}"
                    exit 1
                fi
                cp "${client_output}" "${RUN_DIR}/client-output/${peer_count}-${repeat}-${label}-ttl${ttl}.log"
                popd >/dev/null
                finished=$(monotonic_seconds)
                elapsed=$(awk -v start="${started}" -v end="${finished}" 'BEGIN { value = end - start; if (value < 0) value = 0; printf "%.6f", value }')
                responding_peer_count=$(sed -n 's/.*Responding peers:[[:space:]]*\([0-9][0-9]*\).*/\1/p' "${client_output}" | tail -n 1)
                responding_peer_count=${responding_peer_count:-0}
                result_file=$(find "${SCALING_RESULTS_DIR}" -type f -name '*.json' | head -n 1)
                if [[ -z "${result_file}" ]]; then
                    echo "Client produced no JSON result for ${label} with TTL ${ttl}" >&2
                    cat "${client_output}" >&2
                    rm -f "${client_output}"
                    exit 1
                fi
                rows_returned=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("resultCount", 0))' "${result_file}")
                rows_returned=${rows_returned:-0}
                forwarded_queries=$(count_server_logs "Query sent to peer")
                intermediate_results=$(count_server_logs "Intermediate peer")
                stream_closed=$(count_server_logs "stream closed")
                cp -a "${SCALING_RESULTS_DIR}/." "${query_artifact_dir}/"
                peers_responded=${responding_peer_count}
                echo "${peer_count},${label},${ttl},${elapsed},${peers_responded},${rows_returned}" >> "${CSV_FILE}"
                echo "${RUN_ID},${peer_count},${label},${ttl},${elapsed},${peers_responded},${rows_returned}" >> "${ALL_CSV_FILE}"
                echo "${peer_count},${label},${ttl},${forwarded_queries},${intermediate_results},${stream_closed}" >> "${FORWARDING_CSV_FILE}"
                rm -f "${client_output}"

                if (( peers_responded >= peer_count )); then
                    break
                fi
            done
        done

        preserve_peer_logs
        kill "${launcher_pid}" 2>/dev/null || true
        wait "${launcher_pid}" 2>/dev/null || true
        launcher_pid=""
    done
done

echo "Wrote scaling results to ${CSV_FILE}"
echo "Run artifacts preserved in ${RUN_DIR}"
