#!/bin/bash
# Shared helpers for the IDSS E1-E5 experiment scripts.
# Sourced by experiments/run_e*.sh; not meant to be executed directly.

HARNESS_ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
HARNESS_SERVER_DIR="${HARNESS_ROOT_DIR}/server"
HARNESS_CLIENT_DIR="${HARNESS_ROOT_DIR}/client"

harness_monotonic_seconds() {
    python3 -c 'import time; print(f"{time.monotonic():.9f}")'
}

harness_require_commands() {
    for command in "$@"; do
        if ! command -v "${command}" >/dev/null 2>&1; then
            echo "Required command not found: ${command}" >&2
            exit 1
        fi
    done
}

# harness_new_run_dir <results_root> <prefix>
# Sets HARNESS_RUN_ID, HARNESS_RUN_DIR (created).
harness_new_run_dir() {
    local results_root=$1
    local prefix=$2
    HARNESS_RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-${prefix}"
    HARNESS_RUN_DIR="${results_root}/${HARNESS_RUN_ID}"
    local suffix=1
    while [[ -e "${HARNESS_RUN_DIR}" ]]; do
        suffix=$((suffix + 1))
        HARNESS_RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-${prefix}-${suffix}"
        HARNESS_RUN_DIR="${results_root}/${HARNESS_RUN_ID}"
    done
    mkdir -p "${HARNESS_RUN_DIR}"
}

# harness_start_cluster <peer_count> <log_dir> <launch_log_file> [extra start_peers.sh args...]
# Sets HARNESS_PEER_ADDRESS, HARNESS_PEER_IDS (array), HARNESS_LAUNCHER_PID.
harness_start_cluster() {
    local peer_count=$1; shift
    local log_dir=$1; shift
    local launch_log=$1; shift
    local launch_timeout=${LAUNCH_TIMEOUT_SECONDS:-600}

    mkdir -p "${log_dir}"
    pushd "${HARNESS_SERVER_DIR}" >/dev/null
    LOG_DIR="${log_dir}" LAUNCH_TIMEOUT_SECONDS="${launch_timeout}" \
        DISCOVERY_TIMEOUT_SECONDS="${DISCOVERY_TIMEOUT_SECONDS:-600}" \
        START_BATCH_SIZE="${START_BATCH_SIZE:-5}" \
        ./start_peers.sh "${peer_count}" "$@" > "${launch_log}" 2>&1 &
    HARNESS_LAUNCHER_PID=$!

    HARNESS_PEER_ADDRESS=""
    local launched_count=0
    for _ in $(seq 1 "${launch_timeout}"); do
        HARNESS_PEER_ADDRESS=$(grep "First peer address:" "${launch_log}" | awk '{print $NF}' || true)
        launched_count=$(grep -c 'Peer [0-9][0-9]* launched with ID ' "${launch_log}" 2>/dev/null || true)
        if [[ -n "${HARNESS_PEER_ADDRESS}" ]] && (( launched_count == peer_count )) && grep -q "All peers have joined the overlay." "${launch_log}"; then
            break
        fi
        if ! kill -0 "${HARNESS_LAUNCHER_PID}" 2>/dev/null; then
            echo "Peer launcher failed for ${peer_count} peers:" >&2
            cat "${launch_log}" >&2
            popd >/dev/null
            return 1
        fi
        sleep 1
    done
    popd >/dev/null

    if [[ -z "${HARNESS_PEER_ADDRESS}" ]] || (( launched_count != peer_count )) || ! grep -q "All peers have joined the overlay." "${launch_log}"; then
        echo "Peers did not become ready: expected ${peer_count}, launched ${launched_count:-0}" >&2
        cat "${launch_log}" >&2
        harness_stop_cluster
        return 1
    fi

    mapfile -t HARNESS_PEER_IDS < <(grep 'Peer [0-9][0-9]* launched with ID ' "${launch_log}" | awk '{print $NF}')
    return 0
}

harness_stop_cluster() {
    if [[ -n "${HARNESS_LAUNCHER_PID:-}" ]]; then
        kill "${HARNESS_LAUNCHER_PID}" 2>/dev/null || true
        wait "${HARNESS_LAUNCHER_PID}" 2>/dev/null || true
        HARNESS_LAUNCHER_PID=""
    fi
}

# harness_count_server_logs <log_dir> <pattern>
harness_count_server_logs() {
    local log_dir=$1
    local pattern=$2
    grep -hFic "${pattern}" "${log_dir}"/*.log 2>/dev/null | awk -F: '{ total += $NF } END { print total + 0 }' || true
}

# harness_run_query <results_dir> <client_output_log> <peer_address> <role> <query> <ttl> [timeout_seconds]
# Sets HARNESS_ELAPSED, HARNESS_RESPONDERS, HARNESS_ROWS, HARNESS_WIRE_BYTES, HARNESS_STATUS_LINE.
harness_run_query() {
    local results_dir=$1
    local client_output=$2
    local peer_address=$3
    local role=$4
    local query=$5
    local ttl=$6
    local timeout_seconds=${7:-${QUERY_TIMEOUT_SECONDS:-120}}

    rm -rf "${results_dir}"
    mkdir -p "${results_dir}"

    local started finished
    started=$(harness_monotonic_seconds)
    pushd "${HARNESS_CLIENT_DIR}" >/dev/null
    if ! printf '%s, %s\nexit\n' "${query}" "${ttl}" | IDSS_CLIENT_RESULTS_DIR="${results_dir}" \
        timeout --kill-after=10 "${timeout_seconds}s" go run . -role "${role}" -s "${peer_address}" >"${client_output}" 2>&1; then
        popd >/dev/null
        echo "Client query failed: role=${role} query=${query} ttl=${ttl}" >&2
        cat "${client_output}" >&2
        return 1
    fi
    popd >/dev/null
    finished=$(harness_monotonic_seconds)

    HARNESS_ELAPSED=$(awk -v start="${started}" -v end="${finished}" 'BEGIN { value = end - start; if (value < 0) value = 0; printf "%.6f", value }')
    HARNESS_RESPONDERS=$(sed -n 's/.*Responding peers:[[:space:]]*\([0-9][0-9]*\).*/\1/p' "${client_output}" | tail -n 1)
    HARNESS_RESPONDERS=${HARNESS_RESPONDERS:-0}
    HARNESS_WIRE_BYTES=$(sed -n 's/.*Wire bytes received:[[:space:]]*\([0-9][0-9]*\).*/\1/p' "${client_output}" | tail -n 1)
    HARNESS_WIRE_BYTES=${HARNESS_WIRE_BYTES:-0}
    HARNESS_STATUS_LINE=""

    local result_file
    result_file=$(find "${results_dir}" -type f -name '*.json' | head -n 1)
    if [[ -z "${result_file}" ]]; then
        HARNESS_ROWS=0
        HARNESS_STATUS_LINE=$(sed -n 's/.*Server response:[[:space:]]*\(.*\)$/\1/p' "${client_output}" | tail -n 1)
    else
        HARNESS_ROWS=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("resultCount", 0))' "${result_file}")
        HARNESS_ROWS=${HARNESS_ROWS:-0}
    fi
    return 0
}

# harness_last_access_decision <log_dir> <role>
# Greps peer logs for the most recent policy decision recorded for a role.
harness_last_access_decision() {
    local log_dir=$1
    local role=$2
    grep -h "Access decision for requester" "${log_dir}"/*.log 2>/dev/null \
        | grep -F "(${role})" | tail -n 1 \
        | sed -n 's/.*: \([a-z]*\)[[:space:]]*$/\1/p'
}

# harness_extract_client_peer_id <client_output_log>
# Parses the client's own libp2p peer ID so a policy decision line can be
# correlated to one specific query invocation instead of the last matching role.
harness_extract_client_peer_id() {
    local client_output=$1
    grep -A1 "Client listening on" "${client_output}" 2>/dev/null \
        | grep -o '/p2p/[A-Za-z0-9]*' | tail -n 1 | sed 's#/p2p/##'
}

# harness_decision_for_client <log_dir> <client_peer_id>
harness_decision_for_client() {
    local log_dir=$1
    local client_peer_id=$2
    grep -h "Access decision for requester ${client_peer_id} " "${log_dir}"/*.log 2>/dev/null \
        | tail -n 1 | sed -n 's/.*: \([a-z]*\)[[:space:]]*$/\1/p'
}

# harness_peer_address_from_log <log_dir> <peer_id>
# Reads a specific peer's own multiaddress from its log file, so a scenario
# script can target one named peer (e.g. a specific seller or buyer) rather
# than only the first peer.
harness_peer_address_from_log() {
    local log_dir=$1
    local peer_id=$2
    grep "Listening on peer Address" "${log_dir}/${peer_id}.log" 2>/dev/null | head -n 1 | awk '{print $NF}'
}
