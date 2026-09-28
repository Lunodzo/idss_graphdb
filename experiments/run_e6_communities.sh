#!/bin/bash
# E6: is settlement scoped to the energy community it belongs to?
#
# Starts (or attaches to) a cluster split into K energy communities, ec-1..ec-K
# (COMMUNITIES=K, see server/start_peers.sh): global peers 1..K are the
# managers and every other peer joins ec-((g-1) mod K + 1). Each member finds
# its manager through the DHT and registers its customers with it. Then, per
# repeat:
#   1. registration: the manager's registry must reach the expected number of
#      member peers (read from the manager logs; time to full registration);
#   2. settle (scoped): every manager settles its own community over one
#      billing day; expected: responded = registered members, refused = 0;
#   3. jurisdiction probe on the ec-1 manager, "settle-unscoped": the request
#      goes to its whole routing table; expected: only ec-1 members answer,
#      peers of other communities refuse (community mismatch);
#   4. impersonation probe on the ec-1 manager, claiming ec-2: expected: every
#      peer refuses (ec-2 members: the requester is not their manager).
#
# Writes e6-communities.csv. Usage:
#   ./experiments/run_e6_communities.sh <peer_count> <communities> [repeats]
# Needs peer_count >= 2 * communities. On an external (HPC) cluster, the
# cluster must have been launched with the same COMMUNITIES value.

set -euo pipefail

if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <peer_count> <communities> [repeats]" >&2
    exit 1
fi

PEER_COUNT=$1
COMMUNITY_COUNT=$2
REPEATS=${3:-1}
E6_CUSTOMERS=${E6_CUSTOMERS:-${E5_CUSTOMERS:-10}}
E6_DAYS=${E6_DAYS:-${E5_DAYS:-1}}
E6_INTERVAL_MINUTES=${E6_INTERVAL_MINUTES:-15}
SETTLE_TTL_SECONDS=${SETTLE_TTL_SECONDS:-20}
REGISTRATION_TIMEOUT_SECONDS=${REGISTRATION_TIMEOUT_SECONDS:-300}

if ! [[ "${PEER_COUNT}" =~ ^[0-9]+$ && "${COMMUNITY_COUNT}" =~ ^[0-9]+$ ]] || (( COMMUNITY_COUNT < 2 || PEER_COUNT < 2 * COMMUNITY_COUNT )); then
    echo "Need integers peer_count >= 2 * communities and communities >= 2" >&2
    exit 1
fi
if ! [[ "${REPEATS}" =~ ^[0-9]+$ ]] || (( REPEATS < 1 )); then
    echo "repeats must be a positive integer" >&2
    exit 1
fi
export COMMUNITIES=${COMMUNITY_COUNT}

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=experiments/lib.sh
source "${ROOT_DIR}/lib.sh"
harness_require_commands go python3 timeout

RESULT_DIR="${ROOT_DIR}/results"
harness_new_run_dir "${RESULT_DIR}" "e6-peers${PEER_COUNT}-k${COMMUNITY_COUNT}-repeats${REPEATS}"
RUN_DIR="${HARNESS_RUN_DIR}"
RUN_ID="${HARNESS_RUN_ID}"
CSV_FILE="${RUN_DIR}/e6-communities.csv"
ALL_CSV_FILE="${RESULT_DIR}/e6-all-results.csv"
LOG_DIR="${RUN_DIR}/peer-logs"
CLIENT_RESULTS_DIR="${RUN_DIR}/client-live"

mkdir -p "${LOG_DIR}" "${CLIENT_RESULTS_DIR}" "${RUN_DIR}/client-output"
CSV_HEADER="run_id,repeat,community,step,scope,expected_members,registered_members,targets,responded,refused,unreachable,elapsed_seconds,phase_local_totals_ms,phase_broadcast_collect_ms,phase_write_summaries_ms,phase_total_ms"
if [[ ! -f "${ALL_CSV_FILE}" ]]; then
    echo "${CSV_HEADER}" > "${ALL_CSV_FILE}"
fi
echo "${CSV_HEADER#run_id,}" > "${CSV_FILE}"
cat > "${RUN_DIR}/metadata.txt" <<EOF
run_id=${RUN_ID}
experiment=E6-community-scoped-settlement
started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
peer_count=${PEER_COUNT}
communities=${COMMUNITY_COUNT}
repeats=${REPEATS}
customers=${E6_CUSTOMERS}
days=${E6_DAYS}
interval_minutes=${E6_INTERVAL_MINUTES}
settle_ttl_seconds=${SETTLE_TTL_SECONDS}
external_cluster=${HARNESS_EXTERNAL_PEER_ADDRESS:+yes}
EOF

# expected_member_peers <community_index> -> member peers (manager excluded)
# assigned to ec-c by start_peers.sh's round-robin over global indices 1..N.
expected_member_peers() {
    local c=$1
    local total=0 g
    for ((g=COMMUNITY_COUNT+1; g<=PEER_COUNT; g++)); do
        if (( (g - 1) % COMMUNITY_COUNT + 1 == c )); then total=$((total + 1)); fi
    done
    echo "${total}"
}

# manager_id_for <log_dir> <community_index> -> peer ID of that community's manager.
manager_id_for() {
    local log_dir=$1 c=$2 f
    f=$(grep -l "Community manager for ec-${c}\$" "${log_dir}"/*.log 2>/dev/null | head -n 1 || true)
    [[ -n "${f}" ]] && basename "${f}" .log
}

# registered_member_peers <manager_log> -> member peers in the manager's registry.
registered_member_peers() {
    grep -o 'joined ([0-9]* member peers' "$1" 2>/dev/null | tail -n 1 | grep -o '[0-9][0-9]*' || echo 0
}

# registration_seconds <manager_log> -> seconds from the manager announcing
# itself to the last member joining (from the peer log's own timestamps).
registration_seconds() {
    python3 - "$1" <<'PY'
import re, sys
from datetime import datetime
start = last = None
for line in open(sys.argv[1], errors="replace"):
    stamp = re.match(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+)", line)
    if not stamp:
        continue
    t = datetime.fromisoformat(stamp.group(1))
    if "Community manager for" in line and start is None:
        start = t
    if "member peer" in line and "joined" in line:
        last = t
print(f"{(last - start).total_seconds():.3f}" if start and last else "")
PY
}

# settle_field <log_file> <start_line> <name> -> value of name=... on the
# last broadcast_collect line written since start_line.
settle_field() {
    tail -n "+$(( $2 + 1 ))" "$1" 2>/dev/null | grep -F "Settlement phase=broadcast_collect " | tail -n 1 \
        | sed -n "s/.* $3=\([^ ]*\).*/\1/p"
}

extract_phase_ms() {
    tail -n "+$(( $2 + 1 ))" "$1" 2>/dev/null | grep -F "Settlement phase=$3 " | tail -n 1 \
        | sed -n 's/.*duration_ms=\([0-9.]*\).*/\1/p'
}

record_row() {
    local line="$1"
    echo "${line}" >> "${CSV_FILE}"
    echo "${RUN_ID},${line}" >> "${ALL_CSV_FILE}"
}

# run_settle <repeat> <community_index> <step> <manager_id> <query> <expected> <registered>
run_settle() {
    local repeat=$1 c=$2 step=$3 manager_id=$4 query=$5 expected=$6 registered=$7
    local manager_log="${repeat_log_dir}/${manager_id}.log"
    local manager_address start_line client_output
    manager_address=$(harness_peer_address_from_log "${repeat_log_dir}" "${manager_id}")
    start_line=$(wc -l < "${manager_log}" 2>/dev/null || echo 0)
    client_output="${RUN_DIR}/client-output/${repeat}-ec${c}-${step}.log"
    harness_run_query "${CLIENT_RESULTS_DIR}" "${client_output}" "${manager_address}" manager \
        "${query}" "${SETTLE_TTL_SECONDS}"
    record_row "${repeat},ec-${c},${step},$(settle_field "${manager_log}" "${start_line}" scope),${expected},${registered},$(settle_field "${manager_log}" "${start_line}" targets),$(settle_field "${manager_log}" "${start_line}" responded),$(settle_field "${manager_log}" "${start_line}" refused),$(settle_field "${manager_log}" "${start_line}" unreachable),${HARNESS_ELAPSED},$(extract_phase_ms "${manager_log}" "${start_line}" local_totals),$(extract_phase_ms "${manager_log}" "${start_line}" broadcast_collect),$(extract_phase_ms "${manager_log}" "${start_line}" write_summaries),$(extract_phase_ms "${manager_log}" "${start_line}" total)"
}

cleanup() {
    harness_stop_cluster
}
trap cleanup EXIT

for repeat in $(seq 1 "${REPEATS}"); do
    repeat_log_dir="${LOG_DIR}/repeat-${repeat}"
    base_from=$(python3 -c "from datetime import datetime, timedelta, timezone; print((datetime.now(timezone.utc)-timedelta(minutes=10)).strftime('%Y-%m-%dT%H:%M:%SZ'))")
    period_to=$(python3 -c "from datetime import datetime, timedelta; import sys; print((datetime.strptime(sys.argv[1], '%Y-%m-%dT%H:%M:%SZ')+timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ'))" "${base_from}")

    if ! harness_start_cluster "${PEER_COUNT}" "${repeat_log_dir}" "${RUN_DIR}/launch-${repeat}.log" \
        --customers "${E6_CUSTOMERS}" --days "${E6_DAYS}" --interval-minutes "${E6_INTERVAL_MINUTES}"; then
        exit 1
    fi

    # Step 1: wait until every manager has registered its expected members.
    declare -A MANAGER_IDS=() EXPECTED=() REGISTERED=()
    for c in $(seq 1 "${COMMUNITY_COUNT}"); do
        MANAGER_IDS[$c]=$(manager_id_for "${repeat_log_dir}" "${c}" || true)
        EXPECTED[$c]=$(expected_member_peers "${c}")
        if [[ -z "${MANAGER_IDS[$c]}" ]]; then
            echo "No manager log found for ec-${c} in ${repeat_log_dir}" >&2
            exit 1
        fi
    done
    for _ in $(seq 1 "${REGISTRATION_TIMEOUT_SECONDS}"); do
        complete=1
        for c in $(seq 1 "${COMMUNITY_COUNT}"); do
            REGISTERED[$c]=$(registered_member_peers "${repeat_log_dir}/${MANAGER_IDS[$c]}.log")
            (( REGISTERED[$c] >= EXPECTED[$c] )) || complete=0
        done
        (( complete == 1 )) && break
        sleep 1
    done
    for c in $(seq 1 "${COMMUNITY_COUNT}"); do
        REGISTERED[$c]=$(registered_member_peers "${repeat_log_dir}/${MANAGER_IDS[$c]}.log")
        HARNESS_ELAPSED=$(registration_seconds "${repeat_log_dir}/${MANAGER_IDS[$c]}.log")
        record_row "${repeat},ec-${c},registration,,${EXPECTED[$c]},${REGISTERED[$c]},,,,,${HARNESS_ELAPSED},,,,"
    done

    # Step 2: every manager settles its own community (registered members).
    for c in $(seq 1 "${COMMUNITY_COUNT}"); do
        run_settle "${repeat}" "${c}" settle_scoped "${MANAGER_IDS[$c]}" "settle ${base_from} ${period_to}" "${EXPECTED[$c]}" "${REGISTERED[$c]}"
    done

    # Steps 3-4: jurisdiction and impersonation probes from the ec-1 manager.
    run_settle "${repeat}" 1 probe_routing_table "${MANAGER_IDS[1]}" "settle-unscoped ${base_from} ${period_to}" "${EXPECTED[1]}" "${REGISTERED[1]}"
    run_settle "${repeat}" 1 probe_claim_ec2 "${MANAGER_IDS[1]}" "settle-unscoped ${base_from} ${period_to} ec-2" "${EXPECTED[1]}" "${REGISTERED[1]}"

    unset MANAGER_IDS EXPECTED REGISTERED
    harness_stop_cluster
done
trap - EXIT

echo "Wrote E6 results to ${CSV_FILE}"
column -s, -t < "${CSV_FILE}" || cat "${CSV_FILE}"
