#!/bin/bash
# E7: how do queries behave when part of the community is offline?
#
# Peers are taken "offline" through the churn file read by every peer
# (helpers/wan.go): a deterministic subset holding the given fraction of
# peers stops answering IDSS streams. In "silent" mode (default) an offline
# peer never answers, so its parent waits for its deadline, as with an
# unreachable household; in "reset" mode it refuses at once. The entry peer
# (the manager the requester connects to) is never taken offline.
#
# For each fraction the script runs a raw query (Q3), an in-network aggregate
# (Q4) and a discovery query (Q2) and records how many peers answered and
# how many rows arrived, next to the number of peers that were online. IDSS
# keeps data at its owner and does not replicate it, so the data of offline
# peers is unavailable by design; the measurement shows that the rest of the
# community is still reached, and at what time cost.
#
# Usage: ./experiments/run_e7_churn.sh <peer_count> [repeats]
#   E7_FRACTIONS (default "0 0.1 0.2 0.3"), E7_TTL (default 5), E7_MODES
#   (default "silent reset"), E7_CUSTOMERS, E7_DAYS. IDSS_CHURN_FILE must be the file the
#   peers were started with on a multi-node cluster; locally it is created here.

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <peer_count> [repeats]" >&2
    exit 1
fi
PEER_COUNT=$1
REPEATS=${2:-3}
E7_FRACTIONS=${E7_FRACTIONS:-"0 0.1 0.2 0.3"}
E7_TTL=${E7_TTL:-5}
E7_MODES=${E7_MODES:-silent reset}
E7_CUSTOMERS=${E7_CUSTOMERS:-10}
E7_DAYS=${E7_DAYS:-30}

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=experiments/lib.sh
source "${ROOT_DIR}/lib.sh"
harness_require_commands go python3 timeout

RESULT_DIR="${ROOT_DIR}/results"
harness_new_run_dir "${RESULT_DIR}" "e7-peers${PEER_COUNT}-repeats${REPEATS}"
RUN_DIR="${HARNESS_RUN_DIR}"
RUN_ID="${HARNESS_RUN_ID}"
CSV_FILE="${RUN_DIR}/e7-churn.csv"
ALL_CSV_FILE="${RESULT_DIR}/e7-all-results.csv"
CLIENT_RESULTS_DIR="${RUN_DIR}/client-live"
mkdir -p "${CLIENT_RESULTS_DIR}" "${RUN_DIR}/client-output"

# Local clusters inherit the churn file from this environment; a multi-node
# launcher must have exported the same path to every node before start-up.
export IDSS_CHURN_FILE=${IDSS_CHURN_FILE:-${RUN_DIR}/churn.txt}
echo "0 none silent" > "${IDSS_CHURN_FILE}"

CSV_HEADER="run_id,repeat,fraction,mode,query_label,ttl,online_peers,elapsed_seconds,peers_responded,rows_returned,wire_bytes"
[[ -f "${ALL_CSV_FILE}" ]] || echo "${CSV_HEADER}" > "${ALL_CSV_FILE}"
echo "${CSV_HEADER#run_id,}" > "${CSV_FILE}"
cat > "${RUN_DIR}/metadata.txt" <<EOF
run_id=${RUN_ID}
experiment=E7-churn
started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
peer_count=${PEER_COUNT}
repeats=${REPEATS}
fractions=${E7_FRACTIONS}
ttl_seconds=${E7_TTL}
modes=${E7_MODES}
customers=${E7_CUSTOMERS}
days=${E7_DAYS}
churn_file=${IDSS_CHURN_FILE}
EOF

QUERIES=(
    "q2_open_offers|get Offer where status = \"open\""
    "q3_meter_readings_raw|get MeterReading where readingType = \"activePower\""
    "q4_active_power_sum|get MeterReading where readingType = \"activePower\" show @sum(value)"
)

# online_peers <seed> <fraction> <exempt_peer_id> <peer-id file>
# Same selection as helpers.ChurnOffline: FNV-1a over the sorted pair
# (peer ID, "churn:<seed>"), top 53 bits as a fraction.
online_peers() {
    python3 - "$1" "$2" "$3" "$4" <<'PY'
import sys
seed, fraction, exempt = sys.argv[1], float(sys.argv[2]), sys.argv[3]
def fnv64a(data):
    h = 0xcbf29ce484222325
    for b in data:
        h ^= b
        h = (h * 0x100000001b3) & 0xFFFFFFFFFFFFFFFF
    return h
online = 0
for line in open(sys.argv[4]):
    pid = line.strip()
    if not pid:
        continue
    a, b = sorted([pid, "churn:" + seed])
    f = (fnv64a(a.encode() + b"\0" + b.encode()) >> 11) / float(1 << 53)
    if pid == exempt or fraction <= 0 or f >= fraction:
        online += 1
print(online)
PY
}

cleanup() {
    echo "0 none silent" > "${IDSS_CHURN_FILE}" 2>/dev/null || true
    harness_stop_cluster
}
trap cleanup EXIT

if ! harness_start_cluster "${PEER_COUNT}" "${RUN_DIR}/peer-logs" "${RUN_DIR}/launch.log" \
    --customers "${E7_CUSTOMERS}" --days "${E7_DAYS}" --interval-minutes 15 -policy "${E7_POLICY:-policy.permissive.yaml}"; then
    exit 1
fi
ENTRY_ID=${HARNESS_PEER_ADDRESS##*/p2p/}
PEER_ID_FILE="${RUN_DIR}/peer_ids.txt"
printf '%s\n' "${HARNESS_PEER_IDS[@]}" > "${PEER_ID_FILE}"

for repeat in $(seq 1 "${REPEATS}"); do
    for mode in ${E7_MODES}; do
        for fraction in ${E7_FRACTIONS}; do
            seed="r${repeat}"
            echo "${fraction} ${seed} ${mode}" > "${IDSS_CHURN_FILE}"
            sleep 3   # peers re-read the churn file at most every 2 s
            online=$(online_peers "${seed}" "${fraction}" "${ENTRY_ID}" "${PEER_ID_FILE}")
            for query_spec in "${QUERIES[@]}"; do
                label=${query_spec%%|*}
                query=${query_spec#*|}
                client_output="${RUN_DIR}/client-output/${repeat}-${mode}-${fraction}-${label}.log"
                harness_run_query "${CLIENT_RESULTS_DIR}" "${client_output}" "${HARNESS_PEER_ADDRESS}" manager "${query}" "${E7_TTL}" || true
                row="${repeat},${fraction},${mode},${label},${E7_TTL},${online},${HARNESS_ELAPSED},${HARNESS_RESPONDERS},${HARNESS_ROWS},${HARNESS_WIRE_BYTES:-}"
                echo "${row}" >> "${CSV_FILE}"
                echo "${RUN_ID},${row}" >> "${ALL_CSV_FILE}"
            done
        done
    done
done
echo "0 none silent" > "${IDSS_CHURN_FILE}"
trap - EXIT
harness_stop_cluster

echo "Wrote E7 results to ${CSV_FILE}"
cat "${CSV_FILE}"
