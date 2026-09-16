#!/bin/bash
# E5: does the platform support the community use case end to end?
#
# Runs the exemplary scenario against a live cluster with one manager peer:
#   1. a seller member writes an Offer locally
#   2. a buyer member writes a Bid locally
#   3. a distributed query discovers the open Offer (Q2, the discovery step)
#   4. the match is recorded as a Trade at both counterparts (local writes)
#      and the originating orders are marked matched (local writes)
#   5. the manager compiles settlement over billing periods of one day, one
#      week, and one month on the same dataset, so cost is characterised as a
#      function of period length
#   6. a DSO observer retrieves the aggregate settlement figure it is
#      permitted to see, via the same @sum aggregate path used for Q4, so no
#      raw metering record leaves the peer that owns it
#
# Per-step elapsed time, the settle phase breakdown (see the
# "Settlement phase=" log lines emitted by broadcast.CompileSettlement), and
# recorded policy decisions are written to e5-scenario.csv.
#
# Usage: ./experiments/run_e5_scenario.sh <peer_count> [repeats]
# peer_count must be at least 3: one manager, one seller member, one buyer
# member. Extra peers only add overlay background traffic.

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <peer_count> [repeats]" >&2
    exit 1
fi

PEER_COUNT=$1
REPEATS=${2:-1}
E5_CUSTOMERS=${E5_CUSTOMERS:-5}
E5_DAYS=${E5_DAYS:-30}
E5_INTERVAL_MINUTES=${E5_INTERVAL_MINUTES:-15}
DISCOVERY_TTL_SECONDS=${DISCOVERY_TTL_SECONDS:-10}
SETTLE_TTL_SECONDS=${SETTLE_TTL_SECONDS:-20}

if ! [[ "${PEER_COUNT}" =~ ^[0-9]+$ ]] || (( PEER_COUNT < 3 )); then
    echo "peer_count must be an integer >= 3 (manager, seller, buyer)" >&2
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
harness_new_run_dir "${RESULT_DIR}" "e5-peers${PEER_COUNT}-repeats${REPEATS}"
RUN_DIR="${HARNESS_RUN_DIR}"
RUN_ID="${HARNESS_RUN_ID}"
CSV_FILE="${RUN_DIR}/e5-scenario.csv"
ALL_CSV_FILE="${RESULT_DIR}/e5-all-results.csv"
LOG_DIR="${RUN_DIR}/peer-logs"
CLIENT_RESULTS_DIR="${RUN_DIR}/client-live"

mkdir -p "${LOG_DIR}" "${CLIENT_RESULTS_DIR}" "${RUN_DIR}/client-output"
CSV_HEADER="run_id,repeat,step,period_label,role,elapsed_seconds,peers_responded,rows_returned,result,decision,phase_local_totals_ms,phase_broadcast_collect_ms,phase_write_summaries_ms,phase_total_ms"
if [[ ! -f "${ALL_CSV_FILE}" ]]; then
    echo "${CSV_HEADER}" > "${ALL_CSV_FILE}"
fi
echo "${CSV_HEADER#run_id,}" > "${CSV_FILE}"
cat > "${RUN_DIR}/metadata.txt" <<EOF
run_id=${RUN_ID}
experiment=E5-community-scenario
started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
peer_count=${PEER_COUNT}
repeats=${REPEATS}
customers=${E5_CUSTOMERS}
days=${E5_DAYS}
interval_minutes=${E5_INTERVAL_MINUTES}
discovery_ttl_seconds=${DISCOVERY_TTL_SECONDS}
settle_ttl_seconds=${SETTLE_TTL_SECONDS}
EOF

# customer_key_for_peer <peer_id> -> deterministic mRID of that peer's first
# generated customer, matching server/generate_data.py's own seeding
# (peer_prefix = sha256(peer_id)[:8], customer index 001).
customer_key_for_peer() {
    local peer_id=$1
    local prefix
    prefix=$(python3 -c "import hashlib,sys; print(hashlib.sha256(sys.argv[1].encode()).hexdigest()[:8])" "${peer_id}")
    echo "customer-${prefix}-001"
}

# add_days_utc <RFC3339 timestamp> <days> -> RFC3339 timestamp <days> later.
add_days_utc() {
    local timestamp=$1
    local days=$2
    python3 -c "
from datetime import datetime, timedelta, timezone
import sys
start = datetime.strptime(sys.argv[1], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
print((start + timedelta(days=float(sys.argv[2]))).strftime('%Y-%m-%dT%H:%M:%SZ'))
" "${timestamp}" "${days}"
}

# extract_phase_ms <log_file> <start_line> <phase_name>
# Reads only the lines appended to the manager's log since start_line, so
# repeated settle calls in the same run do not pick up a stale duration.
extract_phase_ms() {
    local log_file=$1
    local start_line=$2
    local phase=$3
    tail -n "+$((start_line + 1))" "${log_file}" 2>/dev/null \
        | grep -F "Settlement phase=${phase} " | tail -n 1 \
        | sed -n 's/.*duration_ms=\([0-9.]*\).*/\1/p'
}

record_row() {
    local repeat=$1 step=$2 period_label=$3 role=$4 result=$5 decision=$6
    local phase_local=${7:-} phase_broadcast=${8:-} phase_write=${9:-} phase_total=${10:-}
    echo "${repeat},${step},${period_label},${role},${HARNESS_ELAPSED},${HARNESS_RESPONDERS},${HARNESS_ROWS},${result},${decision},${phase_local},${phase_broadcast},${phase_write},${phase_total}" >> "${CSV_FILE}"
    echo "${RUN_ID},${repeat},${step},${period_label},${role},${HARNESS_ELAPSED},${HARNESS_RESPONDERS},${HARNESS_ROWS},${result},${decision},${phase_local},${phase_broadcast},${phase_write},${phase_total}" >> "${ALL_CSV_FILE}"
}

cleanup() {
    harness_stop_cluster
}
trap cleanup EXIT

for repeat in $(seq 1 "${REPEATS}"); do
    repeat_log_dir="${LOG_DIR}/repeat-${repeat}"
    base_from=$(python3 -c "from datetime import datetime, timedelta, timezone; print((datetime.now(timezone.utc)-timedelta(minutes=10)).strftime('%Y-%m-%dT%H:%M:%SZ'))")

    if ! harness_start_cluster "${PEER_COUNT}" "${repeat_log_dir}" "${RUN_DIR}/launch-${repeat}.log" \
        --customers "${E5_CUSTOMERS}" --days "${E5_DAYS}" --interval-minutes "${E5_INTERVAL_MINUTES}"; then
        exit 1
    fi

    manager_id=${HARNESS_PEER_IDS[0]}
    seller_id=${HARNESS_PEER_IDS[1]}
    buyer_id=${HARNESS_PEER_IDS[2]}
    manager_address=$(harness_peer_address_from_log "${repeat_log_dir}" "${manager_id}")
    seller_address=$(harness_peer_address_from_log "${repeat_log_dir}" "${seller_id}")
    buyer_address=$(harness_peer_address_from_log "${repeat_log_dir}" "${buyer_id}")
    manager_log_file="${repeat_log_dir}/${manager_id}.log"
    seller_customer=$(customer_key_for_peer "${seller_id}")
    buyer_customer=$(customer_key_for_peer "${buyer_id}")

    run_tag="${RUN_ID}-${repeat}"
    offer_key="offer-e5-${run_tag}"
    bid_key="bid-e5-${run_tag}"
    trade_seller_key="trade-e5-${run_tag}-seller"
    trade_buyer_key="trade-e5-${run_tag}-buyer"
    now_ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)

    # Step 1-2: bids and offers are written locally (add is always local-only).
    client_output="${RUN_DIR}/client-output/${repeat}-add-offer.log"
    harness_run_query "${CLIENT_RESULTS_DIR}" "${client_output}" "${seller_address}" member \
        "add Offer ${offer_key} quantity=3.5 price=0.22 validFrom=${now_ts} validTo=${now_ts} status=open" 1
    record_row "${repeat}" "add_offer" "" member "${HARNESS_STATUS_LINE:-${HARNESS_ROWS} rows}" ""

    client_output="${RUN_DIR}/client-output/${repeat}-add-bid.log"
    harness_run_query "${CLIENT_RESULTS_DIR}" "${client_output}" "${buyer_address}" member \
        "add Bid ${bid_key} quantity=3.5 priceLimit=0.25 validFrom=${now_ts} validTo=${now_ts} status=open" 1
    record_row "${repeat}" "add_bid" "" member "${HARNESS_STATUS_LINE:-${HARNESS_ROWS} rows}" ""

    # Step 3: discovered by distributed query (Q2).
    client_output="${RUN_DIR}/client-output/${repeat}-discover.log"
    harness_run_query "${CLIENT_RESULTS_DIR}" "${client_output}" "${buyer_address}" member \
        'get Offer where status = "open"' "${DISCOVERY_TTL_SECONDS}"
    client_peer_id=$(harness_extract_client_peer_id "${client_output}")
    decision=$(harness_decision_for_client "${repeat_log_dir}" "${client_peer_id}")
    record_row "${repeat}" "discover_offers" "" member "${HARNESS_ROWS} rows" "${decision:-unknown}"

    # Step 4: matched, and recorded as a Trade at both counterparts (local writes).
    client_output="${RUN_DIR}/client-output/${repeat}-trade-seller.log"
    harness_run_query "${CLIENT_RESULTS_DIR}" "${client_output}" "${seller_address}" member \
        "add Trade ${trade_seller_key} volume=3.5 price=0.22 timeStamp=${now_ts} counterparty=${buyer_customer} settlementRef=SET-E5-${run_tag}" 1
    record_row "${repeat}" "add_trade_seller" "" member "${HARNESS_STATUS_LINE:-${HARNESS_ROWS} rows}" ""

    client_output="${RUN_DIR}/client-output/${repeat}-trade-buyer.log"
    harness_run_query "${CLIENT_RESULTS_DIR}" "${client_output}" "${buyer_address}" member \
        "add Trade ${trade_buyer_key} volume=3.5 price=0.22 timeStamp=${now_ts} counterparty=${seller_customer} settlementRef=SET-E5-${run_tag}" 1
    record_row "${repeat}" "add_trade_buyer" "" member "${HARNESS_STATUS_LINE:-${HARNESS_ROWS} rows}" ""

    client_output="${RUN_DIR}/client-output/${repeat}-update-offer.log"
    harness_run_query "${CLIENT_RESULTS_DIR}" "${client_output}" "${seller_address}" member \
        "update Offer ${offer_key} status=matched" 1
    record_row "${repeat}" "update_offer_matched" "" member "${HARNESS_STATUS_LINE:-${HARNESS_ROWS} rows}" ""

    client_output="${RUN_DIR}/client-output/${repeat}-update-bid.log"
    harness_run_query "${CLIENT_RESULTS_DIR}" "${client_output}" "${buyer_address}" member \
        "update Bid ${bid_key} status=matched" 1
    record_row "${repeat}" "update_bid_matched" "" member "${HARNESS_STATUS_LINE:-${HARNESS_ROWS} rows}" ""

    # Step 5: the manager compiles settlement over billing periods of a day, a
    # week, and a month on the same dataset, so cost is a function of period
    # length rather than a single point.
    for period_spec in "1_day|1" "1_week|7" "1_month|30"; do
        period_label=${period_spec%%|*}
        period_days=${period_spec#*|}
        period_to=$(add_days_utc "${base_from}" "${period_days}")

        start_line=$(wc -l < "${manager_log_file}" 2>/dev/null || echo 0)
        client_output="${RUN_DIR}/client-output/${repeat}-settle-${period_label}.log"
        harness_run_query "${CLIENT_RESULTS_DIR}" "${client_output}" "${manager_address}" manager \
            "settle ${base_from} ${period_to}" "${SETTLE_TTL_SECONDS}"

        phase_local=$(extract_phase_ms "${manager_log_file}" "${start_line}" "local_totals")
        phase_broadcast=$(extract_phase_ms "${manager_log_file}" "${start_line}" "broadcast_collect")
        phase_write=$(extract_phase_ms "${manager_log_file}" "${start_line}" "write_summaries")
        phase_total=$(extract_phase_ms "${manager_log_file}" "${start_line}" "total")
        record_row "${repeat}" "settle" "${period_label}" manager "${HARNESS_STATUS_LINE:-${HARNESS_ROWS} rows}" "" \
            "${phase_local}" "${phase_broadcast}" "${phase_write}" "${phase_total}"
    done

    # Step 6: a DSO observer retrieves the aggregate it is permitted to see.
    # SettlementSummary falls under the observer wildcard aggregate rule in
    # server/policy.default.yaml, and an @sum query already returns only the
    # aggregated value regardless of decision, so no raw metering record
    # leaves the manager peer.
    client_output="${RUN_DIR}/client-output/${repeat}-dso-aggregate.log"
    harness_run_query "${CLIENT_RESULTS_DIR}" "${client_output}" "${manager_address}" observer \
        "get SettlementSummary show @sum(meterReadingSum)" "${DISCOVERY_TTL_SECONDS}"
    client_peer_id=$(harness_extract_client_peer_id "${client_output}")
    decision=$(harness_decision_for_client "${repeat_log_dir}" "${client_peer_id}")
    record_row "${repeat}" "dso_aggregate" "" observer "${HARNESS_ROWS} rows" "${decision:-unknown}"

    harness_stop_cluster
done
trap - EXIT

echo "Wrote E5 results to ${CSV_FILE}"
echo "Phase-breakdown summary (mean duration_ms by period_label):"
python3 "${ROOT_DIR}/summarize_csv.py" "${CSV_FILE}" --group period_label --value phase_total_ms
