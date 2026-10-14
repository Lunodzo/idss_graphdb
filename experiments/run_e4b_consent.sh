#!/bin/bash
# E4b: consent conditions in the access policy (purpose limitation,
# time-bounded consent, withdrawal at run time) and the audit trail.
#
# Every peer runs server/policy.consent.yaml (copied into the run directory)
# and writes its decisions to an append-only audit log (IDSS_AUDIT_LOG).
#   1. Decision matrix: Q1 (customers), Q3 (raw readings) and Q4 (in-network
#      sum) for every requester role and declared purpose, checked against the
#      expected decision; the decision is read from the peers' audit logs.
#   2. Withdrawal: the members withdraw the manager's settlement consent by
#      editing their policy file (valid_until moved to the past); the script
#      measures how long until every peer denies the manager.
#   3. Audit check: every query in steps 1-2 must appear in every answering
#      peer's audit log with the policy hash in force at the time.
#
# Usage: ./experiments/run_e4b_consent.sh <peer_count> [repeats]

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <peer_count> [repeats]" >&2
    exit 1
fi
PEER_COUNT=$1
REPEATS=${2:-3}
TTL_SECONDS=${TTL_SECONDS:-15}
E4_CUSTOMERS=${E4_CUSTOMERS:-10}
E4_DAYS=${E4_DAYS:-30}

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=experiments/lib.sh
source "${ROOT_DIR}/lib.sh"
harness_require_commands go python3 timeout

RESULT_DIR="${ROOT_DIR}/results"
harness_new_run_dir "${RESULT_DIR}" "e4b-peers${PEER_COUNT}-repeats${REPEATS}"
RUN_DIR="${HARNESS_RUN_DIR}"
RUN_ID="${HARNESS_RUN_ID}"
CSV_FILE="${RUN_DIR}/e4b-consent.csv"
CLIENT_RESULTS_DIR="${RUN_DIR}/client-live"
mkdir -p "${CLIENT_RESULTS_DIR}" "${RUN_DIR}/client-output"

# The peers read this copy (so the withdrawal step can edit it) and append
# their decisions to IDSS_AUDIT_LOG. On an already-running cluster set
# E4B_POLICY_FILE and IDSS_AUDIT_LOG to the files its peers were started with.
POLICY_COPY=${E4B_POLICY_FILE:-${RUN_DIR}/policy.consent.yaml}
[[ -n "${E4B_POLICY_FILE:-}" ]] || cp "${HARNESS_SERVER_DIR}/policy.consent.yaml" "${POLICY_COPY}"
export IDSS_AUDIT_LOG=${IDSS_AUDIT_LOG:-${RUN_DIR}/audit.jsonl}
touch "${IDSS_AUDIT_LOG}"

echo "step,repeat,role,purpose,query_label,expected,decision,peers_responded,rows_returned,elapsed_seconds,client_peer" > "${CSV_FILE}"
cat > "${RUN_DIR}/metadata.txt" <<EOF
run_id=${RUN_ID}
experiment=E4b-consent
started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
peer_count=${PEER_COUNT}
repeats=${REPEATS}
ttl_seconds=${TTL_SECONDS}
policy=${POLICY_COPY}
EOF

QUERIES=(
    "q1_customers|get Customer"
    "q3_meter_readings_raw|get MeterReading where readingType = \"activePower\""
    "q4_active_power_sum|get MeterReading where readingType = \"activePower\" show @sum(value)"
)
# role|purpose|expected decision for Q1|for Q3 and Q4 (see policy.consent.yaml)
CASES=(
    "manager|settlement|allow|aggregate"
    "manager|marketing|allow|deny"
    "manager|research|allow|deny"
    "manager||allow|deny"
    "observer|grid-planning|allow|aggregate"
    "observer|settlement|allow|deny"
    "member|settlement|allow|deny"
)

# audited_decision <client_peer_id> -> decisions recorded for that client, "a/b" if peers disagree.
audited_decision() {
    python3 - "${IDSS_AUDIT_LOG}" "$1" <<'PY'
import json, sys
seen = set()
for line in open(sys.argv[1]):
    try:
        e = json.loads(line)
    except ValueError:
        continue
    if e.get("requester") == sys.argv[2]:
        seen.add(e["decision"])
print("/".join(sorted(seen)) or "none")
PY
}

cleanup() {
    harness_stop_cluster
}
trap cleanup EXIT

if ! harness_start_cluster "${PEER_COUNT}" "${RUN_DIR}/peer-logs" "${RUN_DIR}/launch.log" \
    --customers "${E4_CUSTOMERS}" --days "${E4_DAYS}" --interval-minutes 15 -policy "${POLICY_COPY}"; then
    exit 1
fi

run_case() {
    local step=$1 repeat=$2 role=$3 purpose=$4 label=$5 query=$6 expected=$7
    local out="${RUN_DIR}/client-output/${step}-${repeat}-${role}-${purpose:-none}-${label}.log"
    HARNESS_CLIENT_PURPOSE="${purpose}" harness_run_query "${CLIENT_RESULTS_DIR}" "${out}" "${HARNESS_PEER_ADDRESS}" \
        "${role}" "${query}" "${TTL_SECONDS}"
    local client decision
    client=$(harness_extract_client_peer_id "${out}") || true
    decision=$(audited_decision "${client}")
    echo "${step},${repeat},${role},${purpose:-none},${label},${expected},${decision},${HARNESS_RESPONDERS},${HARNESS_ROWS},${HARNESS_ELAPSED},${client}" >> "${CSV_FILE}"
}

# 1. Decision matrix.
for repeat in $(seq 1 "${REPEATS}"); do
    for case_spec in "${CASES[@]}"; do
        IFS='|' read -r role purpose expect_q1 expect_readings <<< "${case_spec}"
        for query_spec in "${QUERIES[@]}"; do
            label=${query_spec%%|*}
            query=${query_spec#*|}
            expected=${expect_readings}
            [[ "${label}" == q1_customers ]] && expected=${expect_q1}
            run_case matrix "${repeat}" "${role}" "${purpose}" "${label}" "${query}" "${expected}"
        done
    done
done

# 2. Withdrawal: settlement consent for the manager ends now.
withdraw_start=$(harness_monotonic_seconds)
python3 - "${POLICY_COPY}" <<'PY'
import sys
path = sys.argv[1]
text = open(path).read()
text = text.replace("version: consent-2026-10", "version: consent-2026-10-withdrawn")
text = text.replace("    valid_until: 2027-12-31", "    valid_until: 2026-01-01   # withdrawn")
open(path, "w").write(text)
PY
withdrawn_after=""
for attempt in $(seq 1 30); do
    run_case withdrawal "${attempt}" manager settlement q4_active_power_sum \
        'get MeterReading where readingType = "activePower" show @sum(value)' deny
    if [[ "$(tail -n 1 "${CSV_FILE}" | cut -d, -f7)" == "deny" ]]; then
        withdrawn_after=$(awk -v a="${withdraw_start}" -v b="$(harness_monotonic_seconds)" 'BEGIN { printf "%.2f", b - a }')
        break
    fi
    sleep 0.5
done
echo "withdrawal_effective_seconds=${withdrawn_after}" >> "${RUN_DIR}/metadata.txt"

trap - EXIT
harness_stop_cluster

# 3. Summary: matrix correctness and audit completeness.
python3 - "${CSV_FILE}" "${IDSS_AUDIT_LOG}" "${PEER_COUNT}" <<'PY' | tee "${RUN_DIR}/summary.txt"
import csv, json, sys
rows = list(csv.DictReader(open(sys.argv[1])))
audit = [json.loads(l) for l in open(sys.argv[2]) if l.strip()]
n = int(sys.argv[3])
matrix = [r for r in rows if r["step"] == "matrix"]
wrong = [r for r in matrix if r["decision"] != r["expected"]]
print(f"matrix queries: {len(matrix)}, decisions differing from the policy: {len(wrong)}")
for r in wrong:
    print("  MISMATCH", r["role"], r["purpose"], r["query_label"], "expected", r["expected"], "got", r["decision"])
by_client = {}
for e in audit:
    by_client.setdefault(e["requester"], set()).add(e["peer"])
# The entry peer evaluates a query only if the client is not itself, so every
# peer that received the query writes one entry.
full = sum(1 for r in rows if len(by_client.get(r["client_peer"], ())) >= n)
print(f"queries audited by all {n} peers: {full}/{len(rows)}")
hashes = sorted({(e.get('policy_version', ''), e['policy_hash']) for e in audit})
print("policy versions in the audit log:", hashes)
PY
echo "Wrote E4b results to ${CSV_FILE}"
