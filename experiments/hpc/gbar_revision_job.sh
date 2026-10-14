#!/bin/bash
#BSUB -J idss-rev
#BSUB -q hpc
#BSUB -n 16
#BSUB -R "span[hosts=1]"
#BSUB -R "rusage[mem=12GB]"
#BSUB -W 24:00
#BSUB -o idss_%J.out
#BSUB -e idss_%J.err
#
# Revision campaign on Testbed A (G-bar, one node, N <= 50), all on the same
# code: 30-day horizon starting 2026-09-01 (IDSS_DATA_START), daily reading
# blocks, single-row aggregates, peer list on the final result chunk.
#   E1   N=2..15, 10 repeats (now with Q6: traversal + in-network sum)
#   E2   N=10 and N=50, 5 repeats
#   E3   N=10 and N=50 (30 days); horizon sweep at N=50 (1 and 365 days)
#   E4   N=50 default vs permissive policy; E4b N=50 consent policy + audit
#   E5   N=10 and N=50
#   E6   N=50 in 5 communities (sequential and parallel settlement);
#        integrity probe with 20 % of members altering signed totals
#   E7   N=50 churn (0/10/20/30 % offline, silent and reset)
#   E8   centralised baseline equivalent to N=10 and N=50
# Set SKIP_<EXP>=1 (e.g. SKIP_E8=1) to leave a step out.

module load python3/3.11.9
export PATH=$HOME/go-sdk/go/bin:$PATH
export GOTOOLCHAIN=local
export ROUTING_TARGET_CAP=20
export QUERY_TIMEOUT_SECONDS=600
export IDSS_STORE_RESULTS=0
export RESUME_E1=0
export FORCE_CLIENT_BUILD=1
export IDSS_DATA_START=${IDSS_DATA_START:-2026-09-01T00:00:00Z}
export E5_DAYS=30 E5_CUSTOMERS=10 E6_DAYS=30 E6_CUSTOMERS=10 E4_DAYS=30
cd ~/idss_graphdb/experiments
export DB_PATH_ROOT=${__LSF_JOB_TMPDIR__:-/tmp/$USER-idss-$LSB_JOBID}
mkdir -p "$DB_PATH_ROOT"
echo "code: $(git -C .. log -1 --oneline)"; which go; go version
echo "DB_PATH_ROOT=$DB_PATH_ROOT"; df -h "$DB_PATH_ROOT"
lscpu | grep -E "Model name|^CPU\(s\)" ; free -g | head -2

left=$(pgrep -u "$USER" -c idss_server || true)
echo "idss_server processes already on $(hostname): ${left}"
if [[ "${left:-0}" != "0" ]]; then echo "ABORT: stray idss_server processes"; exit 1; fi

step() { echo "== $(date -u +%FT%TZ) start: $*"; "$@"; rc=$?; echo "== $(date -u +%FT%TZ) end (${rc}): $*"; echo "stray peers: $(pgrep -u "$USER" -c idss_server || true)"; du -sh "$DB_PATH_ROOT" 2>/dev/null; }

(cd ../server && go build -buildvcs=false -o idss_server .) || { echo "server build failed"; exit 1; }

[[ "${SKIP_E1:-0}" == "1" ]] || step ./run_e1_scale.sh 15 10
[[ "${SKIP_E2:-0}" == "1" ]] || { step ./run_e2_ttl.sh 10 5; step ./run_e2_ttl.sh 50 5; }
[[ "${SKIP_E3:-0}" == "1" ]] || { step ./run_e3_aggregation.sh 10 10 30 15 3; step ./run_e3_aggregation.sh 50 10 30 15 3;
                                 step ./run_e3_aggregation.sh 50 10 1 15 3; step ./run_e3_aggregation.sh 50 10 365 15 3; }
[[ "${SKIP_E4:-0}" == "1" ]] || { step ./run_e4_governance.sh 50 3; step ./run_e4b_consent.sh 50 3; }
[[ "${SKIP_E5:-0}" == "1" ]] || { step ./run_e5_scenario.sh 10 3; step ./run_e5_scenario.sh 50 3; }
[[ "${SKIP_E6:-0}" == "1" ]] || { step ./run_e6_communities.sh 50 5 3;
                                 ( export IDSS_TAMPER_SETTLEMENT=0.2; step ./run_e6_communities.sh 50 5 1 ); }
[[ "${SKIP_E7:-0}" == "1" ]] || step ./run_e7_churn.sh 50 3
[[ "${SKIP_E8:-0}" == "1" ]] || { step ./run_e8_central.sh 10 3; step ./run_e8_central.sh 50 3; }

echo "idss_server processes left on $(hostname): $(pgrep -u "$USER" -c idss_server || true)"
rm -rf "$DB_PATH_ROOT"
