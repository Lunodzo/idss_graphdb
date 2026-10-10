#!/bin/bash
#BSUB -J idss-e6
#BSUB -q hpc
#BSUB -n 16
#BSUB -R "span[hosts=1]"
#BSUB -R "rusage[mem=12GB]"
#BSUB -W 6:00
#BSUB -o idss_%J.out
#BSUB -e idss_%J.err
#
# E6 on G-bar: community-scoped settlement with 50 peers split into 5 energy
# communities (5 managers, 9 member peers each), 3 repeats. Each repeat
# starts a fresh cluster. Does not touch the E1-E5 results.

module load python3/3.11.9
export PATH=$HOME/go-sdk/go/bin:$PATH
export GOTOOLCHAIN=local
export ROUTING_TARGET_CAP=20
export QUERY_TIMEOUT_SECONDS=600
export IDSS_STORE_RESULTS=0
export E6_CUSTOMERS=10 E6_DAYS=1
cd ~/idss_graphdb/experiments
export DB_PATH_ROOT=${__LSF_JOB_TMPDIR__:-/tmp/$USER-idss-$LSB_JOBID}
mkdir -p "$DB_PATH_ROOT"
echo "code: $(git -C .. log -1 --oneline)"; which go; go version
echo "DB_PATH_ROOT=$DB_PATH_ROOT"; df -h "$DB_PATH_ROOT"

left=$(pgrep -u "$USER" -c idss_server || true)
echo "idss_server processes already on $(hostname): ${left}"
if [[ "${left:-0}" != "0" ]]; then echo "ABORT: stray idss_server processes"; exit 1; fi

step() { echo "== $(date -u +%FT%TZ) start: $*"; "$@"; rc=$?; echo "== $(date -u +%FT%TZ) end (${rc}): $*"; echo "stray peers: $(pgrep -u "$USER" -c idss_server || true)"; du -sh "$DB_PATH_ROOT" 2>/dev/null; }

# Rebuild the server so the peers run the community-aware code.
(cd ../server && go build -buildvcs=false -o idss_server .) || { echo "server build failed"; exit 1; }
export FORCE_CLIENT_BUILD=1
step ./run_e6_communities.sh 50 5 3

echo "idss_server processes left on $(hostname): $(pgrep -u "$USER" -c idss_server || true)"
rm -rf "$DB_PATH_ROOT"
