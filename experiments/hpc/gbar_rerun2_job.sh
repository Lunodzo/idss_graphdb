#!/bin/bash
#BSUB -J idss-rerun2
#BSUB -q hpc
#BSUB -n 16
#BSUB -R "span[hosts=1]"
#BSUB -R "rusage[mem=12GB]"
#BSUB -W 24:00
#BSUB -o idss_%J.out
#BSUB -e idss_%J.err
#
# Second G-bar rerun (after job 29503324). Peer databases are now deleted
# when each cluster stops, and E5 uses 1 day of data like Sophia: 30 days
# per peer (~40k graph nodes, ~19 GB in EliasDB) filled the node's /tmp.

module load python3/3.11.9
export PATH=$HOME/go-sdk/go/bin:$PATH
export GOTOOLCHAIN=local
export ROUTING_TARGET_CAP=20
export QUERY_TIMEOUT_SECONDS=600
export IDSS_STORE_RESULTS=0
export E5_DAYS=1 E5_CUSTOMERS=10
cd ~/idss_graphdb/experiments
export DB_PATH_ROOT=${__LSF_JOB_TMPDIR__:-/tmp/$USER-idss-$LSB_JOBID}
mkdir -p "$DB_PATH_ROOT"
echo "code: $(git -C .. log -1 --oneline)"; which go; go version
echo "DB_PATH_ROOT=$DB_PATH_ROOT"; df -h "$DB_PATH_ROOT"
echo "idss_server processes already on $(hostname): $(pgrep -u "$USER" -c idss_server || true)"

step() { echo "== $(date -u +%FT%TZ) start: $*"; "$@"; echo "== $(date -u +%FT%TZ) end ($?): $*"; df -h "$DB_PATH_ROOT" | tail -1; du -sh "$DB_PATH_ROOT" 2>/dev/null; }

step ./run_e5_scenario.sh 10 3
step ./run_e2_ttl.sh 50 5
step ./run_e5_scenario.sh 50 3

echo "idss_server processes left on $(hostname): $(pgrep -u "$USER" -c idss_server || true)"
rm -rf "$DB_PATH_ROOT"
