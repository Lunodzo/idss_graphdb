#!/bin/bash
#BSUB -J idss-rerun3
#BSUB -q hpc
#BSUB -n 16
#BSUB -R "span[hosts=1]"
#BSUB -R "rusage[mem=12GB]"
#BSUB -W 12:00
#BSUB -o idss_%J.out
#BSUB -e idss_%J.err
#
# Third G-bar rerun: E4 N=50 (the earlier run predates the forwarding fix,
# 39-43/50 peers) and E1 N=2..15 (the earlier run had orphaned E5 peers
# answering, peers_responded up to N+2).

module load python3/3.11.9
export PATH=$HOME/go-sdk/go/bin:$PATH
export GOTOOLCHAIN=local
export ROUTING_TARGET_CAP=20
export QUERY_TIMEOUT_SECONDS=600
export IDSS_STORE_RESULTS=0
cd ~/idss_graphdb/experiments
export DB_PATH_ROOT=${__LSF_JOB_TMPDIR__:-/tmp/$USER-idss-$LSB_JOBID}
mkdir -p "$DB_PATH_ROOT"
echo "code: $(git -C .. log -1 --oneline)"; which go; go version
echo "DB_PATH_ROOT=$DB_PATH_ROOT"; df -h "$DB_PATH_ROOT"

left=$(pgrep -u "$USER" -c idss_server || true)
echo "idss_server processes already on $(hostname): ${left}"
if [[ "${left:-0}" != "0" ]]; then echo "ABORT: stray idss_server processes"; exit 1; fi

step() { echo "== $(date -u +%FT%TZ) start: $*"; "$@"; echo "== $(date -u +%FT%TZ) end ($?): $*"; echo "stray peers: $(pgrep -u "$USER" -c idss_server || true)"; du -sh "$DB_PATH_ROOT" 2>/dev/null; }

step ./run_e4_governance.sh 50 3
step ./run_e1_scale.sh 15 3

echo "idss_server processes left on $(hostname): $(pgrep -u "$USER" -c idss_server || true)"
rm -rf "$DB_PATH_ROOT"
