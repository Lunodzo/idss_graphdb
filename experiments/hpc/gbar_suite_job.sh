#!/bin/bash
#BSUB -J idss-suite
#BSUB -q hpc
#BSUB -n 16
#BSUB -R "span[hosts=1]"
#BSUB -R "rusage[mem=12GB]"
#BSUB -W 24:00
#BSUB -o idss_%J.out
#BSUB -e idss_%J.err

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

# N = 50
./run_e2_ttl.sh 50 5
./run_e3_aggregation.sh 50 10 1 15 3
./run_e5_scenario.sh 50 3
# N = 10 (small-N baseline) and E1 sweep
./run_e2_ttl.sh 10 5
./run_e4_governance.sh 10 3
./run_e3_aggregation.sh 10 10 1 15 3
./run_e5_scenario.sh 10 3
./run_e1_scale.sh 15 3

rm -rf "$DB_PATH_ROOT"
