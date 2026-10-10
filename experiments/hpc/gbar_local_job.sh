#!/bin/bash
#BSUB -J idss-local
#BSUB -q hpc
#BSUB -n 16
#BSUB -R "span[hosts=1]"
#BSUB -R "rusage[mem=12GB]"
#BSUB -W 08:00
#BSUB -o idss_%J.out
#BSUB -e idss_%J.err

export PATH=$HOME/go-sdk/go/bin:$PATH
export GOTOOLCHAIN=local
module load python3/3.11.9
export PATH=$HOME/go-sdk/go/bin:$PATH
which go; go version
cd ~/idss_graphdb/experiments
export DB_PATH_ROOT=${__LSF_JOB_TMPDIR__:-/tmp/$USER-idss-$LSB_JOBID}
mkdir -p "$DB_PATH_ROOT"
export ROUTING_TARGET_CAP=20
echo "DB_PATH_ROOT=$DB_PATH_ROOT"; df -h "$DB_PATH_ROOT"

./run_e2_ttl.sh 50 5
# ./run_e4_governance.sh 50 3
rm -rf "$DB_PATH_ROOT"
