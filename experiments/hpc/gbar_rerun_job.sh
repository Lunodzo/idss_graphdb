#!/bin/bash
#BSUB -J idss-rerun
#BSUB -q hpc
#BSUB -n 16
#BSUB -R "span[hosts=1]"
#BSUB -R "rusage[mem=12GB]"
#BSUB -W 24:00
#BSUB -o idss_%J.out
#BSUB -e idss_%J.err
#
# Reruns after the suite job 29499374: E2 on the duplicate-reply fix
# (661aacb), and E5, whose 30 days of seed data per peer need a longer
# launch timeout than the 600 s default. E5 N=10 runs first as a quick check.

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
echo "idss_server processes already on $(hostname): $(pgrep -u "$USER" -c idss_server || echo 0)"

LAUNCH_TIMEOUT_SECONDS=3600 ./run_e5_scenario.sh 10 3
./run_e2_ttl.sh 10 5
./run_e2_ttl.sh 50 5
LAUNCH_TIMEOUT_SECONDS=3600 ./run_e5_scenario.sh 50 3

echo "idss_server processes left on $(hostname): $(pgrep -u "$USER" -c idss_server || echo 0)"
rm -rf "$DB_PATH_ROOT"
