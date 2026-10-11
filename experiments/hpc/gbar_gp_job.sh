#!/bin/bash
# Green Power experiment matrix on one DTU G-bar node (LSF). All peers run on
# this node (loopback, GOMAXPROCS=1 each); 16 cores host up to 62 peers.
#   bsub < experiments/hpc/gbar_gp_job.sh            (from the repository root)
# Override the matrix with environment variables before bsub, e.g.
#   SCALES="0.01 0.1" REGIONS="1 5" bsub < experiments/hpc/gbar_gp_job.sh
#BSUB -J idss-gp
#BSUB -q hpc
#BSUB -n 16
#BSUB -R "span[hosts=1]"
#BSUB -R "rusage[mem=4GB]"
#BSUB -W 12:00
#BSUB -o idss_gp_%J.out
#BSUB -e idss_gp_%J.err

export PATH=$HOME/go-sdk/go/bin:$PATH
export GOTOOLCHAIN=local
module load python3/3.11.9
go version
cd ~/idss_graphdb

export TMPDIR=${__LSF_JOB_TMPDIR__:-/tmp/$USER-idss-gp-$LSB_JOBID}   # run directories (peer logs, results)
mkdir -p "$TMPDIR"
export DATA_ROOT=${DATA_ROOT:-$HOME/idss_gp_data}                     # generated datasets, reused across jobs
export OUT=${OUT:-$HOME/idss_graphdb/experiments/results/gp/gbar_$LSB_JOBID}
export SCALES=${SCALES:-"0.01 0.1 1"}
export REGIONS=${REGIONS:-"1 2 5 10"}
export REPEATS=${REPEATS:-10} WARMUP=${WARMUP:-2}
export TTL=${TTL:-60} SETTLE=${SETTLE:-15}
export E3_SCALE=${E3_SCALE:-0.1}
echo "OUT=$OUT DATA_ROOT=$DATA_ROOT TMPDIR=$TMPDIR"

for e in ${EXPERIMENTS:-E2 E1 E3}; do
  experiments/gp/run_matrix.sh "$e"
done
rm -rf "$TMPDIR"
