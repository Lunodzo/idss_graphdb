#!/bin/bash
# Green Power experiment matrix on one DTU Sophia node (Slurm). Same settings
# as gbar_gp_job.sh; all peers run on this node.
#   sbatch experiments/hpc/sophia_gp_job.sbatch      (from the repository root)
#SBATCH --job-name=idss-gp
#SBATCH --partition=workq
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --time=12:00:00
#SBATCH --output=idss_gp_%j.out
#SBATCH --error=idss_gp_%j.err

ml Go Python   # check exact module names with: ml avail Go; ml avail Python
export GOTOOLCHAIN=local
go version
cd "${SLURM_SUBMIT_DIR:-$HOME/idss_graphdb}"

export TMPDIR=${TMPDIR_GP:-/tmp/$USER-idss-gp-$SLURM_JOB_ID}
mkdir -p "$TMPDIR"
export DATA_ROOT=${DATA_ROOT:-$HOME/idss_gp_data}
export OUT=${OUT:-$PWD/experiments/results/gp/sophia_$SLURM_JOB_ID}
export SCALES=${SCALES:-"0.01 0.1 1"}
export REGIONS=${REGIONS:-"1 2 5 10"}
export FORWARDS=${FORWARDS:-"0 3"}
export REPEATS=${REPEATS:-10} WARMUP=${WARMUP:-2}
export TTL=${TTL:-60} SETTLE=${SETTLE:-15}
export E3_SCALE=${E3_SCALE:-0.1}
echo "OUT=$OUT DATA_ROOT=$DATA_ROOT TMPDIR=$TMPDIR"

for e in ${EXPERIMENTS:-E2 E1 E3}; do
  experiments/gp/run_matrix.sh "$e"
done
rm -rf "$TMPDIR"
