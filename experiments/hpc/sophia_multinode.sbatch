#!/bin/bash
#SBATCH --job-name=idss-hpc
#SBATCH --partition=rome
#SBATCH --nodes=50
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=32
#SBATCH --time=04:00:00
#SBATCH --output=idss-hpc-%j.out

# Stands up a 50-node x >=100-peer/node IDSS overlay on Sophia and runs the
# standard E1-E5 experiment scripts against it, unmodified, via
# experiments/hpc/run_distributed_experiments.sh.
#
# Adjust before submitting:
#   --partition   run `sinfo` / see https://dtu-sophia.github.io/docs/scheduler/
#   --nodes       node count (the "50 nodes" requirement)
#   PEERS_PER_NODE below (the "at least 100 peers per node" requirement)
#   the `ml` lines: run `ml avail Go` / `ml avail Python` and use the exact
#   module names available on your allocation.
#
# Submit with: sbatch experiments/hpc/sophia_multinode.sbatch

set -Eeuo pipefail

# --- adjust module names for your allocation (see `ml avail`) ---
export PATH=$HOME/go-sdk/go/bin:$PATH
export GOTOOLCHAIN=local
ml Python/3.11.3-GCCcore-12.3.0
set +u
command -v module >/dev/null 2>&1 || source /etc/profile >/dev/null 2>&1 || true
module load Python/3.11.3-GCCcore-12.3.0 >/dev/null 2>&1 || true
set -u
echo "[batch] python3 = $(command -v python3) ($(python3 --version 2>&1))"

ml purge

PEERS_PER_NODE=${PEERS_PER_NODE:-100}
REPEATS=${REPEATS:-3}
E_CUSTOMERS=${E_CUSTOMERS:-10}
# With few peers per node, total peer count is small, so give each peer more
# history to keep query result volumes realistic (default stays 1 day at
# high peer density; the "latency" one-peer-per-node regime needs more).
E_DAYS=${E_DAYS:-1}
E_INTERVAL_MINUTES=${E_INTERVAL_MINUTES:-15}
POLICY_FILE=${POLICY_FILE:-policy.permissive.yaml}
EXPERIMENTS=${EXPERIMENTS:-"e1 e2 e3 e4 e5"}
# Multi-node runs must use the global protocol id (see common/idss_common.go);
# the local single-machine scripts keep using IDSS_PROTOCOL_LOCAL by default.
PROTOCOL_ID=${PROTOCOL_ID:-/kad/1.0.0}
export ROUTING_TARGET_CAP=${ROUTING_TARGET_CAP:-20}

NODES=${SLURM_JOB_NUM_NODES:?not running under Slurm}
TOTAL_PEERS=$((NODES * PEERS_PER_NODE))

SCRIPT_DIR=${IDSS_HPC_DIR:-${SLURM_SUBMIT_DIR}}
ROOT_DIR=$(cd "${SCRIPT_DIR}/../.." && pwd)
RESULTS_DIR="${ROOT_DIR}/experiments/results"

# $TMPSHARE is Sophia's per-job shared (BeeGFS) scratch directory, visible
# from every allocated node and auto-cleaned at job end - see
# https://dtu-sophia.github.io/docs/scratch/. Fall back to a results-relative
# path for non-Sophia Slurm clusters without it.
COORD_ROOT=${TMPSHARE:-${RESULTS_DIR}/.hpc-scratch}
COORD_DIR="${COORD_ROOT}/coord-${SLURM_JOB_ID}"
BUNDLE_DIR="${COORD_ROOT}/bundle-${SLURM_JOB_ID}"
mkdir -p "${COORD_DIR}/ready" "${BUNDLE_DIR}"

echo "Job ${SLURM_JOB_ID}: ${NODES} nodes x ${PEERS_PER_NODE} peers = ${TOTAL_PEERS} total peers"
echo "Coordination directory: ${COORD_DIR}"

"${SCRIPT_DIR}/stage_bundle.sh" "${BUNDLE_DIR}"

export BUNDLE_DIR COORD_DIR PEERS_PER_NODE PROTOCOL_ID POLICY_FILE
export E_CUSTOMERS E_DAYS E_INTERVAL_MINUTES

echo "Launching peer swarm across ${NODES} nodes..."
srun --ntasks-per-node=1 --nodes="${NODES}" \
    bash "${SCRIPT_DIR}/launch_node_peers.sh" &
SRUN_PID=$!

READY_TIMEOUT_SECONDS=${READY_TIMEOUT_SECONDS:-600}
ready_count=0
for _ in $(seq 1 "${READY_TIMEOUT_SECONDS}"); do
    ready_count=$(find "${COORD_DIR}/ready" -name '*.ready' 2>/dev/null | wc -l)
    if (( ready_count == NODES )); then
        break
    fi
    if ! kill -0 "${SRUN_PID}" 2>/dev/null; then
        echo "srun swarm launcher exited before all nodes became ready (${ready_count}/${NODES})" >&2
        exit 1
    fi
    sleep 1
done
if (( ready_count != NODES )); then
    echo "Timed out waiting for nodes to become ready: ${ready_count}/${NODES}" >&2
    touch "${COORD_DIR}/stop"
    wait "${SRUN_PID}" || true
    exit 1
fi
echo "All ${NODES} nodes are ready (${TOTAL_PEERS} peers total)."

DRIVER_STATUS=0
COORD_DIR="${COORD_DIR}" TOTAL_PEERS="${TOTAL_PEERS}" REPEATS="${REPEATS}" \
    E3_CUSTOMERS="${E_CUSTOMERS}" E3_DAYS="${E_DAYS}" E3_INTERVAL_MINUTES="${E_INTERVAL_MINUTES}" \
    EXPERIMENTS="${EXPERIMENTS}" \
    "${SCRIPT_DIR}/run_distributed_experiments.sh" || DRIVER_STATUS=$?

# Peer/DB state lives on ephemeral node-local and $TMPSHARE storage that Slurm
# deletes at job end; copy the raw per-peer logs out before that happens.
mkdir -p "${RESULTS_DIR}/hpc-peer-logs-${SLURM_JOB_ID}"
cp -r "${COORD_DIR}/peer-logs" "${RESULTS_DIR}/hpc-peer-logs-${SLURM_JOB_ID}/" 2>/dev/null || true

echo "Signalling all nodes to stop..."
touch "${COORD_DIR}/stop"
wait "${SRUN_PID}" || true

echo "Done. Experiment CSVs are in ${RESULTS_DIR}; raw peer logs copied to ${RESULTS_DIR}/hpc-peer-logs-${SLURM_JOB_ID}."
exit "${DRIVER_STATUS}"
