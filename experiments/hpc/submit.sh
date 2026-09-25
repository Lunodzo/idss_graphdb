#!/bin/bash
# Convenience wrapper for the two node-sizing regimes (see README.md):
#   scale    many peers per node, reaches a large total N (E1's large-scale point)
#   latency  one peer per node, every hop is a real cross-host round-trip,
#            uncontaminated by CPU contention between co-located peers
#
# Usage: ./submit.sh scale|latency [sbatch args...]
# Extra sbatch args (e.g. --nodes=20) are passed through. Override any of
# PEERS_PER_NODE/REPEATS/E_CUSTOMERS/E_DAYS/E_INTERVAL_MINUTES/EXPERIMENTS by
# exporting them before calling this script.

set -Eeuo pipefail

if [ $# -lt 1 ]; then
  echo "Usage: $0 scale|latency [sbatch args...]" >&2
  exit 1
fi

MODE=$1
shift
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

case "${MODE}" in
  scale)
    PEERS_PER_NODE=${PEERS_PER_NODE:-100}
    EXPERIMENTS=${EXPERIMENTS:-"e1 e2 e3 e4 e5"}
    ;;
  latency)
    # E_DAYS is left to sophia_multinode.sbatch's own default (30 days at
    # this peer density) unless already exported by the caller.
    PEERS_PER_NODE=1
    EXPERIMENTS=${EXPERIMENTS:-"e2 e4"}
    ;;
  *)
    echo "Unknown mode '${MODE}', expected 'scale' or 'latency'" >&2
    exit 1
    ;;
esac

sbatch --export="ALL,PEERS_PER_NODE=${PEERS_PER_NODE},EXPERIMENTS=${EXPERIMENTS}" \
  "$@" "${SCRIPT_DIR}/sophia_multinode.sbatch"
