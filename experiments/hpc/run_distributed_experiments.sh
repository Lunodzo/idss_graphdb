#!/bin/bash
# Runs the standard E1-E5 experiment scripts, unmodified, against the
# already-running multi-node cluster coordinated under COORD_DIR (see
# launch_node_peers.sh). Invoked by sophia_multinode.sbatch once every node
# has reported ready.
#
# Required environment:
#   COORD_DIR     shared coordination directory used by launch_node_peers.sh
#   TOTAL_PEERS   total peer count across the whole cluster (NODES * PEERS_PER_NODE)
# Optional:
#   REPEATS, E3_CUSTOMERS, E3_DAYS, E3_INTERVAL_MINUTES, EXPERIMENTS
#   (space-separated subset of "e1 e2 e3 e4 e5", default: all)

set -uo pipefail

: "${COORD_DIR:?COORD_DIR must be set}"
: "${TOTAL_PEERS:?TOTAL_PEERS must be set}"
REPEATS=${REPEATS:-3}
E3_CUSTOMERS=${E3_CUSTOMERS:-10}
E3_DAYS=${E3_DAYS:-1}
E3_INTERVAL_MINUTES=${E3_INTERVAL_MINUTES:-15}
EXPERIMENTS=${EXPERIMENTS:-"e1 e2 e3 e4 e5"}

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
BOOTSTRAP_FILE="${COORD_DIR}/bootstrap_addr.txt"

if [[ ! -s "${BOOTSTRAP_FILE}" ]]; then
    echo "No bootstrap address found at ${BOOTSTRAP_FILE}; is the cluster up?" >&2
    exit 1
fi

# Aggregate per-node peer ID lists and flatten per-node log directories into
# one directory keyed by peer ID (globally unique), since run_e*.sh grep
# "${log_dir}"/*.log non-recursively.
cat "${COORD_DIR}"/peer-ids/node-*.txt > "${COORD_DIR}/all_peer_ids.txt" 2>/dev/null || true
FLAT_LOG_DIR="${COORD_DIR}/peer-logs-flat"
mkdir -p "${FLAT_LOG_DIR}"
find "${COORD_DIR}/peer-logs" -mindepth 2 -maxdepth 2 -name '*.log' \
    ! -name 'launcher-*.log' ! -name 'peer_tmp_*.log' -print0 2>/dev/null \
    | xargs -0 -I{} ln -sf {} "${FLAT_LOG_DIR}/"

export HARNESS_EXTERNAL_PEER_ADDRESS
export HARNESS_EXTERNAL_PEER_IDS_FILE="${COORD_DIR}/all_peer_ids.txt"
export HARNESS_EXTERNAL_LOG_DIR="${FLAT_LOG_DIR}"
HARNESS_EXTERNAL_PEER_ADDRESS=$(cat "${BOOTSTRAP_FILE}")

echo "Distributed cluster: ${TOTAL_PEERS} peers, bootstrap ${HARNESS_EXTERNAL_PEER_ADDRESS}"

cd "${ROOT_DIR}"
FAILED=()

for exp in ${EXPERIMENTS}; do
    case "${exp}" in
        e1)
            # A fixed peer count sweep: START_PEERS=MAX_PEERS makes run_e1_scale.sh
            # attach to the external cluster exactly once, appending a single
            # large-scale data point to experiments/results/e1-all-results.csv
            # (small-scale sweep points continue to come from local runs).
            START_PEERS="${TOTAL_PEERS}" ./run_e1_scale.sh "${TOTAL_PEERS}" "${REPEATS}" || FAILED+=("e1")
            ;;
        e2)
            ./run_e2_ttl.sh "${TOTAL_PEERS}" "${REPEATS}" || FAILED+=("e2")
            ;;
        e3)
            ./run_e3_aggregation.sh "${TOTAL_PEERS}" "${E3_CUSTOMERS}" "${E3_DAYS}" "${E3_INTERVAL_MINUTES}" "${REPEATS}" || FAILED+=("e3")
            ;;
        e4)
            ./run_e4_governance.sh "${TOTAL_PEERS}" "${REPEATS}" || FAILED+=("e4")
            ;;
        e5)
            ./run_e5_scenario.sh "${TOTAL_PEERS}" "${REPEATS}" || FAILED+=("e5")
            ;;
        *)
            echo "Unknown experiment '${exp}' in EXPERIMENTS, skipping" >&2
            ;;
    esac
done

if (( ${#FAILED[@]} > 0 )); then
    echo "Experiments failed: ${FAILED[*]}" >&2
    exit 1
fi

echo "All requested distributed experiments completed: ${EXPERIMENTS}"
