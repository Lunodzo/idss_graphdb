#!/bin/bash
# Runs on ONE compute node (invoked once per node by `srun` from
# sophia_multinode.sbatch). Stages the prebuilt bundle to node-local storage,
# starts this node's share of peers, coordinates a single bootstrap address
# across the whole job via shared storage, and keeps the peers running until
# a stop file appears (written by the sbatch driver once experiments finish).
#
# All coordination state lives under COORD_DIR, which must be on storage
# visible to every node (Sophia's /work burst-buffer, i.e. $TMPSHARE).
#
# Required environment (set by the sbatch script):
#   BUNDLE_DIR        shared dir with idss_server + generate_data.py + policy*.yaml
#   COORD_DIR         shared coordination directory for this job
#   PEERS_PER_NODE    number of peers to launch on this node
#   PROTOCOL_ID       libp2p protocol id string (global for multi-node)
#   E_CUSTOMERS/E_DAYS/E_INTERVAL_MINUTES  data generation knobs
#   POLICY_FILE       policy yaml filename (from the bundle)
# Optional:
#   NODE_LOCAL_ROOT   node-local scratch base (default: prefer $TMPDISK, then
#                      $TMPRAM, then /tmp)
#   BOOTSTRAP_WAIT_SECONDS  how long non-bootstrap nodes wait for the address
#                           (default 300)

set -Eeuo pipefail
# Peers call python3 for generate_data.py; make sure it is a modern one
set +u
command -v module >/dev/null 2>&1 || source /etc/profile >/dev/null 2>&1 || true
module load Python/3.11.3-GCCcore-12.3.0 >/dev/null 2>&1 || true
set -u
echo "[node ${SLURM_NODEID:-?}] python3 = $(command -v python3) ($(python3 --version 2>&1))"

: "${BUNDLE_DIR:?BUNDLE_DIR must be set}"
: "${COORD_DIR:?COORD_DIR must be set}"
: "${PEERS_PER_NODE:?PEERS_PER_NODE must be set}"
PROTOCOL_ID=${PROTOCOL_ID:-/kad/1.0.0}
E_CUSTOMERS=${E_CUSTOMERS:-10}
E_DAYS=${E_DAYS:-1}
E_INTERVAL_MINUTES=${E_INTERVAL_MINUTES:-15}
POLICY_FILE=${POLICY_FILE:-policy.permissive.yaml}
BOOTSTRAP_WAIT_SECONDS=${BOOTSTRAP_WAIT_SECONDS:-300}

NODE_RANK=${SLURM_NODEID:-${SLURM_PROCID:-0}}
NODE_LOCAL_ROOT=${NODE_LOCAL_ROOT:-${TMPDISK:-${TMPRAM:-/tmp}}}
WORKDIR="${NODE_LOCAL_ROOT}/idss-${SLURM_JOB_ID:-nojob}-node-${NODE_RANK}"
trap 'rm -rf "${WORKDIR}"' EXIT

BOOTSTRAP_FILE="${COORD_DIR}/bootstrap_addr.txt"
READY_DIR="${COORD_DIR}/ready"
LOGS_ROOT="${COORD_DIR}/peer-logs"
PEER_IDS_DIR="${COORD_DIR}/peer-ids"
STOP_FILE="${COORD_DIR}/stop"
NODE_LOG_DIR="${LOGS_ROOT}/node-${NODE_RANK}"

mkdir -p "${READY_DIR}" "${LOGS_ROOT}" "${PEER_IDS_DIR}" "${WORKDIR}"
rm -rf "${NODE_LOG_DIR}"
mkdir -p "${NODE_LOG_DIR}"
export PRESERVE_EXISTING_LOGS=1

echo "[node ${NODE_RANK}] staging bundle from ${BUNDLE_DIR} to ${WORKDIR}"
cp "${BUNDLE_DIR}/idss_server" "${BUNDLE_DIR}/generate_data.py" \
   "${BUNDLE_DIR}/policy.default.yaml" "${BUNDLE_DIR}/policy.permissive.yaml" \
   "${BUNDLE_DIR}/start_peers.sh" "${WORKDIR}/"
chmod +x "${WORKDIR}/idss_server" "${WORKDIR}/start_peers.sh"
cd "${WORKDIR}"

# Resolve this node's routable IP. Override IDSS_NODE_IP_CMD if `hostname -I`
# does not return the right interconnect address on your allocation.
IDSS_NODE_IP_CMD=${IDSS_NODE_IP_CMD:-"hostname -I"}
LISTEN_IP=$(eval "${IDSS_NODE_IP_CMD}" | awk '{print $1}')
if [[ -z "${LISTEN_IP}" ]]; then
  echo "[node ${NODE_RANK}] failed to resolve a routable IP via '${IDSS_NODE_IP_CMD}'" >&2
  exit 1
fi
echo "[node ${NODE_RANK}] rank=${NODE_RANK} listen_ip=${LISTEN_IP} workdir=${WORKDIR}"

DATA_ARGS=(--customers "${E_CUSTOMERS}" --days "${E_DAYS}" --interval-minutes "${E_INTERVAL_MINUTES}" -policy "${POLICY_FILE}" -pid "${PROTOCOL_ID}")

# mDNS is disabled cluster-wide (see IDSS_DISABLE_MDNS above), so every peer
# needs an explicit DHT bootstrap peer (-peer) to find anyone at all - unlike
# the single-machine scripts, which rely entirely on mDNS and never pass -peer.
# Node 0's peer 1 is the one exception: it is the overlay's seed and starts
# with no bootstrap peer. It is launched alone first (phase A) so its address
# can be published for every other peer in the job, including node 0's own
# remaining peers, which are then launched as phase B pointed at it.
LAUNCHER_PIDS=()
if [[ "${NODE_RANK}" -eq 0 ]]; then
  LOG_DIR="${NODE_LOG_DIR}" LISTEN_IP="${LISTEN_IP}" DISABLE_MDNS=1 SKIP_BUILD=1 \
    DB_PATH_ROOT="${WORKDIR}/idss_graph_db" DISCOVERY_TIMEOUT_SECONDS="${BOOTSTRAP_WAIT_SECONDS}" \
    ./start_peers.sh 1 1 "${DATA_ARGS[@]}" \
    > "${NODE_LOG_DIR}/launcher-seed.log" 2>&1 &
  LAUNCHER_PIDS+=("$!")

  BOOTSTRAP_ADDR=""
  for _ in $(seq 1 "${BOOTSTRAP_WAIT_SECONDS}"); do
    BOOTSTRAP_ADDR=$(grep "First peer address:" "${NODE_LOG_DIR}/launcher-seed.log" | awk '{print $NF}' || true)
    [[ -n "${BOOTSTRAP_ADDR}" ]] && break
    kill -0 "${LAUNCHER_PIDS[0]}" 2>/dev/null || { echo "[node 0] seed launcher exited before publishing bootstrap address" >&2; cat "${NODE_LOG_DIR}/launcher-seed.log" >&2; exit 1; }
    sleep 1
  done
  if [[ -z "${BOOTSTRAP_ADDR}" ]]; then
    echo "[node 0] timed out waiting for bootstrap address" >&2
    exit 1
  fi
  echo "${BOOTSTRAP_ADDR}" > "${BOOTSTRAP_FILE}"
  echo "[node 0] published bootstrap address ${BOOTSTRAP_ADDR}"

  if (( PEERS_PER_NODE > 1 )); then
    LOG_DIR="${NODE_LOG_DIR}" LISTEN_IP="${LISTEN_IP}" DISABLE_MDNS=1 SKIP_BUILD=1 \
      DB_PATH_ROOT="${WORKDIR}/idss_graph_db" PEER_INDEX_OFFSET=1 PRESERVE_EXISTING_LOGS=1 \
      ./start_peers.sh "$((PEERS_PER_NODE - 1))" 999999999 "${DATA_ARGS[@]}" -peer "${BOOTSTRAP_ADDR}" \
      > "${NODE_LOG_DIR}/launcher-rest.log" 2>&1 &
    LAUNCHER_PIDS+=("$!")
  fi
else
  for _ in $(seq 1 "${BOOTSTRAP_WAIT_SECONDS}"); do
    [[ -s "${BOOTSTRAP_FILE}" ]] && break
    sleep 1
  done
  if [[ ! -s "${BOOTSTRAP_FILE}" ]]; then
    echo "[node ${NODE_RANK}] timed out waiting for ${BOOTSTRAP_FILE}" >&2
    exit 1
  fi
  BOOTSTRAP_ADDR=$(cat "${BOOTSTRAP_FILE}")

  LOG_DIR="${NODE_LOG_DIR}" LISTEN_IP="${LISTEN_IP}" DISABLE_MDNS=1 SKIP_BUILD=1 \
    DB_PATH_ROOT="${WORKDIR}/idss_graph_db" \
    ./start_peers.sh "${PEERS_PER_NODE}" 999999999 "${DATA_ARGS[@]}" -peer "${BOOTSTRAP_ADDR}" \
    > "${NODE_LOG_DIR}/launcher-rest.log" 2>&1 &
  LAUNCHER_PIDS+=("$!")
fi

# Wait for this node's own peers to finish joining before declaring readiness.
# ("launcher-rest.log" covers all peers on this node except node 0's lone
# seed peer, which is checked separately via launcher-seed.log.)
for _ in $(seq 1 "${BOOTSTRAP_WAIT_SECONDS}"); do
  grep -q "All peers have joined the overlay." "${NODE_LOG_DIR}/launcher-rest.log" 2>/dev/null && break
  for pid in "${LAUNCHER_PIDS[@]}"; do
    kill -0 "${pid}" 2>/dev/null || { echo "[node ${NODE_RANK}] a launcher exited early" >&2; cat "${NODE_LOG_DIR}"/launcher-*.log >&2; exit 1; }
  done
  sleep 1
done

{ cat "${NODE_LOG_DIR}"/launcher-seed.log 2>/dev/null; cat "${NODE_LOG_DIR}"/launcher-rest.log 2>/dev/null; true; } | grep 'Peer [0-9][0-9]* launched with ID ' | awk '{print $NF}' > "${PEER_IDS_DIR}/node-${NODE_RANK}.txt"
if [[ -f "${NODE_LOG_DIR}/launcher-rest.log" ]] && ! grep -q "All peers have joined the overlay." "${NODE_LOG_DIR}/launcher-rest.log"; then echo "[node ${NODE_RANK}] peers did not join within ${BOOTSTRAP_WAIT_SECONDS}s" >&2; tail -20 "${NODE_LOG_DIR}"/launcher-*.log >&2; exit 1; fi
touch "${READY_DIR}/node-${NODE_RANK}.ready"
echo "[node ${NODE_RANK}] ready with ${PEERS_PER_NODE} peers"
echo "[node ${NODE_RANK}] resources: $(free -g | awk '/Mem:/{print "RAM total="$2"G used="$3"G tmpfs/shared="$5"G"}'), /tmp: $(df -h /tmp | awk 'NR==2{print $3" used of "$2}')"

# Keep the peers alive until the driver signals completion.
while [[ ! -f "${STOP_FILE}" ]]; do
  sleep 5
  for pid in "${LAUNCHER_PIDS[@]}"; do
    kill -0 "${pid}" 2>/dev/null || { echo "[node ${NODE_RANK}] a launcher process died unexpectedly" >&2; exit 1; }
  done
done

echo "[node ${NODE_RANK}] stop file detected, shutting down peers"
for pid in "${LAUNCHER_PIDS[@]}"; do
  kill -TERM "${pid}" 2>/dev/null || true
done
for pid in "${LAUNCHER_PIDS[@]}"; do
  wait "${pid}" 2>/dev/null || true
done
