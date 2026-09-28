#!/bin/bash

# A script to compile and launch IDSS peers.
# It creates a detailed log file ('peer_info.log') with the full address
# and database directory path for each peer.
#
# Copyright 2023-2027, University of Salento, Italy.
# All rights reserved.

set -Eeuo pipefail

# Check if the number of peers is passed as an argument
if [ $# -lt 1 ]; then
  echo "Usage: $0 <number_of_peers> [manager_peer_index] [--customers N] [--days N] [--interval-minutes N]"
  exit 1
fi

# Variables
NUM_PEERS=$1
shift
MANAGER_PEER_INDEX=1
if [[ $# -gt 0 && $1 != --* ]]; then
  MANAGER_PEER_INDEX=$1
  shift
fi
SERVER_ARGS=("$@")
LOG_DIR="${LOG_DIR:-./logs}"
DB_DIR="./idss_graph_db"
START_TIMEOUT_SECONDS=${START_TIMEOUT_SECONDS:-180}
DISCOVERY_TIMEOUT_SECONDS=${DISCOVERY_TIMEOUT_SECONDS:-180}
# ROUTING_CONVERGENCE_TIMEOUT_SECONDS: how long to wait for every peer's
# Kademlia routing table to contain all other peers, after the initial
# one-peer discovery handshake succeeds. Defaults to DISCOVERY_TIMEOUT_SECONDS.
ROUTING_CONVERGENCE_TIMEOUT_SECONDS=${ROUTING_CONVERGENCE_TIMEOUT_SECONDS:-${DISCOVERY_TIMEOUT_SECONDS}}
LAUNCH_TIMEOUT_SECONDS=${LAUNCH_TIMEOUT_SECONDS:-180}
BASE_METRICS_PORT=${BASE_METRICS_PORT:-2112}
BASE_PPROF_PORT=${BASE_PPROF_PORT:-6060}
# LISTEN_IP: bind address for the libp2p host (and metrics/pprof endpoints).
# Defaults to loopback for single-machine runs; set to this node's routable
# IP for multi-node clusters (see experiments/hpc/) so remote peers can dial in.
LISTEN_IP=${LISTEN_IP:-127.0.0.1}
# Do not persist merged query results in experiments (grows without bound); set to 1 to keep
export IDSS_STORE_RESULTS=${IDSS_STORE_RESULTS:-0}
# DISABLE_MDNS=1 turns off mDNS discovery, which cannot see peers on other
# hosts anyway; use for multi-node clusters to avoid noisy failed lookups.
DISABLE_MDNS=${DISABLE_MDNS:-0}
START_BATCH_SIZE=${START_BATCH_SIZE:-5}
PEER_INDEX_OFFSET=${PEER_INDEX_OFFSET:-0}
PRESERVE_EXISTING_LOGS=${PRESERVE_EXISTING_LOGS:-0}
START_PEER_RETRIES=${START_PEER_RETRIES:-3}
START_PEER_DELAY_SECONDS=${START_PEER_DELAY_SECONDS:-0}
# DB_PATH_ROOT overrides where each peer's graph database is created (see
# common.DBRoot()). Leave unset to keep the existing ./idss_graph_db default;
# point it at node-local scratch for multi-node HPC runs.
DB_PATH_ROOT=${DB_PATH_ROOT:-}

if ! [[ "${PEER_INDEX_OFFSET}" =~ ^[0-9]+$ ]]; then
  echo "PEER_INDEX_OFFSET must be a non-negative integer" >&2
  exit 1
fi

if ! [[ "${START_BATCH_SIZE}" =~ ^[1-9][0-9]*$ ]]; then
  echo "START_BATCH_SIZE must be a positive integer" >&2
  exit 1
fi

if [ $NUM_PEERS -gt 50 ]; then
  echo "Warning: Launching >50 peers may cause delays or failures on single machine. Consider reducing or using a cluster."
fi

# Increase system limits
ulimit -n 65535 2>/dev/null || ulimit -n "$(ulimit -Hn)"
ulimit -u 8192 2>/dev/null || true

# Ensure the server code is compiled. SKIP_BUILD=1 reuses an already-present
# ./idss_server binary (e.g. a bundle staged by experiments/hpc/stage_bundle.sh
# onto a node without the full module/vendor tree).
if [[ "${SKIP_BUILD:-0}" == "1" ]]; then
  if [[ ! -x ./idss_server ]]; then
    echo "SKIP_BUILD=1 but ./idss_server is missing or not executable in $(pwd)" >&2
    exit 1
  fi
else
  # -buildvcs=false: don't shell out to git for VCS stamping; avoids build
  # failures when the local .git metadata is missing/corrupted, and this repo
  # embeds no VCS info in the binary anyway.
  go build -buildvcs=false -o idss_server .

  if [ $? -ne 0 ]; then
    echo "Failed to build the Go server. Exiting."
    exit 1
  fi
fi

mkdir -p "${LOG_DIR}" "${DB_DIR}"
if [[ "${PRESERVE_EXISTING_LOGS}" != "1" ]]; then
  rm -f "${LOG_DIR}"/*.log "${LOG_DIR}"/peer_tmp_*.log
fi

# Associative arrays to store PIDs and discovery status
declare -A PIDS=()
declare -A DISCOVERY_COMPLETED=()
declare -A START_PIDS=()
declare -A START_LOGS=()
PEER_IDS=()
SHUTTING_DOWN=0

# Dump load/thread/fd diagnostics so fork failures under resource pressure are debuggable after the fact.
log_resource_diagnostics() {
  local context=$1
  {
    echo "--- resource diagnostics (${context}) ---"
    uptime 2>/dev/null || true
    echo "processes: $(ps -e --no-headers | wc -l)"
    ulimit -a 2>/dev/null || true
  } >> "${LOG_DIR}/launch-diagnostics.log" 2>&1
}

# Start one peer process. Readiness is collected separately so peers in a batch
# can initialize concurrently. Retries a few times if the backgrounded process
# never materializes (e.g. a transient fork failure under load), since bash can
# fail to create the job without raising a script-visible error.
start_peer() {
  local INDEX=$1
  local TMP_LOG="${LOG_DIR}/peer_tmp_${INDEX}.log"
  local metrics_port=$((BASE_METRICS_PORT + INDEX))
  local pprof_port=$((BASE_PPROF_PORT + INDEX))

  for ((attempt=1; attempt<=START_PEER_RETRIES; attempt++)); do
    rm -f "${TMP_LOG}"
    if [ "$INDEX" -eq "$MANAGER_PEER_INDEX" ] || { [ "$MANAGER_PEER_INDEX" -eq 0 ] && [ "$INDEX" -eq "$((PEER_INDEX_OFFSET + 1))" ]; }; then
      IDSS_METRICS_ADDR="${LISTEN_IP}:${metrics_port}" IDSS_PPROF_ADDR="${LISTEN_IP}:${pprof_port}" IDSS_LISTEN_ADDR="/ip4/${LISTEN_IP}/tcp/0" IDSS_DISABLE_MDNS="${DISABLE_MDNS}" IDSS_DB_PATH="${DB_PATH_ROOT}" GOMAXPROCS="${PEER_GOMAXPROCS:-1}" ./idss_server -manager "${SERVER_ARGS[@]}" > "${TMP_LOG}" 2>&1 &
    else
      IDSS_METRICS_ADDR="${LISTEN_IP}:${metrics_port}" IDSS_PPROF_ADDR="${LISTEN_IP}:${pprof_port}" IDSS_LISTEN_ADDR="/ip4/${LISTEN_IP}/tcp/0" IDSS_DISABLE_MDNS="${DISABLE_MDNS}" IDSS_DB_PATH="${DB_PATH_ROOT}" GOMAXPROCS="${PEER_GOMAXPROCS:-1}" ./idss_server "${SERVER_ARGS[@]}" > "${TMP_LOG}" 2>&1 &
    fi
    local new_pid=$!

    sleep 0.2
    if [[ -e "${TMP_LOG}" ]]; then
      START_PIDS["$INDEX"]=$new_pid
      START_LOGS["$INDEX"]="${TMP_LOG}"
      if (( attempt > 1 )); then
        echo "Peer ${INDEX} started on retry ${attempt}" >&2
      fi
      if (( $(echo "${START_PEER_DELAY_SECONDS} > 0" | bc -l 2>/dev/null || echo 0) )); then
        sleep "${START_PEER_DELAY_SECONDS}"
      fi
      return 0
    fi

    echo "Peer ${INDEX} failed to start (attempt ${attempt}/${START_PEER_RETRIES}); retrying" >&2
    log_resource_diagnostics "peer ${INDEX} attempt ${attempt}"
    sleep 1
  done

  echo "Peer ${INDEX} could not be started after ${START_PEER_RETRIES} attempts" >&2
  return 1
}

register_peer() {
  local INDEX=$1
  local PID=${START_PIDS[$INDEX]}
  local TMP_LOG=${START_LOGS[$INDEX]}

  # Try to extract the peer ID from logs
  local PEER_ID=""
  for ((i=1; i<=LAUNCH_TIMEOUT_SECONDS; i++)); do
    if ! kill -0 "${PID}" 2>/dev/null; then
      echo "Peer ${INDEX} exited before becoming ready:" >&2
      cat "${TMP_LOG}" >&2
      return 1
    fi
    PEER_ID=$(grep "Listening on peer Address" "${TMP_LOG}" | head -n 1 | awk -F "/p2p/" '{print $2}')
    if [[ -n "${PEER_ID}" ]]; then
      break
    fi
    sleep 1
  done

  if [ -z "$PEER_ID" ]; then
    cat "${TMP_LOG}"  # Dump log for debug
    echo "Failed to retrieve peer ID for peer ${INDEX} after ${START_TIMEOUT_SECONDS} seconds" >&2
    return 1
  fi

  # Move log to final location named by ID
  local FINAL_LOG="${LOG_DIR}/${PEER_ID}.log"
  mv "${TMP_LOG}" "${FINAL_LOG}"

  # Store process ID and discovery status
  PIDS["$PEER_ID"]=$PID
  DISCOVERY_COMPLETED["$PEER_ID"]=0
  PEER_IDS+=("$PEER_ID")

  # Create a DB directory for this peer
  mkdir -p "${DB_DIR}/${PEER_ID}"

  # Wait for the graph database load to actually finish (not just for data
  # generation to start it) before this peer counts as launched. Batches are
  # gated on this so START_BATCH_SIZE bounds how many peers are concurrently
  # doing the memory-heavy EliasDB load at once, instead of every batch racing
  # ahead into that phase together.
  for ((i=1; i<=START_TIMEOUT_SECONDS; i++)); do
    if grep -q "Data loaded into the graph database" "${FINAL_LOG}"; then
      break
    fi
    if ! kill -0 "${PID}" 2>/dev/null; then
      echo "Peer ${INDEX} exited during data initialization" >&2
      return 1
    fi
    sleep 1
  done

  echo "Peer ${INDEX} launched with ID ${PEER_ID}"

  # Show address for the first peer
  if [ "$INDEX" -eq "$((PEER_INDEX_OFFSET + 1))" ]; then
    PEER_ADDRESS=$(grep "Listening on peer Address" "${FINAL_LOG}" | head -n 1 | awk '{print $NF}')
    echo "First peer address: ${PEER_ADDRESS}"
  fi
}

launch_batch() {
  local first_index=$1
  local last_index=$2

  echo "Starting peer batch ${first_index}-${last_index}"
  for ((INDEX=first_index; INDEX<=last_index; INDEX++)); do
    start_peer "${INDEX}" || return 1
  done

  for ((INDEX=first_index; INDEX<=last_index; INDEX++)); do
    register_peer "${INDEX}" || return 1
  done
}

# Function to check discovery completion
check_all_peers_discovered() {
  for PEER_ID in "${PEER_IDS[@]}"; do
    local LOG_FILE="${LOG_DIR}/${PEER_ID}.log"
    if grep -q "Peer discovery completed" "$LOG_FILE" && [ "${DISCOVERY_COMPLETED[$PEER_ID]}" -eq 0 ]; then
      DISCOVERY_COMPLETED[$PEER_ID]=1
    fi
  done

  for PEER_ID in "${PEER_IDS[@]}"; do
    if [ "${DISCOVERY_COMPLETED[$PEER_ID]}" -eq 0 ]; then
      return 1
    fi
  done

  return 0
}

# Function to check whether every peer's Kademlia routing table has learned
# about all other peers. "Peer discovery completed" above only means a peer
# connected to one other peer; BroadcastQuery/BroadcastAggregateQuery fan out
# using kadDHT.RoutingTable().ListPeers(), which fills in later as the DHT's
# own lookups converge. Without this, queries issued right after startup only
# reach a fraction of the cluster.
check_routing_tables_converged() {
  # PEER_INDEX_OFFSET peers may already be running from a prior additive
  # launch (see README.md), so the expected cluster size is offset+NUM_PEERS.
  local target=$((PEER_INDEX_OFFSET + NUM_PEERS - 1))
  local cap=${ROUTING_TARGET_CAP:-0}
  if (( cap > 0 && target > cap )); then target=${cap}; fi
  (( target < 0 )) && target=0
  local size
  for PEER_ID in "${PEER_IDS[@]}"; do
    size=$(grep 'Routing table peers:' "${LOG_DIR}/${PEER_ID}.log" 2>/dev/null | tail -n 1 | awk '{print $NF}')
    size=${size:-0}
    if (( size < target )); then
      return 1
    fi
  done
  return 0
}

# Cleanup function
cleanup() {
  [[ "${SHUTTING_DOWN}" -eq 1 ]] && return
  SHUTTING_DOWN=1
  echo "Stopping all peers..."

  # Include peers still starting up (START_PIDS): they are not in PIDS until
  # registered, and a launch timeout used to leave them running as orphans.
  local -A seen=()
  local ALL_PIDS=()
  local pid
  for pid in ${PIDS[@]+"${PIDS[@]}"} ${START_PIDS[@]+"${START_PIDS[@]}"}; do
    [[ -n "${pid}" && -z "${seen[$pid]:-}" ]] || continue
    seen[$pid]=1
    ALL_PIDS+=("${pid}")
  done

  for pid in ${ALL_PIDS[@]+"${ALL_PIDS[@]}"}; do
    kill -TERM "${pid}" 2>/dev/null || true
  done

  for ((i=1; i<=30; i++)); do
    local running=0
    for pid in ${ALL_PIDS[@]+"${ALL_PIDS[@]}"}; do
      if kill -0 "${pid}" 2>/dev/null; then running=1; fi
    done
    [[ "${running}" -eq 0 ]] && break
    sleep 1
  done

  for pid in ${ALL_PIDS[@]+"${ALL_PIDS[@]}"}; do
    if kill -0 "${pid}" 2>/dev/null; then
      kill -KILL "${pid}" 2>/dev/null || true
    fi
    wait "${pid}" 2>/dev/null || true
  done

  # Delete the graph databases of the peers this launcher started (their
  # logs are kept). Without this, every repeat left its peers' databases in
  # DB_PATH_ROOT and a long job filled the node's disk. KEEP_PEER_DBS=1 keeps them.
  if [[ "${KEEP_PEER_DBS:-0}" != "1" ]]; then
    local f dbp id
    for f in "${LOG_DIR}"/*.log; do
      if [[ ! -f "${f}" ]]; then continue; fi
      dbp=$(grep -m1 'Database path:' "${f}" 2>/dev/null | awk '{print $NF}' || true)
      if [[ -z "${dbp}" ]]; then continue; fi
      id=${dbp##*/}
      if [[ -n "${DB_PATH_ROOT}" && "${dbp}" == "${DB_PATH_ROOT%/}/"* ]]; then rm -rf -- "${dbp}"; fi
      if [[ -n "${id}" && -d "${DB_DIR}/${id}" ]]; then rm -rf -- "${DB_DIR:?}/${id}"; fi
    done
  fi

  echo "All peers have been stopped."
}

trap cleanup EXIT

# Launch peers in bounded concurrent batches. PEER_INDEX_OFFSET allows a new
# invocation to add peers without reusing ports or temporary log names.
first_peer_index=$((PEER_INDEX_OFFSET + 1))
last_peer_index=$((PEER_INDEX_OFFSET + NUM_PEERS))
for ((batch_start=first_peer_index; batch_start<=last_peer_index; batch_start+=START_BATCH_SIZE)); do
  batch_end=$((batch_start + START_BATCH_SIZE - 1))
  if (( batch_end > last_peer_index )); then
    batch_end=${last_peer_index}
  fi
  launch_batch "${batch_start}" "${batch_end}" || exit 1
done

if (( ${#PEER_IDS[@]} != NUM_PEERS )); then
  echo "Expected ${NUM_PEERS} peers, but launched ${#PEER_IDS[@]}" >&2
  exit 1
fi

# Wait for discovery
for ((i=1; i<=DISCOVERY_TIMEOUT_SECONDS; i++)); do
  if check_all_peers_discovered; then
    break
  fi
  sleep 2
done
if ! check_all_peers_discovered; then
  echo "Peers did not complete discovery after ${DISCOVERY_TIMEOUT_SECONDS} seconds" >&2
  exit 1
fi

# Wait for DHT routing-table convergence before declaring the cluster ready.
for ((i=1; i<=ROUTING_CONVERGENCE_TIMEOUT_SECONDS; i++)); do
  if check_routing_tables_converged; then
    break
  fi
  sleep 2
done
if ! check_routing_tables_converged; then
  echo "Routing tables did not converge after ${ROUTING_CONVERGENCE_TIMEOUT_SECONDS} seconds" >&2
  exit 1
fi

echo "All peers have joined the overlay."

# Wait for all peers to exit
wait

echo "All peers have exited."