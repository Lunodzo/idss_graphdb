#!/bin/bash

# A script to compile and launch IDSS peers.
# It creates a detailed log file ('peer_info.log') with the full address
# and database directory path for each peer.
# A script to compile and launch IDSS peers.
# It creates a detailed log file ('peer_info.log') with the full address
# and database directory path for each peer.
#
# Copyright 2023-2027, University of Salento, Italy.
# All rights reserved.

set -e # Exit immediately if a command exits with a non-zero status.

# Check if the number of peers is passed as an argument
if [ $# -eq 0 ]; then
  echo "Usage: $0 <number_of_peers>"
  exit 1
fi

# Variables
NUM_PEERS=$1
LOG_DIR="./logs"
DB_DIR="./idss_graph_db"

if [ $NUM_PEERS -gt 50 ]; then
  echo "Warning: Launching >50 peers may cause delays or failures on single machine. Consider reducing or using a cluster."
fi

# Increase system limits
ulimit -n 65535  # Open files
ulimit -u 8192   # Processes

# Ensure the server code is compiled
# Ensure the server code is compiled
go build -o idss_server .

if [ $? -ne 0 ]; then
  echo "Failed to build the Go server. Exiting."
  echo "Failed to build the Go server. Exiting."
  exit 1
fi

mkdir -p "${LOG_DIR}"
mkdir -p "${DB_DIR}"

# Associative arrays to store PIDs and discovery status
declare -A PIDS
declare -A DISCOVERY_COMPLETED
PEER_IDS=()
mkdir -p "${DB_DIR}"

# Associative arrays to store PIDs and discovery status
declare -A PIDS
declare -A DISCOVERY_COMPLETED
PEER_IDS=()

# Function to launch a peer
launch_peer() {
  local INDEX=$1
  local TMP_LOG="${LOG_DIR}/peer_tmp_${INDEX}.log"
  local INDEX=$1
  local TMP_LOG="${LOG_DIR}/peer_tmp_${INDEX}.log"

  GOMAXPROCS=1 ./idss_server > "${TMP_LOG}" 2>&1 &

  local PID=$!
  sleep 1
  local PID=$!
  sleep 1

  # Try to extract the peer ID from logs
  # Try to extract the peer ID from logs
  local PEER_ID=""
  for i in {1..60}; do  # Increased timeout
    sleep 1
    PEER_ID=$(grep "Listening on peer Address" "${TMP_LOG}" | head -n 1 | awk -F "/p2p/" '{print $2}')
    PEER_ID=$(grep "Listening on peer Address" "${TMP_LOG}" | head -n 1 | awk -F "/p2p/" '{print $2}')
    if [ ! -z "$PEER_ID" ]; then
      break
    fi
  done

  if [ -z "$PEER_ID" ]; then
    cat "${TMP_LOG}"  # Dump log for debug
    echo "Failed to retrieve peer ID for peer $INDEX after 60 seconds"
    cleanup
    exit 1
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
  # Move log to final location named by ID
  local FINAL_LOG="${LOG_DIR}/${PEER_ID}.log"
  mv "${TMP_LOG}" "${FINAL_LOG}"

  # Store process ID and discovery status
  PIDS["$PEER_ID"]=$PID
  DISCOVERY_COMPLETED["$PEER_ID"]=0
  PEER_IDS+=("$PEER_ID")

  # Create a DB directory for this peer
  mkdir -p "${DB_DIR}/${PEER_ID}"

  # Wait for data generation (optional)
  # Wait for data generation (optional)
  for i in {1..100}; do
    if grep -q "Data generation completed" "${FINAL_LOG}"; then
    if grep -q "Data generation completed" "${FINAL_LOG}"; then
      break
    fi
  done

  echo "Peer ${INDEX} launched with ID ${PEER_ID}"

  # Show address for the first peer
  if [ "$INDEX" -eq 1 ]; then
    PEER_ADDRESS=$(grep "Listening on peer Address" "${FINAL_LOG}" | head -n 1 | awk '{print $NF}')
    echo "First peer address: ${PEER_ADDRESS}"
  fi

  echo "Peer ${INDEX} launched with ID ${PEER_ID}"

  # Show address for the first peer
  if [ "$INDEX" -eq 1 ]; then
    PEER_ADDRESS=$(grep "Listening on peer Address" "${FINAL_LOG}" | head -n 1 | awk '{print $NF}')
    echo "First peer address: ${PEER_ADDRESS}"
  fi
}

# Function to check discovery completion
# Function to check discovery completion
check_all_peers_discovered() {
  local updated=false
  local total_completed=0

  for PEER_ID in "${PEER_IDS[@]}"; do
    local LOG_FILE="${LOG_DIR}/${PEER_ID}.log"
    if grep -q "Peer discovery completed" "$LOG_FILE" && [ "${DISCOVERY_COMPLETED[$PEER_ID]}" -eq 0 ]; then
      DISCOVERY_COMPLETED[$PEER_ID]=1
  local total_completed=0

  for PEER_ID in "${PEER_IDS[@]}"; do
    local LOG_FILE="${LOG_DIR}/${PEER_ID}.log"
    if grep -q "Peer discovery completed" "$LOG_FILE" && [ "${DISCOVERY_COMPLETED[$PEER_ID]}" -eq 0 ]; then
      DISCOVERY_COMPLETED[$PEER_ID]=1
      updated=true
    fi
  done

  for PEER_ID in "${PEER_IDS[@]}"; do
    if [ "${DISCOVERY_COMPLETED[$PEER_ID]}" -eq 0 ]; then
      return 1
  for PEER_ID in "${PEER_IDS[@]}"; do
    if [ "${DISCOVERY_COMPLETED[$PEER_ID]}" -eq 0 ]; then
      return 1
    fi
  done

  if [ "$updated" = true ]; then
    return 0
    return 0
  fi
}

# Cleanup function
# Cleanup function
cleanup() {
  echo "Stopping all peers..."

  for PEER_ID in "${!PIDS[@]}"; do
    kill "${PIDS[$PEER_ID]}" 2>/dev/null

  for PEER_ID in "${!PIDS[@]}"; do
    kill "${PIDS[$PEER_ID]}" 2>/dev/null
  done

  for PEER_ID in "${!PIDS[@]}"; do
    wait "${PIDS[$PEER_ID]}" 2>/dev/null
  for PEER_ID in "${!PIDS[@]}"; do
    wait "${PIDS[$PEER_ID]}" 2>/dev/null
  done

  echo "All peers have been stopped."

  echo "Clearing log files..."
  find "${LOG_DIR}" -type f -exec rm -f {} +
  echo "Clearing graph database directories..."
  find "${DB_DIR}" -mindepth 1 -type d -exec rm -rf {} +
}

trap cleanup EXIT

# Launch peers
for i in $(seq 1 "$NUM_PEERS"); do
  launch_peer "$i"
  sleep 1  # Stagger launches to reduce contention
done

# Wait for discovery
# Wait for discovery
while ! check_all_peers_discovered; do
  sleep 2
  sleep 2
done

echo "All peers have joined the overlay."

echo "All peers have joined the overlay."

# Wait for all peers to exit
# Wait for all peers to exit
wait

echo "All peers have exited."