#!/bin/bash

# A script to compile and launch IDSS peers.
# It creates a detailed log file ('peer_info.log') with the full address
# and database directory path for each peer.
#
# Copyright 2023-2027, University of Salento, Italy.
# All rights reserved.

set -e # Exit immediately if a command exits with a non-zero status.

# --- Configuration ---
if [ $# -eq 0 ]; then
  echo "Usage: $0 <number_of_peers>"
  exit 1
fi

NUM_PEERS=$1
LOG_DIR="./logs"
DB_DIR_RELATIVE="./idss_graph_db"
DB_DIR_ABSOLUTE="$(pwd)/${DB_DIR_RELATIVE}" # Get absolute path for the log
PEER_INFO_FILE="peer_info.log"
PID_FILE="/tmp/.idss_pids" # Use /tmp to avoid permission issues
SERVER_BINARY="idss_server"


# --- Pre-launch Setup ---
echo "Building the IDSS server..."
go build -o ${SERVER_BINARY} .
if [ $? -ne 0 ]; then
  echo "Build failed. Exiting." >&2
  exit 1
fi
echo "Build completed."

# Clean up and create fresh directories and files
rm -rf "${LOG_DIR}" "${DB_DIR_RELATIVE}"
mkdir -p "${LOG_DIR}" "${DB_DIR_RELATIVE}"

rm -f "${PEER_INFO_FILE}" "${PID_FILE}"
touch "${PEER_INFO_FILE}" "${PID_FILE}"

# Write a header to the new information log file
echo "# Peer_Index Peer_Address DB_Repository_Path" > "${PEER_INFO_FILE}"


# --- Launch and Gather Info ---

echo "Launching ${NUM_PEERS} peers in the background..."
for i in $(seq 1 $NUM_PEERS); do
  LOG_FILE="${LOG_DIR}/peer_${i}.log"

  # Run the server in the background
  ./${SERVER_BINARY} > "${LOG_FILE}" 2>&1 &

  # Save the Process ID for the stop_cluster.sh script
  echo $! >> "${PID_FILE}"
done
echo "All peer processes have been started."


# This function waits for all peers to log their address and then records the info.
wait_and_log_peer_info() {
  echo "Gathering addresses and DB paths for all peers..."
  
  local collected_count=0
  local max_wait_seconds=30 # Max time to wait for all peers to start up
  local start_time=$(date +%s)

  # Use an array to track which peers' info has been collected
  declare -a collected_flags
  for i in $(seq 1 $NUM_PEERS); do collected_flags[$i]=0; done

  while [[ ${collected_count} -lt ${NUM_PEERS} ]]; do
    # Check for timeout
    local current_time=$(date +%s)
    if (( current_time - start_time > max_wait_seconds )); then
        echo "Error: Timed out waiting for all peers to report their addresses." >&2
        echo "Please check the log files in '${LOG_DIR}' for errors." >&2
        break
    fi

    for i in $(seq 1 $NUM_PEERS); do
      # If we haven't collected info for this peer yet, check its log
      if [[ ${collected_flags[$i]} -eq 0 ]]; then
        LOG_FILE="${LOG_DIR}/peer_${i}.log"
        
        # Grep for the address line, which indicates the peer is ready
        ADDRESS_LINE=$(grep "Listening on peer Address" "${LOG_FILE}" || true)

        if [[ -n "${ADDRESS_LINE}" ]]; then
          # --- Parse the information ---
          FULL_ADDRESS=$(echo "${ADDRESS_LINE}" | awk '{print $NF}')
          PEER_ID=$(echo "${FULL_ADDRESS}" | awk -F "/p2p/" '{print $2}')
          DB_PATH="${DB_DIR_ABSOLUTE}/${PEER_ID}"

          # --- Log the information ---
          echo "${i} ${FULL_ADDRESS} ${DB_PATH}" >> "${PEER_INFO_FILE}"
          
          # --- Mark as collected ---
          collected_flags[$i]=1
          ((collected_count++))
          echo "  -> Collected info for Peer ${i}."
        fi
      fi
    done
    sleep 0.5 # Wait a bit before re-checking the logs
  done

  if [[ ${collected_count} -eq ${NUM_PEERS} ]]; then
    echo "Successfully collected info for all ${NUM_PEERS} peers."
  fi
}

# Execute the function to gather information
wait_and_log_peer_info


# --- Final Output ---
echo ""
echo "------------------------------------------------------------"
echo "✅ Cluster launch script finished."
echo "✅ Peer information has been saved to '${PEER_INFO_FILE}'."
echo "------------------------------------------------------------"
cat "${PEER_INFO_FILE}"
echo "------------------------------------------------------------"
echo "The peers are running in the background."
echo "To stop the cluster, run: ./stop_cluster.sh"
echo "------------------------------------------------------------"