#!/bin/bash

# Check if the number of peers is passed as an argument
if [ $# -eq 0 ]; then
  echo "Usage: $0 <number_of_peers>"
  exit 1
fi

# Variables
NUM_PEERS=$1       # Number of peers to run
LOG_DIR="./logs"   # Directory to store log files
DB_DIR="./idss_graph_db"  # Directory to store graph databases

# Ensure the server code is compiled before running
go build -o idss_server .

echo "Build completed. The binary is named idss_server."

# Check if the build was successful
if [ $? -ne 0 ]; then
  echo "Failed to build the Go server. Exiting."
  exit 1
fi

# Create the log directory if it doesn't exist
mkdir -p "${LOG_DIR}"

# Array to store process IDs and discovery completion status
PIDS=()
DISCOVERY_COMPLETED=()

# Function to launch a peer
launch_peer() {
  local PEER_INDEX=$1
  local LOG_FILE="${LOG_DIR}/peer_${PEER_INDEX}.log"

  echo "Starting peer ${PEER_INDEX}"

  # Run the server in the background and redirect output to a log file
  ./idss_server > "${LOG_FILE}" 2>&1 &

  # Save the process ID
  PIDS+=($!)
  DISCOVERY_COMPLETED+=("0")  # Initialize discovery status to not completed

  # Sleep for a short time to allow peer to start and log its address
  sleep 2

  # Extract the peer ID from the log file
  local PEER_ID
  for i in {1..5}; do # Try 5 times. We have to ensure that address is avalable because we need it to create a directory for the peer's graph database
    PEER_ID=$(grep "Listening on peer Address" "${LOG_FILE}" | head -n 1 | awk -F "/p2p/" '{print $2}')
    if [ ! -z "$PEER_ID" ]; then
      break
    fi
    sleep 1
  done

  if [ -z "$PEER_ID" ]; then
    echo "Failed to retrieve peer ID for peer ${PEER_INDEX}"
    return 1
  fi

  # Create a directory for the peer's graph database
  local PEER_DB_DIR="${DB_DIR}/${PEER_ID}"
  mkdir -p "${PEER_DB_DIR}"

  # Generate unique fake data for this peer, naming it after the peer ID
  # python3 generate_data.py "${PEER_DB_DIR}/${PEER_ID}_data.json"

  #echo "Generated data saved to ${PEER_DB_DIR}/${PEER_ID}_data.json"
  
  # Print the peer's address
  if [ "$PEER_INDEX" -eq 1 ]; then
    PEER_ADDRESS=$(grep "Listening on peer Address" "${LOG_FILE}" | head -n 1 | awk '{print $NF}')
    echo "Peer 1 is running at address: ${PEER_ADDRESS}"
  fi
}

# Function to check if all peers have completed DHT discovery
check_all_peers_discovered() {
  for ((i = 1; i <= NUM_PEERS; i++)); do
    local LOG_FILE="${LOG_DIR}/peer_${i}.log"

    if grep -q "Peer discovery completed" "${LOG_FILE}"; then
      DISCOVERY_COMPLETED[$((i - 1))]="1"
    else
      DISCOVERY_COMPLETED[$((i - 1))]="0"
    fi
  done

  # Check if all elements in DISCOVERY_COMPLETED are "1"
  for status in "${DISCOVERY_COMPLETED[@]}"; do
    if [ "$status" -eq 0 ]; then
      return 1  # Not all peers have completed discovery
    fi
  done
  return 0  # All peers have completed discovery
}

# Function to clean up log files, graph database directories, and stop peers
cleanup() {
  echo "Stopping all peers..."
  
  # Kill all peer processes
  for PID in "${PIDS[@]}"; do
    kill $PID 2>/dev/null
  done

  # Wait for all peers to exit
  for PID in "${PIDS[@]}"; do
    wait $PID 2>/dev/null
  done

  echo "All peers have been stopped."

  # Clear the logs
  echo "Clearing log files..."
  find "${LOG_DIR}" -type f -exec rm -f {} +
  echo "Log files cleared."

  # Clear the graph database directories
  echo "Clearing graph database directories..."
  find "${DB_DIR}" -mindepth 1 -type d -exec rm -rf {} +
  echo "Graph database directories cleared."
}

# Trap to clean up when the script is interrupted (e.g., Ctrl+C)
trap cleanup EXIT

# Launch the specified number of peers
for i in $(seq 1 $NUM_PEERS); do
  launch_peer $i
done


# Wait for all peers to complete discovery
while ! check_all_peers_discovered; do
  sleep 2  # Wait and recheck every 2 seconds
done
echo "Peers have joined the overlay."

# Wait for all peers to exit (or stop when interrupted)
wait

echo "All peers have exited."
