#!/bin/bash

# A script to stop all running IDSS peers using 'pkill' and optionally clean up.
# It first verifies which processes will be affected and asks for confirmation.
#
# Copyright 2023-2027, University of Salento, Italy.
# All rights reserved.

# --- Configuration ---
# These variables should match the ones in launch_peers.sh for cleanup purposes.
LOG_DIR="./logs"
DB_DIR="./idss_graph_db"
PEER_INFO_FILE="peer_info.log"
PID_FILE="/tmp/.idss_pids"
SERVER_BINARY="idss_server"
SEARCH_KEYWORD="idss_server" # This should match the binary name used in launch_peers.sh

# --- 1. Find and Verify Processes ---

echo "Searching for running processes with the keyword '${SEARCH_KEYWORD}'..."

# Use pgrep to list the processes that pkill *would* target.
# -a shows the PID and full command, -f searches the full command.
PROCESS_LIST=$(pgrep -af "${SEARCH_KEYWORD}")

# Check if any processes were found
if [ -z "${PROCESS_LIST}" ]; then
  echo "✅ No running IDSS processes found."
  # Still try to clean up the PID file in case it's a leftover
  rm -f "${PID_FILE}"
  exit 0
fi

# Show the user what will be stopped and ask for confirmation
echo "The following processes will be stopped:"
echo "------------------------------------------------------------"
echo "${PROCESS_LIST}"
echo "------------------------------------------------------------"
read -p "Do you want to stop these processes? [y/N] " -n 1 -r
echo # Move to a new line for cleaner output

# --- 2. Execute Stop Command ---

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Stop command cancelled by user."
    exit 1
fi

echo "Stopping all processes containing '${SEARCH_KEYWORD}'..."
# Use pkill with the same flags to stop the verified processes.
pkill -f "${SEARCH_KEYWORD}"

# Give the OS a moment to terminate the processes
sleep 1 

# Verify that the processes are gone
if pgrep -af "${SEARCH_KEYWORD}" > /dev/null; then
    echo "⚠️ Some processes could not be stopped. You may need to use a more forceful command:"
    echo "   sudo pkill -9 -f ${SEARCH_KEYWORD}"
else
    echo "✅ All IDSS processes have been stopped successfully."
fi

# Clean up the PID file, as it's no longer needed
rm -f "${PID_FILE}"


# --- 3. Clean Up Generated Artifacts ---
echo ""
read -p "Do you want to delete all logs, databases, and other generated files? [y/N] " -n 1 -r
echo

if [[ $REPLY =~ ^[Yy]$ ]]; then
  echo "Cleaning up generated directories and files..."

  # Remove directories if they exist
  if [ -d "${LOG_DIR}" ]; then
    rm -rf "${LOG_DIR}"
    echo "  -> Removed log directory: ${LOG_DIR}"
  fi

  if [ -d "${DB_DIR}" ]; then
    rm -rf "${DB_DIR}"
    echo "  -> Removed database directory: ${DB_DIR}"
  fi
  
  # Remove files if they exist
  if [ -f "${SERVER_BINARY}" ]; then
      rm "${SERVER_BINARY}"
      echo "  -> Removed server binary: ${SERVER_BINARY}"
  fi
  
  if [ -f "${PEER_INFO_FILE}" ]; then
      rm "${PEER_INFO_FILE}"
      echo "  -> Removed peer info file: ${PEER_INFO_FILE}"
  fi

  echo "✅ Cleanup complete."
else
  echo "Cleanup skipped. Logs and data are preserved."
fi