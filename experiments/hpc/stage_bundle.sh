#!/bin/bash
# Builds idss_server once and stages it plus the runtime files each peer needs
# at relative paths (generate_data.py, policy YAMLs, start_peers.sh) into a
# bundle directory on shared storage. Every compute node then copies this
# small bundle to node-local disk instead of racing to `go build` on a shared
# filesystem or shipping the whole repo (including vendor/) to every node.
#
# Usage: ./stage_bundle.sh <bundle_dir>

set -Eeuo pipefail

if [ $# -lt 1 ]; then
  echo "Usage: $0 <bundle_dir>" >&2
  exit 1
fi

BUNDLE_DIR=$1
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
SERVER_DIR="${ROOT_DIR}/server"

mkdir -p "${BUNDLE_DIR}"

echo "Building idss_server in ${SERVER_DIR}..."
(cd "${SERVER_DIR}" && go build -o "${BUNDLE_DIR}/idss_server" .)

cp "${SERVER_DIR}/generate_data.py" "${BUNDLE_DIR}/"
cp "${SERVER_DIR}/policy.default.yaml" "${BUNDLE_DIR}/"
cp "${SERVER_DIR}/policy.permissive.yaml" "${BUNDLE_DIR}/"
cp "${SERVER_DIR}/start_peers.sh" "${BUNDLE_DIR}/"
chmod +x "${BUNDLE_DIR}/start_peers.sh" "${BUNDLE_DIR}/idss_server"

echo "Bundle staged at ${BUNDLE_DIR}"
