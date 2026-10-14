#!/bin/bash
#SBATCH --job-name=idss-central
#SBATCH --partition=workq
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=32
#SBATCH --time=08:00:00
#SBATCH --output=idss-central-%j.out
#
# E8 on Testbed B hardware: one IDSS peer holding a whole community's data
# (equivalent to N = 100 and N = 400 peers, 10 customers each, 30 days),
# queried alone. Compare with the peer-to-peer runs at the same N.
# Submit from experiments/hpc: sbatch sophia_central.sbatch

set -Eeuo pipefail
export PATH=$HOME/go-sdk/go/bin:$PATH
export GOTOOLCHAIN=local
set +u
command -v module >/dev/null 2>&1 || source /etc/profile >/dev/null 2>&1 || true
module load Python/3.11.3-GCCcore-12.3.0 >/dev/null 2>&1 || true
set -u
export IDSS_STORE_RESULTS=0 QUERY_TIMEOUT_SECONDS=1800 FORCE_CLIENT_BUILD=1 DISABLE_MDNS=1
export IDSS_DATA_START=${IDSS_DATA_START:-2026-09-01T00:00:00Z}
export DB_PATH_ROOT=/tmp/idss-central-${SLURM_JOB_ID}
mkdir -p "${DB_PATH_ROOT}"
trap 'rm -rf "${DB_PATH_ROOT}"' EXIT

cd "${SLURM_SUBMIT_DIR}/.."
echo "code: $(git -C .. log -1 --oneline)"; hostname; lscpu | grep -E "Model name|^CPU\(s\)"; free -g | head -2
(cd ../server && go build -buildvcs=false -o idss_server .)
for n in ${E8_SIZES:-100 400}; do
  echo "== $(date -u +%FT%TZ) start: E8 equivalent N=${n}"
  ./run_e8_central.sh "${n}" "${E8_REPEATS:-3}" || echo "E8 N=${n} failed"
  echo "== $(date -u +%FT%TZ) end: E8 equivalent N=${n}"
done
