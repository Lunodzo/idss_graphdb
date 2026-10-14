#!/bin/bash
# Submits the revision campaign on Testbed B (Sophia, workq, 20 peers per
# node). Run by hand on the Sophia login node from experiments/hpc:
#   ./submit_revision.sh            submit everything
#   DRY_RUN=1 ./submit_revision.sh  print the sbatch commands only
#   ONLY="sweep e6" ./submit_revision.sh   submit a subset (sweep e6 wan central)
#
# Jobs (all on the same code, 30 days per peer from IDSS_DATA_START):
#   sweep    N = 100, 200, 400, 800, 1600 (5, 10, 20, 40, 80 nodes): E1 (10
#            repeats), E2, E3; at N = 400 also E5 and E7 (churn); at N = 1600 also E7
#   e6       scoped settlement, sequential and parallel: N = 400 in 8
#            communities (49 members), N = 800 and 1600 in 2 (399 / 799 members)
#   wan      emulated residential links at N = 400: one-way delay 10-100 ms per
#            peer pair (RTT 20-200 ms), 5 % or 20 % loss stalls, 20 or 10 Mbit/s
#            upstream; E2 and E3, plus E6 under the 5 % profile
#   central  E8 centralised baseline on one workq node (N = 100 and 400 equivalent)

set -Eeuo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
ONLY=${ONLY:-"sweep e6 wan central"}

export PEERS_PER_NODE=20 PEER_LOGS_LOCAL=1 E_DAYS=30 E_CUSTOMERS=10 RESUME_E1=0
export IDSS_DATA_START=${IDSS_DATA_START:-2026-09-01T00:00:00Z}
export E6_PARALLEL=16 QUERY_TIMEOUT_SECONDS=900

run() {
  echo "+ $*"
  if [[ "${DRY_RUN:-0}" != "1" ]]; then "$@"; fi
}

want() { [[ " ${ONLY} " == *" $1 "* ]]; }

if want sweep; then
  for nodes in 5 10 20 40 80; do
    exps="e1 e2 e3"
    (( nodes == 20 )) && exps="e1 e2 e3 e5 e7"
    (( nodes == 80 )) && exps="e1 e2 e3 e7"
    run env EXPERIMENTS="${exps}" REPEATS=3 E1_REPEATS=10 \
      ./submit.sh scale --nodes="${nodes}" --job-name="idss-n$((nodes * 20))"
  done
fi

if want e6; then
  run env EXPERIMENTS=e6 COMMUNITIES=8 REPEATS=3 ./submit.sh scale --nodes=20 --job-name=idss-e6-400
  run env EXPERIMENTS=e6 COMMUNITIES=2 REPEATS=3 ./submit.sh scale --nodes=40 --job-name=idss-e6-800
  run env EXPERIMENTS=e6 COMMUNITIES=2 REPEATS=3 ./submit.sh scale --nodes=80 --job-name=idss-e6-1600
fi

if want wan; then
  WAN_A=(IDSS_LINK_DELAY_MIN_MS=10 IDSS_LINK_DELAY_MAX_MS=100 IDSS_LINK_LOSS=0.05 IDSS_LINK_RATE_MBPS=20)
  WAN_B=(IDSS_LINK_DELAY_MIN_MS=10 IDSS_LINK_DELAY_MAX_MS=100 IDSS_LINK_LOSS=0.20 IDSS_LINK_RATE_MBPS=10)
  run env "${WAN_A[@]}" EXPERIMENTS="e2 e3" REPEATS=3 ./submit.sh scale --nodes=20 --job-name=idss-wan5
  run env "${WAN_B[@]}" EXPERIMENTS="e2 e3" REPEATS=3 ./submit.sh scale --nodes=20 --job-name=idss-wan20
  run env "${WAN_A[@]}" EXPERIMENTS=e6 COMMUNITIES=8 REPEATS=3 ./submit.sh scale --nodes=20 --job-name=idss-wan5-e6
fi

if want central; then
  run sbatch sophia_central.sbatch
fi

echo "Check with: squeue -u \$USER -o \"%.10i %.14j %.8T %.6D %.10M %R\""
