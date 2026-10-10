#!/usr/bin/env bash
# Green Power experiment matrix (CIRED 2027). Generates the datasets it needs
# (cached), runs the central baseline and the overlay configurations through
# run_local.sh, and appends one CSV row per query execution.
#
# Usage: experiments/gp/run_matrix.sh E1|E2|E3|all
#
#   E1  latency and overlay traffic vs number of peers and data scale
#       (peers = 6 DSOs x REGIONS regional partitions + 2 public partitions)
#   E2  fidelity: central baseline on every dataset (timing + exact check);
#       every overlay run is checked as well (column "check")
#   E3  completeness under peer loss, owner policies and bounded forwarding
#
# Environment (defaults in brackets):
#   SCALES ["0.01 0.1"]   REGIONS ["1 2 5 10"]   REPEATS [5]   WARMUP [1]   TTL [20]
#   DATA_ROOT [data/gp]   OUT [experiments/results/gp]   SEED [42]   SETTLE [10]
#   FORWARDS ["0 3"] (E3 bounded-forwarding settings; 0 = flood)   E3_SCALE [0.01]
#   HPC defaults (experiments/hpc/gbar_gp_job.sh): scales 0.01/0.1/1, 10 repeats, 2 warm-ups, TTL 60
# Results: $OUT/e1.csv, $OUT/e3.csv (overlay), $OUT/baseline.csv (E2), and the
# console log of each run in $OUT/logs/.
set -euo pipefail
EXP=${1:?E1|E2|E3|all}
REPO=$(cd "$(dirname "$0")/../.." && pwd)
SCALES=${SCALES:-"0.01 0.1"}
REGIONS=${REGIONS:-"1 2 5 10"}
FORWARDS=${FORWARDS:-"0 3"}
export REPEATS=${REPEATS:-5} WARMUP=${WARMUP:-1} TTL=${TTL:-20}
DATA_ROOT=${DATA_ROOT:-$REPO/data/gp}
OUT=${OUT:-$REPO/experiments/results/gp}
SEED=${SEED:-42}
SETTLE=${SETTLE:-10}
export KEEP_RUN=${KEEP_RUN:-0}
mkdir -p "$DATA_ROOT" "$OUT/logs"

# dataset <scale> <regions>: generate once, print its directory.
dataset() {
  local d="$DATA_ROOT/s${1}_r${2}_seed${SEED}"
  if [[ ! -f "$d/ground_truth.json" ]]; then
    python3 "$REPO/server/generate_cim_data.py" --out "$d" --scale "$1" --regions-per-dso "$2" --seed "$SEED" >&2
  fi
  echo "$d"
}

# overlay <csv> <logname> <scale> <regions> [VAR=value ...]: one overlay configuration.
overlay() {
  local csv=$1 name=$2 scale=$3 regions=$4; shift 4
  local d; d=$(dataset "$scale" "$regions")
  echo "=== $name (scale $scale, regions $regions) $*"
  if env "$@" OUT_CSV="$csv" TAGS="exp=$name scale=$scale regions=$regions seed=$SEED" \
       "$REPO/experiments/gp/run_local.sh" "$d" "$SETTLE" > "$OUT/logs/$name.log" 2>&1; then
    grep -E "^  Q|MISMATCH|WARNING" "$OUT/logs/$name.log" || true
  else
    echo "  run reported failures - see $OUT/logs/$name.log"; grep -E "^  Q|MISMATCH|WARNING|only" "$OUT/logs/$name.log" || true
  fi
}

e1() {
  for s in $SCALES; do for r in $REGIONS; do
    overlay "$OUT/e1.csv" "E1_s${s}_r${r}" "$s" "$r" IDSS_MAX_FORWARD=0
  done; done
}

e2() {
  (cd "$REPO" && go build -o "$OUT/baseline" ./tools/baseline)
  for s in $SCALES; do
    local d; d=$(dataset "$s" 1)
    echo "=== E2 baseline scale $s"
    rm -rf "$OUT/baseline_s$s"
    "$OUT/baseline" -f "$d/baseline_all.json" -queries "$REPO/experiments/gp/queries.tsv" -repeats "$REPEATS" \
      -out "$OUT/baseline_s$s" -csv "$OUT/baseline.csv" -label "s${s}" 2>/dev/null | grep -v "^load" || true
    local chk="python3 $REPO/experiments/gp/check_ground_truth.py $d/ground_truth.json"
    for q in Q1 Q3 Q5 Q6; do $chk $q "$OUT/baseline_s$s/$q.json" | head -1; done
    $chk Q4 "$OUT/baseline_s$s/Q4n.json" "$OUT/baseline_s$s/Q4d.json" | head -1
  done
}

e3() {
  local s=${E3_SCALE:-0.01}
  overlay "$OUT/e3.csv" E3_all_up "$s" 1 IDSS_MAX_FORWARD=0
  overlay "$OUT/e3.csv" E3_kill1 "$s" 1 IDSS_MAX_FORWARD=0 KILL=dso04
  overlay "$OUT/e3.csv" E3_kill2 "$s" 1 IDSS_MAX_FORWARD=0 "KILL=dso04 dso05"
  overlay "$OUT/e3.csv" E3_kill3 "$s" 1 IDSS_MAX_FORWARD=0 "KILL=dso03 dso04 dso05"
  overlay "$OUT/e3.csv" E3_S1_coordinator "$s" 1 IDSS_MAX_FORWARD=0 SCENARIO=S1_coordinator_only REQUESTER=coordinator
  overlay "$OUT/e3.csv" E3_S1_researcher "$s" 1 IDSS_MAX_FORWARD=0 SCENARIO=S1_coordinator_only REQUESTER=researcher
  overlay "$OUT/e3.csv" E3_S2_researcher "$s" 1 IDSS_MAX_FORWARD=0 SCENARIO=S2_mixed REQUESTER=researcher
  for f in $FORWARDS; do
    [[ "$f" == 0 ]] && continue
    for r in $REGIONS; do
      overlay "$OUT/e3.csv" "E3_fwd${f}_r${r}" "$s" "$r" IDSS_MAX_FORWARD="$f"
    done
  done
}

case "$EXP" in
  E1|e1) e1 ;;
  E2|e2) e2 ;;
  E3|e3) e3 ;;
  all) e2; e1; e3 ;;
  *) echo "unknown experiment $EXP"; exit 1 ;;
esac
echo "Results in $OUT"
