---
mode: agent
description: "Tasks 8–9 – baseline queries, CSV output, launch scripts, checker and timing harness"
---
# Tasks 8–9: Experiment harness

Read `.github/copilot-instructions.md` first.

## 1. Central baseline (`tools/baseline`)
Extend the existing tool so it answers **all** of Q1–Q6 using the same EQL and the same `groupagg`
and `spatial` functions as the peers (single process, all data loaded). Add `-out <csv>` writing one
CSV per query, and `-repeat N` to time each query N times (report median and p95 in ms).

## 2. Client output
Add `-csv <dir>` and non-interactive `-q "<query>"` / `-qfile <file>` flags to `client/idss_client.go`
so scripted runs write each result (and its manifest) as CSV, plus a timing line:
`uqid, query_id, start_ns, end_ns, rows, bytes_received, partitions_answered, partitions_expected`.

## 3. Checker (`tools/check/main.go` or `tools/check.py`)
Compare a result CSV with `data/<run>/ground_truth.json` for Q1–Q6 and exit non-zero on any
difference. For Q4 compare rates to 1e-9 relative tolerance; everything else must be equal.
When a manifest shows missing partitions, compare against the ground truth restricted to answered
partitions.

## 4. Launch scripts (`experiments/`)
- `make_data.sh <scale> <dsos> <regions>`: runs the generator into `data/s<scale>_d<dsos>_r<regions>/`.
- `run_local.sh <datadir> [policy_dir]`: starts one peer per file in `<datadir>/peers/` with
  `-f <file> -partition <name> [-policy ...]`, waits for discovery, runs the query file, kills peers.
  Reuse the logic in `server/start_peers.sh`; do not break that script (EC use case).
- `run_loss.sh <datadir> <k>`: as above, but kills k random DSO peers before querying.
- HPC: a SLURM template `experiments/hpc/run.slurm` parameterised by peers × scale for DTU G-bar or
  Sophia. Peer count above the number of partitions is reached with `--regions-per-dso` in the
  generator (e.g. 6 DSOs × 17 regions ≈ 100 peers).

## 5. Experiment matrix (from the paper plan)
| Exp | Vary | Fixed | Output |
|---|---|---|---|
| E1 | peers {8, 32, 104, ~1000 HPC} × scale {0.01, 0.1, 1.0} | all up, policy allow-all | latency median/p95, messages, bytes, ratio to baseline |
| E2 | Q1–Q6 | 8 peers, scale 0.1 | exact-match table vs baseline and ground truth |
| E3 | k unreachable ∈ {0,1,2,3}; policies S1, S2 | 8 peers, scale 0.1 | manifest accuracy, exactness over eligible partitions |
| E4 | add `public_digging` peer to a running overlay | — | mapping steps, lines of config/code, changes to existing peers |
Repeat each configuration ≥ 10 times after 2 warm-up runs; write all raw timings to
`results/<exp>/<config>/run_<i>.csv`.

## 6. Loader speed
`broadcast.LoadGraphData` takes ~9 s for 1 % scale. Profile it (it already writes heap profiles) and
speed up, e.g. larger batches, one transaction per batch of nodes **and** edges, avoiding repeated
attribute maps. Target: 100 % scale partitions load in < 2 min each.
