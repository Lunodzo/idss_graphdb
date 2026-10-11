# Green Power experiments (CIRED 2027)

| File | Purpose |
|---|---|
| `queries.tsv` | the report queries Q1, Q3, Q4n/Q4d (Q4 = 100·Q4n/Q4d), Q5, Q6; single source for all tools |
| `run_local.sh <data> [settle]` | one overlay run: one peer per partition, all queries, checks, metrics |
| `run_matrix.sh E1\|E2\|E3\|all` | experiment matrix; generates and caches datasets |
| `collect_metrics.py` | per-query CSV rows from client results and peer logs (called by `run_local.sh`) |
| `check_ground_truth.py` | exactness check against the generator's `ground_truth.json` |
| `summarize.py <out>` | median / p95 latency, traffic, slowdown vs baseline, completeness, verdicts |
| `policies/` | owner-policy scenarios S1, S2 |
| `../hpc/gbar_gp_job.sh`, `../hpc/sophia_gp_job.sbatch` | the full matrix on one HPC node |

## Experiments
- **E1** latency and overlay traffic vs peers (6 DSOs × `REGIONS` + 2 public partitions: 8, 14, 32, 62)
  and data scale (`SCALES`, fraction of ~65,000 km). Queries propagate along the Kademlia DHT tree
  (`dhtcast/`): one message per peer; join phase 1 asks only the providers of the public register.
- **E2** central baseline (`tools/baseline`, all partitions in one EliasDB, same aggregation code)
  per scale; every overlay run is also checked against the ground truth (`check` column).
- **E3** completeness: 0–3 DSO peers lost (`KILL`, also at 32 peers), hung peers that keep their
  connections open (`FREEZE`, SIGSTOP: delegates time out and the region fails over), policy
  scenarios S1/S2 as coordinator or researcher.

## Metrics (one CSV row per query execution)
- `elapsed_ms`, `client_bytes`: measured by the client (submit to final result).
- `overlay_bytes`: wire bytes on all peer-to-peer streams of the query, each stream counted by the
  peer that opened it (`gptraffic` log lines), including join phase 1 (`phase1_bytes`).
- `streams`, `dup_replies`, `failovers`: forwarded streams (N-1 with all peers up), replies from a
  peer that had already claimed the query (0 with tree propagation), extra region contacts tried.
- `peers_executed`, `local_ms_max`: local executions and the slowest one; `access_ms_mean`: signature
  check + policy evaluation per peer; `phase1_ms`, `phase1_mode` (providers | dht-tree),
  `provider_lookup_ms`, `events`: join phase 1.
- `answered`…`error`: manifest over data partitions; `path_failures`: `peer:`/`region:` entries;
  `check`: MATCH / SKIPPED (rates with a partition missing) / MISMATCH.

Quick local check (≈2 min): `SCALES=0.01 REGIONS="1 5" REPEATS=2 experiments/gp/run_matrix.sh E1`.

## Long-running queries (manual)
In the client: `submit <query>, 3600` prints the UQI and returns; `result <uqid>` (same entry peer, same
requester key) returns `Status <state>` while the query runs and the full result with its manifest
once it has completed. Peers send progress heartbeats every 2 s; a delegate silent for
`IDSS_GP_IDLE_TIMEOUT` (default 10 s) is failed over even when the TTL is long.
