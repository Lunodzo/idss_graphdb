# Multi-node experiments on Sophia HPC

Runs the same E1-E5 experiments as the single-machine scripts
(`experiments/run_e*.sh`), but against an IDSS overlay spread across many
Slurm-allocated compute nodes instead of one machine's local peer processes.
See [dtu-sophia/docs](https://github.com/dtu-sophia/docs) for general Sophia
usage (accounts, storage, `ml` modules).

## What changes vs. the local scripts

| Aspect | Local (`server/start_peers.sh` alone) | Multi-node (this directory) |
|---|---|---|
| Peer discovery | mDNS (same LAN/host) | disabled (`IDSS_DISABLE_MDNS=1`); DHT bootstrap via one shared address |
| libp2p protocol id | `IDSS_PROTOCOL_LOCAL` (`/lan/kad/1.0.0`), the flag default | `IDSS_PROTOCOL_GLOBAL` (`/kad/1.0.0`), passed explicitly via `-pid` |
| Listen address | loopback (`127.0.0.1`), hardcoded default | this node's routable IP, via `IDSS_LISTEN_ADDR` |
| Graph DB location | `./idss_graph_db` next to the binary | node-local scratch, via `IDSS_DB_PATH` |
| Cluster lifecycle | one `start_peers.sh` call per experiment script | one persistent cluster, attached to by every experiment via `HARNESS_EXTERNAL_*` in `experiments/lib.sh` |

`experiments/run_e1_scale.sh` .. `run_e5_scenario.sh` are similar to single machine experiments.
`experiments/lib.sh`'s `harness_start_cluster`/`harness_stop_cluster` attach to
an already-running cluster instead of spawning one locally when
`HARNESS_EXTERNAL_PEER_ADDRESS` is set (see `run_distributed_experiments.sh`).
Running `./run_e1_scale.sh ...` directly, with no such variable set, behaves
exactly as before.

## How the cluster is coordinated

1. `stage_bundle.sh` builds `idss_server` once and copies it, `generate_data.py`,
   and the policy YAMLs into a shared bundle directory (avoids racing `go build`
   across 50 nodes on a shared filesystem, and avoids shipping the whole repo,
   including `vendor/`, to every node).
2. `launch_node_peers.sh` runs once per node (via `srun`). It copies the bundle
   to node-local scratch, resolves the node's IP, and starts its share of
   peers with `server/start_peers.sh` (unchanged script, new env vars).
   - Node 0's first peer starts alone with no bootstrap peer (the overlay's
     seed) and publishes its multiaddress to a file on shared storage.
   - Every other peer in the job - including node 0's own remaining peers -
     starts with `-peer <that address>` so it can join the DHT (mDNS is off).
   - Each node writes a `ready` marker once its peers have joined, and keeps
     them running until a shared `stop` file appears.
3. `sophia_multinode.sbatch` ties it together: builds the bundle, launches
   `launch_node_peers.sh` on all nodes via one `srun` call, waits for every
   node's ready marker, runs `run_distributed_experiments.sh`, then writes the
   stop file and waits for the peers to shut down.
4. `run_distributed_experiments.sh` points `experiments/lib.sh` at the
   cluster's bootstrap address and aggregated peer logs, then calls the
   existing `run_e1_scale.sh` .. `run_e5_scenario.sh` scripts with
   `peer_count = NODES * PEERS_PER_NODE`.

## Usage

```bash
cd experiments/hpc
sbatch sophia_multinode.sbatch
```

Adjust at the top of `sophia_multinode.sbatch` (or via `sbatch --export=...`,
or environment variables before submitting):

- `#SBATCH --partition` - see `sinfo`/the partitions table in the Sophia docs
  (`workq`, `rome`, `fatq`, `windq`, ...).
- `#SBATCH --nodes` - the node count (50, per the current requirement).
- `PEERS_PER_NODE` - peers per node (>= 100, per the current requirement).
- `ml Go` / `ml Python` - run `ml avail Go` and `ml avail Python` on Sophia and
  use the exact module names your allocation provides.
- `EXPERIMENTS` - space-separated subset of `e1 e2 e3 e4 e5 e6` to run
  (default: `e1`-`e5`).
- `COMMUNITIES` - `K > 0` launches the cluster as K energy communities
  (`ec-1`..`ec-K`; global peers 1..K are their managers, every other peer joins
  `ec-((g-1) mod K + 1)`) and is required by `e6` (community-scoped
  settlement). Leave it unset (0) for `e1`-`e5`, which assume the original
  single-manager setup. Example:
  `PEER_LOGS_LOCAL=1 COMMUNITIES=8 PEERS_PER_NODE=20 EXPERIMENTS=e6 ./submit.sh scale --nodes=20`.
  Keep `K <= PEERS_PER_NODE` so every manager runs on node 0, whose logs the
  driver reads live.
- `REPEATS`, `E_CUSTOMERS`, `E_DAYS`, `E_INTERVAL_MINUTES`, `POLICY_FILE` -
  same knobs the local scripts expose. `E_DAYS` defaults to 30 when
  `PEERS_PER_NODE <= 10` and 1 otherwise (see "Peers per node" below); pass it
  explicitly to override.

Results land in the usual place, `experiments/results/e{1..5}-all-results.csv`
and per-run subdirectories, alongside the local runs' rows (the `run_id`
column disambiguates them). Raw per-peer logs are copied to
`experiments/results/hpc-peer-logs-<job-id>/` before the job's scratch storage
is cleaned up.

## Peers per node: scale vs. latency

`PEERS_PER_NODE` trades off two different things you might want to measure:

- **scale** (many peers/node, e.g. 100+): the only way to reach a large total
  N with a fixed node allocation; matches the "at least 100 peers per node"
  requirement and Sophia's guidance to actually utilize whole nodes. Elapsed
  times mix real cross-host hops with cheap same-host hops, and co-located
  peers share CPU, so timing has more noise.
- **latency** (1 peer/node): every hop is a real cross-host network
  round-trip with no CPU contention between co-located peers, giving a
  cleaner TTL/latency signal, but peer count is capped at the node count and
  each peer needs more simulated history to keep query result volumes
  realistic at that much lower peer density (hence the `E_DAYS` default
  above).

Use `submit.sh` to submit either regime without hand-editing the sbatch file:

```bash
cd experiments/hpc
./submit.sh scale             # PEERS_PER_NODE=100, runs e1-e5
./submit.sh latency            # PEERS_PER_NODE=1, E_DAYS=30, runs e2 e4
./submit.sh scale --nodes=20   # extra sbatch args pass through
PEERS_PER_NODE=200 EXPERIMENTS=e1 ./submit.sh scale   # override either mode's defaults
```

## E1 and cluster size

E1 normally sweeps peer count from small to large, spinning up a fresh local
cluster per step. Re-provisioning a 50-node Slurm allocation per data point
isn't practical, so the HPC run instead attaches to the one fixed-size cluster
once (`START_PEERS=TOTAL_PEERS`), appending a single large-scale data point
(e.g. 5000 peers) to `e1-all-results.csv`. Combine it with the existing
small-scale local sweep points for the full scaling curve.

## Assumptions and caveats

- Assumes compute nodes can reach each other over TCP on ephemeral ports on
  Sophia's internal network. If a node firewall blocks this, peers on
  different nodes will publish addresses but never connect; check with your
  HPC contact if the swarm fails to reach "ready" for all nodes.
- Node IP resolution uses `hostname -I | awk '{print $1}'` by default
  (override via `IDSS_NODE_IP_CMD` if that picks the wrong interface on your
  allocation, e.g. picks a management NIC instead of the interconnect).
- Only node 0's first peer is a community manager
  (`config.IsManager`/`-manager`); every other peer on every node is a
  regular member, matching the single-manager assumption the E5 scenario and
  settlement-compilation code rely on.
- Bootstrapping is timing-sensitive: node 0's seed peer only completes its own
  "discovery" once other peers connect to it, which only happens after they
  read the published bootstrap address and start. `BOOTSTRAP_WAIT_SECONDS`
  (default 300s) bounds this; raise it for very large `PEERS_PER_NODE` if
  nodes report timing out.
