# IDSS Energy Community

IDSS Energy Community is a decentralized data storage and query prototype for local energy communities. Each peer owns an embedded EliasDB graph, participates in a libp2p and Kademlia DHT overlay, and executes EQL queries locally or across the overlay. Protocol Buffers carry query and result messages over libp2p streams.

Peers are honest-but-curious. The project provides query-time, per-peer policy enforcement but does not implement market clearing, pricing, blockchain or ledger integration, encryption at rest, or Byzantine fault tolerance.

## Energy Community Model

Sample data is generated at peer startup by `server/generate_data.py`. Node keys act as CIM `mRID` values.

| Node kind | Purpose |
|---|---|
| `Customer` | Community participant, including a manager |
| `UsagePoint`, `EndDevice` | Connection and metering assets |
| `GeneratingUnit`, `BatteryUnit` | PV generation and storage assets |
| `MeterReading` | Quarter-hourly active/reactive power, generation, and state-of-charge readings |
| `Offer`, `Bid`, `Trade` | Local flexibility orders and concluded trades |

The graph defines `owns`, `records`, `memberOf`, `places`, and `matches` edges. The data contract is in `server/energy_community_schema.json`. The generator supports `--num-customers`, `--days`, `--interval-minutes`, and `--seed`; startup derives the seed from the peer ID so a distributed query returns a real union of peer data.

## Requirements

- Go 1.23.6 or compatible Go 1.23 toolchain
- Python 3
- Git
- Docker Engine with the Docker Compose plugin, for containerized execution

`go.mod` and `go.sum` are Go dependency set. The repository vendors its Go modules, so builds use:

```sh
go test -mod=vendor ./...
```


## Run Peers

Build and start peers locally:

```sh
cd server
./start_peers.sh 3
```

`start_peers.sh` is the single supported local launcher. The retired `launch_peers.sh` is no longer used.

An optional second argument makes one peer the community manager:

```sh
./start_peers.sh 3 1
```

Pass generator options through the launcher to simulate larger communities or datasets. These settings apply independently to every peer:

```sh
# 100 peers, each with 1,000 customers and 30 days of 15-minute readings
./start_peers.sh 100 --customers 1000 --days 30 --interval-minutes 15

# 100 peers with peer 1 as manager and a smaller one-day dataset
./start_peers.sh 100 1 --customers 100 --days 1 --interval-minutes 15
```

Large runs require sufficient CPU, memory, disk space, and open-file limits. Start with a smaller dataset to establish a baseline, then increase peer and customer counts separately.

The manager peer loads `policy.manager.yaml` when it exists, otherwise `policy.default.yaml`. It keeps membership registration copies and settlement summaries locally; it is not a central raw-data store. Peer logs include the first peer multiaddress for the client.

Run one peer directly:

```sh
cd server
go run . -manager
```

### Community Topologies

The optional `manager_peer_index` selects one manager only. For example, this starts ten peers with peer 1 as the manager and nine customer peers:

```sh
./start_peers.sh 10 1
```

To run ten managers with additional customer peers, start the binaries separately after building them. Each manager keeps its own membership registry and settlement summaries; customer data remains on the customer peers.

```sh
cd server
go build -o idss_server .
for manager in $(seq 1 10); do
  ./idss_server -manager > "logs/manager-${manager}.log" 2>&1 &
done
for customer in $(seq 1 50); do
  ./idss_server > "logs/customer-${customer}.log" 2>&1 &
done
```

The current protocol does not assign a customer to one particular manager. A customer peer shares its mandatory `Customer` registration with managers it reaches through the overlay; each manager remains a peer rather than a central store.

For a manager-only community, start a single manager peer. Its self-registered `Customer` node is the community's only member:

```sh
cd server
go run . -manager
```

To start several manager-only peers, use the first loop above and omit the customer loop. Those manager peers discover one another, and each has only its own self-registration unless other registration messages arrive.

## Docker

Build and run a three-peer overlay on Linux or WSL with Docker Desktop WSL integration:

```sh
docker compose up --build --scale peer=3
```

The Compose configuration uses host networking to preserve existing ephemeral-port and mDNS discovery behavior. Stop peers with:

```sh
docker compose down
```

Use `docker compose down -v` to remove generated peer data. Run the client container with a peer multiaddress from the logs:

```sh
docker compose run --rm client -s <server_multiaddress>
```

Client JSON results are saved to `client/results` on the host.

## Access Policies

Each peer loads `server/policy.default.yaml` by default. Use `-policy <path>` to choose a peer-specific policy:

```sh
go run . -policy policy.default.yaml
```

Rules grant raw records (`allow`), aggregate/count-only results (`aggregate`), or no local contribution (`deny`). Exact-kind rules override wildcard rules; equally specific matching rules choose the most restrictive decision.

```yaml
default: deny
rules:
  - kind: Customer
    roles: [member, manager, observer]
    decision: allow
  - kind: MeterReading
    roles: [manager, observer]
    decision: aggregate
  - kind: "*"
    roles: [observer]
    decision: aggregate
```

`Customer` registration, settlement aggregates, and concluded trades are mandatory sharing classes. Fine-grained readings, asset details, and open orders are discretionary. A peer querying data from itself bypasses its own policy.

`server/policy.permissive.yaml` grants `allow` to every kind and role. It isolates the cost of query execution from the cost of policy evaluation (see `experiments/run_e4_governance.sh`) and is not intended for a real deployment.

## Submit Queries

Run the client from `client/`, supplying a peer multiaddress and requesting peer role. The role defaults to `member`.

```sh
cd client
go run . -role <member|manager|observer> -s <server_multiaddress>
```

Every query is followed by a TTL in seconds. Local queries include `-l`; `add`, `update`, and `delete` always execute locally and do not enter the broadcast path.

```sh
# Member: registration and community orders
get Customer, 3
get Offer where status = "open", 7

# Observer: aggregate-only telemetry
get MeterReading where readingType = "activePower" show @sum(value), 7

# Manager: settlement records and compilation
get Trade, 7
settle 2026-09-06T00:00:00Z 2026-09-07T00:00:00Z, 10

# Local graph query
get Customer traverse owner:owns:asset:UsagePoint -l, 3
```

Traversal syntax is `<source role>:<relationship kind>:<destination role>:<destination kind>`. For example:

```sh
get Customer traverse owner:owns:asset:UsagePoint traverse point:records:reading:MeterReading, 7
```

Distributed result queries may add a local projection and row limit:

```sh
get Trade fields mRID,volume,price,timeStamp limit 100, 3
```

Result frames larger than 4 KiB are compressed automatically and large result sets are
streamed in chunks. Forwarding selects a deterministic subset of the local DHT routing
table sized to the query's remaining TTL budget (`selectForwardPeers` in
`broadcast/idss_broadcast.go`): 20 peers per hop when 750&nbsp;ms or less remain, 40 up to
1.5&nbsp;s, 60 up to 3&nbsp;s, and 30 (`maxForwardPeers`) beyond that. It records visited peers
in the query so a peer is never forwarded to twice, and applies the 0.75 remaining time
reduction before forwarding. TTL is an end-to-end wall-clock budget in seconds; peers
stop work when the original deadline expires. Aggregate queries remain preferable for
large telemetry scans because peers return partial values instead of raw rows.

Distributed aggregate functions `@sum`, `@avg`, `@min`, and `@max` are implemented. Query results are written as JSON files under `client/results`.

## Metrics and Experiments

Prometheus metrics are exposed at `http://127.0.0.1:2112/metrics`. Metrics include query totals and duration, responding peers, returned rows, and policy decisions. A peer's own overlay ping RTT and success rate, independent of query-level TTL completeness, are exposed as JSON at `http://127.0.0.1:2112/overlay-metrics` (set `IDSS_OVERLAY_PING_COUNT` to change pings per peer, default 3). With the local multi-peer launcher, only one peer can bind these host ports at a time; `start_peers.sh` assigns each peer its own port starting at `BASE_METRICS_PORT`/`BASE_PPROF_PORT`.

Run peer-count and TTL experiments:

```sh
./experiments/run_scaling.sh <max_peers> <repeats>
```

The script runs fixed EC queries from two through `max_peers` peers at adaptive TTL values starting at 1. Each invocation creates a unique UTC directory under `experiments/results/`, so later runs do not overwrite earlier evidence. Each run stores its `scaling.csv`, `forwarding.csv`, peer-launch logs, client logs, client JSON results, and `metadata.txt`. The cumulative `experiments/results/all-results.csv` appends rows from every run and is the recommended input for publication plots. Each row contains run ID, peer count, query label, TTL, elapsed time, responding peers, and returned rows. `forwarding.csv` records peer-to-peer query sends, intermediate results, and closed streams from server logs.

### E1-E5 Experiment Suite

`experiments/lib.sh` holds shared helpers (cluster launch/teardown, query timing, wire-byte and policy-decision extraction) sourced by five scripts, one per research question. Every script writes a timestamped run directory under `experiments/results/` with a `metadata.txt`, an experiment-specific CSV, and appends to a cumulative `e*-all-results.csv`; each also prints a mean-based summary from `experiments/summarize_csv.py`.

```sh
# E1: query cost vs. community size. Q1-Q5 at a single generous TTL, peer count 2..N.
./experiments/run_e1_scale.sh <max_peers> [repeats]

# E2: time budget vs. completeness. Fixed peer count, TTL swept across an explicit list.
./experiments/run_e2_ttl.sh <peer_count> [repeats]

# E3: aggregation savings. Q3 (raw MeterReading) vs Q4 (@sum) at one configuration,
# elapsed time and wire bytes (client/idss_client.go logs "Wire bytes received").
./experiments/run_e3_aggregation.sh <peer_count> <customers> <days> [interval_minutes] [repeats]

# E4: governance correctness and cost. Q1-Q4 x {member,manager,observer} against
# server/policy.default.yaml, then again against server/policy.permissive.yaml.
# experiments/check_e4_decisions.py checks each recorded decision against the
# policy file and confirms a "deny" decision does not suppress propagation.
./experiments/run_e4_governance.sh <peer_count> [repeats]

# E5: the end-to-end community scenario with one manager peer. A seller and a
# buyer member write an Offer/Bid locally, a distributed query discovers the
# open Offer, the match is recorded as a Trade at both counterparts (local
# writes), the manager compiles settlement over 1-day/1-week/1-month billing
# periods (reporting broadcast.CompileSettlement's phase breakdown from its
# "Settlement phase=" log lines), and a DSO observer retrieves the aggregate
# it is permitted to see via an @sum query over SettlementSummary.
./experiments/run_e5_scenario.sh <peer_count> [repeats]
```

Each script accepts environment-variable overrides for TTL, dataset size, and timeouts; run a script with no arguments to see its usage line, or read its header comment for the exact knobs.

### Large-Dataset Experiments

The scaling script changes peer count but uses the generator defaults unless its launcher is extended with dataset options. To test a large dataset per peer, launch the network directly with explicit generator settings, then submit queries from a separate client terminal:

### Web Experiment Console

Start the browser-based experiment controller from the repository root:

```sh
go run ./experiments/web
```

Open `http://localhost:8080`. The console adds peers to the currently running set instead of replacing it, configures customers/days/interval size for each added batch, and runs queries while reporting elapsed time, responding peers, and returned rows. The controller stores launcher logs under `experiments/web-runtime/`.

For command-line additive startup, set `PEER_INDEX_OFFSET` to the number of existing peers and preserve their logs:

```sh
cd server
PEER_INDEX_OFFSET=20 PRESERVE_EXISTING_LOGS=1 ./start_peers.sh 5 --customers 4 --days 1 --interval-minutes 15
```

This adds peers 21-25 and leaves peers 1-20 running. Stop managed peers explicitly with `stop_clusters.sh` or the web console; do not use the scaling script for a persistent peer set because it intentionally tears down each experiment.

```sh
# Three peers; each peer generates 100 customers and 30 days of 15-minute readings
cd server
./start_peers.sh 3 --customers 100 --days 30 --interval-minutes 15
```

Use the first peer address from the launcher log:

```sh
cd client
go run . -role manager -s <first_peer_address>
```

Useful large-result queries are:

```text
get Customer, 5
get MeterReading where readingType = "activePower", 7
get MeterReading where readingType = "activePower" show @sum(value), 7
get Customer traverse owner:owns:asset:UsagePoint traverse point:records:reading:MeterReading, 7
```

At a 15-minute interval, one day contains 96 intervals. Reading volume is approximately:

```text
customers x days x 96 x reading types
```

For example, 100 customers, 30 days, and four reading types produce about 1,152,000 `MeterReading` nodes per peer, before counting customers, assets, offers, bids, and trades. Start with `--customers 10 --days 1`, then increase one dimension at a time. Stop the network from the server terminal with `./stop_clusters.sh` before starting another large run.
