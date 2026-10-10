# Copilot instructions – IDSS, Green Power Denmark use case (CIRED 2027)

## Scope and branches
- Work on branch `green_power_usecase`. Since 10 Oct 2026 it **contains EC_Test** (merged
  deliberately): shared infrastructure (access policy, responding-peer list, duplicate-query and
  concurrency fixes, bounded forwarding, HPC scripts, web console, Go 1.25.7) plus EC-specific code
  (communities, settlement, Customer/Trade/MeterReading data, experiments E1–E6).
- Do **not** modify EC-specific code or EC experiments; the energy-community use case must keep
  working. Add Green Power (GP) behaviour alongside, selected by the data file or an explicit query
  prefix. Future syncs from `EC_Test` are done by Claude, not by Copilot; never run `git merge`.
- Never run `go mod tidy`, `go mod vendor`, `go get` or toolchain changes unless asked.
- Push with `git push origin green_power_usecase` only.

## Shared infrastructure to reuse (do not duplicate)
- `access/` – role-based policy (`allow` / `aggregate` / `deny` per node kind), YAML files
  `server/policy.*.yaml`. GP extends it (GP node kinds, hidden attributes, verified requester);
  it does not write a second policy engine.
- `QueryMessage.responding_peer_ids` (field 18) – basis for the GP completeness manifest.
- Protobuf: EC uses `QueryMessage` fields 13–18 (`requester_id`, `requester_role`,
  `customer_registration`, `settlement_request`, `settlement_result`, `responding_peer_ids`).
  **GP fields start at 19.** Never renumber or reuse existing fields.
- Forwarding: `selectForwardPeers` forwards to a random subset of peers bounded by the time budget.
  For GP exactness experiments set `IDSS_MAX_FORWARD=0` (flood) or rely on the manifest to report
  partitions that were not reached.
- `requester_id` is a plain string set by the submitter; it is **not** authenticated. GP adds a
  signature check before any policy decision.

## What this branch demonstrates
A CIM-based medium-voltage (MV) cable asset and failure model deployed on the IDSS/G-IDSS
peer-to-peer overlay (Go, libp2p, Kademlia DHT, EliasDB). Paper question: can reliability reports
be produced over data that **stay with each DSO**, under **owner-defined sharing rules**, with
**exactly** the same result as a central copy, and at what cost?

## Design rules (do not violate)
1. **Custody.** Each DSO peer holds only its own data: Organisation, Substation, AssetContainer
   (cable system), ACLineSegment (cable subsection), Junction (joint), FailureEvent, WorkOrder.
   Public data live on separate peers: `public_digging` (ActivityRecord) and `public_weather`
   (MeasurementValue).
2. **Identifiers.** Every node's `key` equals its CIM `mRID` (UUID string). Never invent other IDs.
3. **Policy at source.** Any peer may originate a query. Each DSO peer decides what it returns,
   using a local policy keyed by the requester's *verified* peer ID. A peer that refuses to answer
   **still forwards** the query (routing and data roles are separate).
4. **No cable geometry leaves a DSO peer.** For weather and digging correlation, public events are
   fetched first and shipped *to* the DSO peers, which join locally and return only what their
   policy allows.
5. **Exactness.** Distributed results must equal the central result over the partitions that were
   entitled to answer. Each partition's contribution is counted **exactly once**. Never deduplicate
   by row content (two DSOs can legitimately return identical partial counts); deduplicate by
   **partition ID**.
6. **No silent partial results.** Every distributed result carries a manifest:
   partitions answered / denied-by-policy / timed-out / unreachable.

## Implemented Green Power features
- **Use-case switch**: start Green Power peers with `-usecase gp` (default `ec`). GP mode disables the
  energy-community routines (customer-registration broadcast, community manager/member) and ignores
  registration/settlement messages, requires a prepared partition file (`-f`), and uses
  `server/policy.greenpower.yaml` unless `-policy` is given. Never change the `ec` default.
- **Grouped queries** (`groupagg/`, `broadcast/idss_grouped.go`):
  `@group(keys=<i|decade(i)>,...; aggs=count|sum(i)|min(i)|max(i)|avg(i)|exposure(li,yi,from,to)) <EQL with show>`.
  Each peer returns one `PartitionPartial` (field 19) tagged with its `-partition` label; entries are
  unioned by partition and folded only at the originator. The client prints the manifest
  ("answered 8/8") and stores it in the result JSON.
- **Live check**: `experiments/gp/run_local.sh <datadir>` starts one peer per partition, runs Q1 and
  Q3 and compares them with `ground_truth.json` (`experiments/gp/check_ground_truth.py`).
- In the interactive client, the TTL is the number after the **last** comma (`<query>, 10`).

## Data model (EliasDB import format)
Nodes: `{"kind", "key", "mRID", ...attributes}`. Edges: `{"key","kind","end1key","end1kind",
"end1role","end1cascading","end2key","end2kind","end2role","end2cascading"}`.

| Edge | end1 (role) | end2 (role) |
|---|---|---|
| OPERATES | Organisation (operator) | Substation (station) |
| OWNS | Organisation (owner) | AssetContainer (asset) |
| CONNECTS | AssetContainer (system) | Substation (fromStation/toStation) |
| CONTAINS | AssetContainer (container) | ACLineSegment (segment) |
| JOINS | Junction (joint) | ACLineSegment (segment) |
| AFFECTS | FailureEvent (failure) | ACLineSegment (asset) |
| REPAIRS | WorkOrder (workorder) | FailureEvent (failure) |

Key attributes: ACLineSegment `insulation` (PILC/XLPE), `lengthKm`, `inServiceYear`, `lat`, `lon`,
`minLat/maxLat/minLon/maxLon`; FailureEvent `failureYear`, `failureDay` (days since 1970-01-01),
`faultCause` (`ageing/unknown` | `third-party`); ActivityRecord `startDay`, `endDay`, bbox;
MeasurementValue `day`, `measurementType`, bbox (0.25° cell).

EQL tip: traverse **from the sparse side** (FailureEvent upwards). Traversals from segments to
failures fail with "Empty traversal" because most segments have no failure. Example:
```
get FailureEvent traverse failure:AFFECTS:asset:ACLineSegment
  traverse segment:CONTAINS:container:AssetContainer
    traverse asset:OWNS:owner:Organisation end end end
show 4:n:name, 1:n:faultCause
```

## Test data and the oracle
```
python3 server/generate_cim_data.py --out data/s1 --scale 0.01 --dsos 6   # seed 42 by default
go run ./tools/baseline -f data/s1/baseline_all.json
```
`data/<run>/ground_truth.json` holds exact answers Q1–Q6 computed by the generator. Every feature
must be checked against it. `data/` is git-ignored; never commit generated data or `*.pprof`.

Report queries: Q1 failures per year; Q2 DSOs ranked by failures; Q3 failures per DSO and cause;
Q4 failure rate per 100 km·yr by insulation and installation decade
($\lambda = 100 \cdot N_f / \text{km-years}$, 2013–2023); Q5 digging-related failures (segment bbox
overlaps a digging bbox active within 30 days before the failure); Q6 weather-related failures
(segment midpoint in a weather cell with an event within 3 days before the failure).

## Coding conventions
- Go 1.24, existing package layout (`broadcast`, `helpers`, `common`, `flags`, `kaddht`, `server`,
  `client`, `tools`). Use the existing `logger` (`common.Logger`).
- Put pure logic (grouping, merging, policy evaluation, spatial tests) in functions with **no
  network or DB dependencies**, and cover them with table-driven `_test.go` unit tests.
- After any change to `server/idssschema.proto`, regenerate with
  `protoc --proto_path=. --go_out=. --go_opt=paths=source_relative idssschema.proto` in `server/`
  and copy/replace `common/idssschema.pb.go` as the project does today. Add fields; never renumber.
- Keep `go build ./...` and `go vet ./...` clean. Run `go test ./...` before committing.
- Small, single-purpose commits with descriptive messages.
