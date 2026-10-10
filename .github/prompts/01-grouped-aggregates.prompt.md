---
mode: agent
description: "Task 6 – grouped, decomposable aggregates merged exactly across peers"
---

> **ON HOLD (10 Oct 2026): being revised after the EC_Test merge (protobuf fields now start at 19; reuse `access/`, `responding_peer_ids` and the EC HPC scripts). Do not run this prompt until this notice is removed.**

# Task 6: Grouped, decomposable aggregates

Read `.github/copilot-instructions.md` first and follow its design rules.

## Why
The reliability reports (Q1–Q4) need counts and sums **per group** (year, DSO, cause, insulation,
installation decade) and a failure **rate** (ratio of two sums). The current aggregate path
(`helpers.ParseAggregates`, `broadcast.HandleAggregateQuery`, `BroadcastAggregateQuery`,
`ReceiveAggregateResponse`, `helpers.SendPartialAggregateResult`) supports only one ungrouped
sum/avg/min/max and silently drops peers that fail. Do **not** modify that legacy path (the EC
use case uses it); add a new path next to it.

## Query syntax (new prefix)
```
@group(keys=<i,j,...>; aggs=<agg>[,<agg>...]) <EQL query with a show clause>
```
- `keys` are 0-based column indices of the EQL result (after the `show` clause).
- Supported `agg` (all decomposable):
  - `count`
  - `sum(i)`, `min(i)`, `max(i)` over numeric column *i*
  - `avg(i)` (carried as sum and count; divided only at the originator)
  - `exposure(li,yi,from,to)`: Σ length × years in service within [from, to], where *li* is the
    length column and *yi* the in-service-year column; per row it adds
    `length * max(0, min(to, end) - max(from, yi) + 1)` with `end = to`.

Examples:
```
@group(keys=0; aggs=count) get FailureEvent show failureYear                                   # Q1
@group(keys=0,1; aggs=count) get FailureEvent traverse failure:AFFECTS:asset:ACLineSegment traverse segment:CONTAINS:container:AssetContainer traverse asset:OWNS:owner:Organisation end end end show 4:n:name, 1:n:faultCause   # Q2/Q3
@group(keys=0,1; aggs=exposure(2,3,2013,2023)) get ACLineSegment show insulation, inServiceDecade, lengthKm, inServiceYear   # Q4 denominator
```
For Q4, if `inServiceDecade` is not an attribute, support a key transform `decade(i)` in `keys`
(maps a year to e.g. `"1980s"`). Q4's rate = numerator query ÷ denominator query, combined at the
originator (in the client/report tool, not inside the overlay).

## Implementation steps
1. **Pure package** `groupagg` (new folder `groupagg/`):
   - `type Spec struct { Keys []KeySpec; Aggs []AggSpec }` and `Parse(query string) (Spec, eqlQuery string, err error)`.
   - `type Partial struct { Key []string; Values []float64 }` where each agg occupies a fixed number of
     slots (count:1, sum:1, min:1, max:1, avg:2, exposure:1).
   - `Local(spec, header, rows) map[string]Partial` – group the local EQL rows.
   - `Merge(a, b map[string]Partial, spec)` – add counts/sums/exposure, min of mins, max of maxes.
   - `Finalise(spec, merged) (header []string, rows [][]string)` – compute avg, emit sorted rows.
   - Table-driven tests in `groupagg/groupagg_test.go`, including: empty input, a group present on
     only one partition, min/max across partitions, avg = Σsum/Σcount (not the mean of means), and
     exposure clamping at `from`/`to`.
2. **Partition-tagged partials (exactness rule 5).** A peer's reply is a list of
   `(partitionID, []Partial)` entries: its own entry plus every entry received from its children.
   Intermediate peers **union entries by partitionID** (first one wins) and do **not** fold them
   together. Only the originator folds the union with `Merge` and calls `Finalise`. Use the peer ID
   as partitionID for now; add a `-partition` flag (default = peer ID) in `flags/idss_flags.go` so
   experiments can label peers `dso01`, `public_weather`, etc.
3. **Wire format.** Add to `server/idssschema.proto` (new field numbers only):
   ```proto
   message PartitionPartial { string partition_id = 1; repeated Row partials = 2; }
   // in QueryMessage:
   repeated PartitionPartial partition_partials = 13;
   ```
   Each `Row.data` = key strings followed by value strings (`strconv.FormatFloat(v,'g',-1,64)`; do not
   use `%f`, which loses precision). Regenerate `common/idssschema.pb.go`.
4. **Routing.** In `server/idss_main.go`, before the legacy aggregate check, route queries starting
   with `@group(` to a new `broadcast.HandleGroupedQuery(...)`, modelled on `HandleAggregateQuery`
   and `BroadcastAggregateQuery`, but: run the **EQL part** locally, build the local partial with
   `groupagg.Local`, broadcast the **full** query (with prefix) to children, collect their
   `partition_partials`, union by partitionID, and reply. The originator finalises and returns normal
   result rows plus header to the client.
5. **Duplicates.** If a peer receives a query UQID it has already processed (see
   `CheckDuplicateQuery`), it must reply with **no** partition entries, so its data cannot be counted
   twice via two parents.
6. Do not drop failed children silently. Record each child that errors or times out in a
   `failed []string` slice and log it; Task 7 turns this into the manifest.

## Acceptance
- `go test ./groupagg/...` passes; `go build ./...` and `go vet ./...` are clean.
- With data from `generate_cim_data.py --out data/s1 --scale 0.01 --dsos 6`, run 8 peers (6 DSO
  partitions + `public_digging` + `public_weather`) and submit the Q1 and Q2/Q3 queries above from the
  client. Results must equal `data/s1/ground_truth.json` (`Q1`, `Q3`) **exactly**, and must equal the
  output of `go run ./tools/baseline -f data/s1/baseline_all.json`.
- Restarting with a different overlay topology (different peer start order) gives identical results.
