---
mode: agent
description: "Task 5 – two-phase join: ship public weather/digging events to DSO peers, join locally"
---

> **DONE (10 Oct 2026) – implemented by Claude; do not run.** See `gpjoin/`, `collectJoinEvents` in `broadcast/idss_grouped.go`, protobuf field 20 and Q5/Q6 in `experiments/gp/run_local.sh`. Kept for reference only.

> **DONE (10 Oct 2026) – implemented by Claude; do not run.** Code in `gpjoin/` (not `spatial/`) and `broadcast/idss_grouped.go`; events travel in field 20 (`join_events`); geometry columns are listed with `cols=` and may not be group keys or aggregates. Kept for reference only.

# Task 5: Two-phase join (public data moves to private data)

Read `.github/copilot-instructions.md` first. Requires Tasks 6, 7 and 16.

## Why
Q5 (digging-related failures) and Q6 (weather-related failures) combine DSO failure/asset data with
public registers. Cable geometry is critical-infrastructure data and must not leave the DSO peer, so
the public events travel to the DSO peers instead.

## Protocol
**Phase 1 (originator → public peers).** The originator runs a normal distributed query restricted to
public kinds, e.g.
`get ActivityRecord where startDay >= <d0> and endDay >= <d0 - 30>` for digging, or
`get MeasurementValue where day >= <d0>` for weather, and collects the event list (bbox + days).
Only the public partitions answer; DSO peers hold no such kinds.

**Phase 2 (originator → DSO peers).** The originator sends a grouped query with an attached event
payload:
```
@join(type=digging; window=30) @group(keys=0; aggs=count) get FailureEvent traverse failure:AFFECTS:asset:ACLineSegment traverse segment:CONTAINS:container:AssetContainer traverse asset:OWNS:owner:Organisation end end end show 4:n:name, 1:n:failureDay, 2:n:minLat, 2:n:maxLat, 2:n:minLon, 2:n:maxLon
```
- New proto field `repeated Row join_events = 17;` carries the phase-1 events.
- Each DSO peer runs the EQL locally, keeps only rows that satisfy the join predicate against the
  events, **drops the geometry columns**, then feeds the remaining rows into `groupagg.Local`.
  Only grouped partials leave the peer (subject to its policy).

## Join predicates (pure functions in a new `spatial/` package, with unit tests)
- Digging: segment bbox overlaps event bbox **and** `event.startDay <= failureDay` **and**
  `event.endDay >= failureDay - 30`.
- Weather: segment midpoint `((minLat+maxLat)/2, (minLon+maxLon)/2)` lies in the event's 0.25° cell
  **and** `failureDay - 3 <= event.day <= failureDay`.
- These are exactly the generator's definitions (`dig_related`, `wx_related` in
  `server/generate_cim_data.py`); keep them identical so results match `ground_truth.json`.
- Use a simple grid index (bucket events by 0.01° cells for digging, by 0.25° cells for weather) so
  the local join is not O(failures × events).

## Measurements
Log phase-1 bytes and time, phase-2 bytes and time, and the payload size (number of events). This
extra round trip is part of the cost reported for RQ2.

## Acceptance
- Q5 and Q6 per DSO equal `ground_truth.json` (`Q5`, `Q6`) exactly with all peers up.
- A packet capture or debug log shows no `lat/lon/min*/max*` values in any message leaving a DSO peer.
