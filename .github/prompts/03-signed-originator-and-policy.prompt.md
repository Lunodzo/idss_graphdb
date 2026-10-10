---
mode: agent
description: "Task 16 – signed query originator and per-peer access policy (policy at source)"
---

> **DONE (10 Oct 2026) – implemented by Claude; do not run.** Signed requester in `identity/` (protobuf fields 21–22), roles and hidden attributes in `access/`, scenarios in `experiments/gp/policies/`. Kept for reference only.

# Task 16: Signed originator and per-peer access policy

Read `.github/copilot-instructions.md` first. Requires Tasks 6 and 7.

## Why
Each DSO decides what each requester may see. A policy keyed to the requester is meaningless today
because `QueryMessage.originator` is an unsigned string that any forwarding peer could change.
Coordinator-only querying (e.g. only the industry association's peer may query) must be expressible
as one policy configuration.

## Steps
1. **Signature.** Add to `server/idssschema.proto`: `bytes originator_pubkey = 15; bytes signature = 16;`.
   The originating peer signs the canonical bytes `uqid | query | originator | timestamp` with its
   libp2p host private key (`host.Peerstore().PrivKey(host.ID())`, `crypto.PrivKey.Sign`). Every
   receiving peer verifies with `crypto.UnmarshalPublicKey`, checks that
   `peer.IDFromPublicKey(pub) == originator`, then `pub.Verify(...)`. Invalid → do not execute
   locally, mark `DENIED (bad signature)`, but **still forward** (routing role).
   Note: the client currently submits queries to a peer; that entry peer is the originator and signs.
2. **Policy file** per peer, flag `-policy <path>` (JSON). Default when absent: allow everything
   (keeps the EC use case unaffected).
   ```json
   {
     "default": {"allow_kinds": [], "hide_attrs": [], "granularity": "none"},
     "rules": [
       {"requester": "<peerID or role>", "allow_kinds": ["FailureEvent","ACLineSegment","AssetContainer","Organisation"],
        "hide_attrs": ["maxLoadingPct","lat","lon","minLat","maxLat","minLon","maxLon"],
        "granularity": "aggregate", "min_group_size": 0}
     ],
     "roles": {"<peerID>": "coordinator"}
   }
   ```
   `granularity`: `rows` (records allowed), `aggregate` (only `@group` queries are answered),
   `none` (refuse).
3. **Pure evaluator** `policy.Decide(policy, requesterID, query) (Decision, reason string)` in a new
   `policy/` package: deny if granularity is `none`; deny plain queries if granularity is `aggregate`;
   deny if the EQL touches a kind not in `allow_kinds` (parse `get <Kind>` and the destination kinds
   in `traverse` specs); for allowed row queries, strip `hide_attrs` from the result columns. If
   `min_group_size > 0`, suppress groups with count below it in the local partial (report the number
   of suppressed groups in the manifest `detail`).
4. Enforce **before** `RunIDSSQuery` in both the grouped and the plain paths. A denied peer returns a
   manifest entry `DENIED` with the reason and keeps forwarding.
5. Provide two policy sets under `experiments/policies/`:
   - `S1_coordinator_only/`: every DSO allows only the coordinator peer (granularity `rows`).
   - `S2_mixed/`: 2 of 6 DSOs `aggregate` only, 1 hides loading attributes, others `rows` for all.

## Unit tests
Table-driven tests for `policy.Decide`: role match, kind not allowed, aggregate-only vs plain query,
attribute stripping, unknown requester → default. Signature tests: valid, tampered query, originator
field swapped.

## Acceptance
- Scenario S1: a query from the coordinator returns the full ground truth; the same query from a DSO
  peer returns no DSO data and the manifest lists 6 × DENIED.
- Scenario S2: Q3 from a DSO returns exact results over the partitions that answered; manifest lists
  denials with reasons. A tampered forwarded query is rejected by all peers.
- Overhead: log the signature check + policy evaluation time per peer (needed for RQ2).
