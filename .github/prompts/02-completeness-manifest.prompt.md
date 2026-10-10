---
mode: agent
description: "Task 7 – result manifest: answered / denied / timed-out / unreachable partitions"
---
# Task 7: Completeness manifest

Read `.github/copilot-instructions.md` first. Requires Task 6 (partition-tagged partials).

## Why
RQ3 asks whether every distributed result states exactly which partitions are missing. A result
under peer loss or policy denial must be **explicitly partial**, never silently incomplete.

## Steps
1. Extend `PartitionPartial` in `server/idssschema.proto` with
   `Status status = 3;` where `enum Status { ANSWERED = 0; DENIED = 1; TIMEOUT = 2; UNREACHABLE = 3; ERROR = 4; }`
   and `string detail = 4;`. Regenerate `common/idssschema.pb.go`.
2. When a child stream cannot be opened → `UNREACHABLE`; when its reply does not arrive before the
   TTL-derived deadline → `TIMEOUT`; when the reply is malformed → `ERROR`. The parent creates an
   entry for that child with the child's peer ID (or partition label if known) and no partials.
   Note: the parent only knows its *direct* children; partitions behind an unreachable child are
   unknown to it. Handle this in step 3.
3. **Expected partitions.** Add an optional flag/file `-expected <path>` on the originator: a list of
   partition IDs that should answer (e.g. from `data/<run>/manifest.json` → `partitions`). The
   originator marks every expected partition with no entry as `UNREACHABLE (not reached)`.
4. Return the manifest to the client: add `repeated PartitionPartial manifest = 14;` to
   `QueryMessage` (statuses only, no partials). The client prints a one-line summary
   (`answered 7/8, denied 0, timeout 1 [dso04]`) and writes it next to the results
   (`results/<client>_<time>_manifest.csv`).
5. Apply the same manifest to plain (non-grouped) distributed queries in `ExecuteAndBroadcastQuery`
   where feasible; if not feasible in the time available, document that only `@group` queries carry
   a manifest.

## Unit tests (pure functions)
- Combining child manifests: union by partition ID; `ANSWERED` beats `TIMEOUT` if the same partition
  arrives via two paths; unknown expected partitions become `UNREACHABLE`.

## Acceptance
- 8 peers, kill `dso04` before querying Q3: the result equals ground truth **minus DSO-04's rows**,
  and the manifest lists `dso04` as UNREACHABLE or TIMEOUT. With all peers up, the manifest shows
  8/8 answered.
