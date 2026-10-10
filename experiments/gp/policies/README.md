# Green Power policy scenarios

Each scenario directory holds policy templates, resolved per partition by
`experiments/gp/run_local.sh` (`SCENARIO=<dir>`):

1. `<partition>.yaml` (e.g. `dso05.yaml`), else
2. `dso.yaml` for partitions named `dso*`, or `public.yaml` for `public_*`, else
3. `server/policy.greenpower.yaml`.

Placeholders `__COORDINATOR__` and `__RESEARCHER__` are replaced with the peer
IDs of the requester keys the script creates. Run a scenario as a given
requester with `REQUESTER=coordinator|researcher|anonymous`.

| Scenario | DSO peers | Expected |
|---|---|---|
| `S1_coordinator_only` | only the coordinator may query DSO data | coordinator: all partitions answer; anyone else: every DSO partition DENIED |
| `S2_mixed` | dso05 refuses researchers; dso06 hides `inServiceYear` and `maxLoadingPct` | researcher: dso05 DENIED on every report; dso06 DENIED on Q4 only (needs the installation year) |
