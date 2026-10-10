#!/usr/bin/env python3
"""Compare a client result JSON for a grouped report query with ground_truth.json.

Usage: check_ground_truth.py <ground_truth.json> <Q1|Q3> <result.json> [--allow-missing]

Q1: rows "year,count"; Q3: rows "DSO name,faultCause,count". The result is
compared with the ground truth restricted to the DSO partitions that answered
(partition dsoNN holds DSO-NN), so a run with missing partitions is still
checked exactly. Without --allow-missing, any missing partition is a failure;
with it, the result must equal the restricted ground truth and the manifest
must name every missing DSO partition.
"""
import json
import sys


def main():
    if len(sys.argv) < 4:
        print(__doc__)
        return 2
    gt = json.load(open(sys.argv[1]))
    q, res = sys.argv[2], json.load(open(sys.argv[3]))
    allow_missing = "--allow-missing" in sys.argv
    manifest = res.get("manifest", [])
    missing = [m["partition"] for m in manifest if m["status"] != "ANSWERED"]
    rows = [r for r in res["rows"] if r != res.get("header")]
    missing_dsos = {f"DSO-{p[3:]}" for p in missing if p.startswith("dso")}
    if q == "Q1":
        got = {r[0]: int(float(r[1])) for r in rows}
        want = {}
        for key, n in gt["Q1_dso"].items():
            dso, year = key.split("|")
            if dso not in missing_dsos:
                want[year] = want.get(year, 0) + n
    elif q == "Q3":
        got = {f"{r[0]}|{r[1]}": int(float(r[2])) for r in rows}
        want = {k: v for k, v in gt["Q3"].items() if k.split("|")[0] not in missing_dsos}
    else:
        print(f"unsupported query {q}")
        return 2
    ok = got == want
    print(f"{q}: {'MATCH' if ok else 'MISMATCH'} ({len(got)} groups; manifest answered "
          f"{len(manifest) - len(missing)}/{len(manifest)}{'; missing ' + str(missing) if missing else ''})")
    if not ok:
        for k in sorted(set(got) | set(want)):
            if got.get(k) != want.get(k):
                print(f"  {k}: got {got.get(k)} want {want.get(k)}")
    if missing and not allow_missing:
        print("  manifest reports missing partitions")
        ok = False
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
