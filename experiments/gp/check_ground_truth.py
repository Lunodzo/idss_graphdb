#!/usr/bin/env python3
"""Compare a client result JSON for a grouped report query with ground_truth.json.

Usage: check_ground_truth.py <ground_truth.json> <Q1|Q3> <result.json> [--allow-missing]

Q1: rows "year,count"; Q3: rows "DSO name,faultCause,count". Exit code 0 when the
result equals the ground truth restricted to the DSOs whose partitions answered
(per the result manifest), 1 otherwise. A result whose manifest lists missing
partitions fails unless --allow-missing is given.
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
    if q == "Q1":
        got = {r[0]: int(float(r[1])) for r in rows}
        want = {k: v for k, v in gt["Q1"].items()}
    elif q == "Q3":
        got = {f"{r[0]}|{r[1]}": int(float(r[2])) for r in rows}
        want = dict(gt["Q3"])
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
