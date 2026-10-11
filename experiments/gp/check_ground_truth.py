#!/usr/bin/env python3
"""Compare client result JSONs for the Green Power report queries with ground_truth.json.

Usage:
  check_ground_truth.py <ground_truth.json> Q1|Q3|Q5|Q6 <result.json> [--allow-missing]
  check_ground_truth.py <ground_truth.json> Q4 <numerator.json> <denominator.json> [--allow-missing]

Q1: rows "year,count"; Q3: "DSO,faultCause,count"; Q5/Q6: "DSO,count";
Q4: numerator rows "insulation,decade,count" and denominator rows
"insulation,decade,km-years"; the rate is 100 * count / km-years.

The result is compared with the ground truth restricted to the DSO partitions
that answered (partition dsoNN, or regional partitions dsoNN_rK, hold DSO-NN).
A DSO with any regional partition missing is excluded from the comparison
entirely, because its answered regions give only part of its counts. Counts must match exactly;
km-years and rates to a relative tolerance of 1e-9 (floating-point sums depend
on summation order). Without --allow-missing, any missing partition fails.
"""
import json
import sys

REL_TOL = 1e-9


def load(path):
    res = json.load(open(path))
    manifest = res.get("manifest") or []
    missing = sorted(m["partition"] for m in manifest if m["status"] != "ANSWERED")
    rows = [r for r in (res.get("rows") or []) if r != res.get("header")]
    return rows, manifest, missing


def close(a, b):
    return abs(a - b) <= REL_TOL * max(abs(a), abs(b), 1e-300)


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    allow_missing = "--allow-missing" in sys.argv
    if len(args) < 3:
        print(__doc__)
        return 2
    gt, q = json.load(open(args[0])), args[1]
    rows, manifest, missing = load(args[2])
    missing_dsos = {f"DSO-{p[3:5]}" for p in missing if p.startswith("dso")}
    diffs = []

    if q == "Q1":
        if any("_r" in p for p in missing):
            print(f"Q1: skipped - regional partitions missing {missing} (no per-region ground truth)")
            return 0 if allow_missing else 1
        got = {r[0]: int(float(r[1])) for r in rows}
        want = {}
        for key, n in gt["Q1_dso"].items():
            dso, year = key.split("|")
            if dso not in missing_dsos:
                want[year] = want.get(year, 0) + n
        ok = got == want
    elif q == "Q3":
        got = {f"{r[0]}|{r[1]}": int(float(r[2])) for r in rows if r[0] not in missing_dsos}
        want = {k: v for k, v in gt["Q3"].items() if k.split("|")[0] not in missing_dsos}
        ok = got == want
    elif q in ("Q5", "Q6"):
        needs = {"Q5": ("public_digging", "events:ActivityRecord"), "Q6": ("public_weather", "events:MeasurementValue")}[q]
        public = [p for p in missing if p.startswith(needs)]
        if public:
            print(f"{q}: {'skipped' if allow_missing else 'NOT CHECKED'} - public register missing {public}")
            return 0 if allow_missing else 1
        got = {r[0]: int(float(r[1])) for r in rows if r[0] not in missing_dsos}
        want = {k: v for k, v in gt[q].items() if k not in missing_dsos}
        ok = got == want
    elif q == "Q4":
        if len(args) < 4:
            print("Q4 needs a numerator and a denominator result")
            return 2
        drows, dmanifest, dmissing = load(args[3])
        missing = sorted(p for p in set(missing) | set(dmissing) if p.startswith("dso"))  # Q4 reads DSO data only
        if missing:
            print(f"Q4: {'skipped' if allow_missing else 'NOT CHECKED'} - missing partitions {missing} "
                  f"(rates need every partition; no per-DSO ground truth)")
            return 0 if allow_missing else 1
        num = {f"{r[0]}|{r[1]}": int(float(r[2])) for r in rows}
        den = {f"{r[0]}|{r[1]}": float(r[2]) for r in drows}
        ok = num == {k: v for k, v in gt["Q4_failures"].items()}
        if not ok:
            diffs.append("failure counts differ")
        for k, v in gt["Q4_km_years"].items():
            if k not in den or not close(den[k], v):
                ok = False
                diffs.append(f"  km-years {k}: got {den.get(k)} want {v}")
        got, want = {}, {}
        for k, v in den.items():
            if v > 0:
                got[k] = 100 * num.get(k, 0) / v
        for k, v in gt["Q4_km_years"].items():
            if v > 0:
                want[k] = 100 * gt["Q4_failures"].get(k, 0) / v
        if set(got) != set(want) or any(not close(got[k], want[k]) for k in want):
            ok = False
            diffs.append("rates differ")
        print(f"Q4: {'MATCH' if ok else 'MISMATCH'} ({len(got)} insulation/decade groups; "
              f"failure counts exact, km-years and rates within {REL_TOL:g})")
        for k in sorted(want):
            print(f"    {k:<14} λ = {got.get(k, float('nan')):8.4f} per 100 km·yr")
        for d in diffs:
            print(d)
        return 0 if ok else 1
    else:
        print(f"unsupported query {q}")
        return 2

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
