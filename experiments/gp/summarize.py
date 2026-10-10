#!/usr/bin/env python3
"""Summarise Green Power experiment CSVs (run_matrix.sh output) for the paper.

Usage: summarize.py <out dir> [--csv summary.csv]

Reads <out>/e1.csv, <out>/e3.csv and <out>/baseline.csv (whichever exist).
Warm-up rounds (round < WARMUP, detected as round 0 when a configuration has
more than one round) are dropped. For every configuration and query it prints
n, median and 95th-percentile latency, median overlay traffic, the slowdown
against the central baseline at the same scale, the partitions answered and
the set of check verdicts.
"""
import collections
import csv
import os
import statistics
import sys


def pct(xs, p):
    s = sorted(xs)
    if not s:
        return float("nan")
    k = (len(s) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def read(path):
    return list(csv.DictReader(open(path))) if os.path.exists(path) else []


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    out = sys.argv[1]
    base = collections.defaultdict(list)
    for r in read(os.path.join(out, "baseline.csv")):
        if int(r["rep"]) > 0:
            base[(r["dataset"].lstrip("s"), r["query"])].append(float(r["ms"]))
    rows = []
    for name in ("e1.csv", "e3.csv"):
        groups = collections.defaultdict(list)
        for r in read(os.path.join(out, name)):
            groups[(r["exp"], r["query"])].append(r)
        for (exp, q), rs in sorted(groups.items()):
            maxround = max(int(r["round"]) for r in rs)
            keep = [r for r in rs if int(r["round"]) > 0] if maxround > 0 else rs
            lat = [float(r["elapsed_ms"]) for r in keep if r["elapsed_ms"]]
            kib = [int(r["overlay_bytes"]) / 1024 for r in keep]
            b = base.get((rs[0]["scale"], q))
            med = statistics.median(lat) if lat else float("nan")
            total = sum(int(keep[-1][k]) for k in ("answered", "denied", "timeout", "unreachable", "error"))
            rows.append({
                "exp": exp, "scale": rs[0]["scale"], "peers": rs[0]["peers"], "max_forward": rs[0]["max_forward"],
                "query": q, "n": len(lat), "median_ms": round(med, 1), "p95_ms": round(pct(lat, 0.95), 1),
                "overlay_kib": round(statistics.median(kib), 1) if kib else "",
                "baseline_ms": round(statistics.median(b), 1) if b else "",
                "slowdown": round(med / statistics.median(b), 1) if b else "",
                "answered": f"{keep[-1]['answered']}/{total}",
                "check": ",".join(sorted({r["check"] for r in keep})),
            })
    if not rows:
        print("no e1.csv/e3.csv in", out)
        return 1
    cols = list(rows[0])
    print(" ".join(f"{c:>11}" for c in cols))
    for r in rows:
        print(" ".join(f"{str(r[c]):>11}" for c in cols))
    if "--csv" in sys.argv:
        with open(sys.argv[sys.argv.index("--csv") + 1], "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=cols)
            w.writeheader()
            w.writerows(rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
