#!/usr/bin/env python3
"""Collect per-query metrics of one Green Power overlay run into CSV rows.

Usage:
  collect_metrics.py --run <run dir> --data <dataset dir> [--queries queries.tsv]
                     [--csv out.csv] [--tag key=value ...] [--allow-missing]

Inputs (written by experiments/gp/run_local.sh):
  <run>/res/*.json   client results: uqid, query, elapsedMs, wireBytesReceived,
                     header, rows, manifest
  <run>/logs/*.log   peer logs with the lines
                       gptraffic uqi=<u> streams=<n> dup=<n> bytes_out=<b> bytes_in=<b> regions=<n> failover=<n>
                       timing uqi=<u> step=<name> ms=<t>
                       Join <u> phase 1: <n> <type> events in <duration> (via providers|dht-tree)
                     Phase-1 traffic of a join has uqi "<u>-events" and is
                     attributed to query <u>.

One CSV row per query execution. "round" counts the executions of the same
query id (round 0 is the warm-up when run_local.sh uses WARMUP=1). Every result
is checked against <data>/ground_truth.json with check_ground_truth.py; Q4n
and Q4d of the same round are checked together and both get the Q4 verdict.
Exit status 1 if any check fails.
"""
import argparse
import collections
import csv
import glob
import json
import os
import re
import statistics
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
COLUMNS = ["round", "query", "uqid", "elapsed_ms", "client_bytes", "overlay_bytes", "phase1_bytes",
           "streams", "dup_replies", "failovers", "peers_executed", "local_ms_max", "access_ms_mean",
           "phase1_ms", "phase1_mode", "provider_lookup_ms", "events", "groups", "answered", "denied", "timeout", "unreachable", "error", "path_failures", "check"]

RE_TRAFFIC = re.compile(r"gptraffic uqi=(\S+) streams=(\d+) dup=(\d+) bytes_out=(\d+) bytes_in=(\d+)(?: regions=(\d+) failover=(\d+))?")
RE_TIMING = re.compile(r"timing uqi=(\S+) step=(\w+) ms=([\d.]+)")
RE_PHASE1 = re.compile(r"Join (\S+) phase 1: (\d+) \w+ events in (\S+)(?: \(via ([\w-]+)\))?")
UNITS = {"ns": 1e-6, "us": 1e-3, "µs": 1e-3, "ms": 1.0, "s": 1e3, "m": 6e4, "h": 3.6e6}


def go_duration_ms(s):
    """Parse a Go duration string (72.5ms, 1m2.5s, 850µs) into milliseconds."""
    total = 0.0
    for num, unit in re.findall(r"([\d.]+)(ns|us|µs|ms|s|m|h)", s):
        total += float(num) * UNITS[unit]
    return total


def read_queries(path):
    by_text = {}
    for line in open(path, encoding="utf-8"):
        line = line.strip()
        if line and not line.startswith("#"):
            qid, text = line.split("\t", 1)
            by_text[text.strip()] = qid.strip()
    return by_text


def parse_logs(run):
    traffic = collections.defaultdict(lambda: [0, 0, 0, 0, 0])  # streams, dup, out, in, failovers (main query)
    p1_bytes = collections.Counter()
    steps = collections.defaultdict(lambda: collections.defaultdict(list))
    phase1 = {}
    for path in glob.glob(os.path.join(run, "logs", "*.log")):
        for line in open(path, encoding="utf-8", errors="replace"):
            if "gptraffic" in line:
                m = RE_TRAFFIC.search(line)
                if m:
                    u, vals = m.group(1), [int(x or 0) for x in m.groups()[1:]]
                    vals = vals[:4] + [vals[5]]   # drop regions, keep failovers
                    if u.endswith("-events"):
                        p1_bytes[u[:-7]] += vals[2] + vals[3]
                    else:
                        t = traffic[u]
                        for i in range(5):
                            t[i] += vals[i]
            elif "timing uqi=" in line:
                m = RE_TIMING.search(line)
                if m:
                    steps[m.group(1)][m.group(2)].append(float(m.group(3)))
            elif "phase 1:" in line:
                m = RE_PHASE1.search(line)
                if m:
                    phase1[m.group(1)] = (int(m.group(2)), go_duration_ms(m.group(3)), m.group(4) or "")
    return traffic, p1_bytes, steps, phase1


def check(data, qid, files, allow_missing):
    cmd = [sys.executable, os.path.join(HERE, "check_ground_truth.py"),
           os.path.join(data, "ground_truth.json"), qid, *files]
    if allow_missing:
        cmd.append("--allow-missing")
    p = subprocess.run(cmd, capture_output=True, text=True)
    first = (p.stdout.strip().splitlines() or ["ERROR " + p.stderr.strip()])[0]
    verdict = "MATCH" if "MATCH" in first and "MISMATCH" not in first else \
        "SKIPPED" if "skipped" in first else "MISMATCH" if "MISMATCH" in first else "FAIL"
    return verdict, p.returncode == 0, p.stdout.rstrip()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--data", required=True)
    ap.add_argument("--queries", default=os.path.join(HERE, "queries.tsv"))
    ap.add_argument("--csv")
    ap.add_argument("--tag", action="append", default=[], help="key=value written to every row")
    ap.add_argument("--allow-missing", action="store_true")
    a = ap.parse_args()

    by_text = read_queries(a.queries)
    traffic, p1_bytes, steps, phase1 = parse_logs(a.run)
    results = []
    for path in glob.glob(os.path.join(a.run, "res", "*.json")):
        r = json.load(open(path))
        r["_file"] = path
        results.append(r)
    results.sort(key=lambda r: int(r.get("uqid", "0-0").rsplit("-", 1)[-1]))  # uqid = <client>-<unix nanos>

    rounds = collections.Counter()
    rows, by_round = [], {}
    for r in results:
        qid = by_text.get(r.get("query", "").strip(), "?")
        rnd = rounds[qid]
        rounds[qid] += 1
        u = r.get("uqid", "")
        man = r.get("manifest") or []
        # Data partitions only; "peer:<id>" and "region:<cpl>@<peer>" entries are
        # propagation failures on the way (counted separately).
        st = collections.Counter(m["status"] for m in man if not m["partition"].startswith(("peer:", "region:")))
        path_failures = sum(1 for m in man if m["partition"].startswith(("peer:", "region:")))
        t = traffic.get(u, [0, 0, 0, 0, 0])
        lookup = steps.get(u, {}).get("provider_lookup", [])
        local = steps.get(u, {}).get("grouped_local", [])
        access = steps.get(u, {}).get("gp_access", [])
        ev, p1ms, p1mode = phase1.get(u, ("", "", ""))
        row = {"round": rnd, "query": qid, "uqid": u, "elapsed_ms": r.get("elapsedMs", ""),
               "client_bytes": r.get("wireBytesReceived", ""), "overlay_bytes": t[2] + t[3] + p1_bytes.get(u, 0),
               "phase1_bytes": p1_bytes.get(u, 0) if u in phase1 else "", "streams": t[0], "dup_replies": t[1], "failovers": t[4],
               "peers_executed": len(local), "local_ms_max": max(local) if local else "",
               "access_ms_mean": round(statistics.mean(access), 4) if access else "",
               "phase1_ms": p1ms, "phase1_mode": p1mode,
               "provider_lookup_ms": lookup[0] if lookup else "", "events": ev, "groups": len(r.get("rows") or []),
               "answered": st["ANSWERED"], "denied": st["DENIED"], "timeout": st["TIMEOUT"],
               "unreachable": st["UNREACHABLE"], "error": st["ERROR"], "path_failures": path_failures, "check": ""}
        rows.append(row)
        by_round[(qid, rnd)] = (row, r["_file"])

    ok_all = True
    printed = set()
    for (qid, rnd), (row, f) in sorted(by_round.items(), key=lambda kv: (kv[0][1], kv[0][0])):
        if qid in ("Q1", "Q3", "Q5", "Q6"):
            verdict, ok, out = check(a.data, qid, [f], a.allow_missing)
            row["check"] = verdict
        elif qid == "Q4n" and ("Q4d", rnd) in by_round:
            drow, df = by_round[("Q4d", rnd)]
            verdict, ok, out = check(a.data, "Q4", [f, df], a.allow_missing)
            row["check"] = drow["check"] = verdict
            qid = "Q4"
        else:
            continue
        ok_all &= ok
        if qid not in printed or not ok:   # full checker output once per query, and for every failure
            printed.add(qid)
            print(out if not ok else out.splitlines()[0])

    tags = dict(t.split("=", 1) for t in a.tag)
    if a.csv:
        new = not os.path.exists(a.csv)
        with open(a.csv, "a", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(tags) + COLUMNS)
            if new:
                w.writeheader()
            for row in rows:
                w.writerow({**tags, **row})
    # Short summary per query: median latency and overlay traffic over the non-warm-up rounds.
    summary = collections.defaultdict(list)
    for row in rows:
        if row["round"] > 0 or rounds[row["query"]] == 1:
            summary[row["query"]].append(row)
    for qid in sorted(summary):
        rs = summary[qid]
        lat = statistics.median(float(x["elapsed_ms"]) for x in rs if x["elapsed_ms"] != "")
        kb = statistics.median(x["overlay_bytes"] for x in rs) / 1024
        print(f"  {qid:<4} n={len(rs)}  median {lat:8.1f} ms  overlay {kb:9.1f} KiB  "
              f"answered {rs[-1]['answered']}/{sum(rs[-1][k] for k in ('answered', 'denied', 'timeout', 'unreachable', 'error'))}  "
              f"[{','.join(sorted({x['check'] for x in rs}))}]")
    if len(results) == 0:
        print("no results found in", a.run)
        ok_all = False
    return 0 if ok_all else 1


if __name__ == "__main__":
    sys.exit(main())
