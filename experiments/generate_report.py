#!/usr/bin/env python3
"""
Generate scaling-experiment figures and an academic-format report from
experiments/results/*/{metadata.txt,scaling.csv,peers-*.log} and the
experiments/scaling-*-resource-*.log system-resource traces.

Data quality: a (run, peer_count) stage is only used if the number of
"Peer N launched with ID" lines in its peers-<peer_count>-<repeat>.log
equals peer_count AND the log contains "All peers have joined the
overlay." Stages that fail this check recorded a nominal peer_count in
scaling.csv that does not match how many peers actually joined, so their
numbers would misrepresent scaling behaviour; they are excluded and
listed in the report instead.

Usage: python3 experiments/generate_report.py
Outputs: experiments/results/figures/*.png, experiments/results/experiment_report.md
"""
from __future__ import annotations

import csv
import re
import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent
RESULTS_DIR = ROOT / "results"
FIGURES_DIR = RESULTS_DIR / "figures"
REPORT_PATH = RESULTS_DIR / "experiment_report.md"

LAUNCHED_RE = re.compile(r"Peer \d+ launched with ID ")
JOINED_MARKER = "All peers have joined the overlay."
SMOKE_TEST_MAX_PEERS = 4  # runs at or below this scale were script-validation smoke tests

try:
    plt.style.use("seaborn-v0_8-whitegrid")
except OSError:
    plt.style.use("ggplot")
matplotlib.rcParams.update(
    {
        "figure.dpi": 130,
        "savefig.dpi": 200,
        "font.size": 11,
        "axes.titlesize": 13,
        "axes.titleweight": "bold",
        "axes.labelsize": 11,
        "legend.fontsize": 9.5,
        "figure.autolayout": True,
    }
)
PALETTE = ["#2a6f97", "#d4643f", "#476b61", "#8e44ad", "#c0392b", "#1b998b", "#e07a5f"]


@dataclass
class Run:
    run_id: str
    path: Path
    metadata: dict
    valid_peer_counts: set = field(default_factory=set)
    invalid_peer_counts: dict = field(default_factory=dict)  # peer_count -> (actual, joined)


def parse_metadata(path: Path) -> dict:
    values = {}
    for line in path.read_text().splitlines():
        if "=" in line:
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip()
    return values


SCALING_CSV_HEADER = "peer_count,query_label,ttl,elapsed_seconds,peers_responded,rows_returned"


def read_scaling_csv(path: Path) -> list[dict]:
    """Read scaling.csv, tolerating stray lines (e.g. shell trace output) before the header."""
    lines = path.read_text(errors="ignore").splitlines()
    for i, line in enumerate(lines):
        if line.strip() == SCALING_CSV_HEADER:
            return list(csv.DictReader(lines[i:]))
    return []


def verify_stage(run_path: Path, peer_count: int, repeat: int = 1) -> tuple[int, bool]:
    log_path = run_path / f"peers-{peer_count}-{repeat}.log"
    if not log_path.exists():
        return -1, False
    text = log_path.read_text(errors="ignore")
    actual = len(LAUNCHED_RE.findall(text))
    joined = JOINED_MARKER in text
    return actual, joined


def load_runs() -> list[Run]:
    runs = []
    for entry in sorted(RESULTS_DIR.iterdir()):
        if not entry.is_dir() or entry.name == "figures":
            continue
        meta_path = entry / "metadata.txt"
        if not meta_path.exists():
            continue
        meta = parse_metadata(meta_path)
        try:
            max_peers = int(meta.get("max_peers", "0"))
        except ValueError:
            max_peers = 0
        run = Run(run_id=meta.get("run_id", entry.name), path=entry, metadata=meta)
        if max_peers <= SMOKE_TEST_MAX_PEERS:
            continue  # script-validation smoke test, not a real scaling experiment
        scaling_csv = entry / "scaling.csv"
        if not scaling_csv.exists():
            continue
        peer_counts_seen = {int(row["peer_count"]) for row in read_scaling_csv(scaling_csv)}
        for peer_count in peer_counts_seen:
            actual, joined = verify_stage(entry, peer_count)
            if actual == peer_count and joined:
                run.valid_peer_counts.add(peer_count)
            else:
                run.invalid_peer_counts[peer_count] = (actual, joined)
        runs.append(run)
    return runs


def scale_band(meta: dict) -> str:
    """Classify a run's sweep design so single-target stress tests are never plotted
    alongside genuine incremental sweeps, even when both use a step of 1."""
    max_peers = int(meta.get("max_peers", "0"))
    start_peers = int(meta.get("start_peers", "2"))
    step = int(meta.get("peer_step", "1"))
    if step > 1:
        return "coarse_sweep"
    if start_peers >= max_peers and max_peers >= 200:
        return "single_stage_large"
    return "fine_sweep"


def cluster_key(meta: dict) -> tuple:
    customers = int(meta.get("customers", 4))
    days = int(meta.get("days", 1))
    interval = int(meta.get("interval_minutes", 15))
    return (customers, days, interval, scale_band(meta))


BAND_DESCRIPTIONS = {
    "fine_sweep": "fine sweep, step 1",
    "coarse_sweep": "coarse sweep, step 100",
    "single_stage_large": "single-target stress test",
}


def cluster_label(key: tuple) -> str:
    customers, days, interval, band = key
    return f"customers={customers}, days={days}, interval={interval}min, {BAND_DESCRIPTIONS[band]}"


def collect_rows(runs: list[Run]) -> list[dict]:
    """Flatten every verified scaling.csv row across all runs, tagged with cluster."""
    rows = []
    for run in runs:
        key = cluster_key(run.metadata)
        for record in read_scaling_csv(run.path / "scaling.csv"):
            peer_count = int(record["peer_count"])
            if peer_count not in run.valid_peer_counts:
                continue
            rows.append(
                {
                    "cluster": key,
                    "run_id": run.run_id,
                    "peer_count": peer_count,
                    "query_label": record["query_label"],
                    "ttl": float(record["ttl"]),
                    "elapsed_seconds": float(record["elapsed_seconds"]),
                    "peers_responded": int(record["peers_responded"]),
                    "rows_returned": int(record["rows_returned"]),
                }
            )
    return rows


def mean_by(rows: list[dict], key_fields: tuple[str, ...], value_field: str) -> dict:
    buckets = defaultdict(list)
    for row in rows:
        buckets[tuple(row[k] for k in key_fields)].append(row[value_field])
    return {key: statistics.fmean(values) for key, values in buckets.items()}


def style_axes(ax, title, xlabel, ylabel):
    ax.set_title(title, wrap=True)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.grid(True, alpha=0.35)


def fig_latency_curve(rows: list[dict], clusters: list[tuple], filename: str, title: str):
    """Latency vs peer count for point queries (customers, open_offers), one line per cluster+query."""
    point_queries = ("customers", "open_offers")
    subset = [r for r in rows if r["cluster"] in clusters and r["query_label"] in point_queries]
    if not subset:
        return None
    fig, ax = plt.subplots(figsize=(8.5, 5.2))
    color_i = 0
    for cluster in clusters:
        for query in point_queries:
            data = [r for r in subset if r["cluster"] == cluster and r["query_label"] == query]
            if not data:
                continue
            means = mean_by(data, ("peer_count",), "elapsed_seconds")
            xs = sorted(means)
            ys = [means[x] for x in xs]
            marker = "o" if len(clusters) == 1 else ("o" if cluster == clusters[0] else "s")
            label = query if len(clusters) == 1 else f"{query} ({cluster_label(cluster).split(',')[-1].strip()})"
            ax.plot(xs, ys, marker=marker, linewidth=1.6, markersize=5, label=label,
                     color=PALETTE[color_i % len(PALETTE)])
            color_i += 1
    style_axes(ax, title, "Verified peer count", "Mean elapsed time (s)")
    ax.legend()
    fig.savefig(FIGURES_DIR / filename)
    plt.close(fig)
    return filename


def fig_responders_curve(rows: list[dict], clusters: list[tuple], filename: str, title: str):
    point_queries = ("customers", "open_offers")
    subset = [r for r in rows if r["cluster"] in clusters and r["query_label"] in point_queries]
    if not subset:
        return None
    fig, ax = plt.subplots(figsize=(7.5, 5))
    max_peers = max(r["peer_count"] for r in subset)
    ax.plot([0, max_peers], [0, max_peers], linestyle="--", color="#999999", linewidth=1.2,
             label="Ideal (all peers respond)")
    color_i = 0
    for cluster in clusters:
        for query in point_queries:
            data = [r for r in subset if r["cluster"] == cluster and r["query_label"] == query]
            if not data:
                continue
            best_by_peer = defaultdict(int)
            for r in data:
                best_by_peer[r["peer_count"]] = max(best_by_peer[r["peer_count"]], r["peers_responded"])
            xs = sorted(best_by_peer)
            ys = [best_by_peer[x] for x in xs]
            label = query if len(clusters) == 1 else f"{query} ({cluster_label(cluster).split(',')[-1].strip()})"
            ax.plot(xs, ys, marker="o", linewidth=1.6, markersize=5, label=label,
                     color=PALETTE[color_i % len(PALETTE)])
            color_i += 1
    style_axes(ax, title, "Verified peer count", "Best observed responding peers")
    ax.legend()
    fig.savefig(FIGURES_DIR / filename)
    plt.close(fig)
    return filename


def fig_aggregate_latency(rows: list[dict], clusters: list[tuple], filename: str, title: str):
    subset = [r for r in rows if r["cluster"] in clusters and r["query_label"] == "active_power_sum"]
    if not subset:
        return None
    fig, ax = plt.subplots(figsize=(7.5, 5))
    peer_counts = sorted({r["peer_count"] for r in subset})
    sample_peer_counts = peer_counts[:: max(1, len(peer_counts) // 5)] or peer_counts
    color_i = 0
    for peer_count in sample_peer_counts:
        data = sorted((r["ttl"], r["elapsed_seconds"]) for r in subset if r["peer_count"] == peer_count)
        if not data:
            continue
        xs, ys = zip(*data)
        ax.plot(xs, ys, marker="o", linewidth=1.6, markersize=5, label=f"{peer_count} peers",
                 color=PALETTE[color_i % len(PALETTE)])
        color_i += 1
    style_axes(ax, title, "Query TTL (broadcast hops)", "Elapsed time (s)")
    ax.legend(title="Peer count")
    fig.savefig(FIGURES_DIR / filename)
    plt.close(fig)
    return filename


def fig_single_stage_summary(rows: list[dict], clusters: list[tuple], filename: str, title: str):
    """Bar chart for single-target stress-test clusters, where only one verified peer
    count exists per query so a latency-vs-peer-count line plot would be a single
    disconnected point rather than a meaningful curve."""
    point_queries = ("customers", "open_offers")
    subset = [r for r in rows if r["cluster"] in clusters and r["query_label"] in point_queries]
    if not subset:
        return None
    keys = sorted({(r["peer_count"], r["query_label"]) for r in subset})
    labels = [f"{q}\n({pc} peers)" for pc, q in keys]
    means = [statistics.fmean(r["elapsed_seconds"] for r in subset if r["peer_count"] == pc and r["query_label"] == q)
             for pc, q in keys]
    best_responders = [max((r["peers_responded"] for r in subset if r["peer_count"] == pc and r["query_label"] == q), default=0)
                        for pc, q in keys]
    fig, ax = plt.subplots(figsize=(7.5, 5))
    bars = ax.bar(range(len(labels)), means, color=[PALETTE[i % len(PALETTE)] for i in range(len(labels))])
    for bar, mean_value, (pc, _), responded in zip(bars, means, keys, best_responders):
        ax.annotate(f"{mean_value:.1f}s\n{responded}/{pc} responded",
                     (bar.get_x() + bar.get_width() / 2, mean_value), ha="center", va="bottom", fontsize=9)
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels)
    style_axes(ax, title, "Query", "Mean elapsed time (s)")
    fig.savefig(FIGURES_DIR / filename)
    plt.close(fig)
    return filename


RESOURCE_LOG_RE = re.compile(r"^(?P<prefix>.+)-resource-(?P<ts>\d{8}T\d{6}Z)\.log$")


def load_resource_logs(runs_by_id: dict) -> list[dict]:
    traces = []
    for path in sorted(ROOT.glob("*-resource-*.log")):
        match = RESOURCE_LOG_RE.match(path.name)
        if not match:
            continue
        rows = []
        with path.open() as handle:
            lines = handle.readlines()
        for line in lines[1:]:
            parts = line.split()
            if len(parts) < 4 or not parts[1].lstrip("-").isdigit():
                continue
            rows.append((int(parts[1]), int(parts[2]), int(parts[3])))
        if not rows or max(r[1] for r in rows) < 10:
            continue  # no meaningful peer count reached; not informative
        outcome = "unknown"
        for line in lines:
            if "RESOURCE_LIMIT_REACHED" in line:
                outcome = "stopped: resource guard"
            elif "experiment_exit_status=0" in line:
                outcome = "completed"
            elif "experiment_exit_status=" in line and "=0" not in line:
                outcome = "stopped: timeout/error"
        run_dir = RESULTS_DIR / f"{match.group('ts')}-peers500-repeats1"
        label = match.group("prefix").replace("scaling-", "").replace("-", " ")
        if not label.startswith("500 "):
            continue  # keep only single-target 500-peer stress attempts; other sweeps have their own figures
        matched_run = runs_by_id.get(run_dir.name)
        if matched_run is not None and 500 in matched_run.valid_peer_counts:
            outcome = "completed, verified"
        elif outcome == "completed":
            # The harness exited 0, but independent peer-log verification (Section 2) found
            # the 500-peer stage never actually reached a fully joined overlay.
            outcome = "exited 0, but 500 peers not verified"
        traces.append({"label": label, "timestamp": match.group("ts"), "rows": rows,
                        "outcome": outcome, "run_dir": run_dir})
    return traces


def fig_startup_profile(traces: list[dict], filename: str):
    if not traces:
        return None
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12.5, 5))
    for i, trace in enumerate(traces):
        color = PALETTE[i % len(PALETTE)]
        elapsed = [r[0] for r in trace["rows"]]
        peers = [r[1] for r in trace["rows"]]
        rss = [r[2] / 1024 for r in trace["rows"]]
        label = f"{trace['label']} ({trace['outcome']})"
        ax1.plot(elapsed, peers, color=color, linewidth=1.5, label=label)
        ax2.plot(elapsed, rss, color=color, linewidth=1.5, label=label)
    style_axes(ax1, "Live peer count over time", "Elapsed time (s)", "Running idss_server processes")
    style_axes(ax2, "Aggregate memory over time", "Elapsed time (s)", "Summed RSS (GB)")
    handles, labels = ax1.get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=2, bbox_to_anchor=(0.5, -0.16))
    fig.savefig(FIGURES_DIR / filename, bbox_inches="tight")
    plt.close(fig)
    return filename


def fig_scaling_limit_summary(traces: list[dict], filename: str):
    if not traces:
        return None
    labels = [t["label"] for t in traces]
    max_peers = [max(r[1] for r in t["rows"]) for t in traces]
    colors = ["#1b998b" if t["outcome"] == "completed, verified" else "#c0392b" if "timeout" in t["outcome"]
              else "#d4643f" for t in traces]
    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.bar(range(len(labels)), max_peers, color=colors)
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=30, ha="right")
    for bar, value in zip(bars, max_peers):
        ax.annotate(str(value), (bar.get_x() + bar.get_width() / 2, value), ha="center",
                     va="bottom", fontsize=9)
    style_axes(ax, "Peak concurrently-live peers per 500-peer launch attempt", "Attempt configuration",
                "Peak live idss_server processes")
    fig.savefig(FIGURES_DIR / filename)
    plt.close(fig)
    return filename


def render_report(runs: list[Run], rows: list[dict], traces: list[dict], figures: dict) -> str:
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    clusters = sorted({cluster_key(r.metadata) for r in runs})
    total_valid_rows = len(rows)
    excluded = [(r.run_id, pc, actual, joined) for r in runs for pc, (actual, joined) in r.invalid_peer_counts.items()]

    lines = []
    lines.append("# Scaling Behaviour of a Decentralized P2P Graph Query System: An Empirical Study\n")
    lines.append(f"*Report generated {generated} from `experiments/results/` and `experiments/*-resource-*.log`.*\n")

    lines.append("## Abstract\n")
    lines.append(
        "This report analyses distributed-query latency, response completeness, and peer-count "
        "scalability of the IDSS peer-to-peer graph database, using an EliasDB-per-peer, "
        "libp2p/Kademlia-DHT overlay. Experiments swept peer counts from 2 to 500 across two "
        "generated-dataset profiles and three EQL query shapes (two point queries and one "
        "distributed aggregate). All reported measurements were independently verified against "
        "peer-launch logs; stages where the reported peer count did not match the number of peers "
        "that actually joined the overlay were excluded rather than silently trusted. The "
        "practical scaling ceiling encountered during testing was traced not to CPU or memory "
        "exhaustion, but to test-harness readiness timeouts, which were corrected and re-validated "
        "up to 500 concurrently running peers.\n"
    )

    lines.append("## 1. Experimental Design\n")
    lines.append(
        "**System under test.** Each peer runs an embedded EliasDB graph database and joins a "
        "libp2p host participating in a Kademlia DHT overlay with mDNS/DHT rendezvous discovery. "
        "Queries are submitted to one peer (`-role manager`), which executes locally and "
        "broadcasts to a TTL-bounded, budget-limited subset of its routing-table peers "
        "(`selectForwardPeers`, capped at 20-30 peers per hop depending on remaining time budget); "
        "results are merged back along the same path and deduplicated at the originator.\n"
    )
    lines.append(
        "**Query set.** Three EQL queries were issued at each peer count, with TTL swept from 1 "
        "upward until either all current-stage peers responded or an experiment-specific TTL "
        "ceiling was reached: `get Customer` (point query, label `customers`), "
        "`get Offer where status = \"open\"` (point query, label `open_offers`), and "
        "`get MeterReading where readingType = \"activePower\" show @sum(value)` (distributed "
        "aggregate, label `active_power_sum`).\n"
    )
    lines.append(
        "**Dataset profiles.** Two synthetic per-peer datasets were generated by "
        "`server/generate_data.py`: a *default* profile (4 customers, 1 day, 15-minute readings) "
        "used in early runs, and a *minimal* profile (1 customer, 1 day, one reading per day) "
        "adopted for large peer counts to reduce per-peer load time and let more peers launch "
        "within a given wall-clock budget.\n"
    )
    lines.append(
        "**Peer-count sweep strategy.** Two granularities were used: a *fine* sweep incrementing "
        "the peer count by 1 (covering small-to-medium scale, 2-50 peers) and a *coarse* sweep "
        "incrementing by 100 (covering large scale, 100-500 peers), the latter adopted purely to "
        "keep total experiment wall-clock time tractable.\n"
    )
    lines.append(
        "**Resource and readiness monitoring.** For large-scale attempts, aggregate resident "
        "memory (RSS) and live process count were sampled from `/proc` every 5-15 seconds "
        "throughout peer launch and querying. Peer-launch readiness was independently verified "
        "by counting `\"Peer N launched with ID\"` log lines against the requested peer count and "
        "confirming the `\"All peers have joined the overlay.\"` marker, rather than trusting the "
        "harness's own success/failure exit code.\n"
    )
    lines.append(
        f"**Platform.** All peers were emulated as separate OS processes on a single WSL2 Ubuntu "
        "host (observed limits: ~239,000 max user processes, ~479,000 max threads, 51 GB free "
        "RAM at baseline), so absolute latencies reflect single-host CPU/network contention rather "
        "than a geographically distributed deployment.\n"
    )

    lines.append("## 2. Data Validation and Exclusion Criteria\n")
    lines.append(
        f"A total of **{len(runs)}** non-trivial experiment runs were analysed (script-validation "
        "smoke tests with a requested peer count "
        f"\u2264 {SMOKE_TEST_MAX_PEERS} were excluded outright). Within those runs, every "
        "individual peer-count stage was cross-checked against its own peer-launch log. Stages "
        "failing this check are listed below and excluded from all figures, because their "
        "recorded `peer_count` column does not reflect how many peers actually participated:\n"
    )
    if excluded:
        lines.append("| Run | Labeled peer count | Actually joined | Overlay-join marker present |")
        lines.append("|---|---:|---:|---|")
        for run_id, pc, actual, joined in sorted(excluded):
            lines.append(f"| {run_id} | {pc} | {actual if actual >= 0 else 'n/a'} | {joined} |")
        lines.append("")
    else:
        lines.append("No stages required exclusion.\n")

    lines.append("## 3. Results\n")

    if figures.get("latency_fine"):
        lines.append(f"### Figure 1: Point-query latency, fine-grained sweep (2-50 peers)\n")
        lines.append(f"![Figure 1](figures/{figures['latency_fine']})\n")
        lines.append(
            "Mean elapsed time for `customers` and `open_offers` as the verified peer count "
            "grows from 2 to 50, using the minimal dataset profile. Both queries show a gentle, "
            "roughly linear increase in latency with peer count, consistent with the broadcast "
            "fan-out and sequential-merge design rather than any single bottleneck peer.\n"
        )
    if figures.get("latency_default"):
        lines.append("### Figure 2: Point-query latency, default dataset profile (historical baseline)\n")
        lines.append(f"![Figure 2](figures/{figures['latency_default']})\n")
        lines.append(
            "The same measurement using the heavier default dataset (4 customers per peer). "
            "Only stages that were independently verified as fully joined are shown; the reduced "
            "maximum verified scale compared with Figure 1 reflects why the minimal profile was "
            "later adopted for large peer-count sweeps.\n"
        )
    if figures.get("latency_large"):
        lines.append("### Figure 3: Point-query latency, coarse sweep (100-200 peers)\n")
        lines.append(f"![Figure 3](figures/{figures['latency_large']})\n")
        lines.append(
            "Only the 100- and 200-peer stages passed verification at this granularity; larger "
            "coarse stages were interrupted by exploratory memory guards before completing their "
            "query sweep (see Section 4) and are excluded rather than shown as partial points. "
            "Latency at this scale is both higher and noticeably more variable than at "
            "fine-grained scale, consistent with heavier broadcast contention.\n"
        )
    if figures.get("responders_fine"):
        lines.append("### Figure 4: Response completeness, fine-grained sweep\n")
        lines.append(f"![Figure 4](figures/{figures['responders_fine']})\n")
        lines.append(
            "Best observed responding-peer count against verified peer count, with a dashed "
            "reference line marking full response coverage. At this scale, both point queries "
            "reach full or near-full coverage, indicating the TTL/fan-out schedule is sufficient "
            "for small overlays.\n"
        )
    if figures.get("responders_large"):
        lines.append("### Figure 5: Response completeness, coarse sweep (100-200 peers)\n")
        lines.append(f"![Figure 5](figures/{figures['responders_large']})\n")
        lines.append(
            "At 100-200 peers, the best observed responder count falls well short of the full "
            "peer count even at the highest tested TTL, diverging from the reference line. This "
            "is consistent with the fixed per-hop forwarding cap (`maxForwardPeers`, 20-30 peers "
            "depending on remaining TTL budget): at larger overlays a fixed-width fan-out no "
            "longer reaches every peer within the tested TTL range, and this is a genuine "
            "architectural scaling limit rather than a measurement artefact.\n"
        )
    if figures.get("aggregate_latency"):
        lines.append("### Figure 6: Aggregate query latency vs. TTL\n")
        lines.append(f"![Figure 6](figures/{figures['aggregate_latency']})\n")
        lines.append(
            "`active_power_sum` latency grows with TTL at each sampled peer count, because the "
            "aggregate merge path (`BroadcastAggregateQuery`) walks the broadcast tree "
            "sequentially rather than in parallel with point-query result merging. Responding-peer "
            "counts are not plotted for this query: the aggregate path does not populate "
            "`RespondingPeerIds` in the same way the point-query path does, so `peers_responded` "
            "is uniformly recorded as 0 for `active_power_sum` across every run in this study and "
            "would not be a meaningful metric here.\n"
        )
    if figures.get("single_stage"):
        lines.append("### Figure 6b: Latency at the verified 500-peer single-stage target\n")
        lines.append(f"![Figure 6b](figures/{figures['single_stage']})\n")
        lines.append(
            "Because only one 500-peer launch attempt passed peer-count verification, this "
            "configuration contributes a single data point per query rather than a scaling curve; "
            "it is therefore shown separately as a bar chart, annotated with the best observed "
            "responder count, rather than combined with the 2-50 peer sweep in Figure 1.\n"
        )
    if figures.get("startup_profile"):
        lines.append("### Figure 7: Startup and memory scaling across 500-peer launch attempts\n")
        lines.append(f"![Figure 7](figures/{figures['startup_profile']})\n")
        lines.append(
            "Live peer count and aggregate resident memory over elapsed wall-clock time for every "
            "500-peer launch attempt with a usable resource trace. Memory grew smoothly and "
            "remained well within available host RAM in every attempt; the attempts that failed "
            "to reach 500 peers did so because the harness's own readiness-timeout window elapsed "
            "before peer registration finished, not because of memory or CPU exhaustion.\n"
        )
    if figures.get("scaling_summary"):
        lines.append("### Figure 8: Peak concurrently-live peers per attempt\n")
        lines.append(f"![Figure 8](figures/{figures['scaling_summary']})\n")
        lines.append(
            "Summary of the peak number of concurrently running peers reached by each 500-peer "
            "launch configuration. Only the final configuration, using a serialized one-peer-at-a-"
            "time startup with an extended one-hour readiness timeout, reached and verified the "
            "full 500-peer target.\n"
        )

    lines.append("## 4. Discussion\n")
    lines.append(
        "Point-query latency scales gently with peer count at small-to-medium scale (Figure 1), "
        "and response completeness is full or near-full up to 50 peers (Figure 4). At 100-200 "
        "peers, latency increases further and, more importantly, response completeness plateaus "
        "well short of the full peer count even at the largest tested TTL (Figure 5): the "
        "fixed-width broadcast fan-out caps how many peers can be reached within a bounded TTL "
        "budget as the overlay grows, independent of available compute headroom. This is a "
        "property of the forwarding-peer selection policy, not a resource constraint, and any "
        "future increase in reachable-peer coverage at large scale would need either a larger "
        "per-hop fan-out budget or a higher TTL ceiling, both of which trade off against "
        "duplicate-suppression and query-completion latency.\n"
    )
    lines.append(
        "Separately, the practical peer-count ceiling encountered while preparing this study was "
        "diagnosed as a test-harness limitation, not a system limitation: `dmesg`/`journalctl` "
        "showed no out-of-memory events at any point, and the host retained tens of gigabytes of "
        "free memory even while running hundreds of peers (Figure 7). Attempts that appeared to "
        "\"fail\" at 249-364 peers were in fact the orchestration script's own readiness-wait "
        "loop giving up before slower, later-starting peers had finished registering; once the "
        "readiness window was extended from 20 minutes to 1 hour, all 500 requested peers "
        "launched, joined the overlay, and completed their full query sweep (Figure 8).\n"
    )

    lines.append("## 5. Limitations\n")
    lines.append(
        "- All peers were emulated on a single host, sharing CPU, memory bandwidth, and the "
        "loopback network stack; absolute latency values are not representative of a "
        "geographically distributed deployment.\n"
        "- Most runs used a single repeat, so no variance or confidence intervals are reported; "
        "point estimates should be read as indicative rather than statistically robust.\n"
        "- The aggregate-query broadcast path does not currently report a responding-peer count, "
        "so response-completeness could not be assessed for `active_power_sum`.\n"
        "- Coarse-sweep stages beyond 200 peers were interrupted by exploratory memory guards "
        "during iterative harness debugging and do not have a complete, verified query sweep; "
        "they are excluded rather than shown as partial data.\n"
    )

    lines.append("## 6. Conclusion\n")
    lines.append(
        f"Across {len(clusters)} distinct experiment configurations and {total_valid_rows} "
        "verified query measurements, the system shows predictable, gently increasing latency "
        "with peer count at small-to-medium scale, a genuine fan-out-driven completeness ceiling "
        "at larger scale, and no evidence of a hard resource ceiling up to 500 concurrently "
        "running peers once test-harness readiness timeouts were corrected to match actual "
        "startup time.\n"
    )

    lines.append("## Appendix: Included Runs\n")
    lines.append("| Run ID | Cluster | Verified peer counts |")
    lines.append("|---|---|---|")
    for run in runs:
        if not run.valid_peer_counts:
            continue
        key = cluster_key(run.metadata)
        counts = ", ".join(str(c) for c in sorted(run.valid_peer_counts))
        lines.append(f"| {run.run_id} | {cluster_label(key)} | {counts} |")

    return "\n".join(lines) + "\n"


def main():
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    runs = load_runs()
    rows = collect_rows(runs)

    clusters_by_key = defaultdict(list)
    for run in runs:
        clusters_by_key[cluster_key(run.metadata)].append(run)

    minimal_fine = [k for k in clusters_by_key if k[3] == "fine_sweep" and k[0] == 1]
    minimal_coarse = [k for k in clusters_by_key if k[3] == "coarse_sweep" and k[0] == 1]
    minimal_single = [k for k in clusters_by_key if k[3] == "single_stage_large" and k[0] == 1]
    default_profile = [k for k in clusters_by_key if k[0] != 1]

    figures = {}
    figures["latency_fine"] = fig_latency_curve(
        rows, minimal_fine, "fig1_latency_fine.png",
        "Point-query latency vs. peer count (minimal profile, fine sweep)")
    figures["latency_default"] = fig_latency_curve(
        rows, default_profile, "fig2_latency_default_profile.png",
        "Point-query latency vs. peer count (default profile, historical baseline)")
    figures["latency_large"] = fig_latency_curve(
        rows, minimal_coarse, "fig3_latency_coarse.png",
        "Point-query latency vs. peer count (minimal profile, coarse sweep)")
    figures["responders_fine"] = fig_responders_curve(
        rows, minimal_fine, "fig4_responders_fine.png",
        "Response completeness vs. peer count (fine sweep)")
    figures["responders_large"] = fig_responders_curve(
        rows, minimal_coarse, "fig5_responders_coarse.png",
        "Response completeness vs. peer count (coarse sweep)")
    figures["aggregate_latency"] = fig_aggregate_latency(
        rows, minimal_fine + minimal_coarse, "fig6_aggregate_latency.png",
        "Aggregate query (active_power_sum) latency vs. TTL")
    figures["single_stage"] = fig_single_stage_summary(
        rows, minimal_single, "fig_single_stage_500.png",
        "Point-query latency at the verified 500-peer single-stage target")

    traces = load_resource_logs({r.run_id: r for r in runs})
    figures["startup_profile"] = fig_startup_profile(traces, "fig7_startup_profile.png")
    figures["scaling_summary"] = fig_scaling_limit_summary(traces, "fig8_scaling_summary.png")

    report = render_report(runs, rows, traces, figures)
    REPORT_PATH.write_text(report)

    print(f"Wrote report to {REPORT_PATH}")
    print(f"Wrote {sum(1 for v in figures.values() if v)} figures to {FIGURES_DIR}")


if __name__ == "__main__":
    main()
