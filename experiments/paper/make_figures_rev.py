"""Revision figures and tables for the IDSS energy-community paper.

Usage: python3 make_figures_rev.py <bundle_root> [<bundle_root> ...]

Each bundle root holds results/<run_id>/ folders (as unpacked from the G-bar
and Sophia tarballs). Runs are discovered by experiment, size and emulation
settings (data_layout.txt / metadata.txt), not by hard-coded run IDs; when a
configuration was run more than once, the latest run is used. Testbed A =
G-bar (one node), Testbed B = Sophia (workq, 20 peers per node).

Figures are vector PDFs (+ PNG previews) at Applied Energy widths (90, 140,
190 mm), without titles or captions. Tables are booktabs LaTeX.
"""
import sys
from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

OUT = Path(__file__).parent / "out"  # created on first run
OUT.mkdir(exist_ok=True)
MM = 1 / 25.4
W1, W15, W2 = 90 * MM, 140 * MM, 190 * MM
C = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7"]
MK = ["o", "s", "^", "D", "v", "P"]
INK, INK2, GRID = "#1a1a1a", "#555555", "#e2e2e2"
plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 7.5, "axes.labelsize": 7.5,
    "xtick.labelsize": 7, "ytick.labelsize": 7, "legend.fontsize": 7,
    "axes.edgecolor": INK2, "axes.labelcolor": INK, "xtick.color": INK2,
    "ytick.color": INK2, "axes.grid": True, "grid.color": GRID,
    "grid.linewidth": 0.5, "axes.spines.top": False, "axes.spines.right": False,
    "axes.linewidth": 0.6, "figure.facecolor": "white", "axes.facecolor": "white",
    "legend.frameon": False, "lines.linewidth": 1.4, "lines.markersize": 4,
    "pdf.fonttype": 42, "savefig.dpi": 300,
})
Q = {"q1_customers": "Q1 customers", "q2_open_offers": "Q2 open offers",
     "q3_meter_readings_raw": "Q3 raw readings", "q4_active_power_sum": "Q4 in-network sum",
     "q5_customer_traversal": "Q5 traversal", "q6_prosumer_energy_sum": "Q6 traversal + sum"}
QS = list(Q)
TTL = [0.5, 1, 2, 3, 5, 8, 13, 20]
RAW, AGG = "q3_meter_readings_raw", "q4_active_power_sum"
MIN_ID = "20261015T1300"  # first run of the final campaign (990568f / 58f7b58)


# ------------------------------------------------------------------ discovery
def kv(path):
    out = {}
    if path.exists():
        for line in path.read_text().splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip()
    return out


def discover(roots):
    runs = []
    for root in roots:
        for d in sorted(Path(root).glob("**/results/2026*")):
            if not d.is_dir():
                continue
            csvs = list(d.glob("e*.csv"))
            if not csvs:
                continue
            name = d.name
            if name < MIN_ID:
                continue  # earlier campaign (code before 990568f)
            exp = name.split("-")[1]
            meta = {**kv(d / "metadata.txt"), **kv(d / "data_layout.txt")}
            runs.append(dict(dir=d, id=name, exp=exp, csv=csvs[0], meta=meta,
                             testbed="B" if "sophia" in str(d) else "A",
                             loss=float(meta.get("link_loss", 0) or 0),
                             tamper=float(meta.get("tamper_settlement", 0) or 0),
                             ttl=float(meta.get("ttl_seconds", 0) or 0)))
    return pd.DataFrame(runs)


def peers_of(run_id):
    for tok in run_id.split("-"):
        if tok.startswith("peers"):
            return int(tok[5:])
        if tok.startswith("central"):
            return int(tok[7:])
    return None


def pick(runs, exp, **cond):
    sel = runs[runs.exp == exp]
    for k, v in cond.items():
        if k == "N":
            sel = sel[sel.id.map(peers_of) == v]
        elif k == "days":
            sel = sel[sel.id.str.endswith(f"-d{v}")]
        else:
            sel = sel[sel[k] == v]
    if sel.empty:
        return None
    return sel.sort_values("id").iloc[-1]


def load(run):
    return pd.read_csv(run["csv"])


def save(fig, stem):
    fig.savefig(OUT / f"{stem}.pdf", bbox_inches="tight", pad_inches=0.02)
    fig.savefig(OUT / f"{stem}.png", bbox_inches="tight", pad_inches=0.02, dpi=200)
    plt.close(fig)


def panel(ax, text):
    ax.text(-0.02, 1.04, text, transform=ax.transAxes, fontsize=8, fontweight="bold",
            ha="right", va="bottom", color=INK)


def ttl_axis(ax):
    ax.set_xscale("log")
    ax.set_xticks(TTL)
    ax.set_xticklabels([f"{t:g}" for t in TTL])
    ax.minorticks_off()
    ax.set_xlabel("TTL budget (s)")


def completeness(d):
    d = d.copy()
    full = d.groupby("query_label").rows_returned.transform("max")
    d["completeness"] = d.rows_returned / full
    agg = d.query_label == AGG
    d.loc[agg, "completeness"] = d.loc[agg, "responded_fraction"]
    return d


# ------------------------------------------------------------------ E1 scaling
def fig_scaling(runs):
    sel = runs[runs.exp == "e1"]
    frames = []
    for _, r in sel.iterrows():
        d = load(r)
        d["testbed"] = r.testbed
        frames.append(d)
    d = pd.concat(frames)
    fig, axes = plt.subplots(1, 2, figsize=(W2, 64 * MM), gridspec_kw=dict(width_ratios=[1, 1.15]))
    for ax, tb in zip(axes, ("A", "B")):
        x = d[d.testbed == tb]
        for i, q in enumerate(QS):
            m = x[x.query_label == q].groupby("peer_count").elapsed_seconds
            med = m.median()
            if med.empty:
                continue
            ax.plot(med.index, med.values, color=C[i], marker=MK[i], markeredgecolor="white",
                    markeredgewidth=0.5, label=Q[q])
            ax.fill_between(med.index, m.quantile(0.25).values, m.quantile(0.75).values,
                            color=C[i], alpha=0.12, lw=0)
    ax = axes[0]
    ax.set_xlim(1.5, 15.5)
    ax.set_xticks([2, 4, 6, 8, 10, 12, 14])
    ax.set_ylim(0, 1.0)
    ax.set_xlabel("Community size N (peers), Testbed A")
    ax.set_ylabel("End-to-end time, median (s)")
    panel(ax, "(a)")
    ax = axes[1]
    ns = sorted(d[d.testbed == "B"].peer_count.unique())
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xticks(ns)
    ax.set_xticklabels([f"{n:,}" for n in ns])
    ax.set_yticks([0.2, 0.5, 1, 2, 5, 10, 20, 30])
    ax.set_yticklabels(["0.2", "0.5", "1", "2", "5", "10", "20", "30"])
    ax.set_ylim(0.18, 35)
    ax.minorticks_off()
    ax.set_xlabel("Community size N (peers), Testbed B")
    ax.set_ylabel("End-to-end time, median (s)")
    panel(ax, "(b)")
    h, l = axes[0].get_legend_handles_labels()
    fig.legend(h, l, loc="upper center", ncol=6, bbox_to_anchor=(0.5, 1.07), fontsize=6.5,
               handlelength=1.6, columnspacing=1.0)
    fig.tight_layout(w_pad=2.0)
    save(fig, "fig_scaling_e1")
    tab = d.groupby(["testbed", "peer_count", "query_label"]).agg(
        median=("elapsed_seconds", "median"), q1=("elapsed_seconds", lambda s: s.quantile(.25)),
        q3=("elapsed_seconds", lambda s: s.quantile(.75)), n=("elapsed_seconds", "size")).reset_index()
    d["ok"] = d.peers_responded == d.peer_count
    return tab, int((~d.ok).sum()), len(d)


# ------------------------------------------------------------------ E2 completeness and WAN
def fig_completeness(runs):
    sets = [(pick(runs, "e2", N=50, testbed="A"), "Testbed A, N = 50"),
            (pick(runs, "e2", N=400, testbed="B", loss=0.0), "Testbed B, N = 400"),
            (pick(runs, "e2", N=800, testbed="B", loss=0.0), "Testbed B, N = 800")]
    sets = [s for s in sets if s[0] is not None]
    fig, axes = plt.subplots(1, len(sets), figsize=(W2, 55 * MM), sharey=True)
    for ax, (r, title), lab in zip(np.atleast_1d(axes), sets, "abc"):
        d = completeness(load(r))
        for i, q in enumerate(QS[:5]):
            s = d[d.query_label == q].groupby("ttl").completeness.mean()
            ax.plot(s.index, s.values, color=C[i], marker=MK[i], markeredgecolor="white",
                    markeredgewidth=0.6, label=Q[q])
        ttl_axis(ax)
        ax.set_ylim(0, 1.04)
        ax.text(0.97, 0.05, title, transform=ax.transAxes, ha="right", fontsize=7.5, color=INK)
        panel(ax, f"({lab})")
    np.atleast_1d(axes)[0].set_ylabel("Data completeness (share of rows)")
    h, l = np.atleast_1d(axes)[0].get_legend_handles_labels()
    fig.legend(h, l, loc="upper center", ncol=5, bbox_to_anchor=(0.5, 1.07))
    fig.tight_layout(w_pad=1.0)
    save(fig, "fig_e2_completeness")


def fig_wan(runs):
    profiles = [(0.0, "cluster interconnect"), (0.05, "WAN: 20–200 ms RTT, 5 % loss, 20 Mbit/s"),
                (0.2, "WAN: 20–200 ms RTT, 20 % loss, 10 Mbit/s")]
    fig, axes = plt.subplots(1, 2, figsize=(W2, 60 * MM))
    styles = [("q1_customers", "-", 0), (RAW, "-", 2), (AGG, "--", 3)]
    for j, (loss, label) in enumerate(profiles):
        r = pick(runs, "e2", N=400, testbed="B", loss=loss)
        if r is None:
            continue
        d = completeness(load(r))
        for q, ls, ci in styles:
            x = d[d.query_label == q].groupby("ttl")
            kw = dict(color=C[ci], ls=ls, marker=MK[j], markeredgecolor="white", markeredgewidth=0.5,
                      alpha=[1.0, 0.75, 0.5][j])
            axes[0].plot(x.completeness.mean().index, x.completeness.mean().values,
                         label=f"{Q[q].split(' ')[0]}, {label}", **kw)
            axes[1].plot(x.elapsed_seconds.median().index, x.elapsed_seconds.median().values, **kw)
    ttl_axis(axes[0])
    axes[0].set_ylim(0, 1.04)
    axes[0].set_ylabel("Data completeness (share of rows)")
    panel(axes[0], "(a)")
    ttl_axis(axes[1])
    axes[1].set_yscale("log")
    axes[1].set_ylabel("End-to-end time, median (s)")
    ref, = axes[1].plot(TTL, TTL, color=INK2, lw=0.8, ls=":")
    axes[1].legend([ref], ["time = TTL"], loc="upper left", fontsize=6.5)
    panel(axes[1], "(b)")
    h, l = axes[0].get_legend_handles_labels()
    fig.legend(h, l, loc="upper center", ncol=3, bbox_to_anchor=(0.5, 1.16), fontsize=6.3)
    fig.tight_layout(w_pad=2.0)
    save(fig, "fig_e2_wan")


# ------------------------------------------------------------------ E3 bytes model
def e3_rows(runs):
    rows = []
    for _, r in runs[runs.exp == "e3"].iterrows():
        d = load(r)
        for q in (RAW, AGG):
            x = d[d.query_label == q]
            rows.append(dict(id=r.id, testbed=r.testbed, loss=r.loss, N=int(x.peer_count.iloc[0]),
                             days=int(x.days.iloc[0]), q=q, bytes=x.wire_bytes.median(),
                             t=x.elapsed_seconds.median(), tmin=x.elapsed_seconds.min(),
                             tmax=x.elapsed_seconds.max(), rows=int(x.rows_returned.median()),
                             peers=int(x.peers_responded.min())))
    return pd.DataFrame(rows)


def fig_e3(e3):
    base = e3[(e3.loss == 0) & (e3.days == 30)].sort_values("N")
    fig, axes = plt.subplots(1, 2, figsize=(W2, 58 * MM))
    ax = axes[0]
    for j, (q, col) in enumerate([(RAW, C[2]), (AGG, C[3])]):
        x = base[base.q == q].drop_duplicates("N", keep="last")
        ax.plot(x.N, x.bytes / 1e3, color=col, marker=MK[j + 2], markeredgecolor="white", label=Q[q])
    raw = base[base.q == RAW].drop_duplicates("N", keep="last")
    agg = base[base.q == AGG].drop_duplicates("N", keep="last")
    b_block = (raw.bytes / raw.rows).median()
    p_peer = np.polyfit(agg.N, agg.bytes, 1)
    ns = np.logspace(1, 4.2, 50)
    ax.plot(ns, ns * 300 * b_block / 1e3, color=C[2], lw=0.8, ls=":")
    ax.plot(ns, np.polyval(p_peer, ns) / 1e3, color=C[3], lw=0.8, ls=":")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("Community size N (peers), 30 days per peer")
    ax.set_ylabel("Data returned to requester (kB)")
    ax.legend(loc="upper left")
    ax.text(0.98, 0.04, f"dotted: model, {b_block:.0f} B per daily block;\n"
            f"aggregate {p_peer[1]:.0f} B + {p_peer[0]:.0f} B per peer",
            transform=ax.transAxes, ha="right", va="bottom", fontsize=6.3, color=INK2)
    panel(ax, "(a)")
    ax = axes[1]
    hz = e3[(e3.loss == 0) & (e3.N == 50)].drop_duplicates(["days", "q"], keep="last").sort_values("days")
    for j, (q, col) in enumerate([(RAW, C[2]), (AGG, C[3])]):
        x = hz[hz.q == q]
        ax.plot(x.days, x.t, color=col, marker=MK[j + 2], markeredgecolor="white", label=Q[q])
        for dd, v in zip(x.days, x.t):
            ax.text(dd, v * (1.3 if q == RAW else 1 / 1.3), f"{v:.1f} s", ha="center",
                    va="bottom" if q == RAW else "top", fontsize=6.3, color=INK)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xticks([1, 30, 365])
    ax.set_xticklabels(["1", "30", "365"])
    ax.minorticks_off()
    ax.set_xlim(0.6, 650)
    ax.set_ylim(0.2, 40)
    ax.set_xlabel("Horizon held by each peer (days), N = 50")
    ax.set_ylabel("End-to-end time, median (s)")
    panel(ax, "(b)")
    fig.tight_layout(w_pad=2.0)
    save(fig, "fig_e3_aggregation")
    return b_block, p_peer


# ------------------------------------------------------------------ E6 settlement
def e6_rows(runs):
    out = []
    sel = runs[runs.exp == "e6"].assign(N=lambda d: d.id.map(peers_of)).sort_values("id")
    # one run per configuration: the latest (the 18 Oct reruns supersede 15 Oct)
    sel = sel.drop_duplicates(["testbed", "N", "loss", "tamper"], keep="last")
    for _, r in sel.iterrows():
        d = load(r)
        d["run"], d["testbed"], d["loss"], d["tamper"] = r.id, r.testbed, r.loss, r.tamper
        d["N"] = peers_of(r.id)
        out.append(d)
    return pd.concat(out) if out else pd.DataFrame()


def settle_runs(e6):
    """One row per settlement actually run. The mode comes from the manager's
    summary (parallel=1 or 16), not the step label: at 799 members a sequential
    settlement outlasted the client's wait and its summary was logged under the
    next step."""
    sc = e6[e6.step.str.startswith("settle") & e6.phase_broadcast_collect_ms.notna()].copy()
    sc["mode"] = np.where(sc.parallel > 1, "parallel", "sequential")
    sc["members"] = sc.registered_members
    return sc


def fig_settlement(e6, tamper):
    sc = settle_runs(e6)
    sc = sc[(sc.tamper == 0) & (sc.testbed == "B")]
    fig, axes = plt.subplots(1, 2, figsize=(W2, 60 * MM), gridspec_kw=dict(width_ratios=[1.25, 1]))
    ax = axes[0]
    for j, (mode, label) in enumerate([("sequential", "sequential"), ("parallel", "16 in parallel")]):
        for k, loss in enumerate(sorted(sc.loss.unique())):
            x = sc[(sc["mode"] == mode) & (sc.loss == loss)]
            if x.empty:
                continue
            g = x.groupby("members").phase_broadcast_collect_ms
            lab = f"collect, {label}" + (", WAN 5 %" if loss > 0 else "")
            ax.plot(g.median().index, g.median().values / 1e3, color=C[j], marker=MK[j] if loss == 0 else "X",
                    ls="-" if loss == 0 else "none", markersize=4 if loss == 0 else 5,
                    markeredgecolor="white", markeredgewidth=0.5, label=lab)
    w = sc[(sc.loss == 0) & (sc["mode"] == "parallel")].groupby("members").phase_write_summaries_ms.median() / 1e3
    ax.plot(w.index, w.values, color=C[3], marker="D", ls=":", markeredgecolor="white", label="write summaries")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xticks([49, 399, 799])
    ax.set_xticklabels(["49", "399", "799"])
    ax.minorticks_off()
    ax.set_xlabel("Members per community (Testbed B)")
    ax.set_ylabel("Time per settlement, median (s)")
    ax.set_ylim(0.08, 3000)
    ax.legend(loc="upper left", fontsize=6.3, ncol=2, columnspacing=1.0)
    panel(ax, "(a)")
    ax = axes[1]
    cats = ["Registered\nmembers", "Routing-table\nprobe", "Impersonation\nprobe", "Altered\nreports"]
    probes = e6[(e6.testbed == "B") & (e6.tamper == 0) & (e6.loss == 0) & (e6.N == 400)]
    reg = settle_runs(probes)
    vals = [reg.responded.sum() / reg.targets.sum()]
    for step in ["probe_routing_table", "probe_claim_ec2"]:
        x = probes[probes.step == step]
        vals.append(x.responded.sum() / x.targets.sum())
    vals.append(tamper[1] / tamper[0] if tamper else np.nan)
    bars = ax.bar(range(4), vals, 0.6, color=[C[0], C[1], C[1], C[4]], edgecolor="white")
    for b, v, lab in zip(bars, vals, ["answered", "answered", "answered", "rejected"]):
        ax.text(b.get_x() + b.get_width() / 2, (v if v == v else 0) + 0.02, f"{v*100:.0f}%\n{lab}",
                ha="center", va="bottom", fontsize=6.2, color=INK)
    ax.set_xticks(range(4))
    ax.set_xticklabels(cats, fontsize=6.4)
    ax.set_ylim(0, 1.3)
    ax.set_yticks([0, 0.25, 0.5, 0.75, 1])
    ax.set_ylabel("Share of targeted peers / reports")
    ax.grid(axis="x", visible=False)
    panel(ax, "(b)")
    fig.tight_layout(w_pad=2.0)
    save(fig, "fig_e6_settlement")


def tamper_counts(runs):
    for _, r in runs[(runs.exp == "e6") & (runs.tamper > 0)].iterrows():
        f = Path(r.dir) / "tamper_check.txt"
        if f.exists():
            tok = f.read_text().split()
            return int(tok[tok.index("tampered:") + 1]), int(tok[tok.index("rejected:") + 1])
    return None


# ------------------------------------------------------------------ E7 churn
def fig_churn(runs):
    sel = runs[runs.exp == "e7"].sort_values("id")
    sel = sel.assign(N=sel.id.map(peers_of)).drop_duplicates(["testbed", "N"], keep="last")
    fig, axes = plt.subplots(1, 3, figsize=(W2, 56 * MM))
    for j, (_, r) in enumerate(sel.sort_values(["testbed", "N"]).iterrows()):
        d = load(r)
        d["reach"] = d.peers_responded / d.online_peers
        tb = f"N = {r.N} ({r.testbed})"
        for k, mode in enumerate(["silent", "reset"]):
            ls = "-" if mode == "silent" else "--"
            kw = dict(color=C[j], marker=MK[k], ls=ls, markeredgecolor="white", markeredgewidth=0.5)
            for ax, q in ((axes[0], AGG), (axes[1], RAW)):
                x = d[(d["mode"] == mode) & (d.query_label == q)].groupby("fraction").reach.mean()
                ax.plot(x.index * 100, x.values, label=f"{tb}, {mode}", **kw)
            y = d[(d["mode"] == mode) & (d.query_label == AGG)].groupby("fraction").elapsed_seconds.median()
            axes[2].plot(y.index * 100, y.values, **kw)
    for ax, lab, yl in ((axes[0], "(a)", "Online peers reached, Q4 sum"),
                        (axes[1], "(b)", "Online peers reached, Q3 raw")):
        ax.set_ylim(0, 1.05)
        ax.set_xticks([0, 10, 20, 30])
        ax.set_xlabel("Peers offline (%)")
        ax.set_ylabel(yl)
        panel(ax, lab)
    axes[0].text(0.5, 0.45, "every configuration: 1.00\n(lines overlap)", transform=axes[0].transAxes,
                 ha="center", fontsize=6.5, color=INK2)
    axes[2].set_xticks([0, 10, 20, 30])
    axes[2].set_xlabel("Peers offline (%)")
    axes[2].set_ylabel("Q4 time, median (s), TTL 5 s")
    axes[2].set_ylim(0, 6)
    panel(axes[2], "(c)")
    h, l = axes[0].get_legend_handles_labels()
    fig.legend(h, l, loc="upper center", ncol=3, bbox_to_anchor=(0.5, 1.13), fontsize=6.5)
    fig.tight_layout(w_pad=1.5)
    save(fig, "fig_e7_churn")


# ------------------------------------------------------------------ E8 central
def fig_central(runs):
    rows = []
    for _, r in runs[runs.exp == "e8"].iterrows():
        d = load(r)
        if r.ttl < 300 or (d.rows_returned == 0).any():
            continue  # 20 s budget cut off the scans (runs before 58f7b58)
        n = peers_of(r.id)
        for q, g in d.groupby("query_label"):
            rows.append(dict(N=n, q=q, mode="central", t=g.elapsed_seconds.median(), testbed=r.testbed))
    have = set()
    for _, r in runs[runs.exp == "e1"].iterrows():
        d = load(r)
        for (n, q), g in d.groupby(["peer_count", "query_label"]):
            rows.append(dict(N=n, q=q, mode="p2p", t=g.elapsed_seconds.median(), testbed=r.testbed))
            have.add((n, r.testbed))
    # sizes without an E1 run (A, N = 50): E2 at the same 20 s budget (Q1-Q5)
    for _, r in runs[(runs.exp == "e2") & (runs.loss == 0)].iterrows():
        d = load(r)
        d = d[d.ttl == 20]
        for (n, q), g in d.groupby(["peer_count", "query_label"]):
            if (n, r.testbed) not in have:
                rows.append(dict(N=n, q=q, mode="p2p", t=g.elapsed_seconds.median(), testbed=r.testbed))
    t = pd.DataFrame(rows)
    if t.empty or "central" not in set(t["mode"]):
        return None
    # compare with the peer-to-peer run on the same testbed
    t = t[t.set_index(["N", "testbed"]).index.isin(
        t[t["mode"] == "central"].set_index(["N", "testbed"]).index)]
    ns = sorted(set(map(tuple, t[t["mode"] == "central"][["testbed", "N"]].values)))
    if not ns:
        return None
    fig, axes = plt.subplots(1, len(ns), figsize=(W2, 55 * MM), sharey=True)
    for ax, (tb, n), lab in zip(np.atleast_1d(axes), ns, "abcd"):
        x = t[(t.N == n) & (t.testbed == tb)].pivot_table(index="q", columns="mode", values="t").reindex(QS)
        xs = np.arange(len(QS))
        ax.bar(xs - 0.2, x.get("p2p"), 0.38, color=C[0], label="peer-to-peer")
        ax.bar(xs + 0.2, x.get("central"), 0.38, color=C[1], label="central store")
        ax.set_xticks(xs)
        ax.set_xticklabels([q.split("_")[0].upper() for q in QS])
        ax.set_yscale("log")
        ax.set_title(f"Testbed {tb}, N = {n:,}", fontsize=7.5, color=INK)
        ax.grid(axis="x", visible=False)
        panel(ax, f"({lab})")
    np.atleast_1d(axes)[0].set_ylabel("End-to-end time, median (s)")
    h, l = np.atleast_1d(axes)[0].get_legend_handles_labels()
    fig.legend(h, l, loc="upper center", ncol=2, bbox_to_anchor=(0.5, 1.08))
    fig.tight_layout(w_pad=1.0)
    save(fig, "fig_e8_central")
    return t


if __name__ == "__main__":
    roots = sys.argv[1:] or ["."]
    runs = discover(roots)
    print(runs.groupby(["exp", "testbed"]).size().to_string())
    for old in OUT.glob("fig_*"):
        old.unlink()
    tab, bad, n = fig_scaling(runs)
    print(f"E1: {n} queries, {bad} incomplete")
    fig_completeness(runs)
    fig_wan(runs)
    e3 = e3_rows(runs)
    b, p = fig_e3(e3)
    print(f"E3 model: {b:.0f} B per block; aggregate {p[1]:.0f} B + {p[0]:.1f} B/peer")
    e6 = e6_rows(runs)
    if not e6.empty:
        fig_settlement(e6, tamper_counts(runs))
    fig_churn(runs)
    fig_central(runs)
    print("written:", sorted(p.name for p in OUT.glob("*.pdf")))
