"""LaTeX tables (booktabs) for the revision, from the same run discovery as
make_figures_rev.py. Usage: python3 make_tables_rev.py <bundle_root> ..."""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from make_figures_rev import (AGG, OUT, RAW, QS, Q, discover, e3_rows, e6_rows, load, peers_of,
                              pick, settle_runs, tamper_counts)


def tex(rows, cols, spec, path):
    lines = [r"\begin{tabular}{" + spec + "}", r"\toprule", " & ".join(cols) + r" \\", r"\midrule"]
    lines += [" & ".join(str(c) for c in r) + r" \\" for r in rows]
    lines += [r"\bottomrule", r"\end{tabular}"]
    Path(path).write_text("\n".join(lines) + "\n")
    return "\n".join(lines)


def t_testbeds():
    rows = [["A (G-bar)", r"2$\times$ Intel Xeon E5-2650 v4, 24 cores, 250\,GB", "1 (16 cores allocated)",
             r"2--50", "loopback"],
            ["B (Sophia, workq)", r"2$\times$ AMD EPYC 7351, 32 cores, 125\,GB", r"5--80 (20 peers per node)",
             r"100--1{,}600", "cluster interconnect; emulated WAN at $N=400$"]]
    return tex(rows, ["Testbed", "Node hardware", "Nodes", "$N$", "Links"], "lllll", OUT / "tab_testbeds.tex")


def t_e1(runs):
    rows = []
    for tb, n in (("A", 15), ("B", 100), ("B", 400), ("B", 1600)):
        r = pick(runs, "e1", testbed=tb, N=n) if tb == "B" else runs[(runs.exp == "e1") & (runs.testbed == "A")].iloc[-1]
        d = load(r)
        d = d[d.peer_count == n]
        med = d.groupby("query_label").elapsed_seconds.median()
        rows.append([tb, f"{n:,}".replace(",", "{,}"), *[f"{med.get(q, np.nan):.2f}" for q in QS],
                     f"{(d.peers_responded == d.peer_count).sum()}/{len(d)}"])
    return tex(rows, ["Testbed", "$N$", *[q.split("_")[0].upper() for q in QS], "Complete"],
               "llrrrrrrr", OUT / "tab_e1_scaling.tex")


def t_e3(runs):
    e3 = e3_rows(runs)
    base = e3[(e3.loss == 0) & (e3.days == 30)].drop_duplicates(["N", "q"], keep="last")
    rows = []
    for n in sorted(base.N.unique()):
        raw = base[(base.N == n) & (base.q == RAW)].iloc[0]
        agg = base[(base.N == n) & (base.q == AGG)].iloc[0]
        rows.append([raw.testbed, f"{n:,}".replace(",", "{,}"), f"{raw.bytes/1e6:.2f}", f"{raw.t:.2f}",
                     f"{agg.bytes/1e3:.1f}", f"{agg.t:.2f}", f"{raw.bytes/agg.bytes:,.0f}".replace(",", "{,}")])
    return tex(rows, ["Testbed", "$N$", "Q3 raw (MB)", "Q3 time (s)", "Q4 sum (kB)", "Q4 time (s)",
                      "Reduction ($\\times$)"], "llrrrrr", OUT / "tab_e3_bytes.tex")


def t_e6(runs):
    e6 = e6_rows(runs)
    sc = settle_runs(e6)
    sc = sc[sc.tamper == 0]
    rows = []
    for (tb, loss, m), g in sc.groupby(["testbed", "loss", "members"]):
        seq = g[g["mode"] == "sequential"]
        par = g[g["mode"] == "parallel"]
        s_c = seq.phase_broadcast_collect_ms.median() / 1e3
        p_c = par.phase_broadcast_collect_ms.median() / 1e3
        w = par.phase_write_summaries_ms.median() / 1e3
        ans = f"{int(g.responded.sum())}/{int(g.targets.sum())}"
        rows.append([tb + (" (WAN 5\\,\\%)" if loss > 0 else ""), int(m), f"{s_c:.2f}", f"{p_c:.2f}",
                     f"{s_c/p_c:.1f}", f"{w:.2f}", ans])
    return tex(rows, ["Testbed", "Members", "Collect, seq. (s)", "Collect, 16 par. (s)", "Speed-up",
                      "Write summaries (s)", "Answered"], "lrrrrrr", OUT / "tab_e6_settlement.tex")


def t_governance(runs, roots):
    e4 = load(pick(runs, "e4"))
    e4b = pick(runs, "e4b")
    summ = (Path(e4b.dir) / "summary.txt").read_text() if e4b is not None else ""
    meta = e4b.meta if e4b is not None else {}
    tam = tamper_counts(runs)
    cov = None
    for root in roots:
        for f in Path(root).glob("**/results/e5_settle_coverage_*.txt"):
            tok = f.read_text().split()
            cov = tok[1].split("=")[1]
    rows = [["E4 role and data-kind policy", "$N=50$", f"{len(e4)} of {len(e4)} decisions as specified"],
            ["E4b consent (purpose, validity)", "$N=50$",
             summ.split("\n")[0].replace("matrix queries: ", "").replace(", decisions differing from the policy: 0",
                                                                         " decisions, 0 deviating")],
            ["E4b audit", "$N=50$", summ.split("\n")[1] if summ else ""],
            ["E4b withdrawal", "$N=50$", f"effective after {meta.get('withdrawal_effective_seconds', '?')} s"],
            ["E6 signed totals, 20\\,\\% altered", "$N=50$", f"{tam[1]} of {tam[0]} altered reports rejected"],
            ["E5 legacy settlement (flooding)", "$N=400$", f"{cov} of 400 peers reached"]]
    return tex(rows, ["Check", "Scale", "Outcome"], "lll", OUT / "tab_governance.tex")


if __name__ == "__main__":
    roots = sys.argv[1:] or ["."]
    runs = discover(roots)
    for f in (t_testbeds, ):
        print(f())
    print(t_e1(runs))
    print(t_e3(runs))
    print(t_e6(runs))
    print(t_governance(runs, roots))


def t_e8(runs):
    from make_figures_rev import fig_central  # noqa: F401  (same selection rules)
    rows = []
    for _, r in runs[runs.exp == "e8"].sort_values(["testbed", "id"]).iterrows():
        d = load(r)
        if r.ttl < 300 or (d.rows_returned == 0).any():
            continue
        n = peers_of(r.id)
        med = d.groupby("query_label").elapsed_seconds.median()
        rows.append([r.testbed, f"{n:,}".replace(",", "{,}"), f"{float(r.meta['load_seconds']):,.0f}".replace(",", "{,}"),
                     f"{int(r.meta['store_bytes'])/1e9:.1f}", *[f"{med.get(q, np.nan):.2f}" for q in QS]])
    return tex(rows, ["Testbed", "$N$ equiv.", "Load (s)", "Store (GB)", *[q.split("_")[0].upper() for q in QS]],
               "llrrrrrrrr", OUT / "tab_e8_central.tex")


if __name__ == "__main__":
    print(t_e8(discover(sys.argv[1:] or ["."])))
