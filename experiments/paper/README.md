# Paper figures and tables (revision campaign)

Rebuild every figure and table of the energy-community paper from the result
bundles (one `results/<run_id>/` folder per run, as copied from the clusters):

    python3 make_figures_rev.py <bundle_root> [<bundle_root> ...]
    python3 make_tables_rev.py  <bundle_root> [<bundle_root> ...]

A bundle whose path contains `sophia` is Testbed B, anything else Testbed A.
Runs older than `MIN_ID` (code before 990568f) are ignored; if a configuration
ran more than once, the latest run is used. Output goes to `out/`: vector PDFs
with PNG previews (no titles or captions) and booktabs LaTeX tables.
Needs Python 3 with pandas, numpy and matplotlib.
