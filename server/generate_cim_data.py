#!/usr/bin/env python3
"""
Synthetic, CIM-aligned MV cable asset and failure data for G-IDSS experiments.

Each DSO (optionally split into regions) becomes one peer partition that holds
ALL of its own data: Organisation, Substation, AssetContainer (cable system),
ACLineSegment (cable subsection), Junction (joint), FailureEvent and WorkOrder.
Public data are written to two separate partitions:
  public_digging  -> ActivityRecord nodes (digging-register-like)
  public_weather  -> MeasurementValue nodes (daily extreme-weather exceedances)

Calibration (Sundsgaard, PhD thesis, DTU 2025, Sec. 2.1-2.2, Figs. 2.4-2.10):
  * national MV cable network ~65,000 km; PILC ~25 % / XLPE ~75 % of length;
    PILC > 60 % in large cities;
  * ~80 % of PILC older than 30 years, ~80 % of XLPE younger than 30 years;
  * failure rate per 100 km and year, 2013-2023, split into age-related (incl.
    unknown cause) and third-party (mainly excavation) failures (Fig. 2.10,
    values read from the figure, hence approximate).
Assumptions NOT from the thesis are exposed as command-line parameters
(weather share, digging background rate, segment length, DSO count).

Outputs (in --out):
  peers/<partition>.json   EliasDB import format {"nodes": [...], "edges": [...]}
  baseline_all.json        union of all partitions (central baseline)
  ground_truth.json        exact answers to Q1-Q6, computed here (oracle)
  calibration.csv          generated vs target failure rate per year and type
  manifest.json            parameters, seed, partition sizes

Usage:
  python3 generate_cim_data.py --out ./data_s1 --scale 0.01 --dsos 6

Copyright 2023-2027, University of Salento, Italy. All rights reserved.
"""
import argparse
import csv
import json
import math
import os
import random
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone

# --------------------------------------------------------------------------
# Calibration tables (failures per 100 km per year), Sundsgaard 2025, Fig 2.10
# --------------------------------------------------------------------------
YEARS = list(range(2013, 2024))
LAMBDA = {
    ("PILC", "age"):        [1.70, 1.75, 1.85, 2.00, 1.90, 2.35, 2.30, 2.50, 2.90, 2.95, 3.90],
    ("PILC", "thirdparty"): [0.92, 0.82, 0.82, 0.77, 0.82, 0.68, 0.77, 0.77, 0.92, 0.72, 0.88],
    ("XLPE", "age"):        [0.30, 0.23, 0.20, 0.22, 0.22, 0.20, 0.28, 0.25, 0.30, 0.28, 0.33],
    ("XLPE", "thirdparty"): [0.10, 0.10, 0.11, 0.12, 0.13, 0.11, 0.12, 0.11, 0.10, 0.15, 0.15],
}
NATIONAL_KM = 65000.0
NATIONAL_PILC_SHARE = 0.25
URBAN_PILC_SHARE = 0.62
DK_BOX = (54.6, 57.7, 8.1, 12.7)              # lat_min, lat_max, lon_min, lon_max
URBAN_BOXES = [(55.60, 55.75, 12.45, 12.65),  # Copenhagen-like
               (56.10, 56.20, 10.10, 10.25)]  # Aarhus-like
KM_PER_DEG_LAT = 111.0
DIG_WINDOW_DAYS = 30      # digging active within 30 days before failure
WEATHER_WINDOW_DAYS = 3   # extreme weather within 3 days before failure
WEATHER_CELL_DEG = 0.25

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def epoch_days(d):
    return (d - EPOCH).days


def poisson(rng, lam):
    if lam <= 0:
        return 0
    if lam < 30:
        l, k, p = math.exp(-lam), 0, 1.0
        while True:
            p *= rng.random()
            if p <= l:
                return k
            k += 1
    return max(0, int(round(rng.gauss(lam, math.sqrt(lam)))))


def clip(x, lo, hi):
    return max(lo, min(hi, x))


def boxes_overlap(a, b):
    return not (a[1] < b[0] or b[1] < a[0] or a[3] < b[2] or b[3] < a[2])


class Partition:
    def __init__(self, name, kind):
        self.name, self.kind = name, kind
        self.nodes, self.edges = [], []

    def node(self, kind, key, **attrs):
        n = {"kind": kind, "key": key, "mRID": key}
        n.update(attrs)
        self.nodes.append(n)
        return n

    def edge(self, kind, k1, kind1, role1, k2, kind2, role2):
        self.edges.append({
            "key": f"{kind}-{k1[:8]}-{k2[:8]}-{len(self.edges)}", "kind": kind,
            "end1key": k1, "end1kind": kind1, "end1role": role1, "end1cascading": False,
            "end2key": k2, "end2kind": kind2, "end2role": role2, "end2cascading": False,
        })

    def dump(self, path):
        with open(path, "w") as f:
            json.dump({"nodes": self.nodes, "edges": self.edges}, f)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True)
    ap.add_argument("--scale", type=float, default=0.01, help="fraction of the national network (1.0 = ~65,000 km)")
    ap.add_argument("--dsos", type=int, default=6)
    ap.add_argument("--urban-dsos", type=int, default=2, help="DSOs with city-like PILC share (<= 2)")
    ap.add_argument("--urban-km-share", type=float, default=0.15, help="share of network length held by urban DSOs")
    ap.add_argument("--regions-per-dso", type=int, default=1, help="split each DSO into N regional peers")
    ap.add_argument("--mean-seg-km", type=float, default=0.4, help="[assumption] mean cable subsection length")
    ap.add_argument("--weather-share", type=float, default=0.10, help="[assumption] share of age failures preceded by extreme weather")
    ap.add_argument("--weather-bg", type=float, default=3.0, help="[assumption] background extreme days per weather cell and year")
    ap.add_argument("--digs-per-km-yr", type=float, default=0.1, help="[assumption] background digging activities per km of cable per year")
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()

    rng = random.Random(a.seed)
    ns = uuid.UUID("6f1c2a3e-0b7d-4c55-9a1e-0000000027aa")

    def mrid(label):
        return str(uuid.uuid5(ns, f"{a.seed}:{label}"))

    os.makedirs(os.path.join(a.out, "peers"), exist_ok=True)
    total_km = NATIONAL_KM * a.scale
    n_urban = min(a.urban_dsos, 2, a.dsos - 1)
    u_share = a.urban_km_share if n_urban else 0.0
    rural_pilc = clip((NATIONAL_PILC_SHARE - u_share * URBAN_PILC_SHARE) / (1 - u_share), 0, 1)

    # ---------------- DSO profiles and geography ----------------
    rural_n = a.dsos - n_urban
    w = [rng.uniform(0.6, 1.4) for _ in range(rural_n)]
    dsos = []
    for i in range(a.dsos):
        urban = i < n_urban
        if urban:
            km, pilc, box = total_km * u_share / n_urban, URBAN_PILC_SHARE, URBAN_BOXES[i]
        else:
            j = i - n_urban
            km, pilc = total_km * (1 - u_share) * w[j] / sum(w), rural_pilc
            lon_w = (DK_BOX[3] - DK_BOX[2]) / rural_n
            box = (DK_BOX[0], DK_BOX[1], DK_BOX[2] + j * lon_w, DK_BOX[2] + (j + 1) * lon_w)
        dsos.append(dict(idx=i, name=f"DSO-{i+1:02d}", urban=urban, km=km, pilc=pilc, box=box, mrid=mrid(f"dso{i}")))

    parts, segments = {}, []   # segments: global list for failure sampling
    for d in dsos:
        lat0, lat1, lon0, lon1 = d["box"]
        for r in range(a.regions_per_dso):
            pname = f"dso{d['idx']+1:02d}" + (f"_r{r+1}" if a.regions_per_dso > 1 else "")
            p = parts[pname] = Partition(pname, "dso")
            rw = (lon1 - lon0) / a.regions_per_dso
            rbox = (lat0, lat1, lon0 + r * rw, lon0 + (r + 1) * rw)
            p.node("Organisation", d["mrid"], name=d["name"], category="DSO", urban=d["urban"])
            region_km, built = d["km"] / a.regions_per_dso, 0.0
            prim = [p.node("Substation", mrid(f"{pname}-prim{k}"), name=f"{pname}-P{k}", substationType="primary",
                           voltageLevelKV="60/10", lat=rng.uniform(rbox[0], rbox[1]), lon=rng.uniform(rbox[2], rbox[3]))
                    for k in range(max(1, int(region_km / 40)))]
            for s in prim:
                p.edge("OPERATES", d["mrid"], "Organisation", "operator", s["key"], "Substation", "station")
            si = 0
            while built < region_km:
                is_pilc = rng.random() < d["pilc"]
                ins = "PILC" if is_pilc else "XLPE"
                year = int(round(clip(rng.gauss(1981, 13) if is_pilc else rng.gauss(2005, 11),
                                      1940 if is_pilc else 1970, 2005 if is_pilc else 2023)))
                sys_key = mrid(f"{pname}-sys{si}")
                sec = p.node("Substation", mrid(f"{pname}-sec{si}"), name=f"{pname}-S{si}", substationType="secondary",
                             voltageLevelKV="10/0.4", lat=rng.uniform(rbox[0], rbox[1]), lon=rng.uniform(rbox[2], rbox[3]))
                p.edge("OPERATES", d["mrid"], "Organisation", "operator", sec["key"], "Substation", "station")
                src = rng.choice(prim)
                p.node("AssetContainer", sys_key, name=f"{pname}-C{si}", containerType="MVCableSystem",
                       ratedVoltageKV=10 if rng.random() < 0.85 else 20, maxLoadingPct=round(rng.uniform(20, 95), 1))
                p.edge("OWNS", d["mrid"], "Organisation", "owner", sys_key, "AssetContainer", "asset")
                p.edge("CONNECTS", sys_key, "AssetContainer", "system", src["key"], "Substation", "fromStation")
                p.edge("CONNECTS", sys_key, "AssetContainer", "system", sec["key"], "Substation", "toStation")
                n_seg = rng.randint(1, 8)
                lat, lon, prev = sec["lat"], sec["lon"], None
                mat = "Cu" if rng.random() < (0.6 if is_pilc else 0.3) else "Al"
                for k in range(n_seg):
                    L = round(clip(rng.expovariate(1 / a.mean_seg_km), 0.02, 3.0), 3)
                    dlat = L / KM_PER_DEG_LAT / 2
                    dlon = dlat / math.cos(math.radians(lat))
                    lat, lon = clip(lat + rng.uniform(-2, 2) * dlat, rbox[0], rbox[1]), clip(lon + rng.uniform(-2, 2) * dlon, rbox[2], rbox[3])
                    bbox = (lat - dlat, lat + dlat, lon - dlon, lon + dlon)
                    sk = mrid(f"{pname}-sys{si}-seg{k}")
                    seg = p.node("ACLineSegment", sk, name=f"{pname}-C{si}-L{k}", insulation=ins, conductorMaterial=mat,
                           conductorSizeMm2=rng.choice([50, 95, 150, 240]), numberOfConductors=rng.choice([1, 3]),
                           lengthKm=L, inServiceYear=year + rng.randint(0, 2), manufacturer=f"MFR-{rng.choice('ABCDEF')}",
                           lat=round(lat, 6), lon=round(lon, 6), minLat=bbox[0], maxLat=bbox[1], minLon=bbox[2], maxLon=bbox[3])
                    p.edge("CONTAINS", sys_key, "AssetContainer", "container", sk, "ACLineSegment", "segment")
                    if prev:
                        jk = mrid(f"{pname}-sys{si}-joint{k}")
                        p.node("Junction", jk, name=f"{pname}-C{si}-J{k}", jointType=rng.choice(["straight", "transition", "branch"]),
                               installYear=year)
                        p.edge("JOINS", jk, "Junction", "joint", prev, "ACLineSegment", "segment")
                        p.edge("JOINS", jk, "Junction", "joint", sk, "ACLineSegment", "segment")
                    prev = sk
                    segments.append(dict(node=seg, part=pname, dso=d["name"], urban=d["urban"], bbox=bbox))
                    built += L
                si += 1

    # ---------------- failures (calibrated) ----------------
    dig = Partition("public_digging", "public")
    wx = Partition("public_weather", "public")
    failures, calib = [], []
    for yi, y in enumerate(YEARS):
        for ins in ("PILC", "XLPE"):
            pool = [s for s in segments if s["node"]["insulation"] == ins and s["node"]["inServiceYear"] <= y]
            L_T = sum(s["node"]["lengthKm"] for s in pool)
            for cause in ("age", "thirdparty"):
                lam = LAMBDA[(ins, cause)][yi]
                n = poisson(rng, lam * L_T / 100.0)
                if not pool or n == 0:
                    calib.append((y, ins, cause, lam, 0, L_T))
                    continue
                if cause == "age":
                    wts = [s["node"]["lengthKm"] * (((y - s["node"]["inServiceYear"] + 1) / 40.0) ** (2.5 if ins == "PILC" else 1.0))
                           for s in pool]
                else:
                    wts = [s["node"]["lengthKm"] * (2.0 if s["urban"] else 1.0) for s in pool]
                for s in rng.choices(pool, weights=wts, k=n):
                    day = datetime(y, 1, 1, tzinfo=timezone.utc) + timedelta(days=rng.randrange(365))
                    failures.append(dict(seg=s, date=day, cause=cause))
                calib.append((y, ins, cause, lam, n, L_T))

    for i, f in enumerate(failures):
        s, p = f["seg"], parts[f["seg"]["part"]]
        fk, wk = mrid(f"fail{i}"), mrid(f"wo{i}")
        f["key"] = fk
        p.node("FailureEvent", fk, failureDate=f["date"].strftime("%Y-%m-%d"), failureDay=epoch_days(f["date"]),
               failureYear=f["date"].year, faultCause="third-party" if f["cause"] == "thirdparty" else "ageing/unknown",
               customersInterrupted=rng.randint(10, 900), interruptionMinutes=rng.randint(5, 240))
        p.edge("AFFECTS", fk, "FailureEvent", "failure", s["node"]["key"], "ACLineSegment", "asset")
        p.node("WorkOrder", wk, workType="repair", repairHours=round(rng.uniform(2, 48), 1),
               repairKind=rng.choice(["joint", "section replacement", "termination"]))
        p.edge("REPAIRS", wk, "WorkOrder", "workorder", fk, "FailureEvent", "failure")
        # planted public events
        if f["cause"] == "thirdparty":
            b, start = s["bbox"], f["date"] - timedelta(days=rng.randint(0, 20))
            dlat = rng.uniform(0.0002, 0.0015)
            dig.node("ActivityRecord", mrid(f"dig-p{i}"), activityType="digging", reason=rng.choice(["fibre", "water", "district heating", "road"]),
                     startDay=epoch_days(start), endDay=epoch_days(start) + rng.randint(1, 60),
                     minLat=(b[0] + b[1]) / 2 - dlat, maxLat=(b[0] + b[1]) / 2 + dlat, minLon=(b[2] + b[3]) / 2 - dlat * 1.7, maxLon=(b[2] + b[3]) / 2 + dlat * 1.7)
        elif rng.random() < a.weather_share:
            cell = (math.floor(s["node"]["lat"] / WEATHER_CELL_DEG), math.floor(s["node"]["lon"] / WEATHER_CELL_DEG))
            dd = epoch_days(f["date"]) - rng.randint(0, WEATHER_WINDOW_DAYS)
            wx.node("MeasurementValue", mrid(f"wx-p{i}"), measurementType=rng.choice(["lightning", "storm", "heatwave", "frost"]),
                    day=dd, minLat=cell[0] * WEATHER_CELL_DEG, maxLat=(cell[0] + 1) * WEATHER_CELL_DEG,
                    minLon=cell[1] * WEATHER_CELL_DEG, maxLon=(cell[1] + 1) * WEATHER_CELL_DEG)

    # background public events (no planted relation to failures)
    d0, d1 = epoch_days(datetime(YEARS[0], 1, 1, tzinfo=timezone.utc)), epoch_days(datetime(YEARS[-1], 12, 31, tzinfo=timezone.utc))
    for i in range(poisson(rng, a.digs_per_km_yr * total_km * len(YEARS))):
        s = rng.choice(segments)
        lat = s["node"]["lat"] + rng.uniform(-0.01, 0.01)
        lon = s["node"]["lon"] + rng.uniform(-0.016, 0.016)
        dlat, st = rng.uniform(0.0002, 0.0015), rng.randint(d0, d1)
        dig.node("ActivityRecord", mrid(f"dig-b{i}"), activityType="digging", reason=rng.choice(["fibre", "water", "district heating", "road"]),
                 startDay=st, endDay=st + rng.randint(1, 60), minLat=lat - dlat, maxLat=lat + dlat, minLon=lon - dlat * 1.7, maxLon=lon + dlat * 1.7)
    cells = {(math.floor(s["node"]["lat"] / WEATHER_CELL_DEG), math.floor(s["node"]["lon"] / WEATHER_CELL_DEG)) for s in segments}
    k = 0
    for c in sorted(cells):
        for _ in range(poisson(rng, a.weather_bg * len(YEARS))):
            wx.node("MeasurementValue", mrid(f"wx-b{k}"), measurementType=rng.choice(["lightning", "storm", "heatwave", "frost"]),
                    day=rng.randint(d0, d1), minLat=c[0] * WEATHER_CELL_DEG, maxLat=(c[0] + 1) * WEATHER_CELL_DEG,
                    minLon=c[1] * WEATHER_CELL_DEG, maxLon=(c[1] + 1) * WEATHER_CELL_DEG)
            k += 1
    parts[dig.name], parts[wx.name] = dig, wx

    # ---------------- ground truth (oracle, same definitions as the queries) ----------------
    digs = [(n["minLat"], n["maxLat"], n["minLon"], n["maxLon"], n["startDay"], n["endDay"]) for n in dig.nodes]
    wxs = [(n["minLat"], n["maxLat"], n["minLon"], n["maxLon"], n["day"]) for n in wx.nodes]
    dig_by_cell = defaultdict(list)
    for g in digs:
        for ci in range(math.floor(g[0] * 100), math.floor(g[1] * 100) + 1):
            for cj in range(math.floor(g[2] * 100), math.floor(g[3] * 100) + 1):
                dig_by_cell[(ci, cj)].append(g)

    def dig_related(f):
        b, fd = f["seg"]["bbox"], epoch_days(f["date"])
        seen = set()
        for ci in range(math.floor(b[0] * 100), math.floor(b[1] * 100) + 1):
            for cj in range(math.floor(b[2] * 100), math.floor(b[3] * 100) + 1):
                for g in dig_by_cell.get((ci, cj), ()):
                    if id(g) in seen:
                        continue
                    seen.add(id(g))
                    if boxes_overlap(b, g[:4]) and g[4] <= fd and g[5] >= fd - DIG_WINDOW_DAYS:
                        return True
        return False

    wx_by_cell = defaultdict(list)
    for e in wxs:
        wx_by_cell[(math.floor(e[0] / WEATHER_CELL_DEG + 1e-9), math.floor(e[2] / WEATHER_CELL_DEG + 1e-9))].append(e[4])

    def wx_related(f):
        n, fd = f["seg"]["node"], epoch_days(f["date"])
        c = (math.floor(n["lat"] / WEATHER_CELL_DEG), math.floor(n["lon"] / WEATHER_CELL_DEG))
        return any(fd - WEATHER_WINDOW_DAYS <= d <= fd for d in wx_by_cell.get(c, ()))

    gt = {"definitions": {
        "Q1": "failures per year (all DSOs) and per DSO and year",
        "Q2": "DSOs ranked by total failures 2013-2023",
        "Q3": "failures per DSO and faultCause",
        "Q4": "failure rate per 100 km and year by insulation and installation decade = 100*failures/km_years",
        "Q5": f"failures whose segment bbox overlaps a digging bbox active within {DIG_WINDOW_DAYS} days before the failure",
        "Q6": f"failures whose segment midpoint lies in a weather cell with an extreme event within {WEATHER_WINDOW_DAYS} days before the failure"},
        "Q1": defaultdict(int), "Q1_dso": defaultdict(int), "Q2": defaultdict(int), "Q3": defaultdict(int),
        "Q4_failures": defaultdict(int), "Q4_km_years": defaultdict(float), "Q5": defaultdict(int), "Q6": defaultdict(int)}
    for f in failures:
        dso, n = f["seg"]["dso"], f["seg"]["node"]
        dec = f"{n['inServiceYear'] // 10 * 10}s"
        gt["Q1"][str(f["date"].year)] += 1
        gt["Q1_dso"][f"{dso}|{f['date'].year}"] += 1
        gt["Q2"][dso] += 1
        gt["Q3"][f"{dso}|{'third-party' if f['cause'] == 'thirdparty' else 'ageing/unknown'}"] += 1
        gt["Q4_failures"][f"{n['insulation']}|{dec}"] += 1
        if dig_related(f):
            gt["Q5"][dso] += 1
        if wx_related(f):
            gt["Q6"][dso] += 1
    for s in segments:
        n = s["node"]
        yrs = sum(1 for y in YEARS if n["inServiceYear"] <= y)
        gt["Q4_km_years"][f"{n['insulation']}|{n['inServiceYear'] // 10 * 10}s"] += n["lengthKm"] * yrs
    gt["Q4_rate_per_100km_yr"] = {k: round(100 * gt["Q4_failures"].get(k, 0) / v, 4)
                                   for k, v in gt["Q4_km_years"].items() if v > 0}
    gt["Q2"] = sorted(gt["Q2"].items(), key=lambda kv: (-kv[1], kv[0]))
    with open(os.path.join(a.out, "ground_truth.json"), "w") as fh:
        json.dump(gt, fh, indent=1, sort_keys=True)

    with open(os.path.join(a.out, "calibration.csv"), "w", newline="") as fh:
        wr = csv.writer(fh)
        wr.writerow(["year", "insulation", "cause", "target_lambda_per100km", "failures", "km_in_service", "generated_lambda_per100km"])
        for y, ins, cause, lam, n, L in calib:
            wr.writerow([y, ins, cause, lam, n, round(L, 2), round(100 * n / L, 3) if L else ""])

    # ---------------- write partitions + baseline ----------------
    allp = Partition("baseline_all", "central")
    seen_org = set()
    for p in parts.values():
        p.dump(os.path.join(a.out, "peers", f"{p.name}.json"))
        for n in p.nodes:
            if n["kind"] == "Organisation":
                if n["key"] in seen_org:
                    continue
                seen_org.add(n["key"])
            allp.nodes.append(n)
        allp.edges.extend(p.edges)
    allp.dump(os.path.join(a.out, "baseline_all.json"))

    manifest = {"params": vars(a), "total_km_target": total_km, "rural_pilc_share": round(rural_pilc, 4),
                "built_km": round(sum(s["node"]["lengthKm"] for s in segments), 2),
                "pilc_km_share": round(sum(s["node"]["lengthKm"] for s in segments if s["node"]["insulation"] == "PILC")
                                       / sum(s["node"]["lengthKm"] for s in segments), 4),
                "failures": len(failures),
                "partitions": {p.name: {"kind": p.kind, "nodes": len(p.nodes), "edges": len(p.edges)} for p in parts.values()},
                "dsos": [{k: v for k, v in d.items() if k != "box"} | {"box": list(d["box"])} for d in dsos]}
    with open(os.path.join(a.out, "manifest.json"), "w") as fh:
        json.dump(manifest, fh, indent=1)
    print(json.dumps({k: manifest[k] for k in ("built_km", "pilc_km_share", "failures")}),
          {k: v["nodes"] for k, v in manifest["partitions"].items()})


if __name__ == "__main__":
    main()
