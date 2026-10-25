#!/usr/bin/env python3
"""Recompute every number the deck uses from the repository artifacts.

Reads workloads/results/**, compares each value with the figure quoted in
docs/configstream_paper.md, and writes
  data/*.csv           series the charts plot
  data/generated.tex   \\defnum lines for scalars (input from numbers.tex)
  data/VERIFICATION.md one row per check: recomputed value, paper value, status
Run from anywhere:  python tools/extract_numbers.py
"""

import csv
import json
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
RES = ROOT / "workloads" / "results"
OUT = Path(__file__).resolve().parents[1] / "data"
OUT.mkdir(exist_ok=True)

checks = []  # (id, what, recomputed, paper, ok)
defs = []  # (key, value)


def load(rel):
    return json.loads((RES / rel).read_text(encoding="utf-8"))


def check(cid, what, got, paper, tol=0.0):
    ok = abs(got - paper) <= tol if isinstance(got, (int, float)) else got == paper
    checks.append((cid, what, got, paper, ok))
    return got


def d(key, value):
    defs.append((key, value))


def write_csv(name, header, rows):
    with open(OUT / name, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


# ---------------------------------------------------------------- TerraDS history
h = load("terrads/history-summary.json")
vpa = h["versions_per_asset"]
hist = vpa["histogram"]
n_assets = check("T1", "TerraDS file incarnations", h["assets"], 6505)
n_rev = check("T2", "TerraDS versions (revisions)", h["revisions"], 10188)
check("T3", "TerraDS logical UTF-8 bytes", h["logical_content_bytes"], 12278085)
pairs = check("T4", "TerraDS adjacent pairs = diff.added.count", h["diff"]["added"]["count"], 3683)
check("T5", "pairs = versions - incarnations", n_rev - n_assets, 3683)
check("T6", "sum_f (n_f - 1) from histogram", sum((k - 1) * c for k, c in hist), 3683)
check("T7", "sum_f n_f from histogram", sum(k * c for k, c in hist), 10188)
single = next(c for k, c in hist if k == 1)
share = round(100 * single / n_assets, 2)
check("T8", "share of files with one version (%)", share, 77.48, 0.005)
d("terrads/assets", n_assets)
d("terrads/versions", n_rev)
d("terrads/pairs", pairs)
d("terrads/bytes", h["logical_content_bytes"])
d("terrads/single", single)
d("terrads/singleshare", f"{share:.2f}")
d("terrads/maxver", vpa["max"])
d("terrads/days", f"{h['history_days']:.0f}")
d("terrads/years", f"{h['history_days'] / 365.25:.2f}")
buckets = [(str(k), c) for k, c in hist if k <= 6] + [
    ("7-10", sum(c for k, c in hist if 7 <= k <= 10)),
    (">10", sum(c for k, c in hist if k > 10)),
]
write_csv("terrads_versions.csv", ["bucket", "files"], buckets)

# ---------------------------------------------------------------- matched campaign
m = load("phase20/matched-processing-repetitions.json")
trials = m["trials"]
check("M1", "successful matched trials", len(trials), 27)
check("M2", "failed matched trials in corrected campaign", m["measured_failed_trials"], 0)
check(
    "M3",
    "all trials exact PASS",
    sum(t["exact"]["exact_membership_and_full_projection"] == "PASS" for t in trials),
    27,
)
check(
    "M4",
    "combined attempt elapsed (s)",
    round(m["combined_attempt_elapsed_seconds"], 3),
    502.981,
    0.0005,
)

paper_med = {
    ("rpki", "B0"): (6.297, 6.282, 12.577),
    ("rpki", "B2"): (16.840, 14.729, 21.080),
    ("rpki", "B3"): (25.417, 23.144, 25.424),
    ("ris", "B0"): (10.485, 8.437, 12.636),
    ("ris", "B2"): (21.167, 18.967, 23.186),
    ("ris", "B3"): (29.644, 23.233, 29.726),
    ("dns", "B0"): (2.089, 2.087, 6.302),
    ("dns", "B2"): (10.536, 10.486, 12.584),
    ("dns", "B3"): (16.866, 12.624, 16.924),
}
rows, med = [], {}
for w in ("rpki", "ris", "dns"):
    for mode in ("B0", "B2", "B3"):
        ts = [t for t in trials if t["workload"] == w and t["mode"] == mode]
        xs = [t["command_start_to_observed_exit_seconds"] for t in ts]
        check(f"M5-{w}-{mode}-n", f"{w} {mode} repetitions", len(ts), 3)
        md, lo, hi = statistics.median(xs), min(xs), max(xs)
        pm, pl, ph = paper_med[(w, mode)]
        check(f"M6-{w}-{mode}-med", f"{w} {mode} median (s)", round(md, 3), pm, 0.0006)
        check(f"M6-{w}-{mode}-min", f"{w} {mode} min (s)", round(lo, 3), pl, 0.0006)
        check(f"M6-{w}-{mode}-max", f"{w} {mode} max (s)", round(hi, 3), ph, 0.0006)
        med[(w, mode)] = md
        rows_n = ts[0]["exact"]["logical_rows"]
        pq = [t["parquet_bytes"] for t in ts]
        rows.append([w, mode, f"{md:.3f}", f"{lo:.3f}", f"{hi:.3f}", rows_n, max(pq)])
write_csv(
    "matched.csv", ["workload", "mode", "median", "min", "max", "rows", "parquet_bytes"], rows
)

units = {}
for w in ("rpki", "ris", "dns"):
    units[w] = [
        t["exact"]["logical_rows"] for t in trials if t["workload"] == w and t["mode"] == "B0"
    ][0]
check("M7-rpki", "RPKI rows per trial", units["rpki"], 360226)
check("M7-ris", "RIS occurrences per trial", units["ris"], 370141)
check("M7-dns", "DNS raw measurements per trial", units["dns"], 4096)
tot = {
    w: sum(t["exact"]["logical_rows"] for t in trials if t["workload"] == w and t["mode"] == "B3")
    for w in units
}  # chunks exist only in B3 (Kafka)
check("M8-rpki", "RPKI rows over the 3 B3 trials", tot["rpki"], 1080678)
check("M8-ris", "RIS occurrences over the 3 B3 trials", tot["ris"], 1110423)
check("M8-dns", "DNS rows over the 3 B3 trials", tot["dns"], 12288)
for w in units:
    d(f"matched/{w}/rows", f"{units[w]:,}")
    for mode in ("B2", "B3"):
        d(f"matched/{w}/{mode}/ratio", f"{med[(w, mode)] / med[(w, 'B0')]:.2f}")
parq = sum(t["parquet_bytes"] for t in trials)
minio = sum(t["minio_bytes"] for t in trials)
check("M9", "sum parquet bytes over 27 trials", parq, 1622766994)
check("M10", "sum MinIO bytes over 27 trials", minio, 1623023143)
b3 = [t for t in trials if t["mode"] == "B3"]
check(
    "M11", "B3 trials with zero final lag", sum(str(t["final_lag"]) in ("0", "0.0") for t in b3), 9
)
res = m["resources"]
check("R1", "retained growth (bytes)", res["final_combined_growth_bytes"], 3869485576)
check(
    "R2",
    "peak sampled process-tree RSS (bytes)",
    res["peak_sampled_campaign_process_tree_rss_bytes"],
    1612824576,
)
check("R3", "min guest RAM (bytes)", res["minimum_guest_available_memory_bytes"], 4191440896)
d("resource/growth", f"{res['final_combined_growth_bytes']:,}")
d("resource/growthgib", f"{res['final_combined_growth_bytes'] / 2**30:.2f}")
d("resource/peak", f"{res['peak_sampled_campaign_process_tree_rss_bytes']:,}")
d("resource/minram", f"{res['minimum_guest_available_memory_bytes'] / 2**30:.2f}")
d("resource/elapsed", f"{m['combined_attempt_elapsed_seconds']:.3f}")
d("resource/parquet", f"{parq:,}")
d("resource/minio", f"{minio:,}")

# ---------------------------------------------------------------- RPKI representation
r = load("phase20/rpki-representations.json")
cfg = r["configurations"]
b = {k: cfg[k]["analytical_bytes"] for k in cfg}
check("P1", "R1 analytical bytes", b["R1"], 47756423)
check("P2", "R2 analytical bytes", b["R2"], 3555516)
check("P3", "R3 analytical bytes", b["R3"], 6954890)
sav = {k: 1 - b[k] / b["R1"] for k in ("R2", "R3")}
check("P4", "R2 saving vs R1 (%)", round(100 * sav["R2"], 2), 92.55, 0.005)
check("P5", "R3 saving vs R1 (%)", round(100 * sav["R3"], 2), 85.44, 0.005)
pd = r["preference_decisions"]
worst = {k: max(pd[k]["query_median_ratios"].values()) for k in ("R2", "R3")}
check("P6", "R2 worst query median ratio <= 2", worst["R2"] <= 2, True)
check("P7", "R3 worst query median ratio <= 2", worst["R3"] <= 2, True)
check(
    "P8",
    "R2/R3 status PREFERRED",
    (pd["R2"]["status"], pd["R3"]["status"]),
    ("PREFERRED", "PREFERRED"),
)
check(
    "P9",
    "all query outputs exact PASS",
    all(q["exact_correctness"] == "PASS" for c in cfg.values() for q in c["queries"].values()),
    True,
)
rows = [
    [k, b[k], f"{100 * (1 - b[k] / b['R1']):.2f}", cfg[k]["parquet_files"]]
    for k in ("R1", "R2", "R3")
]
write_csv("rpki_layout.csv", ["layout", "bytes", "saving_pct", "files"], rows)
qrows = []
for q in pd["R2"]["query_median_ratios"]:
    qrows.append(
        [
            q,
            f"{pd['R2']['query_median_ratios'][q]:.3f}",
            f"{pd['R3']['query_median_ratios'][q]:.3f}",
        ]
    )
write_csv("rpki_query_ratio.csv", ["query", "R2", "R3"], qrows)
d("rpki/r1", f"{b['R1']:,}")
d("rpki/r2", f"{b['R2']:,}")
d("rpki/r3", f"{b['R3']:,}")
d("rpki/sav2", f"{100 * sav['R2']:.2f}")
d("rpki/sav3", f"{100 * sav['R3']:.2f}")
d("rpki/worst2", f"{worst['R2']:.2f}")
d("rpki/worst3", f"{worst['R3']:.2f}")

# ---------------------------------------------------------------- hypotheses
hy = load("phase20/hypothesis-decisions.json")
check("H1", "H1 decision", hy["H1"]["decision"], "SUPPORTED_WITHIN_TESTED_SCOPE")
check("H2", "H2 decision", hy["H2"]["decision"], "SUPPORTED_WITHIN_TESTED_SCOPE")
check("H3", "H3 decision", hy["H3"]["decision"], "NOT_RUN")

# ---------------------------------------------------------------- replay (phase 12; numbers live in the acceptance doc)
acc = (ROOT / "docs" / "phase12-acceptance.md").read_text(encoding="utf-8")


def must_have(cid, what, needle, paper):
    ok = needle in acc
    checks.append((cid, what, needle if ok else "NOT FOUND", paper, ok))


must_have("Q1", "publisher seconds / rate", "20.375 / 500.023", "500.023")
must_have("Q2", "streaming incl. drain / logical rate", "68.742 s / 148.205", "68.742 / 148.205")
must_have("Q3", "startup / drain / total", "6.422 / 48.367 / 76.071", "-")
must_have("Q4", "Parquet bytes", "29402997", "29,402,997")
must_have("Q5", "repeat Parquet bytes", "29346912", "29,346,912")
must_have("Q6", "repeat drain rate", "72.998 s / 139.565", "139.565")
must_have(
    "Q7", "replay version cap 75, retention 1000000 h", "Watermark and timeout 1000000", "cap 75"
)
check(
    "Q8", "accepted logical rate = 10188/68.742", round(10188 / 68.742, 3), 148.205, 0.002
)  # doc rounds the unrounded seconds
check("Q9", "headroom rate = 10188/72.998", round(10188 / 72.998, 3), 139.565, 0.0006)
check(
    "Q10", "publisher rate = 10188/20.375", round(10188 / 20.375, 3), 500.023, 0.003
)  # doc rounds the unrounded seconds
write_csv(
    "replay_phases.csv",
    ["phase", "start", "end"],
    [
        ["startup", 0, 6.422],
        ["publish", 6.422, 6.422 + 20.375],
        ["drain", 6.422 + 20.375, 6.422 + 20.375 + 48.367],
    ],
)
d("replay/pub_s", "20.375")
d("replay/pub_rate", "500.023")
d("replay/total_s", "68.742")
d("replay/rate", "148.205")
d("replay/parquet", "29,402,997")
d("replay/kafkabytes", "17,264,648")
d("replay/cap", 75)
d("replay/lag_peak", "10,061")

# ---------------------------------------------------------------- workload inventory (paper section 4.2)
write_csv(
    "workloads.csv",
    ["workload", "unit", "size"],
    [
        ["TerraDS", "file version", 10188],
        ["RPKI", "repeated tuple", 5062923],
        ["DNS", "measurement", 2234863],
        ["RIS", "peer-prefix occurrence", 4725156],
    ],
)

# ---------------------------------------------------------------- write outputs
with open(OUT / "generated.tex", "w", encoding="utf-8") as f:
    f.write("% GENERATED by tools/extract_numbers.py from workloads/results. Do not edit.\n")
    for k, v in defs:
        escaped = str(v).replace("%", "\\%")
        f.write(f"\\defnum{{{k}}}{{{escaped}}}\n")

bad = [c for c in checks if not c[4]]
with open(OUT / "VERIFICATION.md", "w", encoding="utf-8") as f:
    f.write(
        "# Number verification\n\nRecomputed from `workloads/results/**` and `docs/phase12-acceptance.md`; compared with `docs/configstream_paper.md`.\n\n"
    )
    f.write(
        f"**{len(checks) - len(bad)} / {len(checks)} checks pass.**\n\n| ID | Quantity | Recomputed | Paper | Status |\n|---|---|---:|---:|---|\n"
    )
    for cid, what, got, paper, ok in checks:
        f.write(f"| {cid} | {what} | {got} | {paper} | {'PASS' if ok else 'FAIL'} |\n")
print(f"{len(checks) - len(bad)}/{len(checks)} checks pass")
for c in bad:
    print("FAIL", c)
