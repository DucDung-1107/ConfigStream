"""Render checkpoint charts from accepted repository evidence; no experiment is run."""

import json
from pathlib import Path
from statistics import median

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
EVIDENCE = ROOT / "workloads/results/phase20"
representations = json.loads((EVIDENCE / "rpki-representations.json").read_text())
accounting = json.loads((EVIDENCE / "rpki-all-in-storage.json").read_text())
modes = ["R1", "R2", "R3"]
configs = [representations["configurations"][mode] for mode in modes]
queries = [config["queries"]["all_days"] for config in configs]
assert all(query["exact_correctness"] == "PASS" for query in queries)
assert len({query["output_sha256"] for query in queries}) == 1
sizes = np.array([config["analytical_bytes"] for config in configs]) / 1_000_000
stats = [query["build_median_statistics"] for query in queries]
times = np.array([stat["median"] for stat in stats])
ranges = np.array(
    [
        [stat["median"] - stat["minimum"] for stat in stats],
        [stat["maximum"] - stat["median"] for stat in stats],
    ]
)
totals = (
    np.array(
        [
            median(
                row["accounted_component_content_bytes"]
                for row in accounting["rows"]
                if row["mode"] == mode
            )
            for mode in modes
        ]
    )
    / 1_000_000
)
analytical_saving = 100 * (1 - sizes[1] / sizes[0])
local_saving = 100 * (1 - totals[1] / totals[0])
colors = ["#60758B", "#045073", "#6B9C8D"]
plt.rcParams.update(
    {
        "font.family": "DejaVu Sans",
        "font.size": 12,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.spines.left": False,
        "axes.edgecolor": "#AFB9C4",
        "axes.labelcolor": "#0D2659",
        "text.color": "#0D2659",
        "xtick.color": "#283746",
        "ytick.color": "#526170",
        "pdf.fonttype": 42,
        "savefig.facecolor": "white",
    }
)


def save(fig, name):
    fig.savefig(HERE / "figs" / f"{name}.pdf", bbox_inches="tight", pad_inches=0.07)
    fig.savefig(HERE / "figs" / f"{name}.png", dpi=220, bbox_inches="tight", pad_inches=0.07)
    plt.close(fig)


labels = ["Full daily states\nR1", "Start + changes\nR2", "R2 + day-8 state\nR3"]
fig, axes = plt.subplots(1, 2, figsize=(11.4, 3.05), layout="constrained")
for ax, values, title, limit, ticks in zip(
    axes,
    [sizes, times],
    ["Analytical storage (MB)", "Reconstruct all 14 days (seconds)"],
    [57, 8.4],
    [[0, 20, 40], [0, 2, 4, 6, 8]],
    strict=True,
):
    ax.bar(np.arange(3), values, width=0.57, color=colors, zorder=3)
    ax.set_xticks(np.arange(3), labels, fontsize=10.5)
    ax.set_ylim(0, limit)
    ax.set_yticks(ticks)
    ax.set_title(title, fontsize=14, fontweight="bold", pad=16)
    ax.grid(axis="y", color="#E8ECF0", zorder=0)
    ax.tick_params(axis="both", length=0, pad=7)
    for i, value in enumerate(values):
        ax.text(
            i, value + limit * 0.035, f"{value:.2f}", ha="center", fontsize=12, fontweight="bold"
        )
axes[1].errorbar(
    np.arange(3), times, yerr=ranges, fmt="none", ecolor="#1A2633", capsize=4, zorder=4
)
save(fig, "rpki-storage-query")

fig, ax = plt.subplots(figsize=(11.4, 3.15), layout="constrained")
y = np.arange(3)
ax.barh(y, sizes, height=0.56, color="#045073", label="Analytical files", zorder=3)
ax.barh(
    y,
    totals - sizes,
    left=sizes,
    height=0.56,
    color="#D7DFE7",
    label="Sources, maps, metadata + query scratch",
    zorder=3,
)
ax.set_yticks(
    y, ["R1  Full daily states", "R2  Start + changes", "R3  R2 + day-8 state"], fontsize=12
)
ax.invert_yaxis()
ax.set_xlim(0, 285)
ax.set_xticks([0, 50, 100, 150, 200, 250])
ax.set_xlabel("Retained local experiment files (MB)", fontsize=12)
ax.grid(axis="x", color="#E8ECF0", zorder=0)
ax.tick_params(axis="both", length=0, pad=8)
for i, total in enumerate(totals):
    ax.text(total + 3, i, f"{total:.2f}", va="center", fontweight="bold", fontsize=12)
ax.legend(loc="lower left", bbox_to_anchor=(0, 1.04), frameon=False, ncol=2, fontsize=11)
save(fig, "rpki-total-storage")

facts = {
    "scope": "14 scoped RPKI observations, 2026-09-01 through 2026-09-14",
    "source": [
        "workloads/results/phase20/rpki-representations.json",
        "workloads/results/phase20/rpki-all-in-storage.json",
    ],
    "unit": "MB = 1,000,000 bytes",
    "modes": modes,
    "analytical_mb": sizes.tolist(),
    "all_days_seconds": times.tolist(),
    "local_file_mb": totals.tolist(),
    "r2_analytical_saving_percent": analytical_saving,
    "r2_local_file_saving_percent": local_saving,
    "query_summary": (
        "Median of three build-level medians, each with three warm cycles; "
        "whiskers are range of build medians, not confidence intervals"
    ),
    "storage_summary": (
        "Median accounted content bytes across three builds; "
        "excludes deployed broker and checkpoint costs"
    ),
    "correctness": "Exact all-days output digests agree across R1, R2, R3",
}
(HERE / "figs/results-data.json").write_text(json.dumps(facts, indent=2) + "\n")
print(f"R2 savings: analytical {analytical_saving:.2f}%; measured local files {local_saving:.2f}%")
