import csv
import json
import os
import subprocess
from collections import defaultdict
from pathlib import Path

from workloads.terrads import file_hash, write_json


def save_edges(snapshots, results):
    grouped = defaultdict(list)
    for snapshot in snapshots:
        grouped[snapshot.asset_id].append(snapshot)
    rows = []
    for asset_id, history in sorted(grouped.items()):
        ordered = sorted(history, key=lambda item: item.version)
        if [item.version for item in ordered] != list(range(1, len(ordered) + 1)):
            raise ValueError("Ground truth requires contiguous unique extracted versions")
        rows.extend(
            {
                "asset_id": asset_id,
                "old_version": previous.version,
                "new_version": current.version,
                "old_snapshot_id": previous.snapshot_id,
                "new_snapshot_id": current.snapshot_id,
            }
            for previous, current in zip(ordered, ordered[1:], strict=False)
        )
    destination = results / "ground-truth-edges.csv"
    results.mkdir(parents=True, exist_ok=True)
    with destination.open("w", newline="") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=[
                "asset_id",
                "old_version",
                "new_version",
                "old_snapshot_id",
                "new_snapshot_id",
            ],
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)
    summary = {"assets": len(grouped), "edges": len(rows), "sha256": file_hash(destination)}
    write_json(results / "ground-truth-summary.json", summary)
    return summary


def save_funnel(manifest, rows, results):
    stages = [
        (
            "Static repository population",
            sum(item["population"] for item in manifest["strata"].values()),
        ),
        ("Sampled repositories", len(rows)),
        ("Reachable repositories", sum(item["reachable"] for item in rows)),
        ("License verified repositories", sum(item["license_accepted"] for item in rows)),
        ("Repositories with safe histories", sum(item["accepted"] for item in rows)),
        ("Accepted file incarnations", sum(item["assets"] for item in rows)),
        ("Observed revisions", sum(item["revisions"] for item in rows)),
    ]
    values = [{"stage": label, "count": count} for label, count in stages]
    write_json(results / "funnel.json", values)
    with (results / "funnel.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=["stage", "count"], lineterminator="\n")
        writer.writeheader()
        writer.writerows(values)
    (results / "funnel.md").write_text(
        "# Measured sampling funnel\n\n"
        "Population is static; the final two rows use different units, not repository attrition.\n"
        "Security acceptance is file-incarnation-level; partially rejected repositories remain.\n\n"
        "| Stage | Count |\n| --- | ---: |\n"
        + "\n".join(f"| {label} | {count} |" for label, count in stages)
        + "\n"
    )
    parts = [
        '<svg xmlns="http://www.w3.org/2000/svg" width="900" height="360" role="img">',
        '<rect width="900" height="360" fill="white"/>',
        '<text x="20" y="25" font-family="sans-serif">'
        "Measured funnel (linear bars; units change)</text>",
    ]
    for index, (label, count) in enumerate(stages):
        vertical = 65 + index * 40
        width = 450 * count / stages[0][1]
        parts.extend(
            [
                f'<text x="20" y="{vertical}" font-family="sans-serif" font-size="13">'
                f"{label}</text>",
                f'<rect x="290" y="{vertical - 15}" width="{width:.3f}" '
                'height="20" fill="#287c8e"/>',
                f'<text x="770" y="{vertical}" font-family="sans-serif">{count}</text>',
            ]
        )
    parts.append("</svg>")
    plot = results / "plots" / "funnel.svg"
    plot.parent.mkdir(parents=True, exist_ok=True)
    plot.write_text("\n".join(parts))
    return values


def unchanged_observations(repository, start, end):
    def offline_git(*arguments):
        return subprocess.check_output(
            ["git", "--git-dir", str(repository), *arguments],
            env={**os.environ, "GIT_NO_LAZY_FETCH": "1", "GIT_TERMINAL_PROMPT": "0"},
            stderr=subprocess.DEVNULL,
            timeout=180,
        )

    relevant = (
        offline_git("rev-list", "--first-parent", "--reverse", f"{start}..{end}", "--", "*.tf")
        .decode()
        .splitlines()
    )
    active = {}
    suppressed = 0
    for commit in [start, *relevant]:
        current = {}
        for entry in offline_git("ls-tree", "-r", "-z", commit).split(b"\0"):
            if entry:
                header, path = entry.split(b"\t", 1)
                mode, kind, identity = header.split()
                if mode in {b"100644", b"100755"} and kind == b"blob" and path.endswith(b".tf"):
                    current[path] = identity
        suppressed += sum(active.get(path) == identity for path, identity in current.items())
        active = current
    return {
        "selected_tree_observations": len(relevant) + 1,
        "unchanged_path_blob_observations": suppressed,
    }


def audit_unchanged(root, manifest, results):
    rows = []
    for entry in manifest["selected"]:
        status = json.loads((root / "statuses" / f"{entry['id']}.json").read_text())
        if not status["license_accepted"]:
            continue
        destination = root / "history" / f"terrads-{entry['id']}" / "manifest.json"
        if not destination.exists():
            destination = root / "incomplete" / f"terrads-{entry['id']}" / "manifest.json"
        source = json.loads(destination.read_text())["source"]
        rows.append(
            {
                "id": entry["id"],
                **unchanged_observations(
                    root / "git" / f"{entry['id']}.git",
                    source["start_revision"],
                    source["end_revision"],
                ),
            }
        )
    summary = {
        "policy": "offline tree/blob identity audit, Terraform-relevant first-parent commits only",
        "bodies_read": False,
        "repositories": len(rows),
        "unchanged_path_blob_observations": sum(
            item["unchanged_path_blob_observations"] for item in rows
        ),
        "rows": rows,
    }
    write_json(results / "unchanged-audit.json", summary)
    return {key: value for key, value in summary.items() if key != "rows"}


def save_comparison_tables(results):
    summaries = json.loads((results / "comparison/observed-summary.json").read_text())
    temporal_rows = []
    for label, summary in summaries.items():
        temporal_rows.append(
            {
                "dataset": label,
                "median_config_bytes": summary["config_size_bytes"]["p50"],
                "p95_config_bytes": summary["config_size_bytes"]["p95"],
                "median_versions": summary["versions_per_asset"]["p50"],
                "p95_versions": summary["versions_per_asset"]["p95"],
                "median_uncensored_interval_seconds": summary["inter_change_seconds"]["p50"],
                "p95_diff_changed": summary["diff"]["changed"]["p95"],
                "multi_version_fraction": 1 - summary["version_bins"]["1"] / summary["assets"],
                "max_files_per_commit": summary["commit_bundles"]["files"]["max"],
                "unique_content_ratio": summary["unique_content_ratio"],
            }
        )
    with (results / "temporal-comparison.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(temporal_rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(temporal_rows)
    with (results / "comparison.csv").open() as stream:
        comparisons = list(csv.DictReader(stream))
    evidence = [
        {
            "dataset": row["dataset"],
            "evidence": row["kind"],
            "assets_or_repositories": row["assets"],
            "files_or_states": row["snapshots"],
            "logical_bytes": row["logical_bytes"],
        }
        for row in comparisons
    ]
    baseline = json.loads(Path("workloads/manifests/batfish-measured.json").read_text())
    static = json.loads((results / "static-summary.json").read_text())
    estimate = json.loads((results / "population-estimate.json").read_text())["estimates"]
    evidence.extend(
        [
            {
                "dataset": "batfish",
                "evidence": "real baseline, not temporal",
                "assets_or_repositories": baseline["usable_files"],
                "files_or_states": baseline["usable_files"],
                "logical_bytes": baseline["usable_bytes"],
            },
            {
                "dataset": "terrads-static",
                "evidence": "real static, not temporal",
                "assets_or_repositories": static["archive_repositories"],
                "files_or_states": static["terraform_files"],
                "logical_bytes": static["logical_terraform_bytes"],
            },
            {
                "dataset": "terrads-population-estimate",
                "evidence": "inferred, NOT observed",
                "assets_or_repositories": estimate["accepted"]["point"],
                "files_or_states": estimate["revisions"]["point"],
                "logical_bytes": estimate["bytes"]["point"],
            },
        ]
    )
    with (results / "evidence-comparison.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(evidence[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(evidence)
    (results / "summary.md").write_text(
        "# Measured evidence comparison\n\n"
        "Units differ: static files versus observed/controlled states; "
        "estimates are not observations.\n"
        "CIs and rejection assumptions: `population-estimate.json`.\n\n"
        "| Dataset | Evidence | Assets/repos | Files/states | Logical bytes |\n"
        "| --- | --- | ---: | ---: | ---: |\n"
        + "\n".join(
            f"| {row['dataset']} | {row['evidence']} | {row['assets_or_repositories']} | "
            f"{row['files_or_states']} | {row['logical_bytes']} |"
            for row in evidence
        )
        + "\n\nTemporal distributions: `temporal-comparison.csv`; "
        "sampler/funnel remain unchanged.\n"
    )
