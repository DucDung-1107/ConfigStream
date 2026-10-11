import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path

from workloads.empirical import distribution, measure, write_distribution_plot, write_report
from workloads.terrads import RESULTS, ROOT, fetch, inspect_static, verify_cached, write_json
from workloads.terrads_history import accepted_dataset, extract_sample
from workloads.terrads_sampling import estimate, save_sample


def compact_replay(result):
    projected = dict(result)
    projected["corpus"] = {
        key: value for key, value in result.get("corpus", {}).items() if key != "sources"
    }
    projected["corpus"]["source_count"] = len(result.get("corpus", {}).get("sources", []))
    return projected


def report(root, results):
    manifest = json.loads((results / "sample-manifest.json").read_text())
    rows = [
        json.loads((root / "statuses" / f"{entry['id']}.json").read_text())
        for entry in manifest["selected"]
    ]
    snapshots, provenance, _ = accepted_dataset(root, manifest)
    if not snapshots:
        write_json(results / "population-estimate.json", estimate(manifest, rows))
        empty = {
            "assets": 0,
            "revisions": 0,
            "logical_content_bytes": 0,
            "reason": "No accepted history; all sampled statuses retained",
        }
        write_json(results / "history-summary.json", empty)
        return empty
    summary = measure(snapshots, provenance, "terraform", bundle_by_repository=True)
    summary["advertised_cutoff_check"] = {
        "advertised": "January 2026",
        "revisions_after_advertised_month": sum(
            item.event_time.isoformat() >= "2026-02" for item in snapshots
        ),
        "policy": (
            "Pinned Git timestamps retained; advertised cutoff is not enforced or assumed exact"
        ),
    }
    summary["revisions_per_repository"] = distribution([row["revisions"] for row in rows])
    repository_windows = {seconds: defaultdict(set) for seconds in (300, 3600, 86400)}
    existing_windows = {label: defaultdict(set) for label in ("commit", "5min", "hour", "day")}
    for snapshot in snapshots:
        metadata = provenance[snapshot.snapshot_id]
        if metadata["boundary_baseline"]:
            continue
        for seconds, windows in repository_windows.items():
            windows[(metadata["repository"], int(snapshot.event_time.timestamp()) // seconds)].add(
                snapshot.asset_id
            )
        if snapshot.version > 1:
            for label, period in (
                ("commit", metadata["git_commit"]),
                ("5min", int(snapshot.event_time.timestamp()) // 300),
                ("hour", int(snapshot.event_time.timestamp()) // 3600),
                ("day", int(snapshot.event_time.timestamp()) // 86400),
            ):
                existing_windows[label][(metadata["repository"], period)].add(snapshot.asset_id)
    summary["repository_correlated_files"] = {
        str(seconds): distribution([len(items) for items in windows.values()])
        for seconds, windows in repository_windows.items()
    }
    summary["existing_file_correlations"] = {
        label: distribution([len(items) for items in windows.values()])
        for label, windows in existing_windows.items()
    }
    summary["censoring"] = dict(
        Counter(
            "bounded"
            if row.get("censoring", {}).get("commit_count_bounded")
            else "root_or_unavailable"
            for row in rows
        )
    )
    summary["temporal_yield"] = {
        "revisions_per_sampled_repository": len(snapshots) / len(rows),
        "revisions_per_reachable_repository": len(snapshots)
        / sum(row["reachable"] for row in rows),
        "revisions_per_accepted_repository": len(snapshots) / sum(row["accepted"] for row in rows),
        "multi_version_files": summary["assets"] - summary["version_bins"]["1"],
        "multi_version_fraction": 1 - summary["version_bins"]["1"] / summary["assets"],
        "logical_bytes_per_sampled_repository": summary["logical_content_bytes"] / len(rows),
        "logical_bytes_per_accepted_repository": summary["logical_content_bytes"]
        / sum(row["accepted"] for row in rows),
        "revisions_per_multi_version_file": (len(snapshots) - summary["version_bins"]["1"])
        / (summary["assets"] - summary["version_bins"]["1"])
        if summary["assets"] > summary["version_bins"]["1"]
        else None,
    }
    write_json(results / "history-summary.json", summary)
    write_json(results / f"history-{manifest['sample_size']}.json", summary)
    write_json(results / "population-estimate.json", estimate(manifest, rows))
    extraction = json.loads((results / "extraction-summary.json").read_text())
    decision = {
        "sample_size": manifest["sample_size"],
        "metrics": extraction,
        "safe_to_expand": (
            extraction["free_disk_bytes"] > 40 * 1024**3
            and extraction["git_cache_bytes"] < 4 * 1024**3
            and extraction["wall_seconds_this_invocation"] < 1800
            and extraction["totals"]["reachable"] / len(rows) >= 0.5
            and extraction["statuses"].get("extraction_failed", 0) / len(rows) <= 0.1
            and summary["revisions"] > 0
        ),
        "policy": "resource/yield gate; no resampling of failed or sparse repositories",
    }
    write_json(results / f"expansion-{manifest['sample_size']}.json", decision)
    fields = [
        "id",
        "url",
        "stratum",
        "status",
        "reachable",
        "license_accepted",
        "accepted",
        "assets",
        "revisions",
        "bytes",
        "security_rejections",
        "git_cache_bytes",
        "seconds",
    ]
    with (results / "repository-summary.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(
            stream, fieldnames=fields, extrasaction="ignore", lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(sorted(rows, key=lambda entry: entry["id"]))
    phase11 = json.loads(Path("workloads/results/real-history/observed-summary.json").read_text())
    reference = json.loads(
        Path("workloads/results/real-history/calibration/observed-summary.json").read_text()
    )
    summaries = {
        "terraform": summary,
        "frr-nginx": phase11["combined"],
        **{key: value for key, value in phase11.items() if key != "combined"},
        "controlled-reference": reference["controlled-reference"],
    }
    write_report(summaries, results / "comparison")
    write_json(results / "comparison" / "observed-summary.json", summaries)
    static = json.loads((results / "static-summary.json").read_text())
    write_distribution_plot(
        [
            ("all introductions/edits", summary["commit_bundles"]["files"]),
            ("existing-file edits", summary["existing_file_correlations"]["commit"]),
        ],
        results / "plots" / "correlated-files-commit.svg",
        "Distinct .tf incarnations per repository commit",
    )
    from workloads.terrads_evidence import save_edges, save_funnel

    save_edges(snapshots, results)
    save_funnel(manifest, rows, results)
    write_distribution_plot(
        [("static .tf", static["file_size_bytes"]), ("observed .tf", summary["config_size_bytes"])],
        results / "plots" / "configuration-size.svg",
        "Terraform file size (bytes)",
    )
    write_distribution_plot(
        [
            (
                "all 500 sampled units" if len(rows) == 500 else "all sampled units",
                summary["revisions_per_repository"],
            )
        ],
        results / "plots" / "revisions-per-repository.svg",
        "Revisions/repository (zero yields included)",
    )
    comparison = [
        {
            "dataset": label,
            "kind": (
                "observed"
                if label in {"terraform", "frr-nginx", *phase11.keys()}
                else "calibrated"
                if label == "phase11-calibrated"
                else "controlled"
            ),
            "assets": item["assets"],
            "snapshots": item["revisions"],
            "adjacent_pairs": item["revisions"] - item["assets"],
            "logical_bytes": item["logical_content_bytes"],
            "unique_contents": item["unique_contents"],
            "unique_ratio": item["unique_content_ratio"],
        }
        for label, item in {**summaries, "phase11-calibrated": reference["calibrated"]}.items()
    ]
    historical = json.loads(
        Path("workloads/results/real-history/replay-analytics.json").read_text()
    )
    main_run = json.loads(Path("benchmarks/results/main-1m-2g-50k-20261011.json").read_text())
    comparison.append(
        {
            "dataset": "phase10-1m",
            "kind": "stress/semi-synthetic",
            "assets": main_run["config"]["assets"],
            "snapshots": main_run["publisher"]["snapshots"],
            "adjacent_pairs": main_run["lake"]["counts"]["config_diff"],
            "logical_bytes": main_run["publisher"]["logical_raw_config_bytes"],
            "unique_contents": historical["main-1m-2g-50k-20261011"]["unique_raw_hashes"],
            "unique_ratio": historical["main-1m-2g-50k-20261011"]["unique_raw_hashes"]
            / main_run["publisher"]["snapshots"],
        }
    )
    with (results / "comparison.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(comparison[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(comparison)
    from workloads.terrads_evidence import save_comparison_tables

    save_comparison_tables(results)
    return {
        key: summary[key]
        for key in [
            "assets",
            "revisions",
            "logical_content_bytes",
            "unique_contents",
            "temporal_yield",
        ]
    }


def main():
    parser = argparse.ArgumentParser(
        description="Pinned TerraDS static corpus and observed .tf history"
    )
    parser.add_argument(
        "command",
        choices=[
            "fetch",
            "verify",
            "inspect",
            "sample",
            "history",
            "report",
            "audit",
            "replay",
            "validate",
            "smoke",
        ],
    )
    parser.add_argument("--cache-dir", type=Path, default=ROOT)
    parser.add_argument("--results", type=Path, default=RESULTS)
    parser.add_argument("--sample-size", type=int, default=100)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-concurrency", type=int, default=4)
    parser.add_argument("--max-file-bytes", type=int, default=1024**2)
    parser.add_argument("--max-commits", type=int, default=2000)
    parser.add_argument("--history-policy", choices=["first-parent"], default="first-parent")
    parser.add_argument(
        "--resume", action="store_true", help="Explicitly acknowledge safe automatic status resume"
    )
    parser.add_argument("--benchmark-id")
    parser.add_argument("--rate", type=float, default=500)
    args = parser.parse_args()
    root, results = args.cache_dir, args.results
    if args.command == "fetch":
        result = fetch(root, results)
    elif args.command == "verify":
        result = verify_cached(root, results)
    elif args.command == "inspect":
        result = inspect_static(root, results)
        result = {
            key: value
            for key, value in result.items()
            if key not in {"file_size_bytes", "repository_distributions"}
        }
    elif args.command == "sample":
        result = save_sample(root, results, args.sample_size, args.seed)
        result = {key: value for key, value in result.items() if key != "selected"}
    elif args.command == "history":
        manifest = json.loads((results / "sample-manifest.json").read_text())
        if manifest["sample_size"] != args.sample_size or manifest["seed"] != args.seed:
            parser.error("History parameters must match the frozen sample manifest")
        result = extract_sample(
            manifest, root, results, args.max_concurrency, args.max_commits, args.max_file_bytes
        )
    elif args.command == "report":
        result = report(root, results)
    elif args.command == "audit":
        from workloads.terrads_evidence import audit_unchanged

        result = audit_unchanged(
            root, json.loads((results / "sample-manifest.json").read_text()), results
        )
    elif args.command == "replay":
        from benchmarks.main import BenchmarkConfig, execute
        from workloads.observed import ObservedHistory

        if not args.benchmark_id:
            parser.error("Replay requires an explicit fresh benchmark ID")
        history = ObservedHistory(root / "history")
        config = BenchmarkConfig(
            benchmark_id=args.benchmark_id,
            assets=len(history.assets),
            versions_per_asset=1,
            rate=args.rate,
            profile="observed-replay",
            seed=args.seed,
            corpus=root / "history",
            start_time=history.start_time,
            watermark="1000000 hours",
            state_timeout="1000000 hours",
            max_versions_per_asset=max(32, max(len(items) for _, items in history.groups) + 16),
            driver_memory="2g",
            max_offsets=10000,
            max_seconds=3600,
        )
        result = execute(config, analytics=False, history=history)
        write_json(results / "replay-summary.json", compact_replay(result))
        print(
            json.dumps(
                {
                    "status": result["status"],
                    "lake": result.get("lake"),
                    "failure": result.get("failure"),
                }
            )
        )
        return 0 if result["status"] == "success" else 1
    elif args.command == "validate":
        from workloads.terrads_validation import validate_replay

        if not args.benchmark_id:
            parser.error("Validation requires benchmark ID")
        result = validate_replay(root, results, args.benchmark_id)
    else:
        from workloads.terrads_validation import serving_smoke

        if not args.benchmark_id:
            parser.error("Smoke requires an explicit fresh ID")
        result = serving_smoke(root, results, args.benchmark_id)
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
