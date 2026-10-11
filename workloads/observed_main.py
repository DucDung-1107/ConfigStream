import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
from time import monotonic, sleep

from messaging.serialization import serialize_record
from workloads.empirical import measure, write_report
from workloads.observed import GitSource, ObservedHistory, extract, fetch_source, load_dataset


def empirical_report(root, destination):
    snapshots, provenance, _ = load_dataset(root)
    groups = defaultdict(list)
    for snapshot in snapshots:
        groups[provenance[snapshot.snapshot_id]["source"]].append(snapshot)
    summaries = {
        source: measure(items, provenance, source) for source, items in sorted(groups.items())
    }
    summaries["combined"] = measure(snapshots, provenance)
    write_report(summaries, destination)
    return summaries


def main():
    parser = argparse.ArgumentParser(
        description="Allowlisted observed Git history and calibrated replay"
    )
    parser.add_argument(
        "command",
        choices=[
            "fetch",
            "inspect",
            "report",
            "calibration-report",
            "generate",
            "replay",
            "benchmark",
        ],
    )
    parser.add_argument("--root", type=Path, default=Path(".cache/datasets/observed-history"))
    parser.add_argument("--results", type=Path, default=Path("workloads/results/real-history"))
    parser.add_argument("--summary-input", type=Path)
    parser.add_argument(
        "--profile", choices=["observed-replay", "observed-calibrated"], default="observed-replay"
    )
    parser.add_argument("--multiplier", type=int, default=1)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--rate", type=float, default=500)
    parser.add_argument("--output", type=Path, default=Path(".cache/workloads/observed.jsonl"))
    parser.add_argument("--benchmark-id")
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--max-seconds", type=int, default=1800)
    parser.add_argument("--max-offsets", type=int, default=10000)
    args = parser.parse_args()
    if not 0 < args.rate <= 100000:
        parser.error("Replay rate must be positive and bounded")
    if args.command == "fetch":
        for manifest in sorted(Path("workloads/manifests/real-history").glob("*.json")):
            source = GitSource.model_validate_json(manifest.read_text())
            repository = fetch_source(source)
            result = extract(source, repository, args.root / source.name)
            print(json.dumps(result, sort_keys=True))
        return
    if args.command in {"report", "inspect"}:
        if args.summary_input:
            summaries = json.loads(args.summary_input.read_text())
            write_report(summaries, args.results)
        else:
            summaries = (
                empirical_report(args.root, args.results)
                if args.command == "report"
                else json.loads((args.results / "observed-summary.json").read_text())
            )
        print(
            json.dumps(
                {
                    label: {
                        key: value
                        for key, value in summary.items()
                        if key
                        in {
                            "assets",
                            "revisions",
                            "logical_content_bytes",
                            "history_days",
                            "unique_contents",
                        }
                    }
                    | {
                        "median_config_bytes": summary["config_size_bytes"]["p50"],
                        "median_versions": summary["versions_per_asset"]["p50"],
                        "median_inter_change_hours": (
                            summary["inter_change_seconds"]["p50"] / 3600
                            if summary["inter_change_seconds"]["p50"] is not None
                            else None
                        ),
                        "adjacent_pairs": summary["diff"]["changed"]["count"],
                        "content_reversions": summary["reverts"]["count"],
                    }
                    for label, summary in summaries.items()
                },
                indent=2,
            )
        )
        return
    if args.command == "calibration-report":
        from workloads.history import History, WorkloadConfig

        snapshots, provenance, _ = load_dataset(args.root)
        history = ObservedHistory(
            args.root, seed=args.seed, multiplier=args.multiplier, calibrated=True
        )
        weights = Counter(items[0].asset_id for _, items in history.groups)
        summaries = {
            "observed": measure(snapshots, provenance, "observed"),
            "calibrated": measure(snapshots, provenance, "calibrated", weights=weights),
        }
        controlled = []
        controlled_provenance = {}
        reference = History(
            WorkloadConfig(
                assets=100, versions_per_asset=100, seed=args.seed, profile="stress-replay"
            )
        )
        for _, snapshot, _ in reference.records():
            controlled.append(snapshot)
            controlled_provenance[snapshot.snapshot_id] = {
                "boundary_baseline": snapshot.version == 1,
                "git_commit": snapshot.snapshot_id,
                "observation_end": "2026-04-11T00:00:00Z",
            }
        summaries["controlled-reference"] = measure(
            controlled, controlled_provenance, "controlled-reference"
        )
        write_report(summaries, args.results / "calibration")
        (args.results / "calibration/manifest.json").write_text(
            json.dumps(history.manifest, indent=2) + "\n"
        )
        print(
            json.dumps(
                {
                    label: {
                        key: summary[key]
                        for key in ["assets", "revisions", "logical_content_bytes"]
                    }
                    for label, summary in summaries.items()
                },
                indent=2,
            )
        )
        return
    history = ObservedHistory(
        args.root,
        seed=args.seed,
        multiplier=args.multiplier,
        calibrated=args.profile == "observed-calibrated",
    )
    history.manifest["physical_replay_target_snapshots_per_second"] = args.rate
    if args.command == "benchmark":
        from benchmarks.main import BenchmarkConfig, execute

        if not args.benchmark_id:
            parser.error("An explicit fresh benchmark ID is required")
        config = BenchmarkConfig(
            benchmark_id=args.benchmark_id,
            assets=len(history.assets),
            versions_per_asset=1,
            rate=args.rate,
            profile=args.profile,
            seed=args.seed,
            master=args.master,
            corpus=args.root,
            start_time=history.start_time,
            watermark="1000000 hours",
            state_timeout="1000000 hours",
            max_versions_per_asset=max(32, max(len(items) for _, items in history.groups)),
            driver_memory="2g",
            max_offsets=args.max_offsets,
            max_seconds=args.max_seconds,
        )
        result = execute(config, analytics=False, history=history)
        print(
            json.dumps(
                {
                    key: result.get(key)
                    for key in [
                        "benchmark_id",
                        "status",
                        "publisher",
                        "lake",
                        "metrics",
                        "query_id",
                    ]
                },
                indent=2,
            )
        )
        raise SystemExit(0 if result["status"] == "success" else 1)
    if args.command == "generate":
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with (
            args.output.open("w") as output,
            args.output.with_suffix(".provenance.jsonl").open("w") as sidecar,
        ):
            for asset in history.assets:
                output.write(serialize_record(asset))
            for event, snapshot, truth in history.records():
                output.write(serialize_record(event))
                output.write(serialize_record(snapshot))
                sidecar.write(json.dumps(truth, sort_keys=True) + "\n")
        args.output.with_suffix(".manifest.json").write_text(
            json.dumps(history.manifest, indent=2) + "\n"
        )
        print(json.dumps(history.manifest, indent=2))
        return
    from messaging.config import KafkaConfig
    from messaging.kafka_producer import KafkaProducer

    started = monotonic()
    with KafkaProducer(KafkaConfig.from_env()) as producer:
        for asset in history.assets:
            producer.publish(asset)
        for count, (_, snapshot, _) in enumerate(history.records()):
            remaining = started + count / args.rate - monotonic()
            if remaining > 0:
                sleep(remaining)
            producer.publish(snapshot)
    print(
        json.dumps(
            {
                **history.manifest,
                "physical_replay_seconds": monotonic() - started,
                "observed_logical_days": history.logical_days,
            }
        )
    )


if __name__ == "__main__":
    main()
