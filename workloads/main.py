import argparse
import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from messaging.serialization import serialize_record
from workloads.corpus import fetch
from workloads.history import PROFILES, History, WorkloadConfig


def main():
    parser = argparse.ArgumentParser(description="Curated baselines and controlled histories")
    parser.add_argument("command", choices=["fetch", "inspect", "generate"])
    parser.add_argument("--corpus", type=Path, default=Path(".cache/datasets/batfish"))
    parser.add_argument("--source", type=Path, default=Path("workloads/manifests/batfish.json"))
    parser.add_argument("--assets", type=int, default=100)
    parser.add_argument("--versions-per-asset", type=int, default=10)
    parser.add_argument("--profile", choices=PROFILES, default="fleet-rollout")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--size-bucket",
        choices=["small", "medium", "large", "corpus-distribution"],
        default="corpus-distribution",
    )
    parser.add_argument("--duplicate-ratio", type=float, default=0)
    parser.add_argument("--out-of-order-ratio", type=float, default=0)
    parser.add_argument("--max-reordering-distance", type=int, default=16)
    parser.add_argument(
        "--start-time",
        type=datetime.fromisoformat,
        default=datetime.fromisoformat("2026-01-01T00:00:00+00:00"),
    )
    parser.add_argument("--output", type=Path, default=Path(".cache/workloads/history.jsonl"))
    args = parser.parse_args()
    if args.command == "fetch":
        manifest = fetch(args.source, args.corpus)
    elif args.command == "inspect":
        manifest = json.loads((args.corpus / "manifest.json").read_text())
    else:
        config = WorkloadConfig(
            assets=args.assets,
            versions_per_asset=args.versions_per_asset,
            profile=args.profile,
            seed=args.seed,
            start_time=args.start_time,
            corpus=args.corpus,
            size_bucket=args.size_bucket,
            duplicate_ratio=args.duplicate_ratio,
            out_of_order_ratio=args.out_of_order_ratio,
            max_reordering_distance=args.max_reordering_distance,
        )
        history = History(config)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        count = logical_bytes = 0
        with (
            args.output.open("w") as output,
            args.output.with_suffix(".truth.jsonl").open("w") as truth,
        ):
            for asset in history.assets:
                output.write(serialize_record(asset))
            for event, snapshot, ground_truth in history.records():
                output.write(serialize_record(event))
                output.write(serialize_record(snapshot))
                truth.write(json.dumps(ground_truth, sort_keys=True) + "\n")
                count += 1
                logical_bytes += len(snapshot.content.encode())
        manifest = {
            "config": {key: str(value) for key, value in asdict(config).items()},
            "snapshots": count,
            "logical_raw_config_bytes": logical_bytes,
            "corpus": json.loads((args.corpus / "manifest.json").read_text())["source"],
        }
        args.output.with_suffix(".manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps({key: value for key, value in manifest.items() if key != "files"}, indent=2))


if __name__ == "__main__":
    main()
