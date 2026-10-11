import argparse
import json
import re
from dataclasses import replace
from pathlib import Path

from analytics.config import AnalyticsConfig
from analytics.main import run_analytics
from lake.config import LakeConfig
from spark.common.config import SparkConfig


def analyze(session, identifier):
    from pyspark.sql import functions as functions

    from spark.streaming.sinks.lake import read_committed

    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,100}", identifier):
        raise ValueError("Unsafe replay identity")
    result = json.loads(Path(f".cache/benchmarks/{identifier}/result.json").read_text())
    if result["status"] != "success":
        raise ValueError("Only completed replays can be analyzed")
    lake = replace(LakeConfig.from_env(), prefix=result["runtime"]["lake_prefix"])
    query_id = result["query_id"]
    snapshot = read_committed(session, lake, "processed/snapshots", query_id).drop(
        "normalized_content"
    )
    diff = read_committed(session, lake, "processed/diffs", query_id).drop("details")
    statistics = (
        snapshot.agg(
            functions.count("*").alias("snapshots"),
            functions.countDistinct("asset_id").alias("assets"),
            functions.countDistinct("raw_hash").alias("unique_raw_hashes"),
        )
        .first()
        .asDict()
    )
    old = snapshot.select(
        functions.col("snapshot_id").alias("old_snapshot_id"),
        functions.col("asset_id").alias("old_asset"),
        functions.col("version").alias("old_version"),
    )
    current = snapshot.select(
        functions.col("snapshot_id").alias("new_snapshot_id"),
        functions.col("asset_id").alias("new_asset"),
        functions.col("version").alias("new_version"),
    )
    paired = diff.join(old, "old_snapshot_id", "left").join(current, "new_snapshot_id", "left")
    statistics["invalid_pairs"] = paired.filter(
        functions.col("old_asset").isNull()
        | functions.col("new_asset").isNull()
        | (functions.col("asset_id") != functions.col("old_asset"))
        | (functions.col("asset_id") != functions.col("new_asset"))
        | (functions.col("new_version") != functions.col("old_version") + 1)
    ).count()
    if statistics["invalid_pairs"]:
        raise ValueError("Replay contains invalid/cross-asset/nonadjacent diff pairs")
    jobs = (
        ["all"]
        if identifier == "phase11-observed"
        else ["change-frequency", "fleet-activity", "diff-magnitude", "asset-summary"]
    )
    statistics["analytics"] = {}
    for job in jobs:
        manifest = run_analytics(session, lake, AnalyticsConfig(window="1 day"), query_id, job)
        for name, dataset in manifest["datasets"].items():
            frame = session.read.parquet(dataset["path"])
            measured = {key: dataset[key] for key in ["rows", "seconds", "schema"]}
            if name == "change-frequency":
                measured["totals"] = (
                    frame.groupBy()
                    .sum("snapshot_count", "diff_count", "alert_count")
                    .first()
                    .asDict()
                )
            elif name in {"anomalies", "fleet-bursts"}:
                measured["statistical_outlier_windows"] = frame.filter("is_anomaly").count()
            elif name == "diff-magnitude":
                measured["summary"] = frame.first().asDict()
            elif name == "fleet-activity":
                measured["max_changed_assets_day"] = frame.agg(
                    functions.max("assets_changed")
                ).first()[0]
            statistics["analytics"][name] = measured
    return statistics


def main():
    parser = argparse.ArgumentParser(
        description="Validate committed observed/calibrated replay analytics"
    )
    parser.add_argument("--runs", nargs="+", required=True)
    parser.add_argument(
        "--output", type=Path, default=Path("workloads/results/real-history/replay-analytics.json")
    )
    args = parser.parse_args()
    from spark.common.runtime import create_session

    session = create_session(
        SparkConfig(master="local[2]", driver_memory="2g", lake=LakeConfig.from_env()), kafka=False
    )
    try:
        results = {identifier: analyze(session, identifier) for identifier in args.runs}
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(results, indent=2, default=str) + "\n")
        print(
            json.dumps(
                {
                    identifier: {key: value for key, value in entry.items() if key != "analytics"}
                    for identifier, entry in results.items()
                },
                indent=2,
            )
        )
    finally:
        session.stop()


if __name__ == "__main__":
    main()
