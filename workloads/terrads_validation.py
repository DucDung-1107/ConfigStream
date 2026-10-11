import json
import os
import re
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

from analytics.config import AnalyticsConfig
from analytics.main import run_analytics
from lake.config import LakeConfig
from spark.common.config import SparkConfig
from workloads.observed import load_dataset
from workloads.terrads import write_json
from workloads.terrads_evidence import save_edges


def validate_replay(root, results, identifier):
    from pyspark.sql import functions as functions

    from spark.common.runtime import create_session
    from spark.streaming.sinks.lake import read_committed

    replay = json.loads(Path(f".cache/benchmarks/{identifier}/result.json").read_text())
    if replay["status"] != "success":
        raise ValueError("Only completed replays may be validated")
    snapshots, _, _ = load_dataset(root / "history")
    grouped = {}
    for snapshot in snapshots:
        grouped.setdefault(snapshot.asset_id, []).append(snapshot)
    expected_pairs = [
        (current.asset_id, previous.snapshot_id, current.snapshot_id)
        for history in grouped.values()
        for previous, current in zip(
            sorted(history, key=lambda item: item.version),
            sorted(history, key=lambda item: item.version)[1:],
            strict=False,
        )
    ]
    edge_summary = save_edges(snapshots, results)
    lake = replace(LakeConfig.from_env(), prefix=replay["runtime"]["lake_prefix"])
    session = create_session(
        SparkConfig(master="local[2]", driver_memory="2g", lake=lake), kafka=False
    )
    try:
        stored = (
            read_committed(session, lake, "processed/snapshots", replay["query_id"])
            .select("snapshot_id", "asset_id", "version", "raw_hash", "event_time")
            .persist()
        )
        expected = session.createDataFrame(
            [
                (item.snapshot_id, item.asset_id, item.version, item.hash, item.event_time)
                for item in snapshots
            ],
            stored.schema,
        )
        pair_schema = "asset_id STRING, old_snapshot_id STRING, new_snapshot_id STRING"
        pairs = (
            read_committed(session, lake, "processed/diffs", replay["query_id"])
            .select("asset_id", "old_snapshot_id", "new_snapshot_id")
            .persist()
        )
        expected_frame = session.createDataFrame(expected_pairs, pair_schema)
        validation = {
            "snapshots": stored.count(),
            "diffs": pairs.count(),
            "missing_snapshots": expected.exceptAll(stored).count(),
            "unexpected_snapshots": stored.exceptAll(expected).count(),
            "missing_pairs": expected_frame.exceptAll(pairs).count(),
            "unexpected_pairs": pairs.exceptAll(expected_frame).count(),
            "duplicate_snapshot_ids": stored.groupBy("snapshot_id")
            .count()
            .filter("count > 1")
            .count(),
            "duplicate_pairs": pairs.groupBy("old_snapshot_id", "new_snapshot_id")
            .count()
            .filter("count > 1")
            .count(),
        }
        if any(value for key, value in validation.items() if key not in {"snapshots", "diffs"}):
            raise ValueError("Exact logical observed-history ground truth mismatch")
        validation["ground_truth"] = edge_summary
        manifest = run_analytics(
            session, lake, AnalyticsConfig(window="1 day"), replay["query_id"], "all"
        )
        validation["analytics"] = {}
        for name, dataset in manifest["datasets"].items():
            frame = session.read.parquet(dataset["path"])
            metrics = {key: dataset[key] for key in ["rows", "seconds", "schema"]}
            if name == "change-frequency":
                metrics["totals"] = (
                    frame.groupBy()
                    .sum("snapshot_count", "diff_count", "alert_count")
                    .first()
                    .asDict()
                )
                if metrics["totals"]["sum(snapshot_count)"] != len(snapshots) or metrics["totals"][
                    "sum(diff_count)"
                ] != len(expected_pairs):
                    raise ValueError("Historical frequency totals differ from extraction truth")
            if name in {"anomalies", "fleet-bursts"}:
                metrics["statistical_outlier_windows"] = frame.filter("is_anomaly").count()
            if name == "diff-magnitude":
                metrics["summary"] = frame.first().asDict()
            if name == "fleet-activity":
                metrics["max_changed_assets_day"] = frame.agg(
                    functions.max("assets_changed")
                ).first()[0]
            validation["analytics"][name] = metrics
        write_json(results / "replay-validation.json", validation)
        return {key: value for key, value in validation.items() if key != "analytics"}
    finally:
        session.stop()


def serving_smoke(root, results, identifier):
    from fastapi.testclient import TestClient

    from api.app import create_app
    from api.config import ApiConfig
    from benchmarks.main import committed_metrics, latest_offsets
    from messaging.config import KafkaConfig
    from messaging.kafka_producer import KafkaProducer
    from search.client import create_client, initialize_indices
    from search.config import KINDS, SearchConfig
    from workloads.observed import ObservedHistory

    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,80}", identifier):
        raise ValueError("Unsafe smoke identity")
    directory = Path(".cache/terrads-smoke") / identifier
    directory.mkdir(parents=True, exist_ok=False)
    history = ObservedHistory(root / "history")
    selected = next(
        (index for index, (_, items) in enumerate(history.groups) if 2 <= len(items) <= 20), None
    )
    if selected is None:
        raise ValueError("Serving smoke requires an actual multi-version Terraform file")
    history.groups = [history.groups[selected]]
    history.assets = [history.assets[selected]]
    snapshots = history.groups[0][1]
    kafka = KafkaConfig.from_env()
    offsets = latest_offsets(kafka)
    lake = replace(LakeConfig.from_env(), prefix=identifier)
    search = replace(SearchConfig.from_env(), index_prefix=identifier)
    checkpoint = directory / "checkpoint"
    environment = {
        **os.environ,
        "CONFIGSTREAM_LAKE_ENABLED": "true",
        "CONFIGSTREAM_SEARCH_ENABLED": "true",
        "CONFIGSTREAM_LAKE_PREFIX": identifier,
        "CONFIGSTREAM_SEARCH_INDEX_PREFIX": identifier,
        "SPARK_MASTER": "local[2]",
        "SPARK_DRIVER_MEMORY": "2g",
        "SPARK_CHECKPOINT_DIR": str(checkpoint),
        "SPARK_STARTING_OFFSETS": json.dumps({"config.snapshots": offsets}),
        "SPARK_WATERMARK_DELAY": "1000000 hours",
        "SPARK_STATE_TIMEOUT": "1000000 hours",
        "SPARK_MAX_VERSIONS_PER_ASSET": str(max(32, len(snapshots))),
    }
    with create_client(search) as client:
        initialize_indices(search, client)
        with KafkaProducer(kafka) as producer:
            for _, snapshot, _ in history.records():
                producer.publish(snapshot)
        with (
            (directory / "stdout.log").open("w") as output,
            (directory / "stderr.log").open("w") as errors,
        ):
            completed = subprocess.run(
                [sys.executable, "-m", "spark.streaming.main", "--available-now"],
                env=environment,
                stdout=output,
                stderr=errors,
                timeout=300,
            )
        if completed.returncode:
            raise ValueError("Real Terraform serving query failed; local diagnostics retained")
        client.indices.refresh(index=",".join(search.alias(kind) for kind in KINDS))
        with TestClient(create_app(ApiConfig(search=search))) as http:
            statuses = {path: http.get(path).status_code for path in ["/healthz", "/readyz"]}
            responses = {kind: http.get(f"/api/v1/{kind}").json() for kind in KINDS}
            latest = http.get(f"/api/v1/assets/{snapshots[0].asset_id}/latest-snapshot")
            if latest.status_code != 200 or latest.json()["version"] != len(snapshots):
                raise ValueError("Serving latest version mismatch")
            identities = {item.snapshot_id: item.version for item in snapshots}
            pairs = {
                (identities[item["old_snapshot_id"]], identities[item["new_snapshot_id"]])
                for item in responses["diffs"]["items"]
            }
            if pairs != {(version, version + 1) for version in range(1, len(snapshots))}:
                raise ValueError("Serving adjacent pairs differ from observed history")
            serialized = json.dumps(responses)
            if any(
                marker in serialized
                for marker in [
                    "normalized_content",
                    "details",
                    "SENSITIVE_TEST_MARKER",
                    "_index",
                    "_score",
                ]
            ):
                raise ValueError("Unsafe serving field exposed")
            result = {
                "id": identifier,
                "asset_id": snapshots[0].asset_id,
                "health": statuses,
                "latest_version": latest.json()["version"],
                "pairs": sorted(pairs),
                "api_counts": {kind: value["count"] for kind, value in responses.items()},
                "configuration_bodies_excluded": True,
                "query_id": json.loads((checkpoint / "metadata").read_text())["id"],
            }
            result["lake"] = committed_metrics(lake, result["query_id"])
            write_json(results / "serving-smoke.json", result)
            return result
