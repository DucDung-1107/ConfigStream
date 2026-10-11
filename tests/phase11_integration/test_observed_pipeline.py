import json
import os
import subprocess
import sys
from dataclasses import replace
from uuid import uuid4

import pytest

from analytics.config import AnalyticsConfig
from analytics.main import run_analytics
from benchmarks.main import committed_metrics, latest_offsets
from lake.config import LakeConfig
from messaging.config import KafkaConfig
from messaging.kafka_producer import KafkaProducer
from spark.common.config import SparkConfig
from spark.common.runtime import create_session
from workloads.empirical import measure
from workloads.observed import ObservedHistory, load_dataset

pytestmark = pytest.mark.phase11_integration


def test_observed_git_pipeline_and_analytics(observed_fixture, tmp_path):
    root, _, _ = observed_fixture
    snapshots, provenance, _ = load_dataset(root)
    assert measure(snapshots, provenance)["reverts"]["count"] == 1
    kafka = KafkaConfig.from_env()
    offsets = latest_offsets(kafka)
    history = ObservedHistory(root)
    with KafkaProducer(kafka) as producer:
        for _, snapshot, _ in history.records():
            producer.publish(snapshot)
    namespace = f"phase11-test-{uuid4().hex[:8]}"
    lake = replace(LakeConfig.from_env(), prefix=namespace)
    checkpoint = tmp_path / "checkpoint"
    environment = {
        **os.environ,
        "CONFIGSTREAM_LAKE_ENABLED": "true",
        "CONFIGSTREAM_SEARCH_ENABLED": "false",
        "CONFIGSTREAM_LAKE_PREFIX": namespace,
        "SPARK_MASTER": "local[2]",
        "SPARK_CHECKPOINT_DIR": str(checkpoint),
        "SPARK_STARTING_OFFSETS": json.dumps({"config.snapshots": offsets}),
        "SPARK_WATERMARK_DELAY": "1000000 hours",
        "SPARK_STATE_TIMEOUT": "1000000 hours",
        "CONFIGSTREAM_RULES_FILE": "rules/default.yaml",
    }
    completed = subprocess.run(
        [sys.executable, "-m", "spark.streaming.main", "--available-now"],
        env=environment,
        capture_output=True,
        text=True,
        timeout=240,
    )
    assert completed.returncode == 0, completed.stderr[-2000:]
    assert "SENSITIVE_TEST_MARKER" not in completed.stdout + completed.stderr
    query_id = json.loads((checkpoint / "metadata").read_text())["id"]
    counts = committed_metrics(lake, query_id)["counts"]
    assert counts["normalized_snapshot"] == 7
    assert counts["config_diff"] == 4
    assert counts["alert"] == 1
    session = create_session(SparkConfig(master="local[2]", lake=lake), kafka=False)
    try:
        from pyspark.sql import functions as functions

        from spark.streaming.sinks.lake import read_committed

        metadata = read_committed(session, lake, "processed/snapshots", query_id).select(
            "snapshot_id",
            "asset_id",
            "version",
            "raw_hash",
            functions.date_format("event_time", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("utc"),
        )
        assert {tuple(row) for row in metadata.collect()} == {
            (
                snapshot.snapshot_id,
                snapshot.asset_id,
                snapshot.version,
                snapshot.hash,
                snapshot.event_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            )
            for snapshot in snapshots
        }
        pairs = read_committed(session, lake, "processed/diffs", query_id).select(
            "old_snapshot_id", "new_snapshot_id"
        )
        assert {tuple(row) for row in pairs.collect()} == {
            (previous.snapshot_id, current.snapshot_id)
            for previous in snapshots
            for current in snapshots
            if previous.asset_id == current.asset_id and current.version == previous.version + 1
        }
        manifest = run_analytics(
            session, lake, AnalyticsConfig(window="1 day"), query_id, job="change-frequency"
        )
        frame = session.read.parquet(manifest["datasets"]["change-frequency"]["path"])
        assert frame.groupBy().sum("snapshot_count").first()[0] == 7
        assert frame.groupBy().sum("diff_count").first()[0] == 4
        assert frame.groupBy().sum("alert_count").first()[0] == 1
    finally:
        session.stop()
