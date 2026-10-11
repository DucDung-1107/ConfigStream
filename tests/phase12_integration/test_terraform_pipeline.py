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

pytestmark = pytest.mark.phase12_integration


def test_terraform_git_kafka_state_lake_and_analytics(terraform_fixture, tmp_path):
    root, _, _ = terraform_fixture
    snapshots, provenance, _ = load_dataset(root)
    assert measure(snapshots, provenance)["reverts"]["count"] == 1
    kafka = KafkaConfig.from_env()
    offsets = latest_offsets(kafka)
    with KafkaProducer(kafka) as producer:
        for _, snapshot, _ in ObservedHistory(root).records():
            producer.publish(snapshot)
    namespace = f"phase12-fixture-{uuid4().hex[:8]}"
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
    }
    result = subprocess.run(
        [sys.executable, "-m", "spark.streaming.main", "--available-now"],
        env=environment,
        capture_output=True,
        text=True,
        timeout=240,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    assert "SENSITIVE_TEST_MARKER" not in result.stdout + result.stderr
    query_id = json.loads((checkpoint / "metadata").read_text())["id"]
    counts = committed_metrics(lake, query_id)["counts"]
    assert counts["normalized_snapshot"] == 8
    assert counts["config_diff"] == 3
    assert counts.get("alert", 0) == 0
    session = create_session(SparkConfig(master="local[2]", lake=lake), kafka=False)
    try:
        from pyspark.sql import functions

        from spark.streaming.sinks.lake import read_committed

        stored = read_committed(session, lake, "processed/snapshots", query_id).select(
            "snapshot_id",
            "asset_id",
            "version",
            functions.date_format("event_time", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_time"),
        )
        assert {tuple(row) for row in stored.collect()} == {
            (
                item.snapshot_id,
                item.asset_id,
                item.version,
                item.event_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            )
            for item in snapshots
        }
        pairs = read_committed(session, lake, "processed/diffs", query_id).select(
            "old_snapshot_id", "new_snapshot_id"
        )
        assert len(pairs.collect()) == 3
        assert {tuple(row) for row in pairs.collect()} == {
            (old.snapshot_id, new.snapshot_id)
            for old in snapshots
            for new in snapshots
            if old.asset_id == new.asset_id and new.version == old.version + 1
        }
        manifest = run_analytics(session, lake, AnalyticsConfig(), query_id, "change-frequency")
        frame = session.read.parquet(manifest["datasets"]["change-frequency"]["path"])
        assert frame.groupBy().sum("snapshot_count").first()[0] == 8
        assert frame.groupBy().sum("diff_count").first()[0] == 3
    finally:
        session.stop()
