import json
import os
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest
from confluent_kafka import TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

from analytics.config import AnalyticsConfig
from analytics.main import run_analytics
from lake.client import list_commits
from lake.config import LakeConfig
from messaging.config import KafkaConfig
from messaging.kafka_producer import KafkaProducer
from spark.common.config import SparkConfig
from spark.common.runtime import create_session
from workloads.history import History, WorkloadConfig

pytestmark = pytest.mark.phase10_integration


def latest_offsets(config):
    admin = AdminClient(config.client_options())
    partitions = admin.list_topics(timeout=10).topics["config.snapshots"].partitions
    requests = {
        TopicPartition("config.snapshots", partition): OffsetSpec.latest()
        for partition in partitions
    }
    return {
        str(partition.partition): future.result(timeout=10).offset
        for partition, future in admin.list_offsets(requests, request_timeout=10).items()
    }


def test_real_fixture_history_and_committed_analytics(tmp_path):
    namespace = f"phase10-test-{uuid4().hex[:8]}"
    lake = replace(LakeConfig.from_env(), prefix=namespace)
    kafka = KafkaConfig.from_env()
    offsets = latest_offsets(kafka)
    history = History(
        WorkloadConfig(assets=6, versions_per_asset=6, corpus=Path("tests/fixtures/phase10"))
    )
    records = list(history.records())
    with KafkaProducer(kafka) as producer:
        for _, snapshot, _ in records:
            producer.publish(snapshot)
    environment = {
        **os.environ,
        "CONFIGSTREAM_LAKE_ENABLED": "true",
        "CONFIGSTREAM_SEARCH_ENABLED": "false",
        "CONFIGSTREAM_LAKE_PREFIX": namespace,
        "SPARK_CHECKPOINT_DIR": str(tmp_path / "checkpoint"),
        "SPARK_MASTER": "local[2]",
        "SPARK_STARTING_OFFSETS": json.dumps({"config.snapshots": offsets}),
        "SPARK_WATERMARK_DELAY": "48 hours",
        "SPARK_STATE_TIMEOUT": "87600 hours",
        "CONFIGSTREAM_RULES_FILE": "workloads/rules.yaml",
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
    query_id = json.loads((tmp_path / "checkpoint/metadata").read_text())["id"]
    counts = {}
    for commit in list_commits(lake, query_id):
        for name, count in commit["counts"].items():
            counts[name] = counts.get(name, 0) + count
    assert counts["normalized_snapshot"] == 36
    assert counts["config_diff"] == 30
    assert counts["alert"] == 12
    session = create_session(SparkConfig(master="local[2]", lake=lake), kafka=False)
    try:
        manifest = run_analytics(session, lake, AnalyticsConfig(window="1 hour"), query_id)
        assert run_analytics(session, lake, AnalyticsConfig(window="1 hour"), query_id) == manifest
        frequency = session.read.parquet(manifest["datasets"]["change-frequency"]["path"])
        assert frequency.groupBy().sum("snapshot_count").first()[0] == 36
        fleet = session.read.parquet(manifest["datasets"]["fleet-activity"]["path"])
        assert (
            fleet.filter("window_start = timestamp '2026-01-03 00:00:00'").first().assets_changed
            == 6
        )
        bursts = session.read.parquet(manifest["datasets"]["fleet-bursts"]["path"])
        assert bursts.filter("is_anomaly").count() == 1
        assert bursts.filter("is_anomaly").first().window_start.day == 3
    finally:
        session.stop()
