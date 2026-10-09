import json
import os
import subprocess
import sys
from collections import Counter
from dataclasses import replace
from datetime import timedelta
from hashlib import sha256
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec
from pyspark.sql import functions as functions

from lake.client import ensure_bucket, list_commits, s3_client
from lake.config import LakeConfig
from messaging.config import KafkaConfig
from messaging.kafka_producer import KafkaProducer
from schemas import Alert, ConfigDiff, ConfigSnapshot
from scripts.kafka_invalid_demo import publish_invalid_records
from scripts.kafka_stateful_demo import demo_history
from spark.common.config import SparkConfig
from spark.common.runtime import create_session
from spark.streaming.sinks.lake import read_committed

pytestmark = pytest.mark.lake_integration


def latest_offsets(topic: str) -> dict[int, int]:
    admin = AdminClient(KafkaConfig.from_env().client_options())
    metadata = admin.list_topics(timeout=10).topics[topic]
    requests = {
        TopicPartition(topic, partition): OffsetSpec.latest() for partition in metadata.partitions
    }
    return {
        partition.partition: future.result(timeout=10).offset
        for partition, future in admin.list_offsets(requests, request_timeout=10).items()
    }


def test_kafka_spark_minio_history_failure_replay_and_restart(tmp_path) -> None:
    prefix = f"phase7-test-{uuid4().hex[:8]}"
    lake = replace(LakeConfig.from_env(), prefix=prefix)
    ensure_bucket(lake)
    kafka = KafkaConfig.from_env()
    history = demo_history(prefix)
    checkpoint = tmp_path / "checkpoint"
    environment = {
        **os.environ,
        "CONFIGSTREAM_LAKE_ENABLED": "true",
        "CONFIGSTREAM_LAKE_PREFIX": prefix,
        "SPARK_MASTER": "local[2]",
        "SPARK_CHECKPOINT_DIR": str(checkpoint),
        "SPARK_STARTING_OFFSETS": json.dumps(
            {"config.snapshots": latest_offsets("config.snapshots")}
        ),
        "SPARK_MAX_OFFSETS_PER_TRIGGER": "10000",
    }
    alert_start = latest_offsets("config.alerts")

    def run(records, fail_alerts: bool = False):
        with KafkaProducer(kafka) as producer:
            for record in records:
                producer.publish(record)
        command = [sys.executable, "-m", "spark.streaming.main", "--available-now"]
        if fail_alerts:
            command = [
                sys.executable,
                "-c",
                (
                    "from spark.streaming import outputs\n"
                    "from spark.streaming.main import main\n"
                    "def fail(batch, config):\n"
                    "    raise RuntimeError('injected Kafka alert sink failure')\n"
                    "outputs.write_alerts = fail\n"
                    "raise SystemExit(main(['--available-now']))\n"
                ),
            ]
        result = subprocess.run(
            command, env=environment, capture_output=True, text=True, timeout=180
        )
        assert result.returncode == (1 if fail_alerts else 0), result.stderr[-16000:]
        output = result.stdout + result.stderr
        assert "synthetic-before" not in output
        assert "synthetic-after" not in output
        assert lake.access_key not in output
        assert lake.secret_key not in output
        return output

    run([history[0]])
    query_id = json.loads((checkpoint / "metadata").read_text())["id"]
    assert list(list_commits(lake, query_id))
    assert publish_invalid_records(kafka) == 4
    conflict = ConfigSnapshot.model_validate(
        {**history[0].model_dump(), "snapshot_id": f"{prefix}-conflict"}
    )
    uri_snapshot = ConfigSnapshot.model_validate(
        {
            **history[0].model_dump(),
            "snapshot_id": f"{prefix}-uri",
            "asset_id": f"{prefix}-uri",
            "content": None,
            "content_uri": "s3://private-bucket/private-object",
        }
    )
    output = run([history[2], history[1], history[1], conflict, uri_snapshot], fail_alerts=True)
    assert "injected Kafka alert sink failure" in output
    client = s3_client(lake)

    def objects():
        return {
            entry["Key"]: entry["ETag"]
            for page in client.get_paginator("list_objects_v2").paginate(
                Bucket=lake.bucket, Prefix=f"{prefix}/"
            )
            for entry in page.get("Contents", [])
        }

    before = objects()
    assert any(key.endswith(".parquet") for key in before)
    environment["SPARK_STARTING_OFFSETS"] = "earliest"
    output = run([])
    assert "already committed; preserving Parquet" in output
    assert objects() == before
    next_content = "logging enabled\ntelnet enabled\npassword synthetic-next\n"
    fourth = ConfigSnapshot.model_validate(
        {
            **history[2].model_dump(),
            "snapshot_id": f"{prefix}-v4",
            "version": 4,
            "event_time": history[2].event_time + timedelta(seconds=1),
            "ingest_time": history[2].ingest_time + timedelta(seconds=1),
            "content": next_content,
            "hash": sha256(next_content.encode()).hexdigest(),
        }
    )
    run([history[2], fourth])

    session = create_session(
        replace(SparkConfig.from_env(), master="local[2]", lake=lake), kafka=False
    )
    try:
        raw = read_committed(session, lake, "raw/snapshots", query_id).collect()
        assert len(raw) == 12
        assert len({(row.kafka_topic, row.kafka_partition, row.kafka_offset) for row in raw}) == 12
        expected = {item.snapshot_id: item for item in (*history, fourth)}
        decoded = []
        for row in raw:
            try:
                envelope = json.loads(bytes(row.kafka_value))
                if envelope.get("record_type") == "config_snapshot":
                    decoded.append(envelope.get("payload", {}))
            except (ValueError, UnicodeDecodeError):
                pass
        for item in expected.values():
            assert any(value == item.model_dump(mode="json") for value in decoded)
        normalized = read_committed(session, lake, "processed/snapshots", query_id).collect()
        assert len(normalized) == 4
        assert {row.version for row in normalized} == {1, 2, 3, 4}
        for row in normalized:
            assert row.raw_hash == expected[row.snapshot_id].hash
            assert row.normalized_hash == sha256(row.normalized_content.encode()).hexdigest()
            assert "synthetic-" not in row.normalized_content
            assert (row.year, row.month, row.day) == (2026, 10, 5)
        diffs = read_committed(session, lake, "processed/diffs", query_id).collect()
        assert len(diffs) == 3
        versions = {row.snapshot_id: row.version for row in normalized}
        pairs = []
        for row in diffs:
            values = {name: row[name] for name in ConfigDiff.model_fields}
            values["details"] = [detail.asDict() for detail in row.details]
            diff = ConfigDiff.model_validate(values)
            pairs.append((versions[diff.old_snapshot_id], versions[diff.new_snapshot_id]))
        assert Counter(pairs) == {(1, 2): 1, (2, 3): 1, (3, 4): 1}
        alerts = (
            read_committed(session, lake, "processed/alerts", query_id)
            .select(functions.to_json(functions.struct(*Alert.model_fields)).alias("payload"))
            .collect()
        )
        assert len(alerts) == 2
        for row in alerts:
            alert = Alert.model_validate_json(row.payload)
            current = next(item for item in history if item.snapshot_id == alert.snapshot_id)
            assert alert.created_at == current.event_time
        diagnostics = read_committed(session, lake, "processed/diagnostics", query_id).collect()
        assert len(diagnostics) == 6
        assert "private-object" not in str(diagnostics)
        assert "SENSITIVE_TEST_MARKER" not in str(diagnostics)
        manifests = list(list_commits(lake, query_id))
        assert sum(item["counts"].get("normalized_snapshot", 0) for item in manifests) == 4
    finally:
        session.stop()
    consumer = Consumer(
        {
            "bootstrap.servers": kafka.bootstrap_servers,
            "group.id": prefix,
            "enable.auto.commit": False,
        }
    )
    try:
        consumer.assign(
            [
                TopicPartition("config.alerts", partition, offset)
                for partition, offset in alert_start.items()
            ]
        )
        received = []
        for _ in range(20):
            message = consumer.poll(0.5)
            if message is not None:
                assert not message.error()
                envelope = json.loads(message.value())
                if envelope["payload"]["asset_id"] == prefix:
                    assert message.key().decode() == prefix
                    received.append(Alert.model_validate(envelope["payload"]))
            if len(received) == 2:
                break
        assert len(received) == 2
        assert {item.rule_id for item in received} == {"logging-disabled", "telnet-enabled"}
    finally:
        consumer.close()
