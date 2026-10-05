import json
import os
import re
import subprocess
import sys
from collections import Counter
from hashlib import sha256
from time import monotonic
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

from messaging.config import KafkaConfig
from messaging.kafka_producer import KafkaProducer
from processing import process_snapshot_pair
from processing.rules import evaluate_rules, load_rules
from schemas import Alert, ConfigSnapshot
from scripts.kafka_invalid_demo import publish_invalid_records
from scripts.kafka_stateful_demo import demo_history

pytestmark = pytest.mark.spark_integration


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


def changed(snapshot: ConfigSnapshot, **overrides) -> ConfigSnapshot:
    values = {**snapshot.model_dump(), **overrides}
    if values["content"] is not None:
        values["hash"] = sha256(values["content"].encode()).hexdigest()
    return ConfigSnapshot.model_validate(values)


def test_real_kafka_alerts_out_of_order_and_process_restart(tmp_path) -> None:
    kafka = KafkaConfig.from_env()
    prefix = f"phase6-test-{uuid4().hex[:8]}"
    histories = {name: demo_history(f"{prefix}-{name}") for name in ("A", "B", "C", "secret")}
    starting = latest_offsets("config.snapshots")
    alert_start = latest_offsets("config.alerts")
    checkpoint = tmp_path / "checkpoint"
    environment = {
        **os.environ,
        "SPARK_MASTER": "local[2]",
        "SPARK_CHECKPOINT_DIR": str(checkpoint),
        "SPARK_STARTING_OFFSETS": json.dumps({"config.snapshots": starting}),
        "SPARK_WATERMARK_DELAY": "10 minutes",
        "SPARK_STATE_TIMEOUT": "30 minutes",
        "SPARK_CONSOLE_ROWS": "100",
        "SPARK_MAX_OFFSETS_PER_TRIGGER": "10000",
    }

    def run(records) -> tuple[list[dict], str]:
        with KafkaProducer(kafka) as producer:
            for record in records:
                producer.publish(record)
        result = subprocess.run(
            [sys.executable, "-m", "spark.streaming.main", "--available-now"],
            env=environment,
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, result.stderr[-12000:]
        output = result.stdout + result.stderr
        assert "synthetic-before" not in output
        assert "synthetic-after" not in output
        summaries = [
            json.loads(match.group(1))
            for match in re.finditer(r": config_diff (\{[^\n]+\})", output)
        ]
        return summaries, output

    first, _ = run(
        [
            histories["A"][0],
            histories["B"][0],
            histories["C"][1],
            histories["A"][1],
            histories["B"][2],
        ]
    )
    assert [(item["asset_id"], item["old_version"], item["new_version"]) for item in first] == [
        (f"{prefix}-A", 1, 2)
    ]
    assert (checkpoint / "commits" / "0").exists()
    assert publish_invalid_records(kafka) == 4
    environment["SPARK_STARTING_OFFSETS"] = "earliest"
    secret_old = changed(
        histories["secret"][0], content="password synthetic-before\n# generated at 10:00\n"
    )
    secret_new = changed(
        histories["secret"][1], content="password synthetic-after\n# generated at 10:01\n"
    )
    conflict = changed(
        histories["C"][1], snapshot_id=f"{prefix}-conflict", content="logging enabled\n"
    )
    second, output = run(
        [
            histories["A"][0],
            histories["A"][1],
            histories["A"][2],
            histories["B"][1],
            histories["B"][1],
            conflict,
            histories["C"][2],
            secret_old,
            secret_new,
        ]
    )
    assert Counter(
        (item["asset_id"], item["old_version"], item["new_version"]) for item in second
    ) == Counter(
        {
            (f"{prefix}-A", 2, 3): 1,
            (f"{prefix}-B", 1, 2): 1,
            (f"{prefix}-B", 2, 3): 1,
            (f"{prefix}-C", 2, 3): 1,
            (f"{prefix}-secret", 1, 2): 1,
        }
    )
    assert '"reason":"version_conflict"' in output
    for reason in ("malformed_json", "wrong_record_type", "missing_asset_id", "kafka_key_mismatch"):
        assert reason in output
    secret_diff = next(item for item in second if item["asset_id"].endswith("-secret"))
    assert (secret_diff["added"], secret_diff["removed"], secret_diff["changed"]) == (0, 0, 0)
    third, _ = run([histories["A"][2], histories["B"][1]])
    assert third == []

    consumer = Consumer(
        {
            "bootstrap.servers": kafka.bootstrap_servers,
            "group.id": prefix,
            "enable.auto.commit": False,
        }
    )
    alerts = []
    try:
        ending = latest_offsets("config.alerts")
        consumer.assign(
            [
                TopicPartition("config.alerts", partition, offset)
                for partition, offset in alert_start.items()
            ]
        )
        deadline = monotonic() + 20
        while monotonic() < deadline:
            message = consumer.poll(0.5)
            if message is not None:
                assert not message.error()
                envelope = json.loads(message.value())
                if envelope["payload"]["asset_id"].startswith(prefix):
                    assert envelope["record_type"] == "alert"
                    alert = Alert.model_validate(envelope["payload"])
                    assert message.key().decode() == alert.asset_id
                    alerts.append(alert)
            positions = consumer.position(consumer.assignment())
            if all(position.offset >= ending[position.partition] for position in positions):
                break
    finally:
        consumer.close()
    assert len(alerts) == 5
    assert len({alert.alert_id for alert in alerts}) == 5
    assert Counter(alert.rule_id for alert in alerts) == {
        "logging-disabled": 2,
        "telnet-enabled": 3,
    }
    for alert in alerts:
        current = next(
            item
            for history in histories.values()
            for item in history
            if item.snapshot_id == alert.snapshot_id
        )
        assert alert.created_at == current.event_time
    expected_alerts = []
    expected_diffs = []
    rules = load_rules("rules/default.yaml")
    for name, previous_index, current_index in (
        ("A", 0, 1),
        ("A", 1, 2),
        ("B", 0, 1),
        ("B", 1, 2),
        ("C", 1, 2),
    ):
        previous = histories[name][previous_index]
        current = histories[name][current_index]
        diff = process_snapshot_pair(previous, current).diff
        expected_diffs.append(diff.diff_id)
        expected_alerts.extend(evaluate_rules(diff, current.event_time, rules))
    assert {
        item["diff_id"] for item in first + second if not item["asset_id"].endswith("-secret")
    } == set(expected_diffs)
    assert {alert.alert_id: alert.model_dump_json() for alert in alerts} == {
        alert.alert_id: alert.model_dump_json() for alert in expected_alerts
    }
