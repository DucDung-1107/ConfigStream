import json
import os
import subprocess
import sys
from dataclasses import replace
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

from lake.client import ensure_bucket, list_commits, s3_client
from lake.config import LakeConfig
from messaging.config import KafkaConfig
from messaging.kafka_producer import KafkaProducer
from schemas import Alert, ConfigSnapshot
from scripts.kafka_invalid_demo import publish_invalid_records
from scripts.kafka_stateful_demo import demo_history
from search.client import create_client, initialize_indices
from search.config import KINDS, SearchConfig
from search.queries import aggregate, query_documents
from search.reindex import rebuild
from spark.common.config import SparkConfig
from spark.common.runtime import create_session
from spark.streaming.sinks.lake import read_committed

pytestmark = pytest.mark.search_integration


def latest_offsets(topic):
    admin = AdminClient(KafkaConfig.from_env().client_options())
    partitions = admin.list_topics(timeout=10).topics[topic].partitions
    requests = {TopicPartition(topic, partition): OffsetSpec.latest() for partition in partitions}
    return {
        partition.partition: future.result(timeout=10).offset
        for partition, future in admin.list_offsets(requests, request_timeout=10).items()
    }


def test_live_history_failure_restart_rebuild_twice(tmp_path):
    namespace = f"phase8-pipeline-{uuid4().hex[:8]}"
    lake = replace(LakeConfig.from_env(), prefix=namespace)
    search = replace(SearchConfig.from_env(), index_prefix=namespace, bulk_size=2)
    ensure_bucket(lake)
    kafka = KafkaConfig.from_env()
    normal = demo_history(namespace + "-normal")
    out_of_order = demo_history(namespace + "-out-of-order")
    checkpoint = tmp_path / "checkpoint"
    environment = {
        **os.environ,
        "CONFIGSTREAM_LAKE_ENABLED": "true",
        "CONFIGSTREAM_SEARCH_ENABLED": "true",
        "CONFIGSTREAM_LAKE_PREFIX": namespace,
        "CONFIGSTREAM_SEARCH_INDEX_PREFIX": namespace,
        "CONFIGSTREAM_SEARCH_BULK_SIZE": "2",
        "SPARK_MASTER": "local[2]",
        "SPARK_CHECKPOINT_DIR": str(checkpoint),
        "SPARK_STARTING_OFFSETS": json.dumps(
            {"config.snapshots": latest_offsets("config.snapshots")}
        ),
    }
    alert_offsets = latest_offsets("config.alerts")
    with create_client(search) as client:
        initialize_indices(search, client)

        def counts():
            client.indices.refresh(index=",".join(search.alias(kind) for kind in KINDS))
            return {kind: client.count(index=search.alias(kind))["count"] for kind in KINDS}

        def run(records, unavailable=False):
            with KafkaProducer(kafka) as producer:
                for record in records:
                    producer.publish(record)
            command = [sys.executable, "-m", "spark.streaming.main", "--available-now"]
            if unavailable:
                command = [
                    sys.executable,
                    "-c",
                    (
                        "from dataclasses import replace\n"
                        "from search import sink\n"
                        "from spark.streaming.main import main\n"
                        "original = sink.index_batch\n"
                        "def unavailable(batch, config):\n"
                        "    unavailable_search = replace(config.search, "
                        "url='http://localhost:1', timeout_seconds=1)\n"
                        "    original(batch, replace(config, search=unavailable_search))\n"
                        "sink.index_batch = unavailable\n"
                        "raise SystemExit(main(['--available-now']))\n"
                    ),
                ]
            result = subprocess.run(
                command, env=environment, text=True, capture_output=True, timeout=240
            )
            assert result.returncode == (1 if unavailable else 0), result.stderr[-12000:]
            output = result.stdout + result.stderr
            for forbidden in (
                lake.access_key,
                lake.secret_key,
                "synthetic-before",
                "synthetic-after",
                "SENSITIVE_TEST_MARKER",
            ):
                assert forbidden not in output
            return output

        try:
            run([normal[0], normal[1], out_of_order[0], out_of_order[2], out_of_order[1]])
            assert counts() == {"snapshots": 5, "diffs": 3, "alerts": 3, "diagnostics": 0}
            latest = query_documents(
                client, search, "snapshots", asset_id=normal[0].asset_id, latest=True
            )
            assert latest["hits"]["hits"][0]["_source"]["version"] == 2
            query_id = json.loads((checkpoint / "metadata").read_text())["id"]
            conflict = ConfigSnapshot.model_validate(
                {**normal[1].model_dump(), "snapshot_id": namespace + "-conflict"}
            )
            assert publish_invalid_records(kafka) == 4
            output = run([normal[2], normal[1], conflict], unavailable=True)
            assert "Search bulk request failed" in output
            control = s3_client(lake)

            def objects():
                return {
                    entry["Key"]: entry["ETag"]
                    for page in control.get_paginator("list_objects_v2").paginate(
                        Bucket=lake.bucket, Prefix=namespace + "/"
                    )
                    for entry in page.get("Contents", [])
                }

            before = objects()
            assert any(key.endswith(".parquet") for key in before)
            assert (
                sum(
                    item["counts"].get("normalized_snapshot", 0)
                    for item in list_commits(lake, query_id)
                )
                == 6
            )
            environment["SPARK_STARTING_OFFSETS"] = "earliest"
            output = run([])
            assert "already committed; preserving Parquet" in output
            assert objects() == before
            expected = {"snapshots": 6, "diffs": 4, "alerts": 4, "diagnostics": 5}
            assert counts() == expected
            run([normal[2]])
            assert counts() == expected
            assert (
                query_documents(
                    client, search, "snapshots", asset_id=normal[0].asset_id, latest=True
                )["hits"]["hits"][0]["_source"]["version"]
                == 3
            )
            for history in (normal, out_of_order):
                versions = {item.snapshot_id: item.version for item in history}
                diffs = query_documents(client, search, "diffs", asset_id=history[0].asset_id)[
                    "hits"
                ]["hits"]
                assert {
                    (
                        versions[item["_source"]["old_snapshot_id"]],
                        versions[item["_source"]["new_snapshot_id"]],
                    )
                    for item in diffs
                } == {(1, 2), (2, 3)}
                for severity in ("high", "critical"):
                    found = query_documents(
                        client,
                        search,
                        "alerts",
                        asset_id=history[0].asset_id,
                        severity=severity,
                        start="2026-10-05T00:00:00Z",
                        end="2026-10-06T00:00:00Z",
                    )
                    assert found["hits"]["total"]["value"] == 1
            assert (
                query_documents(client, search, "diagnostics", diagnostic_type="version_conflict")[
                    "hits"
                ]["total"]["value"]
                == 1
            )
            assert (
                query_documents(client, search, "diagnostics", diagnostic_type="invalid_record")[
                    "hits"
                ]["total"]["value"]
                == 4
            )
            assert (
                query_documents(
                    client,
                    search,
                    "diagnostics",
                    diagnostic_type="invalid_record",
                    start="2026-01-01T00:00:00Z",
                    end="2099-01-01T00:00:00Z",
                )["hits"]["total"]["value"]
                == 4
            )
            assert {
                item["key"]: item["doc_count"]
                for item in aggregate(client, search, "severity")["counts"]["buckets"]
            } == {"high": 2, "critical": 2}
            assert {
                item["key"]: item["doc_count"]
                for item in aggregate(client, search, "rule_id")["counts"]["buckets"]
            } == {"logging-disabled": 2, "telnet-enabled": 2}
            assert len(aggregate(client, search, "asset_id")["counts"]["buckets"]) == 2
            assert (
                sum(
                    item["doc_count"]
                    for item in aggregate(client, search, "diffs_per_hour")["counts"]["buckets"]
                )
                == 4
            )

            session = create_session(
                replace(SparkConfig.from_env(), master="local[2]", lake=lake), kafka=False
            )
            try:
                assert read_committed(session, lake, "raw/snapshots", query_id).count() == 13
                assert read_committed(session, lake, "processed/snapshots", query_id).count() == 6
                assert read_committed(session, lake, "processed/diffs", query_id).count() == 4
                client.indices.delete(index=",".join(search.index(kind) for kind in KINDS))
                initialize_indices(search, client)
                assert counts() == dict.fromkeys(KINDS, 0)
                rebuild(session, lake, search, query_id)
                assert counts() == expected
                first_ids = {
                    kind: sorted(
                        hit["_id"]
                        for hit in client.search(index=search.alias(kind), size=100)["hits"]["hits"]
                    )
                    for kind in KINDS
                }
                rebuild(session, lake, search, query_id)
                assert counts() == expected
                assert first_ids == {
                    kind: sorted(
                        hit["_id"]
                        for hit in client.search(index=search.alias(kind), size=100)["hits"]["hits"]
                    )
                    for kind in KINDS
                }
                for kind in KINDS:
                    documents = client.search(index=search.alias(kind), size=100)["hits"]["hits"]
                    assert not any(
                        marker in json.dumps(documents)
                        for marker in (
                            "synthetic-before",
                            "synthetic-after",
                            "SENSITIVE_TEST_MARKER",
                            "normalized_content",
                            '"details"',
                        )
                    )
            finally:
                session.stop()

            consumer = Consumer(
                {
                    "bootstrap.servers": kafka.bootstrap_servers,
                    "group.id": namespace,
                    "enable.auto.commit": False,
                }
            )
            try:
                consumer.assign(
                    [
                        TopicPartition("config.alerts", partition, offset)
                        for partition, offset in alert_offsets.items()
                    ]
                )
                received = []
                for _ in range(30):
                    message = consumer.poll(0.5)
                    if message is not None:
                        assert not message.error()
                        envelope = json.loads(message.value())
                        if envelope["payload"]["asset_id"] in {
                            normal[0].asset_id,
                            out_of_order[0].asset_id,
                        }:
                            alert = Alert.model_validate(envelope["payload"])
                            assert envelope["record_type"] == "alert"
                            assert message.key().decode() == alert.asset_id
                            received.append(alert.alert_id)
                    if len(received) == 4:
                        break
                assert len(set(received)) == 4
            finally:
                consumer.close()
        finally:
            client.indices.delete(
                index=",".join(search.index(kind) for kind in KINDS), ignore_unavailable=True
            )
