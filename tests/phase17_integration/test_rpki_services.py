"""Explicit service gate, never enabled by normal offline CI."""

import json
import os
import socket
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest

pytestmark = pytest.mark.lake_integration


@pytest.mark.parametrize(
    "failure_point",
    [
        "before_raw_output",
        "partial_raw_output",
        "before_raw_commit",
        "after_raw_commit",
        "before_day_completion",
        "after_members_write",
        "after_day_completion",
        "after_boundary",
        "invalid_transport",
    ],
)
def test_actual_kafka_minio_checkpoint_retry_and_legacy_isolation(
    rpki_fixture, tmp_path, monkeypatch, failure_point
):
    if os.environ.get("RUN_PHASE17_SERVICES") != "1":
        pytest.skip("explicit Phase 17 service authorization/environment required")
    for port in (9092, 9000):
        with socket.create_connection(("127.0.0.1", port), timeout=2):
            pass
    from pyspark.errors import StreamingQueryException

    from lake.client import ensure_bucket
    from lake.config import LakeConfig
    from lake.rpki import MinioStore
    from messaging.config import KafkaConfig
    from messaging.rpki import ensure_topic, publish_source
    from spark.common.config import SparkConfig
    from spark.common.runtime import create_session
    from spark.rpki import committed_rows, read_state, reconstruct
    from spark.rpki_stream import RpkiProcessor, start_query
    from spark.streaming.sinks.lake import read_committed
    from workloads.rpki_oracle import run_oracle
    from workloads.rpki_source import disk_guard

    disk_guard(tmp_path)
    cache, selection, oracle, _, references = rpki_fixture
    summary = run_oracle(selection, cache, oracle)
    identity = str(uuid4())
    topic = f"temporal.rpki.fixture.{identity}"
    kafka = KafkaConfig.from_env()
    ensure_topic(kafka, topic=topic)
    lake = replace(LakeConfig.from_env(), prefix=f"phase17-fixtures/{identity}")
    ensure_bucket(lake)
    cached_ivy = Path(".cache/spark/ivy").resolve()
    if cached_ivy.exists():
        (tmp_path / "runtime").mkdir()
        (tmp_path / "runtime" / "ivy").symlink_to(cached_ivy, target_is_directory=True)
    session = create_session(
        SparkConfig(master="local[2]", runtime_dir=tmp_path / "runtime", lake=lake)
    )
    store = MinioStore(lake)
    checkpoint = tmp_path / "checkpoint"
    processor = RpkiProcessor(session, store, identity, references, oracle, summary["boundaries"])
    try:
        for reference, artifact in zip(references.values(), selection["artifacts"], strict=True):
            result = publish_source(
                cache / artifact["file"],
                reference,
                tmp_path / f"{reference.observation_id}.json",
                kafka,
                chunk_size=2,
                topic=topic,
            )
            assert result["acknowledged_rows_this_run"] in {3, 4}
        from confluent_kafka import Consumer, Producer, TopicPartition

        from workloads.rpki_source import source_chunks

        first = next(iter(references.values()))
        repeated = next(
            source_chunks(cache / selection["artifacts"][0]["file"], first, chunk_size=2)
        )
        producer = Producer(kafka.producer_options())
        for _ in range(2):
            producer.produce(topic, key=first.observation_id, value=repeated.model_dump_json())
        assert producer.flush(10) == 0
        consumer = Consumer(
            kafka.client_options() | {"group.id": str(uuid4()), "enable.auto.commit": False}
        )
        consumer.assign([TopicPartition(topic, partition, 0) for partition in range(2)])
        try:
            received = consumer.consume(6, timeout=10)
            assert len(received) == 6
            for message in received:
                assert message.error() is None
                contents = json.loads(message.value())
                assert contents["transport_version"] == "rpki-source-1.0"
                assert message.key().decode() == contents["observation_id"]
        finally:
            consumer.close()
        if failure_point == "invalid_transport":
            producer.produce(
                topic, key=first.observation_id, value='{"transport_version":"invalid"}'
            )
            assert producer.flush(10) == 0
            query = start_query(
                session, processor, kafka.bootstrap_servers, checkpoint, topic=topic
            )
            with pytest.raises(StreamingQueryException, match="Invalid RPKI chunk"):
                query.awaitTermination(120)
            query.stop()
            assert committed_rows(session, store, identity).count() == 0
            assert not store.list("state")
            assert store.list(f"quarantine/{identity}")
            return
        put = store.put

        import spark.rpki as transforms

        write = transforms.write_parquet

        def interrupted_write(frame, path):
            if "/source_rows/" in path:
                if failure_point == "before_raw_output":
                    raise RuntimeError("owned actual-service interruption before output")
                if failure_point == "partial_raw_output":
                    write(frame.limit(1), path)
                    raise RuntimeError("owned actual-service interruption after partial output")
            write(frame, path)
            if "/members/" in path and failure_point == "after_members_write":
                raise RuntimeError("owned actual-service interruption after members write")

        monkeypatch.setattr(transforms, "write_parquet", interrupted_write)

        def after_raw_commit(key, value):
            if value.get("dataset") == "source_rows" and failure_point == "before_raw_commit":
                raise RuntimeError("owned actual-service interruption before raw commit")
            if key.startswith("state/") and failure_point == "before_day_completion":
                raise RuntimeError("owned actual-service interruption before day completion")
            put(key, value)
            if (
                (value.get("dataset") == "source_rows" and failure_point == "after_raw_commit")
                or (key.startswith("state/") and failure_point == "after_day_completion")
                or (key.startswith("boundary/") and failure_point == "after_boundary")
            ):
                raise RuntimeError("owned actual-service interruption after commit")

        monkeypatch.setattr(store, "put", after_raw_commit)
        query = start_query(session, processor, kafka.bootstrap_servers, checkpoint, topic=topic)
        with pytest.raises(StreamingQueryException, match="interruption"):
            query.awaitTermination(120)
        query.stop()
        assert committed_rows(session, store, identity).count() == (
            0
            if failure_point in {"before_raw_output", "partial_raw_output", "before_raw_commit"}
            else 7
        )
        if failure_point not in {"after_day_completion", "after_boundary"}:
            with pytest.raises(ValueError, match="incomplete"):
                read_state(session, store, next(iter(references.values())))
        monkeypatch.setattr(store, "put", put)
        monkeypatch.setattr(transforms, "write_parquet", write)
        resumed = start_query(session, processor, kafka.bootstrap_servers, checkpoint, topic=topic)
        assert resumed.awaitTermination(120)
        resumed.stop()
        first, second = references.values()
        assert (
            read_state(session, store, first).count()
            == read_state(session, store, second).count()
            == 3
        )
        assert reconstruct(session, store, [first, second]).count() == 3
        raw = store.list(f"raw/{identity}")[0]
        put(f"raw/{raw['identity']}", raw)
        with pytest.raises(ValueError, match="conflict"):
            put(f"raw/{raw['identity']}", raw | {"digest": "f" * 64})
        assert read_committed(session, lake, "processed/snapshots", manifests=[]).count() == 0
    finally:
        for query in session.streams.active:
            query.stop()
        session.stop()
