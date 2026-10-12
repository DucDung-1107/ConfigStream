"""Explicit service gate, never enabled by normal offline CI."""

import os
import socket
from dataclasses import replace
from uuid import uuid4

import pytest

pytestmark = pytest.mark.lake_integration


def test_actual_kafka_minio_checkpoint_retry_and_legacy_isolation(
    rpki_fixture, tmp_path, monkeypatch
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
        put = store.put

        def after_raw_commit(key, value):
            put(key, value)
            if value.get("dataset") == "source_rows":
                raise RuntimeError("owned actual-service interruption after commit")

        monkeypatch.setattr(store, "put", after_raw_commit)
        query = start_query(session, processor, kafka.bootstrap_servers, checkpoint, topic=topic)
        with pytest.raises(StreamingQueryException, match="interruption"):
            query.awaitTermination(120)
        query.stop()
        assert committed_rows(session, store, identity).count() == 7
        with pytest.raises(ValueError, match="incomplete"):
            read_state(session, store, next(iter(references.values())))
        monkeypatch.setattr(store, "put", put)
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
