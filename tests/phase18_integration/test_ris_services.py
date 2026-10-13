"""Actual broker/checkpoint/S3 tests, owned bytes only, opt-in and isolated."""

import gzip
import json
import os
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import pytest

pytestmark = pytest.mark.lake_integration


@pytest.fixture(scope="module")
def service_session(tmp_path_factory):
    if os.environ.get("RUN_PHASE18_SERVICES") != "1":
        pytest.skip("explicit actual-service environment required")
    from lake.config import LakeConfig
    from spark.common.config import SparkConfig
    from spark.common.runtime import create_session

    root = tmp_path_factory.mktemp("ris-service")
    runtime = root / "runtime"
    runtime.mkdir()
    cached = Path(".cache/spark/ivy").resolve()
    if cached.exists():
        (runtime / "ivy").symlink_to(cached, target_is_directory=True)
    session = create_session(
        SparkConfig(runtime_dir=runtime, lake=LakeConfig.from_env(), driver_memory="2g")
    )
    yield session
    session.stop()


@pytest.mark.parametrize(
    "failure", ["before_output", "partial_output", "before_commit", "after_commit"]
)
def test_actual_ris_checkpoint_multiplicity(service_session, tmp_path, failure):
    from confluent_kafka import Producer
    from pyspark.errors import StreamingQueryException

    from lake.client import ensure_bucket
    from lake.config import LakeConfig
    from lake.ris import EventStore
    from messaging.config import KafkaConfig
    from messaging.ris import ensure_topic, publish
    from schemas.ris import EventChunk, RisReference
    from schemas.temporal.provenance import Artifact
    from spark.ris import RisProcessor, committed_events, exact_projection, start_query
    from workloads.ris_oracle import build

    fixture = json.loads(Path("tests/fixtures/phase16/mrt.json").read_text())
    data = bytes.fromhex(fixture["one_update_hex"]) * 2
    data += bytes.fromhex(fixture["malformed_bgp_body_hex"])
    archive = tmp_path / "owned.gz"
    archive.write_bytes(gzip.compress(data, mtime=0))
    artifact = Artifact(
        sha256=sha256(archive.read_bytes()).hexdigest(),
        bytes=archive.stat().st_size,
        safe_reference="fixture:phase18/owned.gz",
        revision="owned-v1",
    )
    oracle = tmp_path / "oracle"
    assert build(archive, artifact, oracle, source_id="owned")["accepted_occurrences"] == 4
    reference = RisReference.model_validate_json((oracle / "reference.json").read_text())
    query_id = str(uuid4())
    topic = f"temporal.ris.source.v1.{query_id}"
    kafka = KafkaConfig.from_env()
    ensure_topic(kafka, topic)
    lake = replace(LakeConfig.from_env(), prefix=f"phase18-fixtures/{query_id}")
    ensure_bucket(lake)
    store = EventStore(config=lake)
    result = publish(archive, reference, tmp_path / "progress.json", kafka, topic, chunk_size=2)
    assert result["acknowledged_occurrences"] == 4
    assert (
        publish(archive, reference, tmp_path / "progress.json", kafka, topic, chunk_size=2)[
            "acknowledged_occurrences"
        ]
        == 0
    )
    import pyarrow.parquet as parquet

    from schemas.ris import EventRow

    first = parquet.read_table(oracle / "events.parquet").to_pylist()[0]
    repeated = EventChunk(
        artifact_sha256=artifact.sha256, first_event=0, rows=(EventRow.model_validate(first),)
    )
    producer = Producer(kafka.producer_options())
    producer.produce(topic, key=artifact.sha256, value=repeated.model_dump_json())
    assert producer.flush(10) == 0
    assert (
        publish(archive, reference, tmp_path / "replay.json", kafka, topic, chunk_size=2)[
            "acknowledged_occurrences"
        ]
        == 4
    )
    fired = False

    def fault(point):
        nonlocal fired
        if point == failure and not fired:
            fired = True
            if point == "partial_output":
                return True
            raise RuntimeError("owned actual-service callback interruption")

    processor = RisProcessor(
        service_session,
        store,
        query_id,
        {artifact.sha256: reference},
        [str(oracle / "events.parquet")],
        fault=fault,
    )
    checkpoint = tmp_path / "checkpoint"
    query = start_query(
        service_session, processor, kafka.bootstrap_servers, checkpoint, topic, max_chunks=1
    )
    with pytest.raises(StreamingQueryException):
        query.awaitTermination(120)
    query.stop()
    assert fired
    before_restart = committed_events(service_session, store, query_id, processor.expected)
    assert before_restart.count() == (2 if failure == "after_commit" else 0)
    processor.fault = lambda point: None
    query = start_query(
        service_session, processor, kafka.bootstrap_servers, checkpoint, topic, max_chunks=1
    )
    assert query.awaitTermination(180)
    logical = committed_events(service_session, store, query_id, processor.expected)
    assert exact_projection(logical, processor.expected, complete=True).count() == 4
    assert logical.select("content_fingerprint").distinct().count() == 2
    assert sum(item["logical_records"] for item in store.inventory(query_id)) == 9
    commit = store.inventory(query_id)[0]
    key = f"batches/{query_id}/{commit['batch_id']:020d}"
    store.put(key, commit)
    with pytest.raises(ValueError, match="conflict"):
        store.put(key, commit | {"logical_records": 99})
    changed = repeated.model_dump()
    changed["rows"][0]["attribute_digest"] = "e" * 64
    producer.produce(topic, key=artifact.sha256, value=json.dumps(changed))
    assert producer.flush(10) == 0
    conflict_query = start_query(
        service_session, processor, kafka.bootstrap_servers, checkpoint, topic, max_chunks=1
    )
    with pytest.raises(StreamingQueryException):
        conflict_query.awaitTermination(120)
    conflict_query.stop()
    assert committed_events(service_session, store, query_id, processor.expected).count() == 4


def test_actual_invalid_transport_is_auditable(service_session, tmp_path):
    from confluent_kafka import Producer
    from pyspark.errors import StreamingQueryException

    from lake.config import LakeConfig
    from lake.ris import EventStore
    from messaging.config import KafkaConfig
    from messaging.ris import ensure_topic
    from spark.ris import RisProcessor, event_schema, start_query
    from tests.unit.test_ris import owned_rows

    reference, rows = owned_rows()
    path = str(tmp_path / "oracle")
    service_session.createDataFrame(
        [tuple(row.model_dump().values()) for row in rows], event_schema()
    ).write.parquet(path)
    query_id = str(uuid4())
    topic = f"temporal.ris.source.v1.{query_id}"
    kafka = KafkaConfig.from_env()
    ensure_topic(kafka, topic)
    producer = Producer(kafka.producer_options())
    producer.produce(topic, key=rows[0].artifact_sha256, value='{"transport_version":"invalid"}')
    assert producer.flush(10) == 0
    store = EventStore(config=replace(LakeConfig.from_env(), prefix=f"phase18-fixtures/{query_id}"))
    processor = RisProcessor(
        service_session, store, query_id, {rows[0].artifact_sha256: reference}, [path]
    )
    query = start_query(
        service_session, processor, kafka.bootstrap_servers, tmp_path / "checkpoint", topic
    )
    with pytest.raises(StreamingQueryException):
        query.awaitTermination(120)
    query.stop()
    assert store.inventory(query_id) == []
    assert store.get(f"errors/{query_id}/00000000000000000000") is not None
