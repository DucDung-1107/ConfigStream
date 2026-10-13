"""Owned isolated S3 corruption tests; no mutation of previous research prefixes."""

import gzip
import json
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import pytest

from tests.phase18_integration.test_ris_services import service_session as ris_service_fixture

pytestmark = pytest.mark.lake_integration
service_session = ris_service_fixture


@pytest.mark.parametrize(
    "damage",
    [
        "missing_source",
        "corrupt_source",
        "missing_parquet",
        "corrupt_parquet",
        "missing_exclusions",
        "conflicting_exclusions",
        "missing_batch",
        "missing_window",
    ],
)
def test_actual_isolated_integrity(service_session, tmp_path, damage):
    from pyspark.errors import AnalysisException

    from lake.client import s3_client
    from lake.config import LakeConfig
    from lake.ris import (
        EventStore,
        publish_accounting,
        publish_window,
        read_accounting,
        read_window,
    )
    from schemas.ris import EventChunk, RisReference
    from schemas.temporal.provenance import Artifact
    from spark.ris import RisProcessor, completed_events, projection_digest
    from workloads.ris_oracle import build

    fixture = json.loads(Path("tests/fixtures/phase16/mrt.json").read_text())
    data = bytes.fromhex(fixture["one_update_hex"]) * 2
    data += bytes.fromhex(fixture["malformed_bgp_body_hex"])
    archive = tmp_path / "owned.gz"
    archive.write_bytes(gzip.compress(data, mtime=0))
    artifact = Artifact(
        sha256=sha256(archive.read_bytes()).hexdigest(),
        bytes=archive.stat().st_size,
        safe_reference="fixture:phase18-closure/owned.gz",
        revision="owned-v1",
    )
    oracle = tmp_path / "oracle"
    summary = build(archive, artifact, oracle, source_id="owned")
    reference = RisReference.model_validate_json((oracle / "reference.json").read_text())
    import pyarrow.parquet as parquet

    values = parquet.read_table(oracle / "events.parquet").to_pylist()
    chunk = EventChunk(artifact_sha256=artifact.sha256, first_event=0, rows=values)
    batch = service_session.createDataFrame([(chunk.model_dump_json(),)], "value STRING")
    query_id = str(uuid4())
    lake = replace(LakeConfig.from_env(), prefix=f"phase18-closure-fixtures/{query_id}")
    store = EventStore(config=lake)
    processor = RisProcessor(
        service_session,
        store,
        query_id,
        {artifact.sha256: reference},
        [str(oracle / "events.parquet")],
    )
    processor.process(batch, 0)
    assert store.get(f"windows/{query_id}") is None
    exclusions = json.loads((oracle / "exclusions.json").read_text())
    publish_accounting(store, reference, summary, exclusions)
    publish_accounting(store, reference, summary, exclusions)
    with pytest.raises(ValueError, match="conflict"):
        store.put(f"exclusions/{artifact.sha256}", [])
    count, digest = projection_digest(processor.expected)
    publish_window(store, query_id, [artifact.sha256], count, digest)
    assert completed_events(service_session, store, query_id, processor.expected).count() == 4
    client = s3_client(lake)
    metadata_key = {
        "missing_source": f"sources/{reference.manifest.manifest_id}",
        "corrupt_source": f"sources/{reference.manifest.manifest_id}",
        "missing_exclusions": f"exclusions/{artifact.sha256}",
        "conflicting_exclusions": f"exclusions/{artifact.sha256}",
        "missing_batch": f"batches/{query_id}/00000000000000000000",
        "missing_window": f"windows/{query_id}",
    }
    if damage == "corrupt_parquet":
        commit = store.inventory(query_id)[0]
        processor.expected.limit(1).write.mode("overwrite").parquet(commit["path"])
    elif damage == "missing_parquet":
        prefix = lake.object_key(f"temporal/ris/occurrences/physical-v1/{query_id}/")
        for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=lake.bucket, Prefix=prefix
        ):
            for item in page.get("Contents", []):
                client.delete_object(Bucket=lake.bucket, Key=item["Key"])
    else:
        key = lake.object_key(store.key(metadata_key[damage]))
        if damage.startswith("missing"):
            client.delete_object(Bucket=lake.bucket, Key=key)
        else:
            client.put_object(
                Bucket=lake.bucket, Key=key, Body=b"{}", ContentType="application/json"
            )
    with pytest.raises((ValueError, AnalysisException)):
        completed_events(service_session, store, query_id, processor.expected).count()
    if damage in {"missing_exclusions", "conflicting_exclusions"}:
        with pytest.raises(ValueError):
            read_accounting(store, artifact.sha256)
    if damage in {"missing_batch", "missing_window"}:
        with pytest.raises(ValueError):
            read_window(store, query_id)


def test_actual_cross_archive_replay_and_window_interruption(service_session, tmp_path):
    from pyspark.errors import StreamingQueryException

    from lake.config import LakeConfig
    from lake.ris import EventStore, publish_accounting, publish_window, read_window
    from messaging.config import KafkaConfig
    from messaging.ris import ensure_topic, publish
    from schemas.ris import RisReference
    from schemas.temporal.provenance import Artifact
    from spark.ris import (
        RisProcessor,
        committed_events,
        exact_projection,
        projection_digest,
        start_query,
    )
    from workloads.ris_oracle import build

    fixture = json.loads(Path("tests/fixtures/phase16/mrt.json").read_text())
    data = bytes.fromhex(fixture["one_update_hex"]) * 2
    query_id = str(uuid4())
    store = EventStore(
        config=replace(LakeConfig.from_env(), prefix=f"phase18-closure-fixtures/{query_id}")
    )
    topic = f"temporal.ris.source.v1.{query_id}"
    kafka = KafkaConfig.from_env()
    ensure_topic(kafka, topic)
    references, paths, archives = {}, [], []
    for index in range(2):
        archive = tmp_path / f"owned-{index}.gz"
        archive.write_bytes(gzip.compress(data, mtime=index))
        artifact = Artifact(
            sha256=sha256(archive.read_bytes()).hexdigest(),
            bytes=archive.stat().st_size,
            safe_reference=f"fixture:phase18-closure/owned-{index}.gz",
            revision="owned-v1",
        )
        root = tmp_path / f"oracle-{index}"
        summary = build(archive, artifact, root, source_id="owned")
        reference = RisReference.model_validate_json((root / "reference.json").read_text())
        references[artifact.sha256] = reference
        paths.append(str(root / "events.parquet"))
        archives.append(archive)
        publish(archive, reference, tmp_path / f"progress-{index}.json", kafka, topic, chunk_size=2)
        publish_accounting(store, reference, summary, [])
    first = next(iter(references.values()))
    publish(archives[0], first, tmp_path / "replay.json", kafka, topic, chunk_size=2)
    fired = False

    def fault(point):
        nonlocal fired
        if point == "after_commit" and not fired:
            fired = True
            raise RuntimeError("owned commit-before-checkpoint interruption")

    processor = RisProcessor(service_session, store, query_id, references, paths, fault=fault)
    checkpoint = tmp_path / "checkpoint"
    query = start_query(
        service_session, processor, kafka.bootstrap_servers, checkpoint, topic, max_chunks=1
    )
    with pytest.raises(StreamingQueryException):
        query.awaitTermination(120)
    query.stop()
    processor.fault = lambda point: None
    query = start_query(
        service_session, processor, kafka.bootstrap_servers, checkpoint, topic, max_chunks=1
    )
    assert query.awaitTermination(180)
    logical = committed_events(service_session, store, query_id, processor.expected).cache()
    try:
        assert exact_projection(logical, processor.expected, complete=True).count() == 8
        assert logical.select("content_fingerprint").distinct().count() == 2
        assert sum(item["logical_records"] for item in store.inventory(query_id)) == 12
        with pytest.raises(ValueError):
            read_window(store, query_id)
        count, digest = projection_digest(logical)
        publish_window(store, query_id, list(references), count, digest)
        assert read_window(store, query_id)["logical_occurrences"] == 8
    finally:
        logical.unpersist()
