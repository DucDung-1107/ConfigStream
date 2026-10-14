"""Bounded owned fixtures through existing typed domain processors in native Spark."""

import gzip
import json
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from uuid import uuid4


def drain(query):
    if not query.awaitTermination(360):
        query.stop()
        raise RuntimeError("Owned typed fixture query deadline exceeded")
    return [json.loads(progress.json) for progress in query.recentProgress]


def run_roles(session, config, directory):
    from messaging.config import KafkaConfig

    directory = Path(directory) / "roles"
    directory.mkdir(parents=True, exist_ok=False)
    kafka = KafkaConfig.from_env()
    report = {}
    for role, function in (("terraform", terraform), ("rpki", rpki), ("ris", ris), ("dns", dns)):
        target = directory / role
        target.mkdir()
        lake = replace(config.lake, prefix=f"{config.lake.prefix}/{role}-{uuid4()}")
        report[role] = function(session, lake, kafka, target)
        (directory / "results.json").write_text(json.dumps(report))
    return report


def terraform(session, lake, kafka, directory):
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    from messaging.serialization import serialize_record
    from processing.rules import load_rules
    from schemas import ConfigSnapshot
    from spark.common.config import SparkConfig
    from spark.streaming.main import start_stateful
    from spark.streaming.parser import parse_records
    from spark.streaming.sinks.lake import read_committed
    from spark.streaming.validation import classify_records
    from tests.conftest import terraform_fixture
    from workloads.observed import load_dataset

    root, _, _ = terraform_fixture.__wrapped__(directory)
    snapshots, _, _ = load_dataset(root)
    expected = {
        snapshot.snapshot_id: (snapshot.asset_id, snapshot.version, snapshot.hash)
        for snapshot in snapshots
    }
    for snapshot in snapshots:
        record = json.loads(serialize_record(snapshot))
        if ConfigSnapshot.model_validate(record["payload"]) != snapshot:
            raise RuntimeError("Terraform existing transport contract changed")
    topic = f"config.terraform-fixture.{uuid4()}"
    AdminClient(kafka.client_options()).create_topics(
        [
            NewTopic(
                topic, num_partitions=1, replication_factor=1, config={"retention.ms": "86400000"}
            )
        ]
    )[topic].result(30)
    producer = Producer(kafka.producer_options())
    errors = []
    for snapshot in (*snapshots, snapshots[0]):
        producer.produce(
            topic,
            key=snapshot.asset_id,
            value=serialize_record(snapshot),
            on_delivery=lambda error, message: errors.append(str(error)) if error else None,
        )
    if producer.flush(15) or errors:
        raise RuntimeError("Owned Terraform transport not acknowledged")
    configuration = replace(
        SparkConfig.from_env(), lake=lake, search=None, checkpoint_dir=directory / "checkpoint"
    )
    raw = (
        session.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka.bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .load()
    )
    query = start_stateful(
        classify_records(parse_records(raw)),
        configuration,
        load_rules(configuration.rules_file),
        available_now=True,
        raw_records=raw,
    )
    progress = drain(query)
    actual_rows = read_committed(session, lake, "processed/snapshots", str(query.id)).collect()
    actual = {row.snapshot_id: (row.asset_id, row.version, row.raw_hash) for row in actual_rows}
    if len(actual_rows) != len(expected) or actual != expected:
        raise RuntimeError("Owned Terraform Git ordinal/source identity changed")
    return dict(
        status="PASS_INTEGRATION",
        topic=topic,
        query_id=str(query.id),
        logical_versions=len(expected),
        physical_deliveries=len(snapshots) + 1,
        assertion="Owned Git fixture extraction and version identities only; commits are not deployments",
        progress=progress,
    )


def rpki(session, lake, kafka, directory):
    from lake.rpki import MinioStore
    from messaging.rpki import ensure_topic, publish_source
    from spark.rpki import committed_rows, read_state, reconstruct
    from spark.rpki_stream import RpkiProcessor, start_query
    from tests.conftest import rpki_fixture
    from workloads.rpki_oracle import run_oracle

    cache, selection, oracle, _, references = rpki_fixture.__wrapped__(directory)
    summary = run_oracle(selection, cache, oracle)
    query_id = str(uuid4())
    topic = f"temporal.rpki.fixture.{query_id}"
    ensure_topic(kafka, topic=topic)
    source_files = {item["sha256"]: item["file"] for item in selection["artifacts"]}
    deliveries = []
    for reference in references.values():
        source = cache / source_files[reference.manifest.artifacts[0].sha256]
        for attempt in ("original", "replay"):
            deliveries.append(
                publish_source(
                    source,
                    reference,
                    directory / f"{reference.observation_id}-{attempt}.json",
                    kafka,
                    topic=topic,
                    chunk_size=2,
                )
            )
    store = MinioStore(lake)
    processor = RpkiProcessor(session, store, query_id, references, oracle, summary["boundaries"])
    progress = drain(
        start_query(
            session, processor, kafka.bootstrap_servers, directory / "checkpoint", topic=topic
        )
    )
    raw_count = committed_rows(session, store, query_id).count()
    states = [read_state(session, store, reference).count() for reference in references.values()]
    if (
        raw_count != 7
        or states != [3, 3]
        or reconstruct(session, store, list(references.values())).count() != 3
    ):
        raise RuntimeError("Owned RPKI exact reconstruction/multiplicity mismatch")
    return dict(
        query_id=query_id,
        topic=topic,
        logical_source_rows=raw_count,
        states=states,
        boundary_additions=1,
        boundary_removals=1,
        producer=deliveries,
        progress=progress,
        status="PASS_INTEGRATION",
    )


def ris(session, lake, kafka, directory):
    from lake.ris import EventStore
    from messaging.ris import ensure_topic, publish
    from schemas.ris import RisReference
    from schemas.temporal.provenance import Artifact
    from spark.ris import RisProcessor, committed_events, exact_projection, start_query
    from workloads.ris_oracle import build

    fixture = json.loads(Path("tests/fixtures/phase16/mrt.json").read_text())
    data = bytes.fromhex(fixture["one_update_hex"]) * 2
    data += bytes.fromhex(fixture["malformed_bgp_body_hex"])
    archive = directory / "owned.gz"
    archive.write_bytes(gzip.compress(data, mtime=0))
    artifact = Artifact(
        sha256=sha256(archive.read_bytes()).hexdigest(),
        bytes=archive.stat().st_size,
        safe_reference="fixture:20k1/owned.gz",
        revision="owned-v1",
    )
    oracle = directory / "oracle"
    summary = build(archive, artifact, oracle, source_id="owned")
    reference = RisReference.model_validate_json((oracle / "reference.json").read_text())
    query_id = str(uuid4())
    topic = f"temporal.ris.source.v1.{query_id}"
    ensure_topic(kafka, topic)
    deliveries = [
        publish(archive, reference, directory / f"{attempt}.json", kafka, topic, chunk_size=2)
        for attempt in ("original", "replay")
    ]
    store = EventStore(config=lake)
    fired = False

    def fault(point):
        nonlocal fired
        if point == "after_commit" and not fired:
            fired = True
            raise RuntimeError("Owned RIS commit-before-checkpoint interruption")

    processor = RisProcessor(
        session,
        store,
        query_id,
        {artifact.sha256: reference},
        [str(oracle / "events.parquet")],
        fault=fault,
    )
    from pyspark.errors import StreamingQueryException

    try:
        drain(
            start_query(
                session,
                processor,
                kafka.bootstrap_servers,
                directory / "checkpoint",
                topic,
                max_chunks=1,
            )
        )
    except StreamingQueryException:
        if not fired:
            raise
    else:
        raise RuntimeError("Expected owned RIS failure did not fire")
    processor.fault = lambda point: None
    progress = drain(
        start_query(
            session,
            processor,
            kafka.bootstrap_servers,
            directory / "checkpoint",
            topic,
            max_chunks=1,
        )
    )
    actual = committed_events(session, store, query_id, processor.expected)
    exact = exact_projection(actual, processor.expected, complete=True)
    count = exact.count()
    if count != 4 or exact.select("logical_record_id").distinct().count() != 4:
        raise RuntimeError("Owned RIS source occurrence multiplicity mismatch")
    if exact.select("content_fingerprint").distinct().count() != 2:
        raise RuntimeError("Owned equal-payload events disappeared")
    from benchmarks.ris_service import finalize_history
    from lake.ris import metadata_digest

    inventory = sorted(store.inventory(query_id), key=lambda item: item["batch_id"])
    completion = finalize_history(
        session,
        store,
        query_id,
        processor.expected,
        {artifact.sha256: reference},
        {artifact.sha256: summary},
        {artifact.sha256: json.loads((oracle / "exclusions.json").read_text())},
        inventory_sha256=metadata_digest(inventory),
        partitions=4,
    )
    return dict(
        query_id=query_id,
        topic=topic,
        logical_occurrences=count,
        distinct_fingerprints=2,
        recovery_after_commit=fired,
        completion=completion,
        source_oracle=summary,
        producer=deliveries,
        progress=progress,
        status="PASS_INTEGRATION",
    )


def dns(session, lake, kafka, directory):
    from lake.dns import DnsStore, read_source_completion
    from messaging.dns import ensure_topic, publish
    from spark.dns import DnsProcessor, start_query
    from spark.dns_history import finalize_source, read_table
    from tests.unit.test_dns_measurements import owned_source
    from workloads.dns_oracle import build

    source_path, reference, _ = owned_source(directory)
    oracle = directory / "oracle"
    summary = build(source_path, reference, oracle)
    query_id = str(uuid4())
    topic = f"temporal.dns.source.v1.{query_id}"
    ensure_topic(kafka, topic)
    deliveries = [
        publish(source_path, reference, directory / f"{attempt}.json", kafka, topic)
        for attempt in ("original", "replay")
    ]
    artifact = reference.manifest.artifacts[0].sha256
    store = DnsStore(config=lake)
    processor = DnsProcessor(
        session, store, query_id, {artifact: reference}, [str(oracle / "raw.parquet")]
    )
    progress = drain(
        start_query(session, processor, kafka.bootstrap_servers, directory / "checkpoint", topic)
    )
    result = finalize_source(session, store, query_id, reference, oracle)
    if result["source_rows"] != 6 or result["address_groups"] != 2:
        raise RuntimeError("Owned DNS exact source/group mismatch")
    if read_source_completion(store, query_id, artifact)["entity_coverage"] != "unknown":
        raise RuntimeError("DNS source completeness incorrectly claims entity coverage")
    if read_table(session, store, query_id, artifact, "contributors").count() != 2:
        raise RuntimeError("DNS equal values lost source contributors")
    if summary["real_proven_empty_rows"] != 0:
        raise RuntimeError("DNS timeout became proven empty")
    return dict(
        query_id=query_id,
        topic=topic,
        logical_source_rows=6,
        groups=2,
        entity_coverage="unknown",
        source_oracle=summary,
        finalization=result,
        producer=deliveries,
        progress=progress,
        status="PASS_INTEGRATION",
    )
