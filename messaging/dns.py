"""Bounded cached-Parquet publisher; no source downloads or configuration topics."""

import json
import re
from time import monotonic

from processing.dns import read_source
from schemas.dns import DnsChunk


def validate_topic(topic):
    if not re.fullmatch(r"temporal\.dns\.source\.v1\.[a-z0-9-]{1,64}", topic):
        raise ValueError("Unregistered DNS topic")


def source_offsets(value, topic):
    validate_topic(topic)
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict) or set(value) != {topic} or not isinstance(value[topic], dict):
        raise ValueError("DNS source offset topic binding mismatch")
    result = value[topic]
    if not 1 <= len(result) <= 2 or any(
        not isinstance(key, str) or not key.isdecimal() or type(offset) is not int or offset < 0
        for key, offset in result.items()
    ):
        raise ValueError("DNS source offset partition/value mismatch")
    return result


def ensure_topic(config, topic):
    from confluent_kafka import KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic

    validate_topic(topic)
    admin = AdminClient({"bootstrap.servers": config.bootstrap_servers})
    try:
        admin.create_topics(
            [
                NewTopic(
                    topic,
                    num_partitions=2,
                    replication_factor=1,
                    config={"retention.ms": "604800000"},
                )
            ]
        )[topic].result(30)
    except KafkaException as error:
        if error.args[0].name() != "TOPIC_ALREADY_EXISTS":
            raise


def publish(path, reference, progress, config, topic, *, limit=None, check=lambda: None):
    from confluent_kafka import Producer

    validate_topic(topic)
    started = monotonic()
    source = reference.manifest.artifacts[0].sha256
    previous = json.loads(progress.read_text()) if progress.exists() else None
    if previous and (previous["artifact_sha256"] != source or previous["topic"] != topic):
        raise ValueError("DNS producer progress binding conflict")
    skip = previous["source_rows"] if previous else 0
    acknowledged = skip
    total_bytes = chunks = 0
    failures = []
    producer = Producer(
        {
            "bootstrap.servers": config.bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
            "queue.buffering.max.kbytes": 1024,
            "message.timeout.ms": 30000,
        }
    )

    def delivery(error, message):
        if error:
            failures.append(type(error).__name__)

    pending = []

    def flush():
        nonlocal total_bytes, chunks, acknowledged
        if not pending:
            return
        value = DnsChunk(artifact_sha256=source, rows=tuple(pending)).model_dump_json().encode()
        if len(value) > 1024**2:
            raise ValueError("DNS transport byte bound")
        producer.produce(topic, key=source, value=value, on_delivery=delivery)
        if producer.flush(30) or failures:
            raise RuntimeError("DNS producer acknowledgment failed")
        acknowledged += len(pending)
        total_bytes += len(value)
        chunks += 1
        temporary = progress.with_suffix(".pending")
        temporary.write_text(
            json.dumps(dict(artifact_sha256=source, topic=topic, source_rows=acknowledged))
        )
        temporary.replace(progress)
        pending.clear()
        check()

    for index, row in enumerate(read_source(path, reference, limit=limit)):
        if index < skip:
            continue
        pending.append(row)
        if len(pending) == 128:
            flush()
    flush()
    return dict(
        source_rows=acknowledged,
        chunks=chunks,
        serialized_value_bytes=total_bytes,
        seconds=monotonic() - started,
        artifact_sha256=source,
    )
