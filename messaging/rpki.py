"""Opt-in RPKI bounded publisher; original producer/serialization union unchanged."""

import json
from pathlib import Path
from time import monotonic

from messaging.config import KafkaConfig
from schemas.rpki import TOPIC, DayReference, validate_topic
from workloads.rpki_source import source_chunks


def ensure_topic(config: KafkaConfig, *, topic=TOPIC):
    from confluent_kafka.admin import AdminClient, NewTopic

    validate_topic(topic)
    admin = AdminClient({"bootstrap.servers": config.bootstrap_servers})
    metadata = admin.list_topics(timeout=10)
    if topic not in metadata.topics:
        admin.create_topics([NewTopic(topic, num_partitions=2, replication_factor=1)])[
            topic
        ].result(15)


def publish_source(
    path: Path,
    reference: DayReference,
    progress: Path,
    config: KafkaConfig,
    *,
    chunk_size: int = 256,
    client=None,
    topic=TOPIC,
):
    validate_topic(topic)
    if client is None:
        from confluent_kafka import Producer

        options = config.producer_options() | {
            "queue.buffering.max.kbytes": 16384,
            "message.max.bytes": 1048576,
        }
        client = Producer(options)
    previous = json.loads(progress.read_text()) if progress.exists() else {}
    if previous and previous.get("observation_id") != reference.observation_id:
        raise ValueError("Publisher progress binds a different source observation")
    start = previous.get("next_row", 1)
    errors = []
    acknowledged = 0
    total_bytes = 0
    began = monotonic()

    def delivered(error, message):
        if error is not None:
            errors.append("delivery_failed")

    for chunk in source_chunks(path, reference, chunk_size=chunk_size, start_row=start):
        chunk.validate_source(reference)
        encoded = chunk.model_dump_json().encode("utf-8")
        if len(encoded) > 1024**2:
            raise ValueError("Kafka source chunk byte budget exceeded")
        deadline = monotonic() + config.enqueue_timeout_seconds
        while True:
            try:
                client.produce(
                    topic,
                    key=reference.observation_id.encode(),
                    value=encoded,
                    on_delivery=delivered,
                )
                break
            except BufferError:
                if monotonic() >= deadline:
                    raise RuntimeError("RPKI producer queue timeout") from None
                client.poll(0.1)
        if client.flush(config.flush_timeout_seconds) or errors:
            raise RuntimeError("RPKI chunk delivery unconfirmed; progress not advanced")
        acknowledged += len(chunk.rows)
        total_bytes += len(encoded)
        progress.parent.mkdir(parents=True, exist_ok=True)
        temporary = progress.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(
                dict(
                    observation_id=reference.observation_id,
                    next_row=chunk.first_row + len(chunk.rows),
                )
            )
        )
        temporary.replace(progress)
    return dict(
        acknowledged_rows_this_run=acknowledged,
        serialized_bytes_this_run=total_bytes,
        elapsed_seconds=monotonic() - began,
        resumed_from_row=start,
    )
