"""Opt-in RPKI bounded publisher; original producer/serialization union unchanged."""

import json
from pathlib import Path
from time import monotonic

from messaging.ack_window import AckWindow
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
    resource_check=None,
    inflight_chunks=1,
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
    began = monotonic()

    def commit(position):
        progress.parent.mkdir(parents=True, exist_ok=True)
        temporary = progress.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(
                dict(observation_id=reference.observation_id, topic=topic, next_row=position)
            )
        )
        temporary.replace(progress)

    if previous.get("topic", topic) != topic:
        raise ValueError("Publisher progress binds a different topic")
    window = AckWindow(
        client,
        config,
        topic,
        commit,
        chunks=inflight_chunks,
        check=resource_check or (lambda: None),
    )

    for chunk in source_chunks(path, reference, chunk_size=chunk_size, start_row=start):
        chunk.validate_source(reference)
        encoded = chunk.model_dump_json().encode("utf-8")
        if len(encoded) > 1024**2:
            raise ValueError("Kafka source chunk byte budget exceeded")
        window.send(
            reference.observation_id.encode(),
            encoded,
            rows=len(chunk.rows),
            position=chunk.first_row + len(chunk.rows),
        )
    window.flush()
    return dict(
        acknowledged_rows_this_run=window.rows,
        serialized_bytes_this_run=window.bytes,
        ack_flushes=window.flushes,
        elapsed_seconds=monotonic() - began,
        resumed_from_row=start,
    )
