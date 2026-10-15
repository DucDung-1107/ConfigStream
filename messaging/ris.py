"""Opt-in restartable RIS transport. ACK progress is not lake completion."""

import json
from time import monotonic

from messaging.ack_window import AckWindow
from processing.ris import read_archive
from schemas.ris import EventChunk, validate_topic


def ensure_topic(config, topic):
    from confluent_kafka.admin import AdminClient, NewTopic

    validate_topic(topic)
    admin = AdminClient({"bootstrap.servers": config.bootstrap_servers})
    if topic not in admin.list_topics(timeout=10).topics:
        admin.create_topics(
            [
                NewTopic(
                    topic,
                    num_partitions=2,
                    replication_factor=1,
                    config={"retention.ms": "604800000"},
                )
            ]
        )[topic].result(15)


def publish(
    path,
    reference,
    progress,
    config,
    topic,
    *,
    chunk_size=128,
    check=lambda: None,
    inflight_chunks=1,
):
    from confluent_kafka import Producer

    from workloads.ris_oracle import parser_module

    validate_topic(topic)
    if type(chunk_size) is not int or not 1 <= chunk_size <= 256:
        raise ValueError("Invalid RIS chunk bound")
    previous = json.loads(progress.read_text()) if progress.exists() else {}
    if previous and previous.get("manifest_id") != reference.manifest.manifest_id:
        raise ValueError("RIS producer progress source conflict")
    if previous.get("topic", topic) != topic:
        raise ValueError("RIS producer progress topic conflict")
    start = previous.get("next_event", 0)
    client = Producer(
        config.producer_options()
        | {
            "queue.buffering.max.kbytes": 16384,
            "message.max.bytes": 1048576,
        }
    )
    pending = []
    began = monotonic()

    def commit(position):
        progress.parent.mkdir(parents=True, exist_ok=True)
        temporary = progress.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(
                {
                    "manifest_id": reference.manifest.manifest_id,
                    "topic": topic,
                    "next_event": position,
                }
            )
        )
        temporary.replace(progress)

    window = AckWindow(client, config, topic, commit, chunks=inflight_chunks, check=check)

    def flush_chunk(first):
        chunk = EventChunk(
            artifact_sha256=reference.manifest.artifacts[0].sha256,
            first_event=first,
            rows=tuple(pending),
        )
        chunk.validate_reference(reference)
        encoded = chunk.model_dump_json().encode()
        if len(encoded) > 1024**2:
            raise ValueError("RIS transport byte bound")
        window.send(
            chunk.artifact_sha256, encoded, rows=len(pending), position=first + len(pending)
        )
        pending.clear()

    position = 0
    excluded = framed = 0
    for frame, error, events in read_archive(path, reference, parser_module()):
        framed += 1
        excluded += error is not None
        if frame.all_record_index % 1000 == 0:
            check()
        for event in events:
            if position >= start:
                pending.append(event)
                if len(pending) == chunk_size:
                    flush_chunk(position + 1 - len(pending))
            position += 1
    if pending:
        flush_chunk(position - len(pending))
    window.flush()
    if (framed, excluded) != (
        reference.manifest.expected_records,
        reference.manifest.excluded_records,
    ):
        raise ValueError("RIS source framing/exclusion inventory changed")
    return dict(
        acknowledged_occurrences=window.rows,
        messages=window.messages,
        serialized_value_bytes=window.bytes,
        ack_flushes=window.flushes,
        elapsed_seconds=monotonic() - began,
        resumed_from_event=start,
    )
