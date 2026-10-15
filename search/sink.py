import json
from collections.abc import Iterator
from itertools import chain

from search.bulk import write_actions
from search.client import create_client
from search.config import KINDS, SearchConfig
from search.documents import bulk_action
from search.mappings import FIELDS


def write_partition(rows: Iterator, kind: str, config: SearchConfig) -> None:
    first = next(rows, None)
    if first is None:
        return
    client = create_client(config)
    try:
        actions = (
            bulk_action(kind, json.loads(row.document_json), config)
            for row in chain((first,), rows)
        )
        write_actions(actions, config, client)
    finally:
        client.close()


def safe_frame(frame, kind: str):
    from pyspark.sql import functions as functions

    columns = (
        ["record_type", "asset_id", "event_time", "summary"]
        if kind == "diagnostics"
        else list(FIELDS[kind])
    )
    return frame.select(
        functions.to_json(
            functions.struct(*columns),
            options={"timeZone": "UTC", "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"},
        ).alias("document_json")
    )


def index_frame(frame, kind: str, config: SearchConfig) -> None:
    safe_frame(frame, kind).foreachPartition(lambda rows: write_partition(rows, kind, config))


def index_batch(batch, config, counts=None) -> None:
    from spark.streaming.sinks.lake import DATASET_RECORD_TYPES, dataset_frame

    for kind in KINDS:
        if counts is not None and not any(
            counts.get(record_type, 0) for record_type in DATASET_RECORD_TYPES[f"processed/{kind}"]
        ):
            continue
        index_frame(dataset_frame(batch, f"processed/{kind}"), kind, config.search)
