"""Historical occurrence queries, never inferred routing state or incidents."""

from ipaddress import ip_network


def query_events(frame, *, start=None, end=None, prefix=None):
    from pyspark.sql import functions as functions

    if (start is None) != (end is None) or start is not None and end <= start:
        raise ValueError("Bounded half-open source-time interval required")
    if start is not None:
        frame = frame.where(
            (functions.col("source_seconds") >= start) & (functions.col("source_seconds") < end)
        )
    if prefix is not None:
        frame = frame.where(functions.col("prefix") == str(ip_network(prefix, strict=True)))
    return frame


def event_summary(frame):
    from pyspark.sql import functions as functions

    occurrences = frame.count()
    return {
        "occurrences": occurrences,
        "peers": frame.select("peer_asn", "peer_ip").distinct().count(),
        "prefixes": frame.select("prefix").distinct().count(),
        "actions": {row.action: row["count"] for row in frame.groupBy("action").count().collect()},
        "events_per_minute": [
            row.asDict()
            for row in frame.withColumn(
                "minute", functions.floor(functions.col("source_seconds") / 60) * 60
            )
            .groupBy("minute")
            .count()
            .orderBy("minute")
            .collect()
        ],
        "repeated_fingerprints_beyond_first": occurrences
        - frame.select("content_fingerprint").distinct().count(),
    }


def peer_activity(frame):
    return frame.groupBy("peer_asn", "peer_ip").count()


def provenance(frame, occurrence_id):
    from pydantic import TypeAdapter
    from pyspark.sql import functions as functions

    from schemas.temporal.base import Digest

    TypeAdapter(Digest).validate_python(occurrence_id)
    return frame.where(functions.col("logical_record_id") == occurrence_id).select(
        "logical_record_id",
        "artifact_sha256",
        "provenance_manifest_id",
        "byte_offset",
        "all_record_index",
        "expansion_ordinal",
        "source_seconds",
        "source_microseconds",
    )
