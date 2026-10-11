from search.config import KINDS, SEARCH_SCHEMA_VERSION

SNAPSHOT_FIELDS = (
    "snapshot_id",
    "asset_id",
    "version",
    "event_time",
    "ingest_time",
    "raw_hash",
    "normalized_hash",
    "normalization_version",
)
DIFF_FIELDS = (
    "schema_version",
    "diff_id",
    "asset_id",
    "old_snapshot_id",
    "new_snapshot_id",
    "added",
    "removed",
    "changed",
    "event_time",
)
ALERT_FIELDS = (
    "schema_version",
    "alert_id",
    "asset_id",
    "snapshot_id",
    "rule_id",
    "severity",
    "message",
    "created_at",
)
DIAGNOSTIC_FIELDS = (
    "diagnostic_id",
    "diagnostic_type",
    "asset_id",
    "snapshot_id",
    "version",
    "versions",
    "event_time",
    "reason",
    "missing_start",
    "missing_end",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "watermark_ms",
    "event_time_ms",
)
FIELDS = dict(
    zip(KINDS, (SNAPSHOT_FIELDS, DIFF_FIELDS, ALERT_FIELDS, DIAGNOSTIC_FIELDS), strict=True)
)


def mapping(kind: str) -> dict:
    properties = {}
    for name in FIELDS[kind]:
        if name in {"event_time", "ingest_time", "created_at", "kafka_timestamp"}:
            properties[name] = {"type": "date_nanos", "format": "strict_date_optional_time_nanos"}
        elif name in {
            "version",
            "versions",
            "added",
            "removed",
            "changed",
            "missing_start",
            "missing_end",
            "kafka_partition",
            "kafka_offset",
            "watermark_ms",
            "event_time_ms",
        }:
            properties[name] = {"type": "long"}
        elif name == "message":
            properties[name] = {"type": "text"}
        elif name in {"raw_hash", "normalized_hash"}:
            properties[name] = {"type": "keyword", "index": False, "doc_values": False}
        else:
            properties[name] = {"type": "keyword"}
    return {
        "dynamic": "strict",
        "_meta": {"search_schema_version": SEARCH_SCHEMA_VERSION},
        "properties": properties,
    }
