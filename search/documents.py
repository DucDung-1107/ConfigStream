import json
from collections.abc import Mapping
from datetime import UTC, datetime
from hashlib import sha256

from search.config import SearchConfig
from search.mappings import FIELDS

DIAGNOSTIC_REASONS = frozenset(
    {
        "version_conflict",
        "unresolved_version_gap",
        "state_eviction",
        "state_timeout",
        "evicted_version",
        "processing_rejected",
        "missing_payload",
        "missing_snapshot_id",
        "missing_asset_id",
        "malformed_json",
        "wrong_record_type",
        "unsupported_schema_version",
        "invalid_snapshot_id",
        "invalid_asset_id",
        "invalid_version",
        "invalid_config_format",
        "invalid_event_time",
        "invalid_ingest_time",
        "invalid_hash",
        "invalid_content_location",
        "kafka_key_mismatch",
    }
)


def utc_timestamp(value: str | datetime | None) -> str | None:
    if value is None:
        return None
    try:
        timestamp = (
            datetime.fromisoformat(value.replace("Z", "+00:00"))
            if isinstance(value, str)
            else value
        )
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)
        return timestamp.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    except (ValueError, AttributeError, TypeError):
        raise ValueError("Invalid search timestamp") from None


def project_document(kind: str, row: Mapping) -> dict:
    if kind == "diagnostics":
        try:
            summary = json.loads(row["summary"])
        except (ValueError, KeyError, TypeError):
            raise ValueError("Invalid diagnostic summary") from None
        reason = summary.get("reason", summary.get("validation_error"))
        if reason not in DIAGNOSTIC_REASONS:
            raise ValueError("Unsupported diagnostic reason")
        document = {name: summary[name] for name in FIELDS[kind] if name in summary}
        document.update(
            asset_id=row.get("asset_id"),
            event_time=row.get("event_time") or summary.get("kafka_timestamp"),
            reason=reason,
            diagnostic_type="invalid_record" if row["record_type"] == "invalid" else reason,
        )
    else:
        document = {name: row[name] for name in FIELDS[kind] if name in row}
    for name in ("event_time", "ingest_time", "created_at", "kafka_timestamp"):
        if name in document:
            document[name] = utc_timestamp(document[name])
    if kind == "diagnostics":
        document.pop("diagnostic_id", None)
        canonical = json.dumps(document, sort_keys=True, separators=(",", ":"))
        document["diagnostic_id"] = (
            "diag-" + sha256(("search-diagnostic-v1:" + canonical).encode()).hexdigest()
        )
    identity = {
        "snapshots": "snapshot_id",
        "diffs": "diff_id",
        "alerts": "alert_id",
        "diagnostics": "diagnostic_id",
    }[kind]
    if not isinstance(document.get(identity), str) or not document[identity].strip():
        raise ValueError("Missing search document identity")
    return document


def bulk_action(kind: str, row: Mapping, config: SearchConfig) -> dict:
    document = project_document(kind, row)
    identity = {
        "snapshots": "snapshot_id",
        "diffs": "diff_id",
        "alerts": "alert_id",
        "diagnostics": "diagnostic_id",
    }[kind]
    return {"_index": config.alias(kind), "_id": document[identity], "_source": document}
