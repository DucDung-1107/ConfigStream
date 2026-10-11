from typing import Literal

from pydantic import Field

from schemas.alert import Severity
from schemas.base import (
    NonBlankString,
    NonNegativeInteger,
    PositiveInteger,
    SchemaModel,
    UTCTimestamp,
)


class SnapshotResponse(SchemaModel):
    snapshot_id: NonBlankString
    asset_id: NonBlankString
    version: PositiveInteger
    event_time: UTCTimestamp
    ingest_time: UTCTimestamp
    raw_hash: NonBlankString
    normalized_hash: NonBlankString
    normalization_version: NonBlankString


class DiffResponse(SchemaModel):
    schema_version: Literal["1.0"]
    diff_id: NonBlankString
    asset_id: NonBlankString
    old_snapshot_id: NonBlankString | None
    new_snapshot_id: NonBlankString
    added: NonNegativeInteger
    removed: NonNegativeInteger
    changed: NonNegativeInteger
    event_time: UTCTimestamp


class AlertResponse(SchemaModel):
    schema_version: Literal["1.0"]
    alert_id: NonBlankString
    asset_id: NonBlankString
    snapshot_id: NonBlankString
    rule_id: NonBlankString
    severity: Severity
    message: NonBlankString
    created_at: UTCTimestamp


class DiagnosticResponse(SchemaModel):
    diagnostic_id: NonBlankString
    diagnostic_type: NonBlankString
    reason: NonBlankString
    asset_id: NonBlankString | None = None
    snapshot_id: NonBlankString | None = None
    version: PositiveInteger | None = None
    versions: list[PositiveInteger] | None = None
    event_time: UTCTimestamp | None = None
    missing_start: PositiveInteger | None = None
    missing_end: PositiveInteger | None = None
    kafka_topic: NonBlankString | None = None
    kafka_partition: NonNegativeInteger | None = None
    kafka_offset: NonNegativeInteger | None = None
    kafka_timestamp: UTCTimestamp | None = None
    watermark_ms: int | None = None
    event_time_ms: int | None = None


class Page[Item: SchemaModel](SchemaModel):
    items: list[Item]
    count: NonNegativeInteger
    has_more: bool
    next_cursor: str | None


class Bucket(SchemaModel):
    key: str
    count: NonNegativeInteger


class AlertStatsResponse(SchemaModel):
    by_severity: list[Bucket] = Field(max_length=100)
    by_rule_id: list[Bucket] = Field(max_length=100)


class TimeBucket(SchemaModel):
    time: UTCTimestamp
    count: NonNegativeInteger


class DiffStatsResponse(SchemaModel):
    interval: Literal["1h"] = "1h"
    buckets: list[TimeBucket] = Field(max_length=746)


class StatusResponse(SchemaModel):
    status: Literal["ok"] = "ok"


class ErrorDetail(SchemaModel):
    code: str
    message: str


class ErrorResponse(SchemaModel):
    error: ErrorDetail


RESPONSE_MODELS = {
    "snapshots": SnapshotResponse,
    "diffs": DiffResponse,
    "alerts": AlertResponse,
    "diagnostics": DiagnosticResponse,
}
