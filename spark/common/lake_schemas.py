RAW_SCHEMA = (
    "kafka_key BINARY, kafka_value BINARY, kafka_topic STRING, kafka_partition INT, "
    "kafka_offset LONG, kafka_timestamp TIMESTAMP"
)
NORMALIZED_SCHEMA = (
    "normalization_version STRING, snapshot_id STRING, asset_id STRING, version LONG, "
    "event_time TIMESTAMP, ingest_time TIMESTAMP, raw_hash STRING, normalized_hash STRING, "
    "normalized_content STRING"
)
DIFF_SCHEMA = (
    "schema_version STRING, diff_id STRING, asset_id STRING, old_snapshot_id STRING, "
    "new_snapshot_id STRING, added LONG, removed LONG, changed LONG, "
    "details ARRAY<STRUCT<operation:STRING,line:STRING>>"
)
ALERT_SCHEMA = (
    "schema_version STRING, alert_id STRING, asset_id STRING, snapshot_id STRING, "
    "rule_id STRING, severity STRING, message STRING, created_at TIMESTAMP"
)
DIAGNOSTIC_SCHEMA = "record_type STRING, asset_id STRING, event_time TIMESTAMP, summary STRING"
DATASET_SCHEMAS = {
    "raw/snapshots": RAW_SCHEMA,
    "processed/snapshots": NORMALIZED_SCHEMA,
    "processed/diffs": DIFF_SCHEMA + ", event_time TIMESTAMP",
    "processed/alerts": ALERT_SCHEMA,
    "processed/diagnostics": DIAGNOSTIC_SCHEMA,
}
