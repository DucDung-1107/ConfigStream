import json
from collections.abc import Iterator
from dataclasses import asdict
from datetime import UTC, datetime, timedelta

import pandas as pd
from pydantic import ValidationError
from pyspark.sql import DataFrame
from pyspark.sql import functions as functions
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.streaming.stateful_processor import ExpiredTimerInfo, TimerValues

from messaging.serialization import serialize_record
from processing import NormalizedConfig, compare_snapshots, normalize_snapshot
from processing.errors import ProcessingError
from processing.rules import Rule, evaluate_rules
from processing.state import (
    AssetVersionState,
    BufferedSnapshot,
    StateTransition,
    accept_snapshot,
    expire_state,
    inspect_snapshot,
)
from schemas import ConfigSnapshot
from spark.common.config import SparkConfig, duration_ms
from spark.common.schemas import SNAPSHOT_COLUMNS
from spark.streaming.validation import console_rows, valid_snapshots

OUTPUT_SCHEMA = (
    "record_type STRING, asset_id STRING, event_time TIMESTAMP, summary STRING, value STRING"
)
BUFFER_SCHEMA = (
    "snapshot_id STRING, version LONG, event_us LONG, content STRING, emitted_next BOOLEAN"
)


def output_record(
    record_type: str, asset_id: str, event_ms: int, summary: dict, value: str | None = None
) -> tuple:
    return (
        record_type,
        asset_id,
        datetime.fromtimestamp(event_ms / 1000, UTC).replace(tzinfo=None),
        json.dumps({"asset_id": asset_id, **summary}, sort_keys=True, separators=(",", ":")),
        value,
    )


class VersionProcessor(StatefulProcessor):
    def __init__(self, config: SparkConfig, rules: tuple[Rule, ...]) -> None:
        self.timeout_ms = duration_ms(config.state_timeout)
        self.max_versions = config.max_versions_per_asset
        self.rules = rules
        self.lake_enabled = config.lake is not None

    def init(self, handle: StatefulProcessorHandle) -> None:
        self.handle = handle
        self.buffer = handle.getListState("versions_v1", BUFFER_SCHEMA)
        self.floor = handle.getValueState("eviction_floor_v1", "version LONG")

    def _load(self, asset_id: str) -> AssetVersionState:
        snapshots = []
        emitted = set()
        if self.buffer.exists():
            for snapshot_id, version, event_us, content, emitted_next in self.buffer.get():
                version = int(version)
                event_us = int(event_us)
                snapshots.append(
                    BufferedSnapshot(
                        NormalizedConfig(asset_id, snapshot_id, version, content), event_us
                    )
                )
                if emitted_next:
                    emitted.add(version)
        floor = int(self.floor.get()[0]) if self.floor.exists() else 0
        return AssetVersionState(asset_id, tuple(snapshots), frozenset(emitted), floor)

    def _save(self, state: AssetVersionState) -> None:
        for timer in self.handle.listTimers():
            self.handle.deleteTimer(timer)
        if not state.snapshots:
            self.buffer.clear()
            self.floor.clear()
            return
        self.buffer.put(
            [
                (
                    item.config.snapshot_id,
                    item.version,
                    item.event_time_us,
                    item.config.normalized_content,
                    item.version in state.emitted,
                )
                for item in state.snapshots
            ]
        )
        self.floor.update((state.floor,))
        self.handle.registerTimer(state.expires_at(self.timeout_ms))

    def _outputs(
        self, transition: StateTransition, event_ms: int, watermark_ms: int
    ) -> list[tuple]:
        records = []
        asset_id = transition.state.asset_id
        for diagnostic in transition.diagnostics:
            records.append(
                output_record(
                    "diagnostic",
                    asset_id,
                    event_ms,
                    {**asdict(diagnostic), "watermark_ms": watermark_ms, "event_time_ms": event_ms},
                )
            )
        for previous, current in transition.pairs:
            result = compare_snapshots(previous.config, current.config)
            diff = result.diff
            records.append(
                output_record(
                    "config_diff",
                    asset_id,
                    current.event_time_ms,
                    {
                        "diff_id": diff.diff_id,
                        "old_version": previous.version,
                        "new_version": current.version,
                        "added": diff.added,
                        "removed": diff.removed,
                        "changed": diff.changed,
                    },
                    diff.model_dump_json(),
                )
            )
            for alert in evaluate_rules(diff, current.event_time, self.rules):
                records.append(
                    output_record(
                        "alert",
                        asset_id,
                        current.event_time_ms,
                        {
                            "alert_id": alert.alert_id,
                            "snapshot_id": alert.snapshot_id,
                            "rule_id": alert.rule_id,
                            "severity": alert.severity.value,
                        },
                        serialize_record(alert),
                    )
                )
        return records

    def handleInputRows(
        self, key: tuple, rows: Iterator[pd.DataFrame], timerValues: TimerValues
    ) -> Iterator[pd.DataFrame]:
        asset_id = key[0]
        watermark = timerValues.getCurrentWatermarkInMs()
        state = self._load(asset_id)
        expired = expire_state(state, watermark, self.timeout_ms)
        initial = self._outputs(expired, max(watermark, 0), watermark)
        state = expired.state
        if initial:
            yield pd.DataFrame(initial)
        for frame in rows:
            for row in frame.to_dict("records"):
                event_time = pd.Timestamp(row["event_time"]).to_pydatetime().replace(tzinfo=UTC)
                event_us = (event_time - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(
                    microseconds=1
                )
                event_ms = event_us // 1000
                metadata = {
                    "snapshot_id": row["snapshot_id"],
                    "version": int(row["version"]),
                    "kafka_topic": row["kafka_topic"],
                    "kafka_partition": row["kafka_partition"],
                    "kafka_offset": row["kafka_offset"],
                }
                if row.get("identity_error") is not None:
                    yield pd.DataFrame(
                        [
                            output_record(
                                "diagnostic",
                                asset_id,
                                event_ms,
                                {**metadata, "reason": row["identity_error"]},
                            )
                        ]
                    )
                    continue
                rejected = inspect_snapshot(state, row["snapshot_id"], int(row["version"]))
                if rejected is not None:
                    records = self._outputs(rejected, event_ms, watermark)
                    if records:
                        yield pd.DataFrame(records)
                    continue
                try:
                    values = {name: row[name] for name in SNAPSHOT_COLUMNS}
                    for name in ("event_time", "ingest_time"):
                        values[name] = (
                            pd.Timestamp(values[name]).to_pydatetime().replace(tzinfo=UTC)
                        )
                    values["version"] = int(values["version"])
                    snapshot = ConfigSnapshot.model_validate(values)
                    if snapshot.asset_id != asset_id or snapshot.snapshot_id != row["snapshot_id"]:
                        raise ProcessingError("Noncanonical snapshot identity")
                    normalized = normalize_snapshot(snapshot)
                except (ValidationError, ProcessingError):
                    yield pd.DataFrame(
                        [
                            output_record(
                                "diagnostic",
                                asset_id,
                                event_ms,
                                {**metadata, "reason": "processing_rejected"},
                            )
                        ]
                    )
                    continue
                transition = accept_snapshot(
                    state, BufferedSnapshot(normalized, event_us), self.max_versions
                )
                records = self._outputs(transition, event_ms, watermark)
                if transition.accepted:
                    records.append(output_record("accepted", asset_id, event_ms, metadata))
                    if self.lake_enabled:
                        records.append(
                            output_record(
                                "normalized_snapshot",
                                asset_id,
                                event_ms,
                                metadata,
                                json.dumps(
                                    {
                                        "normalization_version": "text-v1",
                                        "snapshot_id": snapshot.snapshot_id,
                                        "asset_id": snapshot.asset_id,
                                        "version": snapshot.version,
                                        "event_time": snapshot.event_time.isoformat(),
                                        "ingest_time": snapshot.ingest_time.isoformat(),
                                        "raw_hash": snapshot.hash,
                                        "normalized_hash": normalized.normalized_hash,
                                        "normalized_content": normalized.normalized_content,
                                    },
                                    sort_keys=True,
                                    separators=(",", ":"),
                                ),
                            )
                        )
                if records:
                    yield pd.DataFrame(records)
                state = transition.state
        self._save(state)

    def handleExpiredTimer(
        self, key: tuple, timerValues: TimerValues, expiredTimerInfo: ExpiredTimerInfo
    ) -> Iterator[pd.DataFrame]:
        watermark = timerValues.getCurrentWatermarkInMs()
        transition = expire_state(self._load(key[0]), watermark, self.timeout_ms)
        records = self._outputs(transition, max(watermark, 0), watermark)
        if not transition.state.snapshots:
            self.buffer.clear()
            self.floor.clear()
        if records:
            yield pd.DataFrame(records)


def stateful_records(
    classified: DataFrame,
    config: SparkConfig,
    rules: tuple[Rule, ...],
    raw_records: DataFrame | None = None,
) -> DataFrame:
    from pyspark.sql.types import StructType

    from spark.streaming.identity import IdentityProcessor, checkpoint_contract

    checkpoint_contract(
        config, int(classified.sparkSession.conf.get("spark.sql.shuffle.partitions"))
    )
    valid = valid_snapshots(classified).withWatermark("event_time", config.watermark_delay)
    valid = valid.groupBy("snapshot_id").transformWithStateInPandas(
        IdentityProcessor(config),
        StructType(list(valid.schema.fields)).add("identity_error", "string"),
        outputMode="Append",
        timeMode="EventTime",
        eventTimeColumnName="event_time",
    )
    processed = valid.groupBy("asset_id").transformWithStateInPandas(
        VersionProcessor(config, rules),
        OUTPUT_SCHEMA,
        outputMode="Append",
        timeMode="EventTime",
        eventTimeColumnName="event_time",
    )
    invalid = console_rows(classified.filter(functions.col("validation_error").isNotNull()))
    diagnostics = invalid.select(
        functions.lit("invalid").alias("record_type"),
        "asset_id",
        "event_time",
        functions.to_json(functions.struct(*invalid.columns)).alias("summary"),
        functions.lit(None).cast("string").alias("value"),
    )
    output = processed.unionByName(diagnostics)
    if config.lake is not None:
        if raw_records is None:
            raise ValueError("Lake processing requires the original Kafka source")
        from spark.streaming.sinks.lake import raw_outputs

        output = output.unionByName(raw_outputs(raw_records))
    return output
