import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256

from messaging.serialization import serialize_record
from processing.rules import load_rules
from schemas import Alert, ConfigDiff, ConfigSnapshot
from spark.common.config import SparkConfig
from spark.common.runtime import create_session
from spark.streaming.parser import parse_records
from spark.streaming.stateful import stateful_records
from spark.streaming.validation import classify_records


def snapshot(
    version: int, asset_id: str = "router-state-test", seconds: int | None = None
) -> ConfigSnapshot:
    content = f"logging {'disabled' if version % 2 == 0 else 'enabled'}\n"
    timestamp = datetime(2026, 10, 5, microsecond=123456, tzinfo=UTC) + timedelta(
        seconds=version if seconds is None else seconds
    )
    return ConfigSnapshot(
        asset_id=asset_id,
        snapshot_id=f"{asset_id}-v{version}",
        version=version,
        event_time=timestamp,
        ingest_time=timestamp,
        content=content,
        hash=sha256(content.encode()).hexdigest(),
    )


def test_native_state_dedup_restart_and_timeout(tmp_path) -> None:
    config = SparkConfig(
        master="local[2]",
        runtime_dir=tmp_path / "runtime",
        checkpoint_dir=tmp_path / "checkpoint",
        watermark_delay="10 seconds",
        state_timeout="30 seconds",
        max_versions_per_asset=4,
    )
    directory = tmp_path / "source"
    directory.mkdir()
    session = create_session(config, kafka=False)
    received = []
    progress = []

    def run(records: list[ConfigSnapshot], batch_number: int) -> list:
        rows = [
            {
                "key": item.asset_id,
                "value": serialize_record(item),
                "topic": "config.snapshots",
                "partition": 0,
                "offset": batch_number * 10 + index,
                "timestamp": item.ingest_time.isoformat(),
            }
            for index, item in enumerate(records)
        ]
        (directory / f"{batch_number}.json").write_text(
            "\n".join(json.dumps(row) for row in rows), encoding="utf-8"
        )
        raw = session.readStream.schema(
            "key STRING, value STRING, topic STRING, partition INT, offset LONG, "
            "timestamp TIMESTAMP"
        ).json(str(directory))
        outputs = stateful_records(
            classify_records(parse_records(raw)), config, load_rules(config.rules_file)
        )
        captured = []

        def capture(batch, batch_id):
            captured.extend(batch.collect())

        query = (
            outputs.writeStream.foreachBatch(capture)
            .option("checkpointLocation", str(config.checkpoint_dir))
            .trigger(availableNow=True)
            .start()
        )
        try:
            assert query.awaitTermination(120)
            assert query.exception() is None
            progress.extend(query.recentProgress)
        finally:
            query.stop()
        received.extend(captured)
        return captured

    try:
        run(
            [snapshot(1), snapshot(3), snapshot(1, "gap"), snapshot(3, "gap")]
            + [snapshot(version, "capacity") for version in (1, 3, 4, 5, 6)],
            0,
        )
        assert not any(
            row.record_type == "config_diff" and row.asset_id == "router-state-test"
            for row in received
        )
        assert any(
            row.record_type == "diagnostic"
            and json.loads(row.summary)["reason"] == "state_eviction"
            for row in received
        )
        second = run([snapshot(2), snapshot(2)], 1)
        diffs = [
            ConfigDiff.model_validate_json(row.value)
            for row in second
            if row.record_type == "config_diff"
        ]
        assert {(diff.old_snapshot_id, diff.new_snapshot_id) for diff in diffs} == {
            ("router-state-test-v1", "router-state-test-v2"),
            ("router-state-test-v2", "router-state-test-v3"),
        }
        assert len(diffs) == 2
        alerts = [
            Alert.model_validate(json.loads(row.value)["payload"])
            for row in second
            if row.record_type == "alert"
        ]
        assert len(alerts) == 1
        assert alerts[0].created_at == snapshot(2).event_time
        resumed = run([snapshot(1), snapshot(2), snapshot(3), snapshot(4)], 2)
        assert sum(row.record_type == "config_diff" for row in resumed) == 1
        run([snapshot(1, "clock", seconds=100)], 3)
        late = run([snapshot(5, seconds=5), snapshot(2, "clock", seconds=101)], 4)
        assert not any(
            row.record_type == "config_diff" and row.asset_id == "router-state-test" for row in late
        )
        assert any(
            row.record_type == "diagnostic" and json.loads(row.summary)["reason"] == "state_timeout"
            for row in received
        )
        assert any(
            operator.get("numRowsDroppedByWatermark", 0) > 0
            for batch in progress
            for operator in batch.get("stateOperators", [])
        )
        assert any(
            row.record_type == "diagnostic"
            and json.loads(row.summary)["reason"] == "unresolved_version_gap"
            for row in received
        )
        assert any(
            operator.get("customMetrics", {}).get("numDroppedDuplicateRows", 0) > 0
            for batch in progress
            for operator in batch.get("stateOperators", [])
        )
    finally:
        session.stop()
