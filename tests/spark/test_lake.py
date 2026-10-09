import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pyspark.sql import functions as functions

from lake.config import LakeConfig
from messaging.serialization import serialize_record
from processing import normalize_snapshot, process_snapshot_pair
from processing.rules import evaluate_rules, load_rules
from scripts.kafka_stateful_demo import demo_history
from spark.common.config import SparkConfig
from spark.common.runtime import create_session
from spark.streaming.parser import parse_records
from spark.streaming.sinks import lake
from spark.streaming.stateful import OUTPUT_SCHEMA, stateful_records
from spark.streaming.validation import classify_records


@pytest.fixture(scope="module")
def session(tmp_path_factory):
    runtime = tmp_path_factory.mktemp("lake-sql")
    spark = create_session(SparkConfig(master="local[2]", runtime_dir=runtime), kafka=False)
    yield spark
    spark.stop()


def batch_outputs(session):
    previous, current, _ = demo_history("lake-test")
    result = process_snapshot_pair(previous, current)
    timestamp = current.event_time
    normalized = normalize_snapshot(current)
    rows = [
        (
            "normalized_snapshot",
            current.asset_id,
            timestamp,
            "{}",
            json.dumps(
                {
                    "normalization_version": "text-v1",
                    "asset_id": current.asset_id,
                    "snapshot_id": current.snapshot_id,
                    "version": 2,
                    "event_time": current.event_time.isoformat(),
                    "ingest_time": current.ingest_time.isoformat(),
                    "raw_hash": current.hash,
                    "normalized_hash": normalized.normalized_hash,
                    "normalized_content": normalized.normalized_content,
                }
            ),
        ),
        ("config_diff", current.asset_id, timestamp, "{}", result.diff.model_dump_json()),
        (
            "alert",
            current.asset_id,
            timestamp,
            "{}",
            serialize_record(
                evaluate_rules(result.diff, current.event_time, load_rules("rules/default.yaml"))[0]
            ),
        ),
        (
            "invalid",
            None,
            None,
            json.dumps(
                {
                    "validation_error": "malformed_json",
                    "kafka_timestamp": current.event_time.isoformat(),
                }
            ),
            None,
        ),
    ]
    records = session.createDataFrame(
        [(b"lake-test", b"\xff{invalid-json}", "config.snapshots", 1, 42, timestamp)],
        "key BINARY, value BINARY, topic STRING, partition INT, offset LONG, timestamp TIMESTAMP",
    )
    return session.createDataFrame(rows, OUTPUT_SCHEMA).unionByName(lake.raw_outputs(records))


def test_typed_projections_preserve_raw_bytes_and_safe_processed_fields(session) -> None:
    batch = batch_outputs(session)
    raw = lake.dataset_frame(batch, "raw/snapshots").first()
    assert raw.kafka_key == b"lake-test"
    assert raw.kafka_value == b"\xff{invalid-json}"
    assert (raw.kafka_partition, raw.kafka_offset, raw.year, raw.month, raw.day) == (
        1,
        42,
        2026,
        10,
        5,
    )
    processed = lake.dataset_frame(batch, "processed/snapshots").first()
    assert processed.normalized_content.rstrip().endswith("password ***")
    assert processed.version == 2
    event_us = (
        lake.dataset_frame(batch, "processed/snapshots")
        .selectExpr("unix_micros(event_time)")
        .first()[0]
    )
    assert event_us == int(datetime(2026, 10, 5, 0, 0, 2, tzinfo=UTC).timestamp() * 1_000_000)
    diff = lake.dataset_frame(batch, "processed/diffs").first()
    assert (diff.added, diff.removed, diff.changed) == (1, 1, 1)
    alert = lake.dataset_frame(batch, "processed/alerts").first()
    assert (alert.rule_id, alert.severity) == ("logging-disabled", "high")
    diagnostic = lake.dataset_frame(batch, "processed/diagnostics").first()
    assert diagnostic.year == 2026
    assert "normalized_content" not in diagnostic.asDict()


def test_partition_dates_use_utc_and_null_diagnostic_fallback(session) -> None:
    batch = session.createDataFrame(
        [
            (
                "normalized_snapshot",
                "asset",
                datetime(2026, 10, 6),
                "{}",
                json.dumps({"event_time": "2026-10-06T01:30:00+02:00", "version": 1}),
            ),
            ("diagnostic", "asset", None, "{}", None),
        ],
        OUTPUT_SCHEMA,
    )
    assert lake.dataset_frame(batch, "processed/snapshots").first().day == 5
    assert lake.dataset_frame(batch, "processed/diagnostics").first().year == 1970


def test_partial_write_retry_and_committed_replay(session, tmp_path, monkeypatch) -> None:
    config = SparkConfig(checkpoint_dir=tmp_path / "checkpoint", lake=LakeConfig("user", "value"))
    config.checkpoint_dir.mkdir()
    query_id = str(uuid4())
    (config.checkpoint_dir / "metadata").write_text(json.dumps({"id": query_id}))
    root = tmp_path / "lake"
    monkeypatch.setattr(LakeConfig, "uri", lambda self, suffix: str(root / suffix))
    commits = {}
    monkeypatch.setattr(
        lake, "load_commit", lambda config, query, batch: commits.get((query, batch))
    )
    monkeypatch.setattr(
        lake,
        "save_commit",
        lambda config, item: commits.update({(item["query_id"], item["batch_id"]): item}),
    )
    monkeypatch.setattr(lake, "list_commits", lambda config, query: iter(commits.values()))
    batch = batch_outputs(session).cache()
    counts = {
        row.record_type: row["count"] for row in batch.groupBy("record_type").count().collect()
    }
    original = lake.dataset_frame

    def fail_second_dataset(frame, dataset):
        if dataset == "processed/snapshots":
            raise RuntimeError("injected Parquet write failure")
        return original(frame, dataset)

    try:
        monkeypatch.setattr(lake, "dataset_frame", fail_second_dataset)
        with pytest.raises(RuntimeError, match="injected"):
            lake.write_lake_batch(batch, 0, config, counts)
        assert list(root.rglob("*.parquet"))
        assert not commits
        assert lake.read_committed(session, config.lake, "raw/snapshots").count() == 0
        monkeypatch.setattr(lake, "dataset_frame", original)
        lake.write_lake_batch(batch, 0, config, counts)
        before = {path: path.read_bytes() for path in root.rglob("*.parquet")}
        lake.write_lake_batch(batch, 0, config, counts)
        assert {path: path.read_bytes() for path in root.rglob("*.parquet")} == before
        for dataset in lake.DATASET_SCHEMAS:
            assert lake.read_committed(session, config.lake, dataset).count() == 1
        with pytest.raises(RuntimeError, match="disagrees"):
            lake.write_lake_batch(batch, 0, config, {**counts, "raw_snapshot": 2})
        with pytest.raises(ValueError, match="Unknown"):
            lake.read_committed(session, config.lake, "../other")
        commits[(query_id, 0)]["datasets"]["raw/snapshots"] = str(root / "uncommitted")
        with pytest.raises(RuntimeError, match="unexpected"):
            lake.read_committed(session, config.lake, "raw/snapshots")
    finally:
        batch.unpersist()


def test_raw_archive_branch_keeps_duplicates_and_watermark_drops(session, tmp_path) -> None:
    from datetime import timedelta

    from schemas import ConfigSnapshot

    config = SparkConfig(
        master="local[2]",
        checkpoint_dir=tmp_path / "checkpoint",
        watermark_delay="10 seconds",
        state_timeout="30 seconds",
        lake=LakeConfig("test-user", "test-value"),
    )
    directory = tmp_path / "source"
    directory.mkdir()
    previous, current, _ = demo_history("late-asset")
    clock = ConfigSnapshot.model_validate(
        {
            **previous.model_dump(),
            "asset_id": "clock-asset",
            "snapshot_id": "clock-snapshot",
            "event_time": previous.event_time + timedelta(seconds=100),
        }
    )
    received = []
    progress = []

    def run(records, number):
        (directory / f"{number}.json").write_text(
            "\n".join(
                json.dumps(
                    {
                        "key": item.asset_id,
                        "value": serialize_record(item),
                        "topic": "config.snapshots",
                        "partition": 0,
                        "offset": number * 10 + index,
                        "timestamp": item.ingest_time.isoformat(),
                    }
                )
                for index, item in enumerate(records)
            )
        )
        source = session.readStream.schema(
            "key STRING, value STRING, topic STRING, partition INT, offset LONG, "
            "timestamp TIMESTAMP"
        ).json(str(directory))
        outputs = stateful_records(
            classify_records(parse_records(source)), config, load_rules(config.rules_file), source
        )
        captured = []
        query = (
            outputs.writeStream.foreachBatch(
                lambda batch, batch_id: captured.extend(batch.collect())
            )
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

    run([previous, previous], 0)
    assert sum(row.record_type == "raw_snapshot" for row in received) == 2
    assert sum(row.record_type == "normalized_snapshot" for row in received) == 1
    run([clock], 1)
    late = run([current], 2)
    assert sum(row.record_type == "raw_snapshot" for row in late) == 1
    assert not any(row.record_type == "normalized_snapshot" for row in late)
    assert any(
        operator.get("numRowsDroppedByWatermark", 0) > 0
        for batch in progress
        for operator in batch.get("stateOperators", [])
    )
