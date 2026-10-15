import json
from datetime import timedelta
from hashlib import sha256
from uuid import uuid4

import pyarrow.parquet as parquet
import pytest

from lake.config import LakeConfig
from messaging.serialization import serialize_record
from processing.rules import load_rules
from schemas import ConfigSnapshot
from spark.common.config import SparkConfig
from spark.common.runtime import create_session
from spark.streaming.identity import checkpoint_contract
from spark.streaming.parser import parse_records
from spark.streaming.sinks import lake
from spark.streaming.stateful import OUTPUT_SCHEMA, stateful_records
from spark.streaming.validation import classify_records
from tests.spark.test_stateful import snapshot


@pytest.fixture(scope="module")
def session(tmp_path_factory):
    spark = create_session(
        SparkConfig(master="local[2]", runtime_dir=tmp_path_factory.mktemp("integrity")),
        kafka=False,
    )
    yield spark
    spark.stop()


@pytest.fixture
def committed(session, tmp_path, monkeypatch):
    config = SparkConfig(
        checkpoint_dir=tmp_path / "checkpoint",
        lake=LakeConfig("owned", "owned", max_records_per_file=1),
    )
    config.checkpoint_dir.mkdir()
    query = str(uuid4())
    (config.checkpoint_dir / "metadata").write_text(json.dumps({"id": query}))
    root = tmp_path / "lake"
    monkeypatch.setattr(LakeConfig, "uri", lambda self, suffix: str(root / suffix))
    commits = []
    monkeypatch.setattr(lake, "load_commit", lambda *args: commits[0] if commits else None)
    monkeypatch.setattr(lake, "save_commit", lambda config, commit: commits.append(commit))
    monkeypatch.setattr(lake, "list_commits", lambda *args: iter(commits))
    frame = session.createDataFrame(
        [("diagnostic", "owned", None, json.dumps({"ordinal": index}), None) for index in (1, 2)],
        OUTPUT_SCHEMA,
    ).coalesce(1)
    lake.write_lake_batch(frame, 0, config, {"diagnostic": 2})
    assert lake.read_committed(session, config.lake, "processed/diagnostics").count() == 2
    return config, frame, commits[0], sorted(root.rglob("*.parquet"))


@pytest.mark.parametrize("damage", ["missing", "extra", "changed", "inventory", "dataset"])
def test_committed_corruption_fails_closed(session, committed, damage):
    config, frame, manifest, files = committed
    assert len(files) == 2
    if damage == "missing":
        files[-1].unlink()
    elif damage == "extra":
        files[0].with_name("unexpected.parquet").write_bytes(files[0].read_bytes())
    elif damage == "changed":
        table = parquet.ParquetFile(files[0]).read()
        table = table.set_column(
            table.schema.get_field_index("summary"), "summary", [["changed same row count"]]
        )
        parquet.write_table(table, files[0])
        files[0].with_name(f".{files[0].name}.crc").unlink()
        assert session.read.parquet(str(files[0])).count() == 1
    elif damage == "inventory":
        manifest["files"] = {}
    else:
        manifest["datasets"] = {}
    with pytest.raises(RuntimeError, match="inventory|checksum"):
        lake.read_committed(session, config.lake, "processed/diagnostics")


@pytest.mark.parametrize("legacy", [False, True])
def test_replay_compares_exact_projection_and_preserves_files(session, committed, legacy):
    from pyspark.sql import functions as functions

    config, frame, manifest, files = committed
    if legacy:
        manifest["format_version"] = "1.0"
        del manifest["files"]
    before = [path.read_bytes() for path in files]
    lake.write_lake_batch(frame, 0, config, {"diagnostic": 2})
    with pytest.raises(RuntimeError, match="projection disagrees"):
        lake.write_lake_batch(
            frame.withColumn("summary", functions.lit("changed")), 0, config, {"diagnostic": 2}
        )
    assert before == [path.read_bytes() for path in files]
    if legacy:
        files[-1].unlink()
        with pytest.raises(RuntimeError, match="row count"):
            lake.read_committed(session, config.lake, "processed/diagnostics")


def test_state_upgrade_requires_new_checkpoint_and_fixed_partitions(tmp_path):
    config = SparkConfig(checkpoint_dir=tmp_path / "old")
    config.checkpoint_dir.mkdir()
    (config.checkpoint_dir / "metadata").write_text("owned existing checkpoint")
    with pytest.raises(RuntimeError, match="Legacy checkpoint"):
        checkpoint_contract(config, 2)
    fresh = SparkConfig(checkpoint_dir=tmp_path / "new")
    checkpoint_contract(fresh, 2)
    checkpoint_contract(fresh, 2)
    with pytest.raises(RuntimeError, match="Incompatible"):
        checkpoint_contract(fresh, 4)


def test_search_skips_only_known_empty_categories(session, monkeypatch):
    from search import sink

    observed = []
    monkeypatch.setattr(sink, "index_frame", lambda frame, kind, config: observed.append(kind))
    frame = session.createDataFrame([], OUTPUT_SCHEMA)
    sink.index_batch(frame, SparkConfig(), {"invalid": 1})
    assert observed == ["diagnostics"]
    observed.clear()
    sink.index_batch(frame, SparkConfig(), {})
    assert observed == []


def test_batch_profile_is_explicit_and_streaming_defaults_restored(session, tmp_path):
    config = SparkConfig(master="local[2]", runtime_dir=tmp_path / "profiles")
    batch = create_session(config, kafka=False, profile="batch")
    try:
        assert batch.conf.get("spark.sql.adaptive.enabled") == "true"
        assert batch.conf.get("spark.sql.shuffle.partitions") == "8"
        query = batch.range(10).groupBy().sum("id")
        assert query.first()[0] == 45
        assert "AdaptiveSparkPlan" in query._jdf.queryExecution().executedPlan().toString()
    finally:
        streaming = create_session(config, kafka=False)
        assert streaming.conf.get("spark.sql.adaptive.enabled") == "false"
        assert streaming.conf.get("spark.sql.shuffle.partitions") == "2"


def test_stream_identity_conflicts_across_restart_and_assets(session, tmp_path):
    config = SparkConfig(checkpoint_dir=tmp_path / "checkpoint", lake=LakeConfig("owned", "owned"))
    source = tmp_path / "source"
    source.mkdir()

    def run(records, number):
        (source / f"{number}.json").write_text(
            "\n".join(
                json.dumps(
                    {
                        "key": item.asset_id,
                        "value": serialize_record(item),
                        "topic": "config.snapshots",
                        "partition": 0,
                        "offset": number * 100 + index,
                        "timestamp": item.ingest_time.isoformat(),
                    }
                )
                for index, item in enumerate(records)
            )
        )
        raw = session.readStream.schema(
            "key STRING, value STRING, topic STRING, partition INT, offset LONG, "
            "timestamp TIMESTAMP"
        ).json(str(source))
        rows = []
        query = (
            stateful_records(
                classify_records(parse_records(raw)), config, load_rules(config.rules_file), raw
            )
            .writeStream.foreachBatch(lambda frame, batch_id: rows.extend(frame.collect()))
            .option("checkpointLocation", str(config.checkpoint_dir))
            .trigger(availableNow=True)
            .start()
        )
        try:
            assert query.awaitTermination(120)
            assert query.exception() is None
        finally:
            query.stop()
        return rows

    original = snapshot(1)
    other = original.model_copy(
        update={"snapshot_id": "distinct-native-source", "asset_id": "other-asset"}
    )
    first = run([original, original, other], 0)
    assert sum(row.record_type == "normalized_snapshot" for row in first) == 2
    changes = [
        {"content": "logging changed\n", "hash": sha256(b"logging changed\n").hexdigest()},
        {"asset_id": "wrong-asset"},
        {"version": 2},
        {"event_time": original.event_time + timedelta(seconds=1)},
        {"config_format": "other-format"},
    ]
    records = [ConfigSnapshot.model_validate(original.model_dump() | change) for change in changes]
    second = run(
        records
        + [
            original.model_copy(update={"ingest_time": original.ingest_time + timedelta(seconds=1)})
        ],
        1,
    )
    assert not any(row.record_type == "normalized_snapshot" for row in second)
    conflicts = [json.loads(row.summary) for row in second if row.record_type == "diagnostic"]
    assert len(conflicts) == 5
    assert {item["reason"] for item in conflicts} == {"snapshot_identity_conflict"}
    assert sum(row.record_type == "raw_snapshot" for row in second) == 6
