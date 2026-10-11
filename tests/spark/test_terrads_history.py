import json
from dataclasses import replace

from analytics.config import AnalyticsConfig
from analytics.jobs import asset_summary, change_frequency, diff_magnitude, fleet_activity
from lake.config import LakeConfig
from messaging.serialization import serialize_record
from processing.rules import load_rules
from schemas import ConfigDiff
from spark.common.config import SparkConfig
from spark.common.runtime import create_session
from spark.streaming.parser import parse_records
from spark.streaming.stateful import stateful_records
from spark.streaming.validation import classify_records
from workloads.observed import load_dataset


def test_native_terraform_history_pairing_and_analytics(terraform_fixture, tmp_path):
    snapshots, _, _ = load_dataset(terraform_fixture[0])
    source = tmp_path / "source"
    source.mkdir()
    ordered = sorted(snapshots, key=lambda item: (item.asset_id, -item.version))
    rows = [
        {
            "key": item.asset_id,
            "value": serialize_record(item),
            "topic": "config.snapshots",
            "partition": 0,
            "offset": index,
            "timestamp": item.ingest_time.isoformat(),
        }
        for index, item in enumerate([*ordered, ordered[0]])
    ]
    (source / "input.json").write_text("\n".join(json.dumps(row) for row in rows))
    config = SparkConfig(
        master="local[2]",
        runtime_dir=tmp_path / "runtime",
        checkpoint_dir=tmp_path / "checkpoint",
        watermark_delay="1000000 hours",
        state_timeout="1000000 hours",
        max_versions_per_asset=32,
        lake=LakeConfig(access_key="fixture", secret_key="fixture"),
    )
    session = create_session(replace(config, lake=None), kafka=False)
    captured = []
    try:
        raw = session.readStream.schema(
            "key STRING, value STRING, topic STRING, partition INT, offset LONG, "
            "timestamp TIMESTAMP"
        ).json(str(source))
        outputs = stateful_records(
            classify_records(parse_records(raw)),
            config,
            load_rules(config.rules_file),
            raw_records=raw,
        )

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
        finally:
            query.stop()
        diffs = [
            ConfigDiff.model_validate_json(row.value)
            for row in captured
            if row.record_type == "config_diff"
        ]
        versions = {snapshot.snapshot_id: snapshot for snapshot in snapshots}
        expected = {
            (old.snapshot_id, new.snapshot_id)
            for old in snapshots
            for new in snapshots
            if old.asset_id == new.asset_id and new.version == old.version + 1
        }
        assert len(diffs) == 3
        assert {(diff.old_snapshot_id, diff.new_snapshot_id) for diff in diffs} == expected
        assert sum(row.record_type == "normalized_snapshot" for row in captured) == 8
        assert not any(row.record_type == "alert" for row in captured)
        inputs = {
            "snapshot": session.createDataFrame(
                [(item.asset_id, item.event_time) for item in snapshots],
                "asset_id STRING, event_time TIMESTAMP",
            ),
            "diff": session.createDataFrame(
                [
                    (
                        diff.asset_id,
                        versions[diff.new_snapshot_id].event_time,
                        diff.added,
                        diff.removed,
                        diff.changed,
                    )
                    for diff in diffs
                ],
                "asset_id STRING, event_time TIMESTAMP, added LONG, removed LONG, changed LONG",
            ),
            "alert": session.createDataFrame(
                [], "asset_id STRING, created_at TIMESTAMP, severity STRING, rule_id STRING"
            ),
        }
        frequency = change_frequency(inputs, AnalyticsConfig())
        assert frequency.groupBy().sum("snapshot_count").first()[0] == 8
        assert frequency.groupBy().sum("diff_count").first()[0] == 3
        assert fleet_activity(frequency).agg({"assets_changed": "max"}).first()[0] == 1
        assert diff_magnitude(inputs["diff"]).first().changed_max == 1
        assert (
            sum(row.diff_count for row in asset_summary(inputs, AnalyticsConfig()).collect()) == 3
        )
    finally:
        session.stop()
