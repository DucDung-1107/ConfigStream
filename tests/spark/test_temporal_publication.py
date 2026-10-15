import json
from pathlib import Path

import pytest

from lake import temporal
from lake.config import LakeConfig
from schemas.temporal.provenance import SourceManifest
from schemas.temporal.records import Envelope, Evidence, RisOccurrence
from spark.common.config import SparkConfig
from spark.common.runtime import create_session
from spark.streaming.sinks.lake import read_committed


@pytest.mark.parametrize("failure", ["before_output", "before_manifest"])
def test_legacy_publication_failure_boundaries_unchanged(tmp_path, monkeypatch, failure):
    from scripts.kafka_stateful_demo import demo_history
    from spark.streaming.sinks import lake as legacy
    from spark.streaming.stateful import OUTPUT_SCHEMA

    config = SparkConfig(
        master="local[2]",
        runtime_dir=tmp_path / "runtime",
        checkpoint_dir=tmp_path / "checkpoint",
        lake=LakeConfig("owned", "owned"),
    )
    config.checkpoint_dir.mkdir()
    query = "00000000-0000-4000-8000-000000000016"
    (config.checkpoint_dir / "metadata").write_text(json.dumps({"id": query}))
    monkeypatch.setattr(LakeConfig, "uri", lambda self, suffix: str(tmp_path / "lake" / suffix))
    commits = {}
    monkeypatch.setattr(
        legacy, "load_commit", lambda lake, query, batch: commits.get((query, batch))
    )

    def save(lake, item):
        commits[(item["query_id"], item["batch_id"])] = item

    monkeypatch.setattr(legacy, "save_commit", save)
    snapshot = demo_history("owned")[0]
    payload = json.dumps(
        {
            "normalization_version": "text-v1",
            "snapshot_id": snapshot.snapshot_id,
            "asset_id": snapshot.asset_id,
            "version": 1,
            "event_time": snapshot.event_time.isoformat(),
            "ingest_time": snapshot.ingest_time.isoformat(),
            "raw_hash": snapshot.hash,
            "normalized_hash": snapshot.hash,
            "normalized_content": "owned fixture",
        }
    )
    session = create_session(
        SparkConfig(master="local[2]", runtime_dir=config.runtime_dir), kafka=False
    )
    try:
        batch = session.createDataFrame(
            [("normalized_snapshot", snapshot.asset_id, snapshot.event_time, "{}", payload)],
            OUTPUT_SCHEMA,
        )
        original_frame = legacy.dataset_frame

        def fail(*args):
            raise RuntimeError("owned legacy failure")

        if failure == "before_output":
            monkeypatch.setattr(legacy, "dataset_frame", fail)
        else:
            monkeypatch.setattr(legacy, "save_commit", fail)
        with pytest.raises(RuntimeError, match="legacy failure"):
            legacy.write_lake_batch(batch, 0, config, {"normalized_snapshot": 1})
        assert not commits
        assert (
            legacy.read_committed(session, config.lake, "processed/snapshots", manifests=[]).count()
            == 0
        )
        monkeypatch.setattr(legacy, "dataset_frame", original_frame)
        monkeypatch.setattr(legacy, "save_commit", save)
        legacy.write_lake_batch(batch, 0, config, {"normalized_snapshot": 1})
        published = commits[(query, 0)]
        assert published["format_version"] == "2.0"
        assert published["files"]["processed/snapshots"]
        before = {path: path.read_bytes() for path in (tmp_path / "lake").rglob("*.parquet")}
        legacy.write_lake_batch(batch, 0, config, {"normalized_snapshot": 1})
        assert before == {
            path: path.read_bytes() for path in (tmp_path / "lake").rglob("*.parquet")
        }
        restored = legacy.read_committed(
            session, config.lake, "processed/snapshots", manifests=[published]
        ).first()
        assert (
            restored.snapshot_id == snapshot.snapshot_id
            and restored.normalization_version == "text-v1"
        )
    finally:
        session.stop()


def test_additive_typed_parquet_publication_recovery_and_old_reader(tmp_path, monkeypatch):
    fixture = json.loads(Path("tests/fixtures/phase16/ris.json").read_text())
    record = Evidence.model_validate(fixture["record"])
    manifest = SourceManifest.model_validate(fixture["manifest"])
    session = create_session(
        SparkConfig(master="local[2]", runtime_dir=tmp_path / "runtime"), kafka=False
    )
    config = LakeConfig("owned", "owned")
    query = "00000000-0000-4000-8000-000000000016"
    commits = {}
    monkeypatch.setattr(LakeConfig, "uri", lambda self, suffix: str(tmp_path / "lake" / suffix))
    monkeypatch.setattr(
        temporal,
        "load_typed_commit",
        lambda config, query, batch, kind: commits.get((query, batch, kind)),
    )

    def save(config, item):
        commits[(item["query_id"], item["batch_id"], item["record_kind"])] = item

    monkeypatch.setattr(temporal, "save_typed_commit", save)
    schema = "logical_record_id STRING, envelope_json STRING, payload_json STRING"

    def writer(path, rows):
        session.createDataFrame(
            [
                (
                    row.envelope.logical_record_id,
                    row.envelope.model_dump_json(),
                    row.payload.model_dump_json(),
                )
                for row in rows
            ],
            schema,
        ).write.mode("overwrite").parquet(path)

    try:

        def fail(config, item):
            raise RuntimeError("owned failure before manifest")

        monkeypatch.setattr(temporal, "save_typed_commit", fail)
        with pytest.raises(RuntimeError, match="before manifest"):
            temporal.publish_typed_batch(
                config,
                query,
                0,
                "ris_occurrence",
                (record,),
                {manifest.manifest_id: manifest},
                writer,
            )
        assert list((tmp_path / "lake").rglob("*.parquet"))
        assert (
            temporal.committed_typed_paths(config, "ris_occurrence", tuple(commits.values())) == ()
        )
        monkeypatch.setattr(temporal, "save_typed_commit", save)
        published = temporal.publish_typed_batch(
            config,
            query,
            0,
            "ris_occurrence",
            (record, record),
            {manifest.manifest_id: manifest},
            writer,
        )
        before = {path: path.read_bytes() for path in (tmp_path / "lake").rglob("*.parquet")}
        temporal.publish_typed_batch(
            config, query, 0, "ris_occurrence", (record,), {manifest.manifest_id: manifest}, writer
        )
        assert before == {
            path: path.read_bytes() for path in (tmp_path / "lake").rglob("*.parquet")
        }
        paths = temporal.committed_typed_paths(config, "ris_occurrence", (published,))
        restored = session.read.schema(schema).parquet(*paths).collect()
        assert (
            len(restored) == 1
            and restored[0].logical_record_id == record.envelope.logical_record_id
        )
        assert restored[0].payload_json == record.payload.model_dump_json()

        def loader(path):
            return tuple(
                Evidence(
                    envelope=Envelope.model_validate_json(row.envelope_json),
                    payload=RisOccurrence.model_validate_json(row.payload_json),
                )
                for row in session.read.schema(schema).parquet(path).collect()
            )

        assert temporal.read_typed_projection(
            config, "ris_occurrence", (published,), loader, {manifest.manifest_id: manifest}
        ) == (record,)
        assert read_committed(session, config, "processed/snapshots", manifests=[]).count() == 0
        with pytest.raises(ValueError, match="Unknown"):
            read_committed(session, config, "temporal/ris/occurrences", manifests=[])
    finally:
        session.stop()
