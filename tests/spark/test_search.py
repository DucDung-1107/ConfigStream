import json

from search.config import SearchConfig
from search.documents import project_document
from search.sink import safe_frame
from spark.common.config import SparkConfig
from spark.common.runtime import create_session


def test_safe_spark_projection_keeps_utc_and_excludes_config(tmp_path):
    session = create_session(SparkConfig(master="local[2]", runtime_dir=tmp_path), kafka=False)
    try:
        frame = session.sql("""
            SELECT 'snapshot-1' snapshot_id, 'router' asset_id, 2L version,
            timestamp('2026-10-11T01:30:00.123456+02:00') event_time,
            timestamp('2026-10-11T00:00:00Z') ingest_time,
            'raw-hash' raw_hash, 'normalized-hash' normalized_hash,
            'text-v1' normalization_version, 'SENSITIVE_TEST_MARKER' normalized_content
        """)
        serialized = safe_frame(frame, "snapshots").first().document_json
        assert "SENSITIVE_TEST_MARKER" not in serialized
        document = project_document("snapshots", json.loads(serialized))
        assert document["event_time"] == "2026-10-10T23:30:00.123456Z"
        assert "normalized_content" not in document
    finally:
        session.stop()


def test_output_order_and_search_failure_prevents_kafka(monkeypatch, tmp_path):
    from lake.config import LakeConfig
    from spark.streaming.outputs import publish_batch

    session = create_session(SparkConfig(master="local[2]", runtime_dir=tmp_path), kafka=False)
    order = []
    config = SparkConfig(lake=LakeConfig("test-user", "test-value"), search=SearchConfig())
    try:
        batch = session.createDataFrame([("alert", "{}")], "record_type STRING, summary STRING")
        monkeypatch.setattr(
            "spark.streaming.sinks.lake.write_lake_batch", lambda *args: order.append("lake")
        )
        monkeypatch.setattr("search.sink.index_batch", lambda *args: order.append("search"))
        monkeypatch.setattr(
            "spark.streaming.outputs.write_alerts", lambda *args: order.append("kafka")
        )
        publish_batch(batch, 0, config)
        assert order == ["lake", "search", "kafka"]

        def failure(*args):
            order.append("search-failed")
            raise RuntimeError("injected search failure")

        import pytest

        order.clear()
        monkeypatch.setattr("search.sink.index_batch", failure)
        with pytest.raises(RuntimeError, match="injected"):
            publish_batch(batch, 1, config)
        assert order == ["lake", "search-failed"]
        assert not batch.is_cached
    finally:
        session.stop()
