import json
from datetime import UTC, datetime
from hashlib import sha256
from unittest.mock import Mock

import pandas as pd
import pytest
from pyspark.sql import Row
from pyspark.sql.streaming.stateful_processor import TimerValues

from processing.rules import load_rules
from processing.state import AssetVersionState
from spark.common.config import SparkConfig
from spark.streaming import outputs
from spark.streaming.stateful import VersionProcessor


def test_sink_failure_propagates_and_unpersists(monkeypatch) -> None:
    batch = Mock()
    batch.groupBy.return_value.count.return_value.collect.return_value = [
        Row(record_type="alert", count=1)
    ]
    writer = Mock(side_effect=RuntimeError("sink unavailable"))
    monkeypatch.setattr(outputs, "write_alerts", writer)
    with pytest.raises(RuntimeError, match="sink unavailable"):
        outputs.publish_batch(batch, 1, SparkConfig())
    writer.assert_called_once()
    batch.unpersist.assert_called_once()


@pytest.mark.parametrize(
    "condition", ["format", "uri", "hash", "asset_identity", "snapshot_identity", "multiline"]
)
def test_processing_rejections_are_safe(condition, monkeypatch) -> None:
    content = "logging enabled\n"
    row = {
        "schema_version": "1.0",
        "snapshot_id": "snapshot-1",
        "asset_id": "asset-1",
        "version": 1,
        "config_format": "text",
        "content": content,
        "content_uri": None,
        "hash": sha256(content.encode()).hexdigest(),
        "event_time": datetime(2026, 10, 5, tzinfo=UTC),
        "ingest_time": datetime(2026, 10, 5, tzinfo=UTC),
        "kafka_topic": "config.snapshots",
        "kafka_partition": 0,
        "kafka_offset": 0,
    }
    if condition == "format":
        row["config_format"] = "json"
    elif condition == "uri":
        row.update(content=None, content_uri="s3://config-lake/private-path")
    elif condition == "hash":
        row["hash"] = "a" * 64
    elif condition == "asset_identity":
        row["asset_id"] = " asset-1 "
    elif condition == "snapshot_identity":
        row["snapshot_id"] = " snapshot-1 "
    else:
        row["content"] = 'password "synthetic-private-value\n'
        row["hash"] = sha256(row["content"].encode()).hexdigest()
    processor = VersionProcessor(SparkConfig(), load_rules("rules/default.yaml"))
    monkeypatch.setattr(processor, "_load", lambda asset: AssetVersionState(asset))
    save = Mock()
    monkeypatch.setattr(processor, "_save", save)
    frames = list(
        processor.handleInputRows(
            (row["asset_id"],), iter([pd.DataFrame([row])]), TimerValues(0, 0)
        )
    )
    assert len(frames) == 1
    result = frames[0].iloc[0].tolist()
    assert result[0] == "diagnostic"
    assert json.loads(result[3])["reason"] == "processing_rejected"
    assert result[4] is None
    assert "synthetic-private-value" not in str(result)
    assert "private-path" not in str(result)
    assert save.call_args.args[0].snapshots == ()
