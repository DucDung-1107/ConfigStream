import json
from datetime import UTC, datetime

import pytest

from search.config import KINDS, SearchConfig, boolean
from search.documents import bulk_action, project_document, utc_timestamp
from search.mappings import FIELDS, mapping


@pytest.mark.parametrize("kind", KINDS)
def test_names_and_explicit_safe_mapping(kind):
    config = SearchConfig()
    assert config.alias(kind) == f"configstream-{kind}"
    assert config.index(kind) == f"configstream-{kind}-v1"
    expected = mapping(kind)
    assert expected["dynamic"] == "strict"
    assert set(expected["properties"]) == set(FIELDS[kind])
    assert not {"content", "normalized_content", "details", "kafka_value", "value"} & set(
        FIELDS[kind]
    )
    assert expected["properties"]["asset_id"]["type"] == "keyword"


@pytest.mark.parametrize(
    "values",
    [
        {"ELASTICSEARCH_URL": "http://user:password@host"},
        {"ELASTICSEARCH_URL": "http://host?token=secret"},
        {"ELASTICSEARCH_URL": "ftp://host"},
        {"CONFIGSTREAM_SEARCH_INDEX_PREFIX": "../unsafe"},
        {"CONFIGSTREAM_SEARCH_INDEX_PREFIX": "Uppercase"},
        {"CONFIGSTREAM_SEARCH_BULK_SIZE": "0"},
        {"CONFIGSTREAM_SEARCH_BULK_SIZE": "10001"},
        {"CONFIGSTREAM_SEARCH_TIMEOUT_SECONDS": "0"},
        {"ELASTICSEARCH_USERNAME": "user"},
        {"CONFIGSTREAM_SEARCH_DIFF_DETAILS": "true"},
        {"CONFIGSTREAM_SEARCH_DIFF_DETAILS": "yes"},
    ],
)
def test_bad_config(values):
    with pytest.raises(ValueError):
        SearchConfig.from_env(values)


def test_config_private_and_optional_imports():
    import subprocess
    import sys

    config = SearchConfig.from_env(
        {"ELASTICSEARCH_USERNAME": "user", "ELASTICSEARCH_PASSWORD": "private-value"}
    )
    assert "private-value" not in repr(config)
    assert boolean({}, "CONFIGSTREAM_SEARCH_ENABLED") is False
    with pytest.raises(ValueError):
        boolean({"CONFIGSTREAM_SEARCH_ENABLED": "yes"}, "CONFIGSTREAM_SEARCH_ENABLED")
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import processing, search.documents, search.client; import sys; "
            "assert 'elasticsearch' not in sys.modules; assert 'pyspark' not in sys.modules",
        ],
        check=True,
    )


@pytest.mark.parametrize(
    "kind,identity", [("snapshots", "snapshot_id"), ("diffs", "diff_id"), ("alerts", "alert_id")]
)
def test_safe_projection_and_bulk_identity(kind, identity):
    row = {
        identity: "logical-id",
        "asset_id": "router",
        "event_time": "2026-10-11T07:00:00.123456+07:00",
        "created_at": "2026-10-11T00:00:00Z",
        "normalized_content": "password old-secret",
        "content": "password new-secret",
        "details": [{"line": "arbitrary-source"}],
        "kafka_value": "source-envelope",
    }
    action = bulk_action(kind, row, SearchConfig())
    assert action["_id"] == "logical-id"
    assert action["_index"] == f"configstream-{kind}"
    assert not any(
        value in json.dumps(action)
        for value in ("old-secret", "new-secret", "arbitrary-source", "source-envelope")
    )
    if "event_time" in action["_source"]:
        assert action["_source"]["event_time"] == "2026-10-11T00:00:00.123456Z"


def test_diagnostic_identity_and_exclusion():
    row = {
        "record_type": "invalid",
        "asset_id": None,
        "event_time": None,
        "summary": json.dumps(
            {
                "validation_error": "kafka_key_mismatch",
                "kafka_topic": "config.snapshots",
                "kafka_partition": 2,
                "kafka_offset": 10,
                "kafka_key": "password old-secret",
                "content": "arbitrary-source",
            }
        ),
    }
    first = project_document("diagnostics", row)
    assert first["diagnostic_type"] == "invalid_record"
    assert first == project_document("diagnostics", row)
    assert "old-secret" not in json.dumps(first)
    summary = json.loads(row["summary"])
    summary["kafka_offset"] = 11
    assert (
        first["diagnostic_id"]
        != project_document("diagnostics", {**row, "summary": json.dumps(summary)})["diagnostic_id"]
    )
    with pytest.raises(ValueError, match="Unsupported"):
        project_document("diagnostics", {**row, "summary": '{"reason":"password old-secret"}'})


def test_utc_and_mapping_types():
    assert utc_timestamp(datetime(2026, 10, 11, tzinfo=UTC)) == "2026-10-11T00:00:00.000000Z"
    with pytest.raises(ValueError, match="timestamp"):
        utc_timestamp("invalid")
    assert mapping("snapshots")["properties"]["raw_hash"]["index"] is False
    assert mapping("snapshots")["properties"]["event_time"]["type"] == "date_nanos"
    assert mapping("alerts")["properties"]["message"]["type"] == "text"
    with pytest.raises(ValueError):
        SearchConfig().alias("raw")


def test_invalid_diagnostic_time_falls_back_to_kafka_time():
    row = {
        "record_type": "invalid",
        "asset_id": None,
        "event_time": None,
        "summary": json.dumps(
            {
                "validation_error": "malformed_json",
                "kafka_timestamp": "2026-10-11T08:00:00+07:00",
                "kafka_topic": "config.snapshots",
                "kafka_partition": 1,
                "kafka_offset": 5,
            }
        ),
    }
    document = project_document("diagnostics", row)
    assert document["event_time"] == "2026-10-11T01:00:00.000000Z"
    assert document == project_document("diagnostics", row)
