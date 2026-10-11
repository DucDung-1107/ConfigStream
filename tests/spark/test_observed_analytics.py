import pytest

from analytics.config import AnalyticsConfig
from analytics.jobs import asset_summary, change_frequency, diff_magnitude, fleet_activity
from processing.diff import process_snapshot_pair
from spark.common.config import SparkConfig
from spark.common.runtime import create_session
from workloads.observed import load_dataset


@pytest.fixture(scope="module")
def session(tmp_path_factory):
    session = create_session(
        SparkConfig(master="local[2]", runtime_dir=tmp_path_factory.mktemp("observed-runtime")),
        kafka=False,
    )
    yield session
    session.stop()


def test_observed_spark_counts_windows_and_diff_magnitude(session, observed_fixture):
    snapshots, _, _ = load_dataset(observed_fixture[0])
    pairs = [
        (previous, current)
        for previous in snapshots
        for current in snapshots
        if previous.asset_id == current.asset_id and current.version == previous.version + 1
    ]
    diffs = [
        (
            current.asset_id,
            current.event_time,
            *[
                getattr(process_snapshot_pair(previous, current).diff, field)
                for field in ["added", "removed", "changed"]
            ],
        )
        for previous, current in pairs
    ]
    inputs = {
        "snapshot": session.createDataFrame(
            [(snapshot.asset_id, snapshot.event_time) for snapshot in snapshots],
            "asset_id STRING, event_time TIMESTAMP",
        ),
        "diff": session.createDataFrame(
            diffs, "asset_id STRING, event_time TIMESTAMP, added LONG, removed LONG, changed LONG"
        ),
        "alert": session.createDataFrame(
            [], "asset_id STRING, created_at TIMESTAMP, severity STRING, rule_id STRING"
        ),
    }
    frequency = change_frequency(inputs, AnalyticsConfig(window="1 day"))
    assert frequency.groupBy().sum("snapshot_count").first()[0] == 7
    assert frequency.groupBy().sum("diff_count").first()[0] == 4
    assert fleet_activity(frequency).agg({"assets_changed": "max"}).first()[0] == 2
    assert diff_magnitude(inputs["diff"]).first().changed_max == 1
    summary = asset_summary(inputs, AnalyticsConfig()).collect()
    assert sum(row.snapshot_count for row in summary) == 7
    assert sum(row.diff_count for row in summary) == 4
    assert all(row.alert_count == 0 for row in summary)
