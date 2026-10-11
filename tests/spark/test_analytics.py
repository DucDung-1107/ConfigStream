from datetime import datetime, timedelta

import pytest

from analytics.config import AnalyticsConfig
from analytics.jobs import all_jobs, frequency_anomalies
from spark.common.config import SparkConfig
from spark.common.runtime import create_session


@pytest.fixture(scope="module")
def session(tmp_path_factory):
    session = create_session(
        SparkConfig(master="local[2]", runtime_dir=tmp_path_factory.mktemp("analytics-runtime")),
        kafka=False,
    )
    yield session
    session.stop()


@pytest.fixture(scope="module")
def results(session):
    start = datetime(2026, 1, 1)
    inputs = {
        "snapshot": session.createDataFrame(
            [
                ("A", start),
                ("A", start + timedelta(hours=1)),
                ("B", start + timedelta(hours=1)),
            ],
            "asset_id STRING, event_time TIMESTAMP",
        ),
        "diff": session.createDataFrame(
            [
                ("A", start + timedelta(hours=1), 2, 2, 2),
                ("B", start + timedelta(hours=1), 1, 0, 0),
            ],
            "asset_id STRING, event_time TIMESTAMP, added LONG, removed LONG, changed LONG",
        ),
        "alert": session.createDataFrame(
            [
                ("A", start + timedelta(hours=1), "high", "logging-disabled"),
            ],
            "asset_id STRING, created_at TIMESTAMP, severity STRING, rule_id STRING",
        ),
        "diagnostic": session.createDataFrame(
            [
                ("diagnostic", start, '{"reason":"version_conflict"}'),
                ("invalid", start, '{"validation_error":"invalid_json"}'),
            ],
            "record_type STRING, event_time TIMESTAMP, summary STRING",
        ),
    }
    return {
        name: frame.collect()
        for name, frame in all_jobs(inputs, AnalyticsConfig(window="1 hour")).items()
    }


def test_counts_and_window_ground_truth(results):
    frequency = results["change-frequency"]
    assert sum(row.snapshot_count for row in frequency) == 3
    assert sum(row.diff_count for row in frequency) == 2
    assert sum(row.alert_count for row in frequency) == 1
    fleet = sorted(results["fleet-activity"], key=lambda row: row.window_start)
    assert fleet[1].assets_changed == 2
    assert fleet[0].diff_count == 0


def test_diff_magnitude_and_summary(results):
    magnitude = results["diff-magnitude"][0]
    assert magnitude.added_mean == 1.5
    assert magnitude.changed_max == 2
    assert magnitude.diff_count == 2
    summary = {row.asset_id: row for row in results["asset-summary"]}
    assert summary["A"].snapshot_count == 2
    assert summary["A"].high_alert_count == 1
    assert summary["B"].alert_count == 0


def test_alert_and_diagnostic_trends(results):
    assert results["alert-trends"][0].severity == "high"
    assert {row.diagnostic_type for row in results["diagnostic-trends"]} == {
        "version_conflict",
        "invalid_record",
    }


@pytest.mark.parametrize("fleet", [False, True])
def test_prior_only_anomaly_and_zero_variance(session, fleet):
    start = datetime(2026, 1, 1)
    values = [
        ("A", start + timedelta(days=index), count)
        for index, count in enumerate([1, 1, 1, 10, 1, 100])
    ]
    frame = session.createDataFrame(values, "asset_id STRING, window_start TIMESTAMP, count LONG")
    if fleet:
        frame = frame.drop("asset_id")
    rows = (
        frequency_anomalies(frame, "count", AnalyticsConfig(), fleet=fleet)
        .orderBy("window_start")
        .collect()
    )
    assert not any(row.is_anomaly for row in rows[:3])
    assert rows[3].is_anomaly
    assert rows[3].baseline_mean == 1
    assert rows[3].baseline_stddev == 0
    assert rows[3].z_score is None
    assert not rows[4].is_anomaly


def test_dense_windows_fill_idle_periods(session):
    frame = session.createDataFrame(
        [("A", datetime(2026, 1, 1), 1), ("A", datetime(2026, 1, 5), 10)],
        "asset_id STRING, window_start TIMESTAMP, count LONG",
    )
    rows = frequency_anomalies(frame, "count", AnalyticsConfig()).orderBy("window_start").collect()
    assert len(rows) == 5
    assert [row["count"] for row in rows] == [1, 0, 0, 0, 10]
    assert rows[-1].is_anomaly
