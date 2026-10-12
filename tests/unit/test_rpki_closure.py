"""Offline profiling summaries retain failures and actual task counters."""

import json

from benchmarks.rpki_closure import comparison, event_metrics


def test_comparison_retains_negative_and_slow_runs():
    result = comparison(
        [
            {"mode": "baseline", "status": "PASS", "processing_seconds": 2.0},
            {"mode": "baseline", "status": "PASS", "processing_seconds": 4.0},
            {"mode": "reuse", "status": "FAIL"},
            {"mode": "reuse", "status": "PASS", "processing_seconds": 7.0},
        ]
    )
    assert result["baseline"]["median_seconds"] == 3.0
    assert result["baseline"]["range_seconds"] == [2.0, 4.0]
    assert result["reuse"]["failures"] == 1
    assert result["reuse"]["median_seconds"] == 7.0
    assert comparison([])["reuse"]["median_seconds"] is None


def test_task_metrics_are_execution_counters_not_physical_bytes(tmp_path):
    (tmp_path / "events_1_owned").write_text(
        "\n".join(
            json.dumps(event)
            for event in [
                {"Event": "SparkListenerJobStart"},
                {
                    "Event": "SparkListenerTaskEnd",
                    "Task Metrics": {
                        "Executor Run Time": 10,
                        "Input Metrics": {"Bytes Read": 42, "Records Read": 3},
                        "Shuffle Write Metrics": {"Shuffle Bytes Written": 9},
                    },
                },
                {"Event": "SparkListenerTaskEnd", "Task Metrics": {}},
            ]
        )
    )
    result = event_metrics(tmp_path)
    assert result["tasks"] == 2
    assert result["Input Metrics/Bytes Read"] == 42
    assert result["Executor Run Time"] == 10
    assert result["Shuffle Write Metrics/Shuffle Bytes Written"] == 9


def test_corrupt_event_log_does_not_become_a_successful_zero(tmp_path):
    import pytest

    (tmp_path / "events_1_owned").write_text("{bad}")
    with pytest.raises(json.JSONDecodeError):
        event_metrics(tmp_path)
