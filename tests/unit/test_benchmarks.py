import json

import pytest

from benchmarks.main import BenchmarkConfig
from benchmarks.metrics import consumed_offsets, lag, logical_config_bytes, summarize_progress
from benchmarks.report import build_report, resource_summary


@pytest.mark.parametrize(
    "options",
    [
        {"benchmark_id": "../../bad"},
        {"rate": 0},
        {"rate": float("nan")},
        {"assets": 0},
        {"max_seconds": 0},
        {"duplicate_ratio": 2},
        {"master": "remote"},
        {"watermark": "bad"},
        {"max_reordering_distance": 0},
        {"max_versions_per_asset": 1},
        {"state_timeout": "bad"},
    ],
)
def test_benchmark_config_validation(options):
    with pytest.raises(ValueError):
        BenchmarkConfig(**{"benchmark_id": "fixture", **options})


def test_progress_metrics_and_branch_count_semantics():
    progress = [
        {"batchId": 0, "numInputRows": 0},
        {
            "batchId": 1,
            "numInputRows": 30,
            "durationMs": {"triggerExecution": 2000},
            "processedRowsPerSecond": 15,
            "inputRowsPerSecond": 10,
            "stateOperators": [
                {"numRowsTotal": 8, "memoryUsedBytes": 1000, "numRowsDroppedByWatermark": 2}
            ],
        },
    ]
    summary = summarize_progress(progress)
    assert summary["batches"] == 1
    assert summary["batch_p95_ms"] == 2000
    assert summary["max_state_rows"] == 8
    assert summary["rows_dropped_by_watermark"] == 2
    assert json.loads(json.dumps({"status": "failed", "metrics": summary}))["status"] == "failed"


def test_source_offsets_and_lag():
    progress = {
        "sources": [
            {"endOffset": {"config.snapshots": {"0": 10, "1": 5}}},
            {"endOffset": json.dumps({"config.snapshots": {"0": 10}})},
            {"endOffset": "None"},
        ]
    }
    offsets = consumed_offsets(progress)
    assert offsets == {"0": 10, "1": 5}
    assert lag({"0": 12, "1": 5}, offsets) == 2


def test_empty_failed_run_metrics():
    assert summarize_progress([])["processed_rows_per_second_mean"] is None


def test_logical_volume_counts_original_utf8_bytes_not_characters():
    assert logical_config_bytes("é\n") == 3
    assert logical_config_bytes("") == 0


def test_replay_settings_are_explicit_and_validated():
    config = BenchmarkConfig(benchmark_id="fixture", max_reordering_distance=150)
    assert config.max_reordering_distance == 150
    assert config.start_time.utcoffset().total_seconds() == 0
    assert config.max_versions_per_asset == 32


def test_resources_are_actual_samples_not_inferred_total_memory():
    assert resource_summary([{"spark_process": "1024 150.0"}])["java_max_rss_bytes"] == 1048576
    assert resource_summary([])["java_max_rss_bytes"] is None


def test_container_resource_units_and_safe_projection():
    stats = json.dumps({"Name": "kafka", "MemUsage": "2MiB / 1GiB", "CPUPerc": "12.5%"})
    summary = resource_summary([{"containers": stats}, {"containers": "unavailable"}])
    assert summary["container_peaks"] == {
        "kafka": {"max_memory_bytes": 2097152, "max_cpu_percent": 12.5}
    }


def test_report_retains_failed_experiments_and_excludes_raw_samples(tmp_path):
    source = tmp_path / "input/run"
    source.mkdir(parents=True)
    result = {
        "benchmark_id": "fixture",
        "status": "failed",
        "failure": "TimeoutError",
        "config": {"assets": 2, "rate": 10},
        "resource_samples": [{"note": "fixture-only"}],
        "progress": [],
        "lag_samples": [{"lag": 3}],
    }
    (source / "result.json").write_text(json.dumps(result))
    destination = tmp_path / "report"
    rows = build_report(source.parent, destination)
    assert rows[0]["status"] == "failed"
    assert rows[0]["peak_lag"] == 3
    safe = json.loads((destination / "fixture.json").read_text())
    assert "resource_samples" not in safe
    assert "fixture-only" not in json.dumps(safe)
    reproduced = tmp_path / "reproduced"
    assert build_report(destination, reproduced) == rows
    assert (reproduced / "summary.csv").read_bytes() == (destination / "summary.csv").read_bytes()
    assert b"\r" not in (destination / "summary.csv").read_bytes()


def test_nested_and_flat_results_have_stable_order_for_prefix_ids(tmp_path):
    source = tmp_path / "source"
    for identifier in ["run", "run-wide"]:
        directory = source / identifier
        directory.mkdir(parents=True)
        (directory / "result.json").write_text(
            json.dumps(
                {
                    "benchmark_id": identifier,
                    "status": "failed",
                    "config": {"assets": 1, "rate": 100},
                }
            )
        )
    destination = tmp_path / "flat"
    original = build_report(source, destination)
    assert [row["run"] for row in original] == ["run", "run-wide"]
    assert build_report(destination, tmp_path / "again") == original
