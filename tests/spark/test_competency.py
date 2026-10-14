import pytest

from spark.common.config import SparkConfig
from spark.common.runtime import create_session
from spark.competency.examples import run_examples, stream_modes, verify


@pytest.fixture(scope="module")
def examples(tmp_path_factory):
    root = tmp_path_factory.mktemp("owned-competency")
    session = create_session(
        SparkConfig(master="local[2]", driver_memory="2g", runtime_dir=root / "runtime"),
        kafka=False,
    )
    try:
        result = run_examples(session, root / "examples")
        modes = stream_modes(session, root / "modes")
        verify(result, modes)
        yield result, modes
    finally:
        session.stop()


def test_pivot_unpivot_windows_keep_native_multiplicity(examples):
    result, _ = examples
    assert result["aggregation"]["native_occurrences"] == 8
    assert result["aggregation"]["fingerprints"] == 5
    assert result["aggregation"]["cumulative"] == [4, 8]
    assert result["aggregation"]["statistics"] == {"mean": 4.0, "stddev": 0.0}


def test_actual_udf_and_custom_aggregate_physical_plans(examples):
    result, _ = examples
    assert "BatchEvalPython" in result["plans"]["python_udf"]
    assert "BatchEvalPython" not in result["plans"]["native_expression"]
    assert "AggregateInPandas" in result["plans"]["custom_aggregate"]
    assert {row["worker"]: row["fraction"] for row in result["udf"]["custom_aggregate"]} == {
        "worker-a": 0.5,
        "worker-b": 0.0,
    }


def test_executed_join_strategies_and_exact_multi_join_output(examples):
    result, _ = examples
    assert result["plans"]["merge_join"].count("SortMergeJoin") >= 2
    assert result["plans"]["broadcast_join"].count("BroadcastHashJoin") >= 2
    assert result["joins"]["merge"] == result["joins"]["broadcast"]
    assert [row["id"] for row in result["joins"]["filtered_candidate"]] == [
        "owned-0",
        "owned-1",
        "owned-2",
        "owned-4",
    ]
    assert len(result["join_trials"]["trials"]) == 6


def test_partition_pruning_has_filter_and_executed_file_metrics(examples):
    result, _ = examples
    assert "PartitionFilters" in result["plans"]["partition_pruning"]
    assert sum(scan["metrics"]["numFiles"] for scan in result["pruning"]["all_scan_metrics"]) == 2
    assert (
        sum(scan["metrics"]["numFiles"] for scan in result["pruning"]["selected_scan_metrics"]) == 1
    )
    assert result["pruning"]["selected_ids"] == ["owned-0", "owned-1", "owned-2", "owned-3"]


def test_materialized_persistence_is_released(examples):
    result, _ = examples
    assert result["persistence"]["materialized_rows"] == 8
    assert result["persistence"]["cached"] and result["persistence"]["released"]
    assert "InMemoryTableScan" in result["plans"]["cached_scan"]


def test_catalog_bucketing_is_real_not_repartitioning(examples):
    result, _ = examples
    assert "Bucketed: true" in result["plans"]["bucketed_join"]
    assert len(result["bucketing"]["rows"]) == 8
    assert result["bucketing"]["buckets"] == 2
    assert result["bucketing"]["production_used"] is False


def test_actual_update_and_complete_stream_modes(examples):
    _, modes = examples
    assert modes["update"]["batches"][-1]["rows"] == [{"action": "announcement", "count": 5}]
    assert modes["complete"]["batches"][-1]["rows"] == [
        {"action": "announcement", "count": 5},
        {"action": "withdrawal", "count": 4},
    ]
    assert sum(modes["complete"]["progress_input_rows"]) == 9
