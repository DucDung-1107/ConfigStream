import json
from datetime import UTC, datetime
from hashlib import sha256

import pytest

from analytics.config import AnalyticsConfig, run_identity
from spark.common.config import SparkConfig


def test_identity_is_input_and_parameter_sensitive():
    entries = [{"query_id": "a", "batch_id": 1}, {"query_id": "a", "batch_id": 2}]
    config = AnalyticsConfig()
    assert run_identity(entries, config) == run_identity(list(reversed(entries)), config)
    assert run_identity(entries, config) != run_identity(entries[:1], config)
    assert run_identity(entries, config) != run_identity(entries, AnalyticsConfig(window="1 hour"))
    assert run_identity(entries, AnalyticsConfig(z_threshold=3)) == run_identity(
        entries, AnalyticsConfig(z_threshold=3.0)
    )


def test_reconciliation_version_cannot_reuse_legacy_unchecked_result():
    entries = [{"query_id": "a", "batch_id": 1}]
    config = AnalyticsConfig()
    legacy = json.dumps(
        {"job_version": "batch-v1", "inputs": entries, "parameters": config.parameters()},
        sort_keys=True,
        separators=(",", ":"),
    )
    assert run_identity(entries, config) != sha256(legacy.encode()).hexdigest()


@pytest.mark.parametrize(
    "options",
    [
        {"window": "5 minutes"},
        {"minimum_count": 0},
        {"z_threshold": float("nan")},
        {"baseline_windows": 1},
        {"start": datetime(2026, 1, 1)},
        {"start": datetime(2026, 1, 2, tzinfo=UTC), "end": datetime(2026, 1, 1, tzinfo=UTC)},
    ],
)
def test_invalid_analytics_config(options):
    with pytest.raises(ValueError):
        AnalyticsConfig(**options)


@pytest.mark.parametrize("value", ["0g", "32g", "8x", "1m", "-1g"])
def test_driver_memory_bounds(value):
    with pytest.raises(ValueError):
        SparkConfig(driver_memory=value)


def test_driver_memory_environment():
    assert SparkConfig.from_env({"SPARK_DRIVER_MEMORY": "2g"}).driver_memory == "2g"
