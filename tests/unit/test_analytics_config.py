from datetime import UTC, datetime

import pytest

from analytics.config import AnalyticsConfig, run_identity


def test_identity_is_input_and_parameter_sensitive():
    entries = [{"query_id": "a", "batch_id": 1}, {"query_id": "a", "batch_id": 2}]
    config = AnalyticsConfig()
    assert run_identity(entries, config) == run_identity(list(reversed(entries)), config)
    assert run_identity(entries, config) != run_identity(entries[:1], config)
    assert run_identity(entries, config) != run_identity(entries, AnalyticsConfig(window="1 hour"))


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
