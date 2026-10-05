import json
import os
import subprocess
import sys
from datetime import UTC, datetime

import pytest
import yaml

from messaging.serialization import serialize_record
from messaging.topics import topic_for
from processing import process_snapshot_pair
from processing.rules import Rule, RuleConfigurationError, evaluate_rules, load_rules
from schemas import Alert
from spark.common.config import SparkConfig, duration_ms


@pytest.mark.parametrize(
    ("old", "new", "rule_id", "severity"),
    [
        ("logging enabled", "logging disabled", "logging-disabled", "high"),
        ("telnet disabled", "telnet enabled", "telnet-enabled", "critical"),
        ("debug disabled", "debug enabled", "debug-enabled", "medium"),
        ("access_log enabled", "access_log disabled", "nginx-access-log-disabled", "high"),
        ("timeout 30", "timeout 60", None, None),
        ("password synthetic-old", "password synthetic-new", None, None),
        (
            "# generated at 10:00\nlogging enabled",
            "# generated at 10:01\nlogging enabled",
            None,
            None,
        ),
    ],
)
def test_default_rules(snapshot_factory, old, new, rule_id, severity) -> None:
    previous = snapshot_factory(old)
    current = snapshot_factory(new, snapshot_id="snap-new", version=2)
    diff = process_snapshot_pair(previous, current).diff
    rules = load_rules("rules/default.yaml")
    alerts = evaluate_rules(diff, current.event_time, rules)
    assert alerts == evaluate_rules(diff, current.event_time, rules)
    if rule_id is None:
        assert alerts == []
    else:
        assert len(alerts) == 1
        alert = alerts[0]
        assert (alert.rule_id, alert.severity.value) == (rule_id, severity)
        assert alert.snapshot_id == current.snapshot_id
        assert alert.asset_id == current.asset_id
        assert alert.created_at == current.event_time
        assert topic_for(alert) == "config.alerts"
        envelope = json.loads(serialize_record(alert))
        assert envelope["record_type"] == "alert"
        assert Alert.model_validate(envelope["payload"]) == alert


def test_multiple_matches_once_per_rule_and_operations(snapshot_factory) -> None:
    diff = process_snapshot_pair(
        snapshot_factory("logging enabled"),
        snapshot_factory(
            "logging disabled\nlogging disabled\ntelnet enabled", snapshot_id="new", version=2
        ),
    ).diff
    rules = load_rules("rules/default.yaml")
    now = datetime(2026, 10, 5, tzinfo=UTC)
    assert len(evaluate_rules(diff, now, rules)) == 2
    for operation in ("removed", "any"):
        rule = Rule(
            id="removed",
            severity="low",
            operation=operation,
            pattern="logging enabled",
            message="Removed",
        )
        assert len(evaluate_rules(diff, now, (rule,))) == 1
    disabled = Rule(
        id="disabled",
        enabled=False,
        severity="low",
        operation="any",
        pattern=".*",
        message="Disabled",
    )
    assert evaluate_rules(diff, now, (disabled,)) == []


@pytest.mark.parametrize(
    "mutation",
    [
        {"severity": "unknown"},
        {"operation": "replace"},
        {"pattern": ""},
        {"pattern": "["},
        {"message": ""},
        {"enabled": "false"},
        {"unexpected": 1},
    ],
)
def test_reject_invalid_rule(tmp_path, mutation) -> None:
    rule = load_rules("rules/default.yaml")[0].model_dump(mode="json")
    path = tmp_path / "rules.yaml"
    path.write_text(yaml.safe_dump({"rules": [{**rule, **mutation}]}))
    with pytest.raises(RuleConfigurationError):
        load_rules(path)


@pytest.mark.parametrize(
    "document", ["[broken", "rules: null", "rules:\n - id: missing", "!!python/object:bad {}"]
)
def test_invalid_yaml(tmp_path, document) -> None:
    path = tmp_path / "rules.yaml"
    path.write_text(document)
    with pytest.raises(RuleConfigurationError):
        load_rules(path)


def test_duplicate_ids_and_missing_file(tmp_path) -> None:
    rule = load_rules("rules/default.yaml")[0].model_dump(mode="json")
    path = tmp_path / "rules.yaml"
    path.write_text(yaml.safe_dump({"rules": [rule, rule]}))
    with pytest.raises(RuleConfigurationError, match="Duplicate"):
        load_rules(path)
    with pytest.raises(RuleConfigurationError):
        load_rules(tmp_path / "missing.yaml")


@pytest.mark.parametrize(
    "value", ["", "0 minutes", "-1 seconds", "1 day", "NaN", "10", "1.5 hours"]
)
def test_invalid_duration(value) -> None:
    with pytest.raises(ValueError):
        duration_ms(value)


def test_state_configuration() -> None:
    config = SparkConfig.from_env(
        {
            "SPARK_WATERMARK_DELAY": "2 seconds",
            "SPARK_STATE_TIMEOUT": "1 hour",
            "SPARK_MAX_VERSIONS_PER_ASSET": "4",
            "CONFIGSTREAM_RULES_FILE": "custom.yaml",
        }
    )
    assert duration_ms(config.watermark_delay) == 2000
    assert duration_ms(config.state_timeout) == 3_600_000
    assert config.max_versions_per_asset == 4
    assert str(config.rules_file) == "custom.yaml"
    with pytest.raises(ValueError):
        SparkConfig(max_versions_per_asset=1)


def test_alerts_deterministic_across_processes() -> None:
    code = (
        "from processing.demo import demo_snapshots; "
        "from processing import process_snapshot_pair; "
        "from processing.rules import load_rules, evaluate_rules; "
        "previous, current = demo_snapshots(); "
        "diff = process_snapshot_pair(previous, current).diff; "
        "print([alert.model_dump_json() for alert in "
        "evaluate_rules(diff, current.event_time, load_rules('rules/default.yaml'))])"
    )
    outputs = [
        subprocess.check_output(
            [sys.executable, "-c", code], env={**os.environ, "PYTHONHASHSEED": seed}
        )
        for seed in ("1", "2")
    ]
    assert outputs[0] == outputs[1]


def test_disabled_yaml_and_pattern_whitespace(tmp_path, snapshot_factory) -> None:
    rule = load_rules("rules/default.yaml")[0].model_dump(mode="json")
    path = tmp_path / "disabled.yaml"
    path.write_text(yaml.safe_dump({"rules": [{**rule, "enabled": False}]}))
    diff = process_snapshot_pair(
        snapshot_factory("logging enabled"),
        snapshot_factory("logging disabled", version=2, snapshot_id="new"),
    ).diff
    assert evaluate_rules(diff, datetime(2026, 10, 5, tzinfo=UTC), load_rules(path)) == []
    assert Rule.model_validate({**rule, "pattern": " logging disabled"}).pattern.startswith(" ")
