import json
from dataclasses import replace

import pytest

from lake.config import LakeConfig
from processing.rules import load_rules
from search.config import SearchConfig
from spark.common.config import SparkConfig
from spark.streaming.identity import checkpoint_contract


@pytest.fixture
def bound(tmp_path):
    config = SparkConfig(
        checkpoint_dir=tmp_path / "checkpoint",
        lake=LakeConfig("owned-user", "owned-secret"),
        search=SearchConfig(username="owned-search-user", password="owned-search-secret"),
    )
    rules = load_rules(config.rules_file)
    checkpoint_contract(config, 2, rules)
    return config, rules


@pytest.mark.parametrize(
    "field,value",
    [
        ("severity", "critical"),
        ("message", "Changed alert message"),
        ("pattern", "changed-pattern"),
        ("operation", "removed"),
        ("enabled", False),
        ("id", "changed-rule-id"),
    ],
)
def test_policy_changes_cannot_resume_checkpoint(bound, field, value):
    config, rules = bound
    assert getattr(rules[0], field) != value
    changed_rule = type(rules[0]).model_validate({**rules[0].model_dump(), field: value})
    changed = (changed_rule, *rules[1:])
    with pytest.raises(RuntimeError, match="Incompatible"):
        checkpoint_contract(config, 2, changed)


@pytest.mark.parametrize(
    "change",
    [
        {"bootstrap_servers": "other-cluster:9092"},
        {"lake": None, "search": None},
        {"lake": LakeConfig("user", "password", endpoint="http://other-lake:9000")},
        {"lake": LakeConfig("user", "password", bucket="other-bucket")},
        {"lake": LakeConfig("user", "password", prefix="other-prefix")},
        {"search": None},
        {"search": SearchConfig(url="http://other-search:9200")},
        {"search": SearchConfig(index_prefix="other-index")},
    ],
)
def test_source_or_destination_changes_cannot_resume_checkpoint(bound, change):
    config, rules = bound
    with pytest.raises(RuntimeError, match="Incompatible"):
        checkpoint_contract(replace(config, **change), 2, rules)


def test_enabling_sink_requires_new_checkpoint(tmp_path):
    config = SparkConfig(checkpoint_dir=tmp_path)
    checkpoint_contract(config, 2, ())
    with pytest.raises(RuntimeError, match="Incompatible"):
        checkpoint_contract(replace(config, lake=LakeConfig("user", "password")), 2, ())


def test_delivery_tuning_credentials_and_rule_order_allow_resume(bound):
    config, rules = bound
    marker = config.checkpoint_dir / "configstream-state-contract.json"
    original = marker.read_bytes()
    changed = replace(
        config,
        lake=replace(
            config.lake,
            access_key="rotated-user",
            secret_key="rotated-secret",
            max_records_per_file=5,
        ),
        search=replace(
            config.search,
            username="rotated-user",
            password="rotated-secret",
            bulk_size=5,
            timeout_seconds=60,
        ),
        trigger_seconds=5,
        max_offsets_per_trigger=200,
        starting_offsets="earliest",
    )
    checkpoint_contract(changed, 2, tuple(reversed(rules)))
    assert marker.read_bytes() == original
    for private in (
        "owned-user",
        "owned-secret",
        "owned-search-user",
        "owned-search-secret",
        rules[0].pattern,
        rules[0].message,
    ):
        assert private not in original.decode()


def test_v8_marker_is_rejected_and_preserved(bound):
    config, rules = bound
    marker = config.checkpoint_dir / "configstream-state-contract.json"
    contract = json.loads(marker.read_text())
    for field in ("contract_version", "rules_sha256", "kafka", "lake", "search"):
        del contract[field]
    marker.write_text(json.dumps(contract))
    original = marker.read_bytes()
    with pytest.raises(RuntimeError, match="Incompatible"):
        checkpoint_contract(config, 2, rules)
    assert marker.read_bytes() == original


@pytest.mark.parametrize("marker", ["configstream-state-contract.json", "state"])
def test_parse_only_cannot_advance_stateful_checkpoint(tmp_path, marker):
    from spark.streaming.main import start_console

    config = SparkConfig(checkpoint_dir=tmp_path)
    (tmp_path / marker).mkdir()
    with pytest.raises(ValueError, match="isolated console checkpoint"):
        start_console(None, config)
