import json
from collections import Counter
from pathlib import Path

import pytest

from workloads.empirical import distribution, measure, percentile, write_report
from workloads.observed import GitSource, ObservedHistory, extract, load_dataset, rejection


@pytest.mark.parametrize(
    "options",
    [
        {"repository": "random/example"},
        {"scope": "../conf"},
        {"allowed_patterns": ["other/*.conf"]},
        {"start_revision": "main"},
        {"license_sha256": "bad"},
        {"shallow_since": "2024-01-01"},
    ],
)
def test_source_validation(options):
    base = json.loads(Path("workloads/manifests/real-history/nginx.json").read_text())
    with pytest.raises(ValueError):
        GitSource.model_validate({**base, **options})


def test_git_history_unchanged_security_and_rename(observed_fixture):
    root, _, manifest = observed_fixture
    snapshots, provenance, _ = load_dataset(root)
    assert manifest["source_commit_count"] == 5
    assert manifest["selected_file_count"] == 3
    assert manifest["extracted_revision_count"] == 7
    assert manifest["excluded_file_count"] == 1
    assert "unsafe.conf" not in (root / "fixture/provenance.jsonl").read_text()
    assert (
        "SENSITIVE_TEST_MARKER"
        not in json.dumps(manifest) + (root / "fixture/histories.jsonl").read_text()
    )
    assert {item.event_time.hour for item in snapshots} == {3}
    assert sum(item["boundary_baseline"] for item in provenance.values()) == 2
    assert len({item["path"] for item in provenance.values()}) == 3


def test_extraction_reproducible_provenance(observed_fixture, tmp_path):
    root, source, _ = observed_fixture
    extract(source, tmp_path / "repository/.git", tmp_path / "repeat")
    for filename in ["histories.jsonl", "provenance.jsonl"]:
        assert (root / "fixture" / filename).read_bytes() == (
            tmp_path / "repeat" / filename
        ).read_bytes()
    _, provenance, _ = load_dataset(root)
    assert all(
        len(item["git_commit"]) == 40 and len(item["content_sha256"]) == 64
        for item in provenance.values()
    )


def test_manifest_checksum_failure(observed_fixture):
    root, _, _ = observed_fixture
    file = root / "fixture/provenance.jsonl"
    file.write_text(file.read_text().replace('"version": 1', '"version": 9', 1))
    with pytest.raises(ValueError, match="checksum"):
        load_dataset(root)


def test_measured_reverts_intervals_and_bundles(observed_fixture):
    snapshots, provenance, _ = load_dataset(observed_fixture[0])
    summary = measure(snapshots, provenance)
    assert summary["assets"] == 3
    assert summary["revisions"] == 7
    assert summary["diff"]["changed"]["count"] == 4
    assert summary["reverts"]["count"] == 1
    assert summary["inter_change_seconds"]["histogram"] == [[86400.0, 2]]
    assert summary["commit_bundles"]["files"]["max"] == 2
    assert summary["correlated_files"]["3600"]["max"] == 2
    assert summary["changes_per_period"]["day"]["min"] == 0


@pytest.mark.parametrize(
    "values,probability,expected",
    [([1, 3], 0.5, 2), ([1], 0.95, 1), ([], 0.5, None), ([1, 2, 3, 4, 5], 0.25, 2)],
)
def test_empirical_percentiles(values, probability, expected):
    assert percentile(values, probability) == expected


def test_summary_distribution_and_reproducible_plots(observed_fixture, tmp_path):
    snapshots, provenance, _ = load_dataset(observed_fixture[0])
    summaries = {"fixture": measure(snapshots, provenance)}
    write_report(summaries, tmp_path / "first")
    write_report(summaries, tmp_path / "second")
    assert len(list((tmp_path / "first").glob("*.svg"))) == 5
    for file in (tmp_path / "first").iterdir():
        assert file.read_bytes() == (tmp_path / "second" / file.name).read_bytes()
    assert distribution([1, 1, 3])["histogram"] == [[1, 2], [3, 1]]


@pytest.mark.parametrize(
    "content",
    [
        b"password SENSITIVE_TEST_MARKER",
        b"Authorization: Bearer fake-credential",
        b"-----BEGIN PRIVATE KEY-----",
        b"aws_access_key=FAKE",
        b"api_key=fake",
        b"snmp-server community fake",
        b"x=" + b"a" * 80,
        b"\xff",
    ],
)
def test_observed_security_filter(content):
    assert rejection(content) is not None


def test_observed_not_a_syntax_specific_mutator():
    assert rejection(b"router bgp 65000\n neighbor 192.0.2.1 remote-as 65001\n") is None


def test_calibration_preserves_history_and_is_seeded(observed_fixture):
    root = observed_fixture[0]
    first = ObservedHistory(root, multiplier=10, calibrated=True, seed=42)
    repeated = ObservedHistory(root, multiplier=10, calibrated=True, seed=42)
    different = ObservedHistory(root, multiplier=10, calibrated=True, seed=7)
    records = [snapshot.model_dump_json() for _, snapshot, _ in first.records()]
    assert records == [snapshot.model_dump_json() for _, snapshot, _ in repeated.records()]
    assert records != [snapshot.model_dump_json() for _, snapshot, _ in different.records()]
    assert len(first.assets) == 30
    assert first.expected_diffs == first.expected_snapshots - 30
    assert {
        snapshot.hash
        for _, snapshot, _ in ObservedHistory(root, calibrated=True, multiplier=10).records()
    } <= {snapshot.hash for snapshot in load_dataset(root)[0]}
    with pytest.raises(ValueError, match="single-pass"):
        list(first.records())


@pytest.mark.parametrize("multiplier,calibrated", [(0, True), (1001, True), (2, False)])
def test_scale_semantics(observed_fixture, multiplier, calibrated):
    with pytest.raises(ValueError):
        ObservedHistory(observed_fixture[0], multiplier=multiplier, calibrated=calibrated)


def test_weighted_calibration_equals_materialized_small_history(observed_fixture):
    root = observed_fixture[0]
    originals, provenance, _ = load_dataset(root)
    history = ObservedHistory(root, calibrated=True, multiplier=3)
    weights = Counter(items[0].asset_id for _, items in history.groups)
    weighted = measure(originals, provenance, "calibration", weights=weights)
    copies = []
    copied_provenance = {}
    for _, snapshot, truth in history.records():
        copies.append(snapshot)
        copied_provenance[snapshot.snapshot_id] = {
            **provenance[truth["source_snapshot_id"]],
            "asset_id": snapshot.asset_id,
        }
    assert weighted == measure(copies, copied_provenance, "calibration")


@pytest.mark.parametrize("field,value", [("version", 0), ("event_time", "not-a-time")])
def test_invalid_snapshot_errors_do_not_echo_raw_content(observed_fixture, field, value):
    root = observed_fixture[0]
    file = root / "fixture/histories.jsonl"
    lines = file.read_text().splitlines()
    item = json.loads(lines[0])
    item[field] = value
    item["content"] = "password SENSITIVE_TEST_MARKER"
    lines[0] = json.dumps(item)
    file.write_text("\n".join(lines) + "\n")
    with pytest.raises(ValueError, match="schema validation") as raised:
        load_dataset(root)
    assert "SENSITIVE_TEST_MARKER" not in str(raised.value)


def test_source_license_hash_is_required(observed_fixture, tmp_path):
    _, source, _ = observed_fixture
    with pytest.raises(ValueError, match="license"):
        extract(
            source.model_copy(update={"license_sha256": "0" * 64}),
            tmp_path / "repository/.git",
            tmp_path / "invalid",
        )


def test_calibration_cannot_have_negative_history_weights(observed_fixture):
    snapshots, provenance, _ = load_dataset(observed_fixture[0])
    with pytest.raises(ValueError, match="multiplicities"):
        measure(snapshots, provenance, weights={snapshots[0].asset_id: -1})
