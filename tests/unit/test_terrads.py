import hashlib
import io
import json
import sqlite3
import tarfile

import pytest

from workloads.empirical import measure
from workloads.observed import load_dataset
from workloads.terrads import disk_guard, file_hash, inspect_static, write_json
from workloads.terrads_history import extract_repository, license_identity, terraform_rejection
from workloads.terrads_sampling import estimate, normalize_url, sample


@pytest.mark.parametrize(
    "url",
    [
        "https://github.com/OWNER/Repo.git",
        "git@github.com:owner/repo",
        "git://github.com/owner/repo",
    ],
)
def test_origin_normalization(url):
    assert normalize_url(url) == "https://github.com/owner/repo"


@pytest.mark.parametrize(
    "url",
    [
        "https://example.org/a/b",
        "https://user:password@github.com/a/b",
        "https://github.com/a/b?x=1",
        "https://github.com/a/b/extra",
        "https://github.com/a",
        "ftp://github.com/a/b",
        "https://github.com:9999/a/b",
    ],
)
def test_origin_rejection(url):
    with pytest.raises(ValueError):
        normalize_url(url)


def population_fixture():
    return [
        {"id": index, "url": f"https://github.com/fixture/repo-{index}", "source_bytes": index**2}
        for index in range(120)
    ]


def test_seeded_nested_stratified_sample_and_weights():
    population = population_fixture()
    first = sample(population, 12, 42)
    assert first == sample(list(reversed(population)), 12, 42)
    assert first != sample(population, 12, 7)
    assert {entry["id"] for entry in first["selected"]} <= {
        entry["id"] for entry in sample(population, 60, 42)["selected"]
    }
    assert sum(group["sample"] for group in first["strata"].values()) == 12
    assert sum(group["population"] for group in first["strata"].values()) == 120
    assert all(group["inclusion_probability"] == 0.1 for group in first["strata"].values())


@pytest.mark.parametrize("size", [0, 2, 121])
def test_invalid_sample_size(size):
    with pytest.raises(ValueError):
        sample(population_fixture(), size)


def test_population_bootstrap_known_constant_and_denominator():
    manifest = sample(population_fixture(), 12)
    rows = [{"id": item["id"], "revisions": 3, "reachable": 1} for item in manifest["selected"]]
    result = estimate(manifest, rows, repetitions=100)
    assert result == estimate(manifest, rows, repetitions=100)
    assert result["estimates"]["revisions"] == {"point": 360, "ci95": [360, 360]}
    assert result["estimates"]["reachable"]["point"] == 120
    with pytest.raises(ValueError, match="all sampled"):
        estimate(manifest, rows[:-1])


def test_unavailable_sources_are_zero_not_replaced():
    manifest = sample(population_fixture(), 12)
    rows = [{"id": item["id"], "revisions": 0, "reachable": 0} for item in manifest["selected"]]
    assert estimate(manifest, rows)["estimates"]["revisions"]["point"] == 0


def test_adjacent_pair_estimate_is_not_snapshot_estimate():
    manifest = sample(population_fixture(), 12)
    rows = [{"id": item["id"], "revisions": 5, "assets": 2} for item in manifest["selected"]]
    result = estimate(manifest, rows, repetitions=100)
    assert result["estimates"]["adjacent_pairs"] == {"point": 360, "ci95": [360, 360]}
    assert result["estimates"]["revisions"]["point"] == 600


def test_population_uncertainty_rejects_singleton_non_census_strata():
    manifest = sample(population_fixture(), 3)
    rows = [{"id": item["id"], "revisions": 1} for item in manifest["selected"]]
    with pytest.raises(ValueError, match="non-census stratum"):
        estimate(manifest, rows)


def test_census_has_no_sampling_uncertainty():
    manifest = sample(population_fixture(), 120)
    rows = [{"id": item["id"], "revisions": item["id"] + 1} for item in manifest["selected"]]
    assert estimate(manifest, rows, repetitions=100)["estimates"]["revisions"] == {
        "point": 7260,
        "ci95": [7260, 7260],
    }


def test_compact_replay_keeps_metrics_without_source_bodies():
    from workloads.terrads_main import compact_replay

    result = {
        "status": "success",
        "metrics": {"batches": 3},
        "corpus": {
            "sources": [{"content": "SENSITIVE_TEST_MARKER"}],
            "source_checksums": ["a" * 64],
        },
    }
    projected = compact_replay(result)
    assert projected["metrics"] == result["metrics"]
    assert projected["corpus"]["source_count"] == 1
    assert projected["corpus"]["source_checksums"] == ["a" * 64]
    assert "SENSITIVE_TEST_MARKER" not in json.dumps(projected)


def test_terraform_git_incarnations_security_reversion_and_time(terraform_fixture):
    root, _, manifest = terraform_fixture
    snapshots, provenance, _ = load_dataset(root)
    assert manifest["source_commit_count"] == 7
    assert len(snapshots) == 8
    assert len({item.asset_id for item in snapshots}) == 5
    summary = measure(snapshots, provenance)
    assert summary["reverts"]["count"] == 1
    assert summary["diff"]["changed"]["count"] == 3
    assert summary["inter_change_seconds"]["min"] == 0
    assert all(snapshot.event_time.hour == 2 for snapshot in snapshots)
    assert all(snapshot.config_format == "text" for snapshot in snapshots)
    assert "SENSITIVE_TEST_MARKER" not in (
        root / "terrads-1/histories.jsonl"
    ).read_text() + json.dumps(manifest)
    assert (
        len([metadata for metadata in provenance.values() if metadata["path"] == "unsafe.tf"]) == 1
    )


@pytest.mark.parametrize(
    "content",
    [
        b'password = "fake"',
        b'client_secret = "fake"',
        b'private_key = "fake"',
        b'aws_access_key = "fake"',
        b"-----BEGIN PRIVATE KEY-----",
        b"\x00binary",
        b"\xff",
        b"",
    ],
)
def test_security_rejects_without_echo(content):
    reason = terraform_rejection(content)
    assert reason is not None
    assert reason in {"potential_credentials", "binary", "non_utf8", "empty_or_oversized"}


def test_file_bound_and_safe_hcl():
    assert terraform_rejection(b"a" * 10, max_file_bytes=9) == "oversize"
    assert terraform_rejection(b'variable "region" { default = "test-east" }\n') is None


@pytest.mark.parametrize(
    "content,expected",
    [
        (b"Permission is hereby granted, free of charge", "MIT"),
        (b"Apache License\nVersion 2.0", "Apache-2.0"),
        (b"All rights reserved", None),
    ],
)
def test_license_gate(content, expected):
    assert license_identity(content) == expected


def test_streaming_static_measurement_without_extracting_config(tmp_path):
    with tarfile.open(tmp_path / "TerraDS.tar.gz", "w:gz") as bundle:
        for name, content in [
            ("repositories/1/main.tf", b"a\n"),
            ("repositories/2/main.tf", b"a\n"),
            ("repositories/2/other.tf", b"bbb\n"),
            ("repositories/2/ignored.tfvars", b"excluded"),
        ]:
            member = tarfile.TarInfo(name)
            member.size = len(content)
            bundle.addfile(member, io.BytesIO(content))
    with sqlite3.connect(tmp_path / "TerraDS.sqlite") as connection:
        connection.execute("CREATE TABLE Repositories (Id INTEGER)")
        connection.execute("CREATE TABLE Modules (Id INTEGER, RepositoryId INTEGER)")
        connection.execute("CREATE TABLE Resources (Id INTEGER, ModuleId INTEGER)")
        connection.executemany("INSERT INTO Repositories VALUES (?)", [(1,), (2,)])
        connection.execute("INSERT INTO Modules VALUES (1, 2)")
        connection.execute("INSERT INTO Resources VALUES (1, 1)")
    artifact = tmp_path / "TerraDS.tar.gz"
    results = tmp_path / "results"
    write_json(
        results / "source-manifest.json",
        {
            "files": [
                {
                    "name": artifact.name,
                    "bytes": artifact.stat().st_size,
                    "sha256": file_hash(artifact),
                }
            ]
        },
    )
    summary = inspect_static(tmp_path, results)
    assert summary["terraform_files"] == 3
    assert summary["logical_terraform_bytes"] == 8
    assert summary["unique_contents"] == 2
    assert summary["duplicate_contents"] == 1
    assert summary["repository_distributions"]["terraform_files"]["max"] == 2
    assert summary["repository_distributions"]["resources"]["mean"] == 0.5
    assert (results / "static-summary.md").exists()
    assert summary["archive_repositories"] == 2
    assert summary["file_size_bytes"]["p50"] == 2
    assert not list(tmp_path.glob("**/*.tf"))
    assert file_hash(artifact) == hashlib.sha256(artifact.read_bytes()).hexdigest()


def test_disk_guard_rejects_unsafe_allocation(tmp_path):
    with pytest.raises(ValueError, match="reserve"):
        disk_guard(tmp_path, required=10**20)


def test_nested_repository_archives_are_measured(tmp_path):
    inner = io.BytesIO()
    with tarfile.open(fileobj=inner, mode="w:gz") as bundle:
        content = b'resource "null_resource" "test" {}\n'
        member = tarfile.TarInfo("module/main.tf")
        member.size = len(content)
        bundle.addfile(member, io.BytesIO(content))
    with tarfile.open(tmp_path / "TerraDS.tar.gz", "w:gz") as bundle:
        member = tarfile.TarInfo("1234.tar.gz")
        member.size = len(inner.getvalue())
        bundle.addfile(member, io.BytesIO(inner.getvalue()))
    with sqlite3.connect(tmp_path / "TerraDS.sqlite") as connection:
        connection.execute("CREATE TABLE Repositories (Id INTEGER)")
        connection.execute("CREATE TABLE Modules (Id INTEGER, RepositoryId INTEGER)")
        connection.execute("CREATE TABLE Resources (Id INTEGER, ModuleId INTEGER)")
        connection.execute("INSERT INTO Repositories VALUES (1234)")
    archive = tmp_path / "TerraDS.tar.gz"
    results = tmp_path / "results"
    write_json(
        results / "source-manifest.json",
        {
            "files": [
                {
                    "name": archive.name,
                    "bytes": archive.stat().st_size,
                    "sha256": file_hash(archive),
                }
            ]
        },
    )
    summary = inspect_static(tmp_path, results)
    assert summary["terraform_files"] == 1
    assert summary["logical_terraform_bytes"] == len(content)
    assert json.loads((tmp_path / "repository-source-bytes.json").read_text()) == {
        "1234": len(content)
    }


def test_resume_identity_and_incomplete_retry(tmp_path):
    entry = {
        "id": 1,
        "url": "https://github.com/fixture/repo",
        "end_sha": "invalid",
        "stratum": "size-0",
    }
    first = extract_repository(entry, tmp_path, 42)
    assert first["status"] == "missing_pinned_commit"
    assert extract_repository(entry, tmp_path, 42) == first
    with pytest.raises(ValueError, match="Resume identity"):
        extract_repository(entry, tmp_path, 7)
    first["status"] = "incomplete"
    write_json(tmp_path / "statuses/1.json", first)
    assert extract_repository(entry, tmp_path, 42)["status"] == "missing_pinned_commit"


def test_ground_truth_edges_are_safe_deterministic_and_adjacent(terraform_fixture, tmp_path):
    from workloads.terrads_evidence import save_edges

    snapshots, _, _ = load_dataset(terraform_fixture[0])
    first = save_edges(snapshots, tmp_path)
    assert first == save_edges(list(reversed(snapshots)), tmp_path)
    assert first["assets"] == 5
    assert first["edges"] == 3
    text = (tmp_path / "ground-truth-edges.csv").read_text()
    assert "SENSITIVE_TEST_MARKER" not in text
    assert "resource" not in text


def test_ground_truth_rejects_missing_version(terraform_fixture, tmp_path):
    from workloads.terrads_evidence import save_edges

    snapshots, _, _ = load_dataset(terraform_fixture[0])
    with pytest.raises(ValueError, match="contiguous"):
        save_edges([item for item in snapshots if item.version != 1], tmp_path)


def test_offline_unchanged_audit_only_counts_relevant_commits(terraform_fixture):
    from workloads.terrads_evidence import unchanged_observations

    root, source, _ = terraform_fixture
    repository = root.parents[1] / "terraform" / ".git"
    result = unchanged_observations(repository, source.start_revision, source.end_revision)
    assert result == {
        "selected_tree_observations": 6,
        "unchanged_path_blob_observations": 5,
    }


def test_funnel_preserves_failed_denominator_and_units(tmp_path):
    from workloads.terrads_evidence import save_funnel

    manifest = {"strata": {"a": {"population": 20}}}
    rows = [
        {"reachable": 1, "license_accepted": 1, "accepted": 1, "assets": 4, "revisions": 7},
        {"reachable": 0, "license_accepted": 0, "accepted": 0, "assets": 0, "revisions": 0},
    ]
    values = save_funnel(manifest, rows, tmp_path)
    assert [row["count"] for row in values] == [20, 2, 1, 1, 1, 4, 7]
    assert "units change" in (tmp_path / "plots/funnel.svg").read_text()


def test_comparison_plot_keeps_all_source_legends_on_canvas(tmp_path):
    from xml.etree import ElementTree

    from workloads.empirical import distribution, write_distribution_plot

    plot = tmp_path / "comparison.svg"
    write_distribution_plot(
        [(f"source-{index}", distribution([1, 2])) for index in range(5)], plot, "bytes"
    )
    document = ElementTree.parse(plot).getroot()
    legends = [item for item in document if item.text and item.text.startswith("source-")]
    assert len(legends) == 5
    assert len({item.attrib["fill"] for item in legends}) == 5
    assert all(float(item.attrib["x"]) < float(document.attrib["width"]) for item in legends)
    assert all(float(item.attrib["y"]) < float(document.attrib["height"]) for item in legends)


@pytest.mark.parametrize("corrupt", [False, True])
def test_primary_reverification_preserves_download_evidence(tmp_path, monkeypatch, corrupt):
    from workloads import terrads

    artifact = tmp_path / "TerraDS.sqlite"
    artifact.write_bytes(b"owned fixture database")
    metadata = {
        "doi": "10.5281/zenodo.20339474",
        "publication_date": "2026-05-26",
        "license": {"id": "cc-by-4.0"},
    }
    entry = {
        "key": artifact.name,
        "size": artifact.stat().st_size,
        "checksum": "md5:" + file_hash(artifact, "md5"),
    }
    results = tmp_path / "results"
    write_json(
        results / "source-manifest.json",
        {
            "doi": metadata["doi"],
            "publication_date": metadata["publication_date"],
            "artifact_license": "cc-by-4.0",
            "files": [
                {
                    "name": artifact.name,
                    "bytes": entry["size"],
                    "published_checksum": entry["checksum"],
                    "sha256": file_hash(artifact),
                }
            ],
            "network_bytes": 999,
        },
    )
    original = (results / "source-manifest.json").read_bytes()
    monkeypatch.setattr(
        terrads.urllib.request,
        "urlopen",
        lambda *args, **kwargs: io.BytesIO(
            json.dumps({"metadata": metadata, "files": [entry]}).encode()
        ),
    )
    if corrupt:
        artifact.write_bytes(b"corrupted cache")
        with pytest.raises(ValueError, match="Cached artifact"):
            terrads.verify_cached(tmp_path, results)
    else:
        assert terrads.verify_cached(tmp_path, results)["download_metrics_preserved"]
    assert (results / "source-manifest.json").read_bytes() == original


def test_unclear_repository_license_blocks_history_without_network(tmp_path, monkeypatch):
    from workloads import terrads_history

    identity = "a" * 40
    entry = {
        "id": 1,
        "url": "https://github.com/fixture/repo",
        "end_sha": identity,
        "stratum": "size-0",
    }

    def fake_git(repository, *arguments):
        if arguments[:2] == ("remote", "get-url"):
            return (entry["url"] + ".git\n").encode()
        if arguments[0] in {"rev-parse", "rev-list"}:
            return (identity + "\n").encode()
        if arguments[0] == "ls-tree":
            return f"100644 blob {identity}\tLICENSE\0".encode()
        if arguments[0] == "show":
            return b"All rights reserved"
        return b""

    def forbidden_extraction(*args, **kwargs):
        raise AssertionError("License rejection must precede configuration extraction")

    monkeypatch.setattr(terrads_history, "git", fake_git)
    monkeypatch.setattr(terrads_history, "extract", forbidden_extraction)
    row = extract_repository(entry, tmp_path, 42)
    assert row["status"] == "license_unknown"
    assert row["reachable"] == 1
    assert row["license_accepted"] == 0
    assert row["revisions"] == 0
    assert json.loads((tmp_path / "statuses/1.json").read_text()) == row
