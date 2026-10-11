import io
import tarfile
from dataclasses import replace

import pytest

from processing.diff import process_snapshot_pair
from workloads.corpus import Source, checksum, import_archive, load_baselines, skip_reason
from workloads.history import History, WorkloadConfig, mutate


@pytest.fixture
def corpus(tmp_path):
    archive = tmp_path / "source.tar.gz"
    entries = {
        "root/LICENSE": b"Apache License Version 2.0",
        "root/lab/configs/router.cfg": (
            b"hostname test\ninterface GigabitEthernet0/0\n"
            b" ip address 192.0.2.1 255.255.255.0\nend\n"
        ),
        "root/lab/configs/private.cfg": b"hostname private\npassword SENSITIVE_TEST_MARKER\n",
        "root/lab/configs/unsupported.txt": b"unsupported configuration\n",
    }
    with tarfile.open(archive, "w:gz") as bundle:
        for name, content in entries.items():
            member = tarfile.TarInfo(name)
            member.size = len(content)
            bundle.addfile(member, io.BytesIO(content))
    source = Source(
        name="fixture",
        repository="batfish/batfish",
        revision="a" * 40,
        license="Apache-2.0",
        archive_sha256=checksum(archive.read_bytes()),
    )
    destination = tmp_path / "corpus"
    destination.mkdir()
    manifest = import_archive(archive, source, destination)
    return destination, manifest


def test_measured_import_and_checksum(corpus):
    directory, manifest = corpus
    assert manifest["raw_files"] == 3
    assert manifest["usable_files"] == 1
    assert manifest["skipped_files"] == 2
    assert manifest["skip_reasons"] == {"potential_credentials": 1, "unsupported_syntax": 1}
    assert manifest["usable_bytes"] == manifest["files"][0]["bytes"]
    assert load_baselines(directory) == manifest["files"]
    assert not (directory / "files/lab/configs/private.cfg").exists()


def test_checksum_rejects_modified_baseline(corpus):
    directory, _ = corpus
    (directory / "files/lab/configs/router.cfg").write_text("changed")
    with pytest.raises(ValueError):
        load_baselines(directory)


@pytest.mark.parametrize(
    "profile", ["normal-maintenance", "fleet-rollout", "incident-remediation", "stress-replay"]
)
def test_seeded_history_versions_and_ground_truth(corpus, profile):
    directory, _ = corpus
    config = WorkloadConfig(assets=3, versions_per_asset=6, corpus=directory, profile=profile)
    first = list(History(config).records())
    second = list(History(config).records())
    assert first == second
    assert first != list(History(replace(config, seed=43)).records())
    assert len(first) == 18
    by_asset = {}
    for event, snapshot, truth in first:
        assert event.asset_id == snapshot.asset_id == truth["asset_id"]
        assert truth["baseline_sha256"]
        assert snapshot.hash == checksum(snapshot.content.encode())
        if snapshot.asset_id in by_asset:
            previous = by_asset[snapshot.asset_id]
            result = process_snapshot_pair(previous, snapshot)
            assert result.diff.new_snapshot_id == snapshot.snapshot_id
            assert result.diff.added + result.diff.removed > 0
        by_asset[snapshot.asset_id] = snapshot
    if profile == "fleet-rollout":
        assert len({truth["rollout_id"] for _, _, truth in first if truth["rollout_id"]}) == 1


def test_duplicate_and_reordering_preserve_identity(corpus):
    config = WorkloadConfig(
        assets=2,
        versions_per_asset=4,
        corpus=corpus[0],
        duplicate_ratio=1,
        out_of_order_ratio=1,
        max_reordering_distance=4,
    )
    records = list(History(config).records())
    assert len(records) == 16
    assert len({snapshot.snapshot_id for _, snapshot, _ in records}) == 8
    assert records == list(History(config).records())


@pytest.mark.parametrize(
    "syntax,content",
    [
        ("nginx", "worker_processes 2;\nkeepalive_timeout 65;\n"),
        ("generic_service", "timeout 30\nreplicas 2\n"),
    ],
)
def test_non_network_mutations(syntax, content):
    updated, _, _ = mutate(content, syntax, 2)
    assert updated != content


@pytest.mark.parametrize(
    "content",
    [b"token secret", b"SNMP-server community public", b"password x", b"api_key y", b"\xff"],
)
def test_security_filter(content):
    assert skip_reason(content) is not None


def test_manifest_validation():
    with pytest.raises(ValueError):
        Source(
            name="invalid",
            repository="test",
            revision="latest",
            license="unknown",
            archive_sha256="bad",
        )


@pytest.mark.parametrize(
    "options",
    [{"assets": 0}, {"duplicate_ratio": 2}, {"profile": "bad"}, {"max_reordering_distance": 0}],
)
def test_invalid_config(options):
    with pytest.raises(ValueError):
        WorkloadConfig(**options)
