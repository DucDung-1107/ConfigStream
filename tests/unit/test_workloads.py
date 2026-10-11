import io
import json
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


@pytest.mark.parametrize("field,value", [("bytes", 1), ("syntax", "nginx")])
def test_reject_inaccurate_baseline_metadata(corpus, field, value):
    directory, manifest = corpus
    manifest["files"][0][field] = value
    (directory / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="Baseline validation failed"):
        load_baselines(directory)


def test_seed_changes_baseline_choices(corpus):
    directory, manifest = corpus
    content = (directory / "files/lab/configs/router.cfg").read_bytes().replace(b"test", b"other")
    target = directory / "files/lab/configs/other.cfg"
    target.write_bytes(content)
    manifest["files"].append(
        {
            "path": "lab/configs/other.cfg",
            "bytes": len(content),
            "sha256": checksum(content),
            "syntax": "cisco_ios",
        }
    )
    (directory / "manifest.json").write_text(json.dumps(manifest))
    first = History(WorkloadConfig(assets=20, corpus=directory, seed=42))
    second = History(WorkloadConfig(assets=20, corpus=directory, seed=43))
    assert [state["baseline"]["sha256"] for state in first.states] != [
        state["baseline"]["sha256"] for state in second.states
    ]


def test_history_is_single_pass(corpus):
    history = History(WorkloadConfig(assets=1, versions_per_asset=2, corpus=corpus[0]))
    list(history.records())
    with pytest.raises(ValueError):
        list(history.records())


def test_logical_shape_changes_identity_but_delivery_order_does_not(corpus):
    config = WorkloadConfig(assets=3, versions_per_asset=4, corpus=corpus[0])
    original = {snapshot.snapshot_id for _, snapshot, _ in History(config).records()}
    changed = {
        snapshot.snapshot_id for _, snapshot, _ in History(replace(config, assets=4)).records()
    }
    assert original.isdisjoint(changed)
    reordered = {
        snapshot.snapshot_id
        for _, snapshot, _ in History(
            replace(config, out_of_order_ratio=1, duplicate_ratio=1)
        ).records()
    }
    assert original == reordered


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


def test_logging_mutation_changes_intent_not_only_line_position():
    updated, _, _ = mutate("no logging on\nhostname test\nend\n", "cisco_ios", 2)
    assert "\nlogging on\n" in updated
    assert "no logging on" not in updated
    updated, _, _ = mutate("ip name-server 192.0.2.4\nend\n", "cisco_ios", 3)
    assert "ip name-server 192.0.2.5" in updated


def test_remediation_rollback_is_correlated_and_nearby(corpus):
    records = list(
        History(
            WorkloadConfig(
                assets=3, versions_per_asset=6, corpus=corpus[0], profile="incident-remediation"
            )
        ).records()
    )
    changed = [entry for entry in records if entry[1].version == 3]
    rolled_back = [entry for entry in records if entry[1].version == 4]
    assert len({entry[1].event_time for entry in rolled_back}) == 1
    assert (rolled_back[0][1].event_time - changed[0][1].event_time).total_seconds() == 900
    assert changed[0][2]["rollout_id"] == rolled_back[0][2]["rollout_id"]


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


@pytest.mark.parametrize("path", ["root/../escape/configs/router.cfg", "/root/configs/router.cfg"])
def test_archive_rejects_unsafe_paths(tmp_path, path):
    archive = tmp_path / "unsafe.tar.gz"
    with tarfile.open(archive, "w:gz") as bundle:
        member = tarfile.TarInfo(path)
        member.size = 1
        bundle.addfile(member, io.BytesIO(b"x"))
    source = Source(
        name="fixture",
        repository="batfish/batfish",
        revision="a" * 40,
        license="Apache-2.0",
        archive_sha256=checksum(archive.read_bytes()),
    )
    with pytest.raises(ValueError, match="Unsafe archive path"):
        import_archive(archive, source, tmp_path)


@pytest.mark.parametrize(
    "syntax,content,asset_type",
    [
        ("nginx", "worker_processes 2;\nkeepalive_timeout 65;\n", "nginx_server"),
        ("generic_service", "timeout 30\nreplicas 2\n", "generic_service"),
    ],
)
def test_nonnetwork_baselines_use_the_generic_asset_contract(corpus, syntax, content, asset_type):
    directory, manifest = corpus
    entry = manifest["files"][0]
    (directory / "files" / entry["path"]).write_text(content)
    entry.update(bytes=len(content.encode()), sha256=checksum(content.encode()), syntax=syntax)
    (directory / "manifest.json").write_text(json.dumps(manifest))
    history = History(WorkloadConfig(assets=1, versions_per_asset=2, corpus=directory))
    assert history.assets[0].asset_type == asset_type
    records = list(history.records())
    assert records[0][0].event_type == "config_observed"
    assert records[1][0].event_type == "config_changed"
    assert records[0][1].content != records[1][1].content


@pytest.mark.parametrize(
    "options",
    [{"assets": 0}, {"duplicate_ratio": 2}, {"profile": "bad"}, {"max_reordering_distance": 0}],
)
def test_invalid_config(options):
    with pytest.raises(ValueError):
        WorkloadConfig(**options)
