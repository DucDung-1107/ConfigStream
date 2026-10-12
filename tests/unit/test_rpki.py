import json
from datetime import timedelta
from hashlib import sha256

import pytest

from lake.rpki import PHYSICAL_REGISTRY, LocalStore, safe_key, validated_paths
from messaging.config import KafkaConfig
from messaging.rpki import publish_source
from schemas.rpki import DayReference, parse_chunk
from schemas.temporal.records import Authorization
from workloads.rpki_main import catalog
from workloads.rpki_oracle import independent_member_id, run_oracle
from workloads.rpki_source import source_chunks


def test_owned_oracle_exact_sets_and_reconstruction(rpki_fixture):
    _, _, _, summary, _ = rpki_fixture
    assert [day["source_rows"] for day in summary["days"]] == [4, 3]
    assert [day["unique_members"] for day in summary["days"]] == [3, 3]
    assert [day["duplicate_rows"] for day in summary["days"]] == [1, 0]
    assert summary["boundaries"][0]["additions"] == summary["boundaries"][0]["removals"] == 1
    assert summary["unique_members_window"] == 4
    expected = [
        b'["owned.tal",64500,"192.0.2.0/24",24]',
        b'["owned.tal",64501,"198.51.100.0/24",24]',
        b'["owned.tal",64502,"2001:db8::/32",48]',
    ]
    digest = sha256(b"rpki-set-v1\n")
    for encoding in expected:
        digest.update(len(encoding).to_bytes(4, "big"))
        digest.update(encoding)
    assert summary["days"][0]["set_digest"] == digest.hexdigest()
    assert all(item["missing"] == item["unexpected"] == 0 for item in summary["reconstruction"])


def test_member_id_independent_preimage():
    expected = sha256(
        b'["configstream-temporal-v1","rpki_membership","membership-v1",'
        b'[["string","owned.tal"],["integer",64500],["string","192.0.2.0/24"],["integer",24]]]'
    ).hexdigest()
    assert independent_member_id("owned.tal", 64500, "192.0.2.0/24", 24) == expected
    assert (
        Authorization(
            scope="owned.tal", asn=64500, prefix="192.0.2.0/24", max_length=24
        ).membership_id
        == expected
    )


def test_source_chunk_retries_coordinates_and_repeated_days(rpki_fixture):
    cache, selection, _, _, references = rpki_fixture
    first, second = references.values()
    path = cache / selection["artifacts"][0]["file"]
    chunks = list(source_chunks(path, first, chunk_size=2))
    assert [item.first_row for item in chunks] == [1, 3]
    assert chunks == list(source_chunks(path, first, chunk_size=2))
    assert chunks[0].rows[0].member == chunks[1].rows[0].member
    assert chunks[0].rows[0].row_id != chunks[1].rows[0].row_id
    assert parse_chunk(chunks[0].model_dump_json(), references) == chunks[0]
    other = next(source_chunks(cache / selection["artifacts"][1]["file"], second))
    assert first.observation_id != second.observation_id
    assert chunks[0].rows[0].row_id != other.rows[0].row_id


@pytest.mark.parametrize(
    "field,value",
    [
        ("transport_version", "future"),
        ("observation_id", "f" * 64),
        ("provenance_id", "f" * 64),
        ("chunk_id", "f" * 64),
        ("first_row", 2),
    ],
)
def test_invalid_chunk_rejected(rpki_fixture, field, value):
    cache, selection, _, _, references = rpki_fixture
    chunk = next(
        source_chunks(cache / selection["artifacts"][0]["file"], next(iter(references.values())))
    )
    data = chunk.model_dump(mode="json") | {field: value}
    with pytest.raises(ValueError):
        parse_chunk(json.dumps(data), references)


@pytest.mark.parametrize("problem", ["missing", "checksum", "corrupt_csv"])
def test_invalid_source_integrity_fails_without_valid_completion(rpki_fixture, problem):
    cache, selection, output, _, references = rpki_fixture
    path = cache / selection["artifacts"][0]["file"]
    if problem == "missing":
        path.unlink()
    else:
        path.write_bytes(b"owned invalid source")
    with pytest.raises((ValueError, FileNotFoundError)):
        list(source_chunks(path, next(iter(references.values()))))
    with pytest.raises((ValueError, FileNotFoundError)):
        run_oracle(selection, cache, output, columnar=False)


def test_oracle_resume_and_pinned_revision_conflict(rpki_fixture):
    cache, selection, output, original, _ = rpki_fixture
    resumed = run_oracle(selection, cache, output, columnar=False)
    assert resumed["days"] == original["days"]
    assert resumed["boundaries"] == original["boundaries"]
    modified = json.loads(json.dumps(selection))
    modified["artifacts"][0]["sha256"] = "f" * 64
    with pytest.raises(ValueError, match="checksum"):
        run_oracle(modified, cache, output, columnar=False)


def test_reference_accounting_digest_and_policy_fail_closed(rpki_fixture):
    _, _, _, summary, references = rpki_fixture
    reference = next(iter(references.values()))
    data = reference.model_dump(mode="json")
    data["observation"]["payload"]["expected_records"] += 1
    with pytest.raises(ValueError):
        DayReference.model_validate(data)
    data = reference.model_dump(mode="json")
    data["manifest"]["parser_version"] = "wrong"
    with pytest.raises(ValueError):
        DayReference.model_validate(data)
    assert summary["days"][0]["excluded_rows"] == 0


class AcknowledgingProducer:
    def __init__(self, fail=False):
        self.values = []
        self.fail = fail

    def produce(self, topic, **kwargs):
        self.values.append(kwargs["value"])
        kwargs["on_delivery"]("owned failure" if self.fail else None, None)

    def flush(self, timeout):
        return 0


def test_publisher_resume_and_failed_ack(rpki_fixture, tmp_path):
    cache, selection, _, _, references = rpki_fixture
    reference = next(iter(references.values()))
    path = cache / selection["artifacts"][0]["file"]
    progress = tmp_path / "progress.json"
    failed = AcknowledgingProducer(fail=True)
    with pytest.raises(RuntimeError, match="unconfirmed"):
        publish_source(path, reference, progress, KafkaConfig(), client=failed)
    assert not progress.exists()
    producer = AcknowledgingProducer()
    result = publish_source(path, reference, progress, KafkaConfig(), chunk_size=2, client=producer)
    assert result["acknowledged_rows_this_run"] == 4
    assert len(producer.values) == 2
    replay = publish_source(path, reference, progress, KafkaConfig(), chunk_size=2, client=producer)
    assert replay["acknowledged_rows_this_run"] == 0 and len(producer.values) == 2
    progress.write_text(json.dumps(dict(observation_id="f" * 64, next_row=1)))
    with pytest.raises(ValueError, match="different"):
        publish_source(path, reference, progress, KafkaConfig(), client=producer)


@pytest.mark.parametrize("fail_at", [1, 2])
def test_publisher_resource_failure_cannot_advance_progress(rpki_fixture, tmp_path, fail_at):
    cache, selection, _, _, references = rpki_fixture
    reference = next(iter(references.values()))
    producer = AcknowledgingProducer()
    progress = tmp_path / "guarded-progress.json"
    checks = 0

    def check():
        nonlocal checks
        checks += 1
        if checks == fail_at:
            raise RuntimeError("owned resource failure")

    with pytest.raises(RuntimeError, match="resource failure"):
        publish_source(
            cache / selection["artifacts"][0]["file"],
            reference,
            progress,
            KafkaConfig(),
            client=producer,
            resource_check=check,
        )
    assert len(producer.values) == fail_at - 1 and not progress.exists()


def test_publication_retry_conflict_and_partial_visibility(tmp_path):
    store = LocalStore(tmp_path)
    path = store.path("members", "a" * 64)
    from pathlib import Path

    Path(path).mkdir(parents=True)
    (Path(path) / "partial").write_text("owned partial fixture")
    assert store.list("state") == []
    metadata = dict(format_version="rpki-1.0", dataset="members", identity="a" * 64, path=path)
    store.put("state/owned.tal/2026-09-01", metadata)
    store.put("state/owned.tal/2026-09-01", metadata)
    assert len(store.list("state")) == 1
    with pytest.raises(ValueError, match="conflict"):
        store.put("state/owned.tal/2026-09-01", metadata | {"digest": "wrong"})
    assert validated_paths(store, metadata, "members") == path
    with pytest.raises(ValueError, match="path"):
        validated_paths(store, metadata | {"path": "other"}, "members")
    assert PHYSICAL_REGISTRY["observations"] == "temporal/rpki/observations"


@pytest.mark.parametrize("key", ["../other", "owned/../other", "/absolute", "owned//x", "owned\\x"])
def test_path_traversal_rejected(key):
    with pytest.raises(ValueError):
        safe_key(key)


def test_gap_oracle_rejected(rpki_fixture, tmp_path):
    cache, selection, _, _, _ = rpki_fixture
    source = selection["artifacts"][1].copy()
    import datetime

    day = datetime.date(2026, 9, 2) + timedelta(days=1)
    source["file"] = f"rpki-owned.tal-{day}.xz"
    (cache / source["file"]).write_bytes((cache / selection["artifacts"][1]["file"]).read_bytes())
    with pytest.raises(ValueError, match="missing day"):
        run_oracle(
            selection | {"artifacts": [selection["artifacts"][0], source]},
            cache,
            tmp_path / "gap",
            columnar=False,
        )


def test_catalog_day_identity_binding(rpki_fixture):
    _, selection, _, summary, references = rpki_fixture
    assert catalog(selection, summary, 2) == references
    assert len(catalog(selection, summary, 1)) == 1


def test_equal_archive_bytes_on_different_dates_are_distinct_observations(rpki_fixture):
    from schemas.rpki import row_identity
    from workloads.rpki_reference import day_reference

    _, selection, _, summary, references = rpki_fixture
    first = next(iter(references.values()))
    other_day = day_reference(
        summary["days"][0] | {"date": "2026-09-02"}, selection["artifacts"][0], "b" * 64
    )
    assert first.observation_id != other_day.observation_id
    assert row_identity(first, 1) != row_identity(other_day, 1)


def test_source_row_identity_independent_vector(rpki_fixture):
    from schemas.rpki import row_identity

    reference = next(iter(rpki_fixture[4].values()))
    policy = sha256(
        b'["configstream-temporal-v1","rpki_row_policy","v1",'
        b'[["string","rpki-csv-v1"],["string","membership-v1"],'
        b'["string","csv-data-rows-v1"],["string","observation-bound-row-v1"]]]'
    ).hexdigest()
    components = [
        reference.manifest.source_id,
        reference.manifest.source_scope,
        reference.observation_id,
        reference.manifest.artifacts[0].sha256,
    ]
    preimage = [
        "configstream-temporal-v1",
        "rpki_source_row",
        policy,
        [["string", value] for value in components] + [["integer", 1]],
    ]
    expected = sha256(json.dumps(preimage, separators=(",", ":")).encode()).hexdigest()
    assert row_identity(reference, 1) == expected


def test_resource_tree_accounting_is_bounded_to_run(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from workloads import rpki_resources
    from workloads.rpki_resources import ResourceSampler, directory_bytes, tree_rss

    assert tree_rss("1 0 10\n2 1 20\n3 2 30\n4 0 99", 1) == 60 * 1024
    assert tree_rss("1 0 10\n2 1 20", 99) is None

    def removed_file():
        raise FileNotFoundError("owned transient deletion")

    gone = SimpleNamespace(is_file=lambda: True, stat=removed_file)
    assert directory_bytes(SimpleNamespace(rglob=lambda pattern: [gone])) == 0
    cancelled = []
    sampler = ResourceSampler(
        tmp_path,
        SimpleNamespace(sparkContext=SimpleNamespace(cancelAllJobs=lambda: cancelled.append(True))),
    )

    def denied(root):
        raise PermissionError("owned sampling failure")

    monkeypatch.setattr(rpki_resources, "directory_bytes", denied)
    monkeypatch.setattr(rpki_resources, "command", lambda arguments: None)
    sampler.sample()
    assert sampler.failure == "resource_sampling_failed" and cancelled == [True]
    with pytest.raises(RuntimeError, match="safety validation"):
        sampler.check()


@pytest.mark.parametrize(
    "topic", ["config.snapshots", "arbitrary", "temporal.rpki.fixture.not-a-uuid"]
)
def test_unregistered_rpki_topics_fail_closed(topic):
    from schemas.rpki import validate_topic

    with pytest.raises(ValueError):
        validate_topic(topic)


@pytest.mark.parametrize(
    "row",
    [
        b"broken,row\n",
        b"fixture:a,ASbad,192.0.2.0/24,24,owned,owned\n",
        b"fixture:a,AS64500,192.0.2.1/24,24,owned,owned\n",
        b"fixture:a,AS64500,192.0.2.0/24,33,owned,owned\n",
    ],
)
def test_verified_corrupt_csv_is_rejected(rpki_fixture, tmp_path, row):
    import lzma

    from workloads.rpki_reference import day_reference

    _, selection, _, summary, _ = rpki_fixture
    data = b"URI,ASN,IP Prefix,Max Length,Not Before,Not After\n" + row
    compressed = lzma.compress(data)
    path = tmp_path / "corrupt.xz"
    path.write_bytes(compressed)
    artifact = selection["artifacts"][0] | {
        "sha256": sha256(compressed).hexdigest(),
        "artifact_bytes": len(compressed),
    }
    reference = day_reference(summary["days"][0], artifact, "b" * 64)
    with pytest.raises(ValueError):
        list(source_chunks(path, reference))
