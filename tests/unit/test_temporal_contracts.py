import io
import json
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from struct import pack
from uuid import UUID

import pytest
from pydantic import ValidationError

from lake import temporal as publication
from lake.config import LakeConfig
from messaging.serialization import serialize_record
from processing.mrt_framing import FramingError, frame_mrt
from processing.temporal_state import (
    authorization_digest,
    complete_rpki_state,
    dns_fixture_difference,
    rpki_fixture_transition,
)
from schemas import ConfigSnapshot
from schemas.temporal.coordinates import (
    DerivedCoordinate,
    MrtCoordinate,
    ParquetCoordinate,
    StateCoordinate,
)
from schemas.temporal.identity import canonical_bytes, logical_identity
from schemas.temporal.provenance import Artifact, SourceManifest, verify_artifact_file
from schemas.temporal.reconciliation import reconcile
from schemas.temporal.records import (
    Authorization,
    Delivery,
    DnsMeasurement,
    Envelope,
    Evidence,
    RisOccurrence,
    RpkiObservation,
    RpkiTransition,
    evidence_identity,
    parse_evidence,
)
from schemas.temporal.ris_expansion import MpPrefixes, expansion_order
from schemas.temporal.time import KnownTime, ObservationDate, TimeInterval, UnknownTime

SHA = "a" * 64
QUERY = "00000000-0000-4000-8000-000000000016"
DAY = date(2026, 9, 1)
POINT = datetime(2026, 9, 1, tzinfo=UTC)


def manifest(*, kind="ris_occurrence", count=1, artifact=SHA, **changes):
    values = dict(
        manifest_version="1.0",
        source_id="owned",
        dataset_id="owned-window",
        source_scope="ripencc"
        if kind.startswith("rpki")
        else "rrc00"
        if kind.startswith("ris")
        else "li",
        artifacts=(
            Artifact(
                sha256=artifact, safe_reference="fixture:phase16/source", revision="v1", bytes=12
            ),
        ),
        parser_version="owned-v1",
        canonicalization_version="owned-v1",
        counting_policy_version="owned-v1",
        selection_manifest_id="b" * 64,
        source_time_coverage=UnknownTime(role="source", reason="fixture"),
        declared_observation_coverage="owned fixture only",
        license_terms_reference="project-owned",
        expected_records=count,
        accepted_records=count,
        excluded_records=0,
        integrity_verified=True,
        accounting_complete=True,
    )
    return SourceManifest(**(values | changes))


def point(*, role="source", **changes):
    return KnownTime(
        **(
            dict(
                role=role,
                original="2026-09-01T00:00:00Z",
                value=POINT,
                precision="second",
                encoding="iso8601",
            )
            | changes
        )
    )


def event(*, action="announcement", **changes):
    return RisOccurrence(
        **(
            dict(
                collector="rrc00",
                peer_asn=64500,
                peer_ip="192.0.2.1",
                action=action,
                prefix="198.51.100.0/24",
                afi=1,
                safi=1,
                path_id=None,
                attribute_digest="c" * 64,
                acceptance="accepted",
            )
            | changes
        )
    )


def measurement(*, day=DAY, **changes):
    return DnsMeasurement(
        **(
            dict(
                partition_date=day,
                query_name="example.invalid.",
                query_type="A",
                response_name="example.invalid.",
                response_type="A",
                status="nonempty",
                values=("192.0.2.1",),
                empty_evidence_reference=None,
                worker_context=None,
                collection_method="owned-v1",
                comparison_policy="address-v1",
                entity_coverage="qualified",
            )
            | changes
        )
    )


def evidence(payload=None, source=None, coordinate=None, time=None, quality="accepted"):
    payload = event() if payload is None else payload
    source = manifest(kind=payload.kind) if source is None else source
    coordinate = (
        MrtCoordinate(artifact_sha256=SHA, byte_offset=0, all_record_index=0, expansion_ordinal=0)
        if coordinate is None
        else coordinate
    )
    header = Envelope(
        envelope_version="1.0",
        record_kind=payload.kind,
        payload_schema_version="1.0",
        source_id=source.source_id,
        dataset_id=source.dataset_id,
        source_scope=source.source_scope,
        logical_record_id="0" * 64,
        provenance_manifest_id=source.manifest_id,
        source_coordinate=coordinate,
        tagged_time_descriptor=point() if time is None else time,
        quality_status=quality,
    )
    draft = Evidence(envelope=header, payload=payload)
    return Evidence(
        envelope=Envelope(
            **(header.model_dump() | {"logical_record_id": evidence_identity(draft, source)})
        ),
        payload=payload,
    ), source


def authorization(*, asn=64500, **changes):
    return Authorization(
        **(dict(scope="ripencc", asn=asn, prefix="192.0.2.0/24", max_length=24) | changes)
    )


def state(rows, *, day=DAY, complete=True, **changes):
    source = manifest(kind="rpki_observation", count=len(rows), accounting_complete=complete)
    payload = RpkiObservation(
        **(
            dict(
                observation_date=day,
                comparison_policy="set-v1",
                validation_regime="archive-reported-regime-unknown",
                completion="complete" if complete else "incomplete",
                expected_records=len(rows),
                accepted_records=len(rows),
                duplicate_records=len(rows) - len(set(rows)),
                unique_members=len(set(rows)),
                excluded_records=0,
                set_digest=authorization_digest(rows),
            )
            | changes
        )
    )
    record, source = evidence(
        payload,
        source,
        StateCoordinate(artifact_sha256=SHA, observation_date=day.isoformat()),
        ObservationDate(value=day),
        "accepted" if complete else "incomplete",
    )
    return record, tuple(rows), source


@pytest.mark.parametrize(
    "kind,policy,components,expected",
    [
        (
            "ris_occurrence",
            "owned-v1",
            ("rrc00", SHA, 0, 0),
            "97e8414f4a1c735b225ed56cc75b90b2863f3f99086dc8d81c85a45994f7d2b2",
        ),
        (
            "dns_measurement",
            "owned-v1",
            ("é", 0, None),
            "6756a0abad4774fa297c26175fbef0c1b435a9be4ac02b81430c25ea9fd1bb25",
        ),
        (
            "rpki_membership",
            "membership-v1",
            ("ripencc", 64500, "192.0.2.0/24", 24),
            "0887446457d90ad235fc7fd0f20971d6abf380db1e1ef21a95304de22d4011b2",
        ),
    ],
)
def test_independent_identity_vectors(kind, policy, components, expected):
    assert logical_identity(kind, policy, components) == expected


def test_identity_normalization_and_nonambiguity():
    assert logical_identity("dns", "v1", ("e\u0301",)) == logical_identity("dns", "v1", ("é",))
    assert logical_identity("dns", "v1", ("ab", "c")) != logical_identity("dns", "v1", ("a", "bc"))
    assert logical_identity("dns", "v1", (True,)) != logical_identity("dns", "v1", (1,))
    assert canonical_bytes({"b": 1, "a": None}) == b'{"a":null,"b":1}'


@pytest.mark.parametrize(
    "value", [float("nan"), 1.2, {"e\u0301": 1, "é": 2}, [None] * 10001, "x" * 65537, 2**64]
)
def test_canonical_bounds_and_collisions(value):
    with pytest.raises(ValueError):
        canonical_bytes(value)


@pytest.mark.parametrize(
    "field,value",
    [
        ("record_kind", "config_snapshot"),
        ("envelope_version", "2.0"),
        ("payload_schema_version", "2.0"),
        ("source_scope", "../escape"),
        ("logical_record_id", "x"),
    ],
)
def test_envelope_fail_closed(field, value):
    record, _ = evidence()
    with pytest.raises(ValidationError):
        Envelope.model_validate(record.envelope.model_dump() | {field: value})


def test_kind_coordinate_and_old_transport_isolation():
    record, _ = evidence()
    assert parse_evidence(record.model_dump_json()) == record
    with pytest.raises(ValidationError):
        Evidence.model_validate(record.model_dump() | {"payload": measurement().model_dump()})
    with pytest.raises(ValidationError):
        ConfigSnapshot.model_validate(record.model_dump())
    with pytest.raises(TypeError):
        serialize_record(record)
    wrong = record.envelope.model_dump() | {
        "source_coordinate": ParquetCoordinate(artifact_sha256=SHA, row_group=0, row=0)
    }
    with pytest.raises(ValidationError):
        Evidence(envelope=Envelope(**wrong), payload=record.payload)


def test_provenance_binding_and_artifact_revisions():
    record, source = evidence()
    record.validate_manifest(source)
    revised = manifest(artifact="d" * 64)
    assert revised.manifest_id != source.manifest_id
    with pytest.raises(ValueError, match="provenance"):
        record.validate_manifest(revised)
    coordinate = MrtCoordinate(
        artifact_sha256="d" * 64, byte_offset=0, all_record_index=0, expansion_ordinal=0
    )
    wrong, _ = evidence(source=source, coordinate=coordinate)
    with pytest.raises(ValueError, match="artifact"):
        wrong.validate_manifest(source)
    for policy_field in ["parser_version", "canonicalization_version", "counting_policy_version"]:
        newer = manifest(**{policy_field: "v2"})
        changed, _ = evidence(source=newer)
        assert changed.envelope.logical_record_id != record.envelope.logical_record_id


@pytest.mark.parametrize(
    "reference",
    [
        "http://example.invalid/a",
        "https://user:password@example.invalid/a",
        "https://example.invalid/a?token=x",
        "fixture:../secret",
    ],
)
def test_unsafe_artifact_references(reference):
    with pytest.raises(ValidationError):
        Artifact(sha256=SHA, safe_reference=reference, revision="v1", bytes=1)


def test_source_accounting_and_duplicate_artifact_rejection():
    with pytest.raises(ValidationError):
        manifest(accepted_records=0)
    with pytest.raises(ValidationError):
        manifest(integrity_verified=False)
    source = manifest()
    with pytest.raises(ValidationError):
        SourceManifest(**(source.model_dump() | {"artifacts": source.artifacts * 2}))


def test_time_precision_offsets_unknown_and_arrival():
    converted = point(original="2026-09-01T07:00:00+07:00", value=POINT)
    assert converted.value == POINT and converted.original.endswith("+07:00")
    encoded = point(role="observation", original="1788220800000", encoding="epoch_millisecond")
    assert encoded.precision == "second" and encoded.value == POINT
    assert UnknownTime(role="observation", reason="source missing").kind == "unknown"
    record, source = evidence()
    first = Delivery(evidence=record, ingestion_time=POINT)
    retry = Delivery(evidence=record, ingestion_time=POINT + timedelta(days=1))
    assert first != retry
    assert len(reconcile((first.evidence, retry.evidence), {source.manifest_id: source})) == 1


@pytest.mark.parametrize(
    "changes",
    [
        dict(original="1788220800000", encoding="epoch_second"),
        dict(value=POINT.replace(tzinfo=None)),
        dict(original="2026-09-01T00:00:01Z"),
        dict(original="x", encoding="epoch_millisecond"),
    ],
)
def test_invalid_timestamp_units_or_invented_time(changes):
    with pytest.raises(ValueError):
        point(**changes)


def test_interval_unknown_and_out_of_order_are_not_repaired():
    with pytest.raises(ValidationError):
        TimeInterval(
            role="source", start=POINT, end=POINT - timedelta(seconds=1), precision="second"
        )
    previous, _ = evidence(
        time=point(original="2026-09-01T00:00:01Z", value=POINT + timedelta(seconds=1))
    )
    current, _ = evidence()
    assert (
        previous.envelope.tagged_time_descriptor.value
        > current.envelope.tagged_time_descriptor.value
    )


def test_replay_conflicts_and_multiplicity():
    first, source = evidence()
    second, _ = evidence(
        coordinate=MrtCoordinate(
            artifact_sha256=SHA, byte_offset=63, all_record_index=1, expansion_ordinal=0
        )
    )
    mapping = {source.manifest_id: source}
    assert len(reconcile((first, first, second), mapping)) == 2
    altered, _ = evidence(event(prefix="203.0.113.0/24"))
    with pytest.raises(ValueError, match="conflict"):
        reconcile((first, altered), mapping)
    with pytest.raises(ValueError, match="Missing"):
        reconcile((first,), {})
    with pytest.raises(ValueError, match="limit"):
        reconcile((first, first), mapping, limit=1)


def test_dns_equal_values_distinct_coordinates():
    source = manifest(kind="dns_measurement", count=2)
    records = [
        evidence(
            measurement(),
            source,
            ParquetCoordinate(artifact_sha256=SHA, row_group=0, row=index),
            point(role="observation"),
        )[0]
        for index in range(2)
    ]
    assert len(reconcile((*records, records[0]), {source.manifest_id: source})) == 2


def test_independent_rpki_checksum_duplicates_and_exact_reconstruction():
    member = authorization()
    assert (
        authorization_digest((member,))
        == "7199a2661b3e44755b5dd9117e0438684a3e2f8b80088b5e0cf3320a403f94f8"
    )
    assert (
        member.membership_id == "0887446457d90ad235fc7fd0f20971d6abf380db1e1ef21a95304de22d4011b2"
    )
    old = state((member, member))
    new = state((member, authorization(asn=64501)), day=DAY + timedelta(days=1))
    changes = rpki_fixture_transition(old[0], new[0], old[1], new[1], old[2], new[2])
    assert changes == {
        "added": frozenset({authorization(asn=64501)}),
        "removed": frozenset(),
        "unchanged": frozenset({member}),
    }
    reconstructed = (complete_rpki_state(*old) - changes["removed"]) | changes["added"]
    assert reconstructed == complete_rpki_state(*new)
    assert old[0].payload.duplicate_records == 1
    same, _, _ = state((member, member), day=DAY + timedelta(days=1))
    assert old[0].envelope.logical_record_id != same.envelope.logical_record_id


@pytest.mark.parametrize(
    "scenario", ["gap", "partial", "regime", "policy", "scope", "truncated", "checksum", "restart"]
)
def test_rpki_completeness_barrier(scenario):
    old = state((authorization(),))
    new = state((), day=DAY + timedelta(days=1))
    if scenario == "gap":
        new = state((), day=DAY + timedelta(days=2))
    if scenario in {"partial", "restart"}:
        new = state((), day=DAY + timedelta(days=1), complete=False)
    if scenario == "regime":
        new = state((), day=DAY + timedelta(days=1), validation_regime="changed")
    if scenario == "policy":
        new = state((), day=DAY + timedelta(days=1), comparison_policy="changed")
    if scenario == "scope":
        new = (new[0], new[1], manifest(kind="rpki_observation", count=0, source_scope="other"))
    if scenario == "truncated":
        new = (new[0], (authorization(),), new[2])
    if scenario == "checksum":
        new = state((), day=DAY + timedelta(days=1), set_digest="f" * 64)
    with pytest.raises(ValueError):
        rpki_fixture_transition(old[0], new[0], old[1], new[1], old[2], new[2])


def test_rpki_late_completion_conflicts_and_retry():
    pending = state((authorization(),), complete=False)
    with pytest.raises(ValueError):
        complete_rpki_state(*pending)
    final = state((authorization(),))
    assert complete_rpki_state(*final) == frozenset({authorization()})
    assert len(reconcile((final[0], final[0]), {final[2].manifest_id: final[2]})) == 1
    with pytest.raises(ValueError):
        state((authorization(),), expected_records=2)


@pytest.mark.parametrize(
    "changes",
    [
        dict(asn=True),
        dict(asn=-1),
        dict(prefix="192.0.2.1/24"),
        dict(max_length=23),
        dict(max_length=33),
        dict(prefix="2001:db8::/32", max_length=129),
    ],
)
def test_authorization_invalid(changes):
    with pytest.raises(ValidationError):
        authorization(**changes)


@pytest.mark.parametrize("status", ["absent", "unknown", "timeout", "failure"])
def test_dns_missing_failed_not_empty_or_deleted(status):
    old = measurement()
    new = measurement(
        day=DAY + timedelta(days=1), status=status, values=(), entity_coverage="unknown"
    )
    with pytest.raises(ValueError):
        dns_fixture_difference(old, new, partitions_complete=True)
    with pytest.raises(ValidationError):
        measurement(status=status, values=(), entity_coverage="qualified")


def test_dns_proven_empty_changes_workers_and_partition_barrier():
    old = measurement()
    empty = measurement(
        day=DAY + timedelta(days=1),
        status="proven_empty",
        values=(),
        empty_evidence_reference="owned-query-proof",
    )
    assert dns_fixture_difference(old, empty, partitions_complete=True)[
        "removed_values"
    ] == frozenset({"192.0.2.1"})
    with pytest.raises(ValidationError):
        measurement(status="proven_empty", values=())
    with pytest.raises(ValueError):
        dns_fixture_difference(old, empty, partitions_complete=False)
    changed = measurement(day=DAY + timedelta(days=1), values=("192.0.2.2",))
    assert dns_fixture_difference(old, changed, partitions_complete=True)[
        "added_values"
    ] == frozenset({"192.0.2.2"})
    equal = measurement(day=DAY + timedelta(days=1))
    assert dns_fixture_difference(old, equal, partitions_complete=True) == {
        "added_values": frozenset(),
        "removed_values": frozenset(),
    }
    with pytest.raises(ValueError):
        dns_fixture_difference(
            old,
            measurement(day=equal.partition_date, worker_context="other"),
            partitions_complete=True,
        )


def test_mrt_coordinates_literal_oracle_unsupported_malformed_and_ties():
    fixture = json.loads(Path("tests/fixtures/phase16/mrt.json").read_text())
    update = bytes.fromhex(fixture["one_update_hex"])
    assert len(update) == 63
    for _ in range(2):
        frames = tuple(frame_mrt(io.BytesIO(update * 2)))
        assert [frame.byte_offset for frame in frames] == fixture[
            "expected_offsets_two_identical_updates"
        ]
        assert [frame.next_offset for frame in frames] == fixture["expected_next_offsets"]
        assert [frame.all_record_index for frame in frames] == [0, 1]
        assert [frame.timestamp_seconds for frame in frames] == [1, 1]
    for first, offsets in [
        ("unsupported_hex", "unsupported_then_update_offsets"),
        ("malformed_bgp_body_hex", "malformed_then_update_offsets"),
    ]:
        frames = tuple(frame_mrt(io.BytesIO(bytes.fromhex(fixture[first]) + update)))
        assert [frame.byte_offset for frame in frames] == fixture[offsets]
        assert [frame.all_record_index for frame in frames] == [0, 1]
    extended = next(frame_mrt(io.BytesIO(bytes.fromhex(fixture["extended_timestamp_hex"]))))
    assert extended.microseconds == 500000 and extended.next_offset == 17


@pytest.mark.parametrize(
    "data,category",
    [
        (b"x", "truncated_header"),
        (pack("!IHHI", 1, 16, 4, 10) + b"x", "truncated_body"),
        (pack("!IHHI", 1, 16, 4, 2**32 - 1), "declared_length_limit"),
        (pack("!IHHI", 1, 17, 4, 1) + b"x", "truncated_extended_timestamp"),
    ],
)
def test_mrt_truncation_and_invalid_length_fail_without_resync(data, category):
    with pytest.raises(FramingError) as caught:
        tuple(frame_mrt(io.BytesIO(data)))
    assert (
        caught.value.category == category and caught.value.offset == 0 and caught.value.index == 0
    )


def test_mrt_empty_short_reads_and_bound():
    assert tuple(frame_mrt(io.BytesIO())) == ()

    class ShortRead(io.BytesIO):
        def read(self, size=-1):
            return super().read(min(size, 2))

    update = bytes.fromhex(
        json.loads(Path("tests/fixtures/phase16/mrt.json").read_text())["one_update_hex"]
    )
    assert next(frame_mrt(ShortRead(update))).next_offset == 63
    sliced = io.BytesIO(update)
    sliced.seek(12)
    with pytest.raises(ValueError, match="origin"):
        tuple(frame_mrt(sliced))
    with pytest.raises(ValueError):
        next(frame_mrt(io.BytesIO(update), max_body_bytes=True))


def test_expansion_wire_order_mp_addpath_and_multiplicity():
    withdrawal = event(action="withdrawal")
    announcement = event()
    mp_reach = event(prefix="2001:db8::/32", afi=2, path_id=7)
    mp_unreach = event(action="withdrawal", prefix="2001:db8:1::/48", afi=2)
    expanded = expansion_order(
        (withdrawal,),
        (announcement, announcement),
        (
            MpPrefixes(attribute=15, prefixes=(mp_unreach,)),
            MpPrefixes(attribute=14, prefixes=(mp_reach,)),
        ),
    )
    assert [ordinal for ordinal, _ in expanded] == [0, 1, 2, 3, 4]
    assert [record.action for _, record in expanded] == [
        "withdrawal",
        "announcement",
        "announcement",
        "withdrawal",
        "announcement",
    ]
    assert expanded[-1][1].path_id == 7
    with pytest.raises(ValueError):
        expansion_order((announcement,), (), ())
    with pytest.raises(ValidationError):
        event(safi=2)


def test_full_reduced_roadmap_closure():
    graph = json.loads(Path("workloads/results/phase16/dependency-amendment.json").read_text())
    for track in ["F", "R"]:
        nodes = dict(graph["base_dependencies"])
        if track == "F":
            nodes |= graph["full_only_dependencies"]
            for node, extra in graph["full_extra_dependencies"].items():
                nodes[node] = nodes[node] + extra
        complete = set()
        while len(complete) < len(nodes):
            ready = {
                node
                for node, parents in nodes.items()
                if node not in complete and set(parents) <= complete
            }
            assert ready, "dangling dependency or cycle"
            complete |= ready
        assert ("W9" in complete) == (track == "F")
        assert ("P19" in graph["phase20_dependencies"][track]) == (track == "F")
        assert graph["tracks"][track]["four_role_K3_allowed"] == (track == "F")
    broken = dict(graph["base_dependencies"]) | {"W10": ["W9"]}
    assert "W9" not in broken and "W9" in broken["W10"]


@pytest.fixture
def publication_fixture(tmp_path, monkeypatch):
    config = LakeConfig("owned", "owned")
    commits, writes = {}, []
    monkeypatch.setattr(LakeConfig, "uri", lambda self, suffix: str(tmp_path / suffix))
    monkeypatch.setattr(
        publication,
        "load_typed_commit",
        lambda config, query, batch, kind: commits.get((query, batch, kind)),
    )
    monkeypatch.setattr(
        publication,
        "save_typed_commit",
        lambda config, item: commits.update(
            {(item["query_id"], item["batch_id"], item["record_kind"]): item}
        ),
    )

    def writer(path, rows):
        destination = Path(path) / "owned.json"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(canonical_bytes([row.model_dump(mode="json") for row in rows]))
        writes.append(path)

    return config, commits, writes, writer


@pytest.mark.parametrize("failure", ["before_output", "partial_output", "before_manifest"])
def test_publication_failure_invisible_then_retry(publication_fixture, monkeypatch, failure):
    config, commits, writes, writer = publication_fixture
    record, source = evidence()

    def faulty_writer(path, rows):
        if failure == "before_output":
            raise RuntimeError("owned failure")
        writer(path, rows)
        if failure == "partial_output":
            raise RuntimeError("owned failure")

    original_save = publication.save_typed_commit
    if failure == "before_manifest":
        monkeypatch.setattr(
            publication,
            "save_typed_commit",
            lambda *args: (_ for _ in ()).throw(RuntimeError("owned failure")),
        )
    with pytest.raises(RuntimeError):
        publication.publish_typed_batch(
            config,
            QUERY,
            0,
            "ris_occurrence",
            (record,),
            {source.manifest_id: source},
            faulty_writer,
        )
    assert not commits
    assert (
        publication.committed_typed_paths(config, "ris_occurrence", tuple(commits.values())) == ()
    )
    monkeypatch.setattr(publication, "save_typed_commit", original_save)
    published = publication.publish_typed_batch(
        config, QUERY, 0, "ris_occurrence", (record,), {source.manifest_id: source}, writer
    )
    assert publication.committed_typed_paths(config, "ris_occurrence", (published,)) == (
        writes[-1],
    )


def test_committed_before_checkpoint_retry_and_conflict(publication_fixture):
    config, commits, writes, writer = publication_fixture
    record, source = evidence()
    inputs = {source.manifest_id: source}
    published = publication.publish_typed_batch(
        config, QUERY, 0, "ris_occurrence", (record,), inputs, writer
    )
    again = publication.publish_typed_batch(
        config, QUERY, 0, "ris_occurrence", (record, record), inputs, writer
    )
    assert published == again and len(writes) == 1 and len(commits) == 1
    conflict, _ = evidence(event(prefix="203.0.113.0/24"))
    with pytest.raises(ValueError, match="conflicts"):
        publication.publish_typed_batch(
            config, QUERY, 0, "ris_occurrence", (conflict,), inputs, writer
        )
    with pytest.raises(ValueError):
        publication.committed_typed_paths(
            config, "ris_occurrence", (published | {"dataset_path": "../other"},)
        )


def test_publication_state_barrier_and_descriptor_accounting(publication_fixture):
    config, commits, _, writer = publication_fixture
    final = state((authorization(),))
    record, rows, source = final
    for proof in [None, {record.envelope.logical_record_id: (record, (), source)}]:
        with pytest.raises(ValueError):
            publication.publish_typed_batch(
                config,
                QUERY,
                0,
                "rpki_observation",
                (record,),
                {source.manifest_id: source},
                writer,
                state_inputs=proof,
            )
    assert not commits
    published = publication.publish_typed_batch(
        config,
        QUERY,
        0,
        "rpki_observation",
        (record,),
        {source.manifest_id: source},
        writer,
        state_inputs={record.envelope.logical_record_id: final},
    )
    assert published["logical_records"] == 1


def test_transition_identity_and_publication_guard(publication_fixture):
    old = state(())
    new = state((authorization(),), day=DAY + timedelta(days=1))
    ids = (old[0].envelope.logical_record_id, new[0].envelope.logical_record_id)
    source = manifest(kind="rpki_transition", count=1, input_observation_ids=ids)
    payload = RpkiTransition(action="add", member=authorization(), comparison_policy="set-v1")
    record, _ = evidence(
        payload,
        source,
        DerivedCoordinate(previous_id=ids[0], current_id=ids[1]),
        TimeInterval(
            role="observation", start=POINT, end=POINT + timedelta(days=1), precision="day"
        ),
    )
    record.validate_manifest(source)
    config, _, _, writer = publication_fixture
    with pytest.raises(ValueError, match="input states"):
        publication.publish_typed_batch(
            config, QUERY, 0, "rpki_transition", (record,), {source.manifest_id: source}, writer
        )
    published = publication.publish_typed_batch(
        config,
        QUERY,
        0,
        "rpki_transition",
        (record,),
        {source.manifest_id: source},
        writer,
        state_inputs={ids[0]: old, ids[1]: new},
    )
    assert published["logical_records"] == 1


def test_registry_closed_and_legacy_allowlist_untouched():
    from spark.common.lake_schemas import DATASET_SCHEMAS

    assert set(DATASET_SCHEMAS) == {
        "raw/snapshots",
        "processed/snapshots",
        "processed/diffs",
        "processed/alerts",
        "processed/diagnostics",
    }
    for kind in publication.REGISTRY:
        assert publication.dataset_path(kind).startswith("temporal/")
    for kind in ["config_snapshot", "../escape", "raw/snapshots"]:
        with pytest.raises(ValueError):
            publication.dataset_path(kind)
    with pytest.raises(ValueError):
        publication.dataset_path("ris_occurrence", "2.0")
    with pytest.raises(TypeError):
        publication.REGISTRY["config_snapshot"] = "processed/snapshots"
    assert "_temporal_commits/v1/" in publication.temporal_commit_key(
        LakeConfig("owned", "owned"), QUERY, 0, "ris_occurrence"
    )
    assert UUID(QUERY)


def test_terraform_existing_fixture_reference_unchanged(terraform_fixture):
    from workloads.observed import load_dataset

    root, _, _ = terraform_fixture
    snapshots, _, _ = load_dataset(root)
    assert snapshots
    for snapshot in snapshots:
        serialized = json.loads(serialize_record(snapshot))
        assert serialized["record_type"] == "config_snapshot"
        assert ConfigSnapshot.model_validate(serialized["payload"]) == snapshot
        assert snapshot.hash == sha256(snapshot.content.encode()).hexdigest()


def test_artifact_checksum_size_and_budget_oracle(tmp_path):
    path = tmp_path / "owned"
    path.write_bytes(b"abc")
    digest = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    artifact = Artifact(sha256=digest, safe_reference="fixture:owned", revision="v1", bytes=3)
    verify_artifact_file(path, artifact)
    for change in [{"sha256": SHA}, {"bytes": 4}]:
        with pytest.raises(ValueError, match="integrity"):
            verify_artifact_file(path, Artifact(**(artifact.model_dump() | change)))
    with pytest.raises(ValueError, match="budget"):
        verify_artifact_file(path, artifact, max_bytes=2)


def test_raw_absence_cannot_manufacture_measurement_record():
    source = manifest(kind="dns_measurement")
    absent = measurement(status="absent", values=(), entity_coverage="unknown")
    with pytest.raises(ValueError, match="no raw measurement"):
        evidence(
            absent,
            source,
            ParquetCoordinate(artifact_sha256=SHA, row_group=0, row=0),
            point(role="observation"),
        )


@pytest.mark.parametrize("value", [1, True, 1788220800, datetime(2026, 9, 1, tzinfo=UTC)])
def test_date_does_not_coerce_precise_or_numeric_time(value):
    with pytest.raises(ValueError):
        ObservationDate(value=value)


@pytest.mark.parametrize("value", [True, "1", 1.0])
def test_ris_afi_is_strict(value):
    with pytest.raises(ValueError):
        event(afi=value)


def test_owned_mp_addpath_and_truncation_after_first_record():
    fixture = json.loads(Path("tests/fixtures/phase16/mrt-projections.json").read_text())
    data = bytes.fromhex(fixture["mp_hex"]) + bytes.fromhex(fixture["addpath_hex"])
    frames = tuple(frame_mrt(io.BytesIO(data)))
    assert [(frame.byte_offset, frame.next_offset, frame.all_record_index) for frame in frames] == [
        (0, 109, 0),
        (109, 180, 1),
    ]
    with pytest.raises(FramingError) as caught:
        tuple(frame_mrt(io.BytesIO(data[:-1])))
    assert (caught.value.category, caught.value.offset, caught.value.index) == (
        "truncated_body",
        109,
        1,
    )


def test_probe_crosscheck_offline_parser_failure_no_coordinate_drift():
    from types import SimpleNamespace

    from scripts.phase16_coordinate_probe import crosscheck

    fixture = json.loads(Path("tests/fixtures/phase16/mrt.json").read_text())
    update = bytes.fromhex(fixture["one_update_hex"])

    class OwnedReader:
        def __init__(self, stream):
            self.stream = stream

        def __next__(self):
            header = self.stream.read(12)
            if int.from_bytes(header[4:6], "big") == 99:
                raise KeyError("owned unsupported")
            self.stream.read(int.from_bytes(header[8:12], "big"))
            return SimpleNamespace(err=None, data={"timestamp": {1: "owned"}})

    result = crosscheck(
        io.BytesIO(bytes.fromhex(fixture["unsupported_hex"]) + update),
        SimpleNamespace(Reader=OwnedReader),
        max_records=2,
    )
    assert result["framed_records"] == 2 and result["parser_excluded_records"] == 1
    assert result["first_coordinates"][1]["offset"] == 14
    assert result["categories"]["parser_exception_KeyError"] == 1


def test_conditional_commit_metadata_uses_new_namespace(monkeypatch):
    calls = []

    class OwnedClient:
        def put_object(self, **options):
            calls.append(options)

    monkeypatch.setattr(publication, "s3_client", lambda config: OwnedClient())
    publication.save_typed_commit(
        LakeConfig("owned", "owned"),
        {"query_id": QUERY, "batch_id": 0, "record_kind": "ris_occurrence"},
    )
    assert calls[0]["IfNoneMatch"] == "*"
    assert "_temporal_commits/v1/" in calls[0]["Key"]


def test_committed_projection_checks_actual_output_and_reconciles_runs(publication_fixture):
    config, _, _, writer = publication_fixture
    record, source = evidence()
    mapping = {source.manifest_id: source}
    commit = publication.publish_typed_batch(
        config, QUERY, 0, "ris_occurrence", (record,), mapping, writer
    )
    assert publication.read_typed_projection(
        config, "ris_occurrence", (commit,), lambda path: (record,), mapping
    ) == (record,)
    for corrupted in [(), (evidence(event(prefix="203.0.113.0/24"))[0],)]:
        with pytest.raises(ValueError, match="checksum"):
            publication.read_typed_projection(
                config, "ris_occurrence", (commit,), lambda path, rows=corrupted: rows, mapping
            )
    assert (
        publication.read_typed_projection(
            config,
            "ris_occurrence",
            (),
            lambda path: (_ for _ in ()).throw(AssertionError("uncommitted read")),
            mapping,
        )
        == ()
    )


def test_model_copy_bypass_is_not_a_validation_boundary():
    record, source = evidence()
    illegal = record.model_copy(update={"payload": record.payload.model_copy(update={"afi": 9})})
    with pytest.raises(ValueError):
        reconcile((illegal,), {source.manifest_id: source})
    state_record, rows, provenance = state((authorization(),))
    with pytest.raises(ValueError):
        complete_rpki_state(state_record, (rows[0].model_copy(update={"asn": True}),), provenance)


def test_frame_error_ledger_not_an_event_occurrence():
    from schemas.temporal.errors import FrameAudit

    fields = dict(
        artifact_sha256=SHA,
        source_scope="rrc00",
        byte_offset=14,
        all_record_index=1,
        record_type=16,
        subtype=4,
        declared_body_bytes=1,
    )
    excluded = FrameAudit(**fields, classification="excluded", error_category="malformed_body")
    assert excluded.byte_offset == 14 and excluded.all_record_index == 1
    assert "expansion_ordinal" not in type(excluded).model_fields
    with pytest.raises(ValueError):
        FrameAudit(**fields, classification="parsed", error_category="malformed_body")
    with pytest.raises(ValueError):
        FrameAudit(**fields, classification="excluded", error_category=None)


def test_phase16_registry_matches_frozen_common_fields():
    registry = json.loads(Path("workloads/results/phase16/contract-registry.json").read_text())
    frozen = json.loads(Path("workloads/results/phase15/temporal-contracts.json").read_text())
    assert (
        set(registry["envelope_fields"])
        == set(Envelope.model_fields)
        == set(frozen["common_required"])
    )
    assert {entry["kind"]: entry["dataset"] for entry in registry["registrations"]} == dict(
        publication.REGISTRY
    )
    assert not registry["configuration_reference"]["migrated"]


def test_independent_vector_artifact_and_original_file_hashes():
    vectors = json.loads(Path("workloads/results/phase16/identity-test-vectors.json").read_text())
    for vector in vectors["vectors"]:
        assert (
            logical_identity(vector["kind"], vector["policy"], tuple(vector["components"]))
            == vector["expected"]
        )
    compatibility = json.loads(
        Path("workloads/results/phase16/publication-compatibility.json").read_text()
    )
    for path, expected in compatibility["old_file_sha256"].items():
        assert sha256(Path(path).read_bytes()).hexdigest() == expected


def test_invariant_coverage_has_no_false_service_pass():
    artifact = json.loads(Path("workloads/results/phase16/invariant-coverage.json").read_text())
    assert [entry["id"] for entry in artifact["invariants"]] == [
        f"I{index}" for index in range(1, 10)
    ]
    assert not any(entry["status"] == "PASS_INTEGRATION" for entry in artifact["invariants"])
    assert all(entry["pending"] for entry in artifact["invariants"])


def test_aggregate_normalization_budget_and_strict_dns_completion_flag():
    with pytest.raises(ValueError, match="bounded tuple"):
        logical_identity("dns", "v1", ("x",) * 65)
    with pytest.raises(ValueError, match="budget"):
        canonical_bytes(["x" * 65536] * 17)
    with pytest.raises(ValueError):
        dns_fixture_difference(
            measurement(), measurement(day=DAY + timedelta(days=1)), partitions_complete="false"
        )
