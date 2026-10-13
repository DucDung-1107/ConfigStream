import gzip
import io
import json
from hashlib import sha256
from pathlib import Path
from struct import pack

import mrtparse
import pytest

from lake.ris import EventStore
from processing.mrt_framing import FramingError, frame_mrt
from processing.ris import decode_frame, event_rows
from schemas.ris import CANONICAL, COUNTING, PARSER, EventChunk, RisReference, validate_topic
from schemas.temporal.provenance import SourceManifest
from workloads.ris_reference import extract, identity

FIXTURE = json.loads(Path("tests/fixtures/phase16/mrt.json").read_text())
PROJECTIONS = json.loads(Path("tests/fixtures/phase16/mrt-projections.json").read_text())


def reference():
    raw = json.loads(Path("tests/fixtures/phase16/ris.json").read_text())["manifest"]
    raw.update(
        parser_version=PARSER,
        canonicalization_version=CANONICAL,
        counting_policy_version=COUNTING,
        expected_records=2,
        accepted_records=2,
    )
    return RisReference(manifest=SourceManifest.model_validate(raw))


def owned_rows():
    source = reference()
    data = bytes.fromhex(FIXTURE["one_update_hex"])
    result = []
    for frame in frame_mrt(io.BytesIO(data + data)):
        record, error = decode_frame(frame, mrtparse)
        assert error is None
        result.extend(event_rows(frame, record, source))
    return source, result


@pytest.mark.parametrize("name", ["one", "mp", "addpath"])
def test_independent_owned_expansion(name):
    wire = bytes.fromhex(FIXTURE["one_update_hex"] if name == "one" else PROJECTIONS[name + "_hex"])
    frame = next(frame_mrt(io.BytesIO(wire)))
    record, error = decode_frame(frame, mrtparse)
    assert error is None
    source = reference()
    rows = tuple(event_rows(frame, record, source))
    oracle = extract(frame.record_type, frame.subtype, frame.body)
    assert [(row.action, row.afi, row.prefix, row.path_id) for row in rows] == oracle[3]
    expected = [
        ["announcement", value, None] for value in FIXTURE["expected_announcement_prefixes"]
    ]
    if name != "one":
        expected = PROJECTIONS[name + "_expected"]
    assert [[row.action, row.prefix, row.path_id] for row in rows] == expected
    for row in rows:
        row.validate_reference(source)
        row.evidence(source).validate_manifest(source.manifest)
        assert row.logical_record_id == identity(source, frame.byte_offset, row.expansion_ordinal)


def test_multiplicity_is_not_fingerprint_deduplication():
    source, rows = owned_rows()
    assert [row.byte_offset for row in rows] == [0, 0, 63, 63]
    assert len({row.logical_record_id for row in rows}) == 4
    assert len({row.content_fingerprint for row in rows}) == 2
    assert rows[0].logical_record_id != rows[2].logical_record_id
    assert rows[0].logical_record_id != rows[1].logical_record_id
    assert rows[0].logical_record_id != source.occurrence_id(0, 1)


def test_identity_scope_artifact_and_policy_are_not_payload_keys():
    source, rows = owned_rows()
    changed = source.manifest.model_dump(mode="json")
    changed["source_scope"] = "rrc01"
    other = RisReference(manifest=SourceManifest.model_validate(changed))
    assert other.occurrence_id(0, 0) != rows[0].logical_record_id
    changed = source.manifest.model_dump(mode="json")
    changed["artifacts"][0]["sha256"] = "b" * 64
    other = RisReference(manifest=SourceManifest.model_validate(changed))
    assert other.occurrence_id(0, 0) != rows[0].logical_record_id
    changed["parser_version"] = "unapproved-next"
    with pytest.raises(ValueError):
        RisReference(manifest=SourceManifest.model_validate(changed))


@pytest.mark.parametrize(
    "field,value",
    [
        ("logical_record_id", "f" * 64),
        ("artifact_sha256", "f" * 64),
        ("provenance_manifest_id", "f" * 64),
        ("timestamp_precision", "microsecond"),
        ("prefix", "::/0"),
        ("peer_ip", "not-ip"),
        ("afi", True),
        ("safi", "1"),
        ("all_record_index", 2),
    ],
)
def test_wrong_identity_time_or_family_rejected(field, value):
    source, rows = owned_rows()
    with pytest.raises(ValueError):
        row = type(rows[0]).model_validate(rows[0].model_dump() | {field: value})
        row.validate_reference(source)


@pytest.mark.parametrize(
    "wire", [b"\x00", pack("!IHHI", 1, 16, 4, 2) + b"\x00", pack("!IHHI", 1, 16, 4, 1024**2 + 1)]
)
def test_invalid_framing_never_resynchronizes(wire):
    with pytest.raises(FramingError):
        list(frame_mrt(io.BytesIO(wire)))


@pytest.mark.parametrize("name", ["unsupported_hex", "malformed_bgp_body_hex"])
def test_body_exclusion_does_not_shift_next_record(name):
    bad, good = bytes.fromhex(FIXTURE[name]), bytes.fromhex(FIXTURE["one_update_hex"])
    frames = list(frame_mrt(io.BytesIO(bad + good)))
    assert frames[1].byte_offset == len(bad) and frames[1].all_record_index == 1
    assert decode_frame(frames[0], mrtparse)[1] is not None
    assert decode_frame(frames[1], mrtparse)[1] is None


def test_extended_timestamp_preserved():
    ordinary = next(frame_mrt(io.BytesIO(bytes.fromhex(FIXTURE["one_update_hex"]))))
    wire = pack("!IHHI", 1, 17, 4, ordinary.body_length + 4) + pack("!I", 500000) + ordinary.body
    frame = next(frame_mrt(io.BytesIO(wire)))
    record, error = decode_frame(frame, mrtparse)
    assert error is None
    row = next(event_rows(frame, record, reference()))
    row.validate_reference(reference())
    assert row.source_microseconds == 500000 and row.timestamp_precision == "microsecond"
    assert row.evidence(reference()).envelope.tagged_time_descriptor.value.microsecond == 500000


@pytest.mark.parametrize("topic", ["config.snapshots", "temporal.rpki.source.v1", "bad/../path"])
def test_topic_fail_closed(topic):
    with pytest.raises(ValueError):
        validate_topic(topic)


def test_chunk_schema_and_provenance():
    source, rows = owned_rows()
    chunk = EventChunk(artifact_sha256=rows[0].artifact_sha256, first_event=0, rows=rows)
    EventChunk.model_validate_json(chunk.model_dump_json()).validate_reference(source)
    with pytest.raises(ValueError):
        EventChunk.model_validate(chunk.model_dump() | {"transport_version": "2.0"})
    with pytest.raises(ValueError):
        EventChunk.model_validate(chunk.model_dump() | {"invented": True})


def test_local_conditional_commit_and_isolation(tmp_path):
    store = EventStore(root=tmp_path)
    assert store.inventory("owned") == []
    path = Path(store.path("owned/00000000000000000000"))
    path.mkdir(parents=True)
    (path / "uncommitted").write_text("owned")
    assert store.inventory("owned") == []
    commit = dict(format_version="ris-1.0", query_id="owned", batch_id=0, path=str(path))
    store.put("batches/owned/00000000000000000000", commit)
    store.put("batches/owned/00000000000000000000", commit)
    assert store.committed_path(store.inventory("owned")[0], "owned") == str(path)
    with pytest.raises(ValueError):
        store.put("batches/owned/00000000000000000000", commit | {"batch_id": 1})
    with pytest.raises(ValueError):
        store.committed_path(commit | {"path": "../rpki"}, "owned")
    assert not (tmp_path / "_rpki_commits").exists()
    assert (
        sha256(b"[]").hexdigest()
        == "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945"
    )


@pytest.mark.parametrize("case", ["missing", "wrong_hash", "truncated_gzip"])
def test_source_integrity_and_gzip_fail_closed(tmp_path, case):
    from schemas.temporal.provenance import Artifact, verify_artifact_file
    from workloads.ris_reference import frames

    wire = gzip.compress(bytes.fromhex(FIXTURE["one_update_hex"]), mtime=0)
    path = tmp_path / "source.gz"
    path.write_bytes(wire[:-4] if case == "truncated_gzip" else wire)
    artifact = Artifact(
        sha256="0" * 64 if case == "wrong_hash" else sha256(wire).hexdigest(),
        safe_reference="fixture:phase18/owned",
        revision="owned-v1",
        bytes=len(wire),
    )
    if case == "missing":
        path = tmp_path / "missing.gz"
    with pytest.raises((OSError, EOFError, ValueError)):
        if case == "truncated_gzip":
            list(frames(path))
        else:
            verify_artifact_file(path, artifact)


def test_progress_report_excludes_runtime_uuid_and_payload():
    from uuid import uuid4

    from benchmarks.ris_service import progress_snapshot

    progress = dict(
        id=uuid4(),
        batchId=2,
        numInputRows=5,
        durationMs={"triggerExecution": 100},
        sources=[dict(startOffset='{"topic":{"0":0}}', endOffset='{"topic":{"0":2}}')],
    )
    result = progress_snapshot(progress)
    assert json.loads(json.dumps(result))["batch_id"] == 2
    assert "id" not in result
