import json
from pathlib import Path

import pytest

from workloads.ris_reference import decode_attribute, extract, occurrence_projection


def accounting_fixture(excluded=False):
    from schemas.ris import RisReference
    from schemas.temporal.provenance import SourceManifest
    from tests.unit.test_ris import reference

    source = reference()
    if excluded:
        source = RisReference(
            manifest=SourceManifest.model_validate(
                source.manifest.model_dump() | {"accepted_records": 1, "excluded_records": 1}
            )
        )
    exclusions = (
        [
            dict(
                artifact_sha256=source.manifest.artifacts[0].sha256,
                byte_offset=63,
                all_record_index=1,
                type=16,
                subtype=4,
                category="parser_body_error:owned",
                fully_framed=True,
                disposition="excluded_under_pinned_parser_policy",
                parser_version="mrtparse-2.2.0",
                counting_policy_version="feasibility-v1",
                independent_eligible_prefixes=0,
                independent_attribute_status="decoded",
            )
        ]
        if excluded
        else []
    )
    summary = dict(
        framed_records=2,
        accepted_records=2 - int(excluded),
        update_messages=1,
        excluded_records=int(excluded),
        accepted_occurrences=4,
        framing_sha256="a" * 64,
        source_order_projection_sha256="b" * 64,
        oracle_level=3,
    )
    return source, summary, exclusions


@pytest.mark.parametrize("excluded", [False, True])
def test_committed_accounting_retry_and_conflict(tmp_path, excluded):
    from lake.ris import EventStore, publish_accounting, read_accounting

    source, summary, exclusions = accounting_fixture(excluded)
    store = EventStore(root=tmp_path)
    expected = publish_accounting(store, source, summary, exclusions)
    assert publish_accounting(store, source, summary, exclusions) == expected
    assert read_accounting(store, expected.artifact_sha256) == expected
    with pytest.raises(ValueError, match="conflict"):
        publish_accounting(store, source, summary | {"framing_sha256": "c" * 64}, exclusions)


@pytest.mark.parametrize("damage", ["absent", "corrupt", "missing_source", "wrong_source"])
def test_missing_corrupt_sidecars_fail_closed(tmp_path, damage):
    from lake.ris import EventStore, publish_accounting, read_accounting

    source, summary, exclusions = accounting_fixture(True)
    store = EventStore(root=tmp_path)
    expected = publish_accounting(store, source, summary, exclusions)
    key = f"exclusions/{expected.artifact_sha256}"
    if damage in {"missing_source", "wrong_source"}:
        key = f"sources/{source.manifest.manifest_id}"
    path = tmp_path / store.key(key)
    if damage in {"absent", "missing_source"}:
        path.unlink()
    elif damage == "wrong_source":
        value = source.model_dump(mode="json")
        value["manifest"]["artifacts"][0]["sha256"] = "f" * 64
        path.write_text(json.dumps(value))
    else:
        path.write_text("[]")
    with pytest.raises(ValueError):
        read_accounting(store, expected.artifact_sha256)


def test_window_requires_accounting_and_fixed_inventory(tmp_path):
    from lake.ris import EventStore, publish_accounting, publish_window, read_window

    source, summary, exclusions = accounting_fixture()
    store = EventStore(root=tmp_path)
    artifact = source.manifest.artifacts[0].sha256
    with pytest.raises(ValueError):
        publish_window(store, "owned", [artifact], 4, "a" * 64)
    publish_accounting(store, source, summary, exclusions)
    with pytest.raises(ValueError, match="incomplete"):
        publish_window(store, "owned", [artifact], 3, "a" * 64)
    store.put("batches/owned/00000000000000000000", {"batch_id": 0})
    descriptor = publish_window(store, "owned", [artifact], 4, "a" * 64)
    assert read_window(store, "owned") == descriptor
    path = tmp_path / store.key("batches/owned/00000000000000000000")
    path.unlink()
    with pytest.raises(ValueError, match="inventory"):
        read_window(store, "owned")


@pytest.mark.parametrize(
    "field,value",
    [
        ("framed_records", 3),
        ("accepted_records", 0),
        ("oracle_level", 2),
        ("excluded_records", 1),
        ("parser_version", "next"),
    ],
)
def test_accounting_rejects_invalid_completion(tmp_path, field, value):
    from lake.ris import EventStore, publish_accounting

    source, summary, exclusions = accounting_fixture()
    with pytest.raises(ValueError):
        publish_accounting(EventStore(root=tmp_path), source, summary | {field: value}, exclusions)


def test_independent_reference_has_no_production_imports():
    import ast

    tree = ast.parse(Path("workloads/ris_reference.py").read_text())
    modules = [node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
    assert all(not name.startswith(("processing", "schemas", "mrtparse")) for name in modules)


def test_pathlimit_is_frozen_opaque_not_invented_semantics():
    assert decode_attribute(192, 21, bytes.fromhex("0100000002"), 4, False) == {
        "flag": 192,
        "type": {21: "AS_PATHLIMIT"},
        "length": 5,
        "value": "01 00 00 00 02",
    }


def test_expanded_service_caps_require_explicit_approval():
    from benchmarks.ris_service import approve_execution

    policy = json.loads(Path("workloads/results/phase18-closure/execution-policy.json").read_text())
    approve_execution(policy, 3, 1800)
    with pytest.raises(ValueError, match="approval"):
        approve_execution(policy, 12, 3600)
    with pytest.raises(ValueError, match="approval"):
        approve_execution(policy, 3, 3600)


@pytest.mark.parametrize("version", ["ris-1.0", "ris-1.1"])
def test_legacy_physical_paths_keep_supported_versions(tmp_path, version):
    from lake.ris import EventStore

    store = EventStore(root=tmp_path)
    commit = dict(
        format_version=version,
        query_id="owned",
        batch_id=0,
        path=store.path("owned/00000000000000000000"),
    )
    assert store.committed_path(commit, "owned") == commit["path"]
    with pytest.raises(ValueError):
        store.committed_path(commit | {"format_version": "unregistered"}, "owned")


def test_spark4_progress_uses_json_not_python_offset_repr():
    from benchmarks.ris_service import kafka_offsets, progress_snapshot

    topic = "temporal.ris.source.v1.owned"

    class Progress(dict):
        @property
        def json(self):
            return json.dumps(
                {
                    "batchId": 1,
                    "numInputRows": 2,
                    "durationMs": {"addBatch": 3},
                    "sources": [
                        {
                            "startOffset": {topic: {"0": 0, "1": 1}},
                            "endOffset": {topic: {"0": 0, "1": 3}},
                        }
                    ],
                }
            )

    snapshot = progress_snapshot(Progress())
    assert kafka_offsets(snapshot["sources"][0]["end_offset"], topic) == {"0": 0, "1": 3}
    assert kafka_offsets(json.dumps({topic: {"0": 0}}), topic) == {"0": 0}
    for value in ({"wrong": {"0": 1}}, {topic: {"0": True}}, {topic: {"0": -1}}):
        with pytest.raises(ValueError):
            kafka_offsets(value, topic)


@pytest.mark.parametrize(
    "number,raw,expected",
    [
        (1, "00", {0: "IGP"}),
        (2, "02010000fbf4", [{"type": {2: "AS_SEQUENCE"}, "length": 1, "value": ["64500"]}]),
        (3, "c0000201", "192.0.2.1"),
        (4, "00000064", 100),
        (7, "0000fbf4c0000201", {"as": "64500", "id": "192.0.2.1"}),
        (8, "fbf40001", ["64500:1"]),
        (16, "0000000000000001", [1]),
        (17, "020100010000", [{"type": {2: "AS_SEQUENCE"}, "length": 1, "value": ["65536"]}]),
        (18, "00010000c0000201", {"as": "65536", "id": "192.0.2.1"}),
        (32, "0000fbf40000000100000002", ["64500:1:2"]),
        (35, "0000fbf4", "00 00 fb f4"),
        (255, "aabbcc", "aa bb cc"),
    ],
)
def test_independent_literal_attribute_values(number, raw, expected):
    assert decode_attribute(192, number, bytes.fromhex(raw), 4, False)["value"] == expected


@pytest.mark.parametrize(
    "number,raw",
    [
        (1, ""),
        (1, "0000"),
        (2, "020100"),
        (4, "0001"),
        (8, "0102"),
        (17, "020100"),
        (14, "00020103c0000200"),
        (128, "00000000"),
    ],
)
def test_independent_attribute_invalid_or_unsupported(number, raw):
    with pytest.raises(ValueError):
        decode_attribute(128, number, bytes.fromhex(raw), 4, False)


def test_independent_mp_literal_and_digest():
    from hashlib import sha256
    from struct import unpack

    from tests.unit.test_ris import reference

    fixture = json.loads(Path("tests/fixtures/phase16/mrt-projections.json").read_text())
    wire = bytes.fromhex(fixture["mp_hex"])
    stamp, kind, subtype, size = unpack("!IHHI", wire[:12])
    decoded = extract(kind, subtype, wire[12:], with_attributes=True)
    assert decoded[4][0]["value"] == {
        "afi": {2: "IPv6"},
        "safi": {1: "UNICAST"},
        "next_hop_length": 16,
        "next_hop": ["2001:db8::1"],
        "reserved": 0,
        "nlri": [{"length": 32, "prefix": "2001:db8::"}],
    }
    expected_json = (
        '[{"flag": 128, "length": 26, "type": {"14": "MP_REACH_NLRI"}, '
        '"value": {"afi": {"2": "IPv6"}, "next_hop": ["2001:db8::1"], '
        '"next_hop_length": 16, "nlri": [{"length": 32, "prefix": "2001:db8::"}], '
        '"reserved": 0, "safi": {"1": "UNICAST"}}}, {"flag": 128, "length": 10, '
        '"type": {"15": "MP_UNREACH_NLRI"}, "value": {"afi": {"2": "IPv6"}, '
        '"safi": {"1": "UNICAST"}, "withdrawn_routes": '
        '[{"length": 48, "prefix": "2001:db8:1::"}]}}]'
    )
    rows = list(
        occurrence_projection(reference(), (0, 0, stamp, kind, subtype, wire[12:]), decoded)
    )
    assert len(rows) == 5
    assert rows[0]["attribute_digest"] == sha256(expected_json.encode()).hexdigest()
    assert size == len(wire) - 12
