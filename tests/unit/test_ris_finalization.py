import pytest

from schemas.ris import EventRow
from spark.ris import reconcile_sorted
from tests.unit.test_ris import owned_rows


def tagged(rows, origin):
    return [(*row.model_dump().values(), origin) for row in rows]


def compare(actual, expected):
    return list(
        reconcile_sorted(sorted(tagged(actual, 0) + tagged(expected, 1), key=lambda row: row[0]))
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("attribute_digest", "f" * 64),
        ("peer_asn", 64515),
        ("peer_ip", "192.0.2.254"),
        ("action", "withdrawal"),
        ("prefix", "198.51.100.0/24"),
        ("source_seconds", 2),
        ("artifact_sha256", "b" * 64),
        ("provenance_manifest_id", "c" * 64),
        ("path_id", 123),
        ("source_microseconds", 123),
        ("timestamp_precision", "microsecond"),
        ("byte_offset", 111),
        ("all_record_index", 12),
        ("expansion_ordinal", 7),
        ("afi", 2),
        ("safi", 2),
        ("content_fingerprint", "f" * 64),
    ],
)
def test_streaming_conflict_full_projection(field, value):
    _, rows = owned_rows()
    changed = rows[0].model_copy(update={field: value})
    assert getattr(rows[0], field) != value
    output = compare([*rows, changed], rows)
    conflicts = [row[0] for row in output if row[-1] == "conflict"]
    assert conflicts == [rows[0].logical_record_id]


def test_streaming_missing_extra_multiplicity_and_retries():
    _, rows = owned_rows()
    extra = rows[0].model_copy(update={"logical_record_id": "f" * 64})
    output = compare([*rows[:-1], *rows[:-1], extra], rows)
    assert {row[0] for row in output if row[-1] == "missing"} == {rows[-1].logical_record_id}
    assert {row[0] for row in output if row[-1] == "unexpected"} == {"f" * 64}
    assert len([row for row in output if row[-1] == "accepted"]) == 3
    full = compare(rows * 10, rows)
    assert len(full) == 4
    fingerprint_index = list(EventRow.model_fields).index("content_fingerprint")
    assert len({row[fingerprint_index] for row in full}) == 2


def test_streaming_group_is_not_buffered_and_order_fails_closed():
    _, rows = owned_rows()
    row = tuple(rows[0].model_dump().values())

    def duplicate_stream():
        for _ in range(100000):
            yield (*row, 0)
        yield (*row, 1)

    assert list(reconcile_sorted(duplicate_stream())) == [(*row, "accepted")]
    with pytest.raises(ValueError, match="sorted"):
        list(reconcile_sorted(sorted(tagged(rows, 0), reverse=True)))


def recovery_policy(tmp_path):
    import json
    from uuid import uuid4

    from benchmarks.ris_service import checkpoint_digest

    query = str(uuid4())
    topic = f"temporal.ris.source.v1.{query}"
    checkpoint = tmp_path / "checkpoint"
    (checkpoint / "commits").mkdir(parents=True)
    (checkpoint / "offsets").mkdir()
    (checkpoint / "commits/0").write_text('v1\n{"nextBatchWatermarkMs":0}\n')
    (checkpoint / "offsets/0").write_text("v1\n{}\n" + json.dumps({topic: {"0": 3}}))
    previous = dict(
        query_id=query,
        topic=topic,
        lake_prefix=f"phase18-closure/{query}",
        source_selection=[f"{index:064x}" for index in range(12)],
        broker_high_watermarks={"0": 3},
        spark_end_offsets={"0": 3},
        final_broker_lag=0,
    )
    policy = dict(
        authorization="OWNER_APPROVED_ONE_FINALIZATION_ONLY",
        query_id=query,
        lake_prefix=previous["lake_prefix"],
        mode="finalization_only",
        source_selection=previous["source_selection"],
        max_seconds=1800,
        checkpoint_sha256=checkpoint_digest(checkpoint),
        inventory_sha256="a" * 64,
    )
    return policy, previous, checkpoint


@pytest.mark.parametrize(
    "damage", ["approval", "prefix", "selection", "offset", "checkpoint", "cap"]
)
def test_recovery_approval_fails_closed(tmp_path, damage):
    from benchmarks.ris_service import approve_recovery, checkpoint_digest

    policy, previous, checkpoint = recovery_policy(tmp_path)
    assert approve_recovery(policy, previous, checkpoint) == 0
    if damage == "approval":
        policy["authorization"] = "PENDING"
    elif damage == "prefix":
        policy["lake_prefix"] = previous["lake_prefix"] = "unapproved"
    elif damage == "selection":
        policy["source_selection"] = previous["source_selection"] = previous["source_selection"][
            :-1
        ]
    elif damage == "offset":
        previous["spark_end_offsets"] = previous["broker_high_watermarks"] = {"0": 2}
    elif damage == "checkpoint":
        (checkpoint / "commits/0").write_text("changed")
    else:
        policy["max_seconds"] = True
    before = checkpoint_digest(checkpoint)
    with pytest.raises(ValueError):
        approve_recovery(policy, previous, checkpoint)
    assert checkpoint_digest(checkpoint) == before
