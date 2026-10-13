import hashlib
from datetime import date

import pyarrow as arrow
import pyarrow.parquet as parquet
import pytest

from processing.dns import normalize_row, qualify_pair, read_source
from schemas.dns import CANONICAL, COUNTING, PARSER, DnsReference
from schemas.temporal.provenance import Artifact, SourceManifest
from schemas.temporal.records import DnsMeasurement, Envelope
from workloads.dns_oracle import build, reference_row


def owned_source(tmp_path):
    rows = []
    for index, (query, response, status, address) in enumerate(
        [
            ("A", "A", 0, "192.0.2.1"),
            ("AFSDB", "A", 0, "192.0.2.1"),
            ("MX", "MX", 0, None),
            ("A", None, 65535, None),
            ("A", None, 3, None),
            ("A", None, 65533, None),
        ]
    ):
        rows.append(
            dict(
                query_name="EXAMPLE.TEST.",
                query_type=query,
                response_name="example.test." if response else None,
                response_type=response,
                timestamp=1704067200000 + index * 1000,
                worker_id=7,
                status_code=status,
                ip4_address=address,
                ip6_address=None,
                response_ttl=60,
                rtt=0.25,
                section="ANSWER",
                extended_error=None,
            )
        )
    path = tmp_path / "owned.parquet"
    parquet.write_table(arrow.Table.from_pylist(rows), path, row_group_size=2)
    artifact = Artifact(
        sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        bytes=path.stat().st_size,
        safe_reference="fixture:phase19/owned.parquet",
        revision="owned-v1",
    )
    source = DnsReference(
        partition_date=date(2024, 1, 1),
        row_groups=(2, 2, 2),
        manifest=SourceManifest(
            manifest_version="1.0",
            source_id="owned-fixture",
            dataset_id="dns-owned",
            source_scope="owned:zonefile",
            artifacts=(artifact,),
            parser_version=PARSER,
            canonicalization_version=CANONICAL,
            counting_policy_version=COUNTING,
            selection_manifest_id="a" * 64,
            source_time_coverage={
                "kind": "unknown",
                "role": "observation",
                "reason": "fixture bounds separate",
            },
            declared_observation_coverage="owned selected object; entity coverage unknown",
            license_terms_reference="fixture:owned",
            expected_records=6,
            accepted_records=6,
            excluded_records=0,
            integrity_verified=True,
            accounting_complete=True,
        ),
    )
    return path, source, rows


def test_original_coordinates_oracle_and_typed_version(tmp_path):
    path, source, rows = owned_source(tmp_path)
    actual = list(read_source(path, source, batch_size=1))
    assert [(row.row_group, row.row) for row in actual] == [
        (0, 0),
        (0, 1),
        (1, 0),
        (1, 1),
        (2, 0),
        (2, 1),
    ]
    assert len({row.logical_record_id for row in actual}) == 6
    assert actual == list(read_source(path, source, batch_size=3))
    for index, row in enumerate(actual):
        expected = reference_row(rows[index], source, index // 2, index % 2)
        assert row.model_dump() == expected
        evidence = row.evidence(source)
        evidence.validate_manifest(source.manifest)
        assert evidence.envelope.payload_schema_version == "1.1"
        assert evidence.payload.entity_coverage == "unknown"
        with pytest.raises(ValueError):
            Envelope.model_validate(evidence.envelope.model_dump())
    assert actual[1].query_type == "AFSDB" and actual[1].address_value == "192.0.2.1"
    assert [row.outcome for row in actual] == [
        "success",
        "success",
        "success",
        "timeout",
        "failure",
        "unknown",
    ]
    assert actual[0].time_descriptor().precision == "second"
    assert actual[0].rtt_hex == 0.25.hex()


def test_independent_group_oracle_owned(tmp_path):
    path, source, rows = owned_source(tmp_path)
    summary = build(path, source, tmp_path / "oracle")
    assert summary["source_rows"] == 6 and summary["selected_address_rows"] == 2
    assert summary["address_groups"] == 2 and summary["real_proven_empty_rows"] == 0
    assert summary["status_codes"] == {"0": 3, "65535": 1, "3": 1, "65533": 1}


@pytest.mark.parametrize(
    "change", ["wrong_hash", "bad_coordinate", "unknown_time", "precision", "false_empty"]
)
def test_source_negative_states(tmp_path, change):
    path, source, rows = owned_source(tmp_path)
    row = normalize_row(rows[0], source, 0, 0)
    if change == "wrong_hash":
        path.write_bytes(b"owned corrupt source")
        with pytest.raises(ValueError):
            list(read_source(path, source))
    elif change == "bad_coordinate":
        with pytest.raises(ValueError):
            row.model_copy(update={"row": 2}).validate_reference(source)
    elif change == "unknown_time":
        unknown = normalize_row(rows[0] | {"timestamp": None}, source, 0, 0)
        assert unknown.logical_record_id == row.logical_record_id
        assert unknown.time_descriptor().kind == "unknown"
    elif change == "precision":
        with pytest.raises(ValueError):
            type(row).model_validate(row.model_dump() | {"timestamp_precision": "millisecond"})
    else:
        missing = normalize_row(rows[0] | {"ip4_address": None}, source, 0, 0)
        assert missing.disposition == "unavailable" and missing.address_value is None


def observation(**values):
    return DnsMeasurement(
        kind="dns_measurement",
        partition_date=values.pop("partition_date", date(2024, 1, 1)),
        query_name="example.test",
        query_type="A",
        response_name="example.test",
        response_type="A",
        status=values.pop("status", "nonempty"),
        values=values.pop("values", ("192.0.2.1",)),
        empty_evidence_reference=values.pop("empty_evidence_reference", None),
        worker_context=values.pop("worker_context", "owned-worker-7"),
        collection_method=values.pop("collection_method", "owned-method"),
        comparison_policy="owned-observed-v1",
        entity_coverage=values.pop("entity_coverage", "qualified"),
        **values,
    )


@pytest.mark.parametrize(
    "scenario",
    [
        "missing_partition",
        "missing_chunk",
        "missing_measurement",
        "timeout",
        "failure",
        "nonempty",
        "proven_empty",
        "false_empty",
        "changed_worker",
        "unknown_worker",
        "multiple_workers",
        "method_changed",
        "equal_values",
        "different_values",
        "unverified_vantage",
    ],
)
def test_e4_negative_controls(scenario):
    old = observation()
    new = observation(partition_date=date(2024, 1, 2))
    complete = True
    if scenario in {"missing_partition", "missing_chunk"}:
        complete = False
    elif scenario == "missing_measurement":
        new = None
    elif scenario in {"timeout", "failure"}:
        new = observation(
            partition_date=date(2024, 1, 2), status=scenario, values=(), entity_coverage="unknown"
        )
    elif scenario == "proven_empty":
        new = observation(
            partition_date=date(2024, 1, 2),
            status="proven_empty",
            values=(),
            empty_evidence_reference="owned-complete-query-nodata",
        )
    elif scenario == "false_empty":
        with pytest.raises(ValueError):
            observation(status="proven_empty", values=())
        return
    elif scenario == "changed_worker":
        new = observation(partition_date=date(2024, 1, 2), worker_context="owned-worker-8")
    elif scenario == "unknown_worker":
        new = observation(partition_date=date(2024, 1, 2), worker_context=None)
    elif scenario == "multiple_workers":
        old = observation(worker_context="owned-workers-7-8")
        new = observation(partition_date=date(2024, 1, 2), worker_context="owned-workers-7-8")
    elif scenario == "method_changed":
        new = observation(
            partition_date=date(2024, 1, 2), collection_method="different-owned-method"
        )
    elif scenario == "different_values":
        new = observation(partition_date=date(2024, 1, 2), values=("192.0.2.2",))
    if scenario == "method_changed":
        with pytest.raises(ValueError):
            qualify_pair(old, new, sources_complete=complete, vantage_verified=True)
        return
    status, diff = qualify_pair(
        old, new, sources_complete=complete, vantage_verified=scenario != "unverified_vantage"
    )
    if scenario in {
        "nonempty",
        "proven_empty",
        "equal_values",
        "different_values",
        "multiple_workers",
    }:
        assert status == "qualified_observed_difference"
        assert isinstance(diff["removed_values"], frozenset)
    else:
        assert diff is None and status != "qualified_observed_difference"
    if new is None:
        unsafe_removed = set(old.values)
        assert unsafe_removed and diff is None
