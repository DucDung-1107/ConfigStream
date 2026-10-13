"""OpenINTEL source-row projection; explicit additive DNS payload revision 1.1."""

from datetime import UTC, date, datetime, timedelta
from functools import cached_property
from typing import Literal

from pydantic import Field, model_validator

from schemas.temporal.base import Contract, Count, Digest, Text
from schemas.temporal.coordinates import ParquetCoordinate
from schemas.temporal.identity import logical_identity
from schemas.temporal.provenance import SourceManifest
from schemas.temporal.records import Envelope, Evidence
from schemas.temporal.time import KnownTime, UnknownTime

PARSER = "pyarrow-19.0.1"
CANONICAL = "openintel-source-v1"
COUNTING = "feasibility-dns-v1"


class DnsReference(Contract):
    manifest: SourceManifest
    partition_date: date
    row_groups: tuple[Count, ...] = Field(min_length=1, max_length=4096)

    @model_validator(mode="after")
    def verified(self):
        source = self.manifest
        if len(source.artifacts) != 1 or (
            source.parser_version,
            source.canonicalization_version,
            source.counting_policy_version,
        ) != (PARSER, CANONICAL, COUNTING):
            raise ValueError("DNS reference policy mismatch")
        if (
            sum(self.row_groups) != source.expected_records
            or not source.integrity_verified
            or not source.accounting_complete
        ):
            raise ValueError("DNS verified source accounting required")
        return self

    @cached_property
    def policy(self):
        return logical_identity("policy_bindings", "v1", (PARSER, CANONICAL, COUNTING))

    def row_id(self, row_group, row):
        source = self.manifest
        return logical_identity(
            "dns_measurement",
            self.policy,
            (source.source_id, source.source_scope, source.artifacts[0].sha256, row_group, row),
        )


class DnsRow(Contract):
    logical_record_id: Digest
    artifact_sha256: Digest
    provenance_manifest_id: Digest
    row_group: Count
    row: Count
    partition_date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    query_name: Text | None
    query_type: Text | None
    response_name: Text | None
    response_type: Text | None
    timestamp_ms: int | None = Field(strict=True)
    worker_id: int | None = Field(strict=True)
    status_code: int | None = Field(strict=True)
    ip4_address: Text | None
    ip6_address: Text | None
    response_ttl: int | None = Field(strict=True)
    rtt_hex: Text | None
    section: Text | None
    extended_error: Text | None
    source_row_sha256: Digest
    outcome: Literal["success", "timeout", "failure", "unknown"]
    disposition: Literal[
        "selected_address", "non_target", "unsuccessful", "unavailable", "invalid_address"
    ]
    address_value: Text | None
    timestamp_encoding: Literal["epoch_millisecond"] = "epoch_millisecond"
    timestamp_precision: Literal["second"] = "second"
    collection_method: Literal["zonefile"] = "zonefile"

    @model_validator(mode="after")
    def classification(self):
        from ipaddress import ip_address

        expected = (
            "success"
            if self.status_code == 0
            else "timeout"
            if self.status_code == 65535
            else "unknown"
            if self.status_code is None or not 1 <= self.status_code <= 15
            else "failure"
        )
        if self.outcome != expected:
            raise ValueError("DNS source status classification mismatch")
        if self.disposition == "selected_address":
            if (
                self.outcome != "success"
                or self.response_type not in {"A", "AAAA"}
                or self.address_value is None
                or self.query_name is None
                or self.response_name is None
                or self.query_type is None
            ):
                raise ValueError("DNS address observation lacks source evidence")
            address = ip_address(self.address_value)
            if str(address) != self.address_value or address.version != (
                4 if self.response_type == "A" else 6
            ):
                raise ValueError("DNS address family/canonicalization mismatch")
        elif self.address_value is not None:
            raise ValueError("DNS unsuccessful/non-target evidence cannot contain selected values")
        return self

    def validate_reference(self, reference):
        source = reference.manifest
        if (
            self.artifact_sha256 != source.artifacts[0].sha256
            or self.provenance_manifest_id != source.manifest_id
            or self.partition_date != reference.partition_date.isoformat()
            or self.row_group >= len(reference.row_groups)
            or self.row >= reference.row_groups[self.row_group]
            or self.logical_record_id != reference.row_id(self.row_group, self.row)
        ):
            raise ValueError("DNS source-coordinate/provenance mismatch")

    def time_descriptor(self):
        if self.timestamp_ms is None or not 0 <= self.timestamp_ms <= 253402300799999:
            return UnknownTime(
                role="observation",
                reason="source timestamp absent or outside supported epoch range",
            )
        return KnownTime(
            role="observation",
            original=str(self.timestamp_ms),
            value=datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=self.timestamp_ms),
            precision="second",
            encoding="epoch_millisecond",
        )

    def evidence(self, reference):
        self.validate_reference(reference)
        source = reference.manifest
        return DnsEvidence(
            envelope=DnsEnvelope(
                envelope_version="1.0",
                record_kind="dns_measurement",
                payload_schema_version="1.1",
                source_id=source.source_id,
                dataset_id=source.dataset_id,
                source_scope=source.source_scope,
                logical_record_id=self.logical_record_id,
                provenance_manifest_id=source.manifest_id,
                source_coordinate=ParquetCoordinate(
                    artifact_sha256=self.artifact_sha256, row_group=self.row_group, row=self.row
                ),
                tagged_time_descriptor=self.time_descriptor(),
                quality_status="accepted",
            ),
            payload=DnsPayload(measurement=self),
        )


class DnsEnvelope(Envelope):
    record_kind: Literal["dns_measurement"]
    payload_schema_version: Literal["1.1"]


class DnsPayload(Contract):
    kind: Literal["dns_measurement"] = "dns_measurement"
    measurement: DnsRow
    entity_coverage: Literal["unknown"] = "unknown"
    resolver_vantage: Literal["unknown"] = "unknown"


class DnsEvidence(Evidence):
    envelope: DnsEnvelope
    payload: DnsPayload

    @model_validator(mode="after")
    def observation(self):
        row = self.payload.measurement
        coordinate = self.envelope.source_coordinate
        if (
            not isinstance(coordinate, ParquetCoordinate)
            or (coordinate.artifact_sha256, coordinate.row_group, coordinate.row)
            != (row.artifact_sha256, row.row_group, row.row)
            or self.envelope.logical_record_id != row.logical_record_id
            or self.envelope.provenance_manifest_id != row.provenance_manifest_id
            or self.envelope.tagged_time_descriptor != row.time_descriptor()
        ):
            raise ValueError("DNS typed evidence/source row mismatch")
        return self


class DnsChunk(Contract):
    transport_version: Literal["dns-source-1.1"] = "dns-source-1.1"
    artifact_sha256: Digest
    rows: tuple[DnsRow, ...] = Field(min_length=1, max_length=128)

    def validate_reference(self, reference):
        if self.artifact_sha256 != reference.manifest.artifacts[0].sha256:
            raise ValueError("DNS chunk source binding mismatch")
        for row in self.rows:
            row.validate_reference(reference)
