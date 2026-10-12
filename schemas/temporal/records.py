from datetime import date
from ipaddress import ip_address, ip_network
from typing import Annotated, Literal, Self

from pydantic import AwareDatetime, Field, TypeAdapter, field_validator, model_validator

from schemas.temporal.base import Contract, Count, Digest, Text, Token
from schemas.temporal.coordinates import (
    Coordinate,
    DerivedCoordinate,
    MrtCoordinate,
    ParquetCoordinate,
    StateCoordinate,
)
from schemas.temporal.identity import logical_identity
from schemas.temporal.provenance import SourceManifest
from schemas.temporal.time import (
    KnownTime,
    ObservationDate,
    TimeDescriptor,
    TimeInterval,
    UnknownTime,
)

RecordKind = Literal["rpki_observation", "rpki_transition", "dns_measurement", "ris_occurrence"]


class Envelope(Contract):
    envelope_version: Literal["1.0"]
    record_kind: RecordKind
    payload_schema_version: Literal["1.0"]
    source_id: Token
    dataset_id: Token
    source_scope: Token
    logical_record_id: Digest
    provenance_manifest_id: Digest
    source_coordinate: Coordinate
    tagged_time_descriptor: TimeDescriptor
    quality_status: Literal["accepted", "excluded", "incomplete", "incomparable"]


class Authorization(Contract):
    scope: Token
    asn: int = Field(strict=True, ge=0, le=2**32 - 1)
    prefix: Text
    max_length: int = Field(strict=True, ge=0, le=128)

    @field_validator("prefix")
    @classmethod
    def network(cls, value):
        return str(ip_network(value, strict=True))

    @model_validator(mode="after")
    def length(self) -> Self:
        network = ip_network(self.prefix)
        if not network.prefixlen <= self.max_length <= network.max_prefixlen:
            raise ValueError("Invalid authorization maxLength")
        return self

    @property
    def membership_id(self) -> str:
        return logical_identity(
            "rpki_membership", "membership-v1", (self.scope, self.asn, self.prefix, self.max_length)
        )


class RpkiObservation(Contract):
    kind: Literal["rpki_observation"] = "rpki_observation"
    observation_date: date
    comparison_policy: Token
    validation_regime: Token
    completion: Literal["complete", "incomplete"]
    expected_records: Count
    accepted_records: Count
    duplicate_records: Count
    unique_members: Count
    excluded_records: Count
    set_digest: Digest

    @model_validator(mode="after")
    def accounting(self) -> Self:
        if self.unique_members + self.duplicate_records != self.accepted_records:
            raise ValueError("Canonical/raw accounting mismatch")
        if self.accepted_records + self.excluded_records > self.expected_records:
            raise ValueError("Accounting exceeds source")
        if self.completion == "complete" and (
            self.excluded_records or self.accepted_records != self.expected_records
        ):
            raise ValueError("Complete RPKI state requires all valid input rows")
        return self


class RpkiTransition(Contract):
    kind: Literal["rpki_transition"] = "rpki_transition"
    action: Literal["add", "remove"]
    member: Authorization
    comparison_policy: Token


class DnsMeasurement(Contract):
    kind: Literal["dns_measurement"] = "dns_measurement"
    partition_date: date
    query_name: Text
    query_type: Literal["A", "AAAA"]
    response_name: Text
    response_type: Literal["A", "AAAA"]
    status: Literal["nonempty", "proven_empty", "timeout", "failure", "unknown", "absent"]
    values: tuple[Text, ...] = Field(max_length=4096)
    empty_evidence_reference: Token | None
    worker_context: Token | None
    collection_method: Token
    comparison_policy: Token
    entity_coverage: Literal["qualified", "unknown"]

    @field_validator("query_name", "response_name")
    @classmethod
    def name(cls, value):
        normalized = value.lower().rstrip(".")
        if not normalized or len(normalized) > 253 or any(char.isspace() for char in normalized):
            raise ValueError("Invalid DNS name")
        return normalized

    @field_validator("values")
    @classmethod
    def addresses(cls, values):
        return tuple(sorted({str(ip_address(value)) for value in values}))

    @model_validator(mode="after")
    def evidence(self) -> Self:
        if self.status == "nonempty":
            if not self.values or self.entity_coverage != "qualified":
                raise ValueError("Nonempty result requires qualified address evidence")
        elif self.values:
            raise ValueError("Only nonempty results may contain address values")
        if self.status == "proven_empty":
            if self.empty_evidence_reference is None or self.entity_coverage != "qualified":
                raise ValueError("Empty response requires independently verified query evidence")
        elif self.empty_evidence_reference is not None:
            raise ValueError("Unexpected empty evidence")
        if self.status in {"unknown", "absent", "timeout", "failure"}:
            if self.entity_coverage != "unknown":
                raise ValueError("Unsuccessful/missing measurement cannot certify entity state")
        family = 4 if self.response_type == "A" else 6
        if any(ip_address(value).version != family for value in self.values):
            raise ValueError("Address family and response type disagree")
        return self


class RisOccurrence(Contract):
    kind: Literal["ris_occurrence"] = "ris_occurrence"
    collector: Token
    peer_asn: int = Field(strict=True, ge=0, le=2**32 - 1)
    peer_ip: Text
    action: Literal["announcement", "withdrawal"]
    prefix: Text
    afi: Literal[1, 2]
    safi: Literal[1]
    path_id: int | None = Field(ge=0, le=2**32 - 1, strict=True)
    attribute_digest: Digest
    acceptance: Literal["accepted"]

    @field_validator("afi", "safi", mode="before")
    @classmethod
    def integer_family(cls, value):
        if type(value) is not int:
            raise ValueError("AFI/SAFI must be explicit integer values")
        return value

    @field_validator("peer_ip")
    @classmethod
    def address(cls, value):
        return str(ip_address(value))

    @field_validator("prefix")
    @classmethod
    def network(cls, value):
        return str(ip_network(value, strict=True))

    @model_validator(mode="after")
    def family(self) -> Self:
        if ip_network(self.prefix).version != (4 if self.afi == 1 else 6):
            raise ValueError("Prefix family and AFI disagree")
        return self


Payload = Annotated[
    RpkiObservation | RpkiTransition | DnsMeasurement | RisOccurrence, Field(discriminator="kind")
]


class Evidence(Contract):
    envelope: Envelope
    payload: Payload

    @model_validator(mode="after")
    def typed_binding(self) -> Self:
        header = self.envelope
        if header.record_kind != self.payload.kind:
            raise ValueError("Envelope/payload kind mismatch")
        coordinate = header.source_coordinate
        expected = {
            "rpki_observation": StateCoordinate,
            "rpki_transition": DerivedCoordinate,
            "dns_measurement": ParquetCoordinate,
            "ris_occurrence": MrtCoordinate,
        }[header.record_kind]
        if type(coordinate) is not expected:
            raise ValueError("Record kind/coordinate mismatch")
        if isinstance(self.payload, RpkiObservation):
            if coordinate.observation_date != self.payload.observation_date.isoformat():
                raise ValueError("State date mismatch")
            if not isinstance(header.tagged_time_descriptor, ObservationDate):
                raise ValueError("Daily RPKI observations require day precision")
            if header.tagged_time_descriptor.value != self.payload.observation_date:
                raise ValueError("State time mismatch")
            if (header.quality_status == "accepted") != (self.payload.completion == "complete"):
                raise ValueError("State quality/completion mismatch")
        elif header.quality_status != "accepted":
            raise ValueError("Nonaccepted records belong in error/completion accounting")
        if (
            isinstance(self.payload, RpkiTransition)
            and self.payload.member.scope != header.source_scope
        ):
            raise ValueError("Transition member scope mismatch")
        if isinstance(self.payload, RpkiTransition) and (
            not isinstance(header.tagged_time_descriptor, TimeInterval)
            or header.tagged_time_descriptor.role != "observation"
            or header.tagged_time_descriptor.precision != "day"
        ):
            raise ValueError("State transition requires an observation-day interval")
        if isinstance(self.payload, RisOccurrence) and (
            not isinstance(header.tagged_time_descriptor, KnownTime)
            or header.tagged_time_descriptor.role != "source"
        ):
            raise ValueError("RIS accepted occurrence requires recorded source time")
        if isinstance(self.payload, DnsMeasurement) and (
            not isinstance(header.tagged_time_descriptor, KnownTime | UnknownTime)
            or header.tagged_time_descriptor.role != "observation"
        ):
            raise ValueError("DNS requires recorded or explicitly unknown observation time")
        if isinstance(self.payload, DnsMeasurement) and self.payload.status == "absent":
            raise ValueError("Absent entity coverage has no raw measurement coordinate")
        if (
            isinstance(self.payload, RisOccurrence)
            and self.payload.collector != header.source_scope
        ):
            raise ValueError("RIS collector and source scope disagree")
        return self

    def validate_manifest(self, manifest: SourceManifest) -> None:
        manifest = SourceManifest.model_validate_json(manifest.model_dump_json(warnings=False))
        header = self.envelope
        if header.provenance_manifest_id != manifest.manifest_id or (
            header.source_id,
            header.dataset_id,
            header.source_scope,
        ) != (manifest.source_id, manifest.dataset_id, manifest.source_scope):
            raise ValueError("Invalid provenance binding")
        coordinate = header.source_coordinate
        if not isinstance(coordinate, DerivedCoordinate):
            if coordinate.artifact_sha256 not in {item.sha256 for item in manifest.artifacts}:
                raise ValueError("Coordinate artifact is not in provenance manifest")
        elif coordinate.previous_id == coordinate.current_id or set(
            manifest.input_observation_ids
        ) != {coordinate.previous_id, coordinate.current_id}:
            raise ValueError("Derived inputs are not bound by provenance manifest")
        if isinstance(self.payload, RpkiObservation):
            if (
                self.payload.expected_records != manifest.expected_records
                or self.payload.accepted_records != manifest.accepted_records
                or self.payload.excluded_records != manifest.excluded_records
                or self.payload.completion == "complete"
                and not manifest.accounting_complete
            ):
                raise ValueError("State descriptor/source accounting mismatch")
        if header.logical_record_id != evidence_identity(self, manifest):
            raise ValueError("Logical identity disagrees with source/policy")


def evidence_identity(record: Evidence, manifest: SourceManifest) -> str:
    header = record.envelope
    coordinate = header.source_coordinate
    components = (header.source_id, header.source_scope)
    policy = logical_identity(
        "policy_bindings",
        "v1",
        (
            manifest.parser_version,
            manifest.canonicalization_version,
            manifest.counting_policy_version,
        ),
    )
    if isinstance(coordinate, MrtCoordinate):
        components += (
            coordinate.artifact_sha256,
            coordinate.byte_offset,
            coordinate.expansion_ordinal,
        )
    elif isinstance(coordinate, ParquetCoordinate):
        components += (coordinate.artifact_sha256, coordinate.row_group, coordinate.row)
    elif isinstance(coordinate, StateCoordinate):
        components += (
            coordinate.artifact_sha256,
            coordinate.observation_date,
            manifest.canonicalization_version,
            record.payload.comparison_policy,
            record.payload.validation_regime,
        )
    else:
        components += (
            coordinate.previous_id,
            coordinate.current_id,
            record.payload.member.membership_id,
            record.payload.action,
            record.payload.comparison_policy,
        )
    return logical_identity(header.record_kind, policy, components)


def parse_evidence(value: str) -> Evidence:
    if len(value.encode("utf-8")) > 1024**2:
        raise ValueError("Evidence size limit exceeded")
    return TypeAdapter(Evidence).validate_json(value)


class Delivery(Contract):
    evidence: Evidence
    ingestion_time: AwareDatetime
