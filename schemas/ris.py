"""RIS physical projection v1; existing temporal identity and payload remain authoritative."""

import re
from datetime import UTC, datetime, timedelta
from functools import cached_property
from typing import Literal

from pydantic import Field, field_validator, model_validator

from schemas.temporal.base import Contract, Count, Digest
from schemas.temporal.identity import logical_identity
from schemas.temporal.provenance import SourceManifest
from schemas.temporal.records import Evidence, RisOccurrence

PARSER = "mrtparse-2.2.0"
CANONICAL = "ris-wire-v1"
COUNTING = "feasibility-v1"


def validate_topic(topic):
    if not re.fullmatch(r"temporal\.ris\.source\.v1(?:\.[a-z0-9-]{1,64})?", topic):
        raise ValueError("Unregistered RIS topic")


class RisReference(Contract):
    manifest: SourceManifest

    @model_validator(mode="after")
    def binding(self):
        source = self.manifest
        if len(source.artifacts) != 1 or (
            source.parser_version,
            source.canonicalization_version,
            source.counting_policy_version,
        ) != (PARSER, CANONICAL, COUNTING):
            raise ValueError("RIS reference policy/artifact mismatch")
        if not source.integrity_verified or not source.accounting_complete:
            raise ValueError("Verified complete source framing ledger required")
        return self

    @cached_property
    def policy(self):
        source = self.manifest
        return logical_identity(
            "policy_bindings",
            "v1",
            (
                source.parser_version,
                source.canonicalization_version,
                source.counting_policy_version,
            ),
        )

    def occurrence_id(self, offset, ordinal):
        source = self.manifest
        return logical_identity(
            "ris_occurrence",
            self.policy,
            (
                source.source_id,
                source.source_scope,
                source.artifacts[0].sha256,
                offset,
                ordinal,
            ),
        )


class EventRow(Contract):
    logical_record_id: Digest
    artifact_sha256: Digest
    provenance_manifest_id: Digest
    byte_offset: Count
    all_record_index: Count
    expansion_ordinal: Count
    source_seconds: int = Field(strict=True, ge=0, le=2**32 - 1)
    source_microseconds: int | None = Field(strict=True, ge=0, le=999999)
    timestamp_precision: Literal["second", "microsecond"]
    peer_asn: int = Field(strict=True, ge=0, le=2**32 - 1)
    peer_ip: str = Field(max_length=45)
    action: Literal["announcement", "withdrawal"]
    afi: Literal[1, 2]
    safi: Literal[1]
    prefix: str = Field(max_length=64)
    path_id: int | None = Field(strict=True, ge=0, le=2**32 - 1)
    attribute_digest: Digest
    content_fingerprint: Digest

    @field_validator("afi", "safi", mode="before")
    @classmethod
    def strict_family(cls, value):
        if type(value) is not int:
            raise ValueError("RIS AFI/SAFI must be explicit integers")
        return value

    def validate_reference(self, reference):
        source = reference.manifest
        if (
            self.artifact_sha256 != source.artifacts[0].sha256
            or self.all_record_index >= source.expected_records
            or self.provenance_manifest_id != source.manifest_id
            or self.logical_record_id
            != reference.occurrence_id(self.byte_offset, self.expansion_ordinal)
            or self.timestamp_precision
            != ("second" if self.source_microseconds is None else "microsecond")
        ):
            raise ValueError("RIS identity/provenance/time mismatch")
        # The original typed payload validates canonical IP families without changing its schema.
        payload = RisOccurrence(
            collector=source.source_scope,
            peer_asn=self.peer_asn,
            peer_ip=self.peer_ip,
            action=self.action,
            prefix=self.prefix,
            afi=self.afi,
            safi=self.safi,
            path_id=self.path_id,
            attribute_digest=self.attribute_digest,
            acceptance="accepted",
        )
        if (payload.prefix, payload.peer_ip) != (self.prefix, self.peer_ip):
            raise ValueError("RIS source addresses must already be canonical")
        if self.evidence(reference).envelope.logical_record_id != self.logical_record_id:
            raise ValueError("RIS evidence mismatch")

    def evidence(self, reference):
        micros = self.source_seconds * 1000000 + (self.source_microseconds or 0)
        original = str(self.source_seconds if self.source_microseconds is None else micros)
        source = reference.manifest
        return Evidence.model_validate(
            {
                "envelope": {
                    "envelope_version": "1.0",
                    "payload_schema_version": "1.0",
                    "record_kind": "ris_occurrence",
                    "source_id": source.source_id,
                    "dataset_id": source.dataset_id,
                    "source_scope": source.source_scope,
                    "logical_record_id": self.logical_record_id,
                    "provenance_manifest_id": self.provenance_manifest_id,
                    "source_coordinate": {
                        "kind": "mrt",
                        "artifact_sha256": self.artifact_sha256,
                        "byte_offset": self.byte_offset,
                        "all_record_index": self.all_record_index,
                        "expansion_ordinal": self.expansion_ordinal,
                    },
                    "tagged_time_descriptor": {
                        "kind": "point",
                        "role": "source",
                        "original": original,
                        "value": datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=micros),
                        "precision": self.timestamp_precision,
                        "encoding": "epoch_second"
                        if self.source_microseconds is None
                        else "epoch_microsecond",
                    },
                    "quality_status": "accepted",
                },
                "payload": {
                    "kind": "ris_occurrence",
                    "collector": source.source_scope,
                    "peer_asn": self.peer_asn,
                    "peer_ip": self.peer_ip,
                    "action": self.action,
                    "prefix": self.prefix,
                    "afi": self.afi,
                    "safi": self.safi,
                    "path_id": self.path_id,
                    "attribute_digest": self.attribute_digest,
                    "acceptance": "accepted",
                },
            }
        )


class EventChunk(Contract):
    transport_version: Literal["ris-source-1.0"] = "ris-source-1.0"
    artifact_sha256: Digest
    first_event: Count
    rows: tuple[EventRow, ...] = Field(min_length=1, max_length=256)

    def validate_reference(self, reference):
        if self.artifact_sha256 != reference.manifest.artifacts[0].sha256:
            raise ValueError("Chunk artifact mismatch")
        for row in self.rows:
            row.validate_reference(reference)
