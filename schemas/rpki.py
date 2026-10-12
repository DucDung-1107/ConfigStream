"""RPKI source transport granularity; original temporal evidence contracts stay intact."""

from datetime import date
from hashlib import sha256
from typing import Literal, Self

from pydantic import Field, field_validator, model_validator

from schemas.temporal.base import Contract, Digest
from schemas.temporal.identity import canonical_bytes, logical_identity
from schemas.temporal.provenance import SourceManifest
from schemas.temporal.records import Authorization, Evidence
from schemas.temporal.time import ObservationDate

PARSER = "rpki-csv-v1"
CANONICALIZATION = "membership-v1"
COUNTING = "csv-data-rows-v1"
COMPARISON = "consecutive-scope-set-v1"
REGIME = "ripe-archive-validated-vrps-window-v1"
ROW_POLICY = logical_identity(
    "rpki_row_policy", "v1", (PARSER, CANONICALIZATION, COUNTING, "observation-bound-row-v1")
)
TOPIC = "temporal.rpki.source.v1"


def validate_topic(topic: str):
    from uuid import UUID

    if topic == TOPIC:
        return
    prefix = "temporal.rpki.fixture."
    if not topic.startswith(prefix) or str(UUID(topic[len(prefix) :])) != topic[len(prefix) :]:
        raise ValueError("Unregistered RPKI topic")


class DayReference(Contract):
    manifest: SourceManifest
    observation: Evidence

    @model_validator(mode="after")
    def binding(self) -> Self:
        self.observation.validate_manifest(self.manifest)
        if self.observation.envelope.record_kind != "rpki_observation":
            raise ValueError("RPKI observation reference required")
        if self.observation.payload.completion != "complete":
            raise ValueError("Oracle reference must describe complete input")
        if len(self.manifest.artifacts) != 1 or (
            self.manifest.parser_version,
            self.manifest.canonicalization_version,
            self.manifest.counting_policy_version,
        ) != (PARSER, CANONICALIZATION, COUNTING):
            raise ValueError("Unsupported RPKI source policy")
        return self

    @property
    def observation_id(self):
        return self.observation.envelope.logical_record_id


class SourceRow(Contract):
    row_index: int = Field(strict=True, ge=1, le=100_000_000)
    row_id: Digest
    member: Authorization

    def validate_source(self, reference: DayReference):
        scope = reference.manifest.source_scope
        if self.member.scope != scope or self.row_id != row_identity(reference, self.row_index):
            raise ValueError("Source row coordinate/identity mismatch")


def row_identity(reference: DayReference, index: int) -> str:
    return logical_identity(
        "rpki_source_row",
        ROW_POLICY,
        (
            reference.manifest.source_id,
            reference.manifest.source_scope,
            reference.observation_id,
            reference.manifest.artifacts[0].sha256,
            index,
        ),
    )


def member_encoding(member: Authorization) -> bytes:
    return canonical_bytes([member.scope, member.asn, member.prefix, member.max_length])


class SourceChunk(Contract):
    transport_version: Literal["rpki-source-1.0"] = "rpki-source-1.0"
    observation_id: Digest
    provenance_id: Digest
    artifact_sha256: Digest
    observation_date: date
    chunk_id: Digest
    first_row: int = Field(strict=True, ge=1)
    rows: tuple[SourceRow, ...] = Field(min_length=1, max_length=512)

    @field_validator("observation_date", mode="before")
    @classmethod
    def explicit_day(cls, value):
        return ObservationDate(value=value).value

    def validate_source(self, reference: DayReference):
        if (
            self.observation_id != reference.observation_id
            or self.provenance_id != reference.manifest.manifest_id
            or self.artifact_sha256 != reference.manifest.artifacts[0].sha256
            or self.observation_date != reference.observation.payload.observation_date
        ):
            raise ValueError("Chunk provenance mismatch")
        if (
            tuple(row.row_index for row in self.rows)
            != tuple(range(self.first_row, self.first_row + len(self.rows)))
            or self.first_row + len(self.rows) - 1 > reference.manifest.expected_records
        ):
            raise ValueError("Chunk row inventory mismatch")
        for row in self.rows:
            row.validate_source(reference)
        if self.chunk_id != chunk_identity(reference, self.first_row, self.rows):
            raise ValueError("Chunk identity mismatch")


def chunk_identity(reference: DayReference, first: int, rows: tuple[SourceRow, ...]):
    digest = sha256(canonical_bytes([row.model_dump(mode="json") for row in rows])).hexdigest()
    return logical_identity("rpki_chunk", ROW_POLICY, (reference.observation_id, first, digest))


def parse_chunk(value: str, references: dict[str, DayReference]) -> SourceChunk:
    if len(value.encode("utf-8")) > 1024**2:
        raise ValueError("RPKI chunk transport budget exceeded")
    chunk = SourceChunk.model_validate_json(value)
    reference = references.get(chunk.observation_id)
    if reference is None:
        raise ValueError("Unknown observation source")
    chunk.validate_source(reference)
    return chunk
