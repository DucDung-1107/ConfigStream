from hashlib import sha256
from pathlib import Path
from typing import Literal, Self
from urllib.parse import urlsplit

from pydantic import Field, field_validator, model_validator

from schemas.temporal.base import Contract, Count, Digest, Text, Token
from schemas.temporal.identity import canonical_bytes
from schemas.temporal.time import TimeDescriptor


class Artifact(Contract):
    sha256: Digest
    safe_reference: Text
    revision: Token
    bytes: Count

    @field_validator("safe_reference")
    @classmethod
    def safe(cls, value):
        if value.startswith("fixture:"):
            if any(part in {"", ".", ".."} for part in value[8:].split("/")):
                raise ValueError("Unsafe fixture reference")
            return value
        parsed = urlsplit(value)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("Artifact reference must be safe HTTPS or owned fixture")
        return value


class SourceManifest(Contract):
    manifest_version: Literal["1.0"]
    source_id: Token
    dataset_id: Token
    source_scope: Token
    artifacts: tuple[Artifact, ...] = Field(min_length=1, max_length=32)
    parser_version: Token
    canonicalization_version: Token
    counting_policy_version: Token
    selection_manifest_id: Digest
    source_time_coverage: TimeDescriptor
    declared_observation_coverage: Text
    license_terms_reference: Text
    expected_records: Count
    accepted_records: Count
    excluded_records: Count
    integrity_verified: bool = Field(strict=True)
    accounting_complete: bool = Field(strict=True)
    input_observation_ids: tuple[Digest, ...] = Field(default=(), max_length=2)

    @model_validator(mode="after")
    def accounting(self) -> Self:
        if len({item.sha256 for item in self.artifacts}) != len(self.artifacts):
            raise ValueError("Duplicate artifact hashes; use URL aliases outside manifest")
        if self.accepted_records + self.excluded_records > self.expected_records:
            raise ValueError("Source accounting exceeds expected records")
        if self.accounting_complete and (
            not self.integrity_verified
            or self.accepted_records + self.excluded_records != self.expected_records
        ):
            raise ValueError("Complete source accounting requires verified artifact and all rows")
        return self

    @property
    def manifest_id(self) -> str:
        return sha256(canonical_bytes(self.model_dump(mode="json"))).hexdigest()


def verify_artifact_file(path: Path, artifact: Artifact, *, max_bytes: int = 64 * 1024**2) -> None:
    if artifact.bytes > max_bytes:
        raise ValueError("Artifact verification budget exceeded")
    checksum = sha256()
    total = 0
    with path.open("rb") as stream:
        while chunk := stream.read(65536):
            total += len(chunk)
            if total > max_bytes:
                raise ValueError("Artifact verification budget exceeded")
            checksum.update(chunk)
    if total != artifact.bytes or checksum.hexdigest() != artifact.sha256:
        raise ValueError("Artifact integrity mismatch")
