from typing import Literal, Self

from pydantic import Field, model_validator

from schemas.temporal.base import Contract, Count, Digest, Token


class FrameAudit(Contract):
    artifact_sha256: Digest
    source_scope: Token
    byte_offset: Count
    all_record_index: Count
    record_type: int = Field(strict=True, ge=0, le=65535)
    subtype: int = Field(strict=True, ge=0, le=65535)
    declared_body_bytes: Count
    classification: Literal["parsed", "excluded"]
    error_category: Token | None

    @model_validator(mode="after")
    def error_accounting(self) -> Self:
        if (self.classification == "excluded") != (self.error_category is not None):
            raise ValueError("Excluded framed record requires a safe error category")
        return self
