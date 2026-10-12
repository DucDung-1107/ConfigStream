from typing import Annotated, Literal

from pydantic import Field

from schemas.temporal.base import Contract, Count, Digest


class StateCoordinate(Contract):
    kind: Literal["state"] = "state"
    artifact_sha256: Digest
    observation_date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$", strict=True)


class ParquetCoordinate(Contract):
    kind: Literal["parquet"] = "parquet"
    artifact_sha256: Digest
    row_group: Count
    row: Count


class MrtCoordinate(Contract):
    kind: Literal["mrt"] = "mrt"
    artifact_sha256: Digest
    byte_offset: Count
    all_record_index: Count
    expansion_ordinal: Count


class DerivedCoordinate(Contract):
    kind: Literal["derived"] = "derived"
    previous_id: Digest
    current_id: Digest


Coordinate = Annotated[
    StateCoordinate | ParquetCoordinate | MrtCoordinate | DerivedCoordinate,
    Field(discriminator="kind"),
]
