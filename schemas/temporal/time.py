from datetime import UTC, date, datetime, timedelta
from typing import Annotated, Literal, Self

from pydantic import AwareDatetime, Field, field_validator, model_validator

from schemas.temporal.base import Contract, Text


class KnownTime(Contract):
    kind: Literal["point"] = "point"
    role: Literal["source", "observation"]
    original: Text
    value: AwareDatetime
    precision: Literal["second", "millisecond", "microsecond"]
    encoding: Literal["iso8601", "epoch_second", "epoch_millisecond", "epoch_microsecond"]

    @field_validator("value", mode="before")
    @classmethod
    def explicit_value(cls, value):
        if not isinstance(value, str | datetime):
            raise ValueError("Time value requires ISO timestamp or aware datetime")
        return value

    @field_validator("value")
    @classmethod
    def utc(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def verify_original(self) -> Self:
        if self.encoding == "iso8601":
            original = datetime.fromisoformat(self.original.replace("Z", "+00:00"))
            if original.tzinfo is None:
                raise ValueError("Original time requires explicit timezone")
        else:
            if not self.original.isascii() or not self.original.isdecimal():
                raise ValueError("Epoch value must be an unsigned integer")
            factor = {"epoch_second": 1000000, "epoch_millisecond": 1000, "epoch_microsecond": 1}
            try:
                original = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(
                    microseconds=int(self.original) * factor[self.encoding]
                )
            except OverflowError as error:
                raise ValueError("Epoch outside supported datetime range") from error
        if original != self.value:
            raise ValueError("Original time and normalized value disagree")
        return self


class ObservationDate(Contract):
    kind: Literal["date"] = "date"
    role: Literal["observation"] = "observation"
    value: date
    precision: Literal["day"] = "day"

    @field_validator("value", mode="before")
    @classmethod
    def explicit_day(cls, value):
        if isinstance(value, datetime) or type(value) not in {date, str}:
            raise ValueError("Observation date requires an explicit calendar date")
        if isinstance(value, str):
            date.fromisoformat(value)
        return value


class TimeInterval(Contract):
    kind: Literal["interval"] = "interval"
    role: Literal["source", "observation"]
    start: AwareDatetime
    end: AwareDatetime
    precision: Literal["second", "day"]

    @field_validator("start", "end")
    @classmethod
    def utc(cls, value):
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def ordered(self) -> Self:
        if self.end < self.start:
            raise ValueError("Invalid time interval")
        return self


class UnknownTime(Contract):
    kind: Literal["unknown"] = "unknown"
    role: Literal["source", "observation"]
    reason: Text


TimeDescriptor = Annotated[
    KnownTime | ObservationDate | TimeInterval | UnknownTime, Field(discriminator="kind")
]
