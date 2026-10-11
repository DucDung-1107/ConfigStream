import re
from datetime import UTC, datetime, timedelta
from typing import Annotated

from fastapi import Depends, Query, Request
from pydantic import AfterValidator, AwareDatetime, BaseModel, BeforeValidator, ConfigDict, Field

from api.errors import ApiError
from api.pagination import MAX_CURSOR_LENGTH
from api.service import SearchService
from schemas.alert import Severity
from schemas.base import NonBlankString, as_utc

Identifier = Annotated[NonBlankString, Field(max_length=1024)]


def iso_timestamp(value):
    if not isinstance(value, str) or not re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})", value
    ):
        raise ValueError("Timezone-aware ISO 8601 timestamp required")
    return value


Timestamp = Annotated[AwareDatetime, BeforeValidator(iso_timestamp), AfterValidator(as_utc)]


class Filters(BaseModel):
    model_config = ConfigDict(extra="forbid")
    asset_id: Identifier | None = None
    start: Timestamp | None = None
    end: Timestamp | None = None

    def search_filters(self) -> dict:
        if self.start and self.end and self.start > self.end:
            raise ApiError(422, "invalid_request", "Start must not follow end")
        return self.model_dump(mode="json", exclude_none=True)


class ListFilters(Filters):
    limit: int = Field(default=50, ge=1, le=200)
    cursor: str | None = Field(default=None, max_length=MAX_CURSOR_LENGTH)

    def search_filters(self) -> dict:
        return {
            name: value
            for name, value in super().search_filters().items()
            if name not in {"limit", "cursor"}
        }


class AlertFilters(ListFilters):
    severity: Severity | None = None
    rule_id: Identifier | None = None


class DiagnosticFilters(ListFilters):
    type: Identifier | None = None

    def search_filters(self) -> dict:
        values = super().search_filters()
        if "type" in values:
            values["diagnostic_type"] = values.pop("type")
        return values


class DiffStatsFilters(Filters):
    def search_filters(self) -> dict:
        end = self.end or datetime.now(UTC)
        start = self.start or end - timedelta(hours=24)
        if start > end or end - start > timedelta(days=31):
            raise ApiError(422, "invalid_request", "Diff stats require a range of at most 31 days")
        return {**super().search_filters(), "start": start.isoformat(), "end": end.isoformat()}


def get_service(request: Request) -> SearchService:
    return request.app.state.search_service


Service = Annotated[SearchService, Depends(get_service)]
ListQuery = Annotated[ListFilters, Query()]
