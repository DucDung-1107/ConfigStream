from typing import Annotated

from fastapi import APIRouter, Query

from api.dependencies import DiffStatsFilters, Filters, Service
from api.models import AlertStatsResponse, DiffStatsResponse

router = APIRouter(prefix="/api/v1/stats", tags=["statistics"])


@router.get("/alerts", response_model=AlertStatsResponse)
def alert_stats(service: Service, query: Annotated[Filters, Query()]):
    return service.alert_stats(query.search_filters())


@router.get("/diffs", response_model=DiffStatsResponse)
def diff_stats(service: Service, query: Annotated[DiffStatsFilters, Query()]):
    return service.diff_stats(query.search_filters())
