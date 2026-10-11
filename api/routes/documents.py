from typing import Annotated

from fastapi import APIRouter, Path, Query

from api.dependencies import (
    AlertFilters,
    DiagnosticFilters,
    Identifier,
    ListQuery,
    Service,
)
from api.models import AlertResponse, DiagnosticResponse, DiffResponse, Page, SnapshotResponse

router = APIRouter(prefix="/api/v1", tags=["metadata"])
Identity = Annotated[Identifier, Path()]


@router.get("/snapshots", response_model=Page[SnapshotResponse])
def snapshots(service: Service, query: ListQuery):
    return service.list_documents("snapshots", query.limit, query.cursor, query.search_filters())


@router.get("/snapshots/{snapshot_id:path}", response_model=SnapshotResponse)
def snapshot(snapshot_id: Identity, service: Service):
    return service.get_document("snapshots", snapshot_id)


@router.get("/assets/{asset_id:path}/latest-snapshot", response_model=SnapshotResponse)
def latest_snapshot(asset_id: Identity, service: Service):
    return service.latest_snapshot(asset_id)


@router.get("/diffs", response_model=Page[DiffResponse])
def diffs(service: Service, query: ListQuery):
    return service.list_documents("diffs", query.limit, query.cursor, query.search_filters())


@router.get("/diffs/{diff_id:path}", response_model=DiffResponse)
def diff(diff_id: Identity, service: Service):
    return service.get_document("diffs", diff_id)


@router.get("/alerts", response_model=Page[AlertResponse])
def alerts(service: Service, query: Annotated[AlertFilters, Query()]):
    return service.list_documents("alerts", query.limit, query.cursor, query.search_filters())


@router.get("/alerts/{alert_id:path}", response_model=AlertResponse)
def alert(alert_id: Identity, service: Service):
    return service.get_document("alerts", alert_id)


@router.get("/diagnostics", response_model=Page[DiagnosticResponse])
def diagnostics(service: Service, query: Annotated[DiagnosticFilters, Query()]):
    return service.list_documents("diagnostics", query.limit, query.cursor, query.search_filters())


@router.get("/diagnostics/{diagnostic_id:path}", response_model=DiagnosticResponse)
def diagnostic(diagnostic_id: Identity, service: Service):
    return service.get_document("diagnostics", diagnostic_id)
