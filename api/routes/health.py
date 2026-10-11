from fastapi import APIRouter

from api.dependencies import Service
from api.models import StatusResponse

router = APIRouter(tags=["health"])


@router.get("/healthz", response_model=StatusResponse, summary="Process liveness")
def health():
    return StatusResponse()


@router.get("/readyz", response_model=StatusResponse, summary="Search readiness")
def ready(service: Service):
    service.ready()
    return StatusResponse()
