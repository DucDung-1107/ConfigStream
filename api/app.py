import logging
from contextlib import asynccontextmanager
from time import perf_counter

from fastapi import FastAPI, Request

from api.config import ApiConfig
from api.errors import register_errors
from api.models import ErrorResponse
from api.routes import documents, health, stats
from api.service import SearchService
from search.client import create_client

LOGGER = logging.getLogger(__name__)


def create_app(config: ApiConfig | None = None, *, service: SearchService | None = None) -> FastAPI:
    config = config or ApiConfig.from_env()
    for name in ("elastic_transport", "elasticsearch"):
        logger = logging.getLogger(name)
        if not logger.handlers:
            logger.addHandler(logging.NullHandler())
        logger.propagate = False

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        client = None if service is not None else create_client(config.search)
        application.state.search_service = service or SearchService(client, config.search)
        try:
            yield
        finally:
            if client is not None:
                client.close()

    app = FastAPI(
        title="ConfigStream metadata API",
        version="1.0.0",
        description="Read-only search metadata. Configuration bodies are never served.",
        lifespan=lifespan,
        docs_url="/docs" if config.docs else None,
        redoc_url="/redoc" if config.docs else None,
        openapi_url="/openapi.json" if config.docs else None,
        responses={status: {"model": ErrorResponse} for status in (400, 404, 422, 502, 503, 504)},
    )
    register_errors(app)
    for router in (health.router, documents.router, stats.router):
        app.include_router(router)

    @app.middleware("http")
    async def request_log(request: Request, call_next):
        started = perf_counter()
        response = await call_next(request)
        route = request.scope.get("route")
        LOGGER.info(
            "HTTP method=%s route=%s status=%s elapsed_ms=%.1f",
            request.method,
            getattr(route, "path", "unmatched"),
            response.status_code,
            (perf_counter() - started) * 1000,
        )
        return response

    return app
