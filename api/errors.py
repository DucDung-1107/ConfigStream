import logging

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from starlette.exceptions import HTTPException

from api.models import ErrorDetail, ErrorResponse

LOGGER = logging.getLogger(__name__)


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str):
        self.status, self.code, self.message = status, code, message
        super().__init__(code)


def backend_error(error: Exception) -> ApiError:
    from elasticsearch import ApiError as ElasticsearchError
    from elasticsearch import ConnectionError, ConnectionTimeout

    if isinstance(error, ConnectionTimeout):
        return ApiError(504, "search_timeout", "Search backend timed out")
    if isinstance(error, ConnectionError):
        return ApiError(503, "search_unavailable", "Search backend is unavailable")
    if isinstance(error, ElasticsearchError) and error.status_code in {404, 429, 502, 503}:
        return ApiError(503, "search_unavailable", "Search backend is unavailable")
    if isinstance(error, ElasticsearchError) and error.status_code == 504:
        return ApiError(504, "search_timeout", "Search backend timed out")
    if isinstance(error, (ValidationError, KeyError, TypeError, ValueError)):
        return ApiError(502, "invalid_search_response", "Search response violates serving contract")
    return ApiError(502, "search_failed", "Search request failed")


def register_errors(app) -> None:
    @app.exception_handler(ApiError)
    async def api_error_handler(request: Request, error: ApiError):
        LOGGER.warning("API failure code=%s status=%s", error.code, error.status)
        return JSONResponse(
            status_code=error.status,
            content=ErrorResponse(
                error=ErrorDetail(code=error.code, message=error.message)
            ).model_dump(),
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(request: Request, error: RequestValidationError):
        return await api_error_handler(
            request, ApiError(422, "invalid_request", "Request parameters are invalid")
        )

    @app.exception_handler(HTTPException)
    async def http_error_handler(request: Request, error: HTTPException):
        return await api_error_handler(
            request, ApiError(error.status_code, "http_error", "Request cannot be served")
        )
