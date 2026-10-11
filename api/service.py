import logging
from collections.abc import Callable

from api.errors import ApiError, backend_error
from api.models import (
    RESPONSE_MODELS,
    AlertStatsResponse,
    Bucket,
    DiffStatsResponse,
    Page,
    TimeBucket,
)
from api.pagination import cursor_context, decode_cursor, encode_cursor
from search.config import KINDS, SearchConfig
from search.queries import aggregate, get_document, page_documents, query_documents

LOGGER = logging.getLogger(__name__)


class SearchService:
    def __init__(self, client, config: SearchConfig):
        self.client, self.config = client, config

    def _call(self, operation: Callable):
        try:
            return operation()
        except ApiError:
            raise
        except Exception as error:
            LOGGER.warning("Search operation failed category=%s", type(error).__name__)
            raise backend_error(error) from None

    def ready(self) -> None:
        def check():
            self.client.info()
            for kind in KINDS:
                if not self.client.indices.exists_alias(name=self.config.alias(kind)):
                    raise ApiError(503, "search_unavailable", "Search backend is unavailable")

        try:
            self._call(check)
        except ApiError:
            raise ApiError(503, "search_unavailable", "Search backend is unavailable") from None

    def list_documents(self, kind: str, limit: int, cursor: str | None, filters: dict):
        context = cursor_context(kind, filters)
        after = decode_cursor(cursor, context)

        def query():
            page = page_documents(
                self.client, self.config, kind, limit=limit, after=after, **filters
            )
            items = [RESPONSE_MODELS[kind].model_validate(item) for item in page["items"]]
            return Page[RESPONSE_MODELS[kind]](
                items=items,
                count=len(items),
                has_more=page["has_more"],
                next_cursor=encode_cursor(page["next_sort_values"], context)
                if page["has_more"]
                else None,
            )

        return self._call(query)

    def get_document(self, kind: str, identity: str):
        def query():
            document = get_document(self.client, self.config, kind, identity)
            if document is None:
                raise ApiError(404, "not_found", "Document was not found")
            return RESPONSE_MODELS[kind].model_validate(document)

        return self._call(query)

    def latest_snapshot(self, asset_id: str):
        def query():
            result = query_documents(
                self.client, self.config, "snapshots", asset_id=asset_id, latest=True
            )
            hits = result["hits"]["hits"]
            if not hits:
                raise ApiError(404, "not_found", "Snapshot was not found")
            return RESPONSE_MODELS["snapshots"].model_validate(hits[0]["_source"])

        return self._call(query)

    def alert_stats(self, filters: dict):
        def query():
            buckets = {}
            for group in ("severity", "rule_id"):
                result = aggregate(self.client, self.config, group, **filters)
                buckets[group] = [
                    Bucket(key=item["key"], count=item["doc_count"])
                    for item in result["counts"]["buckets"]
                ]
            return AlertStatsResponse(
                by_severity=buckets["severity"], by_rule_id=buckets["rule_id"]
            )

        return self._call(query)

    def diff_stats(self, filters: dict):
        def query():
            result = aggregate(self.client, self.config, "diffs_per_hour", **filters)
            return DiffStatsResponse(
                buckets=[
                    TimeBucket(time=item["key_as_string"], count=item["doc_count"])
                    for item in result["counts"]["buckets"]
                ]
            )

        return self._call(query)
