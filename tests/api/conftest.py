from collections import Counter
from datetime import datetime
from unittest.mock import Mock

import pytest
from elastic_transport import ApiResponseMeta, NodeConfig
from elasticsearch import NotFoundError
from fastapi.testclient import TestClient

from api.app import create_app
from api.config import ApiConfig
from api.service import SearchService
from search.config import SearchConfig
from search.queries import ID_FIELDS


def snapshot_document(index=1):
    return {
        "snapshot_id": f"snapshot-{index}",
        "asset_id": "router",
        "version": index,
        "event_time": "2026-10-11T00:00:00Z",
        "ingest_time": "2026-10-11T00:00:00.001Z",
        "raw_hash": "a" * 64,
        "normalized_hash": "b" * 64,
        "normalization_version": "text-v1",
    }


def document(kind, index=1):
    if kind == "snapshots":
        return snapshot_document(index)
    if kind == "diffs":
        return {
            "schema_version": "1.0",
            "diff_id": f"diff-{index}",
            "asset_id": "router",
            "old_snapshot_id": "snapshot-1",
            "new_snapshot_id": "snapshot-2",
            "added": 1,
            "removed": 1,
            "changed": 1,
            "event_time": "2026-10-11T00:00:00Z",
        }
    if kind == "alerts":
        return {
            "schema_version": "1.0",
            "alert_id": f"alert-{index}",
            "asset_id": "router",
            "snapshot_id": "snapshot-2",
            "rule_id": "logging-disabled",
            "severity": "high",
            "message": "Logging disabled",
            "created_at": "2026-10-11T00:00:00Z",
        }
    return {
        "diagnostic_id": f"diagnostic-{index}",
        "diagnostic_type": "version_conflict",
        "reason": "version_conflict",
        "asset_id": "router",
        "event_time": "2026-10-11T00:00:00Z",
        "version": 2,
    }


def missing_document():
    meta = ApiResponseMeta(404, "1.1", {}, 0, NodeConfig("http", "localhost", 9200))
    return NotFoundError("not found", meta=meta, body={"found": False})


class FakeClient:
    def __init__(self):
        self.documents = {kind: [document(kind)] for kind in ID_FIELDS}
        self.info = Mock(return_value={"version": {"number": "9.5.5"}})
        self.indices = Mock()
        self.indices.exists_alias.return_value = True
        self.calls = []
        self.failure = None
        self.close = Mock()

    def get(self, *, index, id):
        if self.failure:
            raise self.failure
        kind = index.removeprefix("configstream-")
        for item in self.documents[kind]:
            if item[ID_FIELDS[kind]] == id:
                return {"_source": item}
        raise missing_document()

    def search(self, **options):
        self.calls.append(options)
        if self.failure:
            raise self.failure
        kind = options["index"].removeprefix("configstream-")
        items = self.documents[kind]
        for specification in options.get("query", {}).get("bool", {}).get("filter", []):
            if "term" in specification:
                field, value = next(iter(specification["term"].items()))
                items = [item for item in items if item.get(field) == value]
            else:
                field, bounds = next(iter(specification["range"].items()))
                items = [
                    item
                    for item in items
                    if all(
                        datetime.fromisoformat(item[field]) >= datetime.fromisoformat(bound)
                        if operation == "gte"
                        else datetime.fromisoformat(item[field]) <= datetime.fromisoformat(bound)
                        for operation, bound in bounds.items()
                    )
                ]
        if "aggregations" in options:
            specification = options["aggregations"]["counts"]
            if "terms" in specification:
                field = specification["terms"]["field"]
                counts = Counter(item[field] for item in items)
                buckets = [{"key": key, "doc_count": count} for key, count in counts.items()]
            else:
                buckets = [{"key_as_string": "2026-10-11T00:00:00Z", "doc_count": len(items)}]
            return {"aggregations": {"counts": {"buckets": buckets}}}
        first_field = next(iter(options["sort"][0]))

        def sort_values(item):
            value = (
                item[first_field]
                if first_field == "version"
                else int(datetime.fromisoformat(item[first_field]).timestamp() * 1_000_000_000)
            )
            return [value, item[ID_FIELDS[kind]]]

        items = sorted(items, key=lambda item: (-sort_values(item)[0], sort_values(item)[1]))
        if options.get("search_after"):
            after = options["search_after"]
            items = [
                item
                for item in items
                if (-sort_values(item)[0], sort_values(item)[1]) > (-after[0], after[1])
            ]
        return {
            "hits": {
                "hits": [
                    {"_source": item, "sort": sort_values(item), "_index": "private-index"}
                    for item in items[: options["size"]]
                ]
            }
        }


@pytest.fixture
def backend():
    return FakeClient()


@pytest.fixture
def http(backend):
    with TestClient(create_app(service=SearchService(backend, SearchConfig()))) as client:
        yield client


@pytest.fixture
def app_config():
    return ApiConfig()
