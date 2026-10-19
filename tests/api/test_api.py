import json
from unittest.mock import patch
from urllib.parse import quote

import pytest
from elasticsearch import ConnectionError, ConnectionTimeout
from fastapi.testclient import TestClient

from api.app import create_app
from api.config import ApiConfig
from api.models import RESPONSE_MODELS
from api.pagination import cursor_context, decode_cursor, encode_cursor
from api.service import SearchService
from search.config import SearchConfig
from tests.api.conftest import document


def test_liveness_readiness_and_missing_alias(http, backend):
    assert http.get("/healthz").json() == {"status": "ok"}
    backend.info.assert_not_called()
    assert http.get("/readyz").status_code == 200
    assert backend.indices.exists_alias.call_count == 4
    backend.indices.exists_alias.return_value = False
    assert http.get("/readyz").status_code == 503
    assert http.get("/healthz").status_code == 200


@pytest.mark.parametrize("kind", list(RESPONSE_MODELS))
def test_lists_lookup_and_not_found(http, backend, kind):
    response = http.get(f"/api/v1/{kind}?asset_id=router&limit=2")
    assert response.status_code == 200
    assert response.json()["count"] == 1
    assert response.json()["has_more"] is False
    assert response.json()["next_cursor"] is None
    identity = backend.documents[kind][0][
        next(name for name in document(kind) if name.endswith("_id"))
    ]
    assert http.get(f"/api/v1/{kind}/{identity}").status_code == 200
    assert http.get(f"/api/v1/{kind}/unknown").status_code == 404
    assert http.get(f"/api/v1/{kind}?asset_id=not-here").json()["items"] == []
    assert "_index" not in response.text


def test_latest_uses_version_not_ingest_order(http, backend):
    backend.documents["snapshots"] = [document("snapshots", index) for index in (1, 3, 2)]
    response = http.get("/api/v1/assets/router/latest-snapshot")
    assert response.json()["version"] == 3
    assert backend.calls[-1]["sort"] == [{"version": "desc"}, {"snapshot_id": "asc"}]
    assert http.get("/api/v1/assets/missing/latest-snapshot").status_code == 404


@pytest.mark.parametrize("kind", list(RESPONSE_MODELS))
def test_exclusive_end_preserves_last_fractional_second(http, backend, kind):
    field = "created_at" if kind == "alerts" else "event_time"
    backend.documents[kind] = [
        {**document(kind, index), field: timestamp}
        for index, timestamp in enumerate(
            ("2026-10-11T23:59:59.999999Z", "2026-10-12T00:00:00Z"), start=1
        )
    ]
    params = {"start": "2026-10-11T00:00:00Z", "end_before": "2026-10-12T00:00:00Z"}
    result = http.get(f"/api/v1/{kind}", params=params)
    assert result.status_code == 200
    assert result.json()["count"] == 1
    assert result.json()["items"][0][field] == "2026-10-11T23:59:59.999999Z"
    legacy = http.get(f"/api/v1/{kind}", params={"end": params["end_before"]})
    assert legacy.json()["count"] == 2
    assert (
        http.get(f"/api/v1/{kind}", params={**params, "end": params["end_before"]}).status_code
        == 422
    )
    assert (
        http.get(f"/api/v1/{kind}", params={**params, "start": params["end_before"]}).status_code
        == 422
    )


def test_opaque_identifiers_preserve_existing_schema_semantics(http, backend):
    asset_id = "site/định danh"
    snapshot_id = "snapshot/one"
    backend.documents["snapshots"][0].update(asset_id=asset_id, snapshot_id=snapshot_id)
    encoded = quote(asset_id, safe="")
    assert http.get(f"/api/v1/assets/{encoded}/latest-snapshot").json()["asset_id"] == asset_id
    assert http.get(f"/api/v1/snapshots/{quote(snapshot_id, safe='')}").status_code == 200


@pytest.mark.parametrize("kind", list(RESPONSE_MODELS))
def test_pagination_identical_timestamps_no_missing_or_duplicate(http, backend, kind):
    backend.documents[kind] = [document(kind, index) for index in range(1, 6)]
    backend.documents[kind][-1]["created_at" if kind == "alerts" else "event_time"] = (
        "2026-10-11T01:00:00Z"
    )
    results = []
    counts = []
    cursor = None
    for _ in range(3):
        params = {"limit": 2}
        if cursor:
            params["cursor"] = cursor
        response = http.get(f"/api/v1/{kind}", params=params)
        assert response.status_code == 200
        page = response.json()
        counts.append(page["count"])
        results.extend(page["items"])
        cursor = page["next_cursor"]
    assert counts == [2, 2, 1]
    assert cursor is None
    assert len({json.dumps(item, sort_keys=True) for item in results}) == 5
    assert page["has_more"] is False


@pytest.mark.parametrize(
    "query",
    [
        "limit=0",
        "limit=201",
        "limit=no",
        "asset_id=",
        "severity=invalid",
        "start=invalid",
        "start=2026-10-11T00:00:00",
        "start=1234567",
        "start=2026-10-12T00:00:00Z&end=2026-10-11T00:00:00Z",
        "dsl=secret",
        "rule_id=" + "a" * 1025,
    ],
)
def test_bad_parameters_are_sanitized(http, query):
    response = http.get("/api/v1/alerts?" + query)
    assert response.status_code == 422
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "message": (
                "Start must not follow end" if "end=" in query else "Request parameters are invalid"
            ),
        }
    }


@pytest.mark.parametrize("cursor", ["!", "e30", "bnVsbA", "a", "YWJj", "a" * 16385])
def test_bad_cursor(http, cursor):
    response = http.get("/api/v1/snapshots", params={"cursor": cursor})
    assert response.status_code == (422 if len(cursor) > 16384 else 400)


def test_cursor_bound_to_filters_and_endpoint(http, backend):
    backend.documents["snapshots"] = [document("snapshots", index) for index in range(1, 4)]
    cursor = http.get("/api/v1/snapshots?limit=1").json()["next_cursor"]
    assert http.get("/api/v1/diffs", params={"cursor": cursor}).status_code == 400
    assert (
        http.get("/api/v1/snapshots", params={"cursor": cursor, "asset_id": "other"}).status_code
        == 400
    )


@pytest.mark.parametrize(
    "error,status,code",
    [
        (ConnectionError("private-url-password"), 503, "search_unavailable"),
        (ConnectionTimeout("private-url-password"), 504, "search_timeout"),
        (RuntimeError("private-url-password"), 502, "search_failed"),
    ],
)
def test_backend_failure_mapping_and_recovery(http, backend, error, status, code):
    backend.failure = error
    backend.info.side_effect = error
    assert http.get("/healthz").status_code == 200
    assert http.get("/readyz").status_code == 503
    response = http.get("/api/v1/snapshots")
    assert response.status_code == status
    assert response.json()["error"]["code"] == code
    assert "private" not in response.text
    backend.failure = None
    backend.info.side_effect = None
    assert http.get("/readyz").status_code == 200
    assert http.get("/api/v1/snapshots").status_code == 200


@pytest.mark.parametrize("kind", list(RESPONSE_MODELS))
@pytest.mark.parametrize("field", ["normalized_content", "content", "value", "details"])
def test_forbidden_fields_fail_closed(http, backend, kind, field):
    backend.documents[kind][0][field] = "SENSITIVE_TEST_MARKER"
    response = http.get(f"/api/v1/{kind}")
    assert response.status_code == 502
    for forbidden in (field, "SENSITIVE_TEST_MARKER", "_index", "_score", "_shards"):
        assert forbidden not in response.text


def test_invalid_source_not_silently_dropped(http, backend):
    backend.documents["snapshots"][0]["version"] = 0
    assert http.get("/api/v1/snapshots").status_code == 502


def test_alert_diagnostic_filters_and_utc(http, backend):
    result = http.get("/api/v1/alerts?severity=high&rule_id=logging-disabled").json()
    assert result["count"] == 1
    assert result["items"][0]["created_at"].endswith("Z")
    assert http.get("/api/v1/alerts?severity=low").json()["count"] == 0
    result = http.get("/api/v1/diagnostics?type=version_conflict")
    assert result.json()["count"] == 1
    response = http.get("/api/v1/snapshots", params={"start": "2026-10-11T07:00:00+07:00"})
    assert response.json()["count"] == 1
    assert backend.calls[-1]["query"]["bool"]["filter"][-1] == {
        "range": {"event_time": {"gte": "2026-10-11T00:00:00.000000Z"}}
    }


def test_stats_are_bounded_filtered_and_typed(http, backend):
    response = http.get("/api/v1/stats/alerts?asset_id=router")
    assert response.json() == {
        "by_severity": [{"key": "high", "count": 1}],
        "by_rule_id": [{"key": "logging-disabled", "count": 1}],
    }
    assert backend.calls[-1]["aggregations"]["counts"]["terms"]["size"] == 100
    response = http.get("/api/v1/stats/diffs?start=2026-10-11T00:00:00Z&end=2026-10-12T00:00:00Z")
    assert response.status_code == 200
    assert response.json()["buckets"][0]["count"] == 1
    assert (
        http.get(
            "/api/v1/stats/diffs?start=2020-01-01T00:00:00Z&end=2026-10-11T00:00:00Z"
        ).status_code
        == 422
    )
    assert http.get("/api/v1/stats/diffs").status_code == 200


def test_config_lifespan_docs_and_read_only(backend):
    with patch("api.app.create_client", return_value=backend):
        with TestClient(create_app()) as client:
            assert client.get("/openapi.json").status_code == 200
            assert client.post("/api/v1/snapshots", json={"content": "secret"}).status_code == 405
        backend.close.assert_called_once()
    with TestClient(
        create_app(ApiConfig(docs=False), service=SearchService(backend, SearchConfig()))
    ) as client:
        for path in ("/docs", "/redoc", "/openapi.json", "/api/v1/config/raw"):
            assert client.get(path).status_code == 404
        assert (
            "access-control-allow-origin"
            not in client.get("/healthz", headers={"Origin": "https://outside"}).headers
        )


@pytest.mark.parametrize(
    "values",
    [
        {"CONFIGSTREAM_API_PORT": "0"},
        {"CONFIGSTREAM_API_DOCS": "maybe"},
        {"CONFIGSTREAM_API_LOG_LEVEL": "bad"},
    ],
)
def test_invalid_configuration(values):
    with pytest.raises(ValueError):
        ApiConfig.from_env(values)


def test_configuration_reuses_search_and_defaults():
    config = ApiConfig.from_env({"CONFIGSTREAM_SEARCH_INDEX_PREFIX": "api-test"})
    assert config.host == "127.0.0.1" and config.port == 8000 and config.docs
    assert config.search.index_prefix == "api-test"
    context = cursor_context("snapshots", {})
    assert decode_cursor(encode_cursor([123, "snapshot-1"], context), context) == [
        123,
        "snapshot-1",
    ]
