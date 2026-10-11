from dataclasses import replace
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from api.app import create_app
from api.config import ApiConfig
from search.bulk import write_actions
from search.client import create_client, initialize_indices
from search.config import KINDS, SearchConfig
from search.documents import bulk_action

pytestmark = pytest.mark.api_integration


def test_real_api_search_pagination_filters_stats_and_missing_alias():
    search = replace(SearchConfig.from_env(), index_prefix=f"phase9-http-{uuid4().hex[:8]}")
    config = ApiConfig(search=search)
    with create_client(search) as backend:
        try:
            initialize_indices(search, backend)
            snapshots = [
                {
                    "snapshot_id": f"snapshot-{index}",
                    "asset_id": "http-router",
                    "version": index,
                    "event_time": "2026-10-11T00:00:00.123456Z"
                    if index < 5
                    else "2026-10-11T01:00:00Z",
                    "ingest_time": "2026-10-11T01:00:01Z",
                    "raw_hash": "a" * 64,
                    "normalized_hash": "b" * 64,
                    "normalization_version": "text-v1",
                    "content": "SENSITIVE_TEST_MARKER",
                    "normalized_content": "synthetic-before synthetic-after",
                    "value": "source envelope bytes",
                }
                for index in range(1, 6)
            ]
            alerts = [
                {
                    "schema_version": "1.0",
                    "alert_id": f"alert-{severity}",
                    "asset_id": "http-router",
                    "snapshot_id": "snapshot-5",
                    "rule_id": rule,
                    "severity": severity,
                    "message": "Safe policy message",
                    "created_at": "2026-10-11T00:00:00Z",
                }
                for severity, rule in (("high", "logging-disabled"), ("critical", "telnet-enabled"))
            ]
            diff = {
                "schema_version": "1.0",
                "diff_id": "diff-1-2",
                "asset_id": "http-router",
                "old_snapshot_id": "snapshot-1",
                "new_snapshot_id": "snapshot-2",
                "added": 1,
                "removed": 1,
                "changed": 1,
                "event_time": "2026-10-11T00:00:00Z",
                "details": [{"line": "SENSITIVE_TEST_MARKER"}],
            }
            diagnostic = {
                "record_type": "diagnostic",
                "asset_id": "http-router",
                "event_time": "2026-10-11T00:00:00Z",
                "summary": '{"reason":"version_conflict","version":2}',
            }
            actions = (
                [bulk_action("snapshots", item, search) for item in snapshots]
                + [bulk_action("alerts", item, search) for item in alerts]
                + [
                    bulk_action("diffs", diff, search),
                    bulk_action("diagnostics", diagnostic, search),
                ]
            )
            write_actions(actions, search, backend)
            backend.indices.refresh(index=",".join(search.alias(kind) for kind in KINDS))
            with TestClient(create_app(config)) as http:
                assert http.get("/healthz").status_code == 200
                assert http.get("/readyz").status_code == 200
                ids, cursor = [], None
                for count in (2, 2, 1):
                    params = {"asset_id": "http-router", "limit": 2}
                    if cursor:
                        params["cursor"] = cursor
                    response = http.get("/api/v1/snapshots", params=params)
                    assert response.status_code == 200, response.text
                    page = response.json()
                    assert page["count"] == count
                    ids.extend(item["snapshot_id"] for item in page["items"])
                    cursor = page["next_cursor"]
                    for forbidden in (
                        "content",
                        "value",
                        "SENSITIVE_TEST_MARKER",
                        "synthetic-before",
                        "synthetic-after",
                        "_index",
                        "_score",
                        "_shards",
                    ):
                        assert forbidden not in response.text
                assert len(set(ids)) == 5 and cursor is None
                assert set(ids) == {item["snapshot_id"] for item in snapshots}
                assert http.get("/api/v1/assets/http-router/latest-snapshot").json()["version"] == 5
                assert http.get("/api/v1/snapshots/snapshot-1").status_code == 200
                assert http.get("/api/v1/snapshots/not-found").status_code == 404
                assert http.get("/api/v1/diffs/diff-1-2").status_code == 200
                assert "details" not in http.get("/api/v1/diffs").text
                assert (
                    http.get("/api/v1/alerts?severity=critical&rule_id=telnet-enabled").json()[
                        "count"
                    ]
                    == 1
                )
                stats = http.get("/api/v1/stats/alerts?asset_id=http-router").json()
                assert {item["key"] for item in stats["by_severity"]} == {"high", "critical"}
                assert {item["key"] for item in stats["by_rule_id"]} == {
                    "logging-disabled",
                    "telnet-enabled",
                }
                stats = http.get(
                    "/api/v1/stats/diffs?start=2026-10-11T00:00:00Z&end=2026-10-12T00:00:00Z"
                )
                assert sum(item["count"] for item in stats.json()["buckets"]) == 1
                diagnostics = http.get("/api/v1/diagnostics?type=version_conflict").json()
                assert diagnostics["count"] == 1
                identity = diagnostics["items"][0]["diagnostic_id"]
                assert http.get(f"/api/v1/diagnostics/{identity}").status_code == 200
                backend.indices.delete(index=search.index("snapshots"))
                assert http.get("/readyz").status_code == 503
                assert http.get("/api/v1/snapshots").status_code == 503
                assert http.get("/healthz").status_code == 200
                initialize_indices(search, backend)
                assert http.get("/readyz").status_code == 200
                assert http.get("/api/v1/snapshots").json()["count"] == 0
        finally:
            backend.indices.delete(
                index=",".join(search.index(kind) for kind in KINDS), ignore_unavailable=True
            )
