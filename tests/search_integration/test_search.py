from dataclasses import replace
from uuid import uuid4

import pytest

from search.bulk import write_actions
from search.client import create_client, initialize_indices
from search.config import KINDS, SearchConfig
from search.documents import bulk_action
from search.queries import aggregate, query_documents

pytestmark = pytest.mark.search_integration


def test_real_search_init_bulk_upsert_queries_and_strict_mapping():
    config = replace(SearchConfig.from_env(), index_prefix=f"phase8-unit-{uuid4().hex[:8]}")
    with create_client(config) as client:
        try:
            initialize_indices(config, client)
            initialize_indices(config, client)
            snapshot = {
                "snapshot_id": "snapshot-1",
                "asset_id": "router",
                "version": 1,
                "event_time": "2026-10-11T00:00:00Z",
                "normalized_content": "SENSITIVE_TEST_MARKER",
            }
            alert = {
                "alert_id": "alert-1",
                "asset_id": "router",
                "snapshot_id": "snapshot-1",
                "rule_id": "logging-disabled",
                "severity": "high",
                "message": "Logging disabled",
                "created_at": "2026-10-11T00:00:00Z",
            }
            first = [
                bulk_action("snapshots", snapshot, config),
                bulk_action("alerts", alert, config),
            ]
            write_actions(first, config, client)
            write_actions(first, config, client)
            write_actions(
                [bulk_action("snapshots", {**snapshot, "version": 2}, config)], config, client
            )
            client.indices.refresh(index=",".join(config.alias(kind) for kind in KINDS))
            assert client.count(index=config.alias("snapshots"))["count"] == 1
            result = query_documents(client, config, "snapshots", asset_id="router", latest=True)
            assert result["hits"]["hits"][0]["_source"]["version"] == 2
            assert "SENSITIVE_TEST_MARKER" not in str(result)
            assert aggregate(client, config, "severity")["counts"]["buckets"][0]["key"] == "high"
            bad = {
                "_index": config.alias("snapshots"),
                "_id": "bad",
                "_source": {"unexpected": "SENSITIVE_TEST_MARKER"},
            }
            with pytest.raises(RuntimeError, match="status=400"):
                write_actions([bad], config, client)
            assert client.count(index=config.alias("snapshots"))["count"] == 1
        finally:
            client.indices.delete(
                index=",".join(config.index(kind) for kind in KINDS), ignore_unavailable=True
            )
