from unittest.mock import Mock

import pytest

from search.config import SearchConfig
from search.queries import ID_FIELDS, aggregate, document_filters, page_documents


@pytest.mark.parametrize("kind", ["snapshots", "diffs", "alerts", "diagnostics"])
def test_page_has_stable_tie_breaker_without_total_count(kind):
    client = Mock()
    client.search.return_value = {
        "hits": {
            "hits": [{"_source": {"id": index}, "sort": [123, str(index)]} for index in range(3)]
        }
    }
    result = page_documents(client, SearchConfig(), kind, limit=2, after=[124, "older"])
    assert result == {
        "items": [{"id": 0}, {"id": 1}],
        "has_more": True,
        "next_sort_values": [123, "1"],
    }
    options = client.search.call_args.kwargs
    assert options["size"] == 3
    assert options["sort"][1] == {ID_FIELDS[kind]: "asc"}
    assert options["search_after"] == [124, "older"]
    assert options["track_total_hits"] is False


@pytest.mark.parametrize("limit", [0, 201])
def test_public_page_limits(limit):
    with pytest.raises(ValueError):
        page_documents(Mock(), SearchConfig(), "snapshots", limit=limit)


def test_filtered_stats_and_empty_page():
    client = Mock()
    client.search.return_value = {"aggregations": {"counts": {"buckets": []}}}
    assert aggregate(client, SearchConfig(), "severity", asset_id="router")
    assert client.search.call_args.kwargs["query"] == {
        "bool": {"filter": [{"term": {"asset_id": "router"}}]}
    }
    client.search.return_value = {"hits": {"hits": []}}
    assert page_documents(client, SearchConfig(), "snapshots")["has_more"] is False


@pytest.mark.parametrize(
    "kind,values",
    [
        ("unknown", {}),
        ("snapshots", {"severity": "high"}),
        ("diffs", {"diagnostic_type": "invalid_record"}),
        ("alerts", {"start": "2026-10-12T00:00:00Z", "end": "2026-10-11T00:00:00Z"}),
    ],
)
def test_invalid_filter_combinations(kind, values):
    with pytest.raises(ValueError):
        document_filters(kind, **values)
