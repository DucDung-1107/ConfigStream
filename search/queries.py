from search.config import SearchConfig
from search.documents import utc_timestamp
from search.errors import IncompleteSearchError, SearchTimeoutError

TIME_FIELDS = {
    "snapshots": "event_time",
    "diffs": "event_time",
    "alerts": "created_at",
    "diagnostics": "event_time",
}
ID_FIELDS = {
    "snapshots": "snapshot_id",
    "diffs": "diff_id",
    "alerts": "alert_id",
    "diagnostics": "diagnostic_id",
}


def complete_search(client, **options):
    response = client.search(allow_partial_search_results=False, **options)
    if response.get("timed_out", False):
        raise SearchTimeoutError("Search deadline exceeded")
    if response.get("terminated_early", False) or response.get("_shards", {}).get("failed", 0):
        raise IncompleteSearchError("Search results are incomplete")
    return response


def document_filters(kind: str, **values) -> list[dict]:
    if kind not in TIME_FIELDS:
        raise ValueError("Unknown search document kind")
    if (values.get("severity") or values.get("rule_id")) and kind != "alerts":
        raise ValueError("Severity and rule filters apply only to alerts")
    if values.get("diagnostic_type") and kind != "diagnostics":
        raise ValueError("Diagnostic type filter applies only to diagnostics")
    filters = [
        {"term": {name: values[name]}}
        for name in ("asset_id", "severity", "rule_id", "diagnostic_type")
        if values.get(name) is not None
    ]
    bounds = {
        name: utc_timestamp(values[field])
        for name, field in (("gte", "start"), ("lte", "end"), ("lt", "end_before"))
        if values.get(field) is not None
    }
    if "gte" in bounds and "lte" in bounds and bounds["gte"] > bounds["lte"]:
        raise ValueError("Query start must precede end")
    if "lt" in bounds:
        if "lte" in bounds:
            raise ValueError("Use either end or end_before")
        if "gte" in bounds and bounds["gte"] >= bounds["lt"]:
            raise ValueError("Query start must precede end_before")
    if bounds:
        filters.append({"range": {TIME_FIELDS[kind]: bounds}})
    return filters


def page_documents(client, config: SearchConfig, kind: str, *, limit=50, after=None, **filters):
    if not 1 <= limit <= 200:
        raise ValueError("Page limit must be between 1 and 200")
    response = complete_search(
        client,
        index=config.alias(kind),
        query={"bool": {"filter": document_filters(kind, **filters)}},
        size=limit + 1,
        sort=[{TIME_FIELDS[kind]: "desc"}, {ID_FIELDS[kind]: "asc"}],
        search_after=after,
        track_total_hits=False,
    )
    hits = response["hits"]["hits"]
    return {
        "items": [hit["_source"] for hit in hits[:limit]],
        "has_more": len(hits) > limit,
        "next_sort_values": hits[limit - 1]["sort"] if len(hits) > limit else None,
    }


def get_document(client, config: SearchConfig, kind: str, identity: str):
    from elasticsearch import NotFoundError

    try:
        return client.get(index=config.alias(kind), id=identity)["_source"]
    except NotFoundError as error:
        if error.body.get("found") is False:
            return None
        raise


def query_documents(
    client,
    config: SearchConfig,
    kind: str,
    *,
    asset_id: str | None = None,
    severity: str | None = None,
    rule_id: str | None = None,
    diagnostic_type: str | None = None,
    start: str | None = None,
    end: str | None = None,
    latest: bool = False,
    size: int = 20,
) -> dict:
    alias = config.alias(kind)
    if not 1 <= size <= 1000:
        raise ValueError("Query size must be between 1 and 1000")
    if latest and (kind != "snapshots" or not asset_id):
        raise ValueError("Latest lookup requires a snapshot asset_id")
    filters = document_filters(
        kind,
        asset_id=asset_id,
        severity=severity,
        rule_id=rule_id,
        diagnostic_type=diagnostic_type,
        start=start,
        end=end,
    )
    return dict(
        complete_search(
            client,
            index=alias,
            query={"bool": {"filter": filters}},
            size=1 if latest else size,
            sort=[{"version": "desc"}, {"snapshot_id": "asc"}]
            if latest
            else [{TIME_FIELDS[kind]: "desc"}],
            track_total_hits=True,
        )
    )


def aggregate(client, config: SearchConfig, group: str, **filters) -> dict:
    if group in {"severity", "rule_id", "asset_id"}:
        kind = "alerts"
        specification = {"terms": {"field": group, "size": 100}}
    elif group == "diffs_per_hour":
        kind = "diffs"
        specification = {
            "date_histogram": {"field": "event_time", "fixed_interval": "1h", "time_zone": "UTC"}
        }
    else:
        raise ValueError("Unknown search aggregation")
    return dict(
        complete_search(
            client,
            index=config.alias(kind),
            size=0,
            query={"bool": {"filter": document_filters(kind, **filters)}},
            aggregations={"counts": specification},
        )
    )["aggregations"]
