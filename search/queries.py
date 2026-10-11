from search.config import SearchConfig
from search.documents import utc_timestamp

TIME_FIELDS = {
    "snapshots": "event_time",
    "diffs": "event_time",
    "alerts": "created_at",
    "diagnostics": "event_time",
}


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
    if (severity or rule_id) and kind != "alerts":
        raise ValueError("Severity and rule filters apply only to alerts")
    if diagnostic_type and kind != "diagnostics":
        raise ValueError("Diagnostic type filter applies only to diagnostics")
    filters = [
        {"term": {name: value}}
        for name, value in {
            "asset_id": asset_id,
            "severity": severity,
            "rule_id": rule_id,
            "diagnostic_type": diagnostic_type,
        }.items()
        if value is not None
    ]
    bounds = {
        name: utc_timestamp(value)
        for name, value in {"gte": start, "lte": end}.items()
        if value is not None
    }
    if start and end and utc_timestamp(start) > utc_timestamp(end):
        raise ValueError("Query start must precede end")
    if bounds:
        filters.append({"range": {TIME_FIELDS[kind]: bounds}})
    return dict(
        client.search(
            index=alias,
            query={"bool": {"filter": filters}},
            size=1 if latest else size,
            sort=[{"version": "desc"}, {"snapshot_id": "asc"}]
            if latest
            else [{TIME_FIELDS[kind]: "desc"}],
            track_total_hits=True,
        )
    )


def aggregate(client, config: SearchConfig, group: str) -> dict:
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
        client.search(index=config.alias(kind), size=0, aggregations={"counts": specification})
    )["aggregations"]
