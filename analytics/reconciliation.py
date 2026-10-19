"""Exact logical identity reconciliation across committed configuration queries."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def reconcile_history(frame: DataFrame, identifier: str) -> DataFrame:
    """Remove exact retries while rejecting conflicting payloads under one identity.

    Physical location and ingestion time describe delivery, not source identity.
    Compare the full typed payload before discarding private content for analytics.
    """
    if identifier not in {"snapshot_id", "diff_id", "alert_id"}:
        raise ValueError("Unsupported historical identity")
    distinct = frame.drop(
        "query_id", "batch_id", "year", "month", "day", "ingest_time"
    ).dropDuplicates()
    invalid = (
        distinct.groupBy(identifier)
        .count()
        .where(
            (F.col("count") > 1)
            | F.col(identifier).isNull()
            | (F.length(F.trim(F.col(identifier))) == 0)
        )
    )
    if invalid.limit(1).count():
        raise RuntimeError(
            f"Conflicting or missing historical {identifier}; analytics not published"
        )
    if identifier == "snapshot_id":
        conflicts = distinct.groupBy("asset_id", "version").count().where(F.col("count") > 1)
        if conflicts.limit(1).count():
            raise RuntimeError("Conflicting historical asset/version; analytics not published")
    return distinct
