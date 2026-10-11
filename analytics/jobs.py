from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as functions

from analytics.config import AnalyticsConfig


def filtered(frame: DataFrame, timestamp: str, config: AnalyticsConfig) -> DataFrame:
    if config.start is not None:
        frame = frame.filter(functions.col(timestamp) >= functions.lit(config.start))
    if config.end is not None:
        frame = frame.filter(functions.col(timestamp) < functions.lit(config.end))
    return frame


def change_frequency(inputs: dict[str, DataFrame], config: AnalyticsConfig) -> DataFrame:
    frames = []
    for name, time_column in (
        ("snapshot", "event_time"),
        ("diff", "event_time"),
        ("alert", "created_at"),
    ):
        frames.append(
            filtered(inputs[name], time_column, config)
            .groupBy("asset_id", functions.window(time_column, config.window).alias("window"))
            .agg(functions.count("*").alias(f"{name}_count"))
        )
    joined = reduce(lambda left, right: left.join(right, ["asset_id", "window"], "full"), frames)
    return joined.select(
        "asset_id",
        functions.col("window.start").alias("window_start"),
        functions.col("window.end").alias("window_end"),
        "snapshot_count",
        "diff_count",
        "alert_count",
    ).fillna(0)


def fleet_activity(frequency: DataFrame) -> DataFrame:
    return frequency.groupBy("window_start", "window_end").agg(
        functions.sum("snapshot_count").alias("snapshot_count"),
        functions.sum("diff_count").alias("diff_count"),
        functions.sum("alert_count").alias("alert_count"),
        functions.countDistinct(
            functions.when(functions.col("diff_count") > 0, functions.col("asset_id"))
        ).alias("assets_changed"),
    )


def diff_magnitude(frame: DataFrame) -> DataFrame:
    expressions = []
    for metric in ("added", "removed", "changed"):
        expressions.extend(
            [
                functions.avg(metric).alias(f"{metric}_mean"),
                functions.percentile_approx(metric, 0.5, 10000).alias(f"{metric}_p50"),
                functions.percentile_approx(metric, 0.9, 10000).alias(f"{metric}_p90"),
                functions.percentile_approx(metric, 0.95, 10000).alias(f"{metric}_p95"),
                functions.max(metric).alias(f"{metric}_max"),
            ]
        )
    return frame.agg(functions.count("*").alias("diff_count"), *expressions)


def alert_trends(frame: DataFrame, config: AnalyticsConfig) -> DataFrame:
    return (
        filtered(frame, "created_at", config)
        .groupBy(
            "severity",
            "rule_id",
            "asset_id",
            functions.window("created_at", config.window).alias("window"),
        )
        .count()
        .select(
            "severity",
            "rule_id",
            "asset_id",
            functions.col("window.start").alias("window_start"),
            functions.col("window.end").alias("window_end"),
            "count",
        )
    )


def diagnostic_trends(frame: DataFrame, config: AnalyticsConfig) -> DataFrame:
    tagged = frame.withColumn(
        "diagnostic_type",
        functions.coalesce(
            functions.get_json_object("summary", "$.diagnostic_type"),
            functions.when(
                functions.col("record_type") == "invalid", functions.lit("invalid_record")
            ),
            functions.get_json_object("summary", "$.reason"),
            functions.lit("unknown"),
        ),
    )
    return (
        filtered(tagged, "event_time", config)
        .groupBy("diagnostic_type", functions.window("event_time", config.window).alias("window"))
        .count()
        .select(
            "diagnostic_type",
            functions.col("window.start").alias("window_start"),
            functions.col("window.end").alias("window_end"),
            "count",
        )
    )


def asset_summary(inputs: dict[str, DataFrame], config: AnalyticsConfig) -> DataFrame:
    snapshots = (
        filtered(inputs["snapshot"], "event_time", config)
        .groupBy("asset_id")
        .agg(
            functions.min("event_time").alias("first_event_time"),
            functions.max("event_time").alias("last_event_time"),
            functions.count("*").alias("snapshot_count"),
        )
    )
    diffs = (
        filtered(inputs["diff"], "event_time", config)
        .groupBy("asset_id")
        .agg(
            functions.count("*").alias("diff_count"),
            functions.avg("changed").alias("mean_diff_changed"),
            functions.max("changed").alias("max_diff_changed"),
        )
    )
    alerts = (
        filtered(inputs["alert"], "created_at", config)
        .groupBy("asset_id")
        .agg(
            functions.count("*").alias("alert_count"),
            functions.sum(
                functions.when(functions.col("severity") == "high", 1).otherwise(0)
            ).alias("high_alert_count"),
            functions.sum(
                functions.when(functions.col("severity") == "critical", 1).otherwise(0)
            ).alias("critical_alert_count"),
        )
    )
    return snapshots.join(diffs, "asset_id", "full").join(alerts, "asset_id", "full").fillna(0)


def frequency_anomalies(
    frame: DataFrame, metric: str, config: AnalyticsConfig, *, fleet: bool = False
) -> DataFrame:
    keys = [] if fleet else ["asset_id"]
    groups = frame.groupBy(*keys).agg(
        functions.min("window_start").alias("first"), functions.max("window_start").alias("last")
    )
    dense = groups.select(
        *keys,
        functions.explode(
            functions.sequence("first", "last", functions.expr(f"INTERVAL {config.window}"))
        ).alias("window_start"),
    )
    dense = dense.join(
        frame.select(*keys, "window_start", metric), [*keys, "window_start"], "left"
    ).fillna({metric: 0})
    prior = (
        Window.partitionBy(*keys).orderBy("window_start").rowsBetween(-config.baseline_windows, -1)
    )
    scored = (
        dense.withColumn("prior_windows", functions.count(metric).over(prior))
        .withColumn("baseline_mean", functions.avg(metric).over(prior))
        .withColumn("baseline_stddev", functions.stddev_pop(metric).over(prior))
    )
    scored = scored.withColumn(
        "z_score",
        functions.when(
            functions.col("baseline_stddev") > 0,
            (functions.col(metric) - functions.col("baseline_mean"))
            / functions.col("baseline_stddev"),
        ),
    )
    return scored.withColumn(
        "is_anomaly",
        functions.coalesce(
            (functions.col("prior_windows") >= config.minimum_prior_windows)
            & (functions.col(metric) >= config.minimum_count)
            & (
                (functions.col("z_score") >= config.z_threshold)
                | (
                    (functions.col("baseline_stddev") == 0)
                    & (functions.col(metric) > functions.col("baseline_mean"))
                )
            ),
            functions.lit(False),
        ),
    ).withColumn("baseline_policy", functions.lit("prior-only-population-zscore-v1"))


def all_jobs(inputs: dict[str, DataFrame], config: AnalyticsConfig) -> dict[str, DataFrame]:
    frequency = change_frequency(inputs, config)
    fleet = fleet_activity(frequency)
    return {
        "change-frequency": frequency,
        "fleet-activity": fleet,
        "diff-magnitude": diff_magnitude(filtered(inputs["diff"], "event_time", config)),
        "alert-trends": alert_trends(inputs["alert"], config),
        "diagnostic-trends": diagnostic_trends(inputs["diagnostic"], config),
        "asset-summary": asset_summary(inputs, config),
        "anomalies": frequency_anomalies(frequency, "diff_count", config),
        "fleet-bursts": frequency_anomalies(fleet, "assets_changed", config, fleet=True),
    }
