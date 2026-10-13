"""Descriptive observed values and explicit limits on comparability."""


def compare_groups(previous, current):
    from pyspark.sql import functions as functions
    from pyspark.sql.types import ArrayType, StringType

    paired = previous.alias("p").join(current.alias("c"), "group_key", "inner")
    values = ArrayType(StringType(), False)
    paired = paired.select(
        "group_key",
        functions.col("p.group_id").alias("previous_id"),
        functions.col("c.group_id").alias("current_id"),
        functions.from_json(functions.col("p.addresses_json"), values).alias("before"),
        functions.from_json(functions.col("c.addresses_json"), values).alias("after"),
        (functions.col("p.workers_json") == functions.col("c.workers_json")).alias(
            "same_recorded_workers"
        ),
    )
    differences = (
        paired.withColumn("added_values", functions.array_except("after", "before"))
        .withColumn("removed_values", functions.array_except("before", "after"))
        .withColumn("unchanged_values", functions.array_intersect("before", "after"))
        .withColumn("causal_interpretation", functions.lit("unknown"))
        .withColumn("comparison_policy", functions.lit("phase13-descriptive-v1"))
        .withColumn("strict_qualification", functions.lit("vantage_unverified"))
    )
    summary = (
        differences.agg(
            functions.count("*").alias("common_groups"),
            functions.sum(
                functions.when(
                    (functions.size("added_values") > 0) | (functions.size("removed_values") > 0), 1
                ).otherwise(0)
            ).alias("changed_groups"),
            functions.sum(functions.size("added_values")).alias("added_values"),
            functions.sum(functions.size("removed_values")).alias("removed_values"),
            functions.sum(functions.size("unchanged_values")).alias("unchanged_values"),
            functions.sum(functions.col("same_recorded_workers").cast("long")).alias(
                "same_recorded_worker_sets"
            ),
        )
        .first()
        .asDict()
    )
    summary["new_observed_groups"] = current.join(
        previous.select("group_key"), "group_key", "left_anti"
    ).count()
    summary["no_longer_observed_groups"] = previous.join(
        current.select("group_key"), "group_key", "left_anti"
    ).count()
    summary.update(
        strict_matched_vantage_eligible=0,
        strict_unknown_qualification_groups=summary["common_groups"],
        strict_reason="resolver/vantage and measurement-method version not independently supplied",
        causal_interpretation="unknown",
    )
    return differences, summary
