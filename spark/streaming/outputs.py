import logging

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as functions

from spark.common.config import SparkConfig

LOGGER = logging.getLogger(__name__)


def write_alerts(batch: DataFrame, config: SparkConfig) -> None:
    (
        batch.filter(functions.col("record_type") == "alert")
        .select(functions.col("asset_id").alias("key"), "value")
        .write.format("kafka")
        .option("kafka.bootstrap.servers", config.bootstrap_servers)
        .option("topic", "config.alerts")
        .option("kafka.acks", "all")
        .option("kafka.enable.idempotence", "true")
        .option("kafka.delivery.timeout.ms", "10000")
        .option("kafka.request.timeout.ms", "5000")
        .save()
    )


def publish_batch(batch: DataFrame, batch_id: int, config: SparkConfig) -> None:
    batch.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        counts = {
            row.record_type: row["count"] for row in batch.groupBy("record_type").count().collect()
        }
        if config.lake is not None:
            from spark.streaming.sinks.lake import write_lake_batch

            write_lake_batch(batch, batch_id, config, counts)
        if config.search is not None:
            from search.sink import index_batch

            index_batch(batch, config)
        if counts.get("alert", 0):
            write_alerts(batch, config)
        reasons = {
            row.reason: row["count"]
            for row in batch.filter(functions.col("record_type").isin("diagnostic", "invalid"))
            .groupBy(
                functions.coalesce(
                    functions.get_json_object("summary", "$.reason"),
                    functions.get_json_object("summary", "$.validation_error"),
                ).alias("reason")
            )
            .count()
            .collect()
        }
        LOGGER.info("Output batchId=%s counts=%s diagnostics=%s", batch_id, counts, reasons)
        for row in batch.select("record_type", "summary").limit(config.console_rows).collect():
            LOGGER.info("%s %s", row.record_type, row.summary)
    finally:
        batch.unpersist()
