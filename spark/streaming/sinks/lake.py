import json
import logging
from uuid import UUID

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as functions

from lake.client import list_commits, load_commit, save_commit
from lake.config import LakeConfig
from spark.common.config import SparkConfig
from spark.common.lake_schemas import (
    ALERT_SCHEMA,
    DATASET_SCHEMAS,
    DIFF_SCHEMA,
    NORMALIZED_SCHEMA,
)

LOGGER = logging.getLogger(__name__)
DATASET_RECORD_TYPES = {
    "raw/snapshots": ("raw_snapshot",),
    "processed/snapshots": ("normalized_snapshot",),
    "processed/diffs": ("config_diff",),
    "processed/alerts": ("alert",),
    "processed/diagnostics": ("diagnostic", "invalid"),
}


def raw_outputs(source: DataFrame) -> DataFrame:
    metadata = source.select(
        functions.base64("key").alias("key_base64"),
        functions.base64("value").alias("value_base64"),
        functions.col("topic").alias("kafka_topic"),
        functions.col("partition").alias("kafka_partition"),
        functions.col("offset").alias("kafka_offset"),
        functions.col("timestamp").alias("kafka_timestamp"),
    )
    return metadata.select(
        functions.lit("raw_snapshot").alias("record_type"),
        functions.lit(None).cast("string").alias("asset_id"),
        functions.col("kafka_timestamp").alias("event_time"),
        functions.to_json(functions.struct("kafka_topic", "kafka_partition", "kafka_offset")).alias(
            "summary"
        ),
        functions.to_json(functions.struct(*metadata.columns)).alias("value"),
    )


def dataset_frame(batch: DataFrame, dataset: str) -> DataFrame:
    selected = batch.filter(functions.col("record_type").isin(*DATASET_RECORD_TYPES[dataset]))
    if dataset == "raw/snapshots":
        parsed = selected.select(
            functions.from_json(
                "value",
                "key_base64 STRING, value_base64 STRING, kafka_topic STRING, "
                "kafka_partition INT, kafka_offset LONG, kafka_timestamp TIMESTAMP",
            ).alias("payload")
        ).select("payload.*")
        output = parsed.select(
            functions.unbase64("key_base64").alias("kafka_key"),
            functions.unbase64("value_base64").alias("kafka_value"),
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
        )
        partition_time = functions.col("kafka_timestamp")
    elif dataset == "processed/diagnostics":
        output = selected.select("record_type", "asset_id", "event_time", "summary")
        partition_time = functions.coalesce(
            functions.col("event_time"),
            functions.try_to_timestamp(functions.get_json_object("summary", "$.kafka_timestamp")),
        )
    else:
        schema = {
            "processed/snapshots": NORMALIZED_SCHEMA,
            "processed/diffs": DIFF_SCHEMA,
            "processed/alerts": f"record_type STRING, payload STRUCT<{ALERT_SCHEMA}>",
        }[dataset]
        output = selected.withColumn("payload", functions.from_json("value", schema))
        if dataset == "processed/alerts":
            output = output.select("payload.payload.*")
            partition_time = functions.col("created_at")
        elif dataset == "processed/diffs":
            output = output.select("payload.*", "event_time")
            partition_time = functions.col("event_time")
        else:
            output = output.select("payload.*")
            partition_time = functions.col("event_time")
    partition_time = functions.coalesce(
        partition_time, functions.lit("1970-01-01").cast("timestamp")
    )
    return (
        output.withColumn("year", functions.year(partition_time))
        .withColumn("month", functions.month(partition_time))
        .withColumn("day", functions.dayofmonth(partition_time))
    )


def checkpoint_query_id(config: SparkConfig) -> str:
    metadata = config.checkpoint_dir / "metadata"
    return str(UUID(json.loads(metadata.read_text(encoding="utf-8"))["id"]))


def write_lake_batch(
    batch: DataFrame, batch_id: int, config: SparkConfig, counts: dict[str, int]
) -> None:
    lake = config.lake
    if lake is None:
        raise ValueError("Lake sink requires enabled lake configuration")
    if not counts:
        return
    query_id = checkpoint_query_id(config)
    committed = load_commit(lake, query_id, batch_id)
    if committed is not None:
        if (
            committed.get("format_version") != "1.0"
            or committed.get("query_id") != query_id
            or committed.get("batch_id") != batch_id
            or committed.get("counts") != counts
        ):
            raise RuntimeError("Lake commit disagrees with the replayed batch")
        LOGGER.info("Lake batchId=%s already committed; preserving Parquet", batch_id)
        return
    paths = {}
    for dataset, record_types in DATASET_RECORD_TYPES.items():
        if sum(counts.get(record_type, 0) for record_type in record_types) == 0:
            continue
        path = lake.uri(f"{dataset}/query_id={query_id}/batch_id={batch_id:020d}")
        (
            dataset_frame(batch, dataset)
            .write.mode("overwrite")
            .option("compression", "snappy")
            .option("maxRecordsPerFile", lake.max_records_per_file)
            .partitionBy("year", "month", "day")
            .parquet(path)
        )
        paths[dataset] = path
    save_commit(
        lake,
        {
            "format_version": "1.0",
            "query_id": query_id,
            "batch_id": batch_id,
            "counts": counts,
            "datasets": paths,
        },
    )
    LOGGER.info("Lake batchId=%s committed datasets=%s", batch_id, sorted(paths))


def read_committed(
    session: SparkSession, config: LakeConfig, dataset: str, query_id: str | None = None
) -> DataFrame:
    if dataset not in DATASET_SCHEMAS:
        raise ValueError("Unknown lake dataset")
    paths = []
    for manifest in list_commits(config, query_id):
        if manifest.get("format_version") != "1.0":
            raise RuntimeError("Unsupported lake manifest format")
        path = manifest["datasets"].get(dataset)
        if path is not None:
            expected = config.uri(
                f"{dataset}/query_id={UUID(manifest['query_id'])}/"
                f"batch_id={manifest['batch_id']:020d}"
            )
            if path != expected:
                raise RuntimeError("Lake manifest contains an unexpected dataset path")
            paths.append(path)
    if not paths:
        return session.createDataFrame(
            [],
            DATASET_SCHEMAS[dataset]
            + ", query_id STRING, batch_id LONG, year INT, month INT, day INT",
        )
    return (
        session.read.schema(DATASET_SCHEMAS[dataset])
        .option("basePath", config.uri(dataset))
        .parquet(*paths)
        .withColumn("batch_id", functions.col("batch_id").cast("long"))
    )
