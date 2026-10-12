"""Opt-in Kafka RPKI query, with isolated checkpoint and complete-day barrier."""

from uuid import UUID

from schemas.rpki import TOPIC, validate_topic
from spark.rpki import (
    committed_rows,
    decode_chunks,
    finalize_day,
    publish_boundary,
    publish_raw,
    reconcile_rows,
)


class RpkiProcessor:
    def __init__(self, session, store, query_id, references, oracle_dir, boundaries):
        UUID(query_id)
        self.session = session
        self.store = store
        self.query_id = query_id
        self.references = references
        self.oracle_dir = oracle_dir
        self.boundaries = boundaries

    def __call__(self, batch, batch_id):
        input_chunks = batch.count()
        if input_chunks > 512:
            raise ValueError("RPKI foreachBatch chunk budget exceeded")
        try:
            return self.process(batch, batch_id)
        except Exception as error:
            self.store.put(
                f"quarantine/{self.query_id}/{batch_id:020d}",
                {
                    "format_version": "rpki-1.0",
                    "status": "failed_batch",
                    "input_chunks": input_chunks,
                    "error_class": type(error).__name__,
                    "source_retrieval": "checkpoint offsets and retained Kafka input",
                },
            )
            raise

    def process(self, batch, batch_id):
        rows = decode_chunks(batch, self.references).persist()
        try:
            from pyspark.sql import functions as functions

            active = rows.select("observation_id").distinct().limit(33).collect()
            if len(active) > 32:
                raise ValueError("RPKI source catalog metadata budget exceeded")
            expected = None
            for entry in active:
                reference = self.references[entry.observation_id]
                day = reference.observation.payload.observation_date.isoformat()
                source_map = self.session.read.parquet(
                    str(self.oracle_dir / "source_maps" / f"{day}.parquet")
                ).withColumn("observation_id", functions.lit(entry.observation_id))
                expected = source_map if expected is None else expected.unionByName(source_map)
            if (
                expected is not None
                and rows.join(
                    expected, ["observation_id", "row_index", "member_encoding"], "left_anti"
                )
                .limit(1)
                .count()
            ):
                raise ValueError("Incoming source row disagrees with independent coordinate oracle")
            active_ids = [item.observation_id for item in active]
            previous = committed_rows(
                self.session, self.store, self.query_id, observation_ids=active_ids
            )
            reconcile_rows(previous.unionByName(rows)).count()
            publish_raw(rows, self.store, self.query_id, batch_id)
        finally:
            rows.unpersist()
        accumulated = committed_rows(
            self.session, self.store, self.query_id, observation_ids=active_ids
        ).persist()
        try:
            complete = {}
            for reference in self.references.values():
                day = reference.observation.payload.observation_date.isoformat()
                committed = self.store.get(f"state/{reference.manifest.source_scope}/{day}")
                if reference.observation_id in active_ids:
                    expected = self.session.read.parquet(
                        str(self.oracle_dir / "members" / f"{day}.parquet")
                    )
                    source_map = self.session.read.parquet(
                        str(self.oracle_dir / "source_maps" / f"{day}.parquet")
                    )
                    committed = finalize_day(
                        accumulated, reference, expected, self.store, expected_rows=source_map
                    )
                if committed:
                    complete[day] = reference
            for boundary in self.boundaries:
                old = complete.get(boundary["previous_date"])
                new = complete.get(boundary["current_date"])
                if (
                    old is not None
                    and new is not None
                    and self.store.get(f"boundary/{old.observation_id}/{new.observation_id}")
                    is None
                ):
                    publish_boundary(self.session, self.store, old, new, boundary)
        finally:
            accumulated.unpersist()


def start_query(session, processor, bootstrap, checkpoint, *, max_chunks=64, topic=TOPIC):
    if not 1 <= max_chunks <= 512:
        raise ValueError("RPKI micro-batch chunk budget exceeded")
    validate_topic(topic)
    source = (
        session.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("kafka.allow.auto.create.topics", "false")
        .option("maxOffsetsPerTrigger", max_chunks)
        .load()
        .selectExpr("CAST(value AS STRING) AS value")
    )
    return (
        source.writeStream.foreachBatch(processor)
        .option("checkpointLocation", str(checkpoint))
        .trigger(availableNow=True)
        .start()
    )
