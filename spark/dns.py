"""Typed append-only measurements and contributor-preserving observed groups."""

import json
from contextlib import contextmanager
from hashlib import sha256

from processing.identity_projection import reconcile_sorted
from schemas.dns import DnsChunk, DnsReference
from schemas.temporal.identity import logical_identity
from spark.ris import ordered_rows
from workloads.dns_oracle import group_schema, raw_schema


def schema(arrow_schema):
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    return StructType(
        [
            StructField(
                field.name, LongType() if str(field.type) == "int64" else StringType(), True
            )
            for field in arrow_schema
        ]
    )


def digest(frame, columns=None):
    checksum = sha256()
    count = 0
    columns = columns or [frame.columns[0]]
    for row in ordered_rows(frame, columns):
        checksum.update(
            json.dumps(
                row.asDict(), sort_keys=True, separators=(",", ":"), allow_nan=False
            ).encode()
            + b"\n"
        )
        count += 1
    return count, checksum.hexdigest()


@contextmanager
def verify_projection(actual, expected, *, complete=False, partitions=32):
    from pyspark import StorageLevel
    from pyspark.sql import functions as functions
    from pyspark.sql.types import StringType, StructField, StructType

    fields = actual.columns
    if fields != expected.columns:
        raise ValueError("DNS physical projection schema mismatch")
    identity = fields[0]
    union = actual.withColumn("_origin", functions.lit(0)).unionByName(
        expected.withColumn("_origin", functions.lit(1))
    )
    ordered = union.repartition(partitions, identity).sortWithinPartitions(identity)
    checked = actual.sparkSession.createDataFrame(
        ordered.rdd.mapPartitions(reconcile_sorted),
        StructType([*actual.schema.fields, StructField("_validation", StringType(), False)]),
    ).persist(StorageLevel.DISK_ONLY)
    try:
        counts = {
            row._validation: row["count"]
            for row in checked.groupBy("_validation").count().collect()
        }
        if (
            counts.get("conflict", 0)
            or counts.get("unexpected", 0)
            or complete
            and counts.get("missing", 0)
        ):
            raise ValueError("DNS missing/unexpected/conflicting source projection")
        yield checked.where("_validation='accepted'").select(*fields)
    finally:
        checked.unpersist(blocking=True)


def parse_partition(values, references):
    sources = {key: DnsReference.model_validate(value) for key, value in references.items()}
    for record in values:
        if record.value is None or len(record.value.encode()) > 1024**2:
            raise ValueError("DNS transport byte bound")
        try:
            chunk = DnsChunk.model_validate_json(record.value)
            chunk.validate_reference(sources[chunk.artifact_sha256])
        except (ValueError, KeyError):
            raise ValueError("Invalid DNS typed transport/source binding") from None
        for row in chunk.rows:
            yield tuple(row.model_dump().values())


class DnsProcessor:
    def __init__(
        self, session, store, query, references, oracle_paths, *, fault=lambda point: None
    ):
        self.session, self.store, self.query = session, store, query
        self.references = {key: value.model_dump(mode="json") for key, value in references.items()}
        for source in references.values():
            store.put(f"sources/{source.manifest.manifest_id}", source.model_dump(mode="json"))
        self.expected = session.read.schema(schema(raw_schema())).parquet(*oracle_paths)
        self.fault = fault
        self.metrics = []

    def process(self, batch, batch_id):
        from time import monotonic

        from pyspark import StorageLevel
        from pyspark.sql import functions as functions

        began = monotonic()
        references = self.references
        parsed = self.session.createDataFrame(
            batch.selectExpr("CAST(value AS STRING) AS value").rdd.mapPartitions(
                lambda rows: parse_partition(rows, references)
            ),
            schema(raw_schema()),
        ).persist(StorageLevel.DISK_ONLY)
        try:
            bounds = (
                parsed.groupBy("artifact_sha256", "row_group")
                .agg(functions.min("row").alias("first"), functions.max("row").alias("last"))
                .limit(4097)
                .collect()
            )
            if not bounds:
                return
            if len(bounds) > 4096:
                raise ValueError("DNS batch source-coordinate scope bound")
            condition = functions.lit(False)
            for bound in bounds:
                condition = condition | (
                    (functions.col("artifact_sha256") == bound.artifact_sha256)
                    & (functions.col("row_group") == bound.row_group)
                    & functions.col("row").between(bound.first, bound.last)
                )
            with verify_projection(parsed, self.expected.where(condition), partitions=8) as valid:
                count, checksum = digest(valid)
                commit = dict(
                    format_version="dns-source-1.1",
                    query_id=self.query,
                    batch_id=batch_id,
                    path=self.store.path(f"{self.query}/{batch_id:020d}"),
                    logical_rows=count,
                    projection_sha256=checksum,
                    source_artifact_sha256s=sorted({row.artifact_sha256 for row in bounds}),
                    provenance_ids=[],
                )
                commit["provenance_ids"] = sorted(
                    {
                        DnsReference.model_validate(
                            references[row.artifact_sha256]
                        ).manifest.manifest_id
                        for row in bounds
                    }
                )
                key = f"batches/{self.query}/{batch_id:020d}"
                previous = self.store.get(key)
                if previous is not None:
                    if previous != commit:
                        raise ValueError("DNS immutable batch replay conflict")
                else:
                    sealed = [
                        artifact
                        for artifact in commit["source_artifact_sha256s"]
                        if self.store.get(f"completion/{self.query}/{artifact}") is not None
                    ]
                    if sealed:
                        from lake.dns import read_source_completion

                        if sealed != commit["source_artifact_sha256s"]:
                            raise ValueError("DNS batch mixes sealed and unsealed sources")
                        for artifact in sealed:
                            read_source_completion(self.store, self.query, artifact)
                        self.metrics.append(
                            dict(batch_id=batch_id, logical_rows=0, identical_sealed_retry=count)
                        )
                        return
                    self.fault("before_output")
                    if self.fault("partial_output"):
                        valid.limit(1).write.mode("overwrite").parquet(commit["path"])
                        raise RuntimeError("owned DNS partial output interruption")
                    valid.repartition(2).write.mode("overwrite").parquet(commit["path"])
                    self.fault("before_commit")
                    self.store.put(key, commit)
                self.fault("after_commit")
                self.metrics.append(
                    dict(batch_id=batch_id, logical_rows=count, seconds=monotonic() - began)
                )
        finally:
            parsed.unpersist(blocking=True)


@contextmanager
def committed_raw(session, store, query, expected, *, complete=False, artifacts=None):
    from pyspark.sql import functions as functions

    paths = []
    for commit in store.inventory(query):
        if artifacts is not None and not set(artifacts).intersection(
            commit["source_artifact_sha256s"]
        ):
            continue
        for identity in commit["provenance_ids"]:
            source = DnsReference.model_validate(store.get(f"sources/{identity}"))
            if (
                source.manifest.manifest_id != identity
                or source.manifest.artifacts[0].sha256 not in commit["source_artifact_sha256s"]
            ):
                raise ValueError("DNS committed provenance missing/conflicting")
        path = store.committed_path(commit, query)
        frame = session.read.schema(schema(raw_schema())).parquet(path)
        if digest(frame) != (commit["logical_rows"], commit["projection_sha256"]):
            raise ValueError("DNS committed physical projection corruption")
        paths.append(path)
    physical = (
        session.read.schema(schema(raw_schema())).parquet(*paths)
        if paths
        else session.createDataFrame([], schema(raw_schema()))
    )
    if artifacts is not None:
        physical = physical.where(functions.col("artifact_sha256").isin(artifacts))
    with verify_projection(physical, expected, complete=complete) as logical:
        yield logical


def observed_groups(raw, reference):
    from pyspark.sql import functions as functions

    names = ["query_name", "query_type", "response_name", "response_type"]
    selected = raw.where("disposition='selected_address'")
    for field in ("query_name", "response_name"):
        selected = selected.withColumn(
            field, functions.lower(functions.regexp_replace(functions.col(field), r"\.+$", ""))
        )
    known_time = functions.col("timestamp_ms").between(0, 253402300799999)
    grouped = selected.groupBy(*names).agg(
        functions.sort_array(functions.collect_set("address_value")).alias("addresses"),
        functions.sort_array(functions.collect_set("worker_id")).alias("workers"),
        functions.min(functions.when(known_time, functions.col("timestamp_ms"))).alias(
            "minimum_timestamp_ms"
        ),
        functions.max(functions.when(known_time, functions.col("timestamp_ms"))).alias(
            "maximum_timestamp_ms"
        ),
        functions.max(functions.when(functions.col("worker_id").isNull(), 1).otherwise(0)).alias(
            "unknown_worker"
        ),
        functions.max(functions.when(known_time, 0).otherwise(1)).alias("unknown_time"),
        functions.count("*").alias("contributors"),
    )
    if grouped.where("contributors>4096").limit(1).count():
        raise ValueError("DNS group contributor bound")
    source = reference.manifest
    artifact = source.artifacts[0].sha256
    day = reference.partition_date.isoformat()
    policy = reference.policy

    def project(rows):
        for row in rows:
            key = json.dumps([row[name] for name in names], separators=(",", ":"))
            identity = logical_identity(
                "dns_group_observation",
                policy,
                (source.source_id, source.source_scope, artifact, day, key),
            )
            addresses = json.dumps(row.addresses, separators=(",", ":"))
            value_id = sha256(
                json.dumps(["dns-address-values-v1", row.addresses], separators=(",", ":")).encode()
            ).hexdigest()
            record = dict(
                group_id=identity,
                group_key=key,
                artifact_sha256=artifact,
                partition_date=day,
                query_name=row.query_name,
                query_type=row.query_type,
                response_name=row.response_name,
                response_type=row.response_type,
                value_set_id=value_id,
                addresses_json=addresses,
                workers_json=json.dumps(row.workers, separators=(",", ":")),
                minimum_timestamp_ms=row.minimum_timestamp_ms,
                maximum_timestamp_ms=row.maximum_timestamp_ms,
                unknown_worker=row.unknown_worker,
                unknown_time=row.unknown_time,
                contributors=row.contributors,
                entity_coverage="unknown",
                vantage="unverified",
            )
            yield tuple(record.values())

    return raw.sparkSession.createDataFrame(
        grouped.rdd.mapPartitions(project), schema(group_schema())
    ), selected


def start_query(session, processor, bootstrap, checkpoint, topic, *, max_chunks=256):
    from messaging.dns import validate_topic

    validate_topic(topic)
    source = (
        session.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", max_chunks)
        .load()
    )
    return (
        source.writeStream.foreachBatch(processor.process)
        .option("checkpointLocation", str(checkpoint))
        .trigger(availableNow=True)
        .start()
    )
