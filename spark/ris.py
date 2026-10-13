"""Native append-only occurrences with exact oracle validation and committed logical reads."""

import json
from contextlib import contextmanager
from hashlib import sha256
from time import monotonic
from uuid import UUID

from schemas.ris import EventChunk, EventRow, RisReference, validate_topic


def event_schema():
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    integers = {
        "byte_offset",
        "all_record_index",
        "expansion_ordinal",
        "source_seconds",
        "source_microseconds",
        "peer_asn",
        "afi",
        "safi",
        "path_id",
    }
    return StructType(
        [
            StructField(
                name,
                LongType() if name in integers else StringType(),
                name in {"source_microseconds", "path_id"},
            )
            for name in EventRow.model_fields
        ]
    )


def parse_partition(values, references):
    parsed = {key: RisReference.model_validate(value) for key, value in references.items()}
    for value in values:
        if value.value is None or len(value.value.encode()) > 1024**2:
            raise ValueError("Invalid RIS chunk size")
        try:
            chunk = EventChunk.model_validate_json(value.value)
            reference = parsed[chunk.artifact_sha256]
            chunk.validate_reference(reference)
        except (ValueError, KeyError):
            raise ValueError("Invalid RIS chunk or source binding") from None
        for row in chunk.rows:
            yield tuple(row.model_dump().values())


def exact_projection(actual, expected, *, complete=False):
    from pyspark.sql import functions as functions

    columns = list(EventRow.model_fields)
    distinct = actual.select(*columns).dropDuplicates()
    if (
        distinct.groupBy("logical_record_id")
        .count()
        .where(functions.col("count") > 1)
        .limit(1)
        .count()
    ):
        raise ValueError("RIS occurrence identity conflict")
    if distinct.exceptAll(expected.select(*columns)).limit(1).count():
        raise ValueError("RIS unexpected/conflicting source projection")
    if complete and expected.select(*columns).exceptAll(distinct).limit(1).count():
        raise ValueError("RIS missing source occurrences")
    return distinct


def reconciliation_input(actual, expected, partitions=64):
    """Co-locate each identity across both inputs, including incorrect artifact bindings."""
    from pyspark.sql import functions as functions

    if type(partitions) is not int or not 2 <= partitions <= 256:
        raise ValueError("RIS reconciliation partition bound")
    columns = list(EventRow.model_fields)
    tagged = (
        actual.select(*columns)
        .withColumn("_origin", functions.lit(0))
        .unionByName(expected.select(*columns).withColumn("_origin", functions.lit(1)))
    )
    return tagged.repartition(partitions, "logical_record_id").sortWithinPartitions(
        "logical_record_id"
    )


def reconcile_sorted(rows):
    """One full projection and constant flags per identity; never buffer a duplicate group."""
    previous = None
    exemplar = None
    actual_seen = expected_seen = conflict = False

    def outcome():
        status = (
            "conflict"
            if conflict
            else "accepted"
            if actual_seen and expected_seen
            else "unexpected"
            if actual_seen
            else "missing"
        )
        return (*exemplar, status)

    for row in rows:
        projection, origin = tuple(row[:-1]), row[-1]
        identity = projection[0]
        if origin not in (0, 1) or not isinstance(identity, str):
            raise ValueError("RIS invalid reconciliation input")
        if previous is not None and identity < previous:
            raise ValueError("RIS reconciliation requires sorted identities")
        if identity != previous:
            if exemplar is not None:
                yield outcome()
            previous, exemplar = identity, projection
            actual_seen = expected_seen = conflict = False
        elif projection != exemplar:
            conflict = True
        actual_seen = actual_seen or origin == 0
        expected_seen = expected_seen or origin == 1
    if exemplar is not None:
        yield outcome()


@contextmanager
def bounded_projection(actual, expected, *, complete=True, partitions=64, metrics=None):
    """Exact full-row comparison with spillable sorting and disk-only validated output."""
    from pyspark import StorageLevel
    from pyspark.sql.types import StringType, StructField, StructType

    began = monotonic()
    ordered = reconciliation_input(actual, expected, partitions)
    schema = StructType([*event_schema().fields, StructField("_status", StringType(), False)])
    checked = actual.sparkSession.createDataFrame(
        ordered.rdd.mapPartitions(reconcile_sorted), schema
    ).persist(StorageLevel.DISK_ONLY)
    try:
        counts = {row._status: row["count"] for row in checked.groupBy("_status").count().collect()}
        if metrics is not None:
            metrics.update(
                partitions=partitions,
                counts=counts,
                reconciliation_seconds=monotonic() - began,
            )
        if counts.get("conflict", 0):
            raise ValueError("RIS occurrence identity conflict")
        if counts.get("unexpected", 0):
            raise ValueError("RIS unexpected/conflicting source projection")
        if complete and counts.get("missing", 0):
            raise ValueError("RIS missing source occurrences")
        yield checked.where("_status = 'accepted'").select(*EventRow.model_fields)
    finally:
        checked.unpersist(blocking=True)


def ordered_rows(frame, columns, *, partition_rows=32768):
    from pyspark import StorageLevel
    from pyspark.sql import functions as functions

    if type(partition_rows) is not int or not 1 <= partition_rows <= 32768:
        raise ValueError("RIS digest partition row bound")
    count = frame.count()
    if not count:
        return
    partitions = (count + partition_rows - 1) // partition_rows
    if partitions > 4096:
        raise ValueError("RIS digest partition count bound")
    ordered = frame.repartitionByRange(partitions, *columns).sortWithinPartitions(*columns)
    ordered.persist(StorageLevel.DISK_ONLY)
    try:
        maximum = (
            ordered.groupBy(functions.spark_partition_id())
            .count()
            .agg(functions.max("count").alias("maximum"))
            .first()
            .maximum
        )
        if maximum > 4 * partition_rows:
            raise ValueError("RIS digest partition skew exceeds bounded iteration")
        yield from ordered.toLocalIterator(prefetchPartitions=False)
    finally:
        ordered.unpersist()


def projection_digest(frame, *, partition_rows=32768):
    digest = sha256()
    count = 0
    for row in ordered_rows(frame, ["logical_record_id"], partition_rows=partition_rows):
        encoded = json.dumps(row.asDict(), sort_keys=True, separators=(",", ":")).encode()
        digest.update(encoded + b"\n")
        count += 1
    return count, digest.hexdigest()


def committed_physical_events(session, store, query_id):
    commits = store.inventory(query_id)
    if not commits:
        return session.createDataFrame([], event_schema())
    paths = []
    for commit in commits:
        if commit.get("format_version") == "ris-1.1":
            for manifest_id in commit["provenance_ids"]:
                reference = store.get(f"sources/{manifest_id}")
                if reference is None or (
                    RisReference.model_validate(reference).manifest.manifest_id != manifest_id
                ):
                    raise ValueError("RIS committed provenance missing/corrupt")
                pinned = RisReference.model_validate(reference)
                if pinned.manifest.artifacts[0].sha256 not in commit["source_artifact_sha256s"]:
                    raise ValueError("RIS committed provenance artifact conflict")
        frame = session.read.schema(event_schema()).parquet(store.committed_path(commit, query_id))
        count, digest = projection_digest(frame)
        if count != commit["logical_records"] or digest != commit["projection_sha256"]:
            raise ValueError("RIS committed physical projection corruption")
        paths.append(store.committed_path(commit, query_id))
    return session.read.schema(event_schema()).parquet(*paths)


def committed_events(session, store, query_id, expected):
    return exact_projection(committed_physical_events(session, store, query_id), expected)


@contextmanager
def verified_committed_events(session, store, query_id, expected, *, partitions=64, metrics=None):
    began = monotonic()
    physical = committed_physical_events(session, store, query_id)
    if metrics is not None:
        metrics["physical_readback_seconds"] = monotonic() - began
    with bounded_projection(physical, expected, partitions=partitions, metrics=metrics) as logical:
        yield logical


def source_order_digest(frame, *, partition_rows=32768):
    digest = sha256()
    for row in ordered_rows(
        frame, ["byte_offset", "expansion_ordinal"], partition_rows=partition_rows
    ):
        digest.update(
            json.dumps(row.asDict(), sort_keys=True, separators=(",", ":")).encode() + b"\n"
        )
    return digest.hexdigest()


def completed_events(session, store, query_id, expected):
    from lake.ris import read_window

    descriptor = read_window(store, query_id)
    with verified_committed_events(session, store, query_id, expected) as result:
        count, digest = projection_digest(result)
        if (count, digest) != (
            descriptor["logical_occurrences"],
            descriptor["projection_sha256"],
        ):
            raise ValueError("RIS complete-window physical projection mismatch")
        return result


class RisProcessor:
    def __init__(
        self, session, store, query_id, references, oracle_paths, *, fault=None, scoped_oracle=False
    ):
        UUID(query_id)
        self.session, self.store, self.query_id = session, store, query_id
        self.references = {key: value.model_dump(mode="json") for key, value in references.items()}
        for reference in references.values():
            store.put(
                f"sources/{reference.manifest.manifest_id}", reference.model_dump(mode="json")
            )
        self.expected = session.read.schema(event_schema()).parquet(*oracle_paths)
        self.fault = fault or (lambda point: None)
        self.metrics = []
        self.scoped_oracle = scoped_oracle

    def process(self, batch, batch_id):
        from pyspark import StorageLevel
        from pyspark.sql import functions as functions

        began = monotonic()
        references = self.references
        parsed = self.session.createDataFrame(
            batch.selectExpr("CAST(value AS STRING) AS value").rdd.mapPartitions(
                lambda values: parse_partition(values, references)
            ),
            event_schema(),
        ).persist(StorageLevel.MEMORY_AND_DISK)
        validated = None
        try:
            input_rows = parsed.count()
            if not input_rows:
                return
            artifacts = [
                row.artifact_sha256
                for row in parsed.select("artifact_sha256").distinct().limit(13).collect()
            ]
            if len(artifacts) > 12 or not set(artifacts) <= set(self.references):
                raise ValueError("RIS micro-batch artifact metadata bound")
            expected = (
                self.expected.where(functions.col("artifact_sha256").isin(artifacts))
                if self.scoped_oracle
                else self.expected
            )
            verification_start = monotonic()
            validated = exact_projection(parsed, expected).persist(StorageLevel.MEMORY_AND_DISK)
            verification_seconds = monotonic() - verification_start
            digest_start = monotonic()
            count, digest = projection_digest(validated)
            digest_seconds = monotonic() - digest_start
            if self.store.get(f"windows/{self.query_id}") is not None:
                from lake.ris import read_window

                sealed = read_window(self.store, self.query_id)
                if not set(artifacts) <= set(sealed["sources"]):
                    raise ValueError("RIS new source cannot append to sealed window")
                completed_events(self.session, self.store, self.query_id, self.expected).count()
                self.metrics.append(
                    dict(
                        batch_id=batch_id,
                        input_occurrences=input_rows,
                        batch_logical_occurrences=0,
                        sealed_replay_suppressed=count,
                        seconds=monotonic() - began,
                        verification_seconds=verification_seconds,
                        digest_seconds=digest_seconds,
                        publication_seconds=0,
                        oracle_scope_artifacts=artifacts,
                        oracle_scope_mode="sealed_exact_replay",
                        previous_history_scans_in_callback=1,
                    )
                )
                return
            identity = f"{self.query_id}/{batch_id:020d}"
            key = f"batches/{identity}"
            commit = dict(
                format_version="ris-1.1",
                query_id=self.query_id,
                batch_id=batch_id,
                path=self.store.path(identity),
                logical_records=count,
                projection_sha256=digest,
                provenance_ids=sorted(
                    RisReference.model_validate(value).manifest.manifest_id
                    for value in self.references.values()
                ),
                source_artifact_sha256s=sorted(self.references),
            )
            previous = self.store.get(key)
            publication_start = monotonic()
            if previous:
                if previous != commit:
                    raise ValueError("RIS batch replay conflict")
            else:
                self.fault("before_output")
                if self.fault("partial_output"):
                    validated.limit(1).write.mode("overwrite").parquet(commit["path"])
                    raise RuntimeError("Owned RIS interruption after partial output")
                validated.repartition(4).write.mode("overwrite").parquet(commit["path"])
                self.fault("before_commit")
                self.store.put(key, commit)
            self.fault("after_commit")
            self.metrics.append(
                dict(
                    batch_id=batch_id,
                    input_occurrences=input_rows,
                    batch_logical_occurrences=count,
                    seconds=monotonic() - began,
                    verification_seconds=verification_seconds,
                    digest_seconds=digest_seconds,
                    publication_seconds=monotonic() - publication_start,
                    oracle_scope_artifacts=artifacts if self.scoped_oracle else sorted(references),
                    oracle_scope_mode="artifact_filter" if self.scoped_oracle else "baseline_full",
                    previous_history_scans_in_callback=0,
                )
            )
        except Exception as error:
            self.store.put(
                f"errors/{self.query_id}/{batch_id:020d}",
                {
                    "format_version": "ris-error-1.0",
                    "batch_id": batch_id,
                    "failure_category": type(error).__name__,
                    "disposition": "callback_failed_not_source_event_acceptance",
                },
            )
            raise
        finally:
            parsed.unpersist()
            if validated is not None:
                validated.unpersist()


def start_query(session, processor, bootstrap, checkpoint, topic, *, max_chunks=256):
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
