"""Bounded Spark techniques over independently specified owned projections."""

import io
import json
import random
from contextlib import contextmanager, redirect_stdout
from pathlib import Path
from statistics import median
from time import perf_counter
from uuid import uuid4

import pandas
from pyspark import StorageLevel
from pyspark.sql import Window
from pyspark.sql import functions as functions
from pyspark.sql.types import DoubleType

FIXTURE = Path(__file__).resolve().parents[2] / "tests/fixtures/final-course/spark.json"


def canonical_name(value):
    return value.lower().rstrip(".") or None if value is not None else None


@functions.pandas_udf(DoubleType())
def unknown_fraction(status: pandas.Series) -> float:
    """Actual grouped pandas aggregate; all rows of one small group must fit RAM."""
    return float((status == "unknown").mean())


@contextmanager
def settings(session, values):
    previous = {key: session.conf.get(key) for key in values}
    try:
        for key, value in values.items():
            session.conf.set(key, value)
        yield
    finally:
        for key, value in previous.items():
            session.conf.set(key, value)


def physical_plan(frame, root):
    output = io.StringIO()
    with redirect_stdout(output):
        frame.explain(mode="formatted")
    return output.getvalue().replace(str(root), "owned-fixture-workspace")


def records(frame, order):
    return [row.asDict() for row in frame.orderBy(*order).collect()]


def scan_metrics(frame):
    """Version-specific diagnostics from executed Spark SQL scan nodes."""
    found = []

    def visit(node):
        values = {}
        iterator = node.metrics().iterator()
        while iterator.hasNext():
            entry = iterator.next()
            values[entry._1()] = entry._2().value()
        if "numFiles" in values:
            found.append({"operator": node.nodeName(), "metrics": values})
        children = node.children().iterator()
        while children.hasNext():
            visit(children.next())

    visit(frame._jdf.queryExecution().executedPlan())
    return found


def run_examples(session, root):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=False)
    fixture = json.loads(FIXTURE.read_text())
    events = session.createDataFrame(
        fixture["events"],
        "id STRING, source_time STRING, peer STRING, action STRING, "
        "fingerprint STRING, record_offset LONG",
    ).withColumn("observed_time", functions.to_timestamp("source_time"))
    measurements = session.createDataFrame(
        fixture["measurements"], "id STRING, worker STRING, query STRING, status STRING"
    )
    result = {"evidence": fixture["evidence"], "plans": {}}
    started = perf_counter()
    wide = (
        events.withColumn("minute", functions.date_format("observed_time", "HH:mm"))
        .groupBy("minute")
        .pivot("action", ["announcement", "withdrawal"])
        .count()
    )
    long = wide.unpivot(["minute"], ["announcement", "withdrawal"], "action", "occurrences")
    cumulative = wide.withColumn(
        "total", functions.col("announcement") + functions.col("withdrawal")
    )
    cumulative = cumulative.withColumn(
        "cumulative", functions.sum("total").over(Window.orderBy("minute"))
    )
    result["aggregation"] = {
        "pivot": records(wide, ["minute"]),
        "unpivot": records(long, ["minute", "action"]),
        "cumulative": [row.cumulative for row in cumulative.orderBy("minute").collect()],
        "statistics": cumulative.agg(
            functions.avg("total").alias("mean"), functions.stddev_pop("total").alias("stddev")
        )
        .first()
        .asDict(),
        "native_occurrences": events.count(),
        "fingerprints": events.select("fingerprint").distinct().count(),
        "seconds": perf_counter() - started,
    }
    result["plans"]["pivot"] = physical_plan(wide, root)
    result["plans"]["window"] = physical_plan(cumulative, root)

    scalar = functions.udf(canonical_name, "string")
    udf_projection = measurements.withColumn("canonical", scalar("query"))
    stripped = functions.regexp_replace(functions.lower("query"), r"[.]+$", "")
    native_projection = measurements.withColumn(
        "canonical", functions.when(functions.length(stripped) > 0, stripped)
    )
    custom = measurements.groupBy("worker").agg(unknown_fraction("status").alias("fraction"))
    result["udf"] = {
        "python": records(udf_projection.select("id", "canonical"), ["id"]),
        "native": records(native_projection.select("id", "canonical"), ["id"]),
        "custom_aggregate": records(custom, ["worker"]),
        "custom_aggregate_group_memory": "All members of a group; bounded owned fixture only",
    }
    result["plans"]["python_udf"] = physical_plan(udf_projection, root)
    result["plans"]["native_expression"] = physical_plan(native_projection, root)
    result["plans"]["custom_aggregate"] = physical_plan(custom, root)

    peers = session.createDataFrame([("peer-a", 64500), ("peer-b", 64501)], "peer STRING, asn LONG")
    actions = session.createDataFrame(
        [("announcement", "observed announcement"), ("withdrawal", "observed withdrawal")],
        "action STRING, label STRING",
    )
    with settings(
        session,
        {"spark.sql.autoBroadcastJoinThreshold": "-1", "spark.sql.adaptive.enabled": "false"},
    ):
        merge = (
            events.hint("merge")
            .join(peers.hint("merge"), "peer")
            .join(actions.hint("merge"), "action")
        )
        broadcast = events.join(functions.broadcast(peers), "peer").join(
            functions.broadcast(actions), "action"
        )
        baseline = merge.filter(functions.col("action") == "announcement").select(
            "id", "asn", "label"
        )
        candidate = (
            events.select("id", "peer", "action")
            .filter(functions.col("action") == "announcement")
            .join(functions.broadcast(peers), "peer")
            .join(functions.broadcast(actions), "action")
            .select("id", "asn", "label")
        )
        result["joins"] = {
            "merge": records(merge.select("id", "asn", "label"), ["id"]),
            "broadcast": records(broadcast.select("id", "asn", "label"), ["id"]),
            "filtered_baseline": records(baseline, ["id"]),
            "filtered_candidate": records(candidate, ["id"]),
            "input_rows": 8,
            "small_dimension_rows": 2,
            "aqe": False,
            "auto_broadcast_threshold": -1,
        }
        for name, frame in {
            "merge_join": merge,
            "broadcast_join": broadcast,
            "multi_join_before": baseline,
            "multi_join_after": candidate,
        }.items():
            result["plans"][name] = physical_plan(frame, root)
        order = ["merge", "broadcast"] * 3
        random.Random(42).shuffle(order)
        timings = []
        for mode in order:
            trial_started = perf_counter()
            rows = records(baseline if mode == "merge" else candidate, ["id"])
            timings.append({"mode": mode, "seconds": perf_counter() - trial_started, "rows": rows})
        result["join_trials"] = {
            "seed": 42,
            "timing_boundary": (
                "Action including plan evaluation and bounded collect; startup excluded; "
                "warm process/uncontrolled OS cache"
            ),
            "trials": timings,
            "medians_seconds": {
                mode: median(row["seconds"] for row in timings if row["mode"] == mode)
                for mode in ["merge", "broadcast"]
            },
            "claim": (
                "Educational physical-strategy comparison; "
                "no production speedup or scalability inference"
            ),
        }

    partitioned = root / "partitioned"
    events.withColumn("source_minute", functions.date_format("observed_time", "HH:mm")).coalesce(
        1
    ).write.partitionBy("source_minute").parquet(str(partitioned))
    all_files = session.read.parquet(str(partitioned)).select("id", "source_minute")
    selected = all_files.filter(functions.col("source_minute") == "00:00")
    result["pruning"] = {
        "all_ids": [row.id for row in all_files.orderBy("id").collect()],
        "selected_ids": [row.id for row in selected.orderBy("id").collect()],
    }
    all_files.collect()
    selected.collect()
    result["pruning"]["all_scan_metrics"] = scan_metrics(all_files)
    result["pruning"]["selected_scan_metrics"] = scan_metrics(selected)
    result["pruning"]["physical_io_bytes"] = "NOT_MEASURED"
    result["plans"]["partition_pruning"] = physical_plan(selected, root)

    cached = events.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        count = cached.count()
        cached_count = cached.groupBy("action").count()
        result["persistence"] = {
            "materialized_rows": count,
            "cached": cached.is_cached,
            "counts": records(cached_count, ["action"]),
        }
        result["plans"]["cached_scan"] = physical_plan(cached_count, root)
    finally:
        cached.unpersist(blocking=True)
    result["persistence"]["released"] = not cached.is_cached

    table_suffix = uuid4().hex
    fact_table, peer_table = f"owned_fact_{table_suffix}", f"owned_peer_{table_suffix}"
    with settings(
        session,
        {"spark.sql.autoBroadcastJoinThreshold": "-1", "spark.sql.adaptive.enabled": "false"},
    ):
        try:
            events.write.bucketBy(2, "peer").sortBy("peer").option(
                "path", str(root / "bucket-events")
            ).saveAsTable(fact_table)
            peers.write.bucketBy(2, "peer").sortBy("peer").option(
                "path", str(root / "bucket-peers")
            ).saveAsTable(peer_table)
            bucketed = (
                session.table(fact_table)
                .hint("merge")
                .join(session.table(peer_table).hint("merge"), "peer")
            )
            result["bucketing"] = {
                "rows": records(bucketed.select("id", "asn"), ["id"]),
                "catalog": session.conf.get("spark.sql.catalogImplementation"),
                "buckets": 2,
                "production_used": False,
            }
            result["plans"]["bucketed_join"] = physical_plan(bucketed, root)
        finally:
            session.sql(f"DROP TABLE IF EXISTS {fact_table}")
            session.sql(f"DROP TABLE IF EXISTS {peer_table}")
    result["partitions"] = {
        "sizes": events.rdd.mapPartitions(lambda entries: [sum(1 for _ in entries)]).collect(),
        "input_rows": 8,
    }
    return result


def stream_modes(session, root):
    import os

    fixture = json.loads(FIXTURE.read_text())
    result = {}
    for mode in ["update", "complete"]:
        source = Path(root) / mode / "input"
        source.mkdir(parents=True)
        first = [{"id": row[0], "action": row[3]} for row in fixture["events"]]
        second = [{"id": "owned-8", "action": "announcement"}]
        for ordinal, batch in enumerate([first, second]):
            path = source / f"{ordinal}.json"
            path.write_text("\n".join(json.dumps(row) for row in batch))
            os.utime(path, (ordinal + 1, ordinal + 1))
        frame = (
            session.readStream.schema("id STRING, action STRING")
            .option("maxFilesPerTrigger", 1)
            .json(str(source))
        )
        batches = []

        def capture(batch, batch_id, collected=batches):
            collected.append({"batch": batch_id, "rows": records(batch, ["action"])})

        query = (
            frame.groupBy("action")
            .count()
            .writeStream.outputMode(mode)
            .foreachBatch(capture)
            .option("checkpointLocation", str(Path(root) / mode / "checkpoint"))
            .trigger(availableNow=True)
            .start()
        )
        try:
            if not query.awaitTermination(90) or query.exception() is not None:
                raise RuntimeError("Owned streaming mode exercise did not finish")
            result[mode] = {
                "batches": batches,
                "progress_input_rows": [batch["numInputRows"] for batch in query.recentProgress],
                "state_key_bound": 2,
                "production_applied": False,
            }
        finally:
            query.stop()
    return result


def verify(result, modes):
    expected = json.loads(FIXTURE.read_text())["expected"]
    aggregate = result["aggregation"]
    for key in ["pivot", "unpivot", "cumulative", "native_occurrences", "fingerprints"]:
        if aggregate[key] != expected[key]:
            raise ValueError(f"Independent owned expectation disagrees: {key}")
    if result["udf"]["python"] != result["udf"]["native"]:
        raise ValueError("Spark UDF and native expression disagree")
    if [row["canonical"] for row in result["udf"]["python"]] != expected["canonical_names"]:
        raise ValueError("Independent canonical names disagree")
    if {row["worker"]: row["fraction"] for row in result["udf"]["custom_aggregate"]} != expected[
        "unknown_fraction"
    ]:
        raise ValueError("Independent custom aggregate disagrees")
    if result["joins"]["merge"] != result["joins"]["broadcast"]:
        raise ValueError("Join strategies disagree")
    if result["joins"]["filtered_baseline"] != result["joins"]["filtered_candidate"]:
        raise ValueError("Multi-join candidate loses evidence")
    if result["pruning"]["selected_ids"] != expected["pruned_ids"]:
        raise ValueError("Pruned source identities disagree")
    for mode in ["update", "complete"]:
        if modes[mode]["batches"][-1]["rows"] != expected[f"stream_{mode}_final"]:
            raise ValueError(f"Output mode {mode} disagrees")
