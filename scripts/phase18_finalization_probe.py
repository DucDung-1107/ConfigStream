"""Owned-only, service-free reconciliation profile; no real archive execution."""

import argparse
import contextlib
import io
import json
import os
from pathlib import Path
from time import monotonic

from schemas.ris import EventRow
from spark.common.config import SparkConfig
from spark.common.runtime import create_session
from spark.ris import bounded_projection, event_schema, reconciliation_input
from workloads.rpki_resources import ResourceSampler


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=100000)
    options = parser.parse_args()
    if options.output.exists() or not 1 <= options.rows <= 100000:
        parser.error("Fresh output and at most 100000 owned rows required")
    options.output.mkdir(parents=True)
    events = options.output / "events"
    events.mkdir()
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--conf spark.eventLog.enabled=true --conf spark.eventLog.compress=false "
        f"--conf spark.eventLog.dir={events.resolve().as_uri()} pyspark-shell"
    )
    session = create_session(
        SparkConfig(runtime_dir=options.output / "runtime", driver_memory="2g", master="local[2]"),
        kafka=False,
    )
    from pyspark.sql import functions as functions

    row = dict(
        logical_record_id="a" * 64,
        artifact_sha256="b" * 64,
        provenance_manifest_id="c" * 64,
        byte_offset=0,
        all_record_index=0,
        expansion_ordinal=0,
        source_seconds=1,
        source_microseconds=None,
        timestamp_precision="second",
        peer_asn=64512,
        peer_ip="192.0.2.1",
        action="announcement",
        afi=1,
        safi=1,
        prefix="198.51.100.0/24",
        path_id=None,
        attribute_digest="d" * 64,
        content_fingerprint="e" * 64,
    )
    seed = session.createDataFrame([tuple(row.values())], event_schema())
    expected = (
        session.range(options.rows)
        .crossJoin(seed)
        .withColumn("logical_record_id", functions.sha2(functions.col("id").cast("string"), 256))
        .select(*EventRow.model_fields)
    )
    actual = expected.unionByName(expected.limit(1)).repartition(2)
    old = actual.dropDuplicates().groupBy("logical_record_id").count().where("count > 1")
    new = reconciliation_input(actual, expected, 16)
    report = {"evidence": "PASS_LOCAL", "owned_rows": options.rows, "retry_rows": 1}
    for name, frame in (("old", old), ("new", new)):
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            frame.explain(mode="formatted")
        (options.output / f"{name}-plan.txt").write_text(buffer.getvalue())
        physical = frame._jdf.queryExecution().executedPlan().toString()
        report[name + "_operators"] = {
            kind: physical.count(kind) for kind in ("HashAggregate", "Sort", "Exchange", "Join")
        }
    report["jvm_max_heap_bytes"] = session._jvm.java.lang.Runtime.getRuntime().maxMemory()
    sampler = ResourceSampler(options.output, session)
    began = monotonic()
    try:
        with sampler:
            sizes = new.rdd.mapPartitions(lambda rows: [sum(1 for _ in rows)]).collect()
            report["sorted_partition_rows"] = sizes
            with bounded_projection(actual, expected, partitions=16, metrics=report) as logical:
                assert logical.count() == options.rows
                assert logical.select("content_fingerprint").distinct().count() == 1
            sampler.check()
        report["elapsed_seconds"] = monotonic() - began
        report["peak_sampled_tree_rss_bytes"] = max(
            row["process_tree_rss_bytes"] for row in sampler.samples
        )
    finally:
        session.stop()
    tasks = []
    for path in events.rglob("events_*"):
        if path.is_file():
            with path.open() as stream:
                for line in stream:
                    event = json.loads(line)
                    if event.get("Event") == "SparkListenerTaskEnd":
                        tasks.append(event.get("Task Metrics", {}))
    report["task_metrics_scope"] = (
        "all probe actions including partition measurement; not old/new matched runtime"
    )
    report["task_count"] = len(tasks)
    report["memory_spilled_bytes"] = sum(row.get("Memory Bytes Spilled", 0) for row in tasks)
    report["disk_spilled_bytes"] = sum(row.get("Disk Bytes Spilled", 0) for row in tasks)
    report["shuffle_written_bytes"] = sum(
        row.get("Shuffle Write Metrics", {}).get("Shuffle Bytes Written", 0) for row in tasks
    )
    report["maximum_task_execution_memory_bytes"] = max(
        (row.get("Peak Execution Memory", 0) for row in tasks), default=None
    )
    (options.output / "result.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
