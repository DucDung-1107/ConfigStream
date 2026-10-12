"""Bounded, service-free callback profiling; private data never enters reports."""

import argparse
import json
import os
from collections import defaultdict
from contextlib import ExitStack, contextmanager
from pathlib import Path
from statistics import median
from time import monotonic
from unittest.mock import patch
from uuid import uuid4


def comparison(runs):
    result = {}
    for mode in ("baseline", "reuse"):
        selected = [run for run in runs if run["mode"] == mode]
        seconds = [run["processing_seconds"] for run in selected if run["status"] == "PASS"]
        result[mode] = {
            "runs": len(selected),
            "failures": len(selected) - len(seconds),
            "individual_seconds": seconds,
            "median_seconds": median(seconds) if seconds else None,
            "range_seconds": [min(seconds), max(seconds)] if seconds else None,
        }
    return result


def event_metrics(root):
    totals = defaultdict(int)
    for path in sorted(root.rglob("events_*")):
        if not path.is_file() or path.name.startswith("."):
            continue
        with path.open() as source:
            for line in source:
                event = json.loads(line)
                if event.get("Event") != "SparkListenerTaskEnd":
                    continue
                totals["tasks"] += 1
                metrics = event.get("Task Metrics", {})
                for name in (
                    "Executor Run Time",
                    "Executor CPU Time",
                    "JVM GC Time",
                    "Memory Bytes Spilled",
                    "Disk Bytes Spilled",
                ):
                    totals[name] += metrics.get(name, 0)
                for group, fields in {
                    "Input Metrics": ("Bytes Read", "Records Read"),
                    "Shuffle Read Metrics": ("Remote Bytes Read", "Local Bytes Read"),
                    "Shuffle Write Metrics": ("Shuffle Bytes Written",),
                }.items():
                    for field in fields:
                        totals[f"{group}/{field}"] += metrics.get(group, {}).get(field, 0)
    return dict(totals)


@contextmanager
def profile_callbacks():
    import spark.rpki as transforms
    import spark.rpki_stream as stream

    measurements = defaultdict(lambda: {"calls": 0, "seconds": 0.0, "failures": 0})
    visits = {"committed_segment_visits": 0, "declared_rows_checksum_visited": 0}

    def instrument(name, original):
        def measured(*args, **kwargs):
            started = monotonic()
            measurements[name]["calls"] += 1
            try:
                if name == "committed_rows":
                    _, store, query = args[:3]
                    active = kwargs.get("observation_ids")
                    for commit in store.list(f"raw/{query}"):
                        if active is None or set(
                            commit.get("observation_ids", active)
                        ).intersection(active):
                            visits["committed_segment_visits"] += 1
                            visits["declared_rows_checksum_visited"] += commit["rows"]
                return original(*args, **kwargs)
            except Exception:
                measurements[name]["failures"] += 1
                raise
            finally:
                measurements[name]["seconds"] += monotonic() - started

        return measured

    with ExitStack() as stack:
        for name in (
            "committed_rows",
            "reconcile_rows",
            "projection_digest",
            "state_digest",
            "publish_raw",
            "finalize_day",
            "publish_boundary",
            "write_parquet",
        ):
            wrapper = instrument(name, getattr(transforms, name))
            stack.enter_context(patch.object(transforms, name, wrapper))
            if hasattr(stream, name):
                stack.enter_context(patch.object(stream, name, wrapper))
        yield measurements, visits


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["baseline", "reuse"], default="baseline")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=8192)
    parser.add_argument("--batch-rows", type=int, default=1024)
    args = parser.parse_args()
    if not 256 <= args.rows <= 10000 or not 256 <= args.batch_rows <= 8192:
        parser.error("Bounded partial-day probe only")
    if args.output.exists():
        parser.error("Fresh output directory required; prior results must remain immutable")
    args.output.mkdir(parents=True)
    events = args.output.resolve() / "events"
    events.mkdir()
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f'--conf spark.eventLog.enabled=true --conf spark.eventLog.dir="{events.as_uri()}" '
        "--conf spark.eventLog.compress=false pyspark-shell"
    )
    from lake.rpki import LocalStore
    from spark.common.config import SparkConfig
    from spark.common.runtime import create_session
    from spark.rpki import committed_rows
    from spark.rpki_stream import RpkiProcessor
    from workloads.rpki_main import catalog, prepare
    from workloads.rpki_resources import ResourceSampler, directory_bytes
    from workloads.rpki_source import disk_guard

    disk_guard(args.output)
    oracle = Path(".cache/phase17/oracle")
    selection = json.loads(Path("workloads/results/dataset-feasibility/rpki.json").read_text())
    summary = json.loads((oracle / "summary.json").read_text())
    references = catalog(selection, summary, 1)
    result = {
        "mode": args.mode,
        "status": "FAIL",
        "service_integration": "NOT_RUN",
        "input": selection["artifacts"][0],
        "requested_rows": args.rows,
        "batch_rows": args.batch_rows,
        "cold_jvm": True,
        "os_cache": "uncontrolled; input not evicted",
        "resource_limits": {
            "driver_heap_bytes": 2 * 1024**3,
            "tree_rss_bytes": 12 * 1024**3,
            "min_free_disk_bytes": 20 * 1024**3,
            "run_growth_bytes": 15 * 1024**3,
        },
    }
    started = monotonic()
    preparation = monotonic()
    result["source"] = prepare(
        selection, references, Path(".cache/phase13"), args.output / "input", args.rows
    )
    result["prepare_seconds"] = monotonic() - preparation
    session = create_session(
        SparkConfig(master="local[2]", driver_memory="2g", runtime_dir=args.output / "runtime"),
        kafka=False,
    )
    session.conf.set("spark.sql.shuffle.partitions", "8")
    store = LocalStore(args.output / "lake")
    query = str(uuid4())
    result["query_id"] = query
    processor = RpkiProcessor(
        session,
        store,
        query,
        references,
        oracle,
        summary["boundaries"],
        reuse_verified_rows=args.mode == "reuse",
    )
    resources = ResourceSampler(args.output, session)
    resources.__enter__()
    try:
        from pyspark.sql import functions as functions

        chunks = session.read.text(str(args.output / "input")).withColumn(
            "group",
            functions.floor(
                (functions.get_json_object("value", "$.first_row").cast("long") - 1)
                / args.batch_rows
            ),
        )
        groups = chunks.select("group").distinct().collect()
        costs = []
        with profile_callbacks() as (measurements, visits):
            tick = monotonic()
            for batch_id, group in enumerate(sorted(row.group for row in groups)):
                batch = chunks.filter(functions.col("group") == group).select("value")
                callback = monotonic()
                processor(batch, batch_id)
                resources.check()
                costs.append(monotonic() - callback)
            result["processing_seconds"] = monotonic() - tick
        result["function_measurements"] = dict(measurements)
        result["ledger_visits"] = visits
        result["callback_seconds"] = costs
        verified = committed_rows(session, store, query)
        result["accepted_logical_rows"] = verified.count()
        result["complete_days"] = len(store.list("state"))
        expected = session.read.parquet(str(oracle / "source_maps/2026-09-01.parquet")).filter(
            functions.col("row_index") <= result["source"]["source_rows"]
        )
        keys = ["row_index", "member_encoding"]
        if (
            verified.select(*keys).join(expected, keys, "left_anti").count()
            or expected.join(verified.select(*keys), keys, "left_anti").count()
        ):
            raise ValueError("Independent partial source-map mismatch")
        if (
            result["accepted_logical_rows"] != result["source"]["source_rows"]
            or result["complete_days"]
        ):
            raise ValueError("Partial-day accounting/completeness mismatch")
        result["source_map_digest_reference"] = summary["days"][0]["source_map_digest"]
        resources.check()
        result["status"] = "PASS"
    finally:
        resources.__exit__()
        session.stop()
        result["elapsed_seconds"] = monotonic() - started
        result["spark_task_metrics"] = event_metrics(events)
        result["resource_failure"] = resources.failure
        result["peak_sampled_tree_rss_bytes"] = max(
            (sample["process_tree_rss_bytes"] or 0 for sample in resources.samples), default=None
        )
        result["min_free_disk_bytes"] = min(
            (sample["free_disk_bytes"] for sample in resources.samples), default=None
        )
        result["output_bytes"] = directory_bytes(args.output)
        result["parquet_bytes"] = sum(
            path.stat().st_size for path in (args.output / "lake").rglob("*.parquet")
        )
        result["commit_count"] = len(list((args.output / "lake").rglob("*.json")))
        (args.output / "result.json").write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
