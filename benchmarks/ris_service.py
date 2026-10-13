"""Explicit staged RIS service run. No downloads, no VM changes, no implicit full-window run."""

import argparse
import json
import subprocess
import threading
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from time import monotonic
from uuid import uuid4

from workloads.ris_oracle import write_summary
from workloads.rpki_resources import ResourceSampler, directory_bytes


def progress_snapshot(progress):
    return dict(
        batch_id=progress["batchId"],
        num_input_rows=progress["numInputRows"],
        duration_ms=dict(progress["durationMs"]),
        sources=[
            dict(start_offset=item["startOffset"], end_offset=item["endOffset"])
            for item in progress["sources"]
        ],
    )


def main():
    arguments = argparse.ArgumentParser()
    arguments.add_argument("--files", type=int, choices=[1, 3, 12], required=True)
    arguments.add_argument("--oracle", type=Path, required=True)
    arguments.add_argument("--cache", type=Path, default=Path(".cache/phase13"))
    arguments.add_argument("--output", type=Path, required=True)
    arguments.add_argument("--max-seconds", type=int, choices=[1800, 3600], default=1800)
    options = arguments.parse_args()
    if options.output.exists():
        arguments.error("Fresh output directory required")
    options.output.mkdir(parents=True)
    selection = json.loads(Path("workloads/results/dataset-feasibility/bgp.json").read_text())
    artifacts = selection["artifacts"][: options.files]
    references = {}
    paths, oracle_summaries = [], []
    from schemas.ris import RisReference

    for artifact in artifacts:
        root = options.oracle / artifact["file"]
        reference = RisReference.model_validate_json((root / "reference.json").read_text())
        if reference.manifest.artifacts[0].sha256 != artifact["sha256"]:
            raise ValueError("Oracle/pinned archive mismatch")
        references[artifact["sha256"]] = reference
        paths.append(str((root / "events.parquet").resolve()))
        oracle_summaries.append(json.loads((root / "summary.json").read_text()))
    identity = str(uuid4())
    result = dict(
        status="FAIL",
        mode="actual Kafka-Spark-MinIO",
        query_id=identity,
        files_requested=options.files,
        runtime_cap_seconds=options.max_seconds,
        source_selection=[item["sha256"] for item in artifacts],
        code_commit=subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        code_hashes={
            name: sha256(Path(name).read_bytes()).hexdigest()
            for name in (
                "processing/ris.py",
                "spark/ris.py",
                "messaging/ris.py",
                "lake/ris.py",
                "schemas/ris.py",
                "benchmarks/ris_service.py",
            )
        },
        resource_policy=dict(
            free_disk_bytes=20 * 1024**3,
            per_run_growth_bytes=15 * 1024**3,
            total_host_experiment_bytes=50 * 1024**3,
            tree_rss_bytes=12 * 1024**3,
            guest_available_memory_bytes=512 * 1024**2,
        ),
        input_source_bytes=sum(item["artifact_bytes"] for item in artifacts),
        input_mrt_bytes=sum(item["decompressed_bytes"] for item in oracle_summaries),
        topic=f"temporal.ris.source.v1.{identity}",
        lake_prefix=f"phase18/{identity}",
        acknowledged=[],
        batches=[],
    )
    write_summary(options.output / "registered-plan.json", result)
    from analytics.ris import event_summary
    from benchmarks.rpki_service import GuestGuard
    from lake.client import ensure_bucket, s3_client
    from lake.config import LakeConfig
    from lake.ris import EventStore
    from messaging.config import KafkaConfig
    from messaging.ris import ensure_topic, publish
    from spark.common.config import SparkConfig
    from spark.common.runtime import create_session
    from spark.ris import RisProcessor, committed_events, exact_projection, start_query

    lake = replace(LakeConfig.from_env(), prefix=result["lake_prefix"])
    kafka = KafkaConfig.from_env()
    ensure_topic(kafka, result["topic"])
    ensure_bucket(lake)
    store = EventStore(config=lake)
    runtime = options.output / "runtime"
    runtime.mkdir()
    (runtime / "ivy").symlink_to(Path(".cache/spark/ivy").resolve(), target_is_directory=True)
    began = monotonic()
    session = create_session(
        SparkConfig(runtime_dir=runtime, lake=lake, master="local[2]", driver_memory="2g")
    )
    processor = RisProcessor(session, store, identity, references, paths)
    guest = GuestGuard(session, options.output)
    resources = ResourceSampler(options.output, session)
    deadline = threading.Timer(
        max(0, options.max_seconds - (monotonic() - began)), session.sparkContext.cancelAllJobs
    )
    deadline.daemon = True
    deadline.start()

    def check():
        resources.check()
        guest.check()
        if monotonic() - began > options.max_seconds:
            session.sparkContext.cancelAllJobs()
            raise RuntimeError("RIS preregistered runtime cap exceeded")

    guest.thread.start()
    try:
        with resources:
            for item in artifacts:
                check()
                acknowledged = publish(
                    options.cache / item["file"],
                    references[item["sha256"]],
                    options.output / f"{item['sha256']}.progress.json",
                    kafka,
                    result["topic"],
                    check=check,
                )
                result["acknowledged"].append(acknowledged)
                write_summary(options.output / "progress.json", result)
                query = start_query(
                    session,
                    processor,
                    kafka.bootstrap_servers,
                    options.output / "checkpoint",
                    result["topic"],
                    max_chunks=256,
                )
                try:
                    while query.isActive:
                        check()
                        query.awaitTermination(2)
                    if query.exception():
                        raise RuntimeError("RIS streaming query failed")
                    result["spark_progress"] = [
                        progress_snapshot(item) for item in query.recentProgress
                    ]
                finally:
                    query.stop()
            validation_start = monotonic()
            logical = committed_events(session, store, identity, processor.expected).cache()
            try:
                exact_projection(logical, processor.expected, complete=True).count()
                result["query_summary"] = event_summary(logical)
                result["exact_missing_identities"] = 0
                result["exact_unexpected_identities"] = 0
                result["validation_seconds"] = monotonic() - validation_start
            finally:
                logical.unpersist()
            check()
            client = s3_client(lake)
            sizes = []
            for page in client.get_paginator("list_objects_v2").paginate(
                Bucket=lake.bucket, Prefix=lake.object_key("")
            ):
                sizes.extend((item["Key"], item["Size"]) for item in page.get("Contents", []))
            result["committed_parquet_bytes"] = sum(
                size for key, size in sizes if key.endswith(".parquet")
            )
            result["object_prefix_bytes"] = sum(size for key, size in sizes)
            result["parquet_files"] = sum(key.endswith(".parquet") for key, size in sizes)
            result["commits"] = len(store.inventory(identity))
            result["physical_rows"] = sum(
                item["logical_records"] for item in store.inventory(identity)
            )
            result["checkpoint_bytes"] = directory_bytes(options.output / "checkpoint")
            result["batches"] = processor.metrics
            result["status"] = "PASS_SELECTED_WINDOW"
    except Exception as error:
        result["failure_category"] = type(error).__name__
        raise
    finally:
        deadline.cancel()
        guest.stop.set()
        guest.thread.join(timeout=20)
        result["elapsed_seconds"] = monotonic() - began
        result["resource_failure"] = resources.failure
        result["guest_failure"] = guest.failure
        result["peak_sampled_tree_rss_bytes"] = max(
            (item["process_tree_rss_bytes"] for item in resources.samples), default=None
        )
        result["minimum_guest_free_disk_bytes"] = min(
            (item["guest_free_disk_bytes"] for item in guest.samples), default=None
        )
        result["minimum_guest_available_memory_bytes"] = min(
            (item["guest_available_memory_bytes"] for item in guest.samples), default=None
        )
        result["host_samples"] = len(resources.samples)
        result["guest_samples"] = len(guest.samples)
        try:
            write_summary(options.output / "result.json", result)
        finally:
            session.stop()
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
