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


def approve_execution(policy, files, seconds):
    if policy.get("service_approvals", {}).get(str(files)) != seconds:
        raise ValueError("RIS selected service runtime lacks explicit owner approval")
    attempt = policy.get("three_archive_attempt")
    if files == 3 and attempt and attempt.get("retry") != "OWNER_APPROVED_SAME_CAP_RETRY":
        raise ValueError("RIS another three-archive attempt requires new owner approval")
    if (
        policy.get("minimum_free_host_and_guest_disk_bytes") != 20 * 1024**3
        or policy.get("per_run_growth_limit_bytes") != 15 * 1024**3
        or policy.get("guest_memory_bytes") != 6 * 1024**3
        or policy.get("guest_disk_bytes") != 64 * 1024**3
    ):
        raise ValueError("RIS execution policy requires a controlled resource amendment")


def progress_snapshot(progress):
    if hasattr(progress, "json"):
        progress = json.loads(progress.json)
    return dict(
        batch_id=progress["batchId"],
        num_input_rows=progress["numInputRows"],
        duration_ms=dict(progress["durationMs"]),
        sources=[
            dict(start_offset=item["startOffset"], end_offset=item["endOffset"])
            for item in progress["sources"]
        ],
    )


def kafka_offsets(value, topic):
    from schemas.ris import validate_topic

    validate_topic(topic)
    if isinstance(value, str):
        if len(value) > 16384:
            raise ValueError("RIS offset metadata bound")
        value = json.loads(value)
    if not isinstance(value, dict) or set(value) != {topic}:
        raise ValueError("RIS offset topic binding mismatch")
    partitions = value[topic]
    if not isinstance(partitions, dict) or not 1 <= len(partitions) <= 2:
        raise ValueError("RIS offset partition bound")
    if any(
        not isinstance(key, str) or not key.isdecimal() or type(offset) is not int or offset < 0
        for key, offset in partitions.items()
    ):
        raise ValueError("RIS invalid source offset")
    return partitions


def main():
    arguments = argparse.ArgumentParser()
    arguments.add_argument("--files", type=int, choices=[1, 3, 12], required=True)
    arguments.add_argument("--oracle", type=Path, required=True)
    arguments.add_argument("--cache", type=Path, default=Path(".cache/phase13"))
    arguments.add_argument("--output", type=Path, required=True)
    arguments.add_argument("--max-seconds", type=int, choices=[1800, 3600], default=1800)
    arguments.add_argument(
        "--execution-policy",
        type=Path,
        default=Path("workloads/results/phase18-closure/execution-policy.json"),
    )
    arguments.add_argument("--scoped-oracle", action="store_true")
    options = arguments.parse_args()
    policy = json.loads(options.execution_policy.read_text())
    approve_execution(policy, options.files, options.max_seconds)
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
        if oracle_summaries[-1].get("oracle_level") != 3:
            raise ValueError("Independent required attributes not accepted before service run")
    identity = str(uuid4())
    result = dict(
        status="FAIL",
        mode="actual Kafka-Spark-MinIO",
        query_id=identity,
        files_requested=options.files,
        runtime_cap_seconds=options.max_seconds,
        execution_policy_sha256=sha256(options.execution_policy.read_bytes()).hexdigest(),
        oracle_mode="artifact_filter" if options.scoped_oracle else "baseline_full",
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
        lake_prefix=f"phase18-closure/{identity}",
        acknowledged=[],
        batches=[],
    )
    write_summary(options.output / "registered-plan.json", result)
    from analytics.ris import event_summary, peer_activity, provenance, query_events
    from benchmarks.rpki_service import GuestGuard
    from lake.client import ensure_bucket, s3_client
    from lake.config import LakeConfig
    from lake.ris import EventStore, publish_accounting, publish_window
    from messaging.config import KafkaConfig
    from messaging.ris import ensure_topic, publish
    from spark.common.config import SparkConfig
    from spark.common.runtime import create_session
    from spark.ris import (
        RisProcessor,
        committed_events,
        exact_projection,
        projection_digest,
        source_order_digest,
        start_query,
    )

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
    processor = RisProcessor(
        session,
        store,
        identity,
        references,
        paths,
        scoped_oracle=options.scoped_oracle,
    )
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
            from confluent_kafka import Consumer, TopicPartition

            consumer = Consumer(
                {
                    "bootstrap.servers": kafka.bootstrap_servers,
                    "group.id": f"ris-closure-audit-{identity}",
                    "enable.auto.commit": False,
                }
            )
            try:
                metadata = consumer.list_topics(result["topic"], timeout=15)
                highs = {
                    str(partition): consumer.get_watermark_offsets(
                        TopicPartition(result["topic"], partition), timeout=15
                    )[1]
                    for partition in metadata.topics[result["topic"]].partitions
                }
                progress = result["spark_progress"][-1]["sources"][0]
                ends = kafka_offsets(progress["end_offset"], result["topic"])
                result["broker_high_watermarks"] = highs
                result["spark_end_offsets"] = ends
                result["final_broker_lag"] = sum(
                    high - ends.get(partition, -1) for partition, high in highs.items()
                )
                if result["final_broker_lag"] != 0:
                    raise ValueError("RIS final selected-topic lag is nonzero")
            finally:
                consumer.close()
            validation_start = monotonic()
            logical = committed_events(session, store, identity, processor.expected).cache()
            try:
                exact_projection(logical, processor.expected, complete=True).count()
                from pyspark.sql import functions as functions

                result["source_accounting"] = []
                for item, summary in zip(artifacts, oracle_summaries, strict=True):
                    source = logical.where(functions.col("artifact_sha256") == item["sha256"])
                    digest = source_order_digest(source)
                    if digest != summary["source_order_projection_sha256"]:
                        raise ValueError("RIS committed archive source-order digest mismatch")
                    exclusions = json.loads(
                        (options.oracle / item["file"] / "exclusions.json").read_text()
                    )
                    descriptor = publish_accounting(
                        store, references[item["sha256"]], summary, exclusions
                    )
                    result["source_accounting"].append(descriptor.model_dump(mode="json"))
                count, digest = projection_digest(logical)
                result["window_descriptor"] = publish_window(
                    store, identity, [item["sha256"] for item in artifacts], count, digest
                )
                result["query_summary"] = event_summary(logical)
                expected_summary = event_summary(processor.expected)
                if result["query_summary"] != expected_summary:
                    raise ValueError("RIS historical aggregate queries disagree with oracle")
                if (
                    peer_activity(logical)
                    .exceptAll(peer_activity(processor.expected))
                    .limit(1)
                    .count()
                ):
                    raise ValueError("RIS peer activity query mismatch")
                probe = processor.expected.orderBy("logical_record_id").first()
                source_start = min(
                    int(item[0])
                    for summary in oracle_summaries
                    for item in summary["events_per_minute"]
                )
                source_end = (
                    max(
                        int(item[0])
                        for summary in oracle_summaries
                        for item in summary["events_per_minute"]
                    )
                    + 60
                )
                for arguments in (
                    {"start": source_start, "end": source_end},
                    {"prefix": probe.prefix},
                ):
                    if (
                        query_events(logical, **arguments).count()
                        != query_events(processor.expected, **arguments).count()
                    ):
                        raise ValueError("RIS interval/prefix query mismatch")
                if (
                    provenance(logical, probe.logical_record_id).first()
                    != provenance(processor.expected, probe.logical_record_id).first()
                ):
                    raise ValueError("RIS coordinate/provenance query mismatch")
                result["query_agreement"] = "exact selected projection plus aggregate queries"
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
            from lake.ris import safe_key

            safe_key(result["topic"])
            selected_logs = subprocess.run(
                [
                    "docker",
                    "exec",
                    "configstream-kafka-1",
                    "bash",
                    "-c",
                    f"du -sb /var/lib/kafka/data/{result['topic']}-*",
                ],
                capture_output=True,
                text=True,
                timeout=15,
                check=True,
            )
            result["selected_broker_log_bytes"] = sum(
                int(line.split()[0]) for line in selected_logs.stdout.splitlines()
            )
            result["source_sidecar_bytes"] = sum(
                size for key, size in sizes if "/exclusions/" in key or "/accounting/" in key
            )
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
        result["batches"] = processor.metrics
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
