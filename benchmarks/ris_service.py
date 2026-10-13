"""Explicit staged RIS service run. No downloads, no VM changes, no implicit full-window run."""

import argparse
import json
import os
import subprocess
import sys
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
    if files == 12 and policy.get("full_hour_service") != "OWNER_APPROVED_STAGE_E_ONE_RUN":
        raise ValueError("RIS another full-hour attempt requires new owner approval")
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


def finalize_history(
    session,
    store,
    query_id,
    expected,
    references,
    summaries,
    exclusions,
    *,
    inventory_sha256,
    partitions=64,
    check=lambda: None,
    fault=lambda point: None,
    publish_completion=True,
):
    """Read immutable events; publish completion only after exact source and query checks."""
    from pyspark.sql import functions as functions

    from analytics.ris import event_summary, peer_activity, provenance, query_events
    from lake.ris import metadata_digest, publish_accounting, publish_window, read_window
    from schemas.ris import RisReference
    from spark.ris import projection_digest, source_order_digest, verified_committed_events

    def inventory_check():
        check()
        inventory = sorted(store.inventory(query_id), key=lambda item: item["batch_id"])
        if metadata_digest(inventory) != inventory_sha256:
            raise ValueError("RIS recovery committed inventory mismatch")

    inventory_check()
    if (
        not 1 <= len(references) <= 12
        or set(references) != set(summaries)
        or set(references) != set(exclusions)
    ):
        raise ValueError("RIS recovery source selection mismatch")
    for artifact, reference in references.items():
        if reference.manifest.artifacts[0].sha256 != artifact:
            raise ValueError("RIS recovery artifact binding mismatch")
        stored = RisReference.model_validate(store.get(f"sources/{reference.manifest.manifest_id}"))
        if stored != reference or summaries[artifact].get("oracle_level") != 3:
            raise ValueError("RIS recovery pinned provenance/oracle mismatch")
    sealed = read_window(store, query_id) if store.get(f"windows/{query_id}") is not None else None
    report = {"mode": "retained committed history finalization", "reconciliation": {}}
    previous_partitions = session.conf.get("spark.sql.shuffle.partitions")
    session.conf.set("spark.sql.shuffle.partitions", str(partitions))
    try:
        with verified_committed_events(
            session,
            store,
            query_id,
            expected,
            partitions=partitions,
            metrics=report["reconciliation"],
        ) as logical:
            inventory_check()
            source_began = monotonic()
            for artifact, summary in summaries.items():
                source = logical.where(functions.col("artifact_sha256") == artifact)
                if (
                    source.count() != summary["accepted_occurrences"]
                    or source_order_digest(source) != summary["source_order_projection_sha256"]
                ):
                    raise ValueError("RIS recovery source-order projection mismatch")
                check()
            report["source_verification_seconds"] = monotonic() - source_began
            digest_began = monotonic()
            count, digest = projection_digest(logical)
            report["projection_digest_seconds"] = monotonic() - digest_began
            if sealed is not None and (count, digest) != (
                sealed["logical_occurrences"],
                sealed["projection_sha256"],
            ):
                raise ValueError("RIS recovery sealed projection mismatch")
            query_began = monotonic()
            report["query_summary"] = event_summary(logical)
            if report["query_summary"] != event_summary(expected):
                raise ValueError("RIS recovery historical queries mismatch")
            for left, right in ((logical, expected), (expected, logical)):
                if peer_activity(left).exceptAll(peer_activity(right)).limit(1).count():
                    raise ValueError("RIS recovery peer query mismatch")
            probe = expected.orderBy("logical_record_id").first()
            if probe is not None:
                for arguments in (
                    {"prefix": probe.prefix},
                    {"start": probe.source_seconds, "end": probe.source_seconds + 60},
                ):
                    if (
                        query_events(logical, **arguments).count()
                        != query_events(expected, **arguments).count()
                    ):
                        raise ValueError("RIS recovery interval/prefix query mismatch")
                if (
                    provenance(logical, probe.logical_record_id).first()
                    != provenance(expected, probe.logical_record_id).first()
                ):
                    raise ValueError("RIS recovery coordinate query mismatch")
            inventory_check()
            report["query_validation_seconds"] = monotonic() - query_began
            report.update(
                logical_occurrences=count,
                projection_sha256=digest,
                exact_missing_identities=0,
                exact_unexpected_identities=0,
            )
            if not publish_completion:
                report["mode"] = "read-only retained history verification"
                return report
            fault("before_accounting")
            metadata_began = monotonic()
            report["source_accounting"] = []
            for artifact, reference in references.items():
                descriptor = publish_accounting(
                    store, reference, summaries[artifact], exclusions[artifact]
                )
                report["source_accounting"].append(descriptor.model_dump(mode="json"))
                fault("after_accounting")
                check()
            inventory_check()
            fault("before_window")
            report["window_descriptor"] = publish_window(
                store, query_id, list(references), count, digest
            )
            report["metadata_publication_seconds"] = monotonic() - metadata_began
            report.update(
                logical_occurrences=count,
                projection_sha256=digest,
                exact_missing_identities=0,
                exact_unexpected_identities=0,
            )
            return report
    finally:
        session.conf.set("spark.sql.shuffle.partitions", previous_partitions)


def checkpoint_digest(path):
    digest = sha256()
    if not path.is_dir():
        raise ValueError("RIS recovery checkpoint missing")
    for entry in sorted(path.rglob("*")):
        if entry.is_file():
            digest.update(str(entry.relative_to(path)).encode() + b"\0")
            with entry.open("rb") as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(block)
    return digest.hexdigest()


def approve_recovery(policy, previous, checkpoint):
    from uuid import UUID

    from lake.ris import safe_key

    query_id = previous["query_id"]
    UUID(query_id)
    safe_key(previous["lake_prefix"])
    if (
        policy.get("authorization") != "OWNER_APPROVED_ONE_FINALIZATION_ONLY"
        or policy.get("query_id") != query_id
        or policy.get("lake_prefix") != previous["lake_prefix"]
        or previous["lake_prefix"] != f"phase18-closure/{query_id}"
        or policy.get("mode") != "finalization_only"
        or policy.get("source_selection") != previous["source_selection"]
        or len(previous["source_selection"]) not in {1, 3, 12}
        or len(set(previous["source_selection"])) != len(previous["source_selection"])
        or (
            len(previous["source_selection"]) != 12
            and policy.get("publish_completion") is not False
        )
        or type(policy.get("max_seconds")) is not int
        or not 1 <= policy["max_seconds"] <= 7200
        or policy.get("checkpoint_sha256") != checkpoint_digest(checkpoint)
        or previous.get("broker_high_watermarks") != previous.get("spark_end_offsets")
        or previous.get("final_broker_lag") != 0
    ):
        raise ValueError("RIS recovery authorization/checkpoint/source binding mismatch")
    commits = [
        int(path.name) for path in (checkpoint / "commits").iterdir() if path.name.isdecimal()
    ]
    if not commits:
        raise ValueError("RIS recovery checkpoint has no acknowledged batch")
    last = max(commits)
    offset = (checkpoint / "offsets" / str(last)).read_text().splitlines()
    if (
        len(offset) != 3
        or kafka_offsets(offset[-1], previous["topic"]) != previous["spark_end_offsets"]
    ):
        raise ValueError("RIS recovery checkpoint offset mismatch")
    from pydantic import TypeAdapter

    from schemas.temporal.base import Digest

    TypeAdapter(Digest).validate_python(policy.get("inventory_sha256"))
    return last


def finalize_main():
    parser = argparse.ArgumentParser(
        description="Authorized retained-history finalization; never ingests"
    )
    parser.add_argument("--finalize-only", action="store_true", required=True)
    parser.add_argument("--previous", type=Path, required=True)
    parser.add_argument("--approval", type=Path, required=True)
    parser.add_argument("--oracle", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--read-only", action="store_true")
    options = parser.parse_args()
    previous = json.loads((options.previous / "result.json").read_text())
    policy = json.loads(options.approval.read_text())
    checkpoint = options.previous / "checkpoint"
    last_batch = approve_recovery(policy, previous, checkpoint)
    if policy.get("publish_completion") is not (not options.read_only):
        raise ValueError("RIS recovery publication mode lacks explicit authorization")
    if options.output.exists():
        parser.error("Fresh recovery evidence directory required")
    pinned = json.loads(
        Path("workloads/results/phase18-closure/occurrence-ledger-validation.json").read_text()
    )
    selection = json.loads(Path("workloads/results/dataset-feasibility/bgp.json").read_text())[
        "artifacts"
    ][: len(previous["source_selection"])]
    if previous["source_selection"] != [item["sha256"] for item in selection]:
        raise ValueError("RIS recovery requires exact pinned source window")
    from schemas.ris import RisReference

    references, summaries, exclusions, paths = {}, {}, {}, []
    frozen_errors = json.loads(
        Path("workloads/results/phase18/parser-exclusions.json").read_text()
    )["records"]
    for item, frozen in zip(selection, pinned["archives"][: len(selection)], strict=True):
        root = options.oracle / item["file"]
        reference = RisReference.model_validate_json((root / "reference.json").read_text())
        summary = json.loads((root / "summary.json").read_text())
        if reference.manifest.artifacts[0].sha256 != item["sha256"] or any(
            summary.get(key) != value for key, value in frozen.items()
        ):
            raise ValueError(
                "RIS recovery oracle metadata differs from frozen independent evidence"
            )
        references[item["sha256"]] = reference
        summaries[item["sha256"]] = summary
        exclusions[item["sha256"]] = json.loads((root / "exclusions.json").read_text())
        fields = (
            "artifact_sha256",
            "byte_offset",
            "all_record_index",
            "category",
            "fully_framed",
            "subtype",
        )
        actual_errors = [
            tuple(row[key] for key in fields) + (row["type"], row["independent_eligible_prefixes"])
            for row in exclusions[item["sha256"]]
        ]
        expected_errors = [
            tuple(row[key] for key in fields)
            + (row["record_type"], row["minimal_oracle_prefix_occurrences"])
            for row in frozen_errors
            if row["artifact_sha256"] == item["sha256"]
        ]
        if sorted(actual_errors) != sorted(expected_errors):
            raise ValueError("RIS recovery exclusion ledger differs from frozen source coordinates")
        paths.append(str((root / "events.parquet").resolve(strict=True)))
    options.output.mkdir(parents=True)
    from benchmarks.rpki_service import GuestGuard
    from lake.config import LakeConfig
    from lake.ris import EventStore
    from spark.common.config import SparkConfig
    from spark.common.runtime import create_session
    from spark.ris import event_schema

    lake = replace(LakeConfig.from_env(), prefix=previous["lake_prefix"])
    runtime = options.output / "runtime"
    runtime.mkdir()
    (runtime / "ivy").symlink_to(Path(".cache/spark/ivy").resolve(), target_is_directory=True)
    events = options.output / "events"
    events.mkdir()
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        "--conf spark.eventLog.enabled=true --conf spark.eventLog.compress=false "
        f"--conf spark.eventLog.dir={events.resolve().as_uri()} pyspark-shell"
    )
    began = monotonic()
    report = dict(
        status="FAIL",
        mode="retained-history recovery, not fresh ingestion",
        query_id=previous["query_id"],
        last_checkpoint_batch=last_batch,
        code_commit=subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
    )
    write_summary(options.output / "registered-plan.json", policy | report)
    session = create_session(
        SparkConfig(runtime_dir=runtime, lake=lake, master="local[2]", driver_memory="2g"),
        kafka=False,
    )
    guest = GuestGuard(session, options.output)
    resources = ResourceSampler(options.output, session)
    timer = threading.Timer(
        max(0, policy["max_seconds"] - (monotonic() - began)), session.sparkContext.cancelAllJobs
    )
    timer.daemon = True

    def check():
        resources.check()
        guest.check()
        if monotonic() - began >= policy["max_seconds"]:
            raise RuntimeError("RIS recovery preregistered cap exceeded")
        if checkpoint_digest(checkpoint) != policy["checkpoint_sha256"]:
            raise ValueError("RIS retained checkpoint changed")

    timer.start()
    guest.thread.start()
    try:
        with resources:
            check()
            report.update(
                finalize_history(
                    session,
                    EventStore(config=lake),
                    previous["query_id"],
                    session.read.schema(event_schema()).parquet(*paths),
                    references,
                    summaries,
                    exclusions,
                    inventory_sha256=policy["inventory_sha256"],
                    check=check,
                    publish_completion=not options.read_only,
                )
            )
            check()
            report["status"] = (
                "PASS_READ_ONLY_RETAINED_VERIFICATION"
                if options.read_only
                else "PASS_RETAINED_HISTORY_FINALIZATION"
            )
    except Exception as error:
        report["failure_category"] = type(error).__name__
        raise
    finally:
        timer.cancel()
        guest.stop.set()
        guest.thread.join(timeout=20)
        report.update(
            elapsed_seconds=monotonic() - began,
            resource_failure=resources.failure,
            guest_failure=guest.failure,
            peak_sampled_tree_rss_bytes=max(
                (row["process_tree_rss_bytes"] for row in resources.samples), default=None
            ),
            checkpoint_unchanged=checkpoint_digest(checkpoint) == policy["checkpoint_sha256"],
            minimum_guest_free_disk_bytes=min(
                (row["guest_free_disk_bytes"] for row in guest.samples), default=None
            ),
            minimum_guest_available_memory_bytes=min(
                (row["guest_available_memory_bytes"] for row in guest.samples), default=None
            ),
        )
        write_summary(options.output / "result.json", report)
        session.stop()
        tasks = []
        for path in events.rglob("events_*"):
            if path.is_file():
                with path.open() as stream:
                    for line in stream:
                        event = json.loads(line)
                        if event.get("Event") == "SparkListenerTaskEnd":
                            tasks.append(event.get("Task Metrics", {}))
        report["task_metrics"] = {
            "scope": "all finalization jobs including physical digests and queries",
            "tasks": len(tasks),
            "memory_spilled_bytes": sum(row.get("Memory Bytes Spilled", 0) for row in tasks),
            "disk_spilled_bytes": sum(row.get("Disk Bytes Spilled", 0) for row in tasks),
            "shuffle_written_bytes": sum(
                row.get("Shuffle Write Metrics", {}).get("Shuffle Bytes Written", 0)
                for row in tasks
            ),
            "maximum_task_execution_memory_bytes": max(
                (row.get("Peak Execution Memory", 0) for row in tasks), default=None
            ),
        }
        write_summary(options.output / "result.json", report)


def main():
    arguments = argparse.ArgumentParser()
    arguments.add_argument("--files", type=int, choices=[1, 3, 12], required=True)
    arguments.add_argument("--oracle", type=Path, required=True)
    arguments.add_argument("--cache", type=Path, default=Path(".cache/phase13"))
    arguments.add_argument("--output", type=Path, required=True)
    arguments.add_argument("--max-seconds", type=int, choices=[1800, 3600, 7200], default=1800)
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
    from benchmarks.rpki_service import GuestGuard
    from lake.client import ensure_bucket, s3_client
    from lake.config import LakeConfig
    from lake.ris import EventStore
    from messaging.config import KafkaConfig
    from messaging.ris import ensure_topic, publish
    from spark.common.config import SparkConfig
    from spark.common.runtime import create_session
    from spark.ris import (
        RisProcessor,
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
                if highs != ends or result["final_broker_lag"] != 0:
                    raise ValueError("RIS final selected-topic lag is nonzero")
            finally:
                consumer.close()
            from lake.ris import metadata_digest

            validation_start = monotonic()
            result.update(
                finalize_history(
                    session,
                    store,
                    identity,
                    processor.expected,
                    references,
                    {
                        item["sha256"]: summary
                        for item, summary in zip(artifacts, oracle_summaries, strict=True)
                    },
                    {
                        item["sha256"]: json.loads(
                            (options.oracle / item["file"] / "exclusions.json").read_text()
                        )
                        for item in artifacts
                    },
                    inventory_sha256=metadata_digest(
                        sorted(store.inventory(identity), key=lambda item: item["batch_id"])
                    ),
                    check=check,
                )
            )
            result["mode"] = "actual Kafka-Spark-MinIO"
            result["validation_seconds"] = monotonic() - validation_start
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
    finalize_main() if "--finalize-only" in sys.argv else main()
