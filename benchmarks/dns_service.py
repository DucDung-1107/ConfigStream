"""Preregistered sample/one/two-object experiments; exact private reference acceptance."""

import argparse
import json
import subprocess
import threading
from dataclasses import replace
from pathlib import Path
from time import monotonic
from uuid import uuid4

from workloads.rpki_resources import ResourceSampler, directory_bytes


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", choices=["sample", "one", "two"], required=True)
    parser.add_argument("--oracle", type=Path, default=Path(".cache/phase19/oracle"))
    parser.add_argument("--cache", type=Path, default=Path(".cache/phase13"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Fresh experiment output required")
    policy = json.loads(
        Path("workloads/results/phase19/entry-and-execution-policy.json").read_text()
    )
    caps = {"sample": 600, "one": 1800, "two": 3600}
    cap = caps[args.stage]
    chosen = json.loads(Path("workloads/results/dataset-feasibility/dns.json").read_text())["days"][
        : 2 if args.stage == "two" else 1
    ]
    from schemas.dns import DnsReference

    references = {}
    oracle_paths = []
    summaries = {}
    for day in chosen:
        reference = DnsReference.model_validate_json(
            (args.oracle / day["date"] / "reference.json").read_text()
        )
        artifact = day["artifact"]["sha256"]
        if (
            artifact != reference.manifest.artifacts[0].sha256
            or artifact not in policy["source_artifact_sha256s"]
        ):
            raise ValueError("DNS input selection mismatch")
        references[artifact] = reference
        oracle_paths.append(str((args.oracle / day["date"] / "raw.parquet").resolve()))
        summaries[artifact] = json.loads((args.oracle / day["date"] / "summary.json").read_text())
        if (
            summaries[artifact]["source_rows"],
            summaries[artifact]["address_groups"],
            summaries[artifact]["selected_address_rows"],
        ) != (day["rows"], day["address_groups"], day["address_rows"]):
            raise ValueError("DNS independent oracle disagrees with frozen reference")
    args.output.mkdir(parents=True)
    query_id = str(uuid4())
    result = dict(
        status="FAIL",
        stage=args.stage,
        query_id=query_id,
        topic=f"temporal.dns.source.v1.{query_id}",
        lake_prefix=f"phase19/{query_id}",
        runtime_cap_seconds=cap,
        code_commit=subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        source_artifact_sha256s=list(references),
        acknowledged=[],
        source_completion=[],
    )
    (args.output / "registered-plan.json").write_text(json.dumps(result, indent=2))
    from analytics.dns import compare_groups
    from benchmarks.rpki_service import GuestGuard
    from lake.client import ensure_bucket, s3_client
    from lake.config import LakeConfig
    from lake.dns import DnsStore, read_source_completion
    from messaging.config import KafkaConfig
    from messaging.dns import ensure_topic, publish, source_offsets
    from spark.common.config import SparkConfig
    from spark.common.runtime import create_session
    from spark.dns import DnsProcessor, committed_raw, start_query
    from spark.dns_history import finalize_comparison, finalize_source, read_table

    config = replace(LakeConfig.from_env(), prefix=result["lake_prefix"])
    kafka = KafkaConfig.from_env()
    ensure_bucket(config)
    ensure_topic(kafka, result["topic"])
    store = DnsStore(config=config)
    runtime = args.output / "runtime"
    runtime.mkdir()
    (runtime / "ivy").symlink_to(Path(".cache/spark/ivy").resolve(), target_is_directory=True)
    began = monotonic()
    session = create_session(
        SparkConfig(runtime_dir=runtime, lake=config, master="local[2]", driver_memory="2g")
    )
    processor = DnsProcessor(session, store, query_id, references, oracle_paths)
    if args.stage == "sample":
        processor.expected = processor.expected.where("row_group=0 AND row<4096")
    guest = GuestGuard(session, args.output)
    resources = ResourceSampler(args.output, session)
    timer = threading.Timer(max(0, cap - (monotonic() - began)), session.sparkContext.cancelAllJobs)
    timer.daemon = True

    def check():
        resources.check()
        guest.check()
        if monotonic() - began >= cap:
            raise RuntimeError("DNS preregistered runtime cap exceeded")

    timer.start()
    guest.thread.start()
    try:
        with resources:
            for day in chosen:
                artifact = day["artifact"]["sha256"]
                check()
                result["acknowledged"].append(
                    publish(
                        args.cache / day["artifact"]["file"],
                        references[artifact],
                        args.output / (artifact + ".progress.json"),
                        kafka,
                        result["topic"],
                        limit=4096 if args.stage == "sample" else None,
                        check=check,
                    )
                )
                query = start_query(
                    session,
                    processor,
                    kafka.bootstrap_servers,
                    args.output / "checkpoint",
                    result["topic"],
                )
                try:
                    while query.isActive:
                        check()
                        query.awaitTermination(2)
                    if query.exception():
                        raise RuntimeError("DNS streaming query failed")
                    result["last_progress"] = json.loads(query.lastProgress.json)
                finally:
                    query.stop()
            if args.stage == "sample":
                with committed_raw(
                    session, store, query_id, processor.expected, complete=True
                ) as raw:
                    result["sample_rows"] = raw.count()
                result["selected_source_complete"] = False
            else:
                for day in chosen:
                    artifact = day["artifact"]["sha256"]
                    result["source_completion"].append(
                        finalize_source(
                            session,
                            store,
                            query_id,
                            references[artifact],
                            args.oracle / day["date"],
                            check=check,
                        )
                    )
                    read_source_completion(store, query_id, artifact)
                result["selected_source_complete"] = True
                if args.stage == "two":
                    previous, current = [
                        read_table(session, store, query_id, day["artifact"]["sha256"], "groups")
                        for day in chosen
                    ]
                    differences, result["comparison"] = compare_groups(previous, current)
                    if result["comparison"] != json.loads(
                        (args.oracle / "comparison.json").read_text()
                    ):
                        raise ValueError("DNS descriptive/qualified comparison oracle mismatch")
                    result["observed_differences_only"] = True
                    result["comparison_completion"] = finalize_comparison(
                        session, store, query_id, chosen, args.oracle, differences, check=check
                    )
            from confluent_kafka import Consumer, TopicPartition

            consumer = Consumer(
                {
                    "bootstrap.servers": kafka.bootstrap_servers,
                    "group.id": f"dns-audit-{query_id}",
                    "enable.auto.commit": False,
                }
            )
            try:
                partitions = (
                    consumer.list_topics(result["topic"], timeout=15)
                    .topics[result["topic"]]
                    .partitions
                )
                highs = {
                    str(key): consumer.get_watermark_offsets(
                        TopicPartition(result["topic"], key), timeout=15
                    )[1]
                    for key in partitions
                }
                ends = source_offsets(
                    result["last_progress"]["sources"][0]["endOffset"], result["topic"]
                )
                result["final_kafka_lag"] = sum(highs[key] - ends.get(key, -1) for key in highs)
                if highs != ends:
                    raise ValueError("DNS final broker/checkpoint offsets disagree")
            finally:
                consumer.close()
            check()
            objects = []
            for page in (
                s3_client(config)
                .get_paginator("list_objects_v2")
                .paginate(Bucket=config.bucket, Prefix=config.object_key(""))
            ):
                objects.extend(page.get("Contents", []))
            result.update(
                object_prefix_bytes=sum(row["Size"] for row in objects),
                parquet_bytes=sum(
                    row["Size"] for row in objects if row["Key"].endswith(".parquet")
                ),
                parquet_objects=sum(row["Key"].endswith(".parquet") for row in objects),
                checkpoint_bytes=directory_bytes(args.output / "checkpoint"),
                batches=processor.metrics,
                status="PASS_SELECTED_DNS_STAGE",
            )
    except Exception as error:
        result["failure_category"] = type(error).__name__
        raise
    finally:
        timer.cancel()
        guest.stop.set()
        guest.thread.join(timeout=20)
        result.update(
            elapsed_seconds=monotonic() - began,
            resource_failure=resources.failure,
            guest_failure=guest.failure,
            peak_sampled_tree_rss_bytes=max(
                (row["process_tree_rss_bytes"] for row in resources.samples), default=None
            ),
            minimum_guest_free_disk_bytes=min(
                (row["guest_free_disk_bytes"] for row in guest.samples), default=None
            ),
            minimum_guest_available_memory_bytes=min(
                (row["guest_available_memory_bytes"] for row in guest.samples), default=None
            ),
        )
        (args.output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        session.stop()


if __name__ == "__main__":
    main()
