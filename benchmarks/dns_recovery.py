"""Bounded identical real-source replay after completed DNS acceptance, no reingestion."""

import argparse
import json
import subprocess
from dataclasses import replace
from pathlib import Path
from time import monotonic


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--accepted", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Fresh recovery evidence directory required")
    accepted = json.loads((args.accepted / "result.json").read_text())
    if accepted["status"] != "PASS_SELECTED_DNS_STAGE" or accepted["stage"] != "two":
        parser.error("Only the accepted two-object DNS experiment is eligible")
    query_id = accepted["query_id"]
    if accepted["lake_prefix"] != f"phase19/{query_id}":
        parser.error("Unapproved recovery prefix")
    from confluent_kafka import Consumer, Producer, TopicPartition

    from benchmarks.rpki_service import GuestGuard
    from lake.config import LakeConfig
    from lake.dns import DnsStore, read_source_completion
    from lake.ris import metadata_digest
    from messaging.config import KafkaConfig
    from messaging.dns import source_offsets
    from processing.dns import read_source
    from schemas.dns import DnsChunk, DnsReference
    from spark.common.config import SparkConfig
    from spark.common.runtime import create_session
    from spark.dns import DnsProcessor, start_query
    from workloads.rpki_resources import ResourceSampler, directory_bytes

    args.output.mkdir(parents=True)
    config = replace(LakeConfig.from_env(), prefix=accepted["lake_prefix"])
    kafka = KafkaConfig.from_env()
    store = DnsStore(config=config)
    inventory = metadata_digest(sorted(store.inventory(query_id), key=lambda row: row["batch_id"]))
    sources = {}
    paths = []
    selected = json.loads(Path("workloads/results/dataset-feasibility/dns.json").read_text())[
        "days"
    ]
    for day in selected:
        artifact = day["artifact"]["sha256"]
        descriptor = read_source_completion(store, query_id, artifact)
        reference = DnsReference.model_validate(
            store.get(f"sources/{descriptor['provenance_manifest_id']}")
        )
        sources[artifact] = reference
        paths.append(str((Path(".cache/phase19/oracle") / day["date"] / "raw.parquet").resolve()))
    runtime = args.output / "runtime"
    runtime.mkdir()
    (runtime / "ivy").symlink_to(Path(".cache/spark/ivy").resolve(), target_is_directory=True)
    result = dict(
        status="FAIL",
        evidence="MEASURED_SERVICE",
        mode="256-row identical sealed-source replay",
        code_commit=subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        query_id=query_id,
        runtime_cap_seconds=600,
        checkpoint_reused=True,
        before_inventory_sha256=inventory,
    )
    began = monotonic()
    session = create_session(
        SparkConfig(runtime_dir=runtime, lake=config, master="local[2]", driver_memory="2g")
    )
    resources = ResourceSampler(args.output, session)
    guest = GuestGuard(session, args.output)
    guest.thread.start()

    def check():
        resources.check()
        guest.check()
        if monotonic() - began >= 600 or directory_bytes(args.output) > 15 * 1024**3:
            session.sparkContext.cancelAllJobs()
            raise RuntimeError("DNS replay resource/runtime guard")

    try:
        with resources:
            processor = DnsProcessor(session, store, query_id, sources, paths)
            producer = Producer(
                {
                    "bootstrap.servers": kafka.bootstrap_servers,
                    "acks": "all",
                    "queue.buffering.max.kbytes": 1024,
                }
            )
            failures = []

            def acknowledge(error, message):
                if error:
                    failures.append(type(error).__name__)

            for day in selected:
                check()
                artifact = day["artifact"]["sha256"]
                rows = tuple(
                    read_source(
                        Path(".cache/phase13") / day["artifact"]["file"],
                        sources[artifact],
                        limit=128,
                    )
                )
                payload = DnsChunk(artifact_sha256=artifact, rows=rows).model_dump_json().encode()
                producer.produce(
                    accepted["topic"], key=artifact, value=payload, on_delivery=acknowledge
                )
                if producer.flush(30) or failures:
                    raise RuntimeError("DNS replay broker acknowledgment failed")
            query = start_query(
                session,
                processor,
                kafka.bootstrap_servers,
                args.accepted / "checkpoint",
                accepted["topic"],
            )
            try:
                while query.isActive:
                    check()
                    query.awaitTermination(2)
                if query.exception():
                    raise RuntimeError("DNS replay query failed")
                ends = source_offsets(
                    json.loads(query.lastProgress.json)["sources"][0]["endOffset"],
                    accepted["topic"],
                )
            finally:
                query.stop()
            after = metadata_digest(
                sorted(store.inventory(query_id), key=lambda row: row["batch_id"])
            )
            if after != inventory:
                raise ValueError("DNS replay changed accepted physical history")
            for artifact in sources:
                read_source_completion(store, query_id, artifact)
            consumer = Consumer(
                {
                    "bootstrap.servers": kafka.bootstrap_servers,
                    "group.id": f"dns-replay-audit-{query_id}",
                }
            )
            try:
                highs = {
                    key: consumer.get_watermark_offsets(
                        TopicPartition(accepted["topic"], int(key)), timeout=15
                    )[1]
                    for key in ends
                }
            finally:
                consumer.close()
            if highs != ends:
                raise ValueError("DNS replay broker/checkpoint lag")
            check()
            result.update(
                status="PASS_REAL_SOURCE_REPLAY",
                acknowledged_rows=256,
                additional_logical_rows=0,
                after_inventory_sha256=after,
                final_kafka_lag=0,
                callbacks=processor.metrics,
            )
    finally:
        guest.stop.set()
        guest.thread.join(timeout=20)
        result.update(
            elapsed_seconds=monotonic() - began,
            resource_failure=resources.failure,
            guest_failure=guest.failure,
            peak_sampled_tree_rss_bytes=max(
                (row["process_tree_rss_bytes"] for row in resources.samples), default=None
            ),
        )
        (args.output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        session.stop()


if __name__ == "__main__":
    main()
