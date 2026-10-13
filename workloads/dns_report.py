"""Public aggregate evidence export; never export external DNS names or normalized rows."""

import argparse
import json
import sqlite3
import subprocess
from pathlib import Path


def main():
    import pyarrow.parquet as parquet

    parser = argparse.ArgumentParser()
    parser.add_argument("--service-storage", action="store_true")
    args = parser.parse_args()

    root = Path("workloads/results/phase19")
    private = Path(".cache/phase19")
    selection = json.loads(Path("workloads/results/dataset-feasibility/dns.json").read_text())[
        "days"
    ]
    code = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()

    def save(name, data):
        (root / f"{name}.json").write_text(
            json.dumps(dict(evidence_code_commit=code, **data), indent=2) + "\n"
        )

    sources = []
    for day in selection:
        artifact = day["artifact"]
        footer = parquet.ParquetFile(Path(".cache/phase13") / artifact["file"])
        summary = json.loads((private / "oracle" / day["date"] / "summary.json").read_text())
        database = sqlite3.connect(private / "oracle" / day["date"] / "oracle.sqlite")
        contexts = database.execute(
            "SELECT MIN(json_extract(data,'$.timestamp_ms')),"
            "MAX(json_extract(data,'$.timestamp_ms')),"
            "COUNT(DISTINCT json_extract(data,'$.worker_id')),"
            "SUM(json_extract(data,'$.timestamp_ms') IS NULL),"
            "SUM(json_extract(data,'$.worker_id') IS NULL) FROM raw"
        ).fetchone()
        database.close()
        sources.append(
            dict(
                date=day["date"],
                artifact=artifact,
                evidence="MEASURED_REAL_SOURCE",
                fields=[
                    dict(name=field.name, type=str(field.type), nullable=field.nullable)
                    for field in footer.schema_arrow
                ],
                uncompressed_column_bytes=sum(
                    footer.metadata.row_group(index).total_byte_size
                    for index in range(footer.num_row_groups)
                ),
                minimum_timestamp_ms=contexts[0],
                maximum_timestamp_ms=contexts[1],
                distinct_recorded_workers=contexts[2],
                unknown_source_timestamp_rows=contexts[3],
                unknown_worker_rows=contexts[4],
                timestamp_encoding="epoch_millisecond",
                source_precision="second",
                summary=summary,
            )
        )
    database = sqlite3.connect(":memory:")
    for alias, day in zip(["previous", "current"], selection, strict=True):
        database.execute(
            f"ATTACH DATABASE ? AS {alias}",
            (str(private / "oracle" / day["date"] / "oracle.sqlite"),),
        )
    shared_raw_ids = database.execute(
        "SELECT COUNT(*) FROM previous.raw p JOIN current.raw c USING(identity)"
    ).fetchone()[0]
    shared_group_ids = database.execute(
        "SELECT COUNT(*) FROM previous.groups p JOIN current.groups c USING(identity)"
    ).fetchone()[0]
    database.close()
    if shared_raw_ids or shared_group_ids:
        raise ValueError("DNS cross-artifact logical identities overlap")
    save(
        "independent-oracle",
        dict(
            evidence="MEASURED_REAL_SOURCE",
            sources=sources,
            comparison=json.loads((private / "oracle/comparison.json").read_text()),
            independent_normalization=True,
            cross_artifact_raw_identity_intersection=shared_raw_ids,
            cross_artifact_group_identity_intersection=shared_group_ids,
            exact_private_ledgers=True,
            raw_source_rows=sum(source["summary"]["source_rows"] for source in sources),
            raw_redistribution=False,
            original_source_retention_required_for_nonprojected_fields=True,
            matched_performance_baseline=False,
            oracle_wall_seconds=302.21,
            oracle_peak_process_rss_bytes=1048477696,
        ),
    )
    runs = []
    for directory in [
        "service-sample",
        "service-sample-v2",
        "service-one",
        "service-two",
        "real-replay",
    ]:
        path = private / directory / "result.json"
        if not path.exists():
            runs.append(dict(experiment=directory, evidence="NOT_RUN"))
            continue
        result = json.loads(path.read_text())
        retained = {key: value for key, value in result.items() if key != "last_progress"}
        retained.update(experiment=directory, evidence="MEASURED_SERVICE")
        runs.append(retained)
    save(
        "validation",
        dict(
            evidence="MEASURED_SERVICE",
            runs=runs,
            rights="Private noncommercial research under verified publisher CC BY-NC-SA 4.0 terms",
            instructor_approval="NOT_VERIFIED",
            live_source_downloads=False,
            phase20_started=False,
        ),
    )
    if args.service_storage:
        from dataclasses import replace

        from lake.client import s3_client
        from lake.config import LakeConfig
        from lake.dns import TABLES
        from messaging.dns import validate_topic

        run = json.loads((private / "service-two/result.json").read_text())
        if run["status"] != "PASS_SELECTED_DNS_STAGE":
            raise ValueError("DNS storage report requires accepted service run")
        config = replace(LakeConfig.from_env(), prefix=run["lake_prefix"])
        objects = []
        for page in (
            s3_client(config)
            .get_paginator("list_objects_v2")
            .paginate(Bucket=config.bucket, Prefix=config.object_key(""))
        ):
            objects.extend(page.get("Contents", []))
        tables = {
            table: dict(
                parquet_bytes=sum(
                    row["Size"]
                    for row in objects
                    if row["Key"].startswith(config.object_key(path + "/"))
                    and row["Key"].endswith(".parquet")
                ),
                parquet_files=sum(
                    row["Key"].startswith(config.object_key(path + "/"))
                    and row["Key"].endswith(".parquet")
                    for row in objects
                ),
            )
            for table, path in TABLES.items()
        }
        shared = tables["raw"]["parquet_bytes"] + tables["contributors"]["parquet_bytes"]
        topic = run["topic"]
        validate_topic(topic)
        directories = [f"/var/lib/kafka/data/{topic}-{partition}" for partition in range(2)]
        apparent = subprocess.run(
            ["docker", "exec", "configstream-kafka-1", "du", "-sb", *directories],
            capture_output=True,
            text=True,
            check=True,
        )
        allocated = subprocess.run(
            ["docker", "exec", "configstream-kafka-1", "du", "-sB1", *directories],
            capture_output=True,
            text=True,
            check=True,
        )
        save(
            "representation-costs",
            dict(
                evidence="MEASURED_SERVICE",
                query_id=run["query_id"],
                after_bounded_real_source_replay=True,
                tables=tables,
                source_parquet_bytes=sum(day["artifact"]["artifact_bytes"] for day in selection),
                object_content_bytes=sum(row["Size"] for row in objects),
                parquet_bytes=sum(item["parquet_bytes"] for item in tables.values()),
                full_component_bytes=shared + tables["groups"]["parquet_bytes"],
                reused_component_bytes=shared
                + tables["reuse"]["parquet_bytes"]
                + tables["values"]["parquet_bytes"],
                representation_evaluation=(
                    "Components only; prototype jointly retains full and reused groups for "
                    "validation. Not a standalone reduced-retention deployment."
                ),
                selected_broker_apparent_bytes=sum(
                    int(line.split()[0]) for line in apparent.stdout.splitlines()
                ),
                selected_broker_allocated_bytes=sum(
                    int(line.split()[0]) for line in allocated.stdout.splitlines()
                ),
                checkpoint_bytes=sum(
                    path.stat().st_size
                    for path in (private / "service-two/checkpoint").rglob("*")
                    if path.is_file()
                ),
                private_oracle_bytes=sum(
                    path.stat().st_size
                    for path in (private / "oracle").rglob("*")
                    if path.is_file()
                ),
                network_wire_bytes="NOT_MEASURED",
                minio_backend_filesystem_overhead="NOT_MEASURED",
                matched_batch_stream_speed_comparison="NOT_RUN; Phase20 not started",
            ),
        )
        from confluent_kafka import Consumer, TopicPartition

        from messaging.config import KafkaConfig

        consumer = Consumer(
            {
                "bootstrap.servers": KafkaConfig.from_env().bootstrap_servers,
                "group.id": "dns-timestamp-audit-" + run["query_id"],
                "enable.auto.commit": False,
                "queued.max.messages.kbytes": 1024,
            }
        )
        timeline = []
        previous = {"0": 0, "1": 0}
        try:
            for path in sorted(
                (private / "service-two/checkpoint/offsets").glob("*"),
                key=lambda item: int(item.name) if item.name.isdecimal() else -1,
            ):
                if not path.name.isdecimal():
                    continue
                lines = path.read_text().splitlines()
                metadata = json.loads(lines[1])
                offsets = json.loads(lines[2])[topic]
                samples = []
                for partition, end in offsets.items():
                    start = previous.get(partition, 0)
                    if end <= start:
                        continue
                    for offset in sorted({start, end - 1}):
                        consumer.assign([TopicPartition(topic, int(partition), offset)])
                        message = consumer.poll(10)
                        if message is None or message.error() or message.offset() != offset:
                            raise RuntimeError("DNS timestamp endpoint unavailable")
                        kind, timestamp = message.timestamp()
                        samples.append(
                            dict(
                                partition=int(partition),
                                offset=offset,
                                kafka_timestamp_type=kind,
                                kafka_recorded_timestamp_ms=timestamp,
                            )
                        )
                timeline.append(
                    dict(
                        batch_id=int(path.name),
                        spark_batch_timestamp_ms=metadata["batchTimestampMs"],
                        kafka_start_offsets=previous,
                        kafka_end_offsets=offsets,
                        endpoint_timestamp_samples=samples,
                    )
                )
                previous = offsets
        finally:
            consumer.close()
        save(
            "delivery-time-qualification",
            dict(
                evidence="MEASURED_SERVICE",
                query_id=run["query_id"],
                timeline=timeline,
                kafka_timestamp_type_1="producer CreateTime, not proven broker arrival time",
                spark_timestamp="checkpoint batch initiation clock, not source observation time",
                source_precision="second",
                source_encoding="epoch_millisecond",
                timestamp_sampling_scope="Offset endpoints, not all individual delivery receipts",
                true_network_arrival_time="NOT_MEASURED",
                clock_accuracy="NOT_ESTABLISHED",
            ),
        )


if __name__ == "__main__":
    main()
