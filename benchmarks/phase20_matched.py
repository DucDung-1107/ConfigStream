"""Opt-in matched normalized-projection pilot, not a production ingestion benchmark."""

import argparse
import json
import os
import signal
import sqlite3
import subprocess
import time
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from threading import RLock
from time import monotonic
from uuid import uuid4

from benchmarks.evaluation import randomized_order
from benchmarks.phase20 import guards
from benchmarks.rpki_service import guest_resources
from workloads.rpki_resources import directory_bytes

FORMAT = "phase20-normalized-core-1"
CAP = 7200
GROWTH = 3 * 1024**3
SOURCES = {
    "rpki": (".cache/phase17/oracle/members/2026-09-01.parquet", "membership_id", None),
    "ris": (
        ".cache/phase18-closure/oracle-hour-v2/updates.20260901.0000.gz/events.parquet",
        "logical_record_id",
        None,
    ),
    "dns": (".cache/phase19/oracle/2026-09-01/raw.parquet", "logical_record_id", 4096),
}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def normalized_offsets(value, topic):
    parsed = json.loads(value) if isinstance(value, str) else value
    if not isinstance(parsed, dict) or set(parsed) != {topic}:
        raise ValueError("Normalized Kafka source topic binding mismatch")
    offsets = parsed[topic]
    if (
        not isinstance(offsets, dict)
        or set(offsets) != {"0", "1"}
        or any(type(offset) is not int or offset < 0 for offset in offsets.values())
    ):
        raise ValueError("Normalized Kafka source offset mismatch")
    return offsets


def digest_rows(rows):
    digest = sha256(b"phase20-core-v1\n")
    for identity, payload in rows:
        for value in (identity, payload):
            encoded = value.encode()
            digest.update(len(encoded).to_bytes(4, "big"))
            digest.update(encoded)
    return digest.hexdigest()


class ExactRows:
    def __init__(self, oracle, seen):
        self.lock = RLock()
        self.expected = sqlite3.connect(f"file:{oracle}?mode=ro", uri=True, check_same_thread=False)
        self.observed = sqlite3.connect(seen, check_same_thread=False)
        self.observed.execute("PRAGMA cache_size=-16384")
        self.observed.execute("CREATE TABLE seen(identity TEXT PRIMARY KEY) WITHOUT ROWID")
        self.physical = 0

    def accept(self, identity, payload):
        with self.lock:
            self._accept(identity, payload)

    def _accept(self, identity, payload):
        expected = self.expected.execute(
            "SELECT payload FROM expected WHERE identity=?", (identity,)
        ).fetchone()
        if expected is None or expected[0] != payload:
            raise ValueError("Unexpected identity or conflicting complete normalized projection")
        self.observed.execute("INSERT OR IGNORE INTO seen VALUES (?)", (identity,))
        self.physical += 1

    def complete(self):
        with self.lock:
            return self._complete()

    def _complete(self):
        self.observed.commit()
        observed = self.observed.execute("SELECT identity FROM seen ORDER BY identity")
        expected = self.expected.execute("SELECT identity,payload FROM expected ORDER BY identity")

        def matched():
            for identity, payload in expected:
                actual = observed.fetchone()
                if actual != (identity,):
                    raise ValueError("Missing accepted normalized identity")
                yield identity, payload
            if observed.fetchone() is not None:
                raise ValueError("Unexpected accepted normalized identity")

        digest = digest_rows(matched())
        count = self.observed.execute("SELECT count(*) FROM seen").fetchone()[0]
        return dict(
            logical_rows=count,
            physical_rows=self.physical,
            reconciled_retries=self.physical - count,
            projection_sha256=digest,
            exact_membership_and_full_projection="PASS",
        )

    def close(self):
        with self.lock:
            self.expected.close()
            self.observed.close()


def prepare(workload, root, check):
    import pyarrow as arrow
    import pyarrow.parquet as parquet

    source, identity_column, limit = SOURCES[workload]
    target = root / workload
    target.mkdir()
    database = sqlite3.connect(target / "expected.sqlite")
    database.execute("PRAGMA cache_size=-16384")
    database.execute("CREATE TABLE expected(identity TEXT PRIMARY KEY,payload TEXT) WITHOUT ROWID")
    schema = arrow.schema([("logical_id", arrow.string()), ("row_json", arrow.string())])
    began = monotonic()
    count = 0
    with parquet.ParquetWriter(target / "input.parquet", schema, compression="snappy") as writer:
        for batch in parquet.ParquetFile(source).iter_batches(batch_size=8192):
            check()
            if limit is not None:
                batch = batch.slice(0, min(batch.num_rows, limit - count))
            rows = [(row[identity_column], canonical(row)) for row in batch.to_pylist()]
            database.executemany("INSERT INTO expected VALUES (?,?)", rows)
            writer.write_table(
                arrow.Table.from_arrays(
                    [arrow.array([row[0] for row in rows]), arrow.array([row[1] for row in rows])],
                    schema=schema,
                ),
                row_group_size=65536,
            )
            count += len(rows)
            if limit is not None and count == limit:
                break
    database.commit()
    digest = digest_rows(
        database.execute("SELECT identity,payload FROM expected ORDER BY identity")
    )
    database.close()
    result = dict(
        workload=workload,
        rows=count,
        source_normalized_path=source,
        source_normalized_sha256=sha256(Path(source).read_bytes()).hexdigest(),
        normalized_wrapper_sha256=sha256((target / "input.parquet").read_bytes()).hexdigest(),
        projection_sha256=digest,
        preparation_seconds=monotonic() - began,
        source_parser="EXCLUDED_ALL_MODES; accepted independent normalized projection",
        partial_selected_object=limit is not None,
    )
    save(target / "binding.json", result)
    return result


def object_inventory(client, lake, suffix):
    return [
        item
        for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=lake.bucket, Prefix=lake.object_key(suffix)
        )
        for item in page.get("Contents", [])
    ]


def validate_files(client, lake, entries, verifier, check):
    from urllib.parse import urlsplit

    import pyarrow.fs as filesystem
    import pyarrow.parquet as parquet

    endpoint = urlsplit(lake.endpoint)
    remote = filesystem.S3FileSystem(
        access_key=lake.access_key,
        secret_key=lake.secret_key,
        endpoint_override=endpoint.netloc,
        scheme=endpoint.scheme,
        region=lake.region,
    )
    validated = []
    for entry in entries:
        check()
        digest = sha256()
        size = 0
        with client.get_object(Bucket=lake.bucket, Key=entry["Key"])["Body"] as body:
            while block := body.read(1024 * 1024):
                check()
                digest.update(block)
                size += len(block)
        if size != entry["Size"]:
            raise ValueError("Committed physical file size mismatch")
        count = 0
        with remote.open_input_file(f"{lake.bucket}/{entry['Key']}") as handle:
            physical = parquet.ParquetFile(handle)
            if physical.schema_arrow.names != ["logical_id", "row_json"]:
                raise ValueError("Unexpected normalized physical projection schema")
            for batch in physical.iter_batches(batch_size=4096):
                check()
                for row in batch.to_pylist():
                    verifier.accept(row["logical_id"], row["row_json"])
                    count += 1
        validated.append(dict(key=entry["Key"], bytes=size, sha256=digest.hexdigest(), rows=count))
    if not validated:
        raise ValueError("Missing normalized Parquet output")
    return validated


def publish(kafka, topic, input_path, identity, check):
    import pyarrow.parquet as parquet
    from confluent_kafka import Producer

    producer = Producer(
        kafka.producer_options() | {"compression.type": "zstd", "queue.buffering.max.kbytes": 32768}
    )
    result = dict(acknowledged_chunks=0, acknowledged_rows=0, serialized_value_bytes=0)
    failures = []
    ordinal = 0

    def delivered(error, message, count):
        if error:
            failures.append(str(error.code()))
        else:
            result["acknowledged_chunks"] += 1
            result["acknowledged_rows"] += count

    began = monotonic()
    for batch in parquet.ParquetFile(input_path).iter_batches(batch_size=128):
        check()
        rows = batch.to_pylist()
        value = canonical(dict(format=FORMAT, run_id=identity, rows=rows)).encode()
        row_count = len(rows)
        if len(value) > 1024 * 1024:
            raise ValueError("Normalized transport chunk exceeds declared bound")
        result["serialized_value_bytes"] += len(value)
        while True:
            check()
            if failures:
                raise RuntimeError("Kafka delivery failed")
            try:
                producer.produce(
                    topic,
                    key=rows[0]["logical_id"].encode(),
                    value=value,
                    partition=ordinal % 2,
                    on_delivery=lambda error, message, count=row_count: delivered(
                        error, message, count
                    ),
                )
                break
            except BufferError:
                producer.poll(0.1)
        producer.poll(0)
        ordinal += 1
    if producer.flush(15) or failures:
        raise RuntimeError("Unacknowledged normalized Kafka transport")
    result["producer_seconds"] = monotonic() - began
    return result


def worker(args):
    import pyarrow as arrow
    import pyarrow.parquet as parquet

    from lake.client import list_commits, s3_client, save_commit
    from lake.config import LakeConfig
    from messaging.config import KafkaConfig

    arrow.set_cpu_count(2)
    arrow.set_io_thread_count(2)
    output = args.output
    output.mkdir()
    identity = str(uuid4())
    lake = replace(LakeConfig.from_env(), prefix=f"phase20-matched/{identity}")
    client = s3_client(lake)
    client.head_bucket(Bucket=lake.bucket)
    workload, mode = args.worker.split("-")
    selection = args.inputs / workload
    binding = json.loads((selection / "binding.json").read_text())
    result = dict(
        status="FAIL",
        evidence="MEASURED_SERVICE_NORMALIZED_CORE",
        workload=workload,
        mode=mode,
        repetition=args.repetition,
        query_id=identity,
        prefix=lake.prefix,
        code_commit=subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        input_binding=binding,
        format=FORMAT,
        cache="fresh process; filesystem cache uncontrolled",
        driver_heap="2g",
        master="local[2]",
        publication_seconds=0.0,
        validation_seconds=0.0,
        transformation_seconds=0.0,
        topic=None,
        final_lag="N/A",
        parser_seconds="EXCLUDED_ALL_MODES",
        checkpoint=str(output / "checkpoint"),
    )
    save(output / "isolation.json", result)
    verifier = ExactRows(selection / "expected.sqlite", output / "seen.sqlite")
    session = None
    began = monotonic()
    try:
        with guards(output, args.remaining) as check:

            def commit(batch_id):
                validation_began = monotonic()
                entries = [
                    item
                    for item in object_inventory(client, lake, f"batches/{batch_id}/")
                    if item["Key"].endswith(".parquet")
                ]
                files = validate_files(client, lake, entries, verifier, check)
                result["validation_seconds"] += monotonic() - validation_began
                publication_began = monotonic()
                save_commit(
                    lake,
                    dict(
                        query_id=identity,
                        batch_id=batch_id,
                        schema=FORMAT,
                        input_projection_sha256=binding["projection_sha256"],
                        files=files,
                    ),
                )
                result["publication_seconds"] += monotonic() - publication_began

            if mode == "B0":
                transform_began = monotonic()
                original = parquet.ParquetFile(selection / "input.parquet")
                writers = [
                    parquet.ParquetWriter(
                        output / f"part-{index}.parquet",
                        original.schema_arrow,
                        compression="snappy",
                    )
                    for index in range(2)
                ]
                try:
                    for batch in original.iter_batches(batch_size=8192):
                        check()
                        middle = batch.num_rows // 2
                        for writer, piece in zip(
                            writers, (batch.slice(0, middle), batch.slice(middle)), strict=True
                        ):
                            writer.write_table(
                                arrow.Table.from_batches([piece]), row_group_size=65536
                            )
                finally:
                    for writer in writers:
                        writer.close()
                result["transformation_seconds"] = monotonic() - transform_began
                publication_began = monotonic()
                for index in range(2):
                    client.upload_file(
                        str(output / f"part-{index}.parquet"),
                        lake.bucket,
                        lake.object_key(f"batches/0/part-{index}.parquet"),
                    )
                result["publication_seconds"] += monotonic() - publication_began
                commit(0)
            else:
                from spark.common.config import SparkConfig
                from spark.common.runtime import create_session

                kafka = KafkaConfig.from_env()
                if mode == "B3":
                    from confluent_kafka.admin import AdminClient, NewTopic

                    topic = f"evaluation.phase20.{identity}"
                    result["topic"] = topic
                    save(output / "isolation.json", result)
                    admin = AdminClient(kafka.client_options())
                    for future in admin.create_topics([NewTopic(topic, 2, 1)]).values():
                        future.result(timeout=20)
                    result["producer"] = publish(
                        kafka, topic, selection / "input.parquet", identity, check
                    )
                runtime = output / "runtime"
                runtime.mkdir()
                (runtime / "ivy").symlink_to(
                    Path(".cache/spark/ivy").absolute(), target_is_directory=True
                )
                startup = monotonic()
                session = create_session(
                    SparkConfig(
                        master="local[2]",
                        driver_memory="2g",
                        lake=lake,
                        runtime_dir=runtime,
                        bootstrap_servers=kafka.bootstrap_servers,
                    ),
                    kafka=mode == "B3",
                )
                result["spark_startup_seconds"] = monotonic() - startup
                input_frame = session.read.parquet(str((selection / "input.parquet").absolute()))
                if mode == "B2":
                    transform_began = monotonic()
                    input_frame.repartition(2).write.mode("errorifexists").option(
                        "compression", "snappy"
                    ).option("maxRecordsPerFile", 262144).parquet(lake.uri("batches/0"))
                    result["transformation_seconds"] = monotonic() - transform_began
                    commit(0)
                else:
                    from pyspark.sql import functions as functions
                    from pyspark.sql.types import ArrayType, StringType, StructField, StructType

                    schema = StructType(
                        [
                            StructField("format", StringType()),
                            StructField("run_id", StringType()),
                            StructField("rows", ArrayType(input_frame.schema)),
                        ]
                    )
                    incoming = (
                        session.readStream.format("kafka")
                        .option("kafka.bootstrap.servers", kafka.bootstrap_servers)
                        .option("subscribe", result["topic"])
                        .option("startingOffsets", "earliest")
                        .option("failOnDataLoss", "true")
                        .load()
                    )
                    decoded = incoming.select(
                        functions.from_json(
                            functions.col("value").cast("string"), schema, {"mode": "FAILFAST"}
                        ).alias("envelope")
                    )

                    def callback(frame, batch_id):
                        check()
                        if (
                            frame.where(
                                (functions.col("envelope.format") != FORMAT)
                                | functions.col("envelope.format").isNull()
                                | (functions.col("envelope.run_id") != identity)
                                | functions.col("envelope.run_id").isNull()
                                | functions.col("envelope.rows").isNull()
                            )
                            .limit(1)
                            .count()
                        ):
                            raise ValueError("Invalid normalized benchmark envelope")
                        rows = frame.select(functions.explode("envelope.rows").alias("row")).select(
                            "row.*"
                        )
                        transform_began = monotonic()
                        rows.repartition(2).write.mode("errorifexists").option(
                            "compression", "snappy"
                        ).option("maxRecordsPerFile", 262144).parquet(
                            lake.uri(f"batches/{batch_id}")
                        )
                        result["transformation_seconds"] += monotonic() - transform_began
                        commit(batch_id)

                    stream_began = monotonic()
                    query = (
                        decoded.writeStream.foreachBatch(callback)
                        .option("checkpointLocation", str((output / "checkpoint").absolute()))
                        .trigger(availableNow=True)
                        .start()
                    )
                    while query.isActive:
                        check()
                        query.awaitTermination(1)
                    if query.exception() is not None:
                        raise RuntimeError("Normalized streaming query failed")
                    result["stream_processing_seconds"] = monotonic() - stream_began
                    progress = [
                        json.loads(item.json) if hasattr(item, "json") else dict(item)
                        for item in query.recentProgress
                    ]
                    save(output / "progress.json", progress)
                    from confluent_kafka import Consumer, TopicPartition

                    consumer = Consumer(kafka.client_options() | {"group.id": identity})
                    try:
                        high = {
                            str(partition): consumer.get_watermark_offsets(
                                TopicPartition(result["topic"], partition), timeout=10
                            )[1]
                            for partition in range(2)
                        }
                    finally:
                        consumer.close()
                    ended = normalized_offsets(
                        progress[-1]["sources"][0]["endOffset"], result["topic"]
                    )
                    if ended != high:
                        raise ValueError("Selected normalized Kafka offsets not completely drained")
                    result["final_lag"] = 0
                    result["selected_end_offsets"] = ended
                    broker = subprocess.run(
                        [
                            "docker",
                            "exec",
                            "configstream-kafka-1",
                            "sh",
                            "-c",
                            f"du -sb /var/lib/kafka/data/{result['topic']}-*; "
                            f"du -sB1 /var/lib/kafka/data/{result['topic']}-*",
                        ],
                        capture_output=True,
                        text=True,
                        check=True,
                        timeout=15,
                    ).stdout.splitlines()
                    if len(broker) != 4:
                        raise ValueError("Selected Kafka broker storage inventory unavailable")
                    result["broker_apparent_bytes"] = sum(
                        int(line.split()[0]) for line in broker[:2]
                    )
                    result["broker_allocated_bytes"] = sum(
                        int(line.split()[0]) for line in broker[2:]
                    )
            validation_began = monotonic()
            result["exact"] = verifier.complete()
            if (result["exact"]["logical_rows"], result["exact"]["projection_sha256"]) != (
                binding["rows"],
                binding["projection_sha256"],
            ):
                raise ValueError("Normalized complete logical projection mismatch")
            result["validation_seconds"] += monotonic() - validation_began
            commits = list(list_commits(lake, identity))
            if (
                sum(item["rows"] for manifest in commits for item in manifest["files"])
                != result["exact"]["physical_rows"]
            ):
                raise ValueError("Conditional commit inventory mismatch")
            completion = canonical(
                dict(format=FORMAT, query_id=identity, projection=result["exact"], commits=commits)
            ).encode()
            client.put_object(
                Bucket=lake.bucket,
                Key=lake.object_key("completion.json"),
                Body=completion,
                IfNoneMatch="*",
            )
            with client.get_object(Bucket=lake.bucket, Key=lake.object_key("completion.json"))[
                "Body"
            ] as body:
                if body.read() != completion:
                    raise ValueError("Completion metadata readback mismatch")
            inventory = object_inventory(client, lake, "")
            result["minio_bytes"] = sum(item["Size"] for item in inventory)
            result["parquet_bytes"] = sum(
                item["Size"] for item in inventory if item["Key"].endswith(".parquet")
            )
            result["minio_objects"] = len(inventory)
            result["parquet_files"] = sum(item["Key"].endswith(".parquet") for item in inventory)
            result["committed_batches"] = len(commits)
            result["status"] = "PASS"
    except Exception as error:
        result["failure_category"] = type(error).__name__
        raise
    finally:
        result["to_validated_commit_seconds"] = monotonic() - began
        if session is not None:
            session.stop()
        verifier.close()
        result["checkpoint_content_bytes"] = directory_bytes(output / "checkpoint")
        save(output / "result.json", result)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--worker")
    parser.add_argument("--inputs", type=Path)
    parser.add_argument("--repetition", type=int)
    parser.add_argument("--remaining", type=float, default=CAP)
    parser.add_argument("--retry", action="store_true")
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Fresh private output required; no implicit retry")
    if args.worker:
        worker(args)
        return
    policy_path = Path(
        "workloads/results/phase20/matched-pilot-retry-registration.json"
        if args.retry
        else "workloads/results/phase20/matched-pilot-registration.json"
    )
    policy = json.loads(policy_path.read_text())
    growth_cap = 6 * 1024**3 if args.retry else GROWTH
    expected_approval = (
        "OWNER_APPROVED_ONE_BOUNDED_RETRY" if args.retry else "OWNER_APPROVED_BOUNDED_MATCHED_PILOT"
    )
    if (
        policy["approval"] != expected_approval
        or policy["cap_seconds"] != CAP
        or policy["growth_bytes"] != growth_cap
    ):
        parser.error("Specific bounded matched pilot approval missing")
    import pyarrow as arrow

    arrow.set_cpu_count(2)
    arrow.set_io_thread_count(2)
    args.output.mkdir(parents=True)
    retained = Path(policy["retained_failed_directory"]) if args.retry else None
    previous = json.loads((retained / "campaign.json").read_text()) if retained else None
    if previous is not None and previous["status"] != "FAIL":
        parser.error("Approved retry must retain the failed original campaign")
    if retained:
        usage = Path(".cache/phase20/authorizations")
        usage.mkdir(exist_ok=True)
        with (usage / "P20-MATCHED-RETRY-1.json").open("x") as receipt:
            receipt.write(
                canonical(
                    dict(
                        output=str(args.output),
                        policy_sha256=sha256(policy_path.read_bytes()).hexdigest(),
                    )
                )
            )
    inputs = retained / "inputs" if retained else args.output / "inputs"
    if not retained:
        inputs.mkdir()
    initial_guest = previous["initial_guest"] if previous else guest_resources()
    prior_seconds = previous["elapsed_seconds"] if previous else 0
    effective_cap = CAP - prior_seconds
    began = monotonic()
    order = randomized_order(
        [f"{workload}-{mode}" for workload in SOURCES for mode in ("B0", "B2", "B3")]
    )
    save(args.output / "order.json", order)
    campaign = dict(
        status="FAIL",
        cap_seconds=CAP,
        growth_bytes=growth_cap,
        initial_guest=initial_guest,
        prior_elapsed_seconds=prior_seconds,
        retained_failed_directory=str(retained) if retained else None,
        inputs_directory=str(inputs),
        trials=[],
    )
    try:
        with guards(args.output, effective_cap) as resource_check:

            def check():
                resource_check()
                guest = guest_resources()
                retained_bytes = directory_bytes(retained) if retained else 0
                growth = (
                    retained_bytes
                    + directory_bytes(args.output)
                    + max(
                        0, initial_guest["guest_free_disk_bytes"] - guest["guest_free_disk_bytes"]
                    )
                )
                if growth > growth_cap or monotonic() - began >= effective_cap:
                    raise RuntimeError("Approved aggregate matched pilot budget exceeded")

            if retained:
                bindings = []
                for workload in SOURCES:
                    check()
                    selected = inputs / workload
                    binding = json.loads((selected / "binding.json").read_text())
                    source = Path(binding["source_normalized_path"])
                    if (
                        sha256(source.read_bytes()).hexdigest()
                        != binding["source_normalized_sha256"]
                        or sha256((selected / "input.parquet").read_bytes()).hexdigest()
                        != binding["normalized_wrapper_sha256"]
                    ):
                        raise ValueError("Retained matched normalized input checksum mismatch")
                    expected = sqlite3.connect(
                        f"file:{selected / 'expected.sqlite'}?mode=ro", uri=True
                    )
                    try:
                        if (
                            expected.execute("SELECT count(*) FROM expected").fetchone()[0]
                            != binding["rows"]
                            or digest_rows(
                                expected.execute(
                                    "SELECT identity,payload FROM expected ORDER BY identity"
                                )
                            )
                            != binding["projection_sha256"]
                        ):
                            raise ValueError("Retained exact oracle projection mismatch")
                    finally:
                        expected.close()
                    bindings.append(binding)
                campaign["inputs"] = bindings
                campaign["input_preparation"] = "Revalidated immutable retained input; no rewriting"
            else:
                campaign["inputs"] = [prepare(workload, inputs, check) for workload in SOURCES]
            for configuration, repetition in order:
                check()
                name = f"{configuration}-{repetition}"
                command = [
                    str(Path(".venv/bin/python").absolute()),
                    "-m",
                    "benchmarks.phase20_matched",
                    "--output",
                    str(args.output / name),
                    "--inputs",
                    str(inputs),
                    "--worker",
                    configuration,
                    "--repetition",
                    str(repetition),
                    "--remaining",
                    str(effective_cap - (monotonic() - began)),
                ]
                started = monotonic()
                with (args.output / f"{name}.log").open("w") as log:
                    process = subprocess.Popen(
                        command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True
                    )
                    try:
                        while process.poll() is None:
                            check()
                            time.sleep(2)
                        if process.returncode:
                            raise RuntimeError("Matched pilot trial failed; no automatic retry")
                    finally:
                        if process.poll() is None:
                            os.killpg(process.pid, signal.SIGINT)
                            try:
                                process.wait(timeout=20)
                            except subprocess.TimeoutExpired:
                                os.killpg(process.pid, signal.SIGTERM)
                                process.wait(timeout=20)
                trial = dict(
                    configuration=configuration,
                    repetition=repetition,
                    command_start_to_exit_seconds=monotonic() - started,
                )
                campaign["trials"].append(trial)
                save(args.output / "campaign.json", campaign)
                print(canonical(trial), flush=True)
            campaign["status"] = "PASS"
    finally:
        campaign["elapsed_seconds"] = monotonic() - began
        campaign["final_guest"] = guest_resources()
        campaign["host_content_bytes"] = directory_bytes(args.output)
        campaign["retained_failed_content_bytes"] = directory_bytes(retained) if retained else 0
        campaign["combined_elapsed_seconds"] = prior_seconds + campaign["elapsed_seconds"]
        save(args.output / "campaign.json", campaign)


if __name__ == "__main__":
    main()
