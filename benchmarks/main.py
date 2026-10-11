import argparse
import fcntl
import json
import logging
import re
import threading
from collections import Counter
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic, sleep
from urllib.parse import urlsplit

from benchmarks.metrics import (
    command,
    consumed_offsets,
    lag,
    logical_config_bytes,
    machine_metadata,
    summarize_progress,
)
from lake.client import ensure_bucket, list_commits, s3_client
from lake.config import LakeConfig
from messaging.config import KafkaConfig
from messaging.kafka_producer import KafkaProducer
from messaging.serialization import serialize_record
from processing.rules import load_rules
from spark.common.config import SparkConfig
from workloads.history import PROFILES, History, WorkloadConfig


@dataclass(frozen=True)
class BenchmarkConfig:
    benchmark_id: str
    assets: int = 100
    versions_per_asset: int = 100
    rate: float = 500
    profile: str = "stress-replay"
    seed: int = 42
    master: str = "local[2]"
    corpus: Path = Path(".cache/datasets/batfish")
    max_seconds: int = 1800
    max_offsets: int = 10000
    duplicate_ratio: float = 0
    out_of_order_ratio: float = 0
    watermark: str = "48 hours"
    driver_memory: str = "1g"
    max_reordering_distance: int = 16
    start_time: datetime = datetime(2026, 1, 1, tzinfo=UTC)
    state_timeout: str = "87600 hours"
    max_versions_per_asset: int = 32

    def __post_init__(self):
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,100}", self.benchmark_id):
            raise ValueError("Use a bounded lowercase benchmark ID")
        if self.assets < 1 or self.versions_per_asset < 1 or not 0 < self.rate <= 100000:
            raise ValueError("Positive counts and finite bounded replay rate required")
        if self.max_seconds < 1 or self.max_offsets < 1:
            raise ValueError("Positive timeout and offset batch cap required")
        WorkloadConfig(
            assets=self.assets,
            versions_per_asset=self.versions_per_asset,
            profile=self.profile,
            duplicate_ratio=self.duplicate_ratio,
            out_of_order_ratio=self.out_of_order_ratio,
            max_reordering_distance=self.max_reordering_distance,
            start_time=self.start_time,
        )
        SparkConfig(
            master=self.master,
            driver_memory=self.driver_memory,
            watermark_delay=self.watermark,
            max_offsets_per_trigger=self.max_offsets,
            state_timeout=self.state_timeout,
            max_versions_per_asset=self.max_versions_per_asset,
        )


def latest_offsets(config: KafkaConfig) -> dict[str, int]:
    from confluent_kafka import TopicPartition
    from confluent_kafka.admin import AdminClient, OffsetSpec

    admin = AdminClient(config.client_options())
    partitions = admin.list_topics(timeout=10).topics["config.snapshots"].partitions
    requests = {
        TopicPartition("config.snapshots", partition): OffsetSpec.latest()
        for partition in partitions
    }
    return {
        str(partition.partition): future.result(timeout=10).offset
        for partition, future in admin.list_offsets(requests, request_timeout=10).items()
    }


def committed_metrics(lake, query_id):
    manifests = list(list_commits(lake, query_id))
    counts = Counter()
    prefixes = set()
    for manifest in manifests:
        counts.update(manifest["counts"])
        prefixes.update(
            urlsplit(path).path.lstrip("/") + "/" for path in manifest["datasets"].values()
        )
    physical = 0
    for page in (
        s3_client(lake)
        .get_paginator("list_objects_v2")
        .paginate(Bucket=lake.bucket, Prefix=lake.object_key(""))
    ):
        for entry in page.get("Contents", []):
            if entry["Key"].endswith(".parquet") and any(
                entry["Key"].startswith(prefix) for prefix in prefixes
            ):
                physical += entry["Size"]
    return {"counts": dict(counts), "parquet_bytes": physical, "commits": len(manifests)}


def execute(config: BenchmarkConfig, *, analytics: bool = True) -> dict:
    directory = Path(".cache/benchmarks")
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / ".publisher.lock").open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError(
                "Another benchmark is running; shared Kafka input must be isolated"
            ) from error
        return _execute(config, analytics=analytics)


def _execute(config: BenchmarkConfig, *, analytics: bool = True) -> dict:
    directory = Path(".cache/benchmarks") / config.benchmark_id
    if directory.exists():
        raise ValueError("Benchmark ID already exists; choose a new ID (old evidence is preserved)")
    directory.mkdir(parents=True)
    result = {
        "schema_version": "1.0",
        "benchmark_id": config.benchmark_id,
        "config": {
            key: str(value) if isinstance(value, (Path, datetime)) else value
            for key, value in asdict(config).items()
        },
        "machine": machine_metadata(),
        "started_at": datetime.now(UTC).isoformat(),
        "status": "incomplete",
        "progress": [],
        "resource_samples": [],
        "lag_samples": [],
        "progress_capture": "recent-progress-history-v1",
    }
    stop = threading.Event()
    session = query = publisher = None
    publisher_errors = []
    prepared = monotonic()
    runtime = None
    measured_start = None

    def save():
        (directory / "result.json").write_text(json.dumps(result, indent=2, default=str) + "\n")

    save()
    try:
        from spark.common.runtime import create_session
        from spark.streaming.kafka_source import read_snapshots
        from spark.streaming.main import start_stateful
        from spark.streaming.parser import parse_records
        from spark.streaming.validation import classify_records

        lake = replace(LakeConfig.from_env(), prefix=f"benchmarks/{config.benchmark_id}")
        kafka = KafkaConfig.from_env()
        start_offsets = latest_offsets(kafka)
        checkpoint = directory / "checkpoint"
        runtime = SparkConfig(
            master=config.master,
            driver_memory=config.driver_memory,
            app_name=f"Benchmark-{config.benchmark_id}",
            bootstrap_servers=kafka.bootstrap_servers,
            checkpoint_dir=checkpoint,
            starting_offsets=json.dumps({"config.snapshots": start_offsets}),
            lake=lake,
            max_offsets_per_trigger=config.max_offsets,
            state_timeout=config.state_timeout,
            max_versions_per_asset=config.max_versions_per_asset,
            watermark_delay=config.watermark,
            console_rows=1,
            rules_file=Path("workloads/rules.yaml"),
        )
        ensure_bucket(lake)
        history_config = WorkloadConfig(
            assets=config.assets,
            versions_per_asset=config.versions_per_asset,
            seed=config.seed,
            corpus=config.corpus,
            profile=config.profile,
            duplicate_ratio=config.duplicate_ratio,
            out_of_order_ratio=config.out_of_order_ratio,
            max_reordering_distance=config.max_reordering_distance,
            start_time=config.start_time,
        )
        history = History(history_config)
        result["corpus"] = {
            key: value
            for key, value in json.loads((config.corpus / "manifest.json").read_text()).items()
            if key != "files"
        }
        session = create_session(runtime)
        raw = read_snapshots(session, runtime)
        query = start_stateful(
            classify_records(parse_records(raw)),
            runtime,
            load_rules(runtime.rules_file),
            raw_records=raw,
        )
        result["startup_seconds"] = monotonic() - prepared
        result["runtime"] = {
            "checkpoint": str(checkpoint),
            "lake_prefix": lake.prefix,
            "starting_offsets": start_offsets,
            "partitions": len(start_offsets),
            "watermark": runtime.watermark_delay,
            "state_timeout": runtime.state_timeout,
            "max_versions_per_asset": runtime.max_versions_per_asset,
            "max_offsets_per_trigger": runtime.max_offsets_per_trigger,
            "search_enabled": False,
            "trigger_seconds": runtime.trigger_seconds,
            "logical_history_days": config.versions_per_asset,
            "starting_event_time": history_config.start_time.isoformat(),
            "driver_memory": runtime.driver_memory,
            "driver_max_heap_bytes": session._jvm.java.lang.Runtime.getRuntime().maxMemory(),
            "workload_policy": history.identity.split(":")[0],
        }
        measured_start = monotonic()

        def publish():
            snapshots = logical_bytes = serialized_bytes = 0
            started = monotonic()
            try:
                with (
                    KafkaProducer(kafka) as producer,
                    (directory / "truth.jsonl").open("w") as truth,
                ):
                    for asset in history.assets:
                        producer.publish(asset)
                    for _, snapshot, ground_truth in history.records():
                        if stop.is_set():
                            raise InterruptedError("Benchmark publishing interrupted")
                        remaining = started + snapshots / config.rate - monotonic()
                        if remaining > 0:
                            stop.wait(remaining)
                        producer.publish(snapshot)
                        truth.write(json.dumps(ground_truth, sort_keys=True) + "\n")
                        snapshots += 1
                        logical_bytes += logical_config_bytes(snapshot.content)
                        serialized_bytes += len(serialize_record(snapshot).encode())
                result["publisher"] = {
                    "snapshots": snapshots,
                    "logical_raw_config_bytes": logical_bytes,
                    "kafka_value_bytes": serialized_bytes,
                    "seconds": monotonic() - started,
                    "achieved_snapshots_per_second": snapshots / (monotonic() - started),
                    "end_offsets": latest_offsets(kafka),
                }
            except BaseException as error:
                result["publisher"] = {
                    "snapshots": snapshots,
                    "logical_raw_config_bytes": logical_bytes,
                    "kafka_value_bytes": serialized_bytes,
                    "seconds": monotonic() - started,
                }
                publisher_errors.append(type(error).__name__)

        publisher = threading.Thread(target=publish, name="benchmark-publisher", daemon=True)
        publisher.start()
        previous_batch = None
        next_sample = 0
        while True:
            if not query.isActive:
                raise RuntimeError("Streaming query terminated before benchmark completion")
            if publisher_errors:
                raise RuntimeError("Benchmark publisher failed")
            elapsed = monotonic() - measured_start
            if elapsed > config.max_seconds:
                raise TimeoutError("Benchmark exceeded its drain deadline")
            native_progress = query.lastProgress
            progress = json.loads(native_progress.json) if native_progress else None
            for recent in query.recentProgress:
                if previous_batch is None or recent["batchId"] > previous_batch:
                    result["progress"].append(json.loads(recent.json))
                    previous_batch = recent["batchId"]
            save()
            if elapsed >= next_sample:
                consumed = consumed_offsets(progress or {}) or start_offsets
                latest = latest_offsets(kafka)
                result["lag_samples"].append(
                    {
                        "seconds": elapsed,
                        "latest": latest,
                        "consumed": consumed,
                        "lag": lag(latest, consumed),
                    }
                )
                resources = {
                    "seconds": elapsed,
                    "containers": command(
                        [
                            "docker",
                            "stats",
                            "--no-stream",
                            "--format",
                            "{{json .}}",
                            "configstream-kafka-1",
                            "configstream-minio-1",
                        ]
                    ),
                }
                process = session.sparkContext._gateway.proc
                if process is not None:
                    resources["spark_process"] = command(
                        ["ps", "-p", str(process.pid), "-o", "rss=,%cpu="]
                    )
                result["resource_samples"].append(resources)
                disk = command(["docker", "exec", "configstream-kafka-1", "df", "-Pk", "/tmp"])
                if disk and int(disk.splitlines()[-1].split()[3]) < 2_000_000:
                    raise RuntimeError("VM free disk below the 2 GB safety reserve")
                next_sample = elapsed + 5
                save()
            if (
                not publisher.is_alive()
                and "end_offsets" in result.get("publisher", {})
                and progress
            ):
                if lag(result["publisher"]["end_offsets"], consumed_offsets(progress)) == 0:
                    break
            sleep(0.5)
        result["streaming_seconds"] = monotonic() - measured_start
        result["final_lag"] = lag(result["publisher"]["end_offsets"], consumed_offsets(progress))
        result["drain_seconds"] = result["streaming_seconds"] - result["publisher"]["seconds"]
        query.stop()
        query_id = json.loads((checkpoint / "metadata").read_text())["id"]
        result["query_id"] = query_id
        result["lake"] = committed_metrics(lake, query_id)
        expected = config.assets * config.versions_per_asset
        if result["lake"]["counts"].get("normalized_snapshot", 0) != expected:
            raise RuntimeError("Accepted snapshot count differs from canonical ground truth")
        if result["lake"]["counts"].get("config_diff", 0) != config.assets * (
            config.versions_per_asset - 1
        ):
            raise RuntimeError("Adjacent diff count differs from canonical ground truth")
        if analytics:
            from analytics.config import AnalyticsConfig
            from analytics.main import run_analytics

            result["analytics"] = run_analytics(session, lake, AnalyticsConfig(), query_id)
        result["status"] = "success"
    except KeyboardInterrupt:
        result["status"], result["failure"] = "interrupted", "KeyboardInterrupt"
    except Exception as error:
        result["status"], result["failure"] = "failed", type(error).__name__
        logging.getLogger(__name__).error(
            "Benchmark failed (%s); evidence retained", type(error).__name__
        )
    finally:
        if measured_start is not None:
            result.setdefault("streaming_seconds", monotonic() - measured_start)
        stop.set()
        if publisher:
            publisher.join(timeout=30)
        try:
            if query and query.isActive:
                query.stop()
        except Exception as error:
            result["status"] = "failed"
            result["cleanup_failure"] = type(error).__name__
        try:
            if runtime is not None and (runtime.checkpoint_dir / "metadata").exists():
                result["query_id"] = json.loads((runtime.checkpoint_dir / "metadata").read_text())[
                    "id"
                ]
                result["lake"] = committed_metrics(runtime.lake, result["query_id"])
        except Exception as error:
            result["status"] = "failed"
            result["cleanup_failure"] = type(error).__name__
        finally:
            try:
                if session:
                    session.stop()
            except Exception as error:
                result["status"] = "failed"
                result["cleanup_failure"] = type(error).__name__
        result["metrics"] = summarize_progress(result["progress"])
        result["finished_at"] = datetime.now(UTC).isoformat()
        result["total_seconds"] = monotonic() - prepared
        save()
    return result


def main():
    parser = argparse.ArgumentParser(description="Isolated local accelerated historical replay")
    parser.add_argument("--benchmark-id", required=True)
    parser.add_argument("--assets", type=int, default=100)
    parser.add_argument("--versions-per-asset", type=int, default=100)
    parser.add_argument("--rate", type=float, default=500)
    parser.add_argument("--profile", choices=PROFILES, default="stress-replay")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--corpus", type=Path, default=Path(".cache/datasets/batfish"))
    parser.add_argument("--max-seconds", type=int, default=1800)
    parser.add_argument("--max-offsets", type=int, default=10000)
    parser.add_argument("--duplicate-ratio", type=float, default=0)
    parser.add_argument("--out-of-order-ratio", type=float, default=0)
    parser.add_argument("--watermark", default="48 hours")
    parser.add_argument("--driver-memory", default="1g")
    parser.add_argument("--max-reordering-distance", type=int, default=16)
    parser.add_argument(
        "--start-time", type=datetime.fromisoformat, default=datetime(2026, 1, 1, tzinfo=UTC)
    )
    parser.add_argument("--state-timeout", default="87600 hours")
    parser.add_argument("--max-versions-per-asset", type=int, default=32)
    parser.add_argument("--no-analytics", action="store_true")
    args = vars(parser.parse_args())
    analytics = not args.pop("no_analytics")
    result = execute(BenchmarkConfig(**args), analytics=analytics)
    print(
        json.dumps(
            {
                "benchmark_id": result["benchmark_id"],
                "status": result["status"],
                "publisher": result.get("publisher"),
                "lake": result.get("lake"),
                "metrics": result.get("metrics"),
                "failure": result.get("failure"),
            }
        )
    )
    return 0 if result["status"] == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
