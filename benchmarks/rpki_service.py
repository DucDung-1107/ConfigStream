"""Opt-in bounded reference experiment using the existing RPKI service path."""

import argparse
import json
import subprocess
import threading
from dataclasses import replace
from pathlib import Path
from time import monotonic
from uuid import uuid4

from workloads.rpki_resources import ResourceSampler, directory_bytes


def guest_resources():
    disk = (
        subprocess.run(
            ["limactl", "shell", "k", "--", "df", "-B1", "/"],
            capture_output=True,
            text=True,
            timeout=15,
            check=True,
        )
        .stdout.splitlines()[-1]
        .split()
    )
    memory = (
        subprocess.run(
            ["limactl", "shell", "k", "--", "free", "-b"],
            capture_output=True,
            text=True,
            timeout=15,
            check=True,
        )
        .stdout.splitlines()[1]
        .split()
    )
    return {"guest_free_disk_bytes": int(disk[3]), "guest_available_memory_bytes": int(memory[-1])}


class GuestGuard:
    def __init__(self, session, output=None):
        self.session = session
        self.samples = []
        self.failure = None
        self.stop = threading.Event()
        self.initial = guest_resources()
        self.output = output
        self.initial_host_bytes = directory_bytes(output) if output is not None else 0
        self.thread = threading.Thread(target=self.sample, daemon=True)

    def sample(self):
        while not self.stop.is_set():
            try:
                value = guest_resources()
                host_growth = (
                    max(0, directory_bytes(self.output) - self.initial_host_bytes)
                    if self.output is not None
                    else 0
                )
                self.samples.append(value)
                if (
                    value["guest_free_disk_bytes"] < 20 * 1024**3
                    or host_growth
                    + max(0, self.initial["guest_free_disk_bytes"] - value["guest_free_disk_bytes"])
                    > 15 * 1024**3
                    or (
                        self.output is not None
                        and directory_bytes(self.output.parent) > 50 * 1024**3
                    )
                    or value["guest_available_memory_bytes"] < 512 * 1024**2
                ):
                    raise RuntimeError("Guest resource guard exceeded")
            except (OSError, ValueError, subprocess.SubprocessError, RuntimeError) as error:
                self.failure = type(error).__name__
                self.session.sparkContext.cancelAllJobs()
                return
            self.stop.wait(10)

    def check(self):
        if self.failure:
            raise RuntimeError("Guest resource sampling/limit failed")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, choices=[1, 2, 14], required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--legacy-prefix", default="")
    args = parser.parse_args()
    if args.output.exists():
        parser.error("A fresh isolated experiment directory is required")
    args.output.mkdir(parents=True)
    from lake.client import ensure_bucket, list_commits
    from lake.config import LakeConfig
    from lake.rpki import MinioStore
    from messaging.config import KafkaConfig
    from messaging.rpki import ensure_topic, publish_source
    from spark.common.config import SparkConfig
    from spark.common.runtime import create_session
    from spark.rpki import committed_rows, read_state, reconstruct
    from spark.rpki_stream import RpkiProcessor, start_query
    from workloads.rpki_main import catalog
    from workloads.rpki_source import disk_guard

    disk_guard(args.output)
    selection = json.loads(Path("workloads/results/dataset-feasibility/rpki.json").read_text())
    oracle = Path(".cache/phase17/oracle")
    summary = json.loads((oracle / "summary.json").read_text())
    references = catalog(selection, summary, args.days)
    identity = str(uuid4())
    topic = f"temporal.rpki.fixture.{identity}"
    lake = replace(LakeConfig.from_env(), prefix=f"phase17-closure/{identity}")
    kafka = KafkaConfig.from_env()
    ensure_topic(kafka, topic=topic)
    ensure_bucket(lake)
    store = MinioStore(lake)
    result = {
        "status": "FAIL",
        "days_requested": args.days,
        "query_id": identity,
        "topic": topic,
        "lake_prefix": lake.prefix,
        "processing_mode": "reuse",
        "max_chunks": 256,
        "per_day_timeout_seconds": 1800,
        "total_budget_seconds": 3600,
        "days": [],
        "boundaries": [],
        "wire_network_bytes": None,
        "mode": "Kafka-Spark-MinIO",
    }
    checkpoint = args.output / "checkpoint"
    checkpoint.mkdir()
    (checkpoint / "source-binding.json").write_text(
        json.dumps(
            {key: value.model_dump(mode="json") for key, value in references.items()},
            sort_keys=True,
        )
    )
    started = monotonic()
    ivy = Path(".cache/spark/ivy").resolve()
    if ivy.exists():
        (args.output / "runtime").mkdir()
        (args.output / "runtime" / "ivy").symlink_to(ivy, target_is_directory=True)
    session = create_session(
        SparkConfig(
            master="local[2]", driver_memory="2g", runtime_dir=args.output / "runtime", lake=lake
        )
    )
    session.conf.set("spark.sql.shuffle.partitions", "8")
    processor = RpkiProcessor(session, store, identity, references, oracle, summary["boundaries"])
    resources = ResourceSampler(args.output, session)
    guest = GuestGuard(session, args.output)
    resources.__enter__()
    guest.thread.start()
    try:

        def check_budget():
            resources.check()
            guest.check()
            if monotonic() - started > 3600:
                raise RuntimeError("Predeclared overall experiment budget exceeded")

        from itertools import islice

        from spark.streaming.sinks.lake import read_committed

        legacy_lake = replace(LakeConfig.from_env(), prefix=args.legacy_prefix)
        legacy = next(
            (
                item
                for item in islice(list_commits(legacy_lake), 20)
                if "processed/snapshots" in item["datasets"]
            ),
            None,
        )
        result["legacy_reader"] = {
            "status": "NOT_RUN",
            "reason": "No existing snapshot manifest in first 20 commits",
        }
        if legacy is not None:
            legacy_rows = read_committed(
                session, legacy_lake, "processed/snapshots", manifests=[legacy]
            ).count()
            if legacy_rows != legacy["counts"]["normalized_snapshot"]:
                raise ValueError("Existing configuration manifest/reader mismatch")
            result["legacy_reader"] = {
                "status": "PASS",
                "query_id": legacy["query_id"],
                "batch_id": legacy["batch_id"],
                "snapshot_rows": legacy_rows,
            }

        ordered = list(references.values())
        for index, reference in enumerate(ordered):
            if monotonic() - started > 3600:
                raise RuntimeError("Predeclared overall experiment budget exceeded")
            resources.check()
            guest.check()
            artifact = next(
                item
                for item in selection["artifacts"]
                if item["sha256"] == reference.manifest.artifacts[0].sha256
            )
            published = publish_source(
                Path(".cache/phase13") / artifact["file"],
                reference,
                args.output / "producer" / f"{reference.observation_id}.json",
                kafka,
                topic=topic,
                resource_check=check_budget,
            )
            tick = monotonic()
            query = start_query(
                session,
                processor,
                kafka.bootstrap_servers,
                checkpoint,
                max_chunks=256,
                topic=topic,
            )
            remaining = max(1, min(1800, int(3600 - (monotonic() - started))))
            if not query.awaitTermination(remaining):
                query.stop()
                raise RuntimeError("Predeclared day drain budget exceeded")
            if query.exception():
                raise RuntimeError("Service query failed")
            progress = [json.loads(item.json) for item in query.recentProgress]
            processing_seconds = monotonic() - tick
            day = str(reference.observation.payload.observation_date)
            completion = store.get(f"state/{reference.manifest.source_scope}/{day}")
            if (
                completion is None
                or completion["source_map_digest"] != summary["days"][index]["source_map_digest"]
            ):
                raise ValueError("Independent completed-day source-map mismatch")
            full = read_state(session, store, reference)
            rebuilding = monotonic()
            rebuilt = reconstruct(session, store, ordered[: index + 1])
            validation = {
                "date": day,
                "observation_id": reference.observation_id,
                "source_rows": completion["source_rows"],
                "members": full.count(),
                "set_digest": completion["digest"],
                "source_map_digest": completion["source_map_digest"],
                "rebuilt_members": rebuilt.count(),
                "reconstruction_seconds": monotonic() - rebuilding,
                "processing_seconds": processing_seconds,
                "publisher": published,
                "progress": progress,
                "status": "PASS",
            }
            result["days"].append(validation)
            if index:
                boundary = store.get(
                    f"boundary/{ordered[index - 1].observation_id}/{reference.observation_id}"
                )
                if boundary is None:
                    raise ValueError("Missing committed boundary")
                result["boundaries"].append(boundary)
            resources.check()
            guest.check()
            (args.output / "progress-summary.json").write_text(json.dumps(result, indent=2) + "\n")
        exact_raw = committed_rows(session, store, identity)
        result["accepted_logical_source_rows"] = exact_raw.count()
        if result["accepted_logical_source_rows"] != sum(
            day["source_rows"] for day in result["days"]
        ):
            raise ValueError("Final source identity accounting mismatch")
        check_budget()
        result["status"] = "PASS"
    except Exception as error:
        result["error_class"] = type(error).__name__
        raise
    finally:
        for query in session.streams.active:
            query.stop()
        guest.stop.set()
        guest.thread.join(timeout=35)
        resources.__exit__()
        result["elapsed_seconds"] = monotonic() - started
        result["resources"] = {
            "host_samples": resources.samples,
            "guest_samples": guest.samples,
            "host_failure": resources.failure,
            "guest_failure": guest.failure,
        }
        result["raw_commits"] = store.list(f"raw/{identity}")
        (args.output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
        session.stop()


if __name__ == "__main__":
    main()
