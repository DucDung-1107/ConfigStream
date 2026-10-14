"""Owned Kubernetes demonstration adapter; semantic processing remains in ConfigStream."""

import argparse
import json
import os
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

from spark.common.config import SparkConfig
from spark.common.runtime import create_session


def evidence_dir():
    directory = Path(os.environ.get("CONFIGSTREAM_DEMO_EVIDENCE_DIR", "/evidence"))
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def run(mode):
    from lake.client import ensure_bucket

    config = SparkConfig.from_env()
    if config.lake is None:
        raise ValueError("Kubernetes demonstration requires the committed lake")
    ensure_bucket(config.lake)
    if mode == "seed":
        from messaging.config import KafkaConfig
        from messaging.kafka_producer import KafkaProducer
        from schemas import ConfigSnapshot

        timestamp = datetime(2026, 1, 1, tzinfo=UTC)
        snapshots = [
            ConfigSnapshot(
                snapshot_id=f"k8s-owned-v{version}",
                asset_id="k8s-owned-router",
                version=version,
                event_time=timestamp,
                ingest_time=timestamp,
                hash=sha256(content.encode()).hexdigest(),
                content=content,
            )
            for version, content in ((1, "timeout=10\n"), (2, "timeout=20\n"))
        ]
        with KafkaProducer(KafkaConfig.from_env()) as producer:
            for snapshot in (*snapshots, snapshots[0]):
                producer.publish(snapshot)
        (evidence_dir() / "producer.json").write_text(
            json.dumps({"acknowledged": producer.delivered, "distinct_versions": 2})
        )
        return
    session = create_session(config, from_submit=True, kafka=mode in {"stream", "fail-stream"})
    try:
        if mode in {"stream", "fail-stream"}:
            from processing.rules import load_rules
            from spark.streaming.kafka_source import read_snapshots
            from spark.streaming.main import monitor_query, start_stateful
            from spark.streaming.parser import parse_records
            from spark.streaming.validation import classify_records

            if config.search is not None:
                from search.client import initialize_indices

                initialize_indices(config.search)
            raw = read_snapshots(session, config)
            classified = classify_records(parse_records(raw))
            if mode == "fail-stream":
                from spark.streaming.sinks import lake as sink

                original = sink.write_lake_batch

                def interrupted(batch, batch_id, configuration, counts):
                    original(batch, batch_id, configuration, counts)
                    marker = evidence_dir() / "commit-before-ack.json"
                    if not marker.exists():
                        marker.write_text(
                            json.dumps({"batch_id": batch_id, "fault": "after_commit"})
                        )
                        raise RuntimeError("Owned commit-before-checkpoint interruption")

                sink.write_lake_batch = interrupted
            query = start_stateful(
                classified,
                config,
                load_rules(config.rules_file),
                available_now=True,
                raw_records=raw,
            )
            monitor_query(query, available_now=True)
            (evidence_dir() / "query-id").write_text(str(query.id))
            (evidence_dir() / "stream-progress.json").write_text(
                json.dumps([json.loads(progress.json) for progress in query.recentProgress])
            )
        elif mode == "roles":
            from roles import run_roles

            run_roles(session, config, evidence_dir())
        else:
            from spark.streaming.sinks.lake import read_committed

            query_id = (evidence_dir() / "query-id").read_text().strip()
            frame = read_committed(session, config.lake, "processed/snapshots", query_id=query_id)
            count = frame.count()
            if count != 2:
                raise RuntimeError("Owned fixture must reconstruct exactly two version identities")
            actual = {
                row.snapshot_id: (
                    row.asset_id,
                    row.version,
                    row.event_time,
                    row.ingest_time,
                    row.raw_hash,
                    row.normalized_hash,
                    row.normalization_version,
                    row.normalized_content,
                )
                for row in frame.select(
                    "snapshot_id",
                    "asset_id",
                    "version",
                    "event_time",
                    "ingest_time",
                    "raw_hash",
                    "normalized_hash",
                    "normalization_version",
                    "normalized_content",
                ).collect()
            }
            timestamp = datetime(2026, 1, 1)
            expected = {
                f"k8s-owned-v{version}": (
                    "k8s-owned-router",
                    version,
                    timestamp,
                    timestamp,
                    sha256(content.encode()).hexdigest(),
                    sha256(content.encode()).hexdigest(),
                    "text-v1",
                    content,
                )
                for version, content in ((1, "timeout=10\n"), (2, "timeout=20\n"))
            }
            if actual != expected:
                raise RuntimeError("Independent owned fixture projection disagrees")
            diffs = read_committed(session, config.lake, "processed/diffs", query_id=query_id)
            if diffs.count() != 1:
                raise RuntimeError("Owned adjacent-version analytical query disagrees")
            pair = diffs.select("old_snapshot_id", "new_snapshot_id", "asset_id").first()
            if tuple(pair) != ("k8s-owned-v1", "k8s-owned-v2", "k8s-owned-router"):
                raise RuntimeError("Owned version pairing is not adjacent source identity")
            result = {
                "mode": "native-kubernetes-batch",
                "master": session.sparkContext.master,
                "snapshots": count,
                "exact_projection": "PASS",
                "adjacent_diffs": 1,
                "query_id": query_id,
            }
            (evidence_dir() / "batch-result.json").write_text(json.dumps(result))
            if mode == "integrity":
                from integrity import run as check_integrity

                report = check_integrity(session, config.lake, query_id)
                (evidence_dir() / "integrity-result.json").write_text(json.dumps(report))
            if config.search is not None:
                from search.reindex import rebuild

                rebuild(session, config.lake, config.search, query_id=query_id)
    finally:
        session.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "mode", choices=("seed", "stream", "fail-stream", "batch", "roles", "integrity")
    )
    run(parser.parse_args().mode)
