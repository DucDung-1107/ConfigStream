"""Owned Kubernetes demonstration adapter; semantic processing remains in ConfigStream."""

import argparse
import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

from spark.common.config import SparkConfig
from spark.common.runtime import create_session


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
        Path("/evidence/producer.json").write_text(
            json.dumps({"acknowledged": producer.delivered, "distinct_versions": 2})
        )
        return
    session = create_session(config, from_submit=True, kafka=mode == "stream")
    try:
        if mode == "stream":
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
            query = start_stateful(
                classified,
                config,
                load_rules(config.rules_file),
                available_now=True,
                raw_records=raw,
            )
            monitor_query(query, available_now=True)
            Path("/evidence/query-id").write_text(str(query.id))
        else:
            from spark.streaming.sinks.lake import read_committed

            query_id = Path("/evidence/query-id").read_text().strip()
            frame = read_committed(session, config.lake, "processed/snapshots", query_id=query_id)
            count = frame.count()
            if count != 2:
                raise RuntimeError("Owned fixture must reconstruct exactly two version identities")
            actual = {
                row.snapshot_id: (row.version, row.normalized_content, row.normalized_hash)
                for row in frame.select(
                    "snapshot_id", "version", "normalized_content", "normalized_hash"
                ).collect()
            }
            expected = {
                f"k8s-owned-v{version}": (version, content, sha256(content.encode()).hexdigest())
                for version, content in ((1, "timeout=10\n"), (2, "timeout=20\n"))
            }
            if actual != expected:
                raise RuntimeError("Independent owned fixture projection disagrees")
            result = {
                "mode": "native-kubernetes-batch",
                "master": session.sparkContext.master,
                "snapshots": count,
                "query_id": query_id,
            }
            Path("/evidence/batch-result.json").write_text(json.dumps(result))
            if config.search is not None:
                from search.reindex import rebuild

                rebuild(session, config.lake, config.search, query_id=query_id)
    finally:
        session.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("seed", "stream", "batch"))
    run(parser.parse_args().mode)
