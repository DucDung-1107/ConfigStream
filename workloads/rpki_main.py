"""Explicit bounded RPKI workflows; no downloads or service startup."""

import argparse
import json
from hashlib import sha256
from pathlib import Path
from time import monotonic
from uuid import uuid4

from workloads.rpki_oracle import run_oracle
from workloads.rpki_reference import day_reference
from workloads.rpki_source import disk_guard, source_chunks


def catalog(selection, summary, days):
    selected = summary["days"][:days]
    selection_id = sha256(
        json.dumps(selection, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    artifacts = {item["sha256"]: item for item in selection["artifacts"]}
    references = [
        day_reference(day, artifacts[day["artifact_sha256"]], selection_id) for day in selected
    ]
    return {reference.observation_id: reference for reference in references}


def prepare(selection, references, cache, destination, limit_rows=None):
    destination.mkdir(parents=True, exist_ok=True)
    source_files = {item["sha256"]: item["file"] for item in selection["artifacts"]}
    serialized, records = 0, 0
    for reference in references.values():
        path = destination / f"{reference.observation_id}.jsonl"
        temporary = path.with_suffix(".tmp")
        accepted = 0
        with temporary.open("w") as target:
            for chunk in source_chunks(
                cache / source_files[reference.manifest.artifacts[0].sha256], reference
            ):
                if limit_rows is not None and accepted + len(chunk.rows) > limit_rows:
                    break
                value = chunk.model_dump_json()
                target.write(value + "\n")
                serialized += len(value.encode())
                records += len(chunk.rows)
                accepted += len(chunk.rows)
                if accepted % 4096 == 0:
                    disk_guard(destination)
        temporary.replace(path)
    return dict(
        serialized_transport_projection_bytes=serialized,
        source_rows=records,
        actual_kafka_bytes=None,
        partial=limit_rows is not None,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["oracle", "local", "publish", "stream", "reconstruct"])
    parser.add_argument(
        "--selection", type=Path, default=Path("workloads/results/dataset-feasibility/rpki.json")
    )
    parser.add_argument("--cache", type=Path, default=Path(".cache/phase13"))
    parser.add_argument("--output", type=Path, default=Path(".cache/phase17"))
    parser.add_argument("--days", type=int, choices=range(1, 15), default=1)
    parser.add_argument("--limit-rows", type=int)
    parser.add_argument("--minio", action="store_true")
    parser.add_argument("--query-id", default="00000000-0000-4000-8000-000000000017")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    disk_guard(args.output)
    if args.limit_rows is not None and (
        args.command != "local" or not 256 <= args.limit_rows <= 10000
    ):
        parser.error("Partial inputs are local pilot only, between 256 and 10000 source rows")
    selection = json.loads(args.selection.read_text())
    oracle_dir = args.output / "oracle"
    if args.command == "oracle":
        run_oracle(selection, args.cache, oracle_dir)
        return
    summary = json.loads((oracle_dir / "summary.json").read_text())
    references = catalog(selection, summary, args.days)
    if args.command == "publish":
        from messaging.config import KafkaConfig
        from messaging.rpki import ensure_topic, publish_source

        config = KafkaConfig.from_env()
        ensure_topic(config)
        result = []
        for reference in references.values():
            artifact = next(
                item
                for item in selection["artifacts"]
                if item["sha256"] == reference.manifest.artifacts[0].sha256
            )
            result.append(
                publish_source(
                    args.cache / artifact["file"],
                    reference,
                    args.output / "producer" / f"{reference.observation_id}.json",
                    config,
                )
            )
        print(json.dumps(result))
        return
    from lake.rpki import LocalStore, MinioStore
    from spark.common.config import SparkConfig
    from spark.common.runtime import create_session
    from spark.rpki import read_state, reconstruct
    from spark.rpki_stream import RpkiProcessor, start_query

    if args.command == "stream" or args.minio:
        from lake.config import LakeConfig

        config = SparkConfig(
            master="local[2]",
            driver_memory="2g",
            runtime_dir=args.output / "spark",
            lake=LakeConfig.from_env(),
        )
        store = MinioStore(config.lake)
    else:
        config = SparkConfig(
            master="local[2]", driver_memory="2g", runtime_dir=args.output / "spark"
        )
        store = LocalStore(args.output / "local-lake")
    started = monotonic()
    session = create_session(config, kafka=args.command == "stream")
    session.conf.set("spark.sql.shuffle.partitions", "8")
    session.conf.set("spark.sql.streaming.numRecentProgressUpdates", "512")
    from workloads.rpki_resources import ResourceSampler

    resources = ResourceSampler(args.output, session)
    resources.__enter__()
    try:
        processor = RpkiProcessor(
            session, store, args.query_id, references, oracle_dir, summary["boundaries"]
        )
        if args.command == "stream":
            checkpoint = args.output / "rpki-checkpoints-v1" / args.query_id
            checkpoint.mkdir(parents=True, exist_ok=True)
            binding = json.dumps(
                {key: value.model_dump(mode="json") for key, value in references.items()},
                sort_keys=True,
            )
            pinned = checkpoint / "source-binding.json"
            if pinned.exists() and pinned.read_text() != binding:
                raise ValueError("RPKI checkpoint selection changed; use a new query/checkpoint")
            if not pinned.exists():
                pinned.write_text(binding)
            query = start_query(session, processor, config.bootstrap_servers, checkpoint)
            query.awaitTermination(1800)
            if query.isActive:
                query.stop()
                raise RuntimeError("Bounded RPKI query exceeded 1800 seconds")
            if query.exception():
                raise RuntimeError("RPKI query failed")
            (args.output / "stream-progress.json").write_text(
                json.dumps([json.loads(item.json) for item in query.recentProgress], indent=2)
            )
        elif args.command == "local":
            prep_started = monotonic()
            input_path = args.output / "input" / str(uuid4())
            measurements = prepare(selection, references, args.cache, input_path, args.limit_rows)
            measurements["source_prepare_seconds"] = monotonic() - prep_started
            processing_started = monotonic()
            from pyspark.sql import functions as functions

            chunks = (
                session.read.text(str(input_path))
                .withColumn("observation", functions.get_json_object("value", "$.observation_id"))
                .withColumn(
                    "group",
                    functions.floor(
                        (functions.get_json_object("value", "$.first_row").cast("long") - 1) / 65536
                    ),
                )
            )
            groups = chunks.select("observation", "group").distinct().limit(513).collect()
            if len(groups) > 512:
                raise ValueError("Local source batch inventory budget exceeded")
            batch_costs = []
            for batch_id, group in enumerate(
                sorted(groups, key=lambda row: (row.observation, row.group))
            ):
                subset = chunks.filter(
                    functions.col("observation").eqNullSafe(group.observation)
                    & functions.col("group").eqNullSafe(group.group)
                ).select("value")
                tick = monotonic()
                processor(subset, batch_id)
                resources.check()
                batch_costs.append(
                    dict(
                        batch_id=batch_id, source_chunks=subset.count(), seconds=monotonic() - tick
                    )
                )
            resources.check()
            measurements["local_batch_costs"] = batch_costs
            measurements["spark_local_processing_seconds"] = monotonic() - processing_started
            measurements["elapsed_seconds"] = monotonic() - started
            measurements["service_integration"] = "NOT_RUN"
            measurements["complete_days"] = len(store.list("state"))
            (args.output / "local-measurements.json").write_text(
                json.dumps(measurements, indent=2) + "\n"
            )
        if args.command == "reconstruct" or args.limit_rows is None:
            validation = []
            ordered = list(references.values())
            for index, reference in enumerate(ordered):
                full = read_state(session, store, reference)
                rebuilt = reconstruct(session, store, ordered[: index + 1])
                validation.append(
                    dict(
                        date=str(reference.observation.payload.observation_date),
                        full_members=full.count(),
                        reconstructed_members=rebuilt.count(),
                        digest=reference.observation.payload.set_digest,
                    )
                )
            (args.output / "local-reconstruction.json").write_text(
                json.dumps(validation, indent=2) + "\n"
            )
    finally:
        resources.__exit__()
        (args.output / "resource-samples.json").write_text(
            json.dumps(
                {
                    "samples": resources.samples,
                    "failure": resources.failure,
                    "rss_scope": "sampled process-tree sum; shared pages may be counted twice",
                },
                indent=2,
            )
        )
        session.stop()


if __name__ == "__main__":
    main()
