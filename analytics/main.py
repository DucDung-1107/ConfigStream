import argparse
import json
from datetime import datetime
from time import perf_counter

from analytics.config import JOB_VERSION, AnalyticsConfig, run_identity
from lake.client import list_commits, s3_client
from spark.common.config import SparkConfig


def run_analytics(session, lake, config, query_id=None, job="all"):
    from analytics.jobs import all_jobs
    from spark.streaming.sinks.lake import read_committed

    manifests = list(list_commits(lake, query_id))
    if not manifests:
        raise ValueError("No committed input history")
    identity = run_identity(manifests, config)
    client = s3_client(lake)
    marker = lake.object_key(f"_analytics_commits/{identity}/{job}.json")
    from botocore.exceptions import ClientError

    try:
        response = client.get_object(Bucket=lake.bucket, Key=marker)
        with response["Body"] as body:
            return json.loads(body.read())
    except ClientError as error:
        if error.response["Error"]["Code"] != "NoSuchKey":
            raise
    inputs = {}
    datasets = {
        "snapshot": ("snapshots", "snapshot_id"),
        "diff": ("diffs", "diff_id"),
        "alert": ("alerts", "alert_id"),
        "diagnostic": ("diagnostics", None),
    }
    for name, (dataset, identifier) in datasets.items():
        frame = read_committed(session, lake, f"processed/{dataset}", query_id, manifests=manifests)
        if identifier:
            frame = frame.dropDuplicates([identifier])
        inputs[name] = frame.drop("normalized_content", "details")
    outputs = all_jobs(inputs, config)
    if job != "all":
        if job not in outputs:
            raise ValueError("Unknown analytics job")
        outputs = {job: outputs[job]}
    results = {}
    for name, frame in outputs.items():
        started = perf_counter()
        frame = frame.persist()
        try:
            count = frame.count()
            path = lake.uri(f"analytics/{name}/run_id={identity}")
            frame.write.mode("overwrite").option("compression", "snappy").parquet(path)
            results[name] = {
                "path": path,
                "rows": count,
                "seconds": perf_counter() - started,
                "schema": frame.schema.jsonValue(),
            }
        finally:
            frame.unpersist()
    manifest = {
        "schema_version": "1.0",
        "run_id": identity,
        "job_version": JOB_VERSION,
        "input_commits": [[entry["query_id"], entry["batch_id"]] for entry in manifests],
        "parameters": config.parameters(),
        "datasets": results,
    }
    client.put_object(
        Bucket=lake.bucket,
        Key=marker,
        Body=json.dumps(manifest, sort_keys=True).encode(),
        ContentType="application/json",
        IfNoneMatch="*",
    )
    return manifest


def main():
    parser = argparse.ArgumentParser(
        description="Historical Spark analytics over committed Parquet"
    )
    parser.add_argument("--job", default="all")
    parser.add_argument("--query-id")
    parser.add_argument("--start", type=datetime.fromisoformat)
    parser.add_argument("--end", type=datetime.fromisoformat)
    parser.add_argument("--window", choices=["1 hour", "1 day"], default="1 day")
    parser.add_argument("--inspect", action="store_true")
    parser.add_argument("--baseline-windows", type=int, default=7)
    parser.add_argument("--minimum-prior-windows", type=int, default=3)
    parser.add_argument("--minimum-count", type=int, default=5)
    parser.add_argument("--z-threshold", type=float, default=3)
    args = parser.parse_args()
    runtime = SparkConfig.from_env()
    if runtime.lake is None:
        parser.error("Set CONFIGSTREAM_LAKE_ENABLED=true and an explicit lake prefix")
    if args.inspect:
        client = s3_client(runtime.lake)
        for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=runtime.lake.bucket, Prefix=runtime.lake.object_key("_analytics_commits/")
        ):
            for entry in page.get("Contents", []):
                response = client.get_object(Bucket=runtime.lake.bucket, Key=entry["Key"])
                with response["Body"] as body:
                    manifest = json.loads(body.read())
                print(
                    json.dumps(
                        {
                            "run_id": manifest["run_id"],
                            "datasets": {
                                name: value["rows"] for name, value in manifest["datasets"].items()
                            },
                        }
                    )
                )
        return
    config = AnalyticsConfig(
        window=args.window,
        start=args.start,
        end=args.end,
        baseline_windows=args.baseline_windows,
        minimum_prior_windows=args.minimum_prior_windows,
        minimum_count=args.minimum_count,
        z_threshold=args.z_threshold,
    )
    from spark.common.runtime import create_session

    session = create_session(runtime, kafka=False, profile="batch")
    try:
        print(json.dumps(run_analytics(session, runtime.lake, config, args.query_id, args.job)))
    finally:
        session.stop()


if __name__ == "__main__":
    main()
