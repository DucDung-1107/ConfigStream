"""Fault injection into disposable owned clones; never mutate accepted fixture history."""

import json
from dataclasses import replace
from uuid import uuid4


def validate(session, lake, query_id, expected):
    from spark.common.lake_schemas import NORMALIZED_SCHEMA
    from spark.streaming.sinks.lake import read_committed

    columns = [entry.strip().split()[0] for entry in NORMALIZED_SCHEMA.split(",")]
    frame = read_committed(session, lake, "processed/snapshots", query_id)
    rows = frame.select(*columns).collect()
    actual = {
        row.snapshot_id: tuple(row[name] for name in columns if name != "snapshot_id")
        for row in rows
    }
    if len(rows) != len(expected) or actual != expected:
        raise ValueError("Exact owned committed projection disagrees")


def run(session, lake, query_id):
    from lake.client import list_commits, s3_client, save_commit
    from spark.common.lake_schemas import NORMALIZED_SCHEMA
    from spark.streaming.sinks.lake import read_committed

    client = s3_client(lake)
    prefix = lake.prefix + "/"
    objects = [
        entry
        for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=lake.bucket, Prefix=prefix
        )
        for entry in page.get("Contents", [])
    ]
    columns = [entry.strip().split()[0] for entry in NORMALIZED_SCHEMA.split(",")]
    baseline = read_committed(session, lake, "processed/snapshots", query_id).select(*columns)
    rows = baseline.collect()
    expected = {
        row.snapshot_id: tuple(row[name] for name in columns if name != "snapshot_id")
        for row in rows
    }
    report = {}
    for case in ("uncommitted", "missing", "corrupt"):
        clone = replace(lake, prefix=lake.prefix + "-disposable-" + str(uuid4()))
        for entry in objects:
            suffix = entry["Key"][len(prefix) :]
            target = clone.object_key(suffix)
            if suffix.startswith("_commits/"):
                original = client.get_object(Bucket=lake.bucket, Key=entry["Key"])
                manifest = json.loads(original["Body"].read())
                manifest["datasets"] = {
                    name: path.replace(lake.prefix, clone.prefix, 1)
                    for name, path in manifest["datasets"].items()
                }
                client.put_object(
                    Bucket=lake.bucket,
                    Key=target,
                    Body=json.dumps(manifest).encode(),
                    IfNoneMatch="*",
                )
            else:
                client.copy_object(
                    Bucket=lake.bucket,
                    Key=target,
                    CopySource={"Bucket": lake.bucket, "Key": entry["Key"]},
                )
        if case == "uncommitted":
            baseline.write.parquet(clone.uri("processed/snapshots/uncommitted-owned"))
            validate(session, clone, query_id, expected)
            report[case] = "PASS_INVISIBLE"
            continue
        files = [
            entry
            for entry in objects
            if "/processed/snapshots/" in entry["Key"] and entry["Key"].endswith(".parquet")
        ]
        selected = max(files, key=lambda entry: entry["Size"])
        target = clone.object_key(selected["Key"][len(prefix) :])
        if case == "missing":
            client.delete_object(Bucket=lake.bucket, Key=target)
        else:
            client.put_object(Bucket=lake.bucket, Key=target, Body=b"owned corrupt parquet")
        try:
            validate(session, clone, query_id, expected)
        except Exception as error:
            report[case] = dict(status="PASS_REJECTED", error_class=type(error).__name__)
        else:
            raise RuntimeError("Owned corrupted/missing history accepted")
    commit = next(iter(list_commits(lake, query_id)))
    try:
        save_commit(lake, commit | {"counts": {"normalized_snapshot": 999}})
    except Exception as error:
        from botocore.exceptions import ClientError

        if (
            not isinstance(error, ClientError)
            or error.response["ResponseMetadata"]["HTTPStatusCode"] != 412
        ):
            raise
        report["conflicting_commit"] = "PASS_CONDITIONAL_REJECTION"
    else:
        raise RuntimeError("Conflicting immutable commit overwritten")
    validate(session, lake, query_id, expected)
    report["accepted_original_unchanged"] = True
    return report
