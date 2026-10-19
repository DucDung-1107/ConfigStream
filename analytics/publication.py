"""Validate immutable analytics outputs before publishing or reusing a completion marker."""

import re

from analytics.config import JOB_NAMES, JOB_VERSION


def output_path(lake, name, identity, job, *, legacy=False, attempt_id=None):
    if legacy:
        return lake.uri(f"analytics/{name}/run_id={identity}")
    if attempt_id is not None:
        return lake.uri(
            f"analytics/{name}/publication_v3/run_id={identity}/selection={job}/attempt_id={attempt_id}"
        )
    return lake.uri(f"analytics/{name}/publication_v2/run_id={identity}/selection={job}")


def validate_output(session, result, *, checksums):
    from pyspark.sql.types import StructType

    from spark.streaming.sinks.lake import parquet_inventory

    if not isinstance(result, dict) or type(result.get("rows")) is not int or result["rows"] < 0:
        raise RuntimeError("Invalid analytics output row count")
    if checksums and parquet_inventory(session, result["path"]) != result.get("files"):
        raise RuntimeError("Committed analytics Parquet inventory/checksum mismatch")
    try:
        schema = StructType.fromJson(result["schema"])
    except (KeyError, TypeError, ValueError) as error:
        raise RuntimeError("Invalid analytics output schema") from error
    actual = session.read.parquet(result["path"])

    def signature(fields):
        return [(field.name, field.dataType.simpleString()) for field in fields]

    if signature(actual.schema) != signature(schema):
        raise RuntimeError("Committed analytics schema mismatch")
    if actual.count() != result["rows"]:
        raise RuntimeError("Committed analytics row count mismatch")


def validate_commit(session, lake, manifest, identity, config, inputs, job):
    if not isinstance(manifest, dict) or manifest.get("schema_version") not in {
        "1.0",
        "2.0",
        "3.0",
    }:
        raise RuntimeError("Unsupported analytics manifest format")
    if (
        manifest.get("run_id") != identity
        or manifest.get("job_version") != JOB_VERSION
        or manifest.get("parameters") != config.parameters()
    ):
        raise RuntimeError("Analytics manifest disagrees with the requested run")
    try:
        if sorted(manifest["input_commits"]) != sorted(inputs):
            raise ValueError
    except (KeyError, TypeError, ValueError) as error:
        raise RuntimeError("Analytics input commit inventory mismatch") from error
    expected = JOB_NAMES if job == "all" else {job}
    datasets = manifest.get("datasets")
    if not isinstance(datasets, dict) or set(datasets) != expected:
        raise RuntimeError("Analytics dataset inventory disagrees with the requested job")
    legacy = manifest["schema_version"] == "1.0"
    attempt_id = manifest.get("attempt_id") if manifest["schema_version"] == "3.0" else None
    if manifest["schema_version"] == "3.0" and (
        not isinstance(attempt_id, str) or re.fullmatch(r"[0-9a-f]{32}", attempt_id) is None
    ):
        raise RuntimeError("Invalid analytics publication attempt identity")
    for name, result in datasets.items():
        if not isinstance(result, dict) or result.get("path") != output_path(
            lake, name, identity, job, legacy=legacy, attempt_id=attempt_id
        ):
            raise RuntimeError("Analytics manifest contains an unexpected dataset path")
        validate_output(session, result, checksums=not legacy)
