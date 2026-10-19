import io
import json
from copy import deepcopy
from pathlib import Path

import pyarrow.parquet as parquet
import pytest
from botocore.exceptions import ClientError

from analytics.config import JOB_NAMES, AnalyticsConfig
from analytics.main import run_analytics
from analytics.publication import output_path
from lake.config import LakeConfig
from spark.common.config import SparkConfig
from spark.common.runtime import create_session


class MarkerStore:
    def __init__(self):
        self.objects = {}

    def get_object(self, *, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, *, Bucket, Key, Body, **options):
        assert options["IfNoneMatch"] == "*"
        if Key in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.objects[Key] = Body


@pytest.fixture(scope="module")
def session(tmp_path_factory):
    spark = create_session(
        SparkConfig(
            master="local[2]", runtime_dir=tmp_path_factory.mktemp("analytics-publication")
        ),
        kafka=False,
    )
    yield spark
    spark.stop()


@pytest.fixture
def publication(session, tmp_path, monkeypatch):
    from analytics import jobs, main
    from spark.streaming.sinks import lake as sink

    store = MarkerStore()
    lake = LakeConfig("owned", "owned")
    root = tmp_path / "lake"
    monkeypatch.setattr(LakeConfig, "uri", lambda self, suffix: str(root / suffix))
    monkeypatch.setattr(main, "s3_client", lambda config: store)
    monkeypatch.setattr(
        main, "list_commits", lambda *args: [{"query_id": "owned-query", "batch_id": 0}]
    )
    input_frame = session.createDataFrame(
        [("s", "d", "a", "router", 1)],
        "snapshot_id STRING, diff_id STRING, alert_id STRING, asset_id STRING, version LONG",
    )
    monkeypatch.setattr(sink, "read_committed", lambda *args, **kwargs: input_frame)
    output = session.createDataFrame([("a", 1), ("b", 2)], "asset_id STRING, total LONG").coalesce(
        1
    )
    monkeypatch.setattr(jobs, "all_jobs", lambda *args: dict.fromkeys(JOB_NAMES, output))
    return lake, store, output


def run(session, publication, job="change-frequency"):
    return run_analytics(session, publication[0], AnalyticsConfig(), job=job)


def test_exact_retry_preserves_files_and_rejects_missing_output(session, publication):
    manifest = run(session, publication)
    result = manifest["datasets"]["change-frequency"]
    paths = list(Path(result["path"]).glob("*.parquet"))
    before = [(p.name, p.read_bytes(), p.stat().st_mtime_ns) for p in paths]
    assert run(session, publication) == manifest
    assert [(p.name, p.read_bytes(), p.stat().st_mtime_ns) for p in paths] == before
    paths[0].unlink()
    with pytest.raises(RuntimeError, match="Parquet"):
        run(session, publication)


@pytest.mark.parametrize(
    "damage",
    [
        "changed",
        "extra",
        "rows",
        "schema",
        "schema_type",
        "path",
        "run_id",
        "datasets",
        "attempt_id",
    ],
)
def test_corrupt_cached_output_fails_closed(session, publication, damage):
    manifest = run(session, publication)
    result = manifest["datasets"]["change-frequency"]
    file = next(Path(result["path"]).glob("*.parquet"))
    if damage == "changed":
        table = parquet.ParquetFile(file).read()
        table = table.set_column(table.schema.get_field_index("total"), "total", [[7, 8]])
        parquet.write_table(table, file)
        file.with_name(f".{file.name}.crc").unlink()
    elif damage == "extra":
        file.with_name("extra.parquet").write_bytes(file.read_bytes())
    elif damage == "rows":
        result["rows"] += 1
    elif damage == "schema":
        result["schema"] = {}
    elif damage == "schema_type":
        result["schema"]["fields"][1]["type"] = "string"
    elif damage == "path":
        result["path"] += "-different-run"
    elif damage == "run_id":
        manifest["run_id"] = "unexpected"
    elif damage == "attempt_id":
        manifest["attempt_id"] = "../other-run"
    else:
        manifest["datasets"] = {}
    store = publication[1]
    key = next(iter(store.objects))
    store.objects[key] = json.dumps(manifest).encode()
    with pytest.raises(RuntimeError):
        run(session, publication)


def test_individual_job_cannot_rewrite_committed_all_outputs(session, publication):
    complete = run(session, publication, "all")
    result = complete["datasets"]["change-frequency"]
    files = [
        (p, p.read_bytes(), p.stat().st_mtime_ns) for p in Path(result["path"]).glob("*.parquet")
    ]
    individual = run(session, publication)
    assert individual["datasets"]["change-frequency"]["path"] != result["path"]
    assert all(p.read_bytes() == raw and p.stat().st_mtime_ns == mtime for p, raw, mtime in files)
    assert run(session, publication, "all") == complete


def test_overlapping_attempts_publish_one_immutable_winner(session, publication, monkeypatch):
    store = publication[1]
    original = store.put_object
    attempts = []
    winner_files = []

    def race(**options):
        attempts.append(json.loads(options["Body"]))
        if len(attempts) == 1:
            winner = run(session, publication)
            path = winner["datasets"]["change-frequency"]["path"]
            winner_files.extend(
                (p, p.read_bytes(), p.stat().st_mtime_ns) for p in Path(path).glob("*.parquet")
            )
        original(**options)

    monkeypatch.setattr(store, "put_object", race)
    result = run(session, publication)
    assert len(attempts) == 2
    assert attempts[0]["attempt_id"] != attempts[1]["attempt_id"]
    assert (
        attempts[0]["datasets"]["change-frequency"]["path"]
        != attempts[1]["datasets"]["change-frequency"]["path"]
    )
    assert result == attempts[1]
    assert run(session, publication) == result
    assert len(store.objects) == 1
    assert winner_files
    assert all(
        p.read_bytes() == raw and p.stat().st_mtime_ns == mtime for p, raw, mtime in winner_files
    )


def test_v2_cache_is_still_readable_without_rewrite(session, publication):
    lake, store, output = publication
    manifest = run(session, publication)
    manifest["schema_version"] = "2.0"
    del manifest["attempt_id"]
    result = manifest["datasets"]["change-frequency"]
    result["path"] = output_path(lake, "change-frequency", manifest["run_id"], "change-frequency")
    output.write.parquet(result["path"])
    from spark.streaming.sinks.lake import parquet_inventory

    result["files"] = parquet_inventory(session, result["path"])
    key = next(iter(store.objects))
    store.objects[key] = json.dumps(manifest).encode()
    before = store.objects[key]
    assert run(session, publication) == manifest
    assert store.objects[key] == before


def test_legacy_cache_checks_physical_rows_without_upgrading_manifest(session, publication):
    lake, store, output = publication
    manifest = deepcopy(run(session, publication))
    manifest["schema_version"] = "1.0"
    result = manifest["datasets"]["change-frequency"]
    result.pop("files")
    result["path"] = output_path(
        lake, "change-frequency", manifest["run_id"], "change-frequency", legacy=True
    )
    output.write.parquet(result["path"])
    key = next(iter(store.objects))
    store.objects[key] = json.dumps(manifest).encode()
    before = store.objects[key]
    assert run(session, publication) == manifest
    assert store.objects[key] == before
    output.limit(1).write.mode("overwrite").parquet(result["path"])
    with pytest.raises(RuntimeError, match="row count"):
        run(session, publication)


def test_empty_analytics_output_is_verified(session, publication, monkeypatch):
    from analytics import jobs

    empty = publication[2].limit(0)
    monkeypatch.setattr(jobs, "all_jobs", lambda *args: dict.fromkeys(JOB_NAMES, empty))
    result = run(session, publication)
    assert result["datasets"]["change-frequency"]["rows"] == 0
    assert run(session, publication) == result


@pytest.mark.parametrize(
    "kind,identifier",
    [
        ("snapshots", "snapshot_id"),
        ("diffs", "diff_id"),
        ("alerts", "alert_id"),
    ],
)
def test_cross_query_identity_compares_full_payload(session, kind, identifier):
    from pyspark.sql import functions as F

    from analytics.reconciliation import reconcile_history
    from spark.streaming.sinks.lake import dataset_frame
    from tests.spark.test_lake import batch_outputs

    frame = dataset_frame(batch_outputs(session), f"processed/{kind}").withColumn(
        "query_id", F.lit("first")
    )
    retry = frame.withColumn("query_id", F.lit("second")).withColumn("batch_id", F.lit(99))
    if kind == "snapshots":
        retry = retry.withColumn("ingest_time", F.col("ingest_time") + F.expr("INTERVAL 1 DAY"))
    deliveries = frame.unionByName(retry, allowMissingColumns=True)
    accepted = reconcile_history(deliveries, identifier)
    assert accepted.count() == frame.count()
    assert "query_id" not in accepted.columns
    assert "ingest_time" not in accepted.columns

    if kind == "diffs":
        conflicting = retry.withColumn(
            "details",
            F.transform(
                "details",
                lambda line: F.struct(
                    line.operation.alias("operation"),
                    F.concat(line.line, F.lit(" altered")).alias("line"),
                ),
            ),
        )
    else:
        column = "normalized_content" if kind == "snapshots" else "message"
        conflicting = retry.withColumn(column, F.concat(F.col(column), F.lit(" altered")))
    with pytest.raises(RuntimeError, match="Conflicting"):
        reconcile_history(frame.unionByName(conflicting, allowMissingColumns=True), identifier)


def test_conflicting_versions_and_missing_ids_fail_before_publication(
    session, publication, monkeypatch
):
    from pyspark.sql import functions as F

    from analytics.reconciliation import reconcile_history
    from spark.streaming.sinks import lake as sink

    valid = session.createDataFrame(
        [("s1", "router", 1), ("s2", "router", 1)],
        "snapshot_id STRING, asset_id STRING, version LONG",
    )
    with pytest.raises(RuntimeError, match="asset/version"):
        reconcile_history(valid, "snapshot_id")
    for missing in (None, " "):
        with pytest.raises(RuntimeError, match="missing"):
            reconcile_history(
                valid.limit(1).withColumn("snapshot_id", F.lit(missing).cast("string")),
                "snapshot_id",
            )
    monkeypatch.setattr(sink, "read_committed", lambda *args, **kwargs: valid)
    with pytest.raises(RuntimeError, match="asset/version"):
        run(session, publication)
    assert publication[1].objects == {}
    assert not list(Path(publication[0].uri("analytics")).rglob("*.parquet"))
