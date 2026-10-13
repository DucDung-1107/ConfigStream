"""Actual MinIO integrity tests in disposable owned namespaces, never external evidence."""

import json
from dataclasses import replace
from uuid import uuid4

import pytest

from tests.phase19_integration.test_dns_services import dns_services

pytestmark = pytest.mark.lake_integration
__all__ = ["dns_services"]


@pytest.mark.parametrize(
    "scenario",
    [
        "missing_source",
        "corrupt_source",
        "missing_raw_file",
        "corrupt_raw_projection",
        "missing_group_commit",
        "corrupt_value_reference",
        "sealed_retry",
        "conflicting_coordinate",
        "late_completion",
    ],
)
def test_actual_owned_integrity(dns_services, tmp_path, scenario):
    from py4j.protocol import Py4JJavaError

    from lake.client import s3_client
    from lake.config import LakeConfig
    from lake.dns import DnsStore, read_source_completion
    from processing.dns import read_source
    from schemas.dns import DnsChunk
    from spark.dns import DnsProcessor, committed_raw
    from spark.dns_history import finalize_source, read_table
    from tests.unit.test_dns_measurements import owned_source
    from workloads.dns_oracle import build

    path, source, rows = owned_source(tmp_path)
    oracle = tmp_path / "oracle"
    build(path, source, oracle)
    query = str(uuid4())
    config = replace(LakeConfig.from_env(), prefix=f"phase19-owned-integrity/{query}")
    store = DnsStore(config=config)
    artifact = source.manifest.artifacts[0].sha256
    processor = DnsProcessor(
        dns_services, store, query, {artifact: source}, [str(oracle / "raw.parquet")]
    )
    values = tuple(read_source(path, source))

    def batch(members):
        chunk = DnsChunk(artifact_sha256=artifact, rows=members)
        return dns_services.createDataFrame([(chunk.model_dump_json(),)], "value STRING")

    processor.process(batch(values), 0)
    client = s3_client(config)

    def remove(key):
        client.delete_object(Bucket=config.bucket, Key=config.object_key(store.key(key)))

    def change(key, value):
        client.put_object(
            Bucket=config.bucket,
            Key=config.object_key(store.key(key)),
            Body=json.dumps(value).encode(),
        )

    if scenario in {"missing_source", "corrupt_source"}:
        key = f"sources/{source.manifest.manifest_id}"
        if scenario == "missing_source":
            remove(key)
        else:
            broken = source.model_dump(mode="json")
            broken["manifest"]["source_scope"] = "owned:wrong"
            change(key, broken)
        with pytest.raises(ValueError):
            with committed_raw(dns_services, store, query, processor.expected, complete=True):
                pass
    elif scenario in {"missing_raw_file", "corrupt_raw_projection"}:
        commit = store.inventory(query)[0]
        if scenario == "missing_raw_file":
            prefix = commit["path"].split(f"s3a://{config.bucket}/", 1)[1]
            files = client.list_objects_v2(Bucket=config.bucket, Prefix=prefix)["Contents"]
            part = next(row["Key"] for row in files if row["Key"].endswith(".parquet"))
            client.delete_object(Bucket=config.bucket, Key=part)
        else:
            frame = dns_services.read.parquet(commit["path"])
            frame.limit(0).write.mode("overwrite").parquet(commit["path"])
        with pytest.raises((ValueError, Py4JJavaError)):
            with committed_raw(dns_services, store, query, processor.expected, complete=True):
                pass
    elif scenario == "conflicting_coordinate":
        conflict = values[0].model_copy(update={"worker_id": 99})
        with pytest.raises(ValueError):
            processor.process(batch((conflict,)), 1)
        assert len(store.inventory(query)) == 1
    else:
        if scenario == "late_completion":

            def interrupt(point):
                if point == "before_completion":
                    raise RuntimeError("owned analytical completion interruption")

            with pytest.raises(RuntimeError):
                finalize_source(dns_services, store, query, source, oracle, fault=interrupt)
            with pytest.raises(ValueError):
                read_source_completion(store, query, artifact)
        finalize_source(dns_services, store, query, source, oracle)
        if scenario == "missing_group_commit":
            remove(f"tables/{query}/groups/{artifact}")
            with pytest.raises(ValueError):
                read_source_completion(store, query, artifact)
        elif scenario == "corrupt_value_reference":
            key = f"tables/{query}/values/{artifact}"
            broken = store.get(key) | {"projection_sha256": "0" * 64}
            change(key, broken)
            with pytest.raises(ValueError):
                read_source_completion(store, query, artifact)
            with pytest.raises(ValueError):
                read_table(dns_services, store, query, artifact, "values")
        elif scenario == "sealed_retry":
            inventory = store.inventory(query)
            processor.process(batch(values), 1)
            assert store.inventory(query) == inventory
        else:
            assert read_source_completion(store, query, artifact)["source_rows"] == 6
