"""Owned fixtures on actual Kafka, checkpoints and MinIO; opt-in, isolated."""

import os
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest

pytestmark = pytest.mark.lake_integration


@pytest.fixture(scope="module")
def dns_services(tmp_path_factory):
    if os.environ.get("RUN_PHASE19_SERVICES") != "1":
        pytest.skip("explicit DNS service fixture opt-in required")
    from lake.config import LakeConfig
    from spark.common.config import SparkConfig
    from spark.common.runtime import create_session

    runtime = tmp_path_factory.mktemp("dns-services")
    (runtime / "ivy").symlink_to(Path(".cache/spark/ivy").resolve(), target_is_directory=True)
    session = create_session(
        SparkConfig(
            runtime_dir=runtime, lake=LakeConfig.from_env(), master="local[2]", driver_memory="2g"
        )
    )
    yield session
    session.stop()


@pytest.mark.parametrize(
    "failure", ["before_output", "partial_output", "before_commit", "after_commit"]
)
def test_actual_source_checkpoint_recovery(dns_services, tmp_path, failure):
    from lake.config import LakeConfig
    from lake.client import ensure_bucket
    from lake.dns import DnsStore, read_source_completion
    from messaging.config import KafkaConfig
    from messaging.dns import ensure_topic, publish
    from spark.dns import DnsProcessor, start_query
    from spark.dns_history import finalize_source, read_table
    from tests.unit.test_dns_measurements import owned_source
    from workloads.dns_oracle import build

    path, source, rows = owned_source(tmp_path)
    oracle = tmp_path / "oracle"
    build(path, source, oracle)
    query_id = str(uuid4())
    topic = f"temporal.dns.source.v1.{query_id}"
    kafka = KafkaConfig.from_env()
    ensure_topic(kafka, topic)
    config = replace(LakeConfig.from_env(), prefix=f"phase19-owned/{query_id}")
    ensure_bucket(config)
    store = DnsStore(config=config)
    publish(path, source, tmp_path / "progress.json", kafka, topic)
    artifact = source.manifest.artifacts[0].sha256
    processor = DnsProcessor(
        dns_services, store, query_id, {artifact: source}, [str(oracle / "raw.parquet")]
    )
    active = [True]

    def fault(point):
        if active[0] and point == failure:
            if point == "partial_output":
                return True
            raise RuntimeError("owned DNS checkpoint interruption")
        return False

    processor.fault = fault
    query = start_query(
        dns_services, processor, kafka.bootstrap_servers, tmp_path / "checkpoint", topic
    )
    from pyspark.errors import StreamingQueryException

    with pytest.raises(StreamingQueryException):
        query.awaitTermination(120)
    query.stop()
    active[0] = False
    retry = start_query(
        dns_services, processor, kafka.bootstrap_servers, tmp_path / "checkpoint", topic
    )
    assert retry.awaitTermination(120)
    retry.stop()
    result = finalize_source(dns_services, store, query_id, source, oracle)
    assert result["source_rows"] == 6 and result["address_groups"] == 2
    assert read_source_completion(store, query_id, artifact)["entity_coverage"] == "unknown"
    assert read_table(dns_services, store, query_id, artifact, "contributors").count() == 2
