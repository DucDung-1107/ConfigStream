import json
import os
import subprocess
import sys
from dataclasses import replace
from uuid import uuid4

import pytest
from confluent_kafka import TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec
from fastapi.testclient import TestClient

from api.app import create_app
from api.config import ApiConfig
from lake.client import ensure_bucket, list_commits
from lake.config import LakeConfig
from messaging.config import KafkaConfig
from messaging.kafka_producer import KafkaProducer
from scripts.kafka_stateful_demo import demo_history
from search.client import create_client, initialize_indices
from search.config import KINDS, SearchConfig
from search.reindex import rebuild
from spark.common.config import SparkConfig
from spark.common.runtime import create_session

pytestmark = pytest.mark.api_integration


def test_out_of_order_restart_rebuild_is_http_visible(tmp_path):
    namespace = f"phase9-e2e-{uuid4().hex[:8]}"
    lake = replace(LakeConfig.from_env(), prefix=namespace)
    search = replace(SearchConfig.from_env(), index_prefix=namespace)
    ensure_bucket(lake)
    kafka = KafkaConfig.from_env()
    admin = AdminClient(kafka.client_options())
    partitions = admin.list_topics(timeout=10).topics["config.snapshots"].partitions
    requested = {
        TopicPartition("config.snapshots", partition): OffsetSpec.latest()
        for partition in partitions
    }
    offsets = {
        partition.partition: result.result(timeout=10).offset
        for partition, result in admin.list_offsets(requested, request_timeout=10).items()
    }
    checkpoint = tmp_path / "checkpoint"
    environment = {
        **os.environ,
        "CONFIGSTREAM_LAKE_ENABLED": "true",
        "CONFIGSTREAM_SEARCH_ENABLED": "true",
        "CONFIGSTREAM_LAKE_PREFIX": namespace,
        "CONFIGSTREAM_SEARCH_INDEX_PREFIX": namespace,
        "SPARK_MASTER": "local[2]",
        "SPARK_CHECKPOINT_DIR": str(checkpoint),
        "SPARK_STARTING_OFFSETS": json.dumps({"config.snapshots": offsets}),
    }
    out_of_order = demo_history(namespace + "-out-of-order")
    normal = demo_history(namespace + "-normal")

    def run(records):
        with KafkaProducer(kafka) as producer:
            for record in records:
                producer.publish(record)
        result = subprocess.run(
            [sys.executable, "-m", "spark.streaming.main", "--available-now"],
            env=environment,
            text=True,
            capture_output=True,
            timeout=240,
        )
        assert result.returncode == 0, result.stderr[-10000:]
        for forbidden in (lake.access_key, lake.secret_key, "synthetic-before", "synthetic-after"):
            assert forbidden not in result.stdout + result.stderr

    with create_client(search) as backend:
        initialize_indices(search, backend)
        try:
            with TestClient(create_app(ApiConfig(search=search))) as http:
                run([out_of_order[0], out_of_order[2], out_of_order[1], normal[0], normal[1]])

                def refresh():
                    backend.indices.refresh(index=",".join(search.alias(kind) for kind in KINDS))

                def visible():
                    responses = {}
                    for kind in KINDS:
                        response = http.get(f"/api/v1/{kind}")
                        assert response.status_code == 200, response.text
                        for forbidden in (
                            "normalized_content",
                            "content",
                            "value",
                            "details",
                            "synthetic-before",
                            "synthetic-after",
                            "SENSITIVE_TEST_MARKER",
                            "_index",
                            "_score",
                            "_shards",
                        ):
                            assert forbidden not in response.text
                        responses[kind] = response.json()["items"]
                    return responses

                refresh()
                assert (
                    http.get(f"/api/v1/assets/{out_of_order[0].asset_id}/latest-snapshot").json()[
                        "version"
                    ]
                    == 3
                )
                assert (
                    http.get(f"/api/v1/assets/{normal[0].asset_id}/latest-snapshot").json()[
                        "version"
                    ]
                    == 2
                )
                old_pairs = http.get(
                    "/api/v1/diffs", params={"asset_id": normal[0].asset_id}
                ).json()["items"]
                assert len(old_pairs) == 1
                run([normal[2], out_of_order[1]])
                refresh()
                expected = visible()
                assert {kind: len(items) for kind, items in expected.items()} == {
                    "snapshots": 6,
                    "diffs": 4,
                    "alerts": 4,
                    "diagnostics": 0,
                }
                assert old_pairs[0] in expected["diffs"]
                for history in (out_of_order, normal):
                    versions = {snapshot.snapshot_id: snapshot.version for snapshot in history}
                    diffs = http.get(
                        "/api/v1/diffs", params={"asset_id": history[0].asset_id}
                    ).json()["items"]
                    assert {
                        (versions[item["old_snapshot_id"]], versions[item["new_snapshot_id"]])
                        for item in diffs
                    } == {(1, 2), (2, 3)}
                    alerts = http.get(
                        "/api/v1/alerts", params={"asset_id": history[0].asset_id}
                    ).json()["items"]
                    assert {item["rule_id"] for item in alerts} == {
                        "logging-disabled",
                        "telnet-enabled",
                    }
                stats = http.get("/api/v1/stats/alerts").json()
                assert {item["key"]: item["count"] for item in stats["by_severity"]} == {
                    "high": 2,
                    "critical": 2,
                }
                query_id = json.loads((checkpoint / "metadata").read_text())["id"]
                commits = list_commits(lake, query_id)
                assert (
                    sum(commit["counts"].get("normalized_snapshot", 0) for commit in commits) == 6
                )
                backend.indices.delete(index=",".join(search.index(kind) for kind in KINDS))
                assert http.get("/readyz").status_code == 503
                initialize_indices(search, backend)
                assert http.get("/readyz").status_code == 200
                assert http.get("/api/v1/snapshots").json()["count"] == 0
                session = create_session(
                    replace(SparkConfig.from_env(environment), lake=lake), kafka=False
                )
                try:
                    for _ in range(2):
                        rebuild(session, lake, search, query_id)
                        refresh()
                        assert visible() == expected
                finally:
                    session.stop()
        finally:
            backend.indices.delete(
                index=",".join(search.index(kind) for kind in KINDS), ignore_unavailable=True
            )
