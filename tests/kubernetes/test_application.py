"""Read-only acceptance of already executed, independently checked owned Kubernetes fixtures."""

import json
import os
from pathlib import Path

import httpx
import pytest

from tests.kubernetes.test_platform import kubectl

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.skipif(
    os.environ.get("CONFIGSTREAM_K8S_TESTS") != "closure",
    reason="Explicit completed owned-fixture application acceptance required",
)


def evidence(suffix):
    return json.loads(
        kubectl(
            "exec", "-n", "cs-processing", "closure-inspector", "--", "cat", "/evidence/" + suffix
        )
    )


def test_native_driver_and_separate_executor_completed():
    pods = json.loads(kubectl("get", "pods", "-n", "cs-processing", "-o", "json"))["items"]
    drivers = [
        pod
        for pod in pods
        if pod["metadata"].get("labels", {}).get("spark-role") == "driver"
        and pod["status"]["phase"] == "Succeeded"
    ]
    executors = [
        pod
        for pod in pods
        if pod["metadata"].get("labels", {}).get("spark-role") == "executor"
        and pod["status"]["phase"] == "Succeeded"
    ]
    assert drivers and executors
    driver_ids = {pod["metadata"]["labels"]["spark-app-selector"] for pod in drivers}
    assert any(pod["metadata"]["labels"]["spark-app-selector"] in driver_ids for pod in executors)


def test_exact_batch_replay_and_recovered_history():
    for prefix in ("closure-20k1", "closure-20k1-recovery"):
        result = evidence(prefix + "/batch-result.json")
        assert result["master"].startswith("k8s://")
        assert result["snapshots"] == 2 and result["adjacent_diffs"] == 1
        assert result["exact_projection"] == "PASS"
    marker = evidence("closure-20k1-recovery/commit-before-ack.json")
    assert marker["fault"] == "after_commit"


def test_owned_corrupt_and_uncommitted_history_fails_closed():
    result = evidence("closure-20k1-recovery/integrity-result.json")
    assert result["uncommitted"] == "PASS_INVISIBLE"
    assert result["missing"]["status"] == result["corrupt"]["status"] == "PASS_REJECTED"
    assert result["conflicting_commit"] == "PASS_CONDITIONAL_REJECTION"
    assert result["accepted_original_unchanged"] is True


def test_four_semantic_roles_preserved():
    result = evidence("closure-20k1-domains02/roles/results.json")
    assert set(result) == {"terraform", "rpki", "ris", "dns"}
    assert all(row["status"] == "PASS_INTEGRATION" for row in result.values())
    assert result["terraform"]["logical_versions"] == 8
    assert result["rpki"]["logical_source_rows"] == 7 and result["rpki"]["states"] == [3, 3]
    assert result["ris"]["logical_occurrences"] == 4 and result["ris"]["distinct_fingerprints"] == 2
    assert result["ris"]["recovery_after_commit"] is True
    assert result["dns"]["logical_source_rows"] == 6 and result["dns"]["groups"] == 2
    assert result["dns"]["entity_coverage"] == "unknown"


def test_verified_api_projection_and_readiness():
    with httpx.Client(base_url="http://127.0.0.1:18800", timeout=10) as client:
        assert client.get("/readyz").json() == {"status": "ok"}
        snapshots = client.get("/api/v1/snapshots?asset_id=k8s-owned-router").json()
        assert snapshots["count"] == 2
        assert {row["snapshot_id"] for row in snapshots["items"]} == {
            "k8s-owned-v1",
            "k8s-owned-v2",
        }
        assert client.get("/api/v1/assets/k8s-owned-router/latest-snapshot").json()["version"] == 2


def test_retained_kafka_minio_restart_evidence():
    directory = ROOT / ".cache/phase20-k8s-closure/20k1-7g"
    before = json.loads((directory / "persistence-before.json").read_text())
    after = json.loads((directory / "persistence-after.json").read_text())
    assert before == after and len(before["objects"]) == 20
    assert before["kafka_config_snapshots_end_offset"] == 3
    old = json.loads((directory / "stateful-pods-before.json").read_text())["items"]
    new = json.loads((directory / "stateful-pods-after.json").read_text())["items"]
    assert {row["metadata"]["uid"] for row in old}.isdisjoint(
        {row["metadata"]["uid"] for row in new}
    )
