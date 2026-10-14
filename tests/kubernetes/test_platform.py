"""Opt-in read-only Kubernetes checks; not Spark pipeline acceptance."""

import json
import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.skipif(
    os.environ.get("CONFIGSTREAM_K8S_TESTS") != "read-only",
    reason="Explicit owned-cluster read-only opt-in required",
)


def kubectl(*arguments):
    if not os.environ.get("KUBECONFIG"):
        raise RuntimeError("Explicit dedicated cluster kubeconfig required")
    return subprocess.run(
        [str(ROOT / ".tools/k8s/kubectl"), *arguments],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    ).stdout


def test_owned_single_arm64_node_ready():
    nodes = json.loads(kubectl("get", "nodes", "-o", "json"))["items"]
    assert len(nodes) == 1
    node = nodes[0]
    assert node["metadata"]["name"] == "configstream-k8s"
    assert node["status"]["nodeInfo"]["architecture"] == "arm64"
    assert node["status"]["nodeInfo"]["kubeletVersion"] == "v1.35.9+k3s1"
    assert any(
        condition["type"] == "Ready" and condition["status"] == "True"
        for condition in node["status"]["conditions"]
    )


def test_core_stateful_pods_and_volumes_ready():
    pods = json.loads(kubectl("get", "pods", "-n", "cs-infra", "-o", "json"))["items"]
    for name in ("minio-0", "configstream-single-0"):
        pod = next(item for item in pods if item["metadata"]["name"] == name)
        assert all(status["ready"] for status in pod["status"]["containerStatuses"])
    claims = json.loads(kubectl("get", "pvc", "-n", "cs-infra", "-o", "json"))["items"]
    assert len(claims) == 2
    assert all(claim["status"]["phase"] == "Bound" for claim in claims)


def test_internal_dns_and_minio_health():
    resolved = kubectl(
        "exec",
        "-n",
        "cs-infra",
        "configstream-single-0",
        "--",
        "getent",
        "hosts",
        "minio.cs-infra.svc.cluster.local",
    )
    assert "minio.cs-infra.svc.cluster.local" in resolved
    kubectl(
        "exec",
        "-n",
        "cs-infra",
        "configstream-single-0",
        "--",
        "curl",
        "-fsS",
        "--max-time",
        "10",
        "http://minio.cs-infra.svc.cluster.local:9000/minio/health/ready",
    )
