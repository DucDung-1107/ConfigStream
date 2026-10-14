from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml

from spark.common.config import SparkConfig
from spark.common.runtime import create_session

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("master,mode", [("local[2]", "cluster"), ("k8s://server", "client")])
def test_native_submission_rejects_local_or_client(monkeypatch, tmp_path, master, mode):
    import pyspark

    monkeypatch.setattr("spark.common.runtime.check_runtime", lambda: None)
    monkeypatch.setattr(
        pyspark,
        "SparkConf",
        lambda: Mock(
            get=lambda name, default: {"spark.master": master, "spark.submit.deployMode": mode}.get(
                name, default
            )
        ),
    )
    with pytest.raises(RuntimeError, match="Kubernetes cluster"):
        create_session(SparkConfig(runtime_dir=tmp_path), from_submit=True)


def test_manifests_never_embed_credentials_or_public_admin_services():
    for path in (ROOT / "infra/k8s").rglob("*.yaml"):
        for document in yaml.safe_load_all(path.read_text()):
            if not isinstance(document, dict):
                continue
            assert document.get("kind") != "Secret"
            if document.get("kind") == "Service":
                assert document["spec"].get("type", "ClusterIP") == "ClusterIP"
        assert ":latest" not in path.read_text()


def test_dedicated_vm_has_no_private_source_mounts():
    platform = yaml.safe_load((ROOT / "infra/k8s/platform/lima.yaml").read_text())
    assert platform["cpus"] == 3
    assert platform["memory"] == "6GiB"
    assert platform["disk"] == "48GiB"
    assert platform["mounts"] == []


def test_spark_rbac_is_namespaced_and_has_no_secrets_access():
    documents = list(yaml.safe_load_all((ROOT / "infra/k8s/base/processing.yaml").read_text()))
    role = next(document for document in documents if document["kind"] == "Role")
    assert role["metadata"]["namespace"] == "cs-processing"
    assert "secrets" not in role["rules"][0]["resources"]
    assert "*" not in role["rules"][0]["verbs"]
