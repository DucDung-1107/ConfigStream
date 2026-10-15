import sys
from pathlib import Path
from time import monotonic
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import yaml

from spark.common.config import SparkConfig
from spark.common.runtime import create_session

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("changed", [None, "pyproject.toml", "uv.lock", "infra/k8s/Dockerfile"])
def test_cached_runtime_requires_exact_dependency_inputs(monkeypatch, tmp_path, changed):
    monkeypatch.syspath_prepend(str(ROOT / "infra/k8s/scripts"))
    from build import validate_cached_runtime

    (tmp_path / "infra/k8s").mkdir(parents=True)
    (tmp_path / "infra/k8s/versions.json").write_text(
        '{"accepted_application_source_sha":"accepted", "accepted_application_manifest":"digest"}'
    )
    for name in ("pyproject.toml", "uv.lock", "infra/k8s/Dockerfile"):
        (tmp_path / name).write_bytes(b"changed" if name == changed else b"accepted")
    monkeypatch.setattr("build.subprocess.check_output", lambda *args, **kwargs: b"accepted")
    if changed:
        with pytest.raises(RuntimeError, match="Cached runtime incompatible"):
            validate_cached_runtime(tmp_path)
    else:
        assert validate_cached_runtime(tmp_path) == "digest"


def test_cached_dockerfile_binds_accepted_image_and_checks_container_lock():
    import json

    versions = json.loads((ROOT / "infra/k8s/versions.json").read_text())
    dockerfile = (ROOT / "infra/k8s/Dockerfile.cached").read_text()
    assert dockerfile.startswith(
        f"FROM configstream-k8s:{versions['accepted_application_source_sha']}"
        f"@{versions['accepted_application_manifest']}\n"
    )
    assert "cmp pyproject.toml /tmp/dependency-check/pyproject.toml" in dockerfile
    assert "cmp uv.lock /tmp/dependency-check/uv.lock" in dockerfile
    assert dockerfile.endswith("USER 185\n")


def test_build_stages_archive_in_fresh_directory(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "infra/k8s/scripts"))
    import build

    guest = Mock(side_effect=["/tmp/configstream-source-revision.unique\n", ""])
    command = Mock()
    monkeypatch.setattr(build, "guest", guest)
    monkeypatch.setattr(build, "command", command)
    context = build.stage_source(Path("source.tar"), "revision")
    assert context == "/tmp/configstream-source-revision.unique"
    assert guest.call_args_list[0].args[0] == [
        "mktemp",
        "-d",
        "/tmp/configstream-source-revision.XXXXXX",
    ]
    command.assert_called_once_with(
        ["limactl", "copy", "source.tar", f"{build.VM}:{context}/source.tar"]
    )
    assert guest.call_args_list[1].args[0] == ["tar", "-xf", f"{context}/source.tar", "-C", context]


@pytest.mark.parametrize("master,mode", [("local[2]", "cluster"), ("k8s://server", "client")])
def test_native_submission_rejects_local_or_client(monkeypatch, tmp_path, master, mode):
    monkeypatch.setattr("spark.common.runtime.check_runtime", lambda: None)
    pyspark = SimpleNamespace(
        SparkContext=SimpleNamespace(_ensure_initialized=lambda: None),
        SparkConf=lambda: Mock(
            get=lambda name, default: {"spark.master": master, "spark.submit.deployMode": mode}.get(
                name, default
            )
        ),
    )
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", SimpleNamespace(SparkSession=Mock()))
    with pytest.raises(RuntimeError, match="Kubernetes cluster"):
        create_session(SparkConfig(runtime_dir=tmp_path), from_submit=True)


def test_native_submission_does_not_override_master_or_heap(monkeypatch, tmp_path):
    monkeypatch.setattr("spark.common.runtime.check_runtime", lambda: None)
    builder = Mock()
    builder.appName.return_value = builder
    builder.config.return_value = builder
    session = Mock(version="4.0.2")
    session.sparkContext.master = "k8s://https://kubernetes.default.svc:443"
    builder.getOrCreate.return_value = session
    submitted = Mock(
        get=lambda name, default: {
            "spark.master": "k8s://https://kubernetes.default.svc:443",
            "spark.submit.deployMode": "cluster",
        }.get(name, default)
    )
    monkeypatch.setitem(
        sys.modules,
        "pyspark",
        SimpleNamespace(
            SparkConf=lambda: submitted,
            SparkContext=SimpleNamespace(_ensure_initialized=lambda: None),
        ),
    )
    monkeypatch.setitem(
        sys.modules, "pyspark.sql", SimpleNamespace(SparkSession=SimpleNamespace(builder=builder))
    )
    assert create_session(SparkConfig(runtime_dir=tmp_path), from_submit=True) is session
    builder.master.assert_not_called()
    assert not any(
        call.args[0] in {"spark.driver.memory", "spark.jars.packages"}
        for call in builder.config.call_args_list
    )


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


def test_allocated_growth_guard_fails_closed(monkeypatch, tmp_path):
    from infra.k8s.scripts import bootstrap

    monkeypatch.setenv("LIMA_HOME", str(tmp_path))
    monkeypatch.setattr(bootstrap, "allocated_bytes", lambda path: 16 * 1024**3)
    monkeypatch.setattr(
        bootstrap.shutil, "disk_usage", lambda path: SimpleNamespace(free=100 * 1024**3)
    )
    monkeypatch.setattr(bootstrap, "command", lambda args: "free percentage: 50%")
    guard = bootstrap.Guard(monotonic())
    guard.sample()
    assert guard.failure
    with pytest.raises(RuntimeError, match="safety"):
        guard.check()


def test_missing_guest_sampling_fails_closed(monkeypatch, tmp_path):
    from infra.k8s.scripts import bootstrap

    monkeypatch.setenv("LIMA_HOME", str(tmp_path))
    monkeypatch.setattr(bootstrap, "allocated_bytes", lambda path: 0)
    monkeypatch.setattr(
        bootstrap.shutil, "disk_usage", lambda path: SimpleNamespace(free=100 * 1024**3)
    )
    monkeypatch.setattr(bootstrap, "command", lambda args: "free percentage: 50%")
    guard = bootstrap.Guard(monotonic())
    guard.guest_ready = True
    guard.sample()
    assert guard.failure


@pytest.mark.parametrize(
    "value,expected", [("512Mi", 536870912), ("200m", 0.2), ("1000n", 0.000001)]
)
def test_kubernetes_usage_units(monkeypatch, value, expected):
    monkeypatch.syspath_prepend(str(ROOT / "infra/k8s/scripts"))
    from observe import quantity

    assert quantity(value) == pytest.approx(expected)


@pytest.mark.parametrize("state", ["stale", "failed", "wrong_policy", "outside"])
def test_closure_campaign_fails_closed_without_fresh_authorization(monkeypatch, tmp_path, state):
    import json
    from time import time

    monkeypatch.syspath_prepend(str(ROOT / "infra/k8s/scripts"))
    import safety

    monkeypatch.setattr(safety, "CAMPAIGNS", tmp_path / "authorized")
    directory = tmp_path / ("outside" if state == "outside" else "authorized/run")
    directory.mkdir(parents=True)
    policy = json.loads(safety.POLICY.read_text())
    (directory / "receipt.json").write_text(
        json.dumps({"decision_id": "wrong" if state == "wrong_policy" else policy["decision_id"]})
    )
    (directory / "resources.json").write_text(
        json.dumps(
            {
                "failure": state == "failed",
                "samples": [{"epoch": time() - (100 if state == "stale" else 0)}],
            }
        )
    )
    with pytest.raises(RuntimeError):
        safety.validate_live_campaign(directory)


def test_safety_stop_preserves_completed_pod_evidence(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "infra/k8s/scripts"))
    from safety import active_owned_names

    items = [
        {"metadata": {"name": phase}, "status": {"phase": phase}}
        for phase in ("Pending", "Running", "Unknown", "Succeeded", "Failed")
    ]
    assert active_owned_names("pods", items) == ["Pending", "Running", "Unknown"]


def test_safety_stop_preserves_completed_job_evidence(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "infra/k8s/scripts"))
    from safety import active_owned_names

    assert active_owned_names(
        "jobs",
        [
            {"metadata": {"name": "active"}, "status": {"active": 1}},
            {"metadata": {"name": "succeeded"}, "status": {"succeeded": 1}},
            {"metadata": {"name": "failed"}, "status": {"failed": 1}},
        ],
    ) == ["active"]
