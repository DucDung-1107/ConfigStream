"""Build a pinned clean source revision inside the dedicated containerd worker."""

import argparse
import json
import os
import subprocess
from pathlib import Path
from time import sleep, time

from bootstrap import LIMA_HOME, VM, command, guest
from safety import validate_live_campaign


def validate_cached_runtime(root):
    versions = json.loads((root / "infra/k8s/versions.json").read_text())
    revision = versions["accepted_application_source_sha"]
    for name in ("pyproject.toml", "uv.lock", "infra/k8s/Dockerfile"):
        accepted = subprocess.check_output(["git", "show", f"{revision}:{name}"], cwd=root)
        if accepted != (root / name).read_bytes():
            raise RuntimeError(f"Cached runtime incompatible with changed {name}")
    return versions["accepted_application_manifest"]


def stage_source(archive, revision):
    context = guest(["mktemp", "-d", f"/tmp/configstream-source-{revision}.XXXXXX"]).strip()
    command(["limactl", "copy", str(archive), f"{VM}:{context}/source.tar"])
    guest(["tar", "-xf", f"{context}/source.tar", "-C", context])
    return context


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--campaign-directory", type=Path, required=True)
    parser.add_argument("--cached-runtime", action="store_true")
    args = parser.parse_args()
    directory = args.campaign_directory.resolve()
    validate_live_campaign(directory)
    if command(["git", "status", "--porcelain", "--untracked-files=no"]).strip():
        raise RuntimeError("Tracked image source must be a clean committed revision")
    revision = command(["git", "rev-parse", "HEAD"]).strip()
    runtime_digest = validate_cached_runtime(Path.cwd()) if args.cached_runtime else None
    receipt_path = directory / f"build-{revision}.json"
    if receipt_path.exists():
        raise RuntimeError("Existing build receipt cannot be overwritten")
    started = time()
    report = dict(
        source_sha=revision,
        started_epoch=started,
        status="FAIL",
        image=f"configstream-k8s:{revision}",
        timeout_seconds=1800,
        build_mode="verified_cached_runtime" if args.cached_runtime else "full_dependency_build",
        runtime_manifest=runtime_digest,
    )
    receipt_path.write_text(json.dumps(report))
    os.environ["LIMA_HOME"] = str(LIMA_HOME)
    archive = directory / f"source-{revision}.tar"
    command(["git", "archive", "--format=tar", "-o", str(archive), revision])
    context = stage_source(archive, revision)
    try:
        try:
            guest(["sudo", "buildctl", "debug", "workers"])
        except subprocess.CalledProcessError:
            guest(
                [
                    "sudo",
                    "sh",
                    "-c",
                    "PATH=/var/lib/rancher/k3s/data/current/bin:$PATH nohup buildkitd "
                    "--oci-worker=false --containerd-worker=true "
                    "--containerd-worker-addr /run/k3s/containerd/containerd.sock "
                    "--containerd-worker-namespace k8s.io "
                    "> /tmp/closure-buildkit-current.log 2>&1 < /dev/null &",
                ]
            )
            for _attempt in range(30):
                try:
                    guest(["sudo", "buildctl", "debug", "workers"])
                    break
                except subprocess.CalledProcessError:
                    sleep(1)
            else:
                raise RuntimeError("BuildKit readiness deadline exceeded")
        validate_live_campaign(directory)
        with (directory / f"build-{revision}.log").open("x") as output:
            subprocess.run(
                [
                    "limactl",
                    "shell",
                    VM,
                    "--",
                    "sudo",
                    "buildctl",
                    "build",
                    "--frontend",
                    "dockerfile.v0",
                    "--opt",
                    "filename=Dockerfile.cached" if args.cached_runtime else "filename=Dockerfile",
                    "--opt",
                    "image-resolve-mode=local"
                    if args.cached_runtime
                    else "image-resolve-mode=default",
                    "--local",
                    f"context={context}",
                    "--local",
                    f"dockerfile={context}/infra/k8s",
                    "--output",
                    f"type=image,name=docker.io/library/{report['image']},push=false",
                ],
                stdout=output,
                stderr=subprocess.STDOUT,
                check=True,
                timeout=1800,
            )
        validate_live_campaign(directory)
        report["status"] = "PASS_BUILD_ONLY"
    except Exception as error:
        report["error_class"] = type(error).__name__
        raise
    finally:
        report["elapsed_seconds"] = time() - started
        receipt_path.write_text(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
