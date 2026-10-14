"""Build a pinned clean source revision inside the dedicated containerd worker."""

import argparse
import json
import os
import subprocess
from pathlib import Path
from time import sleep, time

from bootstrap import LIMA_HOME, VM, command, guest
from safety import validate_live_campaign


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--campaign-directory", type=Path, required=True)
    args = parser.parse_args()
    directory = args.campaign_directory.resolve()
    validate_live_campaign(directory)
    if command(["git", "status", "--porcelain"]).strip():
        raise RuntimeError("Image source must be a clean committed revision")
    revision = command(["git", "rev-parse", "HEAD"]).strip()
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
    )
    receipt_path.write_text(json.dumps(report))
    os.environ["LIMA_HOME"] = str(LIMA_HOME)
    archive = directory / f"source-{revision}.tar"
    command(["git", "archive", "--format=tar", "-o", str(archive), revision])
    command(["limactl", "copy", str(archive), f"{VM}:/tmp/closure-source.tar"])
    guest(
        [
            "sh",
            "-c",
            "mkdir -p /tmp/configstream-closure-current && "
            "tar -xf /tmp/closure-source.tar -C /tmp/configstream-closure-current",
        ]
    )
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
                    "--local",
                    "context=/tmp/configstream-closure-current",
                    "--local",
                    "dockerfile=/tmp/configstream-closure-current/infra/k8s",
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
