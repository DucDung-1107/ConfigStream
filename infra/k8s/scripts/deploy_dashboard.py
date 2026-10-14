"""Build/deploy only the approved static dashboard in the dedicated K3s VM."""

import json
import os
import subprocess
import threading
from time import sleep, time
from uuid import uuid4

import yaml
from bootstrap import LIMA_HOME, PRIVATE, ROOT, TOOLS, VM, command, guest
from safety import sample


def check(measurement):
    if (
        min(measurement["host_free_disk_bytes"], measurement["guest_free_disk_bytes"])
        < 20 * 1024**3
    ):
        raise RuntimeError("Disk reserve below20GiB")
    if measurement["guest_available_memory_bytes"] < 512 * 1024**2:
        raise RuntimeError("Guest available memory below512MiB")
    if measurement["host_memory_free_percent"] < 15:
        raise RuntimeError("Unsafe host memory pressure")


def main():
    os.environ["LIMA_HOME"] = str(LIMA_HOME)
    os.environ["KUBECONFIG"] = str(PRIVATE / "kubeconfig")
    if command(["git", "status", "--porcelain"]).strip():
        raise RuntimeError("Dashboard image requires a clean source commit")
    revision = command(["git", "rev-parse", "HEAD"]).strip()
    directory = ROOT / ".cache/phase20v/runs" / str(uuid4())
    directory.mkdir(parents=True, exist_ok=False)
    receipt = dict(
        source_sha=revision,
        image=f"configstream-dashboard:{revision}",
        mode="static-owned-dashboard",
        started_epoch=time(),
        build_cap_seconds=1800,
        status="RUNNING",
        samples=[],
    )
    receipt["samples"].append(sample())
    check(receipt["samples"][-1])
    (directory / "receipt.json").write_text(json.dumps(receipt, indent=2))
    archive = directory / "source.tar"
    command(["git", "archive", "--format=tar", "-o", str(archive), revision, "frontend"])
    command(["limactl", "copy", str(archive), f"{VM}:/tmp/dashboard-source.tar"])
    guest(
        [
            "sh",
            "-c",
            "mkdir -p /tmp/configstream-dashboard && "
            "tar -xf /tmp/dashboard-source.tar -C /tmp/configstream-dashboard",
        ]
    )
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
                "> /tmp/dashboard-buildkit.log 2>&1 < /dev/null &",
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
    stop = threading.Event()
    process = None

    def monitor():
        swapping = 0
        while not stop.wait(5):
            try:
                current = sample()
                previous = receipt["samples"][-1]
                rate = max(
                    0, current["host_swapout_total_bytes"] - previous["host_swapout_total_bytes"]
                ) / (current["epoch"] - previous["epoch"])
                current["new_swapout_bytes_per_second"] = rate
                receipt["samples"].append(current)
                check(current)
                swapping = swapping + 1 if rate > 16 * 1024**2 else 0
                if swapping >= 3:
                    raise RuntimeError("Sustained new host swap-out")
            except Exception as error:
                receipt["guard_failure"] = str(error)
                if process is not None:
                    process.terminate()
                return
            (directory / "receipt.json").write_text(json.dumps(receipt))

    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()
    try:
        with (directory / "build.log").open("x") as output:
            process = subprocess.Popen(
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
                    "context=/tmp/configstream-dashboard/frontend",
                    "--local",
                    "dockerfile=/tmp/configstream-dashboard/frontend",
                    "--output",
                    f"type=image,name=docker.io/library/{receipt['image']},push=false",
                ],
                stdout=output,
                stderr=subprocess.STDOUT,
            )
            if process.wait(timeout=1800) != 0:
                raise RuntimeError("Dashboard image build failed")
        if receipt.get("guard_failure"):
            raise RuntimeError(receipt["guard_failure"])
        check(sample())
        rendered = command(
            [str(TOOLS / "kustomize"), "build", str(ROOT / "infra/k8s/overlays/frontend")]
        )
        documents = list(yaml.safe_load_all(rendered))
        for document in documents:
            if document["kind"] == "Deployment":
                document["spec"]["template"]["spec"]["containers"][0]["image"] = receipt["image"]
        subprocess.run(
            [str(TOOLS / "kubectl"), "apply", "-f", "-"],
            input=yaml.safe_dump_all(documents),
            text=True,
            check=True,
        )
        command(
            [
                str(TOOLS / "kubectl"),
                "rollout",
                "status",
                "deployment/dashboard",
                "-n",
                "cs-app",
                "--timeout=120s",
            ]
        )
        receipt["status"] = "PASS_DEPLOYMENT_ONLY"
    except Exception as error:
        receipt["status"] = "FAIL"
        receipt["error"] = str(error)
        if process is not None and process.poll() is None:
            process.terminate()
        raise
    finally:
        stop.set()
        thread.join(timeout=25)
        receipt["elapsed_seconds"] = time() - receipt["started_epoch"]
        (directory / "receipt.json").write_text(json.dumps(receipt, indent=2))
        print(directory)


if __name__ == "__main__":
    main()
