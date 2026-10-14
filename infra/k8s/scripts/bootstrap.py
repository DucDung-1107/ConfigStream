"""Install pinned tooling and one explicitly approved, isolated Lima/K3s server."""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import tarfile
import threading
from pathlib import Path
from time import monotonic, sleep

ROOT = Path(__file__).resolve().parents[3]
PRIVATE = ROOT / ".cache/phase20-k8s"
TOOLS = ROOT / ".tools/k8s"
VM = "configstream-k8s"
LIMA_HOME = Path.home() / ".configstream-k8s-lima"
IMAGE_CACHE = (
    Path.home() / "Library/Caches/lima/download/by-url-sha256/"
    "ea9e18624081b444d7bb30609b098f4e5f507ffabe414dfcda1d02fdec3afea4"
)


def command(arguments, *, capture=True, timeout=300):
    return subprocess.run(
        arguments,
        check=True,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
        timeout=timeout,
    ).stdout


def download(url, path):
    command(["curl", "-fL", "--connect-timeout", "15", "--max-time", "300", "-o", str(path), url])


def verified_archive(url, checksum_url, path):
    download(url, path)
    checksum = path.with_suffix(path.suffix + ".sha256")
    download(checksum_url, checksum)
    lines = checksum.read_text().splitlines()
    matching = [line for line in lines if path.name in line]
    if len(lines) == 1 and not matching:
        matching = lines
    if (
        len(matching) != 1
        or hashlib.sha256(path.read_bytes()).hexdigest() != matching[0].split()[0]
    ):
        raise RuntimeError("Pinned software SHA256 verification failed")


def guest(arguments, **options):
    return command(["limactl", "shell", VM, "--", *arguments], **options)


def allocated_bytes(path):
    if not path.exists():
        return 0
    return int(command(["du", "-sk", str(path)]).split()[0]) * 1024


class Guard:
    def __init__(self, started):
        self.started = started
        self.stop = threading.Event()
        self.failure = None
        self.samples = []
        self.guest_ready = False
        self.thread = threading.Thread(target=self.sample, daemon=True)

    def sample(self):
        while not self.stop.is_set():
            try:
                free = shutil.disk_usage(ROOT).free
                pressure = command(["memory_pressure", "-Q"]).split("free percentage:")[-1]
                host_percent = int(pressure.strip().rstrip("%"))
                vm_root = Path(os.environ["LIMA_HOME"]) / VM
                growth = sum(
                    allocated_bytes(path) for path in (vm_root, PRIVATE, TOOLS, IMAGE_CACHE)
                )
                sample = dict(
                    seconds=monotonic() - self.started,
                    host_free_disk_bytes=free,
                    host_memory_free_percent=host_percent,
                    new_allocated_bytes=growth,
                )
                state = (
                    json.loads(command(["limactl", "list", VM, "--json"]))
                    if vm_root.exists()
                    else None
                )
                if self.guest_ready and (not state or state["status"] != "Running"):
                    raise RuntimeError("Guest resource sampling unavailable")
                if self.guest_ready:
                    disk = guest(["df", "-B1", "/"], timeout=20).splitlines()[-1].split()
                    memory = guest(["free", "-b"], timeout=20).splitlines()[1].split()
                    sample.update(
                        guest_free_disk_bytes=int(disk[3]),
                        guest_available_memory_bytes=int(memory[-1]),
                    )
                    if (
                        sample["guest_free_disk_bytes"] < 20 * 1024**3
                        or sample["guest_available_memory_bytes"] < 512 * 1024**2
                    ):
                        raise RuntimeError("Guest resource guard failed")
                self.samples.append(sample)
                if (
                    free < 20 * 1024**3
                    or host_percent < 15
                    or growth > 15 * 1024**3
                    or sample["seconds"] > 7200
                ):
                    raise RuntimeError("Host/runtime/growth guard failed")
            except Exception as error:
                self.failure = type(error).__name__
                return
            self.stop.wait(5)

    def check(self):
        if self.failure or monotonic() - self.started > 7200:
            raise RuntimeError("Kubernetes safety sampling/limit failed; stop owned execution")


def install_tools(versions):
    TOOLS.mkdir(parents=True, exist_ok=True)
    version = versions["kubectl"]
    binary = TOOLS / "kubectl"
    verified_archive(
        f"https://dl.k8s.io/release/{version}/bin/darwin/arm64/kubectl",
        f"https://dl.k8s.io/release/{version}/bin/darwin/arm64/kubectl.sha256",
        binary,
    )
    version = versions["helm"]
    archive = TOOLS / f"helm-{version}-darwin-arm64.tar.gz"
    verified_archive(
        f"https://get.helm.sh/{archive.name}",
        f"https://get.helm.sh/{archive.name}.sha256sum",
        archive,
    )
    with tarfile.open(archive) as package:
        with package.extractfile("darwin-arm64/helm") as source:
            (TOOLS / "helm").write_bytes(source.read())
    version = versions["kustomize"]
    archive = TOOLS / f"kustomize_{version}_darwin_arm64.tar.gz"
    base = f"https://github.com/kubernetes-sigs/kustomize/releases/download/kustomize%2F{version}"
    verified_archive(f"{base}/{archive.name}", f"{base}/checksums.txt", archive)
    with tarfile.open(archive) as package:
        with package.extractfile("kustomize") as source:
            (TOOLS / "kustomize").write_bytes(source.read())
    for name in ("kubectl", "helm", "kustomize"):
        (TOOLS / name).chmod(0o755)


def install_cluster(versions, guard):
    if (Path(os.environ["LIMA_HOME"]) / VM).exists():
        raise RuntimeError(
            "Existing new VM requires explicit inspection, not implicit reinstallation"
        )
    guard.check()
    command(
        [
            "limactl",
            "start",
            "--name",
            VM,
            "--tty=false",
            str(ROOT / "infra/k8s/platform/lima.yaml"),
        ],
        capture=False,
        timeout=900,
    )
    guard.guest_ready = True
    version = versions["k3s"].replace("+", "%2B")
    binary = PRIVATE / "k3s-arm64"
    verified_archive(
        f"https://github.com/k3s-io/k3s/releases/download/{version}/k3s-arm64",
        f"https://github.com/k3s-io/k3s/releases/download/{version}/sha256sum-arm64.txt",
        binary,
    )
    command(["limactl", "copy", str(binary), f"{VM}:/tmp/k3s-arm64"])
    guest(["sudo", "install", "-m", "755", "/tmp/k3s-arm64", "/usr/local/bin/k3s"])
    unit = PRIVATE / "k3s.service"
    unit.write_text(
        "[Unit]\nDescription=ConfigStream bounded K3s\nAfter=network-online.target\n"
        "Wants=network-online.target\n[Service]\nType=notify\n"
        "ExecStart=/usr/local/bin/k3s server --disable traefik --disable servicelb "
        "--secrets-encryption --write-kubeconfig-mode 600 --tls-san 127.0.0.1 "
        "--node-name configstream-k8s "
        "--kubelet-arg system-reserved=cpu=300m,memory=768Mi "
        "--kubelet-arg kube-reserved=cpu=200m,memory=256Mi "
        "--kubelet-arg eviction-hard=memory.available<512Mi,nodefs.available<20Gi\n"
        "Restart=on-failure\nRestartSec=5\nLimitNOFILE=1048576\nDelegate=yes\n"
        "[Install]\nWantedBy=multi-user.target\n"
    )
    command(["limactl", "copy", str(unit), f"{VM}:/tmp/k3s.service"])
    guest(["sudo", "install", "-m", "644", "/tmp/k3s.service", "/etc/systemd/system/k3s.service"])
    guest(["sudo", "systemctl", "daemon-reload"])
    guest(["sudo", "systemctl", "enable", "--now", "k3s"])
    for _attempt in range(120):
        guard.check()
        try:
            guest(
                [
                    "sudo",
                    "k3s",
                    "kubectl",
                    "wait",
                    "--for=condition=Ready",
                    "node/configstream-k8s",
                    "--timeout=5s",
                ],
                timeout=15,
            )
            break
        except subprocess.SubprocessError:
            sleep(5)
    else:
        raise RuntimeError("K3s readiness cap exceeded")
    kubeconfig = PRIVATE / "kubeconfig"
    kubeconfig.write_text(
        guest(["sudo", "cat", "/etc/rancher/k3s/k3s.yaml"]).replace(
            "127.0.0.1:6443", "127.0.0.1:16443"
        )
    )
    kubeconfig.chmod(0o600)
    guard.check()
    command(
        [
            str(TOOLS / "kubectl"),
            "--kubeconfig",
            str(kubeconfig),
            "wait",
            "--for=condition=Ready",
            "node/configstream-k8s",
            "--timeout=30s",
        ]
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--install", action="store_true")
    parser.add_argument("--resume-before-vm", action="store_true")
    args = parser.parse_args()
    if not args.install:
        parser.error("Explicit --install and registered owner approval required")
    policy = json.loads((ROOT / "workloads/results/phase20-k8s/execution-policy.json").read_text())
    if policy["owner_reply"] != "Duyệt VM riêng và bounded fixtures" or policy["vm_name"] != VM:
        raise RuntimeError("Owner-approved dedicated platform policy missing")
    PRIVATE.mkdir(parents=True, exist_ok=True)
    receipt_path = PRIVATE / "execution-receipt.json"
    result_path = PRIVATE / "bootstrap.json"
    if args.resume_before_vm:
        receipt = json.loads(receipt_path.read_text())
        if receipt["vm"] != VM or receipt["cap_seconds"] != 7200:
            raise RuntimeError("Original execution receipt conflicts")
        original = json.loads(result_path.read_text())
        if original["status"] != "FAIL" or (LIMA_HOME / VM).exists():
            raise RuntimeError("Resume permitted only before dedicated VM creation")
        started = monotonic() - (time_epoch() - receipt["started_epoch"])
        result_path = PRIVATE / "bootstrap-resume.json"
        if result_path.exists():
            raise RuntimeError("Resume attempt already recorded")
    else:
        started = monotonic()
        with receipt_path.open("x") as receipt:
            json.dump(
                dict(
                    started_epoch=time_epoch(), started_monotonic=started, cap_seconds=7200, vm=VM
                ),
                receipt,
            )
    if monotonic() - started >= 7200:
        raise RuntimeError("Original aggregate runtime budget exhausted")
    os.environ["LIMA_HOME"] = str(LIMA_HOME)
    guard = Guard(started)
    guard.thread.start()
    result = dict(status="FAIL", scope="Pinned tools/dedicated single-node platform only", vm=VM)
    try:
        versions = json.loads((ROOT / "infra/k8s/versions.json").read_text())
        install_tools(versions)
        guard.check()
        install_cluster(versions, guard)
        result["status"] = "PASS_PLATFORM_ONLY"
    finally:
        guard.stop.set()
        guard.thread.join(timeout=45)
        result.update(
            seconds=monotonic() - started, samples=guard.samples, guard_failure=guard.failure
        )
        result["lima_home"] = str(LIMA_HOME)
        result_path.write_text(json.dumps(result, indent=2) + "\n")


def time_epoch():
    from time import time

    return time()


if __name__ == "__main__":
    main()
