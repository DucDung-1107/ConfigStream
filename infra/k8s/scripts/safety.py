"""Phase20K.1 host/guest safety with exclusive campaign receipts and no expired-budget reuse."""

import argparse
import json
import os
import re
import subprocess
from pathlib import Path
from time import sleep, time
from uuid import uuid4

from bootstrap import (
    IMAGE_CACHE,
    LIMA_HOME,
    PRIVATE,
    ROOT,
    TOOLS,
    VM,
    allocated_bytes,
    command,
    guest,
)

POLICY = ROOT / "workloads/results/phase20-k8s/closure-policy-amendment.json"
CAMPAIGNS = ROOT / ".cache/phase20-k8s-closure"


def validate_live_campaign(directory):
    directory = Path(directory).resolve()
    if not directory.is_relative_to(CAMPAIGNS.resolve()):
        raise RuntimeError("Only explicitly registered20K.1 local campaign permitted")
    receipt = json.loads((directory / "receipt.json").read_text())
    policy = json.loads(POLICY.read_text())
    if receipt["decision_id"] != policy["decision_id"]:
        raise RuntimeError("Campaign authorization mismatch")
    record = json.loads((directory / "resources.json").read_text())
    if record["failure"] or not record["samples"]:
        raise RuntimeError("Live safety guard failed")
    if not 0 <= time() - record["samples"][-1]["epoch"] <= 20:
        raise RuntimeError("Live safety sample stale")
    return receipt


def sample():
    pressure = command(["memory_pressure", "-Q"])
    host_percent = int(pressure.split("free percentage:")[-1].strip().rstrip("%"))
    statistics = command(["vm_stat"])
    page_size = int(re.search(r"page size of (\d+) bytes", statistics)[1])
    swapouts = int(re.search(r"Swapouts:\s+(\d+)", statistics)[1]) * page_size
    disk = guest(["df", "-B1", "/"], timeout=20).splitlines()[-1].split()
    memory = guest(["free", "-b"], timeout=20).splitlines()[1].split()
    return dict(
        epoch=time(),
        host_memory_free_percent=host_percent,
        host_free_disk_bytes=__import__("shutil").disk_usage(ROOT).free,
        host_swapout_total_bytes=swapouts,
        guest_free_disk_bytes=int(disk[3]),
        guest_available_memory_bytes=int(memory[-1]),
        allocated_bytes=sum(
            allocated_bytes(path)
            for path in (LIMA_HOME / VM, PRIVATE, TOOLS, IMAGE_CACHE, CAMPAIGNS)
        ),
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--campaign")
    args = parser.parse_args()
    policy = json.loads(POLICY.read_text())
    directory = CAMPAIGNS / (args.campaign or str(uuid4()))
    directory.mkdir(parents=True, exist_ok=False)
    os.environ["LIMA_HOME"] = str(LIMA_HOME)
    receipt = dict(
        decision_id=policy["decision_id"],
        started_epoch=time(),
        campaign_id=directory.name,
        vm=VM,
        source_sha=command(["git", "rev-parse", "HEAD"]).strip(),
    )
    with (directory / "receipt.json").open("x") as output:
        json.dump(receipt, output)
    print(directory, flush=True)
    record = dict(samples=[], failure=None)
    swapped = 0
    while True:
        try:
            current = sample()
            if record["samples"]:
                previous = record["samples"][-1]
                current["swapout_bytes_per_second"] = max(
                    0, current["host_swapout_total_bytes"] - previous["host_swapout_total_bytes"]
                ) / (current["epoch"] - previous["epoch"])
                swapped = swapped + 1 if current["swapout_bytes_per_second"] > 16 * 1024**2 else 0
            record["samples"].append(current)
            if current["host_memory_free_percent"] < policy["minimum_host_memory_free_percent"]:
                raise RuntimeError("Host memory pressure unsafe")
            if (
                min(current["host_free_disk_bytes"], current["guest_free_disk_bytes"])
                < policy["minimum_host_guest_free_disk_bytes"]
            ):
                raise RuntimeError("Free disk reserve unsafe")
            if (
                current["guest_available_memory_bytes"]
                < policy["minimum_guest_available_memory_bytes"]
            ):
                raise RuntimeError("Guest memory unsafe")
            if swapped >= 3:
                raise RuntimeError("Sustained new host swap activity")
        except Exception as error:
            record["failure"] = str(error)
        (directory / "resources.json").write_text(json.dumps(record))
        if record["failure"]:
            environment = {**os.environ, "KUBECONFIG": str(PRIVATE / "kubeconfig")}
            for resource in ("jobs", "pods"):
                subprocess.run(
                    [
                        str(TOOLS / "kubectl"),
                        "delete",
                        resource,
                        "-n",
                        "cs-processing",
                        "-l",
                        "configstream-owned=phase20k",
                        "--wait=false",
                    ],
                    env=environment,
                    timeout=30,
                    check=False,
                )
            subprocess.run(
                ["limactl", "shell", VM, "--", "sudo", "pkill", "-TERM", "-x", "buildkitd"],
                timeout=30,
                check=False,
            )
            raise RuntimeError(record["failure"])
        sleep(policy["sampling_interval_seconds"])


if __name__ == "__main__":
    main()
