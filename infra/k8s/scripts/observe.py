"""Retain pod lifecycle and sampled Kubernetes working-set/CPU evidence, not raw logs."""

import argparse
import json
import os
import re
import subprocess
from pathlib import Path
from time import sleep, time

from bootstrap import PRIVATE, TOOLS


def quantity(value):
    matched = re.fullmatch(r"(\d+(?:\.\d+)?)([A-Za-z]*)", value)
    if matched is None:
        raise ValueError("Unsupported Kubernetes quantity")
    factors = {"": 1, "n": 1e-9, "u": 1e-6, "m": 1e-3, "Ki": 1024, "Mi": 1024**2, "Gi": 1024**3}
    return float(matched[1]) * factors[matched[2]]


def get(*arguments):
    result = subprocess.run(
        [str(TOOLS / "kubectl"), *arguments],
        text=True,
        capture_output=True,
        check=True,
        timeout=20,
        env={**os.environ, "KUBECONFIG": str(PRIVATE / "kubeconfig")},
    )
    return json.loads(result.stdout)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise RuntimeError("Existing observation evidence cannot be overwritten")
    report = dict(
        metric_definition="Kubernetes sampled working-set bytes/CPU cores; not process RSS",
        samples=[],
    )
    while True:
        sample = dict(epoch=time())
        try:
            sample["pods"] = [
                dict(
                    namespace=pod["metadata"]["namespace"],
                    name=pod["metadata"]["name"],
                    uid=pod["metadata"]["uid"],
                    phase=pod["status"].get("phase"),
                    labels=pod["metadata"].get("labels", {}),
                    containers=pod["status"].get("containerStatuses", []),
                )
                for pod in get("get", "pods", "-A", "-o", "json")["items"]
            ]
            sample["usage"] = [
                dict(
                    namespace=pod["metadata"]["namespace"],
                    name=pod["metadata"]["name"],
                    working_set_bytes=sum(
                        quantity(container["usage"]["memory"]) for container in pod["containers"]
                    ),
                    cpu_cores=sum(
                        quantity(container["usage"]["cpu"]) for container in pod["containers"]
                    ),
                )
                for pod in get("get", "--raw", "/apis/metrics.k8s.io/v1beta1/pods")["items"]
            ]
        except Exception as error:
            sample["observation_error"] = type(error).__name__
        report["samples"].append(sample)
        args.output.write_text(json.dumps(report))
        sleep(5)


if __name__ == "__main__":
    main()
