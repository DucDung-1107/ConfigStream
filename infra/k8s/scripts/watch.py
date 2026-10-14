"""Watch the original owner-approved campaign budget without resetting its clock."""

import json
import os
import subprocess
from time import monotonic, sleep

from bootstrap import LIMA_HOME, PRIVATE, ROOT, Guard, guest, time_epoch


def main():
    receipt = json.loads((PRIVATE / "execution-receipt.json").read_text())
    started = monotonic() - (time_epoch() - receipt["started_epoch"])
    os.environ["LIMA_HOME"] = str(LIMA_HOME)
    guard = Guard(started)
    guard.guest_ready = True
    guard.thread.start()
    try:
        while not guard.failure:
            sleep(5)
            (PRIVATE / "resource-watch.json").write_text(
                json.dumps(dict(samples=guard.samples, failure=guard.failure))
            )
        environment = {**os.environ, "KUBECONFIG": str(PRIVATE / "kubeconfig")}
        for resource in ("jobs", "pods"):
            subprocess.run(
                [
                    str(ROOT / ".tools/k8s/kubectl"),
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
        guest(["sudo", "pkill", "-TERM", "-x", "buildkitd"], timeout=30)
        raise RuntimeError("Campaign guard failed; stopped owned jobs/build only, retained PVCs")
    finally:
        guard.stop.set()
        guard.thread.join(timeout=45)


if __name__ == "__main__":
    main()
