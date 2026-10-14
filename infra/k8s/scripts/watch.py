"""Watch the original owner-approved campaign budget without resetting its clock."""

import json
import os
from time import monotonic, sleep

from bootstrap import LIMA_HOME, PRIVATE, Guard, time_epoch


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
        raise RuntimeError("Campaign guard failed: stop current owned execution")
    finally:
        guard.stop.set()
        guard.thread.join(timeout=45)


if __name__ == "__main__":
    main()
