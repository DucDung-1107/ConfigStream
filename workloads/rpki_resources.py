"""Lightweight local safeguards; sampled RSS is not a physical-memory profiler."""

import os
import shutil
import threading
from pathlib import Path
from time import monotonic

from benchmarks.metrics import command


def tree_rss(table: str, root: int):
    processes = {}
    for line in table.splitlines():
        fields = line.split()
        if len(fields) == 3 and all(value.isdecimal() for value in fields):
            identity, parent, rss = map(int, fields)
            processes[identity] = (parent, rss)
    if root not in processes:
        return None
    selected = {root}
    while True:
        expanded = selected | {
            identity for identity, (parent, _) in processes.items() if parent in selected
        }
        if expanded == selected:
            break
        selected = expanded
    return sum(processes[identity][1] * 1024 for identity in selected)


def directory_bytes(root: Path):
    total = 0
    for path in root.rglob("*"):
        try:
            if path.is_file():
                total += path.stat().st_size
        except FileNotFoundError:
            continue
    return total


class ResourceSampler:
    def __init__(self, output: Path, session):
        self.output = output
        self.session = session
        self.initial_bytes = directory_bytes(output)
        self.samples = []
        self.failure = None
        self.stop = threading.Event()
        self.started = monotonic()
        self.thread = threading.Thread(target=self.sample, daemon=True, name="rpki-resource-guard")

    def sample(self):
        while not self.stop.is_set():
            rss = command(["ps", "-axo", "pid=,ppid=,rss="])
            usage = tree_rss(rss, os.getpid()) if rss is not None else None
            try:
                if usage is None:
                    raise OSError("Process-tree resource sampling unavailable")
                free = shutil.disk_usage(self.output).free
                size = directory_bytes(self.output)
            except OSError:
                self.failure = "resource_sampling_failed"
                self.session.sparkContext.cancelAllJobs()
                return
            self.samples.append(
                dict(
                    seconds=monotonic() - self.started,
                    process_tree_rss_bytes=usage,
                    free_disk_bytes=free,
                    experiment_bytes=size,
                )
            )
            if (
                (usage is not None and usage > 12 * 1024**3)
                or free < 20 * 1024**3
                or (size > 50 * 1024**3 or size - self.initial_bytes > 15 * 1024**3)
            ):
                self.failure = "resource_guard_exceeded"
                self.session.sparkContext.cancelAllJobs()
                return
            self.stop.wait(2)

    def check(self):
        if self.failure:
            raise RuntimeError(
                "RPKI resource safety validation failed; incomplete work remains unpublished"
            )

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.stop.set()
        self.thread.join(timeout=15)
