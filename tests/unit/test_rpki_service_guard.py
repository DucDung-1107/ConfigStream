"""Owned offline resource failures must stop the opt-in service experiment."""

import subprocess

import pytest

from benchmarks.rpki_service import GuestGuard


@pytest.mark.parametrize("failure", ["sample", "disk", "memory", "growth"])
def test_guest_guard_fails_closed(monkeypatch, failure):
    import benchmarks.rpki_service as module

    healthy = {"guest_free_disk_bytes": 48 * 1024**3, "guest_available_memory_bytes": 4 * 1024**3}
    monkeypatch.setattr(module, "guest_resources", lambda: healthy)

    class Context:
        cancelled = False

        def cancelAllJobs(self):
            self.cancelled = True

    class Session:
        sparkContext = Context()

    session = Session()
    guard = GuestGuard(session)
    value = healthy.copy()
    if failure == "disk":
        value["guest_free_disk_bytes"] = 19 * 1024**3
    elif failure == "memory":
        value["guest_available_memory_bytes"] = 511 * 1024**2
    elif failure == "growth":
        value["guest_free_disk_bytes"] = 32 * 1024**3

    def sample():
        if failure == "sample":
            raise subprocess.TimeoutExpired("owned", 15)
        return value

    monkeypatch.setattr(module, "guest_resources", sample)
    guard.sample()
    assert session.sparkContext.cancelled
    with pytest.raises(RuntimeError, match="sampling/limit"):
        guard.check()
