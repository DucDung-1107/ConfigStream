import pytest

from messaging.ack_window import AckWindow
from messaging.config import KafkaConfig


class Producer:
    def __init__(self, mode="success"):
        self.callbacks = []
        self.mode = mode
        self.flushes = 0

    def produce(self, topic, **kwargs):
        self.callbacks.append(kwargs["on_delivery"])

    def flush(self, timeout):
        self.flushes += 1
        callbacks, self.callbacks = self.callbacks, []
        for index, callback in reversed(list(enumerate(callbacks))):
            if self.mode == "missing" and index == 0:
                continue
            callback("failure" if self.mode == "error" and index == 0 else None, None)
        return 1 if self.mode == "pending" else 0


def test_grouped_ack_matches_sequential_and_reduces_flushes():
    results = []
    for size in (1, 4):
        producer, frontiers = Producer(), []
        window = AckWindow(producer, KafkaConfig(), "owned", frontiers.append, chunks=size)
        for position in range(1, 10):
            window.send("source", b"fixture", rows=1, position=position)
        window.flush()
        assert frontiers == (list(range(1, 10)) if size == 1 else [4, 8, 9])
        assert producer.flushes == (9 if size == 1 else 3)
        results.append((window.rows, window.messages, window.bytes))
    assert results == [(9, 9, 63), (9, 9, 63)]


@pytest.mark.parametrize("mode", ["missing", "error", "pending"])
def test_later_ack_cannot_hide_missing_or_failed_earlier_delivery(mode):
    producer, frontiers = Producer(), []
    window = AckWindow(producer, KafkaConfig(), "owned", frontiers.append, chunks=3)
    for position in (1, 2, 3):
        window.send("source", b"fixture", rows=1, position=position)
    producer.mode = mode
    window.send("source", b"fixture", rows=1, position=4)
    window.send("source", b"fixture", rows=1, position=5)
    with pytest.raises(RuntimeError, match="unconfirmed"):
        window.send("source", b"fixture", rows=1, position=6)
    assert frontiers == [3]
    assert window.rows == 3
    with pytest.raises(RuntimeError, match="cannot be reused"):
        window.flush()


def test_ack_byte_bound_and_partial_final_window():
    producer, frontiers = Producer(), []
    window = AckWindow(producer, KafkaConfig(), "owned", frontiers.append, chunks=32)
    for position in range(1, 10):
        window.send("source", b"x" * 1024**2, rows=1, position=position)
    assert frontiers == [8] and window.pending_bytes == 1024**2
    window.flush()
    assert frontiers == [8, 9]


def test_publication_failure_poisoned_window_preserves_accounting():
    def fail(position):
        raise RuntimeError("owned progress failure")

    window = AckWindow(Producer(), KafkaConfig(), "owned", fail)
    with pytest.raises(RuntimeError, match="progress failure"):
        window.send("source", b"fixture", rows=1, position=1)
    assert window.rows == 0
    with pytest.raises(RuntimeError, match="cannot be reused"):
        window.send("source", b"fixture", rows=1, position=2)


@pytest.mark.parametrize("size", [0, 33, True])
def test_window_bound(size):
    with pytest.raises(ValueError):
        AckWindow(Producer(), KafkaConfig(), "owned", lambda position: None, chunks=size)


def test_progress_hole_is_rejected_without_advancing():
    frontiers = []
    window = AckWindow(Producer(), KafkaConfig(), "owned", frontiers.append, chunks=4)
    window.send("source", b"fixture", rows=2, position=2)
    with pytest.raises(ValueError, match="Non-contiguous"):
        window.send("source", b"fixture", rows=2, position=5)
    assert frontiers == []
    with pytest.raises(RuntimeError, match="cannot be reused"):
        window.flush()
