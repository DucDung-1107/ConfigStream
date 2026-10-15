"""Bounded all-ACK publication windows; failures retain the previous progress frontier."""

from time import monotonic


class AckWindow:
    def __init__(self, client, config, topic, commit, *, chunks=1, check=lambda: None):
        if type(chunks) is not int or not 1 <= chunks <= 32:
            raise ValueError("ACK window must contain 1..32 chunks")
        self.client, self.config, self.topic = client, config, topic
        self.commit, self.check, self.chunks = commit, check, chunks
        self.pending = []
        self.pending_bytes = 0
        self.rows = self.messages = self.bytes = self.flushes = 0
        self.failed = False
        self.position = None

    def send(self, key, value, *, rows, position):
        if self.failed:
            raise RuntimeError("Failed ACK window cannot be reused")
        if len(value) > 1024**2:
            raise ValueError("Kafka source chunk byte budget exceeded")
        if self.pending and self.pending_bytes + len(value) > 8 * 1024**2:
            self.flush()
        receipt = {"ack": None, "rows": rows, "bytes": len(value), "position": position}

        def delivered(error, message):
            receipt["ack"] = error is None

        deadline = monotonic() + self.config.enqueue_timeout_seconds
        try:
            self.check()
            if type(rows) is not int or rows < 1 or type(position) is not int or position < rows:
                raise ValueError("Invalid source progress coordinate")
            if self.position is not None and position != self.position + rows:
                raise ValueError("Non-contiguous source progress coordinate")
            while True:
                try:
                    self.client.produce(self.topic, key=key, value=value, on_delivery=delivered)
                    break
                except BufferError:
                    if monotonic() >= deadline:
                        raise RuntimeError("Kafka producer queue timeout") from None
                    self.client.poll(0.1)
        except Exception:
            self.failed = True
            raise
        self.pending.append(receipt)
        self.position = position
        self.pending_bytes += len(value)
        if len(self.pending) >= self.chunks:
            self.flush()

    def flush(self):
        if self.failed:
            raise RuntimeError("Failed ACK window cannot be reused")
        if not self.pending:
            return
        try:
            self.flushes += 1
            if self.client.flush(self.config.flush_timeout_seconds) or any(
                item["ack"] is not True for item in self.pending
            ):
                raise RuntimeError("Kafka delivery unconfirmed; progress not advanced")
            self.check()
            self.commit(self.pending[-1]["position"])
        except Exception:
            self.failed = True
            raise
        self.rows += sum(item["rows"] for item in self.pending)
        self.messages += len(self.pending)
        self.bytes += self.pending_bytes
        self.pending.clear()
        self.pending_bytes = 0
