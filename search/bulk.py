import json
import logging
import time
from collections.abc import Iterable

from search.config import SearchConfig

LOGGER = logging.getLogger(__name__)
RETRY_STATUSES = {429, 500, 502, 503, 504}
MAX_RETRIES = 3
MAX_BULK_BYTES = 5 * 1024 * 1024


def write_actions(actions: Iterable[dict], config: SearchConfig, client) -> dict[str, int]:
    metrics = {"attempted": 0, "indexed": 0, "requests": 0, "retries": 0, "failures": 0}
    started = time.monotonic()

    def send(chunk: list[dict]) -> None:
        pending = chunk
        for attempt in range(MAX_RETRIES + 1):
            if attempt:
                metrics["retries"] += 1
                time.sleep(2 ** (attempt - 1))
            operations = []
            for action in pending:
                operations.extend(
                    [
                        {"index": {"_index": action["_index"], "_id": action["_id"]}},
                        action["_source"],
                    ]
                )
            metrics["requests"] += 1
            try:
                response = client.bulk(operations=operations, require_alias=True)
            except Exception as error:
                status = getattr(error, "status_code", None)
                transient = status in RETRY_STATUSES or type(error).__name__ in {
                    "ConnectionError",
                    "ConnectionTimeout",
                }
                if transient and attempt < MAX_RETRIES:
                    continue
                raise RuntimeError(
                    f"Search bulk request failed (status={status}); retry from checkpoint"
                ) from None
            items = response.get("items", [])
            if len(items) != len(pending):
                raise RuntimeError("Search bulk response has an unexpected item count")
            retry = []
            for action, item in zip(pending, items, strict=True):
                status = item.get("index", {}).get("status")
                if isinstance(status, int) and 200 <= status < 300:
                    metrics["indexed"] += 1
                elif status in RETRY_STATUSES and attempt < MAX_RETRIES:
                    retry.append(action)
                else:
                    raise RuntimeError(
                        f"Search bulk item failed (status={status}, index={action['_index']}); "
                        "verify mapping/connectivity"
                    )
            pending = retry
            if not pending:
                return
        raise RuntimeError("Search bulk retries exhausted")

    chunk = []
    chunk_bytes = 0
    try:
        for action in actions:
            size = len(json.dumps(action, ensure_ascii=False).encode("utf-8")) + 32
            if size > MAX_BULK_BYTES:
                raise ValueError("Search document exceeds the bulk byte limit")
            if chunk and (len(chunk) >= config.bulk_size or chunk_bytes + size > MAX_BULK_BYTES):
                send(chunk)
                chunk, chunk_bytes = [], 0
            chunk.append(action)
            chunk_bytes += size
            metrics["attempted"] += 1
        if chunk:
            send(chunk)
        return metrics
    except Exception:
        metrics["failures"] += 1
        raise
    finally:
        LOGGER.warning(
            "Search partition metrics=%s elapsed_seconds=%.3f", metrics, time.monotonic() - started
        )
