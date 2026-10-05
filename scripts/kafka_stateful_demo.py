"""Development-only, schema-valid version histories; never target production topics."""

import argparse
import logging
from datetime import UTC, datetime, timedelta
from hashlib import sha256

from messaging.config import KafkaConfig
from messaging.kafka_producer import KafkaProducer
from schemas import ConfigSnapshot

LOGGER = logging.getLogger(__name__)


def demo_history(
    asset_id: str = "phase6-router", start: datetime = datetime(2026, 10, 5, tzinfo=UTC)
) -> tuple[ConfigSnapshot, ...]:
    records = []
    for version, logging_state, telnet, password in (
        (1, "enabled", "disabled", "synthetic-before"),
        (2, "disabled", "disabled", "synthetic-after"),
        (3, "disabled", "enabled", "synthetic-after"),
    ):
        content = (
            f"hostname {asset_id}\nlogging {logging_state}\ntelnet {telnet}\n"
            f"password {password}\n# generated at demo-{version}\n"
        )
        timestamp = start + timedelta(seconds=version)
        records.append(
            ConfigSnapshot(
                snapshot_id=f"{asset_id}-v{version}-{start.strftime('%Y%m%dT%H%M%S%fZ')}",
                asset_id=asset_id,
                version=version,
                event_time=timestamp,
                ingest_time=timestamp,
                content=content,
                hash=sha256(content.encode()).hexdigest(),
            )
        )
    return tuple(records)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--order", choices=("normal", "out-of-order"), default="out-of-order")
    parser.add_argument("--asset-id", default="phase6-router")
    parser.add_argument("--start-time", default="2026-10-05T00:00:00Z")
    args = parser.parse_args(argv)
    start = datetime.fromisoformat(args.start_time.replace("Z", "+00:00"))
    if start.tzinfo is None:
        parser.error("--start-time requires a timezone")
    start = start.astimezone(UTC)
    records = demo_history(args.asset_id, start)
    indices = (0, 1, 2) if args.order == "normal" else (0, 2, 1)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    with KafkaProducer(KafkaConfig.from_env()) as producer:
        for index in indices:
            producer.publish(records[index])
    LOGGER.info("Published asset=%s versions=%s", args.asset_id, [index + 1 for index in indices])


if __name__ == "__main__":
    main()
