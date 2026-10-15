"""Bounded source-identity binding before asset-grouped normalization."""

import json
from datetime import UTC

import pandas as pd
from pydantic import ValidationError
from pyspark.sql.streaming import StatefulProcessor

from schemas import ConfigSnapshot
from spark.common.config import duration_ms
from spark.common.schemas import SNAPSHOT_COLUMNS


def checkpoint_contract(config, partitions):
    directory = config.checkpoint_dir
    marker = directory / "configstream-state-contract.json"
    contract = {
        "version": "source-identity-v2",
        "shuffle_partitions": partitions,
        "watermark_delay": config.watermark_delay,
        "state_timeout": config.state_timeout,
        "max_versions_per_asset": config.max_versions_per_asset,
    }
    if marker.exists():
        if json.loads(marker.read_text()) != contract:
            raise RuntimeError("Incompatible state checkpoint contract; use an isolated checkpoint")
        return
    if any((directory / name).exists() for name in ("metadata", "offsets", "state")):
        raise RuntimeError("Legacy checkpoint requires an explicit isolated v2 rollout")
    directory.mkdir(parents=True, exist_ok=True)
    try:
        with marker.open("x") as output:
            json.dump(contract, output, sort_keys=True)
    except FileExistsError:
        if json.loads(marker.read_text()) != contract:
            raise RuntimeError("Conflicting state checkpoint contract") from None


class IdentityProcessor(StatefulProcessor):
    def __init__(self, config):
        self.timeout_ms = duration_ms(config.state_timeout)

    def init(self, handle):
        self.handle = handle
        self.binding = handle.getValueState("source_identity_v2", "projection STRING")

    def handleInputRows(self, key, rows, timerValues):
        for frame in rows:
            accepted = []
            for row in frame.to_dict("records"):
                values = {name: row[name] for name in SNAPSHOT_COLUMNS}
                for name in ("event_time", "ingest_time"):
                    values[name] = pd.Timestamp(values[name]).to_pydatetime().replace(tzinfo=UTC)
                values["version"] = int(values["version"])
                error = None
                try:
                    snapshot = ConfigSnapshot.model_validate(values)
                    projection = snapshot.model_dump_json(exclude={"ingest_time", "content"})
                    if self.binding.exists():
                        if self.binding.get()[0] == projection:
                            continue
                        error = "snapshot_identity_conflict"
                    else:
                        self.binding.update((projection,))
                        self.handle.registerTimer(
                            int(pd.Timestamp(values["event_time"]).value // 1_000_000)
                            + self.timeout_ms
                        )
                except ValidationError:
                    error = "processing_rejected"
                accepted.append({**row, "identity_error": error})
            if accepted:
                yield pd.DataFrame(accepted)

    def handleExpiredTimer(self, key, timerValues, expiredTimerInfo):
        self.binding.clear()
        return iter(())
