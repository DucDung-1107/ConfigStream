import random
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from difflib import SequenceMatcher
from hashlib import sha256
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from schemas import Asset, ChangeEvent, ConfigSnapshot
from workloads.corpus import load_baselines

PROFILES = ("normal-maintenance", "fleet-rollout", "incident-remediation", "stress-replay")


@dataclass(frozen=True)
class WorkloadConfig:
    assets: int = 100
    versions_per_asset: int = 10
    seed: int = 42
    profile: str = "fleet-rollout"
    start_time: datetime = datetime(2026, 1, 1, tzinfo=UTC)
    corpus: Path = Path(".cache/datasets/batfish")
    size_bucket: str = "corpus-distribution"
    duplicate_ratio: float = 0
    out_of_order_ratio: float = 0
    max_reordering_distance: int = 16

    def __post_init__(self):
        if self.assets < 1 or self.versions_per_asset < 1 or self.profile not in PROFILES:
            raise ValueError("Positive asset/version counts and a supported profile are required")
        if self.start_time.tzinfo is None:
            raise ValueError("Starting time must be timezone-aware")
        if not 0 <= self.duplicate_ratio <= 1 or not 0 <= self.out_of_order_ratio <= 1:
            raise ValueError("Replay ratios must be between zero and one")
        if self.max_reordering_distance < 1:
            raise ValueError("Reordering distance must be positive")


def mutate(content: str, syntax: str, version: int) -> tuple[str, str, str]:
    if syntax == "cisco_ios":
        if version % 2 == 0:
            command = "no logging on" if version % 4 == 2 else "logging on"
            updated = re.sub(r"(?m)^(?:no )?logging on\s*\n?", "", content)
            mutation, group = "logging-policy", "operations"
        else:
            command = f"ip name-server 192.0.2.{version % 254 + 1}"
            updated = re.sub(r"(?m)^ip name-server[^\n]*\n?", "", content)
            mutation, group = "dns-change", "operations"
        updated = re.sub(r"(?m)^end\s*$", "", updated).rstrip() + f"\n{command}\nend\n"
    elif syntax == "nginx":
        updated = re.sub(r"(keepalive_timeout\s+)\d+(\s*;)", rf"\g<1>{30 + version}\2", content)
        mutation, group = "timeout-change", "service"
    elif syntax == "generic_service":
        updated = re.sub(r"(?m)^timeout \d+$", f"timeout {30 + version}", content)
        mutation, group = "timeout-change", "service"
    else:
        raise ValueError("Unsupported baseline mutation syntax")
    if updated == content:
        raise ValueError("No applicable non-no-op mutation for this baseline")
    return updated, mutation, group


class History:
    def __init__(self, config: WorkloadConfig):
        self.config = config
        self.random = random.Random(config.seed)
        baselines = load_baselines(config.corpus, config.size_bucket)
        self.states = []
        self.assets = []
        self.identity = f"history-v1:{config.seed}:{config.profile}:{config.start_time.isoformat()}"
        for index in range(config.assets):
            baseline = self.random.choice(baselines)
            asset_id = f"corpus-{uuid5(NAMESPACE_URL, self.identity + ':' + str(index))}"
            content = (config.corpus / "files" / baseline["path"]).read_text()
            self.states.append({"content": content, "baseline": baseline, "rollback": content})
            self.assets.append(
                Asset(
                    asset_id=asset_id,
                    asset_type="network_device"
                    if baseline["syntax"] == "cisco_ios"
                    else "nginx_server",
                    created_at=config.start_time,
                    metadata={
                        "dataset_category": "semi-synthetic",
                        "baseline_sha256": baseline["sha256"],
                    },
                )
            )

    def canonical(self):
        config = self.config
        burst_version = max(2, config.versions_per_asset // 2)
        for version in range(1, config.versions_per_asset + 1):
            rollout = str(uuid5(NAMESPACE_URL, f"{self.identity}:rollout:{version}"))
            for index, asset in enumerate(self.assets):
                state = self.states[index]
                previous = state["content"]
                mutation, group = "real-baseline", "baseline"
                burst = config.profile in {"fleet-rollout", "incident-remediation"} and (
                    version == burst_version
                )
                if version > 1:
                    if config.profile == "incident-remediation" and version == burst_version + 1:
                        state["content"] = state["rollback"]
                        mutation, group = "rollback", "fleet"
                    else:
                        if version == burst_version:
                            state["rollback"] = previous
                        state["content"], mutation, group = mutate(
                            previous, state["baseline"]["syntax"], version
                        )
                spacing = 0 if burst else index * (86400 / config.assets)
                event_time = config.start_time + timedelta(days=version - 1, seconds=spacing)
                digest = sha256(state["content"].encode()).hexdigest()
                identity = f"{self.identity}:{asset.asset_id}:{version}:{digest}"
                snapshot_id = f"snapshot-{uuid5(NAMESPACE_URL, identity)}"
                snapshot = ConfigSnapshot(
                    snapshot_id=snapshot_id,
                    asset_id=asset.asset_id,
                    version=version,
                    event_time=event_time,
                    ingest_time=event_time + timedelta(milliseconds=5),
                    hash=sha256(state["content"].encode()).hexdigest(),
                    content=state["content"],
                )
                truth = {
                    "asset_id": asset.asset_id,
                    "previous_version": version - 1 if version > 1 else None,
                    "new_version": version,
                    "mutation_type": mutation,
                    "mutation_group": group,
                    "rollout_id": rollout if burst else None,
                    "logical_time": event_time.isoformat(),
                    "baseline_path": state["baseline"]["path"],
                    "baseline_sha256": state["baseline"]["sha256"],
                    "dataset_category": "semi-synthetic",
                    "profile": config.profile,
                    "intended_lines_changed": sum(
                        max(old_end - old_start, new_end - new_start)
                        for tag, old_start, old_end, new_start, new_end in SequenceMatcher(
                            None,
                            previous.splitlines(),
                            state["content"].splitlines(),
                            autojunk=False,
                        ).get_opcodes()
                        if tag != "equal"
                    )
                    if version > 1
                    else 0,
                }
                event = ChangeEvent(
                    event_id=f"event-{uuid5(NAMESPACE_URL, snapshot_id)}",
                    asset_id=asset.asset_id,
                    source="semi-synthetic",
                    event_time=event_time,
                    ingest_time=snapshot.ingest_time,
                    metadata={
                        "snapshot_id": snapshot_id,
                        "mutation": mutation,
                        "rollout_id": truth["rollout_id"],
                    },
                )
                yield event, snapshot, truth

    def records(self):
        buffer = []
        for record in self.canonical():
            buffer.append(record)
            if len(buffer) >= self.config.max_reordering_distance:
                if self.random.random() < self.config.out_of_order_ratio:
                    self.random.shuffle(buffer)
                while buffer:
                    selected = buffer.pop(0)
                    yield selected
                    if self.random.random() < self.config.duplicate_ratio:
                        yield selected
        for selected in buffer:
            yield selected
            if self.random.random() < self.config.duplicate_ratio:
                yield selected
