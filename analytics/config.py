import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

JOB_VERSION = "batch-v1"


@dataclass(frozen=True)
class AnalyticsConfig:
    window: str = "1 day"
    start: datetime | None = None
    end: datetime | None = None
    baseline_windows: int = 7
    minimum_prior_windows: int = 3
    minimum_count: int = 5
    z_threshold: float = 3.0

    def __post_init__(self):
        if self.window not in {"1 hour", "1 day"}:
            raise ValueError("Analytics window must be 1 hour or 1 day")
        if any(value is not None and value.tzinfo is None for value in (self.start, self.end)):
            raise ValueError("Analytics timestamps must be timezone-aware")
        if self.start and self.end and self.start >= self.end:
            raise ValueError("Analytics start must precede end")
        if not 2 <= self.minimum_prior_windows <= self.baseline_windows <= 366:
            raise ValueError("Require 2 <= minimum prior windows <= baseline windows <= 366")
        if self.minimum_count < 1 or not 0 < self.z_threshold < 100:
            raise ValueError("Anomaly count and finite threshold must be positive")
        object.__setattr__(self, "z_threshold", float(self.z_threshold))

    def parameters(self) -> dict:
        return {
            key: value.astimezone(UTC).isoformat() if isinstance(value, datetime) else value
            for key, value in asdict(self).items()
        }


def run_identity(manifests: list[dict], config: AnalyticsConfig) -> str:
    inputs = sorted(manifests, key=lambda entry: (entry["query_id"], entry["batch_id"]))
    canonical = json.dumps(
        {"job_version": JOB_VERSION, "inputs": inputs, "parameters": config.parameters()},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode()).hexdigest()
