import os
from collections.abc import Mapping
from dataclasses import dataclass, field

from search.config import SearchConfig, boolean


@dataclass(frozen=True, slots=True)
class ApiConfig:
    host: str = "127.0.0.1"
    port: int = 8000
    log_level: str = "info"
    docs: bool = True
    search: SearchConfig = field(default_factory=SearchConfig)

    def __post_init__(self) -> None:
        if not self.host.strip() or not 1 <= self.port <= 65535:
            raise ValueError("API host and port must be valid")
        if self.log_level not in {"critical", "error", "warning", "info", "debug", "trace"}:
            raise ValueError("Unsupported API log level")

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "ApiConfig":
        values = os.environ if environ is None else environ
        return cls(
            host=values.get("CONFIGSTREAM_API_HOST", "127.0.0.1"),
            port=int(values.get("CONFIGSTREAM_API_PORT", "8000")),
            log_level=values.get("CONFIGSTREAM_API_LOG_LEVEL", "info").lower(),
            docs=boolean({"CONFIGSTREAM_API_DOCS": "true", **values}, "CONFIGSTREAM_API_DOCS"),
            search=SearchConfig.from_env(values),
        )
