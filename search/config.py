import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from urllib.parse import urlsplit

SEARCH_SCHEMA_VERSION = "v1"
SERVER_VERSION = "9.5.5"
CLIENT_VERSION = "9.5.1"
KINDS = ("snapshots", "diffs", "alerts", "diagnostics")


def boolean(values: Mapping[str, str], name: str) -> bool:
    value = values.get(name, "false").lower()
    if value not in {"true", "false"}:
        raise ValueError(f"{name} must be true or false")
    return value == "true"


@dataclass(frozen=True, slots=True)
class SearchConfig:
    url: str = "http://localhost:9200"
    index_prefix: str = "configstream"
    bulk_size: int = 1000
    timeout_seconds: int = 30
    username: str = field(default="", repr=False)
    password: str = field(default="", repr=False)

    def __post_init__(self) -> None:
        endpoint = urlsplit(self.url)
        if (
            endpoint.scheme not in {"http", "https"}
            or not endpoint.hostname
            or endpoint.username
            or endpoint.password
            or endpoint.path not in {"", "/"}
            or endpoint.query
            or endpoint.fragment
        ):
            raise ValueError("ELASTICSEARCH_URL must be an HTTP(S) endpoint without credentials")
        if not re.fullmatch(r"[a-z][a-z0-9_-]{0,79}", self.index_prefix):
            raise ValueError("Search index prefix must be a safe lowercase identifier")
        if bool(self.username) != bool(self.password):
            raise ValueError("Elasticsearch username and password must be provided together")
        if type(self.bulk_size) is not int or not 1 <= self.bulk_size <= 10_000:
            raise ValueError("Search bulk size must be between 1 and 10000")
        if type(self.timeout_seconds) is not int or self.timeout_seconds < 1:
            raise ValueError("Search timeout must be positive")

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "SearchConfig":
        values = os.environ if environ is None else environ
        if boolean(values, "CONFIGSTREAM_SEARCH_DIFF_DETAILS"):
            raise ValueError("Diff detail indexing is not supported by the safe v1 projection")
        return cls(
            url=values.get("ELASTICSEARCH_URL", "http://localhost:9200"),
            index_prefix=values.get("CONFIGSTREAM_SEARCH_INDEX_PREFIX", "configstream"),
            bulk_size=int(values.get("CONFIGSTREAM_SEARCH_BULK_SIZE", "1000")),
            timeout_seconds=int(values.get("CONFIGSTREAM_SEARCH_TIMEOUT_SECONDS", "30")),
            username=values.get("ELASTICSEARCH_USERNAME", ""),
            password=values.get("ELASTICSEARCH_PASSWORD", ""),
        )

    def alias(self, kind: str) -> str:
        if kind not in KINDS:
            raise ValueError("Unknown search document kind")
        return f"{self.index_prefix}-{kind}"

    def index(self, kind: str) -> str:
        return f"{self.alias(kind)}-{SEARCH_SCHEMA_VERSION}"
