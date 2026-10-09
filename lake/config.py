import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from urllib.parse import urlsplit


@dataclass(frozen=True, slots=True)
class LakeConfig:
    access_key: str = field(repr=False)
    secret_key: str = field(repr=False)
    endpoint: str = "http://localhost:9000"
    bucket: str = "config-lake"
    prefix: str = ""
    region: str = "us-east-1"
    max_records_per_file: int = 100_000

    def __post_init__(self) -> None:
        endpoint = urlsplit(self.endpoint)
        if (
            endpoint.scheme not in {"http", "https"}
            or not endpoint.hostname
            or endpoint.username
            or endpoint.password
            or endpoint.path not in {"", "/"}
            or endpoint.query
            or endpoint.fragment
        ):
            raise ValueError("MINIO_ENDPOINT must be an HTTP(S) endpoint without credentials/path")
        if not self.access_key.strip() or not self.secret_key.strip():
            raise ValueError("Export MINIO_ROOT_USER and MINIO_ROOT_PASSWORD before using the lake")
        if (
            not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", self.bucket)
            or ".." in self.bucket
            or re.fullmatch(r"\d+\.\d+\.\d+\.\d+", self.bucket)
        ):
            raise ValueError("CONFIGSTREAM_LAKE_BUCKET must be a valid S3 bucket name")
        if self.prefix and not re.fullmatch(r"[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+)*", self.prefix):
            raise ValueError("CONFIGSTREAM_LAKE_PREFIX must contain safe relative path segments")
        if not re.fullmatch(r"[a-z0-9-]+", self.region):
            raise ValueError("MINIO_REGION must be a nonempty region identifier")
        if type(self.max_records_per_file) is not int or self.max_records_per_file < 1:
            raise ValueError("CONFIGSTREAM_LAKE_MAX_RECORDS_PER_FILE must be positive")

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "LakeConfig":
        values = os.environ if environ is None else environ
        return cls(
            access_key=values.get("MINIO_ROOT_USER", ""),
            secret_key=values.get("MINIO_ROOT_PASSWORD", ""),
            endpoint=values.get("MINIO_ENDPOINT", "http://localhost:9000"),
            bucket=values.get("CONFIGSTREAM_LAKE_BUCKET", "config-lake"),
            prefix=values.get("CONFIGSTREAM_LAKE_PREFIX", ""),
            region=values.get("MINIO_REGION", "us-east-1"),
            max_records_per_file=int(
                values.get("CONFIGSTREAM_LAKE_MAX_RECORDS_PER_FILE", "100000")
            ),
        )

    def object_key(self, suffix: str) -> str:
        return f"{self.prefix}/{suffix}" if self.prefix else suffix

    def uri(self, suffix: str) -> str:
        return f"s3a://{self.bucket}/{self.object_key(suffix)}"


def lake_enabled(values: Mapping[str, str]) -> bool:
    value = values.get("CONFIGSTREAM_LAKE_ENABLED", "false").lower()
    if value not in {"true", "false"}:
        raise ValueError("CONFIGSTREAM_LAKE_ENABLED must be true or false")
    return value == "true"
