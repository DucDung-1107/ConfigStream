"""Additive RPKI physical registry and conditional metadata store, single writer."""

import json
import os
import re
from pathlib import Path
from types import MappingProxyType
from uuid import uuid4

from lake.client import s3_client
from lake.temporal import dataset_path

PHYSICAL_REGISTRY = MappingProxyType(
    {
        "source_rows": "temporal/rpki/source_rows/v1",
        "members": "temporal/rpki/members/v1",
        "observations": dataset_path("rpki_observation"),
        "transitions": dataset_path("rpki_transition"),
        "boundaries": "temporal/rpki/boundaries/v1",
    }
)


def safe_key(key: str):
    if not re.fullmatch(r"[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*", key) or any(
        part in {".", ".."} for part in key.split("/")
    ):
        raise ValueError("Unsafe RPKI metadata key")


class LocalStore:
    """Local fault-test/batch store; not evidence of MinIO conditional behavior."""

    def __init__(self, root: Path):
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def path(self, dataset, identity):
        safe_key(identity)
        return str(self.root / PHYSICAL_REGISTRY[dataset] / identity)

    def get(self, key):
        safe_key(key)
        path = self.root / "_rpki_commits/v1" / f"{key}.json"
        return json.loads(path.read_text()) if path.exists() else None

    def put(self, key, value):
        safe_key(key)
        path = self.root / "_rpki_commits/v1" / f"{key}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f"{path.name}.{uuid4()}.tmp")
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
        try:
            with temporary.open("x") as target:
                target.write(encoded)
                target.flush()
                os.fsync(target.fileno())
            try:
                os.link(temporary, path)
            except FileExistsError:
                if self.get(key) != value:
                    raise ValueError("RPKI committed metadata conflict") from None
        finally:
            temporary.unlink(missing_ok=True)

    def list(self, prefix):
        safe_key(prefix)
        root = self.root / "_rpki_commits/v1" / prefix
        return (
            [json.loads(path.read_text()) for path in sorted(root.rglob("*.json"))]
            if root.exists()
            else []
        )


class MinioStore:
    def __init__(self, config):
        self.config = config

    def path(self, dataset, identity):
        safe_key(identity)
        return self.config.uri(f"{PHYSICAL_REGISTRY[dataset]}/{identity}")

    def key(self, key):
        safe_key(key)
        return self.config.object_key(f"_rpki_commits/v1/{key}.json")

    def get(self, key):
        from botocore.exceptions import ClientError

        try:
            response = s3_client(self.config).get_object(
                Bucket=self.config.bucket, Key=self.key(key)
            )
        except ClientError as error:
            if error.response["Error"]["Code"] == "NoSuchKey":
                return None
            raise
        with response["Body"] as body:
            return json.loads(body.read(1024 * 1024 + 1))

    def put(self, key, value):
        from botocore.exceptions import ClientError

        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        if len(encoded) > 1024 * 1024:
            raise ValueError("RPKI commit metadata limit exceeded")
        try:
            s3_client(self.config).put_object(
                Bucket=self.config.bucket,
                Key=self.key(key),
                Body=encoded,
                ContentType="application/json",
                IfNoneMatch="*",
            )
        except ClientError as error:
            if error.response["Error"]["Code"] not in {
                "PreconditionFailed",
                "412",
                "ConditionalRequestConflict",
                "409",
            }:
                raise
            if self.get(key) != value:
                raise ValueError("RPKI committed metadata conflict") from None

    def list(self, prefix):
        safe_key(prefix)
        client = s3_client(self.config)
        result = []
        for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=self.config.bucket, Prefix=self.config.object_key(f"_rpki_commits/v1/{prefix}/")
        ):
            for item in page.get("Contents", []):
                if len(result) >= 10000:
                    raise ValueError("RPKI metadata inventory budget exceeded")
                if item["Key"].endswith(".json"):
                    response = client.get_object(Bucket=self.config.bucket, Key=item["Key"])
                    with response["Body"] as body:
                        result.append(json.loads(body.read(1024 * 1024 + 1)))
        return result


def validated_paths(store, commit, dataset):
    if (
        commit.get("format_version") != "rpki-1.0"
        or commit.get("dataset") != dataset
        or commit.get("path") != store.path(dataset, commit["identity"])
    ):
        raise ValueError("RPKI committed path/schema mismatch")
    return commit["path"]
