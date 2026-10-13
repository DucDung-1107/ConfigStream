"""Additive RIS commits, isolated from configuration and RPKI formats; single writer."""

import json
import os
import re
from pathlib import Path
from uuid import uuid4

from lake.client import s3_client
from lake.temporal import dataset_path


def safe_key(key):
    if not re.fullmatch(r"[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*", key) or any(
        part in {".", ".."} for part in key.split("/")
    ):
        raise ValueError("Unsafe RIS metadata key")


class EventStore:
    def __init__(self, *, root=None, config=None):
        if (root is None) == (config is None):
            raise ValueError("Exactly one RIS storage mode required")
        self.root = Path(root).resolve() if root is not None else None
        self.config = config

    def path(self, identity):
        safe_key(identity)
        suffix = f"{dataset_path('ris_occurrence')}/physical-v1/{identity}"
        return str(self.root / suffix) if self.root else self.config.uri(suffix)

    def key(self, key):
        safe_key(key)
        return f"_ris_commits/v1/{key}.json"

    def get(self, key):
        from botocore.exceptions import ClientError

        key = self.key(key)
        if self.root:
            path = self.root / key
            return json.loads(path.read_text()) if path.exists() else None
        try:
            response = s3_client(self.config).get_object(
                Bucket=self.config.bucket, Key=self.config.object_key(key)
            )
        except ClientError as error:
            if error.response["Error"]["Code"] == "NoSuchKey":
                return None
            raise
        with response["Body"] as body:
            encoded = body.read(1024**2 + 1)
        if len(encoded) > 1024**2:
            raise ValueError("RIS commit metadata bound")
        return json.loads(encoded)

    def put(self, key, value):
        from botocore.exceptions import ClientError

        encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        if len(encoded) > 1024**2:
            raise ValueError("RIS commit metadata bound")
        suffix = self.key(key)
        if self.root:
            path = self.root / suffix
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_name(path.name + f".{uuid4()}.tmp")
            try:
                with temporary.open("xb") as target:
                    target.write(encoded)
                    target.flush()
                    os.fsync(target.fileno())
                try:
                    os.link(temporary, path)
                except FileExistsError:
                    if self.get(key) != value:
                        raise ValueError("RIS committed metadata conflict") from None
            finally:
                temporary.unlink(missing_ok=True)
            return
        try:
            s3_client(self.config).put_object(
                Bucket=self.config.bucket,
                Key=self.config.object_key(suffix),
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
                raise ValueError("RIS committed metadata conflict") from None

    def inventory(self, query_id):
        safe_key(query_id)
        prefix = self.key(f"batches/{query_id}").removesuffix(".json") + "/"
        if self.root:
            return [
                json.loads(path.read_text()) for path in sorted((self.root / prefix).glob("*.json"))
            ]
        client = s3_client(self.config)
        result = []
        for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=self.config.bucket, Prefix=self.config.object_key(prefix)
        ):
            for item in page.get("Contents", []):
                if len(result) >= 10000:
                    raise ValueError("RIS commit inventory bound")
                name = item["Key"].removeprefix(self.config.object_key("_ris_commits/v1/"))
                result.append(self.get(name.removesuffix(".json")))
        return result

    def committed_path(self, commit, query_id):
        if (
            type(commit.get("batch_id")) is not int
            or commit["batch_id"] < 0
            or commit.get("format_version") not in {"ris-1.0", "ris-1.1"}
        ):
            raise ValueError("RIS committed metadata schema mismatch")
        expected = self.path(f"{query_id}/{commit['batch_id']:020d}")
        if (
            commit.get("format_version") not in {"ris-1.0", "ris-1.1"}
            or commit.get("query_id") != query_id
            or commit.get("path") != expected
        ):
            raise ValueError("RIS committed path/version mismatch")
        return expected
