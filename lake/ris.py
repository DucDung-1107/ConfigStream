"""Additive RIS commits, isolated from configuration and RPKI formats; single writer."""

import json
import os
import re
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from lake.client import s3_client
from lake.temporal import dataset_path
from schemas.ris import ArchiveAccounting, RisReference, SourceExclusion, WindowAccounting


def metadata_digest(value):
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def publish_accounting(store, reference, summary, exclusions):
    for key, expected in (
        ("parser_version", reference.manifest.parser_version),
        ("canonicalization_version", reference.manifest.canonicalization_version),
        ("counting_policy_version", reference.manifest.counting_policy_version),
    ):
        if summary.get(key, expected) != expected:
            raise ValueError("RIS accounting policy mismatch")
    rows = [SourceExclusion.model_validate(row).model_dump(mode="json") for row in exclusions]
    name = reference.manifest.artifacts[0].safe_reference.rsplit("/", 1)[-1]
    match = re.fullmatch(r"updates\.([0-9]{8})\.([0-9]{4})\.gz", name)
    archive_time = {
        "kind": "unknown",
        "role": "observation",
        "reason": "no archive selection time supplied",
    }
    if match:
        start = datetime.strptime("".join(match.groups()), "%Y%m%d%H%M").replace(tzinfo=UTC)
        archive_time = dict(
            kind="interval",
            role="observation",
            start=start,
            end=start + timedelta(minutes=5),
            precision="second",
        )
    descriptor = ArchiveAccounting(
        artifact_sha256=reference.manifest.artifacts[0].sha256,
        provenance_manifest_id=reference.manifest.manifest_id,
        collector=reference.manifest.source_scope,
        archive_selection_time=archive_time,
        framed_records=summary["framed_records"],
        accepted_records=summary["accepted_records"],
        update_messages=summary["update_messages"],
        non_event_records=summary["accepted_records"] - summary["update_messages"],
        excluded_records=summary["excluded_records"],
        accepted_occurrences=summary["accepted_occurrences"],
        framing_sha256=summary["framing_sha256"],
        source_order_projection_sha256=summary["source_order_projection_sha256"],
        exclusion_sha256=metadata_digest(rows),
        oracle_level=summary["oracle_level"],
        completeness="selected_archive_under_pinned_policy",
    )
    descriptor.validate_reference(reference)
    validate_exclusions(descriptor, rows)
    artifact = descriptor.artifact_sha256
    store.put(f"sources/{reference.manifest.manifest_id}", reference.model_dump(mode="json"))
    store.put(f"exclusions/{artifact}", rows)
    store.put(f"accounting/{artifact}", descriptor.model_dump(mode="json"))
    return read_accounting(store, artifact)


def validate_exclusions(descriptor, rows):
    if not isinstance(rows, list) or len(rows) != descriptor.excluded_records:
        raise ValueError("RIS missing/mismatched exclusion ledger")
    validated = [SourceExclusion.model_validate(row) for row in rows]
    if (
        metadata_digest(rows) != descriptor.exclusion_sha256
        or any(
            row.artifact_sha256 != descriptor.artifact_sha256
            or row.all_record_index >= descriptor.framed_records
            for row in validated
        )
        or len({(row.byte_offset, row.all_record_index) for row in validated}) != len(rows)
        or len({row.all_record_index for row in validated}) != len(rows)
    ):
        raise ValueError("RIS exclusion ledger integrity mismatch")


def read_accounting(store, artifact):
    descriptor = ArchiveAccounting.model_validate(store.get(f"accounting/{artifact}"))
    if descriptor.artifact_sha256 != artifact:
        raise ValueError("RIS accounting artifact mismatch")
    reference = RisReference.model_validate(
        store.get(f"sources/{descriptor.provenance_manifest_id}")
    )
    descriptor.validate_reference(reference)
    validate_exclusions(descriptor, store.get(f"exclusions/{artifact}"))
    return descriptor


def publish_window(store, query_id, artifacts, count, projection_sha256):
    if not 1 <= len(artifacts) <= 12 or len(set(artifacts)) != len(artifacts):
        raise ValueError("RIS selected window bound/duplicate artifact")
    sources = [read_accounting(store, artifact) for artifact in artifacts]
    if count != sum(source.accepted_occurrences for source in sources):
        raise ValueError("RIS incomplete selected window")
    inventory = store.inventory(query_id)
    if not inventory and count:
        raise ValueError("RIS window has no committed events")
    descriptor = WindowAccounting(
        format_version="ris-window-1.0",
        query_id=query_id,
        sources={
            source.artifact_sha256: metadata_digest(source.model_dump(mode="json"))
            for source in sources
        },
        inventory_sha256=metadata_digest(sorted(inventory, key=lambda item: item["batch_id"])),
        logical_occurrences=count,
        projection_sha256=projection_sha256,
        completeness="selected_archives_not_routing_state",
    ).model_dump(mode="json")
    store.put(f"windows/{query_id}", descriptor)
    return read_window(store, query_id)


def read_window(store, query_id):
    descriptor = store.get(f"windows/{query_id}")
    WindowAccounting.model_validate(descriptor)
    if descriptor["query_id"] != query_id:
        raise ValueError("RIS complete-window descriptor missing/corrupt")
    count = 0
    for artifact, expected in descriptor["sources"].items():
        source = read_accounting(store, artifact)
        count += source.accepted_occurrences
        if metadata_digest(source.model_dump(mode="json")) != expected:
            raise ValueError("RIS window source accounting conflict")
    if count != descriptor["logical_occurrences"]:
        raise ValueError("RIS window accounting count mismatch")
    inventory = sorted(store.inventory(query_id), key=lambda item: item["batch_id"])
    if metadata_digest(inventory) != descriptor["inventory_sha256"]:
        raise ValueError("RIS window committed inventory missing/conflicting")
    return descriptor


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
        key = self.key(key)
        if self.root:
            path = self.root / key
            if path.exists() and path.stat().st_size > 1024**2:
                raise ValueError("RIS commit metadata bound")
            return json.loads(path.read_text()) if path.exists() else None
        from botocore.exceptions import ClientError

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
        from botocore.exceptions import ClientError

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
                self.get(str(path.relative_to(self.root / "_ris_commits/v1")).removesuffix(".json"))
                for path in sorted((self.root / prefix).glob("*.json"))
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
