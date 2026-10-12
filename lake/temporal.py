"""Opt-in typed publication foundation; existing lake entry points are unchanged."""

import json
from collections.abc import Callable, Mapping
from hashlib import sha256
from types import MappingProxyType
from typing import Literal
from uuid import UUID

from lake.client import s3_client
from lake.config import LakeConfig
from processing.temporal_state import complete_rpki_state, rpki_fixture_transition
from schemas.temporal.base import Contract, Count, Digest, Text, Token
from schemas.temporal.identity import canonical_bytes
from schemas.temporal.provenance import SourceManifest
from schemas.temporal.reconciliation import reconcile
from schemas.temporal.records import Authorization, Evidence, RecordKind

REGISTRY = MappingProxyType(
    {
        "rpki_observation": "temporal/rpki/observations",
        "rpki_transition": "temporal/rpki/transitions",
        "dns_measurement": "temporal/dns/measurements",
        "ris_occurrence": "temporal/ris/occurrences",
    }
)


def dataset_path(kind: str, schema_version: str = "1.0") -> str:
    if schema_version != "1.0" or kind not in REGISTRY:
        raise ValueError("Unknown typed lake registration/version")
    return REGISTRY[kind]


class TypedCommit(Contract):
    format_version: Literal["temporal-1.0"]
    query_id: Token
    batch_id: Count
    record_kind: RecordKind
    payload_schema_version: Literal["1.0"]
    dataset_path: Text
    logical_records: Count
    projection_sha256: Digest
    provenance_ids: tuple[Digest, ...]


def temporal_commit_key(config, query_id, batch_id, kind):
    UUID(query_id)
    if type(batch_id) is not int or batch_id < 0:
        raise ValueError("Invalid typed batch identity")
    dataset_path(kind)
    return config.object_key(f"_temporal_commits/v1/{query_id}/{batch_id:020d}/{kind}.json")


def load_typed_commit(config, query_id, batch_id, kind):
    from botocore.exceptions import ClientError

    try:
        response = s3_client(config).get_object(
            Bucket=config.bucket, Key=temporal_commit_key(config, query_id, batch_id, kind)
        )
    except ClientError as error:
        if error.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise
    with response["Body"] as body:
        return json.loads(body.read())


def save_typed_commit(config, manifest):
    s3_client(config).put_object(
        Bucket=config.bucket,
        Key=temporal_commit_key(
            config, manifest["query_id"], manifest["batch_id"], manifest["record_kind"]
        ),
        Body=canonical_bytes(manifest),
        ContentType="application/json",
        IfNoneMatch="*",
    )


def publish_typed_batch(
    config: LakeConfig,
    query_id: str,
    batch_id: int,
    kind: str,
    records: tuple[Evidence, ...],
    manifests: Mapping[str, SourceManifest],
    write_output: Callable[[str, tuple[Evidence, ...]], None],
    *,
    state_inputs: Mapping[str, tuple[Evidence, tuple[Authorization, ...], SourceManifest]]
    | None = None,
) -> dict:
    """Single writer, bounded fixtures; callback owns physical output, no new Spark job."""
    temporal_commit_key(config, query_id, batch_id, kind)
    rows = reconcile(records, manifests)
    if not rows or any(row.envelope.record_kind != kind for row in rows):
        raise ValueError("Typed batch requires one registered nonempty kind")
    states = {} if state_inputs is None else state_inputs
    for row in rows:
        if kind == "rpki_observation":
            entry = states.get(row.envelope.logical_record_id)
            if entry is None or entry[0] != row:
                raise ValueError("Verified complete state input required")
            complete_rpki_state(*entry)
        elif kind == "rpki_transition":
            coordinate = row.envelope.source_coordinate
            previous, current = (
                states.get(coordinate.previous_id),
                states.get(coordinate.current_id),
            )
            if previous is None or current is None:
                raise ValueError("Verified complete transition input states required")
            if (
                previous[0].envelope.logical_record_id != coordinate.previous_id
                or current[0].envelope.logical_record_id != coordinate.current_id
            ):
                raise ValueError("State proof key does not bind the referenced observation ID")
            difference = rpki_fixture_transition(
                previous[0], current[0], previous[1], current[1], previous[2], current[2]
            )
            input_artifacts = {
                artifact.sha256
                for source in (previous[2], current[2])
                for artifact in source.artifacts
            }
            output_source = manifests[row.envelope.provenance_manifest_id]
            if {artifact.sha256 for artifact in output_source.artifacts} != input_artifacts:
                raise ValueError("Derived provenance artifact inventory mismatch")
            if (
                row.payload.member
                not in difference["added" if row.payload.action == "add" else "removed"]
                or row.payload.comparison_policy != current[0].payload.comparison_policy
                or row.envelope.source_scope != current[0].envelope.source_scope
                or row.envelope.source_id != current[0].envelope.source_id
                or row.envelope.tagged_time_descriptor.start.date()
                != previous[0].payload.observation_date
                or row.envelope.tagged_time_descriptor.end.date()
                != current[0].payload.observation_date
            ):
                raise ValueError("Transition disagrees with complete input states")
    path = config.uri(f"{dataset_path(kind)}/query_id={query_id}/batch_id={batch_id:020d}")
    projection = sha256()
    for row in rows:
        encoded = canonical_bytes(row.model_dump(mode="json"))
        projection.update(len(encoded).to_bytes(4, "big"))
        projection.update(encoded)
    manifest = TypedCommit(
        format_version="temporal-1.0",
        query_id=query_id,
        batch_id=batch_id,
        record_kind=kind,
        payload_schema_version="1.0",
        dataset_path=path,
        logical_records=len(rows),
        projection_sha256=projection.hexdigest(),
        provenance_ids=tuple(sorted({row.envelope.provenance_manifest_id for row in rows})),
    ).model_dump(mode="json")
    committed = load_typed_commit(config, query_id, batch_id, kind)
    if committed is not None:
        if committed != manifest:
            raise ValueError("Typed commit conflicts with replayed logical projection")
        return committed
    write_output(path, rows)
    save_typed_commit(config, manifest)
    return manifest


def committed_typed_paths(
    config: LakeConfig, kind: str, manifests: tuple[dict, ...]
) -> tuple[str, ...]:
    registered = dataset_path(kind)
    paths = []
    for raw in manifests:
        manifest = TypedCommit.model_validate(raw)
        temporal_commit_key(config, manifest.query_id, manifest.batch_id, kind)
        expected = config.uri(
            f"{registered}/query_id={manifest.query_id}/batch_id={manifest.batch_id:020d}"
        )
        if (
            manifest.format_version != "temporal-1.0"
            or manifest.payload_schema_version != "1.0"
            or manifest.record_kind != kind
            or manifest.dataset_path != expected
        ):
            raise ValueError("Invalid committed typed dataset/version/path")
        paths.append(expected)
    return tuple(sorted(set(paths)))


def read_typed_projection(
    config: LakeConfig,
    kind: str,
    commits: tuple[dict, ...],
    read_output: Callable[[str], tuple[Evidence, ...]],
    provenances: Mapping[str, SourceManifest],
) -> tuple[Evidence, ...]:
    """Validated evidence projection; RPKI state reconstruction needs member proof separately."""
    collected = []
    for commit in commits:
        path = committed_typed_paths(config, kind, (commit,))[0]
        rows = reconcile(read_output(path), provenances)
        digest = sha256()
        for row in rows:
            if row.envelope.record_kind != kind:
                raise ValueError("Committed output kind mismatch")
            encoded = canonical_bytes(row.model_dump(mode="json"))
            digest.update(len(encoded).to_bytes(4, "big"))
            digest.update(encoded)
        if (
            len(rows) != commit["logical_records"]
            or digest.hexdigest() != commit["projection_sha256"]
            or sorted({row.envelope.provenance_manifest_id for row in rows})
            != commit["provenance_ids"]
        ):
            raise ValueError("Committed logical output checksum/accounting mismatch")
        collected.extend(rows)
    return reconcile(collected, provenances)
