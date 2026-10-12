from collections.abc import Iterable
from datetime import timedelta
from hashlib import sha256

from schemas.temporal.identity import canonical_bytes
from schemas.temporal.provenance import SourceManifest
from schemas.temporal.records import Authorization, DnsMeasurement, Evidence, RpkiObservation


def authorization_digest(members: Iterable[Authorization]) -> str:
    encodings = sorted(
        {
            canonical_bytes([member.scope, member.asn, member.prefix, member.max_length])
            for member in members
        }
    )
    digest = sha256()
    digest.update(b"rpki-set-v1\n")
    for encoded in encodings:
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def complete_rpki_state(
    observation: Evidence, rows: tuple[Authorization, ...], manifest: SourceManifest
) -> frozenset[Authorization]:
    observation = Evidence.model_validate_json(observation.model_dump_json(warnings=False))
    observation.validate_manifest(manifest)
    descriptor = observation.payload
    if not isinstance(descriptor, RpkiObservation) or descriptor.completion != "complete":
        raise ValueError("State completion descriptor absent or incomplete")
    if len(rows) > 10000:
        raise ValueError("Offline state fixture limit exceeded; not a corpus processor")
    rows = tuple(
        Authorization.model_validate_json(row.model_dump_json(warnings=False)) for row in rows
    )
    if any(member.scope != observation.envelope.source_scope for member in rows):
        raise ValueError("State member scope mismatch")
    members = frozenset(rows)
    if (
        len(rows) != descriptor.accepted_records
        or len(members) != descriptor.unique_members
        or authorization_digest(members) != descriptor.set_digest
    ):
        raise ValueError("State completion checksum/accounting mismatch")
    return members


def rpki_fixture_transition(
    previous: Evidence,
    current: Evidence,
    old_rows: tuple[Authorization, ...],
    new_rows: tuple[Authorization, ...],
    previous_manifest: SourceManifest,
    current_manifest: SourceManifest,
) -> dict[str, frozenset[Authorization]]:
    """Offline completeness oracle boundary; not a production transition processor."""
    old = complete_rpki_state(previous, old_rows, previous_manifest)
    new = complete_rpki_state(current, new_rows, current_manifest)
    old_descriptor, new_descriptor = previous.payload, current.payload
    if (
        (previous.envelope.source_id, previous.envelope.source_scope)
        != (current.envelope.source_id, current.envelope.source_scope)
        or old_descriptor.comparison_policy != new_descriptor.comparison_policy
        or old_descriptor.validation_regime != new_descriptor.validation_regime
        or previous_manifest.canonicalization_version != current_manifest.canonicalization_version
        or previous_manifest.counting_policy_version != current_manifest.counting_policy_version
        or previous_manifest.parser_version != current_manifest.parser_version
        or previous_manifest.dataset_id != current_manifest.dataset_id
        or old_descriptor.observation_date + timedelta(days=1) != new_descriptor.observation_date
    ):
        raise ValueError("States are not comparable consecutive observations")
    return {"added": new - old, "removed": old - new, "unchanged": old & new}


def dns_fixture_difference(
    previous: DnsMeasurement, current: DnsMeasurement, *, partitions_complete: bool
) -> dict[str, frozenset[str]]:
    """Qualified observed-value difference only; no authoritative DNS deletion inference."""
    comparable = ("query_name", "query_type", "response_name", "response_type")
    qualifiers = ("collection_method", "comparison_policy", "worker_context")
    if (
        partitions_complete is not True
        or previous.status not in {"nonempty", "proven_empty"}
        or current.status not in {"nonempty", "proven_empty"}
        or any(
            getattr(previous, field) != getattr(current, field) for field in comparable + qualifiers
        )
        or previous.partition_date + timedelta(days=1) != current.partition_date
    ):
        raise ValueError("Measurements cannot establish a comparable observed difference")
    old, new = frozenset(previous.values), frozenset(current.values)
    return {"added_values": new - old, "removed_values": old - new}
