"""Bind independently measured oracle summaries to unchanged Phase 16 descriptors."""

from datetime import date

from schemas.rpki import CANONICALIZATION, COMPARISON, COUNTING, PARSER, REGIME, DayReference
from schemas.temporal.coordinates import StateCoordinate
from schemas.temporal.provenance import Artifact, SourceManifest
from schemas.temporal.records import Envelope, Evidence, RpkiObservation, evidence_identity
from schemas.temporal.time import ObservationDate


def day_reference(summary: dict, artifact: dict, selection_id: str) -> DayReference:
    day = date.fromisoformat(summary["date"])
    time = ObservationDate(value=day)
    manifest = SourceManifest(
        manifest_version="1.0",
        source_id="ripe-rpki-archive",
        dataset_id="rpki-reference-202609",
        source_scope=summary["scope"],
        artifacts=(
            Artifact(
                sha256=artifact["sha256"],
                safe_reference=artifact["url"],
                revision=day.isoformat(),
                bytes=artifact["artifact_bytes"],
            ),
        ),
        parser_version=PARSER,
        canonicalization_version=CANONICALIZATION,
        counting_policy_version=COUNTING,
        selection_manifest_id=selection_id,
        source_time_coverage=time,
        declared_observation_coverage=f"complete published CSV {day}",
        license_terms_reference="https://www.ripe.net/about-us/legal/copyright-statement/",
        expected_records=summary["source_rows"],
        accepted_records=summary["accepted_rows"],
        excluded_records=summary["excluded_rows"],
        integrity_verified=True,
        accounting_complete=True,
    )
    payload = RpkiObservation(
        observation_date=day,
        comparison_policy=COMPARISON,
        validation_regime=REGIME,
        completion="complete",
        expected_records=summary["source_rows"],
        accepted_records=summary["accepted_rows"],
        duplicate_records=summary["duplicate_rows"],
        unique_members=summary["unique_members"],
        excluded_records=summary["excluded_rows"],
        set_digest=summary["set_digest"],
    )
    envelope = Envelope(
        envelope_version="1.0",
        record_kind="rpki_observation",
        payload_schema_version="1.0",
        source_id=manifest.source_id,
        dataset_id=manifest.dataset_id,
        source_scope=manifest.source_scope,
        logical_record_id="0" * 64,
        provenance_manifest_id=manifest.manifest_id,
        source_coordinate=StateCoordinate(
            artifact_sha256=artifact["sha256"], observation_date=day.isoformat()
        ),
        tagged_time_descriptor=time,
        quality_status="accepted",
    )
    initial = Evidence(envelope=envelope, payload=payload)
    return DayReference(
        manifest=manifest,
        observation=Evidence(
            envelope=envelope.model_copy(
                update={"logical_record_id": evidence_identity(initial, manifest)}
            ),
            payload=payload,
        ),
    )
