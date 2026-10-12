from collections.abc import Iterable, Mapping

from schemas.temporal.identity import canonical_bytes
from schemas.temporal.provenance import SourceManifest
from schemas.temporal.records import Evidence


def reconcile(
    records: Iterable[Evidence], manifests: Mapping[str, SourceManifest], *, limit: int = 10000
) -> tuple[Evidence, ...]:
    """Bounded logical projection, not a Kafka/sink transaction or persistent dedup store."""
    if type(limit) is not int or not 1 <= limit <= 10000:
        raise ValueError("Reconciliation limit must be 1 through 10000")
    accepted = {}
    payloads = {}
    for count, record in enumerate(records, 1):
        if count > limit:
            raise ValueError("Reconciliation input limit exceeded")
        record = Evidence.model_validate_json(record.model_dump_json(warnings=False))
        manifest = manifests.get(record.envelope.provenance_manifest_id)
        if manifest is None:
            raise ValueError("Missing provenance manifest")
        record.validate_manifest(manifest)
        identity = record.envelope.logical_record_id
        encoded = canonical_bytes(record.model_dump(mode="json"))
        if identity in accepted and payloads[identity] != encoded:
            raise ValueError("Logical identity/payload conflict")
        accepted[identity] = record
        payloads[identity] = encoded
    return tuple(accepted[key] for key in sorted(accepted))
