"""Pinned source iteration and observation projection; no authoritative DNS inference."""

import json
from hashlib import sha256
from ipaddress import ip_address

from schemas.dns import DnsRow
from schemas.temporal.provenance import verify_artifact_file

COLUMNS = (
    "query_name",
    "query_type",
    "response_name",
    "response_type",
    "timestamp",
    "worker_id",
    "status_code",
    "ip4_address",
    "ip6_address",
    "response_ttl",
    "rtt",
    "section",
    "extended_error",
)


def normalize_row(raw, reference, row_group, row):
    outcome = (
        "success"
        if raw["status_code"] == 0
        else "timeout"
        if raw["status_code"] == 65535
        else "unknown"
        if raw["status_code"] is None or not 1 <= raw["status_code"] <= 15
        else "failure"
    )
    address = None
    disposition = "unsuccessful" if outcome != "success" else "non_target"
    if outcome == "success" and raw["response_type"] in {"A", "AAAA"}:
        value = raw["ip4_address"] if raw["response_type"] == "A" else raw["ip6_address"]
        disposition = "unavailable"
        if value and raw["query_name"] and raw["response_name"] and raw["query_type"]:
            try:
                parsed = ip_address(value)
                if parsed.version != (4 if raw["response_type"] == "A" else 6):
                    raise ValueError("address family")
                address, disposition = str(parsed), "selected_address"
            except ValueError:
                disposition = "invalid_address"
    source = reference.manifest
    values = {field: raw[field] for field in COLUMNS if field not in {"timestamp", "rtt"}}
    return DnsRow(
        logical_record_id=reference.row_id(row_group, row),
        artifact_sha256=source.artifacts[0].sha256,
        provenance_manifest_id=source.manifest_id,
        row_group=row_group,
        row=row,
        partition_date=reference.partition_date.isoformat(),
        timestamp_ms=raw["timestamp"],
        rtt_hex=float(raw["rtt"]).hex() if raw["rtt"] is not None else None,
        source_row_sha256=sha256(
            json.dumps(raw, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        ).hexdigest(),
        outcome=outcome,
        disposition=disposition,
        address_value=address,
        **values,
    )


def read_source(path, reference, *, batch_size=8192, limit=None):
    import pyarrow as arrow
    import pyarrow.parquet as parquet

    if arrow.__version__ != "19.0.1" or not 1 <= batch_size <= 32768:
        raise ValueError("DNS pinned parser/batch policy mismatch")
    verify_artifact_file(path, reference.manifest.artifacts[0])
    source = parquet.ParquetFile(path)
    if tuple(
        source.metadata.row_group(index).num_rows for index in range(source.num_row_groups)
    ) != reference.row_groups or not set(COLUMNS) <= set(source.schema_arrow.names):
        raise ValueError("DNS Parquet source schema/coordinate mismatch")
    count = 0
    for group in range(source.num_row_groups):
        ordinal = 0
        for batch in source.iter_batches(row_groups=[group], batch_size=batch_size):
            for raw in batch.to_pylist():
                if limit is not None and count >= limit:
                    return
                try:
                    result = normalize_row(raw, reference, group, ordinal)
                except (ValueError, TypeError):
                    raise ValueError(
                        f"DNS source projection invalid at row group {group}, row {ordinal}"
                    ) from None
                yield result
                ordinal += 1
                count += 1
        if ordinal != reference.row_groups[group]:
            raise ValueError("DNS source coordinate gap")


def qualify_pair(previous, current, *, sources_complete, vantage_verified=False):
    from processing.temporal_state import dns_fixture_difference

    if not sources_complete:
        return "incomplete_source", None
    if previous is None or current is None:
        return "not_observed", None
    if previous.status not in {"nonempty", "proven_empty"} or current.status not in {
        "nonempty",
        "proven_empty",
    }:
        return "unsuccessful_or_unknown", None
    if previous.worker_context is None or current.worker_context is None:
        return "unknown_worker", None
    if previous.worker_context != current.worker_context:
        return "worker_changed", None
    if not vantage_verified:
        return "vantage_unverified", None
    return "qualified_observed_difference", dns_fixture_difference(
        previous, current, partitions_complete=True
    )
