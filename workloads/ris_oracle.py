"""Cached-only RIS layered oracle; private columnar ledgers, compact public summaries."""

import argparse
import gzip
import json
import sqlite3
from collections import Counter
from hashlib import sha256
from itertools import zip_longest
from pathlib import Path
from time import monotonic

from processing.mrt_framing import frame_mrt
from processing.ris import decode_frame, read_archive
from schemas.ris import CANONICAL, COUNTING, PARSER, RisReference
from schemas.temporal.provenance import Artifact, SourceManifest, verify_artifact_file
from workloads.ris_reference import extract, frames, identity


def parser_module():
    import mrtparse

    if mrtparse.__version__ != "2.2.0":
        raise ValueError("Pinned mrtparse required")
    return mrtparse


def write_summary(path, value):
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")


def build(path, artifact, output, *, source_id="ripe-ris", collector="rrc00"):
    import pyarrow as arrow
    import pyarrow.parquet as parquet

    if output.exists():
        raise ValueError("Oracle requires fresh private directory")
    output.mkdir(parents=True)
    started = monotonic()
    verify_artifact_file(path, artifact)
    parser = parser_module()
    categories, types = Counter(), Counter()
    records = excluded = decompressed = updates = 0
    exclusions = []
    with gzip.open(path, "rb") as stream:
        for frame in frame_mrt(stream):
            record, error = decode_frame(frame, parser)
            records += 1
            decompressed = frame.next_offset
            types[f"{frame.record_type}/{frame.subtype}"] += 1
            if error:
                excluded += 1
                categories[error] += 1
                exclusions.append(
                    {
                        "artifact_sha256": artifact.sha256,
                        "byte_offset": frame.byte_offset,
                        "all_record_index": frame.all_record_index,
                        "type": frame.record_type,
                        "subtype": frame.subtype,
                        "category": error,
                        "fully_framed": True,
                        "disposition": "excluded_under_pinned_parser_policy",
                    }
                )
            elif 2 in record.get("bgp_message", {}).get("type", {}):
                updates += 1
    reference = RisReference(
        manifest=SourceManifest(
            manifest_version="1.0",
            source_id=source_id,
            dataset_id="ris-20260901-hour",
            source_scope=collector,
            artifacts=(artifact,),
            parser_version=PARSER,
            canonicalization_version=CANONICAL,
            counting_policy_version=COUNTING,
            selection_manifest_id=sha256(b"rrc00-20260901-0000-0100-pinned-phase13").hexdigest(),
            source_time_coverage={
                "kind": "unknown",
                "role": "source",
                "reason": "actual source bounds in separate coordinate ledger",
            },
            declared_observation_coverage="complete pinned gzip archive; not complete route state",
            license_terms_reference="https://www.ripe.net/about-us/legal/copyright-statement/",
            expected_records=records,
            accepted_records=records - excluded,
            excluded_records=excluded,
            integrity_verified=True,
            accounting_complete=True,
        )
    )
    write_summary(output / "reference.json", reference.model_dump(mode="json"))
    write_summary(output / "exclusions.json", exclusions)
    counts, minutes = Counter(), Counter()
    frame_digest, occurrence_digest = sha256(), sha256()
    database = sqlite3.connect(output / "fingerprints.sqlite")
    database.execute("CREATE TABLE fingerprints (fingerprint TEXT PRIMARY KEY)")
    database.execute("CREATE TABLE peers (peer TEXT PRIMARY KEY)")
    database.execute("CREATE TABLE prefixes (prefix TEXT PRIMARY KEY)")
    repeated = total = 0
    rows = []
    writer = None
    oracle_excluded_decodable = 0
    try:
        for actual, expected in zip_longest(read_archive(path, reference, parser), frames(path)):
            if actual is None or expected is None:
                raise ValueError("Independent frame inventory disagreement")
            frame, error, events = actual
            offset, index, stamp, kind, subtype, body = expected
            if (
                frame.byte_offset,
                frame.all_record_index,
                frame.timestamp_seconds,
                frame.record_type,
                frame.subtype,
                frame.body,
            ) != expected:
                raise ValueError("Independent frame coordinate disagreement")
            frame_digest.update(f"{offset},{index},{stamp},{kind},{subtype},{len(body)}\n".encode())
            try:
                decoded = extract(kind, subtype, body)
                oracle_error = None
            except (ValueError, KeyError) as failure:
                decoded, oracle_error = None, type(failure).__name__
            if error:
                oracle_excluded_decodable += decoded is not None
                continue
            if events and (decoded is None or oracle_error):
                raise ValueError("Independent supported-subset extractor cannot validate events")
            if decoded is not None:
                peer_asn, peer_ip, micros, prefixes = decoded
                actual_values = [
                    (event.action, event.afi, event.prefix, event.path_id) for event in events
                ]
                if prefixes != actual_values:
                    raise ValueError("Independent expanded semantic projection disagreement")
                if events and any(
                    (event.peer_asn, event.peer_ip, event.source_microseconds)
                    != (peer_asn, peer_ip, micros)
                    for event in events
                ):
                    raise ValueError("Independent peer/time disagreement")
            for event in events:
                if event.logical_record_id != identity(reference, offset, event.expansion_ordinal):
                    raise ValueError("Independent occurrence identity disagreement")
                if event.source_seconds != stamp:
                    raise ValueError("Independent timestamp disagreement")
                total += 1
                counts[event.action] += 1
                minutes[stamp // 60 * 60] += 1
                encoded = json.dumps(
                    event.model_dump(), sort_keys=True, separators=(",", ":")
                ).encode()
                occurrence_digest.update(encoded + b"\n")
                repeated += not database.execute(
                    "INSERT OR IGNORE INTO fingerprints VALUES (?)", (event.content_fingerprint,)
                ).rowcount
                database.execute(
                    "INSERT OR IGNORE INTO peers VALUES (?)", (f"{event.peer_asn}:{event.peer_ip}",)
                )
                database.execute("INSERT OR IGNORE INTO prefixes VALUES (?)", (event.prefix,))
                rows.append(event.model_dump())
                if len(rows) == 4096:
                    table = arrow.Table.from_pylist(rows, schema=event_arrow_schema())
                    if writer is None:
                        writer = parquet.ParquetWriter(output / "events.parquet", table.schema)
                    writer.write_table(table)
                    rows.clear()
            if index % 10000 == 0:
                database.commit()
        if rows:
            table = arrow.Table.from_pylist(rows, schema=event_arrow_schema())
            if writer is None:
                writer = parquet.ParquetWriter(output / "events.parquet", table.schema)
            writer.write_table(table)
        database.commit()
        summary = dict(
            artifact=artifact.model_dump(),
            framed_records=records,
            accepted_records=records - excluded,
            update_messages=updates,
            excluded_records=excluded,
            exclusions_by_category=categories,
            record_types=types,
            decompressed_bytes=decompressed,
            accepted_occurrences=total,
            actions=counts,
            events_per_minute=sorted(minutes.items()),
            source_rate_fixed_five_minutes=total / 300,
            peak_events_per_minute=max(minutes.values()),
            unique_peers=database.execute("SELECT COUNT(*) FROM peers").fetchone()[0],
            unique_prefixes=database.execute("SELECT COUNT(*) FROM prefixes").fetchone()[0],
            repeated_fingerprints_beyond_first=repeated,
            framing_sha256=frame_digest.hexdigest(),
            source_order_projection_sha256=occurrence_digest.hexdigest(),
            independent_identity_semantics_match=True,
            excluded_records_decodable_by_minimal_oracle=oracle_excluded_decodable,
            attribute_and_fingerprint_oracle="pinned mrtparse JSON projection, not independent",
            elapsed_seconds=monotonic() - started,
            source_download_bytes=0,
        )
        write_summary(output / "summary.json", summary)
        return summary
    finally:
        if writer is not None:
            writer.close()
        database.close()


def event_arrow_schema():
    import pyarrow as arrow

    integers = {
        "byte_offset",
        "all_record_index",
        "expansion_ordinal",
        "source_seconds",
        "source_microseconds",
        "peer_asn",
        "afi",
        "safi",
        "path_id",
    }
    from schemas.ris import EventRow

    return arrow.schema(
        [
            (name, arrow.int64() if name in integers else arrow.string())
            for name in EventRow.model_fields
        ]
    )


def main():
    arguments = argparse.ArgumentParser()
    arguments.add_argument("--files", type=int, choices=[1, 3, 12], default=1)
    arguments.add_argument("--cache", type=Path, default=Path(".cache/phase13"))
    arguments.add_argument("--output", type=Path, required=True)
    options = arguments.parse_args()
    selection = json.loads(Path("workloads/results/dataset-feasibility/bgp.json").read_text())
    for item in selection["artifacts"][: options.files]:
        artifact = Artifact(
            sha256=item["sha256"],
            safe_reference=item["url"],
            revision="phase13-pinned",
            bytes=item["artifact_bytes"],
        )
        result = build(options.cache / item["file"], artifact, options.output / item["file"])
        print(json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
