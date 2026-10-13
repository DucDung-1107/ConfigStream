"""Cached-only RIS layered oracle; private columnar ledgers, compact public summaries."""

import argparse
import gzip
import json
import shutil
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
from workloads.ris_reference import extract, frames, occurrence_projection


def parser_module():
    import mrtparse

    if mrtparse.__version__ != "2.2.0":
        raise ValueError("Pinned mrtparse required")
    return mrtparse


def write_summary(path, value):
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")


def build(path, artifact, output, *, source_id="ripe-ris", collector="rrc00", check=lambda: None):
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
            if records % 10000 == 0:
                check()
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
    counts, minutes, attribute_types = Counter(), Counter(), Counter()
    frame_digest, occurrence_digest = sha256(), sha256()
    database = sqlite3.connect(output / "fingerprints.sqlite")
    database.execute("CREATE TABLE fingerprints (fingerprint TEXT PRIMARY KEY)")
    database.execute("CREATE TABLE peers (peer TEXT PRIMARY KEY)")
    database.execute("CREATE TABLE prefixes (prefix TEXT PRIMARY KEY)")
    database.execute("CREATE TABLE identities (identity TEXT PRIMARY KEY)")
    repeated = total = 0
    rows = []
    writer = None
    oracle_excluded_decodable = 0
    attributes_verified = 0
    exclusion_by_index = {item["all_record_index"]: item for item in exclusions}
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
                decoded = extract(kind, subtype, body, with_attributes=True)
                oracle_error = None
            except (ValueError, KeyError) as failure:
                decoded, oracle_error = None, type(failure).__name__
            if error:
                try:
                    minimal = extract(kind, subtype, body)
                    eligible = len(minimal[3]) if minimal is not None else None
                    oracle_excluded_decodable += minimal is not None
                except (ValueError, KeyError):
                    eligible = None
                exclusion_by_index[index].update(
                    parser_version=PARSER,
                    counting_policy_version=COUNTING,
                    independent_eligible_prefixes=eligible,
                    independent_attribute_status="decoded"
                    if decoded is not None
                    else "unsupported_or_invalid",
                )
                continue
            if events and (decoded is None or oracle_error):
                raise ValueError("Independent supported-subset extractor cannot validate events")
            if decoded is not None:
                peer_asn, peer_ip, micros, prefixes, attributes = decoded
                attributes_verified += bool(attributes or prefixes)
                attribute_types.update(str(next(iter(item["type"]))) for item in attributes)
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
            independent = (
                list(occurrence_projection(reference, expected, decoded))
                if decoded is not None
                else []
            )
            if independent != [event.model_dump() for event in events]:
                raise ValueError(
                    "Independent complete occurrence/attribute projection disagreement"
                )
            for event in independent:
                total += 1
                counts[event["action"]] += 1
                minutes[stamp // 60 * 60] += 1
                encoded = json.dumps(event, sort_keys=True, separators=(",", ":")).encode()
                occurrence_digest.update(encoded + b"\n")
                database.execute("INSERT INTO identities VALUES (?)", (event["logical_record_id"],))
                repeated += not database.execute(
                    "INSERT OR IGNORE INTO fingerprints VALUES (?)", (event["content_fingerprint"],)
                ).rowcount
                database.execute(
                    "INSERT OR IGNORE INTO peers VALUES (?)",
                    (f"{event['peer_asn']}:{event['peer_ip']}",),
                )
                database.execute("INSERT OR IGNORE INTO prefixes VALUES (?)", (event["prefix"],))
                rows.append(event)
                if len(rows) == 4096:
                    table = arrow.Table.from_pylist(rows, schema=event_arrow_schema())
                    if writer is None:
                        writer = parquet.ParquetWriter(output / "events.parquet", table.schema)
                    writer.write_table(table)
                    rows.clear()
            if index % 10000 == 0:
                check()
                database.commit()
        if rows:
            table = arrow.Table.from_pylist(rows, schema=event_arrow_schema())
            if writer is None:
                writer = parquet.ParquetWriter(output / "events.parquet", table.schema)
            writer.write_table(table)
        database.commit()
        summary = dict(
            evidence="PASS_OFFLINE" if source_id == "owned" else "MEASURED_REAL_SOURCE",
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
            attribute_and_fingerprint_oracle=(
                "independent raw-wire values with frozen JSON/fingerprint serialization"
            ),
            oracle_level=3,
            attribute_type_counts=attribute_types,
            attribute_records_verified=attributes_verified,
            accepted_disposition_policy=(
                "pinned mrtparse classifications retained; not independent global BGP validity"
            ),
            elapsed_seconds=monotonic() - started,
            source_download_bytes=0,
        )
        write_summary(output / "summary.json", summary)
        write_summary(output / "exclusions.json", exclusions)
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


def reuse_verified(path, artifact, previous, target, *, check=lambda: None):
    """Reuse completed level-3 evidence unaffected by the type-21 coverage addition."""
    import pyarrow.parquet as parquet

    verify_artifact_file(path, artifact)
    summary = json.loads((previous / "summary.json").read_text())
    reference = RisReference.model_validate_json((previous / "reference.json").read_text())
    if (
        summary.get("oracle_level") != 3
        or summary.get("attribute_type_counts", {}).get("21", 0)
        or summary.get("artifact") != artifact.model_dump(mode="json")
        or reference.manifest.artifacts[0] != artifact
    ):
        raise ValueError("RIS cache not eligible for controlled oracle reuse")
    digest, count = sha256(), 0
    for batch in parquet.ParquetFile(previous / "events.parquet").iter_batches(batch_size=4096):
        check()
        for row in batch.to_pylist():
            digest.update(json.dumps(row, sort_keys=True, separators=(",", ":")).encode() + b"\n")
            count += 1
    if (count, digest.hexdigest()) != (
        summary["accepted_occurrences"],
        summary["source_order_projection_sha256"],
    ):
        raise ValueError("RIS cached independent projection checksum mismatch")
    exclusions = json.loads((previous / "exclusions.json").read_text())
    if len(exclusions) != reference.manifest.excluded_records:
        raise ValueError("RIS cached exclusion accounting mismatch")
    target.symlink_to(previous.resolve(), target_is_directory=True)
    return summary


def verify_pinned_accounting(summary, exclusions):
    """Bind both new and reused source ledgers to the immutable accepted framing audit."""
    framing = json.loads(Path("workloads/results/phase18/framing-validation.json").read_text())
    previous = next(
        item
        for item in framing["archives"]
        if item["artifact"]["sha256"] == summary["artifact"]["sha256"]
    )
    for key in ("framed_records", "update_messages", "decompressed_bytes", "framing_sha256"):
        if summary[key] != previous[key]:
            raise ValueError("RIS pinned framing evidence disagreement")
    original = json.loads(Path("workloads/results/phase18/parser-exclusions.json").read_text())
    expected = [
        (
            row["byte_offset"],
            row["all_record_index"],
            row["record_type"],
            row["subtype"],
            row["category"],
        )
        for row in original["records"]
        if row["artifact_sha256"] == summary["artifact"]["sha256"]
    ]
    actual = [
        (row["byte_offset"], row["all_record_index"], row["type"], row["subtype"], row["category"])
        for row in exclusions
    ]
    if sorted(actual) != sorted(expected):
        raise ValueError("RIS pinned exclusion coordinates/categories disagreement")


def window_ledger(output, files, *, check=lambda: None, evidence="MEASURED_REAL_SOURCE"):
    """Exact disk-backed union; repeats are calculated over the entire selected window."""
    import pyarrow.parquet as parquet

    began = monotonic()
    database = sqlite3.connect(output / "window.sqlite")
    database.execute("PRAGMA cache_size=-16384")
    database.execute(
        "CREATE TABLE occurrences (identity TEXT PRIMARY KEY, artifact TEXT, fingerprint TEXT, "
        "peer_asn INTEGER, peer_ip TEXT, prefix TEXT, stamp INTEGER, action TEXT, projection TEXT)"
    )
    summaries = []
    try:
        for filename in files:
            root = output / filename
            summaries.append(json.loads((root / "summary.json").read_text()))
            for batch in parquet.ParquetFile(root / "events.parquet").iter_batches(batch_size=4096):
                check()
                values = []
                for row in batch.to_pylist():
                    projection = sha256(
                        json.dumps(row, sort_keys=True, separators=(",", ":")).encode()
                    ).hexdigest()
                    values.append(
                        (
                            row["logical_record_id"],
                            row["artifact_sha256"],
                            row["content_fingerprint"],
                            row["peer_asn"],
                            row["peer_ip"],
                            row["prefix"],
                            row["source_seconds"],
                            row["action"],
                            projection,
                        )
                    )
                database.executemany("INSERT INTO occurrences VALUES (?,?,?,?,?,?,?,?,?)", values)
                database.commit()
        total = database.execute("SELECT COUNT(*) FROM occurrences").fetchone()[0]
        fingerprints = database.execute(
            "SELECT COUNT(DISTINCT fingerprint) FROM occurrences"
        ).fetchone()[0]
        repeated = total - fingerprints
        within = sum(item["repeated_fingerprints_beyond_first"] for item in summaries)
        digest = sha256(b"ris-window-ledger-v1\n")
        for identity_value, projection in database.execute(
            "SELECT identity,projection FROM occurrences ORDER BY identity"
        ):
            digest.update(
                json.dumps([identity_value, projection], separators=(",", ":")).encode() + b"\n"
            )
        minutes = [
            dict(minute=minute, announcement=announcement, withdrawal=withdrawal, occurrences=count)
            for minute, announcement, withdrawal, count in database.execute(
                "SELECT stamp/60*60, SUM(action='announcement'),SUM(action='withdrawal'), COUNT(*) "
                "FROM occurrences GROUP BY stamp/60 ORDER BY stamp/60"
            )
        ]
        result = dict(
            evidence=evidence,
            oracle_level=3,
            archive_count=len(files),
            accepted_occurrences=total,
            distinct_occurrence_ids=total,
            distinct_fingerprints=fingerprints,
            repeats_beyond_first=repeated,
            within_archive_repeats=within,
            cross_archive_additional_repeats=repeated - within,
            cross_archive_repeated_keys=database.execute(
                "SELECT COUNT(*) FROM (SELECT fingerprint FROM occurrences GROUP BY fingerprint "
                "HAVING COUNT(DISTINCT artifact)>1)"
            ).fetchone()[0],
            unique_peers=database.execute(
                "SELECT COUNT(*) FROM (SELECT peer_asn,peer_ip FROM occurrences "
                "GROUP BY peer_asn,peer_ip)"
            ).fetchone()[0],
            unique_prefixes=database.execute(
                "SELECT COUNT(DISTINCT prefix) FROM occurrences"
            ).fetchone()[0],
            actions=dict(
                database.execute("SELECT action,COUNT(*) FROM occurrences GROUP BY action")
            ),
            events_per_minute=minutes,
            source_rate_fixed_window=total / (300 * len(files)),
            peak_events_per_minute=max(item["occurrences"] for item in minutes),
            ledger_sha256=digest.hexdigest(),
            per_archive=summaries,
            elapsed_seconds=monotonic() - began,
            exact_cross_archive_identity_disjointness=True,
        )
        write_summary(output / "window-summary.json", result)
        return result
    finally:
        database.close()


def main():
    arguments = argparse.ArgumentParser()
    arguments.add_argument("--files", type=int, choices=[1, 3, 12], default=1)
    arguments.add_argument("--cache", type=Path, default=Path(".cache/phase13"))
    arguments.add_argument("--output", type=Path, required=True)
    arguments.add_argument("--max-seconds", type=int, choices=[1800, 3600], default=3600)
    arguments.add_argument("--verified-cache", type=Path)
    options = arguments.parse_args()
    began = monotonic()
    options.output.mkdir(parents=True, exist_ok=False)
    write_summary(
        options.output / "execution-policy.json",
        {
            "evidence": "ESTIMATE",
            "mode": "offline independent oracle",
            "files": options.files,
            "max_seconds": options.max_seconds,
            "minimum_free_disk_bytes": 20 * 1024**3,
            "maximum_private_output_bytes": 15 * 1024**3,
            "source_download_bytes": 0,
        },
    )

    def check():
        from workloads.rpki_resources import directory_bytes

        if (
            monotonic() - began > options.max_seconds
            or shutil.disk_usage(options.output).free < 20 * 1024**3
            or directory_bytes(options.output) > 15 * 1024**3
        ):
            raise RuntimeError("RIS offline oracle execution guard exceeded")

    selection = json.loads(Path("workloads/results/dataset-feasibility/bgp.json").read_text())
    for item in selection["artifacts"][: options.files]:
        artifact = Artifact(
            sha256=item["sha256"],
            safe_reference=item["url"],
            revision="phase13-pinned",
            bytes=item["artifact_bytes"],
        )
        check()
        previous = options.verified_cache / item["file"] if options.verified_cache else None
        if previous and (previous / "summary.json").exists():
            result = reuse_verified(
                options.cache / item["file"],
                artifact,
                previous,
                options.output / item["file"],
                check=check,
            )
        else:
            result = build(
                options.cache / item["file"], artifact, options.output / item["file"], check=check
            )
        verify_pinned_accounting(
            result,
            json.loads((options.output / item["file"] / "exclusions.json").read_text()),
        )
        print(json.dumps(result, sort_keys=True), flush=True)
    print(
        json.dumps(
            window_ledger(
                options.output,
                [item["file"] for item in selection["artifacts"][: options.files]],
                check=check,
            ),
            sort_keys=True,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
