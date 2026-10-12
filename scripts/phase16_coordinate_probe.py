"""Explicit cached-only MRT coordinate proof; never invoked by normal CI."""

import argparse
import gzip
import io
import json
import sys
import time
from collections import Counter
from hashlib import file_digest
from itertools import islice
from pathlib import Path
from struct import pack

from processing.mrt_framing import frame_mrt
from schemas.temporal.errors import FrameAudit
from workloads.feasibility import mrt_events


def crosscheck(stream, parser, *, max_records=1000, artifact_sha256="a" * 64, source_scope="owned"):
    accepted = excluded = offset = 0
    categories = Counter()
    sample = []
    for frame in islice(frame_mrt(stream), max_records):
        if frame.next_offset > 16 * 1024**2:
            raise ValueError("Probe framing budget exceeded")
        header = pack(
            "!IHHI", frame.timestamp_seconds, frame.record_type, frame.subtype, frame.body_length
        )
        isolated = io.BytesIO(header + frame.body)
        try:
            parsed = next(parser.Reader(isolated))
            category = "body_or_header_parse_error" if parsed.err else "parsed"
        except KeyError:
            category = "parser_exception_KeyError"
        if category != "parsed":
            excluded += 1
        else:
            accepted += 1
            if isolated.tell() != 12 + frame.body_length:
                raise ValueError("Accepted parser frame length disagreement")
            if next(iter(parsed.data["timestamp"])) != frame.timestamp_seconds:
                raise ValueError("Timestamp mismatch")
        categories[category] += 1
        audit = FrameAudit(
            artifact_sha256=artifact_sha256,
            source_scope=source_scope,
            byte_offset=frame.byte_offset,
            all_record_index=frame.all_record_index,
            record_type=frame.record_type,
            subtype=frame.subtype,
            declared_body_bytes=frame.body_length,
            classification="parsed" if category == "parsed" else "excluded",
            error_category=None if category == "parsed" else category,
        )
        if len(sample) < 8:
            sample.append(
                {
                    "offset": audit.byte_offset,
                    "all_record_index": audit.all_record_index,
                    "next_offset": frame.next_offset,
                    "classification": category,
                    "record_type": audit.record_type,
                    "subtype": audit.subtype,
                }
            )
        offset = frame.next_offset
    return {
        "framed_records": accepted + excluded,
        "parser_accepted_records": accepted,
        "parser_excluded_records": excluded,
        "tested_decompressed_bytes": offset,
        "categories": dict(categories),
        "first_coordinates": sample,
    }


def main():
    args = argparse.ArgumentParser(description=__doc__)
    args.add_argument("--parser-dir", type=Path, required=True)
    args.add_argument("--cache-dir", type=Path, default=Path(".cache/phase13"))
    args.add_argument("--max-records", type=int, default=1000)
    args.add_argument("--output", type=Path, required=True)
    options = args.parse_args()
    if not 1 <= options.max_records <= 10000:
        raise ValueError("Probe limited to 1–10000 source records")
    sys.path.insert(0, str(options.parser_dir.resolve()))
    import mrtparse

    if mrtparse.__version__ != "2.2.0":
        raise ValueError("Only pinned mrtparse 2.2.0 accepted")
    started = time.monotonic()
    fixture = json.loads(Path("tests/fixtures/phase16/mrt.json").read_text())
    update = bytes.fromhex(fixture["one_update_hex"])
    with io.BytesIO(update) as stream:
        parsed = next(mrtparse.Reader(stream))
        if parsed.err:
            raise ValueError("Owned UPDATE parser failure")
        events = mrt_events(parsed.data)
        if [f"{event[2]}/{event[3]}" for event in events] != fixture[
            "expected_announcement_prefixes"
        ]:
            raise ValueError("Owned UPDATE expansion disagrees with literal oracle")
    owned_results = {}
    projections = json.loads(Path("tests/fixtures/phase16/mrt-projections.json").read_text())
    for name in ["mp", "addpath"]:
        data = bytes.fromhex(projections[name + "_hex"])
        if len(data) != projections[name + "_length"]:
            raise ValueError("Owned literal frame length mismatch")
        with io.BytesIO(data) as stream:
            parsed = next(mrtparse.Reader(stream))
            if parsed.err:
                raise ValueError("Owned projection parse failure")
            values = mrt_events(parsed.data)
            actual = [[value[0], f"{value[2]}/{value[3]}", value[4]] for value in values]
            if actual != projections[name + "_expected"]:
                raise ValueError("Owned MP/ADD-PATH expansion mismatch")
            owned_results[name] = {
                "framed_bytes": len(data),
                "expected_occurrences": len(actual),
                "manual_oracle_match": True,
            }
    for name in ["unsupported_hex", "malformed_bgp_body_hex"]:
        with io.BytesIO(bytes.fromhex(fixture[name]) + update) as stream:
            owned_results[name] = crosscheck(stream, mrtparse, max_records=2)
        if (
            owned_results[name]["parser_excluded_records"],
            owned_results[name]["parser_accepted_records"],
        ) != (1, 1):
            raise ValueError("Excluded owned body altered subsequent parser accounting")
    reference = json.loads(Path("workloads/results/dataset-feasibility/bgp.json").read_text())
    artifact = reference["artifacts"][0]
    path = options.cache_dir / artifact["file"]
    with path.open("rb") as stream:
        digest = file_digest(stream, "sha256").hexdigest()
    if digest != artifact["sha256"]:
        raise ValueError("Cached source artifact checksum mismatch")
    with gzip.open(path, "rb") as stream:
        measured = crosscheck(
            stream,
            mrtparse,
            max_records=options.max_records,
            artifact_sha256=digest,
            source_scope="rrc00",
        )
    result = {
        "decision": "RIS_OFFSET_IDENTITY_PROVEN",
        "proof_scope": "owned fixtures and bounded cached prefix; not full-hour oracle",
        "artifact": artifact,
        "parser": "mrtparse==2.2.0",
        "framer": "RFC6396-framing-v1",
        "selection": {
            "start_decompressed_offset": 0,
            "maximum_all_record_count": options.max_records,
        },
        "measured": measured,
        "owned_parser_crosschecks": owned_results,
        "owned_update_expanded_occurrences": len(events),
        "reference_hour_exclusions_unchanged": 57,
        "reference_repeated_fingerprints_unchanged": 118172,
        "downloads_bytes": 0,
        "elapsed_seconds": time.monotonic() - started,
        "limitations": [
            "No full-hour replay",
            "Framing does not prove every BGP body semantic",
            "Partial gzip read does not validate decompressed EOF/CRC",
            "Full occurrence/MP/ADD-PATH ledger remains Phase18 gate",
        ],
    }
    options.output.parent.mkdir(parents=True, exist_ok=True)
    options.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
