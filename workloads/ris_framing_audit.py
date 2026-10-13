"""Full cached source-coordinate audit, not a full occurrence pipeline experiment."""

import argparse
import gzip
import json
import shutil
from collections import Counter
from hashlib import sha256
from itertools import zip_longest
from pathlib import Path
from time import monotonic

from processing.mrt_framing import frame_mrt
from processing.ris import decode_frame
from schemas.temporal.provenance import Artifact, verify_artifact_file
from workloads.ris_oracle import parser_module, write_summary
from workloads.ris_reference import extract, frames


def audit(cache, artifacts, output, *, max_seconds=600):
    if output.exists():
        raise ValueError("Fresh full framing audit output required")
    output.mkdir(parents=True)
    write_summary(
        output / "plan.json",
        dict(
            runtime_cap_seconds=max_seconds,
            downloads=0,
            source_files=[item["file"] for item in artifacts],
        ),
    )
    began = monotonic()
    parser = parser_module()
    summaries, exclusions = [], []
    for item in artifacts:
        artifact = Artifact(
            sha256=item["sha256"],
            bytes=item["artifact_bytes"],
            safe_reference=item["url"],
            revision="phase13-pinned",
        )
        path = cache / item["file"]
        verify_artifact_file(path, artifact)
        digest = sha256()
        records = errors = updates = end = 0
        types = Counter()
        with gzip.open(path, "rb") as stream:
            for actual, independent in zip_longest(frame_mrt(stream), frames(path)):
                if actual is None or independent is None:
                    raise ValueError("Independent framing inventory disagreement")
                coordinate = (
                    actual.byte_offset,
                    actual.all_record_index,
                    actual.timestamp_seconds,
                    actual.record_type,
                    actual.subtype,
                    actual.body,
                )
                if coordinate != independent:
                    raise ValueError("Independent frame/body coordinate disagreement")
                if records % 1000 == 0 and (
                    monotonic() - began > max_seconds
                    or shutil.disk_usage(output).free < 20 * 1024**3
                ):
                    raise RuntimeError("Framing audit resource/time guard")
                record, error = decode_frame(actual, parser)
                records += 1
                end = actual.next_offset
                types[f"{actual.record_type}/{actual.subtype}"] += 1
                digest.update(
                    f"{actual.byte_offset},{actual.all_record_index},"
                    f"{actual.timestamp_seconds},{actual.record_type},"
                    f"{actual.subtype},{actual.body_length}\n".encode()
                )
                if error:
                    errors += 1
                    try:
                        decoded = extract(actual.record_type, actual.subtype, actual.body)
                        possible = None if decoded is None else len(decoded[3])
                    except (ValueError, KeyError):
                        possible = None
                    exclusions.append(
                        dict(
                            artifact_sha256=artifact.sha256,
                            byte_offset=actual.byte_offset,
                            all_record_index=actual.all_record_index,
                            record_type=actual.record_type,
                            subtype=actual.subtype,
                            category=error,
                            fully_framed=True,
                            minimal_oracle_prefix_occurrences=possible,
                            disposition="not_accepted_under_pinned_mrtparse_policy",
                        )
                    )
                elif 2 in record.get("bgp_message", {}).get("type", {}):
                    updates += 1
        summaries.append(
            dict(
                artifact=artifact.model_dump(),
                framed_records=records,
                parser_excluded_records=errors,
                update_messages=updates,
                decompressed_bytes=end,
                record_types=dict(types),
                framing_sha256=digest.hexdigest(),
                independent_frame_match=True,
            )
        )
    result = dict(
        status="PASS_FULL_FRAMING_ONLY",
        archives=summaries,
        framed_records=sum(item["framed_records"] for item in summaries),
        update_messages=sum(item["update_messages"] for item in summaries),
        parser_excluded_records=len(exclusions),
        exclusions=exclusions,
        decompressed_bytes=sum(item["decompressed_bytes"] for item in summaries),
        elapsed_seconds=monotonic() - began,
        occurrence_projection_status="NOT_RUN_FULL_HOUR",
        source_download_bytes=0,
        runtime_cap_seconds=max_seconds,
    )
    write_summary(output / "result.json", result)
    return result


def main():
    arguments = argparse.ArgumentParser()
    arguments.add_argument("--output", type=Path, required=True)
    options = arguments.parse_args()
    selection = json.loads(Path("workloads/results/dataset-feasibility/bgp.json").read_text())
    print(json.dumps(audit(Path(".cache/phase13"), selection["artifacts"], options.output)))


if __name__ == "__main__":
    main()
