"""Disk-backed union oracle with independently specified cross-archive expectations."""

import gzip
import json
from hashlib import sha256
from pathlib import Path

from schemas.temporal.provenance import Artifact
from workloads.ris_oracle import build, reuse_verified, window_ledger


def test_cross_archive_repeats_are_not_per_file_sum(tmp_path):
    fixture = json.loads(Path("tests/fixtures/phase16/mrt.json").read_text())
    wire = bytes.fromhex(fixture["one_update_hex"]) * 2
    output = tmp_path / "oracle"
    output.mkdir()
    files = ["owned-first.gz", "owned-second.gz"]
    for index, filename in enumerate(files):
        source = tmp_path / filename
        source.write_bytes(gzip.compress(wire, mtime=index))
        artifact = Artifact(
            sha256=sha256(source.read_bytes()).hexdigest(),
            bytes=source.stat().st_size,
            safe_reference=f"fixture:phase18-closure/{filename}",
            revision="owned-v1",
        )
        build(source, artifact, output / filename, source_id="owned")
        cached = tmp_path / f"reused-{index}"
        assert (
            reuse_verified(source, artifact, output / filename, cached)["accepted_occurrences"] == 4
        )
    summary = window_ledger(output, files, evidence="PASS_OFFLINE")
    assert summary["accepted_occurrences"] == 8
    assert summary["distinct_occurrence_ids"] == 8
    assert summary["distinct_fingerprints"] == 2
    assert summary["repeats_beyond_first"] == 6
    assert summary["within_archive_repeats"] == 4
    assert summary["cross_archive_additional_repeats"] == 2
    assert summary["cross_archive_repeated_keys"] == 2


def test_unsupported_source_is_unknown_not_zero_prefix_proof(tmp_path):
    fixture = json.loads(Path("tests/fixtures/phase16/mrt.json").read_text())
    wire = bytes.fromhex(fixture["unsupported_hex"]) + bytes.fromhex(fixture["one_update_hex"])
    source = tmp_path / "owned.gz"
    source.write_bytes(gzip.compress(wire, mtime=0))
    artifact = Artifact(
        sha256=sha256(source.read_bytes()).hexdigest(),
        bytes=source.stat().st_size,
        safe_reference="fixture:phase18-closure/unsupported.gz",
        revision="owned-v1",
    )
    summary = build(source, artifact, tmp_path / "oracle", source_id="owned")
    assert summary["evidence"] == "PASS_OFFLINE"
    exclusions = json.loads((tmp_path / "oracle/exclusions.json").read_text())
    assert exclusions[0]["independent_eligible_prefixes"] is None
