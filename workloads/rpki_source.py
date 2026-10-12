"""Verified bounded CSV source reading, without executing external content."""

import csv
import lzma
import shutil
from pathlib import Path

from schemas.rpki import DayReference, SourceChunk, SourceRow, chunk_identity, row_identity
from schemas.temporal.provenance import verify_artifact_file
from schemas.temporal.records import Authorization

HEADER = ["URI", "ASN", "IP Prefix", "Max Length", "Not Before", "Not After"]


def disk_guard(path: Path, *, reserve: int = 20 * 1024**3):
    if shutil.disk_usage(path).free < reserve:
        raise RuntimeError("RPKI disk safety reserve reached")


def csv_rows(path: Path, *, max_logical_bytes: int = 256 * 1024**2):
    logical = 0
    with lzma.open(path, "rb") as source:
        header = source.readline(65537)
        logical += len(header)
        if next(csv.reader([header.decode("utf-8")])) != HEADER:
            raise ValueError("RPKI CSV header mismatch")
        for index, line in enumerate(iter(lambda: source.readline(65537), b""), 1):
            logical += len(line)
            if len(line) > 65536 or logical > max_logical_bytes:
                raise ValueError("CSV source budget exceeded")
            try:
                fields = next(csv.reader([line.decode("utf-8")], strict=True))
            except (UnicodeError, csv.Error, StopIteration):
                raise ValueError(f"Malformed CSV encoding at source row {index}") from None
            if len(fields) != len(HEADER):
                raise ValueError(f"Invalid CSV field count at source row {index}")
            yield index, fields


def source_chunks(
    path: Path, reference: DayReference, *, chunk_size: int = 256, start_row: int = 1
):
    if not 1 <= chunk_size <= 512 or type(start_row) is not int or start_row < 1:
        raise ValueError("Invalid bounded source chunk parameters")
    verify_artifact_file(path, reference.manifest.artifacts[0])
    if start_row > reference.manifest.expected_records + 1:
        raise ValueError("Resume row exceeds source inventory")
    buffered = []
    total = 0
    for index, fields in csv_rows(path):
        total = index
        if (
            not fields[1].startswith("AS")
            or not fields[1][2:].isascii()
            or not fields[1][2:].isdigit()
        ):
            raise ValueError(f"Invalid ASN at source row {index}")
        if not fields[3].isascii() or not fields[3].isdigit():
            raise ValueError(f"Invalid maxLength at source row {index}")
        try:
            member = Authorization(
                scope=reference.manifest.source_scope,
                asn=int(fields[1][2:]),
                prefix=fields[2],
                max_length=int(fields[3]),
            )
        except ValueError:
            raise ValueError(f"Invalid authorization at source row {index}") from None
        if index < start_row:
            continue
        buffered.append(
            SourceRow(row_index=index, row_id=row_identity(reference, index), member=member)
        )
        if len(buffered) == chunk_size:
            yield make_chunk(reference, tuple(buffered))
            buffered.clear()
    if total != reference.manifest.expected_records:
        raise ValueError("CSV input accounting disagrees with pinned reference")
    if buffered:
        yield make_chunk(reference, tuple(buffered))


def make_chunk(reference, rows):
    first = rows[0].row_index
    return SourceChunk(
        observation_id=reference.observation_id,
        provenance_id=reference.manifest.manifest_id,
        artifact_sha256=reference.manifest.artifacts[0].sha256,
        observation_date=reference.observation.payload.observation_date,
        chunk_id=chunk_identity(reference, first, rows),
        first_row=first,
        rows=rows,
    )
