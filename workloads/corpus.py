import hashlib
import json
import re
import tarfile
import urllib.request
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from statistics import mean, median

from pydantic import BaseModel, ConfigDict, Field


class Source(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    repository: str
    revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    license: str
    license_path: str = "LICENSE"
    archive_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


def checksum(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def classify(content: str) -> str | None:
    if re.search(r"^hostname \S+\s*$", content, re.M) and re.search(
        r"^interface (?:Ethernet|GigabitEthernet|FastEthernet|Loopback|Serial)\S*", content, re.M
    ):
        return "cisco_ios"
    if re.search(r"^\s*worker_processes\s+\S+;", content, re.M):
        return "nginx"
    return None


def skip_reason(content: bytes) -> str | None:
    try:
        decoded = content.decode("utf-8")
    except UnicodeDecodeError:
        return "non_utf8"
    if not decoded.strip() or len(content) > 2_000_000:
        return "empty_or_oversized"
    if re.search(
        r"(?im)(?:password|passwd|secret|api[_-]key|token|snmp-server community|"
        r"BEGIN .*PRIVATE KEY|authentication-key|encrypted-password)\b",
        decoded,
    ):
        return "potential_credentials"
    if classify(decoded) is None:
        return "unsupported_syntax"
    return None


def size_distribution(sizes: list[int]) -> dict:
    if not sizes:
        raise ValueError("Corpus contains no usable configurations")
    ordered = sorted(sizes)

    def percentile(fraction: float) -> float:
        position = (len(ordered) - 1) * fraction
        lower = int(position)
        upper = min(lower + 1, len(ordered) - 1)
        return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)

    return {
        "min": min(sizes),
        "median": median(sizes),
        "p90": percentile(0.90),
        "p95": percentile(0.95),
        "max": max(sizes),
        "mean": mean(sizes),
    }


def import_archive(archive: Path, source: Source, destination: Path) -> dict:
    if checksum(archive.read_bytes()) != source.archive_sha256:
        raise ValueError("Archive checksum does not match the pinned source")
    entries = []
    skipped = Counter()
    raw_bytes = 0
    with tarfile.open(archive, "r:gz") as bundle:
        candidates = sorted(
            (
                member
                for member in bundle.getmembers()
                if member.isfile() and "/configs/" in member.name
            ),
            key=lambda member: member.name,
        )
        for member in candidates:
            relative = PurePosixPath(*PurePosixPath(member.name).parts[1:])
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError("Unsafe archive path")
            if member.size > 2_000_000:
                skipped["oversized"] += 1
                raw_bytes += member.size
                continue
            stream = bundle.extractfile(member)
            if stream is None:
                raise ValueError("Archive member unavailable")
            with stream:
                content = stream.read()
            raw_bytes += len(content)
            reason = skip_reason(content)
            if reason:
                skipped[reason] += 1
                continue
            target = destination / "files" / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            entries.append(
                {
                    "path": str(relative),
                    "bytes": len(content),
                    "sha256": checksum(content),
                    "syntax": classify(content.decode()),
                }
            )
        license_members = [
            member
            for member in bundle.getmembers()
            if member.name.split("/", 1)[-1] == source.license_path
        ]
        if len(license_members) != 1:
            raise ValueError("Expected source license not found")
        license_stream = bundle.extractfile(license_members[0])
        if license_stream is None:
            raise ValueError("Source license unavailable")
        with license_stream:
            (destination / "LICENSE.source").write_bytes(license_stream.read())
    manifest = {
        "schema_version": "1.0",
        "source": source.model_dump(),
        "retrieved_at": datetime.now(UTC).date().isoformat(),
        "archive_bytes": archive.stat().st_size,
        "raw_files": len(candidates),
        "raw_bytes": raw_bytes,
        "usable_files": len(entries),
        "usable_bytes": sum(entry["bytes"] for entry in entries),
        "skipped_files": sum(skipped.values()),
        "skip_reasons": dict(sorted(skipped.items())),
        "types": dict(Counter(entry["syntax"] for entry in entries)),
        "size_bytes": size_distribution([entry["bytes"] for entry in entries]),
        "corpus_sha256": checksum(json.dumps(entries, sort_keys=True).encode()),
        "files": entries,
    }
    (destination / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def fetch(source_path: Path, destination: Path) -> dict:
    source = Source.model_validate_json(source_path.read_text())
    destination.mkdir(parents=True, exist_ok=True)
    archive = destination / "source.tar.gz"
    if not archive.exists():
        url = f"https://codeload.github.com/{source.repository}/tar.gz/{source.revision}"
        request = urllib.request.Request(url, headers={"User-Agent": "ConfigStream-dataset"})
        partial = archive.with_suffix(".partial")
        try:
            with (
                urllib.request.urlopen(request, timeout=120) as response,
                partial.open("wb") as out,
            ):
                total = 0
                while chunk := response.read(1_048_576):
                    total += len(chunk)
                    if total > 250_000_000:
                        raise ValueError("Archive exceeds the 250 MB download limit")
                    out.write(chunk)
            partial.replace(archive)
        finally:
            partial.unlink(missing_ok=True)
    return import_archive(archive, source, destination)


def load_baselines(directory: Path, size_bucket: str = "corpus-distribution") -> list[dict]:
    manifest = json.loads((directory / "manifest.json").read_text())
    entries = sorted(manifest["files"], key=lambda entry: entry["path"])
    for entry in entries:
        content = (directory / "files" / entry["path"]).read_bytes()
        if checksum(content) != entry["sha256"] or skip_reason(content):
            raise ValueError("Baseline validation failed; refetch the pinned corpus")
    if size_bucket != "corpus-distribution":
        ordered = sorted(entries, key=lambda entry: (entry["bytes"], entry["path"]))
        boundaries = {"small": (0, 1), "medium": (1, 2), "large": (2, 3)}
        if size_bucket not in boundaries:
            raise ValueError("Unknown size bucket")
        lower, upper = boundaries[size_bucket]
        entries = ordered[len(ordered) * lower // 3 : len(ordered) * upper // 3]
    if not entries:
        raise ValueError("No baselines available in the requested size bucket")
    return entries
