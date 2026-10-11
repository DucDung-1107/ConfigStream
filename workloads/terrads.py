import csv
import hashlib
import json
import os
import platform
import re
import shutil
import sqlite3
import tarfile
import time
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

from workloads.empirical import distribution

RECORD = "20339474"
ROOT = Path(".cache/datasets/terrads")
RESULTS = Path("workloads/results/terrads")
RESERVE = 20 * 1024**3


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    compact = json.dumps(value, sort_keys=True, separators=(",", ":"))
    serialized = compact if len(compact) > 65536 else json.dumps(value, indent=2, sort_keys=True)
    temporary.write_text(serialized + "\n")
    temporary.replace(path)


def file_hash(path, algorithm="sha256"):
    digest = hashlib.new(algorithm)
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def disk_guard(root, required=0):
    root.mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(root).free
    if free - required < RESERVE or required > free // 2:
        raise ValueError("Disk safety reserve would be exceeded")
    return free


def download_range(url, destination, start, end, total):
    disk_guard(destination.parent, end - start + 1)
    expected = end - start + 1
    transferred = 0
    for attempt in range(4):
        offset = destination.stat().st_size if destination.exists() else 0
        if offset == expected:
            return transferred
        if offset > expected:
            raise ValueError("Range cache exceeds pinned range size")
        try:
            request = urllib.request.Request(
                url, headers={"Range": f"bytes={start + offset}-{end}"}
            )
            with urllib.request.urlopen(request, timeout=120) as response:
                if (
                    response.status != 206
                    or response.headers.get("Content-Range")
                    != f"bytes {start + offset}-{end}/{total}"
                ):
                    raise ValueError("Server did not honor bounded range")
                with destination.open("ab") as stream:
                    while block := response.read(1024 * 1024):
                        if stream.tell() + len(block) > expected:
                            raise ValueError("Range exceeds pinned byte boundary")
                        disk_guard(destination.parent)
                        stream.write(block)
                        transferred += len(block)
            if destination.stat().st_size != expected:
                raise OSError("Incomplete range transfer")
            return transferred
        except (OSError, TimeoutError):
            if attempt == 3:
                raise ValueError("Bounded range download failed; cache retained") from None
            time.sleep(2**attempt)


def download(url, destination, size, checksum):
    started = time.monotonic()
    transferred = 0
    disk_guard(destination.parent, size)
    if destination.exists():
        if destination.stat().st_size != size or file_hash(destination, "md5") != checksum:
            raise ValueError("Existing artifact fails size/checksum verification")
        return {"network_bytes": 0, "seconds": time.monotonic() - started, "cached": True}
    partial = destination.with_suffix(destination.suffix + ".part")
    if destination.name == "TerraDS.sqlite":
        prefix = partial.stat().st_size if partial.exists() else 0
        ranges = [
            (offset, min(size - 1, offset + 64 * 1024**2 - 1))
            for offset in range(prefix, size, 64 * 1024**2)
        ]
        chunks = [
            destination.with_name(f"{destination.name}.range-{offset}.part") for offset, _ in ranges
        ]
        with ThreadPoolExecutor(max_workers=4) as executor:
            for completed in as_completed(
                [
                    executor.submit(download_range, url, chunk, start, end, size)
                    for chunk, (start, end) in zip(chunks, ranges, strict=True)
                ]
            ):
                transferred += completed.result()
        assembled = destination.with_suffix(".assembled")
        disk_guard(destination.parent, size)
        with assembled.open("wb") as output:
            for path in ([partial] if prefix else []) + chunks:
                with path.open("rb") as stream:
                    shutil.copyfileobj(stream, output, 1024 * 1024)
        if assembled.stat().st_size != size or file_hash(assembled, "md5") != checksum:
            raise ValueError("Assembled artifact checksum mismatch; evidence retained")
        assembled.replace(destination)
        for path in ([partial] if prefix else []) + chunks:
            path.unlink()
        return {
            "network_bytes": transferred,
            "seconds": time.monotonic() - started,
            "cached": False,
            "resumed_prefix_bytes": prefix,
            "range_workers": 4,
        }
    for attempt in range(4):
        offset = partial.stat().st_size if partial.exists() else 0
        try:
            request = urllib.request.Request(url, headers={"Range": f"bytes={offset}-"})
            with urllib.request.urlopen(request, timeout=120) as response:
                if offset and response.status != 206:
                    offset = 0
                if response.status == 206 and not response.headers.get(
                    "Content-Range", ""
                ).startswith(f"bytes {offset}-"):
                    raise ValueError("Unexpected artifact range response")
                with partial.open("ab" if offset else "wb") as output:
                    while block := response.read(1024 * 1024):
                        transferred += len(block)
                        if output.tell() + len(block) > size:
                            raise ValueError("Artifact exceeded advertised size")
                        disk_guard(destination.parent)
                        output.write(block)
            if partial.stat().st_size != size or file_hash(partial, "md5") != checksum:
                raise ValueError("Downloaded artifact fails size/checksum verification")
            partial.replace(destination)
            return {
                "network_bytes": transferred,
                "seconds": time.monotonic() - started,
                "cached": False,
            }
        except (OSError, TimeoutError):
            if attempt == 3:
                raise ValueError(
                    "Bounded artifact download failed; partial file retained"
                ) from None
            time.sleep(2**attempt)


def fetch(root=ROOT, results=RESULTS):
    free = disk_guard(root)
    with urllib.request.urlopen(f"https://zenodo.org/api/records/{RECORD}", timeout=60) as response:
        record = json.load(response)
    metadata = record["metadata"]
    if metadata["doi"] != f"10.5281/zenodo.{RECORD}" or metadata["license"]["id"] != "cc-by-4.0":
        raise ValueError("Unexpected pinned TerraDS record/license")
    description = metadata["description"]
    counts = {}
    for name in ("repositories", "modules", "resources"):
        matches = re.findall(r"(\d[\d,]*)\s+" + name, description)
        if not matches:
            raise ValueError("Reported TerraDS counts unavailable")
        counts[name] = int(matches[0].replace(",", ""))
    manifest = {
        "schema_version": "1.0",
        "record": RECORD,
        "version": "v2",
        "doi": metadata["doi"],
        "url": f"https://zenodo.org/records/{RECORD}",
        "publication_date": metadata["publication_date"],
        "cutoff": "2026-01",
        "artifact_license": metadata["license"]["id"],
        "reported_counts": counts,
        "verified_at": datetime.now(UTC).isoformat(),
        "machine": {
            "os": platform.platform(),
            "architecture": platform.machine(),
            "cpu_cores": os.cpu_count(),
            "initial_free_bytes": free,
            "total_disk_bytes": shutil.disk_usage(root).total,
            "disk_reserve_bytes": RESERVE,
        },
        "files": [],
        "available_artifacts": [
            {"name": entry["key"], "bytes": entry["size"], "checksum": entry["checksum"]}
            for entry in record["files"]
        ],
        "download_metric_scope": (
            "Final resumed invocation only; prior attempt time/traffic not measured"
        ),
    }
    if platform.system() == "Darwin":
        import subprocess

        manifest["machine"]["ram_bytes"] = int(
            subprocess.check_output(["sysctl", "-n", "hw.memsize"])
        )
    selected = [
        entry for entry in record["files"] if entry["key"] in {"TerraDS.sqlite", "TerraDS.tar.gz"}
    ]
    if len(selected) != 2:
        raise ValueError("Pinned TerraDS source archive and SQLite artifact must both exist")
    disk_guard(root, sum(entry["size"] for entry in selected))

    def retrieve(entry):
        name = entry["key"]
        if Path(name).name != name or not entry["checksum"].startswith("md5:"):
            raise ValueError("Unexpected artifact filename/checksum")
        print(json.dumps({"downloading": name, "bytes": entry["size"]}), flush=True)
        metrics = download(
            entry["links"]["self"], root / name, entry["size"], entry["checksum"][4:]
        )
        return {
            "name": name,
            "bytes": entry["size"],
            "published_checksum": entry["checksum"],
            "sha256": file_hash(root / name),
            **metrics,
        }

    with ThreadPoolExecutor(max_workers=2) as executor:
        for completed in as_completed([executor.submit(retrieve, entry) for entry in selected]):
            manifest["files"].append(completed.result())
            manifest["files"].sort(key=lambda entry: entry["name"])
            write_json(results / "source-manifest.json", manifest)
    return manifest


def database(root=ROOT):
    connection = sqlite3.connect(f"file:{(root / 'TerraDS.sqlite').resolve()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def verify_cached(root=ROOT, results=RESULTS):
    previous = json.loads((results / "source-manifest.json").read_text())
    with urllib.request.urlopen(f"https://zenodo.org/api/records/{RECORD}", timeout=60) as response:
        record = json.load(response)
    metadata = record["metadata"]
    if (
        metadata["doi"] != previous["doi"]
        or metadata["publication_date"] != previous["publication_date"]
        or metadata["license"]["id"] != previous["artifact_license"]
    ):
        raise ValueError("Pinned primary-source metadata changed")
    available = {entry["key"]: entry for entry in record["files"]}
    verified = []
    for artifact in previous["files"]:
        name = artifact["name"]
        current = available[name]
        path = root / name
        if (
            current["size"] != artifact["bytes"]
            or current["checksum"] != artifact["published_checksum"]
            or path.stat().st_size != current["size"]
            or "md5:" + file_hash(path, "md5") != current["checksum"]
            or file_hash(path) != artifact["sha256"]
        ):
            raise ValueError("Cached artifact no longer matches pinned primary source")
        verified.append(
            {key: artifact[key] for key in ("name", "bytes", "sha256", "published_checksum")}
        )
    summary = {
        "verified_at": datetime.now(UTC).isoformat(),
        "doi": metadata["doi"],
        "publication_date": metadata["publication_date"],
        "artifact_license": metadata["license"]["id"],
        "files": verified,
        "download_metrics_preserved": True,
        "free_disk_bytes": disk_guard(root),
    }
    write_json(results / "source-reverification.json", summary)
    return summary


def inspect_static(root=ROOT, results=RESULTS):
    manifest = json.loads((results / "source-manifest.json").read_text())
    artifact = next(entry for entry in manifest["files"] if entry["name"] == "TerraDS.tar.gz")
    if file_hash(root / artifact["name"]) != artifact["sha256"]:
        raise ValueError("Static archive checksum mismatch")
    sizes = Counter()
    hashes = set()
    repositories = set()
    per_repository = Counter()
    repository_files = Counter()
    skipped = Counter()

    def scan(archive, repository_id=None):
        for member in archive:
            path = PurePosixPath(member.name)
            if path.is_absolute() or ".." in path.parts:
                raise ValueError("Unsafe archive member")
            if not member.isfile():
                continue
            if repository_id is None and re.fullmatch(r"\d+\.tar\.gz", path.name):
                identity = path.name.removesuffix(".tar.gz")
                with archive.extractfile(member) as stream:
                    with tarfile.open(fileobj=stream, mode="r|gz") as inner:
                        scan(inner, identity)
                continue
            if not member.name.endswith(".tf"):
                continue
            stream = archive.extractfile(member)
            digest = hashlib.sha256()
            with stream:
                while block := stream.read(1024 * 1024):
                    digest.update(block)
            identifiers = [part for part in path.parts[:-1] if part.isdecimal()]
            identity = repository_id or (identifiers[0] if identifiers else None)
            if identity is None:
                skipped["repository_id_unresolved"] += 1
            else:
                repositories.add(identity)
                per_repository[identity] += member.size
                repository_files[identity] += 1
            sizes[member.size] += 1
            hashes.add(digest.hexdigest())

    with tarfile.open(root / "TerraDS.tar.gz", "r|gz") as archive:
        scan(archive)
    with database(root) as connection:
        tables = [
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        ]
        schema = {
            table: [dict(row) for row in connection.execute(f'PRAGMA table_info("{table}")')]
            for table in tables
        }
        counts = {
            table: connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            for table in ("Repositories", "Modules", "Resources")
        }
        repository_metadata = connection.execute("""
            SELECT r.Id, COALESCE(m.modules, 0), COALESCE(m.resources, 0)
            FROM Repositories r LEFT JOIN
            (SELECT m.RepositoryId, COUNT(DISTINCT m.Id) AS modules,
              COUNT(rr.Id) AS resources FROM Modules m
              LEFT JOIN Resources rr ON rr.ModuleId=m.Id GROUP BY m.RepositoryId) m
            ON m.RepositoryId=r.Id ORDER BY r.Id
        """).fetchall()
    repository_rows = [
        {
            "repository_id": row[0],
            "terraform_files": repository_files[str(row[0])],
            "logical_bytes": per_repository[str(row[0])],
            "modules": row[1],
            "resources": row[2],
        }
        for row in repository_metadata
    ]
    summary = {
        "artifact_sha256": artifact["sha256"],
        "database_counts": counts,
        "archive_repositories": len(repositories),
        "terraform_files": sum(sizes.values()),
        "logical_terraform_bytes": sum(size * count for size, count in sizes.items()),
        "compressed_archive_bytes": artifact["bytes"],
        "unique_contents": len(hashes),
        "duplicate_contents": sum(sizes.values()) - len(hashes),
        "unique_content_ratio": len(hashes) / sum(sizes.values()),
        "file_size_bytes": distribution(sizes),
        "unresolved": dict(skipped),
        "repository_distributions": {
            field: distribution([row[field] for row in repository_rows])
            for field in ("terraform_files", "logical_bytes", "modules", "resources")
        },
    }
    with (results / "static-repositories.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(repository_rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(repository_rows)
    write_json(root / "repository-source-bytes.json", dict(per_repository))
    write_json(root / "database-schema.json", schema)
    write_json(results / "static-summary.json", summary)
    (results / "static-summary.md").write_text(
        "# Measured TerraDS static corpus\n\n"
        "Evidence type: real static source; not observed temporal history.\n\n"
        + "\n".join(
            f"- {key}: {value}" for key, value in summary.items() if not isinstance(value, dict)
        )
        + "\n\nRepository counts/bytes/modules/resources: `static-repositories.csv`.\n"
        "File and repository percentile distributions: `static-summary.json`.\n"
    )
    with (results / "static-summary.csv").open("w", newline="") as stream:
        flat = {
            **counts,
            **{key: value for key, value in summary.items() if not isinstance(value, dict)},
            **{
                "file_size_" + key: value
                for key, value in summary["file_size_bytes"].items()
                if key != "histogram"
            },
        }
        writer = csv.DictWriter(stream, fieldnames=list(flat), lineterminator="\n")
        writer.writeheader()
        writer.writerow(flat)
    return summary
