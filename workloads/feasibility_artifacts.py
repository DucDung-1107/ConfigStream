import argparse
import csv
import gzip
import hashlib
import io
import json
import time
import urllib.parse
import zipfile
from pathlib import Path

from workloads.feasibility import fetch, write_json

GIT_ARTIFACTS = {
    "A5": (
        "gdrosos/iac-bug-study-artifact",
        "994c6efd581f1802b578a61feb5c774d93aed44d",
        [
            "data/bugs.csv",
            "data/collection/repositories/ansible_roles_urls.csv",
            "data/collection/repositories/ansible_urls.csv",
            "data/collection/repositories/chef_urls.csv",
            "data/collection/repositories/puppet_urls.csv",
        ],
    ),
    "A6": (
        "SAILResearch/replication-21-ibrahim-dockercompose",
        "589c1f44f277e9400ec8083a2ae8250dd6176142",
        [
            "Docker-Compose-Paper-R-project/rqs/data/feature_changes_by_commit.csv",
            "Docker-Compose-Paper-R-project/rqs/data/root_compose_projects.csv",
            "Docker-Compose-Paper-R-project/rqs/data/root_compose_projects_commit_count_100.csv",
        ],
    ),
    "A11": (
        "hkerma/kubernetes-event-dataset",
        "e39cc98e8b27b6862a3f32a85a17565265db36d5",
        [
            "audit-logs.csv",
            "events-dataset.txt",
        ],
    ),
}
ZENODO_ARTIFACTS = {
    "A7": ("10648448", ["selfhosted_projects_extraction.zip"]),
    "A8": ("7194189", ["list_of_github_repositories.txt", "list_of_executed_repositories.csv"]),
    "A10": ("7552697", ["Helm_Replication_Package.zip"]),
    "A13": ("22747162", ["BPlan-SEFM26.zip"]),
}


def inspect_csv(body, *, has_header=True):
    text = body.decode("utf-8-sig")
    first_line = text.partition("\n")[0]
    delimiter = ";" if first_line.count(";") > first_line.count(",") else ","
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    first = next(reader, [])
    header = first if has_header else [f"column_{index}" for index in range(len(first))]
    rows = int(bool(first) and not has_header)
    origins = set()
    for row in reader:
        if not row:
            continue
        rows += 1
        for value in row:
            if value.startswith(("https://github.com/", "http://github.com/")):
                parts = value.split("/")
                if len(parts) >= 5:
                    origins.add("/".join(parts[3:5]).removesuffix(".git").lower())
    return {
        "columns": header,
        "rows": rows,
        "distinct_github_origins": len(origins),
        "logical_bytes": len(body),
        "unit": "CSV metadata rows, not recovered configuration versions",
    }


def inspect_zip(path):
    with zipfile.ZipFile(path) as archive:
        bad = archive.testzip()
        if bad:
            raise ValueError("ZIP CRC mismatch")
        names = archive.namelist()
        metadata = {}
        for name in names:
            if name.endswith(".csv") and archive.getinfo(name).file_size <= 50_000_000:
                metadata[name] = inspect_csv(archive.read(name))
            elif name.endswith(".csv.gz"):
                with archive.open(name) as source, gzip.GzipFile(fileobj=source) as decoded:
                    reader = csv.reader(io.TextIOWrapper(decoded))
                    columns = next(reader, [])
                    metadata[name] = {"columns": columns, "rows": sum(1 for row in reader if row)}
        return {
            "members": len(names),
            "logical_member_bytes": sum(item.file_size for item in archive.infolist()),
            "config_members": sum(name.endswith((".tf", ".yaml", ".yml")) for name in names),
            "csv": metadata,
            "crc_verified": True,
        }


def artifact_probe(cache, output):
    started = time.monotonic()
    records, failures = [], []
    for candidate, (repo, revision, paths) in GIT_ARTIFACTS.items():
        for source_path in paths:
            path = cache / (candidate + "-" + Path(source_path).name)
            url = f"https://raw.githubusercontent.com/{repo}/{revision}/{source_path}"
            try:
                artifact = fetch(url, path)
                body = path.read_bytes()
                metadata = (
                    inspect_csv(body, has_header=candidate != "A11")
                    if source_path.endswith(".csv")
                    else {
                        "nonempty_lines": sum(bool(line.strip()) for line in body.splitlines()),
                        "logical_bytes": len(body),
                    }
                )
                records.append({"candidate": candidate, "artifact": artifact, **metadata})
            except (OSError, ValueError) as error:
                failures.append(
                    {"candidate": candidate, "url": url, "category": type(error).__name__}
                )
    for candidate, (record, filenames) in ZENODO_ARTIFACTS.items():
        if not (cache / (record + ".json")).exists():
            fetch(
                f"https://zenodo.org/api/records/{record}",
                cache / (record + ".json"),
                limit=2000000,
            )
        specification = json.loads((cache / (record + ".json")).read_text())
        for filename in filenames:
            item = next(item for item in specification["files"] if item["key"] == filename)
            path = cache / filename
            try:
                artifact = fetch(item["links"]["self"], path)
                with path.open("rb") as stream:
                    checksum = hashlib.file_digest(stream, "md5").hexdigest()
                if item["checksum"] != "md5:" + checksum or path.stat().st_size != item["size"]:
                    raise ValueError("Zenodo artifact checksum/size mismatch")
                artifact["published_md5_verified"] = checksum
                if filename.endswith(".zip"):
                    metadata = inspect_zip(path)
                elif filename.endswith(".csv"):
                    metadata = inspect_csv(path.read_bytes())
                else:
                    lines = [line for line in path.read_text().splitlines() if line.strip()]
                    metadata = {
                        "repository_lines": len(lines),
                        "distinct_lines": len(set(lines)),
                        "logical_bytes": path.stat().st_size,
                    }
                records.append({"candidate": candidate, "artifact": artifact, **metadata})
            except (OSError, ValueError, zipfile.BadZipFile) as error:
                failures.append(
                    {
                        "candidate": candidate,
                        "url": item["links"]["self"],
                        "category": type(error).__name__,
                    }
                )
    result = {
        "parser": "feasibility-artifacts-v1",
        "records": records,
        "failures": failures,
        "elapsed_seconds": time.monotonic() - started,
        "security": "external scripts never executed; raw values never printed",
    }
    write_json(output, result)
    return result


def recover_bug_pairs(cache, output):
    from difflib import SequenceMatcher

    from workloads.observed import rejection

    records = []
    for ordinal in range(5):
        path = cache / f"bug-fix-{ordinal}.json"
        if not path.exists():
            records.append({"ordinal": ordinal, "status": "metadata_unavailable"})
            continue
        source = json.loads(path.read_text())
        metadata = source["metadata"]
        if metadata["license"] not in {"MIT", "Apache-2.0", "GPL-3.0", "GPL-2.0"}:
            records.append({"ordinal": ordinal, "status": "license_unknown"})
            continue
        candidates = [
            file
            for file in source["files"]
            if file["filename"].endswith((".yaml", ".yml", ".pp", ".rb"))
        ]
        if not candidates:
            records.append({"ordinal": ordinal, "status": "no_iac_extension"})
            continue
        for file in candidates[:1]:
            bodies, artifacts = [], []
            for version, revision in enumerate([metadata["before_sha"], metadata["after_sha"]]):
                url = (
                    "https://raw.githubusercontent.com/"
                    + metadata["repository"]
                    + "/"
                    + revision
                    + "/"
                    + urllib.parse.quote(file["filename"])
                )
                try:
                    artifact = fetch(
                        url, cache / f"bug-pair-{ordinal}-{version}.txt", limit=1000000
                    )
                    body = (cache / artifact["file"]).read_bytes()
                    bodies.append(body)
                    artifacts.append(artifact)
                except (OSError, ValueError):
                    break
            reasons = [rejection(body) for body in bodies]
            status = (
                "retrieval_failed"
                if len(bodies) != 2
                else "security_rejected"
                if any(reasons)
                else "recovered"
            )
            row = {
                "ordinal": ordinal,
                "status": status,
                "artifacts": artifacts,
                "path_hash": hashlib.sha256(file["filename"].encode()).hexdigest(),
                "license": metadata["license"],
                "categories": sorted(set(filter(None, reasons))),
                "note": "Extension does not prove deployment IaC; tests/changelogs included.",
            }
            if status == "recovered":
                matcher = SequenceMatcher(
                    None,
                    bodies[0].decode().splitlines(),
                    bodies[1].decode().splitlines(),
                    autojunk=False,
                )
                row["logical_bytes"] = sum(map(len, bodies))
                row["raw_changed_lines"] = sum(
                    max(old_end - old_start, new_end - new_start)
                    for operation, old_start, old_end, new_start, new_end in matcher.get_opcodes()
                    if operation != "equal"
                )
                row["unique_contents"] = len({hashlib.sha256(body).hexdigest() for body in bodies})
            records.append(row)
    result = {
        "candidate": "A5",
        "records": records,
        "policy": "First five CSV fixes; first extension candidate each; no replacement; "
        "both bodies filtered; no raw content redistribution or execution",
    }
    write_json(output, result)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache-dir", type=Path, default=Path(".cache/phase13"))
    parser.add_argument(
        "--output", type=Path, default=Path("workloads/results/dataset-feasibility/artifacts.json")
    )
    args = parser.parse_args()
    result = artifact_probe(args.cache_dir, args.output)
    print(json.dumps({"records": len(result["records"]), "failures": result["failures"]}))


if __name__ == "__main__":
    main()
