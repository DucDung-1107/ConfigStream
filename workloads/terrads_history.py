import fnmatch
import hashlib
import json
import os
import re
import subprocess
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path

from pydantic import BaseModel, Field

from processing.normalization import normalize_text
from workloads.observed import extract, git, load_dataset, rejection
from workloads.terrads import RECORD, disk_guard, write_json
from workloads.terrads_sampling import normalize_url

LICENSE_PATTERNS = {
    "MIT": r"permission is hereby granted, free of charge",
    "Apache-2.0": r"apache license\s+version 2\.0",
    "BSD-3-Clause": r"neither the name .*?endorse or promote",
    "BSD-2-Clause": r"redistribution and use in source and binary forms",
    "ISC": r"permission to use, copy, modify, and.distribute this software",
}
HISTORY_POLICY = "terrads-first-parent-file-v1"


def license_identity(content):
    text = content.decode("utf-8", errors="replace").lower()
    for identity, pattern in LICENSE_PATTERNS.items():
        if re.search(pattern, text, re.S):
            return identity
    return None


def terraform_rejection(content, max_file_bytes=1024**2):
    if len(content) > max_file_bytes:
        return "oversize"
    if b"\x00" in content:
        return "binary"
    reason = rejection(content)
    if reason:
        return reason
    if re.search(
        rb"(?i)client[_-]?secret|private[_-]?key|credentials\s*[=:]|"
        rb"AIza[0-9A-Za-z_-]{35}|ASIA[A-Z0-9]{16}",
        content,
    ):
        return "potential_credentials"
    try:
        normalize_text(content.decode("utf-8"))
    except ValueError:
        return "unsupported_normalization"
    return None


class TerraformSource(BaseModel):
    name: str = Field(pattern=r"^terrads-\d+$")
    repository: str
    start_revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    end_revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    license: str
    license_path: str
    license_sha256: str
    scope: str = "."
    asset_type: str = "terraform_file"
    sample_seed: int
    stratum: str
    repository_id: int
    dataset_id: str = RECORD

    @property
    def manifest_id(self):
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()

    @property
    def history_pathspecs(self):
        return ["*.tf"]

    def accepts(self, path):
        return fnmatch.fnmatchcase(path, "*.tf") and not path.startswith(".git/")

    def prefetch(self, repository, commits):
        objects = set()
        for commit in commits:
            for entry in git(repository, "ls-tree", "-r", "-z", commit).split(b"\0"):
                if entry:
                    header, path = entry.split(b"\t", 1)
                    mode, kind, identity = header.decode().split()
                    if mode in {"100644", "100755"} and kind == "blob" and path.endswith(b".tf"):
                        objects.add(identity)
        if len(objects) > 50000:
            raise ValueError("Selected history exceeds the 50000 distinct Terraform blob guard")
        ordered = sorted(objects)
        for offset in range(0, len(ordered), 256):
            batch = ordered[offset : offset + 256]
            result = subprocess.run(
                ["git", "--git-dir", str(repository), "fetch", "--no-tags", "origin", "--stdin"],
                input="\n".join(batch).encode(),
                capture_output=True,
                timeout=180,
                env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
            )
            if result.returncode:
                raise ValueError("Bounded selected Terraform blob fetch failed")


def directory_bytes(root):
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def extract_repository(entry, root, seed, max_commits=2000, max_file_bytes=1024**2):
    started = time.monotonic()
    repository = root / "git" / f"{entry['id']}.git"
    destination = root / "history" / f"terrads-{entry['id']}"
    status_path = root / "statuses" / f"{entry['id']}.json"
    identity = {
        "id": entry["id"],
        "url": entry["url"],
        "end_sha": entry["end_sha"],
        "stratum": entry["stratum"],
        "seed": seed,
        "max_commits": max_commits,
        "max_file_bytes": max_file_bytes,
        "policy": HISTORY_POLICY,
    }
    if status_path.exists():
        previous = json.loads(status_path.read_text())
        if previous["identity"] != identity:
            raise ValueError("Resume identity differs from frozen extraction policy")
        if previous["status"] != "incomplete":
            return previous
    row = {
        **entry,
        "identity": identity,
        "reachable": 0,
        "license_accepted": 0,
        "accepted": 0,
        "assets": 0,
        "revisions": 0,
        "bytes": 0,
        "status": "incomplete",
        "attempts": 0,
        "started_at": datetime.now(UTC).isoformat(),
        "local_cache_path": str(repository),
    }
    try:
        disk_guard(root)
        if directory_bytes(root / "git") > 16 * 1024**3:
            raise ValueError("Git cache exceeded the 16 GiB experiment guard")
        if normalize_url(entry["url"]) != entry["url"] or not re.fullmatch(
            r"[0-9a-f]{40}", entry["end_sha"]
        ):
            row["status"] = "missing_pinned_commit"
            return row
        repository.parent.mkdir(parents=True, exist_ok=True)
        if not repository.exists():
            subprocess.run(
                ["git", "init", "--bare", str(repository)], capture_output=True, check=True
            )
            git(repository, "remote", "add", "origin", entry["url"] + ".git")
            git(repository, "config", "remote.origin.promisor", "true")
            git(repository, "config", "remote.origin.partialclonefilter", "blob:none")
        if git(repository, "remote", "get-url", "origin").decode().strip() != entry["url"] + ".git":
            raise ValueError("Cached repository origin differs from sampled source")
        for attempt in range(2):
            row["attempts"] += 1
            try:
                git(
                    repository,
                    "fetch",
                    "--filter=blob:none",
                    "--no-tags",
                    "origin",
                    entry["end_sha"],
                )
                break
            except (ValueError, subprocess.TimeoutExpired):
                if attempt:
                    row["status"] = "unreachable"
                    return row
                time.sleep(1)
        if git(repository, "rev-parse", "FETCH_HEAD").decode().strip() != entry["end_sha"]:
            raise ValueError("Fetched commit differs from TerraDS pinned end SHA")
        row["reachable"] = 1
        commits = (
            git(repository, "rev-list", "--first-parent", "--reverse", entry["end_sha"])
            .decode()
            .splitlines()
        )
        selected = commits[-max_commits:]
        row["censoring"] = {
            "full_first_parent_commits": len(commits),
            "selected_commits": len(selected),
            "complete_to_root": len(commits) <= max_commits,
            "commit_count_bounded": len(commits) > max_commits,
            "max_file_bytes": max_file_bytes,
            "full_dag": False,
        }
        root_tree = git(repository, "ls-tree", "-z", entry["end_sha"])
        license_candidates = []
        for tree_entry in root_tree.split(b"\0"):
            if not tree_entry:
                continue
            header, path = tree_entry.split(b"\t", 1)
            if header.startswith(b"100") and re.match(
                rb"(?i)^(license|licence|copying)(\.|$)", path
            ):
                license_candidates.append(path.decode())
        license_bytes = None
        for license_path in sorted(license_candidates):
            content = git(repository, "show", f"{entry['end_sha']}:{license_path}")
            detected = license_identity(content)
            if detected:
                license_bytes = content
                break
        if license_bytes is None:
            row["status"] = "license_unknown"
            return row
        row.update(
            {
                "license_accepted": 1,
                "verified_license": detected,
                "license_path": license_path,
                "license_sha256": hashlib.sha256(license_bytes).hexdigest(),
                "license_scope": "root license at pinned end; per-file exceptions not adjudicated",
            }
        )
        source = TerraformSource(
            name=f"terrads-{entry['id']}",
            repository=entry["url"].split("github.com/")[1],
            start_revision=selected[0],
            end_revision=entry["end_sha"],
            license=detected,
            license_path=license_path,
            license_sha256=row["license_sha256"],
            sample_seed=seed,
            stratum=entry["stratum"],
            repository_id=entry["id"],
        )
        manifest = extract(
            source,
            repository,
            destination,
            content_filter=lambda content: terraform_rejection(content, max_file_bytes),
            incarnation_rejection=True,
            max_file_bytes=max_file_bytes,
        )
        provenance_path = destination / "provenance.jsonl"
        provenance = [json.loads(line) for line in provenance_path.read_text().splitlines()]
        for item in provenance:
            item.update(
                {
                    "terrads_dataset_id": RECORD,
                    "terrads_version": "v2",
                    "repository_id": entry["id"],
                    "repository_url": entry["url"],
                    "pinned_end_sha": entry["end_sha"],
                    "sample_seed": seed,
                    "stratum": entry["stratum"],
                    "censoring": row["censoring"],
                    "file_incarnation_id": item["asset_id"],
                    "module_group": str(Path(item["path"]).parent),
                }
            )
        provenance_path.write_text(
            "".join(json.dumps(item, sort_keys=True) + "\n" for item in provenance)
        )
        manifest["aggregate_sha256"] = hashlib.sha256(
            json.dumps(provenance, sort_keys=True).encode()
        ).hexdigest()
        manifest["censoring"] = row["censoring"]
        write_json(destination / "manifest.json", manifest)
        row.update(
            {
                "assets": manifest["selected_asset_count"],
                "revisions": len(provenance),
                "security_rejections": manifest["rejection_categories"].get(
                    "potential_credentials", 0
                ),
                "rejected_incarnations": manifest["excluded_file_count"],
                "rejection_categories": manifest["rejection_categories"],
            }
        )
        row["bytes"] = sum(
            len(json.loads(line)["content"].encode())
            for line in (destination / "histories.jsonl").read_text().splitlines()
        )
        row["accepted"] = int(row["revisions"] > 0)
        row["status"] = "accepted" if row["accepted"] else "no_accepted_history"
    except KeyboardInterrupt:
        raise
    except (ValueError, OSError, subprocess.SubprocessError, UnicodeError) as error:
        row["status"] = "extraction_failed"
        row["failure_category"] = type(error).__name__
    finally:
        row["finished_at"] = datetime.now(UTC).isoformat()
        row["seconds"] = time.monotonic() - started
        row["git_cache_bytes"] = directory_bytes(repository) if repository.exists() else 0
        row["network_bytes"] = None
        row["network_metric_note"] = (
            "Git transport bytes not instrumented; cache bytes are not network bytes"
        )
        if not row["accepted"] and destination.exists():
            quarantine = root / "incomplete" / destination.name
            quarantine.parent.mkdir(parents=True, exist_ok=True)
            if not quarantine.exists():
                destination.replace(quarantine)
        write_json(status_path, row)
    return row


def extract_sample(
    manifest, root, results, concurrency=4, max_commits=2000, max_file_bytes=1024**2
):
    if (
        not 1 <= concurrency <= 4
        or not 1 <= max_commits <= 10000
        or not 1 <= max_file_bytes <= 1024**2
    ):
        raise ValueError("Bounded concurrency/commit/file limits required")
    started = time.monotonic()
    rows = []
    executor = ThreadPoolExecutor(max_workers=concurrency)
    try:
        futures = [
            executor.submit(
                extract_repository, entry, root, manifest["seed"], max_commits, max_file_bytes
            )
            for entry in manifest["selected"]
        ]
        for completed in as_completed(futures):
            row = completed.result()
            rows.append(row)
            print(
                json.dumps(
                    {
                        "id": row["id"],
                        "status": row["status"],
                        "revisions": row["revisions"],
                        "completed": len(rows),
                        "sample_size": manifest["sample_size"],
                    }
                ),
                flush=True,
            )
            write_json(
                results / "repository-summary.json", sorted(rows, key=lambda item: item["id"])
            )
    finally:
        executor.shutdown(wait=True, cancel_futures=True)
    summary = {
        "sample_size": len(rows),
        "wall_seconds_this_invocation": time.monotonic() - started,
        "max_concurrency": concurrency,
        "free_disk_bytes": disk_guard(root),
        "git_cache_bytes": directory_bytes(root / "git"),
        "statuses": dict(Counter(row["status"] for row in rows)),
        "totals": {
            name: sum(row.get(name, 0) for row in rows)
            for name in [
                "reachable",
                "license_accepted",
                "accepted",
                "assets",
                "revisions",
                "bytes",
                "security_rejections",
            ]
        },
    }
    write_json(results / f"extraction-{manifest['sample_size']}.json", summary)
    write_json(results / "extraction-summary.json", summary)
    return summary


def accepted_dataset(root, manifest):
    if not any((root / "history").glob("*/manifest.json")):
        return [], {}, []
    snapshots, provenance, sources = load_dataset(root / "history")
    selected = {entry["id"] for entry in manifest["selected"]}
    snapshots = [
        snapshot
        for snapshot in snapshots
        if provenance[snapshot.snapshot_id]["repository_id"] in selected
    ]
    provenance = {snapshot.snapshot_id: provenance[snapshot.snapshot_id] for snapshot in snapshots}
    return snapshots, provenance, sources
