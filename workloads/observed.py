import fnmatch
import json
import re
import subprocess
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path, PurePosixPath
from uuid import NAMESPACE_URL, uuid5

from pydantic import BaseModel, ConfigDict, Field, model_validator

from schemas import Asset, ChangeEvent, ConfigSnapshot
from workloads.corpus import skip_reason

ALLOWLIST = {"FRRouting/frr", "nginx/nginx"}
POLICY = "observed-git-v1"


class GitSource(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(pattern=r"^[a-z0-9-]+$")
    repository: str
    default_branch: str
    start_revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    end_revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    shallow_since: str
    license: str = Field(min_length=1)
    license_path: str
    license_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    scope: str
    allowed_patterns: list[str] = Field(min_length=1)
    excluded_patterns: list[str] = Field(default_factory=list)
    asset_type: str

    @model_validator(mode="after")
    def safe_scope(self):
        if self.repository not in ALLOWLIST:
            raise ValueError("Repository is not explicitly allowlisted")
        for value in [
            self.scope,
            self.license_path,
            *self.allowed_patterns,
            *self.excluded_patterns,
        ]:
            if PurePosixPath(value).is_absolute() or ".." in PurePosixPath(value).parts:
                raise ValueError("Unsafe source path")
        if not all(value.startswith(self.scope + "/") for value in self.allowed_patterns):
            raise ValueError("Patterns must remain within the selected source scope")
        if datetime.fromisoformat(self.shallow_since).tzinfo is None:
            raise ValueError("Fetch boundary requires a timezone")
        return self

    @property
    def manifest_id(self):
        return sha256(self.model_dump_json().encode()).hexdigest()

    def accepts(self, path):
        return any(
            fnmatch.fnmatchcase(path, pattern) for pattern in self.allowed_patterns
        ) and not any(fnmatch.fnmatchcase(path, pattern) for pattern in self.excluded_patterns)


def git(repository: Path, *arguments: str) -> bytes:
    result = subprocess.run(
        ["git", "--git-dir", str(repository), *arguments],
        capture_output=True,
        timeout=180,
    )
    if result.returncode:
        raise ValueError("Git source command failed; verify pinned objects and fetch boundary")
    return result.stdout


def fetch_source(source: GitSource, root=Path(".cache/datasets/git")) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    repository = root / f"{source.name}.git"
    url = f"https://github.com/{source.repository}.git"
    if not repository.exists():
        subprocess.run(["git", "init", "--bare", str(repository)], check=True, capture_output=True)
        git(repository, "remote", "add", "origin", url)
    if git(repository, "remote", "get-url", "origin").decode().strip() != url:
        raise ValueError("Existing Git source has an unexpected origin")
    git(
        repository,
        "fetch",
        f"--shallow-since={source.shallow_since}",
        "origin",
        source.end_revision,
    )
    if git(repository, "rev-parse", "FETCH_HEAD").decode().strip() != source.end_revision:
        raise ValueError("Fetched revision differs from pinned source")
    return repository


def rejection(content: bytes) -> str | None:
    if len(content) > 2_000_000:
        return "oversize"
    reason = skip_reason(content)
    if reason and reason != "unsupported_syntax":
        return reason
    text = content.decode("utf-8")
    if re.search(
        r"(?i)bearer\s+\S+|AKIA[A-Z0-9]{16}|(?:aws_)?(?:access_key|secret_key)\s*[=:]"
        r"|[A-Za-z0-9+/=_-]{64,}",
        text,
    ):
        return "potential_credentials"
    return None


def extract(source: GitSource, repository: Path, destination: Path) -> dict:
    license_bytes = git(repository, "show", f"{source.end_revision}:{source.license_path}")
    if sha256(license_bytes).hexdigest() != source.license_sha256:
        raise ValueError("Pinned source license verification failed")
    commits = [
        source.start_revision,
        *git(
            repository,
            "rev-list",
            "--first-parent",
            "--reverse",
            f"{source.start_revision}..{source.end_revision}",
        )
        .decode()
        .splitlines(),
    ]
    if (
        source.start_revision
        not in git(repository, "rev-list", "--first-parent", source.end_revision)
        .decode()
        .splitlines()
    ):
        raise ValueError("Start revision is not on the selected first-parent history")
    relevant = set(
        git(
            repository,
            "rev-list",
            "--first-parent",
            f"{source.start_revision}..{source.end_revision}",
            "--",
            source.scope,
        )
        .decode()
        .splitlines()
    )
    timestamps = {
        line.split(" ", 1)[0]: line.split(" ", 1)[1]
        for line in git(repository, "log", "--first-parent", "--format=%H %cI", source.end_revision)
        .decode()
        .splitlines()
    }
    active = {}
    histories = defaultdict(list)
    rejected = {}
    blob_cache = {}
    incarnations = Counter()
    seen_paths = set()
    for ordinal, commit in enumerate(commits):
        if ordinal and commit not in relevant:
            continue
        tree = git(repository, "ls-tree", "-r", "-z", commit, "--", source.scope)
        current = {}
        for entry in tree.split(b"\0"):
            if not entry:
                continue
            header, encoded_path = entry.split(b"\t", 1)
            mode, kind, blob = header.decode().split()
            path = encoded_path.decode("utf-8")
            if mode not in {"100644", "100755"} or kind != "blob" or not source.accepts(path):
                continue
            current[path] = blob
            seen_paths.add(path)
        for path in active.keys() - current.keys():
            if histories.get((path, incarnations[path])):
                histories[(path, incarnations[path])][-1]["observation_end"] = timestamps[commit]
        for path, blob in sorted(current.items()):
            if path in rejected or active.get(path) == blob:
                continue
            if blob not in blob_cache:
                content = git(repository, "cat-file", "blob", blob)
                blob_cache[blob] = (rejection(content), content)
            reason, content = blob_cache[blob]
            if reason:
                rejected[path] = reason
                continue
            if path not in active:
                incarnations[path] += 1
            histories[(path, incarnations[path])].append(
                {
                    "commit": commit,
                    "original_timestamp": timestamps[commit],
                    "ordinal": ordinal,
                    "content": content.decode("utf-8"),
                    "boundary_baseline": ordinal == 0,
                }
            )
        active = current
    destination.mkdir(parents=True, exist_ok=True)
    provenance = []
    snapshots = []
    end_time = timestamps[source.end_revision]
    for (path, incarnation), versions in sorted(histories.items()):
        if path in rejected:
            continue
        observation_end = versions[-1].get("observation_end", end_time)
        versions.sort(
            key=lambda item: (datetime.fromisoformat(item["original_timestamp"]), item["ordinal"])
        )
        asset_identity = source.manifest_id + ":" + path + ":" + str(incarnation)
        asset_id = f"observed-{uuid5(NAMESPACE_URL, asset_identity)}"
        previous_commit = None
        previous_hash = None
        version = 0
        for item in versions:
            digest = sha256(item["content"].encode()).hexdigest()
            if digest == previous_hash:
                continue
            version += 1
            timestamp = datetime.fromisoformat(item["original_timestamp"]).astimezone(UTC)
            snapshot_identity = f"{asset_id}:{item['commit']}:{digest}"
            snapshot = ConfigSnapshot(
                snapshot_id=f"snapshot-{uuid5(NAMESPACE_URL, snapshot_identity)}",
                asset_id=asset_id,
                version=version,
                config_format="text",
                event_time=timestamp,
                ingest_time=timestamp + timedelta(milliseconds=5),
                hash=digest,
                content=item["content"],
            )
            snapshots.append(snapshot)
            provenance.append(
                {
                    "source": source.name,
                    "repository": source.repository,
                    "path": path,
                    "asset_type": source.asset_type,
                    "source_manifest_id": source.manifest_id,
                    "license": source.license,
                    "snapshot_id": snapshot.snapshot_id,
                    "asset_id": asset_id,
                    "version": version,
                    "git_commit": item["commit"],
                    "commit_ordinal": item["ordinal"],
                    "previous_selected_commit": previous_commit,
                    "original_timestamp": item["original_timestamp"],
                    "event_time": timestamp.isoformat(),
                    "content_sha256": digest,
                    "boundary_baseline": item["boundary_baseline"],
                    "observation_end": observation_end,
                }
            )
            previous_hash, previous_commit = digest, item["commit"]
    manifest = {
        "schema_version": "1.0",
        "policy": POLICY,
        "source": source.model_dump(),
        "source_manifest_id": source.manifest_id,
        "repository_url": f"https://github.com/{source.repository}",
        "retrieval_date": datetime.now(UTC).date().isoformat(),
        "source_commit_count": len(commits),
        "relevant_scope_commits": len(relevant),
        "candidate_file_count": len(seen_paths),
        "selected_file_count": len({item["path"] for item in provenance}),
        "selected_asset_count": len({item.asset_id for item in snapshots}),
        "extracted_revision_count": len(snapshots),
        "excluded_file_count": len(rejected),
        "rejection_categories": dict(Counter(rejected.values())),
        "rejections": [
            {
                "source": source.name,
                "path_sha256": sha256(path.encode()).hexdigest(),
                "category": reason,
            }
            for path, reason in sorted(rejected.items())
        ],
        "aggregate_sha256": sha256(json.dumps(provenance, sort_keys=True).encode()).hexdigest(),
        "observation_start": timestamps[source.start_revision],
        "observation_end": end_time,
    }
    with (destination / "histories.jsonl").open("w") as output:
        for snapshot in sorted(
            snapshots, key=lambda item: (item.event_time, item.asset_id, item.version)
        ):
            output.write(snapshot.model_dump_json() + "\n")
    with (destination / "provenance.jsonl").open("w") as output:
        for item in provenance:
            output.write(json.dumps(item, sort_keys=True) + "\n")
    (destination / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (destination / "LICENSE.source").write_bytes(license_bytes)
    return manifest


def load_dataset(root: Path):
    snapshots = []
    provenance = {}
    manifests = []
    for directory in sorted(root.glob("*/manifest.json")):
        manifest = json.loads(directory.read_text())
        manifests.append(manifest)
        items = [
            json.loads(line)
            for line in (directory.parent / "provenance.jsonl").read_text().splitlines()
        ]
        if (
            sha256(json.dumps(items, sort_keys=True).encode()).hexdigest()
            != manifest["aggregate_sha256"]
        ):
            raise ValueError("Observed provenance checksum mismatch")
        for item in items:
            if item["snapshot_id"] in provenance:
                raise ValueError("Duplicate observed snapshot provenance")
            provenance[item["snapshot_id"]] = item
        lines = (directory.parent / "histories.jsonl").read_text().splitlines()
        if len(lines) != manifest["extracted_revision_count"] or len(lines) != len(items):
            raise ValueError("Observed extraction count mismatch")
        for line in lines:
            try:
                snapshot = ConfigSnapshot.model_validate_json(line)
            except ValueError:
                raise ValueError("Observed snapshot schema validation failed") from None
            if (
                snapshot.content is None
                or rejection(snapshot.content.encode())
                or sha256(snapshot.content.encode()).hexdigest() != snapshot.hash
            ):
                raise ValueError("Observed snapshot security/hash validation failed")
            if provenance[snapshot.snapshot_id]["content_sha256"] != snapshot.hash:
                raise ValueError("Observed snapshot differs from source provenance")
            metadata = provenance[snapshot.snapshot_id]
            if (
                metadata["asset_id"],
                metadata["version"],
                datetime.fromisoformat(metadata["event_time"]),
            ) != (snapshot.asset_id, snapshot.version, snapshot.event_time):
                raise ValueError("Observed identity/timestamp provenance mismatch")
            snapshots.append(snapshot)
    if not snapshots:
        raise ValueError("No extracted observed histories available")
    grouped = defaultdict(list)
    for snapshot in snapshots:
        grouped[snapshot.asset_id].append(snapshot)
    for history in grouped.values():
        history.sort(key=lambda snapshot: snapshot.version)
        if [snapshot.version for snapshot in history] != list(range(1, len(history) + 1)):
            raise ValueError("Observed versions must be unique, contiguous and start at one")
        if any(
            current.event_time < previous.event_time
            for previous, current in zip(history, history[1:], strict=False)
        ):
            raise ValueError("Observed versions must be ordered by commit timestamp")
    return snapshots, provenance, manifests


class ObservedHistory:
    def __init__(self, root: Path, *, seed=42, multiplier=1, calibrated=False):
        import random

        if type(multiplier) is not int or multiplier < 1 or multiplier > 1000:
            raise ValueError("Scale multiplier must be an integer between 1 and 1000")
        if not calibrated and multiplier != 1:
            raise ValueError("Observed replay cannot clone measured assets")
        snapshots, provenance, manifests = load_dataset(root)
        grouped = defaultdict(list)
        for snapshot in snapshots:
            grouped[snapshot.asset_id].append(snapshot)
        for history in grouped.values():
            history.sort(key=lambda snapshot: snapshot.version)
        self.identity = "observed-calibrated-v1" if calibrated else POLICY
        self.profile = "observed-calibrated" if calibrated else "observed-replay"
        self.manifest = {
            "profile": self.profile,
            "source_manifest_ids": [entry["source_manifest_id"] for entry in manifests],
            "source_checksums": [entry["aggregate_sha256"] for entry in manifests],
            "empirical_stat_version": "empirical-v1",
            "seed": seed,
            "scale_multiplier": multiplier,
            "sampling": "whole-history bootstrap with replacement" if calibrated else "no sampling",
            "preserves": ["content", "versions", "original inter-change times"],
        }
        self.groups = []
        self.assets = []
        randomizer = random.Random(seed)
        population = sorted(grouped)
        for index in range(len(population) * multiplier):
            original = randomizer.choice(population) if calibrated else population[index]
            identity = json.dumps([self.identity, self.manifest, index, original], sort_keys=True)
            asset_id = f"calibrated-{uuid5(NAMESPACE_URL, identity)}" if calibrated else original
            history = grouped[original]
            metadata = provenance[history[0].snapshot_id]
            self.assets.append(
                Asset(
                    asset_id=asset_id,
                    asset_type=metadata["asset_type"],
                    created_at=history[0].event_time,
                    metadata={"dataset_category": self.profile, "source": metadata["source"]},
                )
            )
            self.groups.append((asset_id, history))
        self.expected_snapshots = sum(len(history) for _, history in self.groups)
        self.expected_diffs = self.expected_snapshots - len(self.groups)
        self.manifest.update({"assets": len(self.assets), "snapshots": self.expected_snapshots})
        self.provenance_manifest = {"sources": manifests, **self.manifest}
        self.logical_days = (
            max(item.event_time for item in snapshots) - min(item.event_time for item in snapshots)
        ).total_seconds() / 86400
        self.start_time = min(item.event_time for item in snapshots)
        self._used = False

    def records(self):
        import heapq

        if self._used:
            raise ValueError("History is single-pass; construct another instance for replay")
        self._used = True
        queue = []
        for index, (_, history) in enumerate(self.groups):
            heapq.heappush(queue, (history[0].event_time, index, 0))
        while queue:
            _, index, position = heapq.heappop(queue)
            asset_id, history = self.groups[index]
            original = history[position]
            snapshot = (
                original
                if asset_id == original.asset_id
                else original.model_copy(
                    update={
                        "asset_id": asset_id,
                        "snapshot_id": "snapshot-"
                        + str(uuid5(NAMESPACE_URL, asset_id + ":" + original.snapshot_id)),
                    }
                )
            )
            event = ChangeEvent(
                event_id=f"event-{uuid5(NAMESPACE_URL, snapshot.snapshot_id)}",
                asset_id=asset_id,
                event_type="config_observed",
                source=self.profile,
                event_time=snapshot.event_time,
                ingest_time=snapshot.ingest_time,
                metadata={"snapshot_id": snapshot.snapshot_id},
            )
            yield (
                event,
                snapshot,
                {
                    "profile": self.profile,
                    "source_snapshot_id": original.snapshot_id,
                    "asset_id": asset_id,
                    "version": snapshot.version,
                },
            )
            if position + 1 < len(history):
                heapq.heappush(queue, (history[position + 1].event_time, index, position + 1))
