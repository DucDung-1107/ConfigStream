import hashlib
import json
import random
import re
from collections import Counter, defaultdict
from urllib.parse import urlsplit

from workloads.empirical import percentile
from workloads.terrads import RECORD, database, write_json

SAMPLING_POLICY = "terrads-source-bytes-stratified-v1"


def normalize_url(value):
    if not isinstance(value, str):
        raise ValueError("Repository origin must be a string")
    value = value.strip()
    if value.startswith("git@github.com:"):
        value = "https://github.com/" + value.split(":", 1)[1]
    parsed = urlsplit(value)
    if (
        parsed.scheme not in {"https", "http", "git", "ssh"}
        or parsed.hostname != "github.com"
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parsed.port not in {None, 443, 80}
    ):
        raise ValueError("Only credential-free GitHub origins from TerraDS are allowed")
    path = parsed.path.strip("/").removesuffix(".git")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", path):
        raise ValueError("Invalid repository origin")
    return "https://github.com/" + path.lower()


def population(root):
    sizes = json.loads((root / "repository-source-bytes.json").read_text())
    with database(root) as connection:
        rows = connection.execute("""
            SELECT r.*, COALESCE(m.modules,0) AS ModuleCount,
              COALESCE(m.resources,0) AS ResourceCount
            FROM Repositories r LEFT JOIN
            (SELECT m.RepositoryId, COUNT(DISTINCT m.Id) AS modules,
               COUNT(rr.Id) AS resources FROM Modules m
             LEFT JOIN Resources rr ON rr.ModuleId=m.Id GROUP BY m.RepositoryId) m
             ON m.RepositoryId=r.Id ORDER BY r.Id
        """).fetchall()
    origins = {}
    excluded = Counter()
    for row in rows:
        entry = dict(row)
        try:
            origin = normalize_url(entry["CloneUrl"])
        except ValueError:
            excluded["invalid_origin"] += 1
            continue
        if origin in origins:
            excluded["duplicate_origin"] += 1
            continue
        origins[origin] = {
            "id": entry["Id"],
            "url": origin,
            "end_sha": entry["LatestCommitSha"],
            "reported_license": entry["License"],
            "module_count": entry["ModuleCount"],
            "resource_count": entry["ResourceCount"],
            "repository_size_kb": entry["SizeInKb"],
            "source_bytes": sizes.get(str(entry["Id"]), 0),
            "latest_commit_at": entry["LatestCommitAt"],
        }
    return sorted(origins.values(), key=lambda entry: entry["url"]), dict(excluded)


def sample(population, size=100, seed=42):
    if not 3 <= size <= len(population):
        raise ValueError("Sample requires 3 <= size <= population")
    values = [entry["source_bytes"] for entry in population]
    thresholds = [percentile(values, 1 / 3), percentile(values, 2 / 3)]
    strata = defaultdict(list)
    for entry in population:
        index = sum(entry["source_bytes"] > threshold for threshold in thresholds)
        strata[f"size-{index}"].append(entry)
    allocations = {
        name: max(1, int(size * len(items) / len(population))) for name, items in strata.items()
    }
    while sum(allocations.values()) < size:
        selected = max(
            strata,
            key=lambda name: (size * len(strata[name]) / len(population) - allocations[name], name),
        )
        allocations[selected] += 1
    while sum(allocations.values()) > size:
        selected = max(
            (name for name in strata if allocations[name] > 1),
            key=lambda name: allocations[name] - size * len(strata[name]) / len(population),
        )
        allocations[selected] -= 1
    selected = []
    for name, items in sorted(strata.items()):
        ordered = sorted(
            items,
            key=lambda entry: hashlib.sha256(
                f"{SAMPLING_POLICY}:{seed}:{entry['url']}".encode()
            ).hexdigest(),
        )
        selected.extend(
            {**entry, "stratum": name, "rank": index + 1}
            for index, entry in enumerate(ordered[: allocations[name]])
        )
    return {
        "policy": SAMPLING_POLICY,
        "seed": seed,
        "population_size": len(population),
        "sample_size": size,
        "source_byte_thresholds": thresholds,
        "strata": {
            name: {
                "population": len(items),
                "sample": allocations[name],
                "inclusion_probability": allocations[name] / len(items),
            }
            for name, items in sorted(strata.items())
        },
        "selected": selected,
    }


def estimate(manifest, rows, *, repetitions=2000, seed=42):
    expected = {entry["id"] for entry in manifest["selected"]}
    if {row["id"] for row in rows} != expected or len(rows) != len(expected):
        raise ValueError("Estimator requires all sampled statuses, without replacements")
    metrics = [
        "reachable",
        "license_accepted",
        "accepted",
        "assets",
        "revisions",
        "adjacent_pairs",
        "bytes",
    ]
    groups = defaultdict(list)
    strata_by_id = {entry["id"]: entry["stratum"] for entry in manifest["selected"]}
    for row in rows:
        groups[strata_by_id[row["id"]]].append(row)
    randomizer = random.Random(seed)
    estimates = {}
    for metric in metrics:
        point = 0
        simulated = [0.0] * repetitions
        for name, entries in sorted(groups.items()):
            count = manifest["strata"][name]["population"]
            if len(entries) == 1 and count > 1:
                raise ValueError("Bootstrap needs >=2 sampled units per non-census stratum")
            values = [
                max(0, row.get("revisions", 0) - row.get("assets", 0))
                if metric == "adjacent_pairs"
                else row.get(metric, 0)
                for row in entries
            ]
            mean = sum(values) / len(values)
            point += count * mean
            correction = (
                ((1 - len(values) / count) * len(values) / (len(values) - 1)) ** 0.5
                if len(values) > 1
                else 0
            )
            for index in range(repetitions):
                sampled_mean = sum(randomizer.choices(values, k=len(values))) / len(values)
                simulated[index] += count * (mean + correction * (sampled_mean - mean))
        estimates[metric] = {
            "point": point,
            "ci95": [max(0, percentile(simulated, 0.025)), percentile(simulated, 0.975)],
        }
    return {
        "policy": "stratified-repository-bootstrap-fpc-v1",
        "sample_size": len(rows),
        "population_size": manifest["population_size"],
        "seed": seed,
        "repetitions": repetitions,
        "stratum_weights": {
            name: group["population"] / group["sample"]
            for name, group in manifest["strata"].items()
        },
        "estimates": estimates,
        "estimand": "recoverable accepted history under pinned extraction/filter/censor policy",
        "unavailable_policy": "zero yield; no replacement, not estimated underlying history",
        "limitations": [
            "conditional on extraction caps and filters",
            "heavy tails may dominate",
            "bootstrap does not recover unseen extremes or inaccessible histories",
        ],
    }


def save_sample(root, results, size, seed):
    entries, excluded = population(root)
    manifest = sample(entries, size, seed)
    manifest["population_exclusions"] = excluded
    source = json.loads((results / "source-manifest.json").read_text())
    manifest["dataset_record"] = RECORD
    manifest["source_artifact_sha256"] = {
        entry["name"]: entry["sha256"] for entry in source["files"]
    }
    manifest["population_frame_sha256"] = hashlib.sha256(
        json.dumps(entries, sort_keys=True).encode()
    ).hexdigest()
    write_json(results / f"sample-{size}.json", manifest)
    write_json(results / "sample-manifest.json", manifest)
    return manifest
