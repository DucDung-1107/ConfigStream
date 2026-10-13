"""Read-only accepted Terraform lookup/ordinal-edge reference; no Git mining."""

import csv
import hashlib
import json
from collections import defaultdict
from pathlib import Path
from time import monotonic

from benchmarks.evaluation import statistics, summary_digest


def measure():
    edges_path = Path("workloads/results/terrads/ground-truth-edges.csv")
    with edges_path.open() as stream:
        edges = list(csv.DictReader(stream))
    expected = {
        (
            item["asset_id"],
            int(item["old_version"]),
            int(item["new_version"]),
            item["old_snapshot_id"],
            item["new_snapshot_id"],
        )
        for item in edges
    }
    target = min(expected)[3]
    started = monotonic()
    snapshots = {}
    grouped = defaultdict(list)
    logical_bytes = 0
    for path in sorted(Path(".cache/datasets/terrads/history").glob("*/histories.jsonl")):
        with path.open() as stream:
            for line in stream:
                row = json.loads(line)
                identity = row["snapshot_id"]
                if identity in snapshots:
                    raise ValueError("Duplicate accepted version")
                content = row["content"].encode()
                if hashlib.sha256(content).hexdigest() != row["hash"]:
                    raise ValueError("Accepted content hash mismatch")
                logical_bytes += len(content)
                snapshots[identity] = row
                grouped[row["asset_id"]].append(row)
    actual = set()
    for asset, versions in grouped.items():
        versions.sort(key=lambda row: row["version"])
        if [row["version"] for row in versions] != list(range(1, len(versions) + 1)):
            raise ValueError("Ordinal version gap")
        actual.update(
            (asset, old["version"], new["version"], old["snapshot_id"], new["snapshot_id"])
            for old, new in zip(versions, versions[1:], strict=False)
        )
    reference = json.loads(Path("workloads/results/terrads/history-summary.json").read_text())
    if (len(snapshots), len(grouped), logical_bytes) != (
        reference["revisions"],
        reference["assets"],
        reference["logical_content_bytes"],
    ) or actual != expected:
        raise ValueError("Accepted Terraform history changed")
    preparation = monotonic() - started
    times, outputs = [], []
    for _ in range(3):
        started = monotonic()
        row = snapshots[target]
        asset = row["asset_id"]
        output = dict(
            content_sha256=hashlib.sha256(row["content"].encode()).hexdigest(),
            versions=[item["version"] for item in grouped[asset]],
            adjacent_edges=sorted(edge for edge in actual if edge[0] == asset),
        )
        times.append(monotonic() - started)
        outputs.append(summary_digest(output))
    if len(set(outputs)) != 1:
        raise ValueError("Lookup changed across repetitions")
    return dict(
        evidence="MEASURED_LOCAL_REFERENCE",
        accepted_versions=len(snapshots),
        incarnations=len(grouped),
        adjacent_pairs=len(actual),
        logical_utf8_history_bytes=logical_bytes,
        missing_edges=0,
        unexpected_edges=0,
        query_parameter="old snapshot of first lexicographic accepted ordinal edge",
        query="content hash + version and adjacent-pair metadata for selected incarnation",
        preparation_seconds=preparation,
        warm_query_seconds=statistics(times),
        output_sha256=outputs[0],
        correctness="PASS",
        re_mining=False,
        timing="load/validation separately; reused-process lookup cycles, OS cache uncontrolled",
        limitations="No raw-from-normalized reconstruction, deployments or speed ranking",
    )


if __name__ == "__main__":
    print(json.dumps(measure(), sort_keys=True))
