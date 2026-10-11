import csv
import html
import json
import math
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

from processing.diff import compare_snapshots
from processing.models import NormalizedConfig
from processing.normalization import normalize_snapshot

STAT_VERSION = "empirical-v1"


def percentile(values, probability):
    if not values:
        return None
    if not 0 <= probability <= 1:
        raise ValueError("Percentile must lie between zero and one")
    histogram = values if isinstance(values, Counter) else Counter(values)
    position = (sum(histogram.values()) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    cumulative = 0
    lower_value = None
    for value, count in sorted(histogram.items()):
        cumulative += count
        if lower_value is None and cumulative > lower:
            lower_value = value
        if cumulative > upper:
            return lower_value + (value - lower_value) * (position - lower)


def distribution(values):
    values = values if isinstance(values, Counter) else Counter(values)
    count = sum(values.values())
    return {
        "count": count,
        "min": min(values) if values else None,
        "mean": sum(value * weight for value, weight in values.items()) / count if count else None,
        "max": max(values) if values else None,
        **{
            f"p{int(probability * 100)}": percentile(values, probability)
            for probability in (0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99)
        },
        "histogram": [[value, count] for value, count in sorted(Counter(values).items())],
    }


def period_key(timestamp, period):
    day = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
    if period == "day":
        return day
    if period == "week":
        return day - timedelta(days=day.weekday())
    if period == "month":
        return day.replace(day=1)
    raise ValueError("Period must be day, week or month")


def next_period(timestamp, period):
    if period == "month":
        return (
            timestamp.replace(year=timestamp.year + 1, month=1)
            if timestamp.month == 12
            else timestamp.replace(month=timestamp.month + 1)
        )
    return timestamp + timedelta(days=7 if period == "week" else 1)


def measure(snapshots, provenance, label="combined", *, weights=None, bundle_by_repository=False):
    grouped = defaultdict(list)
    for snapshot in snapshots:
        grouped[snapshot.asset_id].append(snapshot)
    weights = dict.fromkeys(grouped, 1) if weights is None else weights
    if any(type(value) is not int or value < 1 for value in weights.values()):
        raise ValueError("History multiplicities must be positive integers")
    versions = Counter()
    sizes = Counter()
    baseline_sizes = Counter()
    intervals = Counter()
    raw_changed = Counter()
    added = Counter()
    removed = Counter()
    changed = Counter()
    revert_seconds = Counter()
    reverting_assets = set()
    unchanged_normalized = 0
    bundles = defaultdict(lambda: {"assets": set(), "bytes": 0, "changed": 0, "timestamp": None})
    correlated = {seconds: defaultdict(set) for seconds in (300, 3600, 86400)}
    period_counts = {period: Counter() for period in ("day", "week", "month")}
    ordinal_inversions = 0
    for asset_id, history in sorted(grouped.items()):
        weight = weights.get(asset_id, 0)
        if not weight:
            continue
        history.sort(key=lambda snapshot: snapshot.version)
        versions[len(history)] += weight
        baseline_sizes[len(history[0].content.encode("utf-8"))] += weight
        observed_end = datetime.fromisoformat(
            provenance[history[-1].snapshot_id]["observation_end"]
        ).astimezone(UTC)
        per_period = {period: Counter() for period in period_counts}
        hashes = {}
        previous = None
        previous_normalized = None
        for snapshot in history:
            metadata = provenance[snapshot.snapshot_id]
            sizes[len(snapshot.content.encode("utf-8"))] += weight
            normalized = normalize_snapshot(snapshot)
            magnitude = 0
            if previous is not None:
                diff = compare_snapshots(previous_normalized, normalized).diff
                raw_diff = compare_snapshots(
                    NormalizedConfig(
                        previous.asset_id, previous.snapshot_id, previous.version, previous.content
                    ),
                    NormalizedConfig(
                        snapshot.asset_id, snapshot.snapshot_id, snapshot.version, snapshot.content
                    ),
                ).diff
                added[diff.added] += weight
                removed[diff.removed] += weight
                changed[diff.changed] += weight
                raw_changed[raw_diff.changed] += weight
                magnitude = diff.added + diff.removed
                unchanged_normalized += weight * (
                    normalized.normalized_hash == previous_normalized.normalized_hash
                )
                if not provenance[previous.snapshot_id]["boundary_baseline"]:
                    intervals[(snapshot.event_time - previous.event_time).total_seconds()] += weight
                if snapshot.hash in hashes:
                    reverting_assets.add(asset_id)
                    revert_seconds[(snapshot.event_time - previous.event_time).total_seconds()] += (
                        weight
                    )
                for period in period_counts:
                    per_period[period][period_key(snapshot.event_time, period)] += 1
            if not metadata["boundary_baseline"]:
                bundle_key = (
                    metadata["repository"] + ":" + metadata["git_commit"]
                    if bundle_by_repository
                    else metadata["git_commit"]
                )
                bundle = bundles[bundle_key]
                bundle["assets"].add(asset_id)
                bundle["bytes"] += weight * len(snapshot.content.encode("utf-8"))
                bundle["changed"] += weight * magnitude
                bundle["timestamp"] = snapshot.event_time.isoformat()
                for seconds, windows in correlated.items():
                    windows[int(snapshot.event_time.timestamp()) // seconds].add(asset_id)
            hashes[snapshot.hash] = snapshot.event_time
            previous, previous_normalized = snapshot, normalized
        for period in period_counts:
            cursor = period_key(history[0].event_time, period)
            final = period_key(observed_end, period)
            while cursor <= final:
                period_counts[period][per_period[period][cursor]] += weight
                cursor = next_period(cursor, period)
        original_orders = [
            provenance[item.snapshot_id].get("commit_ordinal", item.version) for item in history
        ]
        ordinal_inversions += weight * sum(
            current < previous
            for previous, current in zip(original_orders, original_orders[1:], strict=False)
        )
    selected = [snapshot for snapshot in snapshots if weights.get(snapshot.asset_id, 0)]
    times = [snapshot.event_time for snapshot in selected]
    revisions = sum(sizes.values())
    unique = len({snapshot.hash for snapshot in selected})

    def asset_count(assets):
        return sum(weights.get(asset_id, 0) for asset_id in assets)

    return {
        "stat_version": STAT_VERSION,
        "label": label,
        "assets": sum(versions.values()),
        "revisions": revisions,
        "unique_contents": unique,
        "unique_content_ratio": unique / revisions,
        "logical_content_bytes": sum(value * weight for value, weight in sizes.items()),
        "start": min(times).isoformat(),
        "end": max(times).isoformat(),
        "history_days": (max(times) - min(times)).total_seconds() / 86400,
        "config_size_bytes": distribution(sizes),
        "baseline_size_bytes": distribution(baseline_sizes),
        "versions_per_asset": distribution(versions),
        "version_bins": {
            "1": versions[1],
            "2-5": sum(count for value, count in versions.items() if 2 <= value <= 5),
            "6-20": sum(count for value, count in versions.items() if 6 <= value <= 20),
            "21-100": sum(count for value, count in versions.items() if 21 <= value <= 100),
            ">100": sum(count for value, count in versions.items() if value > 100),
        },
        "inter_change_seconds": distribution(intervals),
        "changes_per_period": {
            period: distribution(values) for period, values in period_counts.items()
        },
        "diff": {
            "added": distribution(added),
            "removed": distribution(removed),
            "changed": distribution(changed),
            "raw_changed": distribution(raw_changed),
            "normalized_equal_pairs": unchanged_normalized,
        },
        "reverts": {
            "count": sum(revert_seconds.values()),
            "assets": asset_count(reverting_assets),
            "seconds_since_previous_change": distribution(revert_seconds),
        },
        "timestamp_order_inversions": ordinal_inversions,
        "correlated_files": {
            str(seconds): distribution([asset_count(assets) for assets in windows.values()])
            for seconds, windows in correlated.items()
        },
        "commit_bundles": {
            "files": distribution([asset_count(bundle["assets"]) for bundle in bundles.values()]),
            "revision_bytes": distribution([bundle["bytes"] for bundle in bundles.values()]),
            "total_added_removed": distribution([bundle["changed"] for bundle in bundles.values()]),
            "largest": [
                {
                    "commit": commit,
                    "timestamp": bundle["timestamp"],
                    "files": asset_count(bundle["assets"]),
                }
                for commit, bundle in sorted(
                    bundles.items(), key=lambda item: (-asset_count(item[1]["assets"]), item[0])
                )[:5]
            ],
        },
    }


def write_distribution_plot(curves, destination: Path, name):
    maximum = max((stats["max"] or 0 for _, stats in curves), default=1) or 1
    wrapped_legend = len(curves) > 3
    height = 380 if not wrapped_legend else 380 + ((len(curves) - 1) // 2) * 24
    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="760" height="{height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        f'<text x="60" y="25">{html.escape(name)}: empirical CDF (log1p x)</text>',
        '<path d="M60 50 V300 H720" fill="none" stroke="black"/>',
    ]
    for fraction in (0, 0.25, 0.5, 0.75, 1):
        svg.append(f'<text x="5" y="{300 - fraction * 250}">{fraction:g}</text>')
        value = math.expm1(fraction * math.log1p(maximum))
        svg.append(
            f'<text x="{60 + fraction * 660}" y="325" text-anchor="middle">{value:.1f}</text>'
        )
    for index, (label, stats) in enumerate(curves):
        color = ["#125cb3", "#b83b20", "#268443", "#743db0", "#9a6500", "#17696a"][index % 6]
        legend_x = 60 + (index % 2) * 350 if wrapped_legend else 60 + index * 220
        legend_y = 365 + (index // 2) * 24 if wrapped_legend else 365
        cumulative = 0
        points = ["60.000,300.000"]
        for value, count in stats["histogram"]:
            horizontal = 60 + math.log1p(value) / math.log1p(maximum) * 660
            points.append(f"{horizontal:.3f},{300 - cumulative / stats['count'] * 250:.3f}")
            cumulative += count
            points.append(f"{horizontal:.3f},{300 - cumulative / stats['count'] * 250:.3f}")
        svg.extend(
            [
                f'<polyline points="{" ".join(points)}" fill="none" stroke="{color}"/>',
                f'<text x="{legend_x}" y="{legend_y}" fill="{color}">'
                f"{html.escape(label)} (n={stats['count']})</text>",
            ]
        )
    svg.append("</svg>")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text("\n".join(svg) + "\n")


def write_report(summaries, destination: Path):
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "observed-summary.json").write_text(json.dumps(summaries, indent=2) + "\n")
    fields = [
        "source",
        "assets",
        "revisions",
        "history_days",
        "content_bytes",
        "median_file_bytes",
        "median_versions",
        "median_change_interval_hours",
        "median_changed_lines",
        "p95_changed_lines",
        "max_correlated_files",
    ]
    rows = []
    for label, summary in summaries.items():
        interval = summary["inter_change_seconds"]["p50"]
        rows.append(
            dict(
                zip(
                    fields,
                    [
                        label,
                        summary["assets"],
                        summary["revisions"],
                        summary["history_days"],
                        summary["logical_content_bytes"],
                        summary["config_size_bytes"]["p50"],
                        summary["versions_per_asset"]["p50"],
                        interval / 3600 if interval is not None else None,
                        summary["diff"]["changed"]["p50"],
                        summary["diff"]["changed"]["p95"],
                        summary["correlated_files"]["3600"]["max"],
                    ],
                    strict=True,
                )
            )
        )
    with (destination / "source-summary.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    metrics = {
        "configuration-size": lambda item: item["config_size_bytes"],
        "versions-per-asset": lambda item: item["versions_per_asset"],
        "inter-change-seconds": lambda item: item["inter_change_seconds"],
        "diff-changed-lines": lambda item: item["diff"]["changed"],
        "correlated-files-hour": lambda item: item["correlated_files"]["3600"],
    }
    for name, getter in metrics.items():
        curves = [
            (label, getter(summary)) for label, summary in summaries.items() if label != "combined"
        ]
        write_distribution_plot(curves, destination / f"{name}.svg", name)
    lines = [
        "# Measured public Git history",
        "",
        "Generated from empirical-v1 summaries; no configuration bodies.",
        "",
        "| Source | Assets | Revisions | Bytes | Median bytes | Median versions |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    lines.extend(
        f"| {row['source']} | {row['assets']} | {row['revisions']} | {row['content_bytes']} "
        f"| {row['median_file_bytes']} | {row['median_versions']} |"
        for row in rows
    )
    (destination / "summary.md").write_text("\n".join(lines) + "\n")
