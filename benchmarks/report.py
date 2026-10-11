import argparse
import csv
import html
import json
import re
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path


def summary(result: dict) -> dict:
    publisher = result.get("publisher", {})
    lake = result.get("lake", {}) or {}
    metrics = result.get("metrics", {})
    elapsed = result.get("streaming_seconds")
    accepted = lake.get("counts", {}).get("normalized_snapshot", 0)
    return {
        "run": result["benchmark_id"],
        "status": result["status"],
        "assets": result["config"]["assets"],
        "master": result["config"].get("master"),
        "driver_memory": result["config"].get("driver_memory", "1g"),
        "max_offsets": result["config"].get("max_offsets", 10000),
        "workload_policy": result.get("runtime", {}).get("workload_policy", "unrecorded"),
        "snapshots": publisher.get("snapshots", 0),
        "accepted_snapshots": accepted,
        "logical_config_bytes": publisher.get("logical_raw_config_bytes", 0),
        "target_rate": result["config"]["rate"],
        "producer_rate": publisher.get("achieved_snapshots_per_second"),
        "logical_processed_rate": accepted / elapsed if elapsed else None,
        "spark_reported_processed_rate": metrics.get("processed_rows_per_second_mean"),
        "batch_p95_ms": metrics.get("batch_p95_ms"),
        "streaming_seconds": elapsed,
        "parquet_bytes": lake.get("parquet_bytes"),
        "max_state_rows": metrics.get("max_state_rows"),
        "max_state_memory_bytes": metrics.get("max_state_memory_bytes"),
        "peak_lag": max(
            (sample["lag"] for sample in result.get("lag_samples", [])),
            default=result.get("sample_summary", {}).get("peak_lag"),
        ),
        "rows_dropped_by_watermark": metrics.get("rows_dropped_by_watermark"),
        "analytics_seconds": sum(
            entry["seconds"] for entry in result.get("analytics", {}).get("datasets", {}).values()
        )
        if result.get("analytics")
        else None,
    }


def resource_summary(samples: list[dict]) -> dict:
    rss = []
    cpu = []
    containers = {}
    for sample in samples:
        values = (sample.get("spark_process") or "").split()
        if len(values) == 2:
            rss.append(int(values[0]) * 1024)
            cpu.append(float(values[1]))
        for line in (sample.get("containers") or "").splitlines():
            try:
                entry = json.loads(line)
                memory = re.fullmatch(
                    r"([0-9.]+)\s*([KMGT]?)(i?)B", entry["MemUsage"].split("/")[0].strip()
                )
                if memory is None:
                    continue
                base = 1024 if memory[3] else 1000
                used = float(memory[1]) * base ** " KMGT".index(memory[2] or " ")
                peak = containers.setdefault(
                    entry["Name"], {"max_cpu_percent": 0, "max_memory_bytes": 0}
                )
                peak["max_cpu_percent"] = max(
                    peak["max_cpu_percent"], float(entry["CPUPerc"].removesuffix("%"))
                )
                peak["max_memory_bytes"] = max(peak["max_memory_bytes"], int(used))
            except (ValueError, KeyError, TypeError):
                continue
    return {
        "java_max_rss_bytes": max(rss) if rss else None,
        "java_max_ps_cpu_percent": max(cpu) if cpu else None,
        "samples": len(samples),
        "container_peaks": containers,
    }


def refresh_failed_storage(source: Path):
    from benchmarks.main import committed_metrics
    from lake.config import LakeConfig

    for path in source.glob("*/result.json"):
        result = json.loads(path.read_text())
        metadata = path.parent / "checkpoint/metadata"
        if result["status"] != "failed" or result.get("lake") is not None or not metadata.exists():
            continue
        query_id = json.loads(metadata.read_text())["id"]
        lake = replace(LakeConfig.from_env(), prefix=result["runtime"]["lake_prefix"])
        result["lake"] = committed_metrics(lake, query_id)
        result["query_id"] = query_id
        result["storage_observed_after_failure_at"] = datetime.now(UTC).isoformat()
        path.write_text(json.dumps(result, indent=2) + "\n")


def plot(rows: list[dict], x_field: str, y_field: str, filename: Path, title: str):
    points = [
        (row[x_field], row[y_field], row["run"])
        for row in rows
        if row.get(x_field) is not None and row.get(y_field) is not None
    ]
    if not points:
        return
    width, height = 720, 360
    max_x = max(value[0] for value in points) or 1
    max_y = max(value[1] for value in points) or 1
    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        f'<text x="70" y="25" font-family="sans-serif" font-size="16">{html.escape(title)}</text>',
        '<path d="M70 50 V290 H680" fill="none" stroke="black"/>',
    ]
    for fraction in (0, 0.25, 0.5, 0.75, 1):
        position_x = 70 + fraction * 610
        position_y = 290 - fraction * 230
        svg.extend(
            [
                f'<text x="{position_x}" y="310" font-size="11" text-anchor="middle">'
                f"{max_x * fraction:.1f}</text>",
                f'<text x="5" y="{position_y}" font-size="11">{max_y * fraction:.1f}</text>',
            ]
        )
    for value_x, value_y, name in points:
        svg.append(
            f'<circle cx="{70 + value_x / max_x * 610}" '
            f'cy="{290 - value_y / max_y * 230}" r="4" fill="#125cb3">'
            f"<title>{html.escape(name)}</title></circle>"
        )
    svg.extend(
        [
            f'<text x="210" y="345" font-size="12">{html.escape(x_field)}</text>',
            f'<text x="75" y="45" font-size="12">{html.escape(y_field)}</text>',
            "</svg>",
        ]
    )
    filename.write_text("\n".join(svg) + "\n")


def build_report(source: Path, destination: Path) -> list[dict]:
    destination.mkdir(parents=True, exist_ok=True)
    rows = []
    paths = sorted(source.glob("*/result.json")) or sorted(source.glob("*.json"))
    results = [json.loads(path.read_text()) for path in paths]
    measured = [
        result for result in results if isinstance(result, dict) and "benchmark_id" in result
    ]
    for result in sorted(measured, key=lambda entry: entry["benchmark_id"]):
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,100}", result["benchmark_id"]):
            raise ValueError("Unsafe benchmark result identity")
        rows.append(summary(result))
        safe = {
            key: value
            for key, value in result.items()
            if key not in {"progress", "resource_samples", "lag_samples"}
        }
        if "resource_samples" in result or "lag_samples" in result:
            safe["sample_summary"] = {
                "resource_samples": len(result.get("resource_samples", [])),
                "lag_samples": len(result.get("lag_samples", [])),
                "peak_lag": rows[-1]["peak_lag"],
                **resource_summary(result.get("resource_samples", [])),
            }
        (destination / f"{result['benchmark_id']}.json").write_text(
            json.dumps(safe, indent=2) + "\n"
        )
    if not rows:
        raise ValueError("No measured result files found")
    with (destination / "summary.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    groups = {}
    for row in rows:
        key = tuple(
            row[field]
            for field in (
                "assets",
                "snapshots",
                "master",
                "driver_memory",
                "max_offsets",
                "workload_policy",
            )
        )
        groups.setdefault(key, []).append(row)
    rate_rows = max(
        groups.values(), key=lambda group: (len({row["target_rate"] for row in group}), len(group))
    )
    plot(
        rate_rows,
        "target_rate",
        "logical_processed_rate",
        destination / "throughput.svg",
        "Accelerated replay: accepted snapshots / streaming wall time",
    )
    plot(
        rate_rows,
        "target_rate",
        "batch_p95_ms",
        destination / "batch-duration.svg",
        "Micro-batch trigger duration (not event-time latency)",
    )
    plot(
        rows,
        "logical_config_bytes",
        "analytics_seconds",
        destination / "batch-analytics.svg",
        "Committed-history analytics runtime (eight persisted jobs)",
    )
    return rows


def main():
    parser = argparse.ArgumentParser(description="Summarize actual local experiment files")
    parser.add_argument("--input", type=Path, default=Path(".cache/benchmarks"))
    parser.add_argument("--output", type=Path, default=Path("benchmarks/results"))
    parser.add_argument("--refresh-failed-storage", action="store_true")
    args = parser.parse_args()
    if args.refresh_failed_storage:
        refresh_failed_storage(args.input)
    print(json.dumps(build_report(args.input, args.output), indent=2))


if __name__ == "__main__":
    main()
