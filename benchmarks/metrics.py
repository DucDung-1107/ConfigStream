import json
import os
import platform
import subprocess
import sys
from statistics import mean


def logical_config_bytes(content: str) -> int:
    return len(content.encode("utf-8"))


def command(arguments: list[str]) -> str | None:
    try:
        result = subprocess.run(arguments, capture_output=True, text=True, timeout=10)
        return (result.stdout + result.stderr).strip() if result.returncode == 0 else None
    except (OSError, subprocess.TimeoutExpired):
        return None


def machine_metadata() -> dict:
    ram = command(["sysctl", "-n", "hw.memsize"])
    if ram is None:
        try:
            ram = str(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES"))
        except (ValueError, OSError):
            ram = None
    return {
        "os": platform.platform(),
        "architecture": platform.machine(),
        "cpu_cores": os.cpu_count(),
        "cpu_model": command(["sysctl", "-n", "machdep.cpu.brand_string"]),
        "ram_bytes": int(ram) if ram is not None else None,
        "python": sys.version.split()[0],
        "java": command(["java", "-version"]),
        "spark": "4.0.2",
        "kafka": "4.1.2",
        "docker": command(["docker", "--version"]),
        "docker_engine": command(
            [
                "docker",
                "info",
                "--format",
                "{{json .NCPU}} CPUs; {{json .MemTotal}} RAM bytes; {{.Driver}}",
            ]
        ),
        "storage": "local single-machine storage; physical device not probed",
    }


def summarize_progress(progress: list[dict]) -> dict:
    active = [entry for entry in progress if entry.get("numInputRows", 0) > 0]
    durations = sorted(entry.get("durationMs", {}).get("triggerExecution", 0) for entry in active)
    operators = [operator for entry in progress for operator in entry.get("stateOperators", [])]
    return {
        "batches": len(active),
        "spark_reported_input_rows": sum(entry.get("numInputRows", 0) for entry in active),
        "processed_rows_per_second_mean": mean(
            entry.get("processedRowsPerSecond", 0) for entry in active
        )
        if active
        else None,
        "input_rows_per_second_mean": mean(entry.get("inputRowsPerSecond", 0) for entry in active)
        if active
        else None,
        "batch_p50_ms": durations[(len(durations) - 1) // 2] if durations else None,
        "batch_p95_ms": durations[min(len(durations) - 1, int(len(durations) * 0.95))]
        if durations
        else None,
        "max_state_rows": max((entry.get("numRowsTotal", 0) for entry in operators), default=0),
        "max_state_memory_bytes": max(
            (entry.get("memoryUsedBytes", 0) for entry in operators), default=0
        ),
        "rows_dropped_by_watermark": sum(
            entry.get("numRowsDroppedByWatermark", 0) for entry in operators
        ),
        "watermark": progress[-1].get("eventTime", {}).get("watermark") if progress else None,
        "duplicate_rows_dropped": sum(
            entry.get("customMetrics", {}).get("numDroppedDuplicateRows", 0) for entry in operators
        )
        if any("numDroppedDuplicateRows" in entry.get("customMetrics", {}) for entry in operators)
        else None,
    }


def consumed_offsets(progress: dict) -> dict[str, int]:
    result = {}
    for source in progress.get("sources", []):
        offset = source.get("endOffset", {})
        if isinstance(offset, str):
            try:
                offset = json.loads(offset)
            except json.JSONDecodeError:
                continue
        for partition, value in (offset or {}).get("config.snapshots", {}).items():
            result[str(partition)] = max(value, result.get(str(partition), 0))
    return result


def lag(latest: dict[str, int], consumed: dict[str, int]) -> int:
    return sum(max(0, offset - consumed.get(partition, 0)) for partition, offset in latest.items())
