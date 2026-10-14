"""Opt-in, bounded educational execution with plans and independent owned expectations."""

import argparse
import json
import subprocess
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from time import perf_counter

from spark.common.config import SparkConfig
from spark.common.runtime import create_session
from spark.competency.examples import FIXTURE, run_examples, stream_modes, verify
from workloads.rpki_resources import ResourceSampler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    started = perf_counter()
    receipt = {
        "evidence": "MEASURED_OWNED_SPARK",
        "status": "RUNNING",
        "code_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "started_at": datetime.now(UTC).isoformat(),
        "spark_version": "4.0.2",
        "master": "local[2]",
        "driver_heap": "2g",
        "fixture_sha256": sha256(FIXTURE.read_bytes()).hexdigest(),
        "production_paths_changed": False,
        "private_sources_read": False,
        "task_peak_execution_memory": "NOT_MEASURED",
    }
    config = SparkConfig(master="local[2]", driver_memory="2g", runtime_dir=args.output / "runtime")
    session = create_session(config, kafka=False)
    resources = ResourceSampler(args.output, session)
    try:
        with resources:
            examples = run_examples(session, args.output / "examples")
            modes = stream_modes(session, args.output / "modes")
            verify(examples, modes)
            resources.check()
        receipt.update(status="PASS", examples=examples, output_modes=modes)
    except Exception as error:
        receipt.update(status="FAIL", failure_category=type(error).__name__, error=str(error))
        raise
    finally:
        receipt["elapsed_including_startup_seconds"] = perf_counter() - started
        receipt["resource_samples"] = resources.samples
        receipt["resource_failure"] = resources.failure
        receipt["peak_sampled_process_tree_rss_bytes"] = max(
            (row["process_tree_rss_bytes"] for row in resources.samples), default=None
        )
        receipt["completed_at"] = datetime.now(UTC).isoformat()
        (args.output / "receipt.json").write_text(json.dumps(receipt, indent=2))
        session.stop()


if __name__ == "__main__":
    main()
