"""Summarize all approved pilot attempts without ranking non-equivalent production work."""

import argparse
import json
from pathlib import Path

from benchmarks.evaluation import statistics
from workloads.rpki_resources import directory_bytes


def summarize(root):
    campaign = json.loads((root / "campaign.json").read_text())
    launches = {
        (trial["configuration"], trial["repetition"]): trial for trial in campaign["trials"]
    }
    trials = []
    groups = {}
    for path in sorted(root.glob("*/result.json")):
        result = json.loads(path.read_text())
        resources = json.loads((path.parent / "resources.json").read_text())
        result["peak_sampled_process_tree_rss_bytes"] = max(
            sample["process_tree_rss_bytes"] for sample in resources["host_samples"]
        )
        result["minimum_guest_available_memory_bytes"] = min(
            sample["guest_available_memory_bytes"] for sample in resources["guest_samples"]
        )
        result["minimum_guest_free_disk_bytes"] = min(
            sample["guest_free_disk_bytes"] for sample in resources["guest_samples"]
        )
        result["resource_guard_failures"] = [resources["host_failure"], resources["guest_failure"]]
        result["host_retained_content_bytes"] = directory_bytes(path.parent)
        configuration = f"{result['workload']}-{result['mode']}"
        launch = launches.get((configuration, result["repetition"]))
        result["command_start_to_observed_exit_seconds"] = (
            launch["command_start_to_exit_seconds"] if launch else None
        )
        result["launch_observation_resolution_seconds"] = 2
        result["native_unit"] = {
            "rpki": "canonical authorization memberships; not raw source rows or transitions",
            "ris": "accepted native event occurrences",
            "dns": "raw measurement source rows in a partial selected artifact",
        }[result["workload"]]
        selected_raw = {
            "rpki": ".cache/phase13/rpki-ripencc.tal-2026-09-01.xz",
            "ris": ".cache/phase13/updates.20260901.0000.gz",
            "dns": ".cache/phase13/dns-li-2026-09-01.parquet",
        }[result["workload"]]
        shared = root / "inputs" / result["workload"]
        result["defined_retained_component_content_bytes"] = (
            (
                Path(selected_raw).stat().st_size
                + Path(result["input_binding"]["source_normalized_path"]).stat().st_size
                + directory_bytes(shared)
                + result["host_retained_content_bytes"]
                + result["minio_bytes"]
                + (result["broker_apparent_bytes"] if result["mode"] == "B3" else 0)
            )
            if result["status"] == "PASS"
            else "NOT_MEASURED_COMPLETE_BOUNDARY"
        )
        trials.append(result)
        if result["status"] == "PASS":
            groups.setdefault(configuration, []).append(result)
    summaries = {}
    digests = {}
    for configuration, runs in groups.items():
        if len({run["repetition"] for run in runs}) != len(runs):
            raise ValueError("Duplicate measured pilot repetition")
        for run in runs:
            if (
                run["exact"]["exact_membership_and_full_projection"] != "PASS"
                or run["exact"]["logical_rows"] != run["input_binding"]["rows"]
                or run["exact"]["projection_sha256"] != run["input_binding"]["projection_sha256"]
                or any(run["resource_guard_failures"])
            ):
                raise ValueError("Incorrect pilot run cannot enter performance summary")
            digests.setdefault(run["workload"], set()).add(run["exact"]["projection_sha256"])
        summaries[configuration] = {
            "accepted_repetitions": len(runs),
            "repetition_status": "PASS" if len(runs) == 3 else "INSUFFICIENT_REPETITIONS",
            "measurements": {
                field: statistics([run[field] for run in runs])
                for field in (
                    "command_start_to_observed_exit_seconds",
                    "to_validated_commit_seconds",
                    "transformation_seconds",
                    "publication_seconds",
                    "validation_seconds",
                    "parquet_bytes",
                    "minio_bytes",
                    "checkpoint_content_bytes",
                    "peak_sampled_process_tree_rss_bytes",
                    "defined_retained_component_content_bytes",
                )
                if all(run.get(field) is not None for run in runs)
            },
        }
    if any(len(values) != 1 for values in digests.values()):
        raise ValueError("Matched normalized projections disagree")
    parent = json.loads((root / "resources.json").read_text())
    return dict(
        evidence="MEASURED_SERVICE_NORMALIZED_CORE",
        status=campaign["status"],
        registration="matched-pilot-registration.json",
        private_run_directory=str(root),
        cap_seconds=campaign["cap_seconds"],
        growth_cap_bytes=campaign["growth_bytes"],
        campaign_elapsed_seconds=campaign["elapsed_seconds"],
        measured_successful_trials=sum(trial["status"] == "PASS" for trial in trials),
        measured_failed_trials=sum(trial["status"] != "PASS" for trial in trials),
        unexecuted_trial_count=27 - len(trials),
        trials=trials,
        configurations=summaries,
        projection_digests={workload: next(iter(values)) for workload, values in digests.items()},
        resources=dict(
            initial_guest=campaign["initial_guest"],
            final_guest=campaign["final_guest"],
            host_new_content_bytes=campaign["host_content_bytes"],
            final_combined_growth_bytes=campaign["host_content_bytes"]
            + max(
                0,
                campaign["initial_guest"]["guest_free_disk_bytes"]
                - campaign["final_guest"]["guest_free_disk_bytes"],
            ),
            peak_sampled_campaign_process_tree_rss_bytes=max(
                sample["process_tree_rss_bytes"] for sample in parent["host_samples"]
            ),
            minimum_guest_free_disk_bytes=min(
                sample["guest_free_disk_bytes"] for sample in parent["guest_samples"]
            ),
            minimum_guest_available_memory_bytes=min(
                sample["guest_available_memory_bytes"] for sample in parent["guest_samples"]
            ),
            parent_guard_failures=[parent["host_failure"], parent["guest_failure"]],
        ),
        accounting=(
            "Defined component-content lower bound: selected raw artifact and accepted normalized "
            "projection once, shared wrapper/oracle once, trial scratch including checkpoints, "
            "MinIO namespace and selected broker apparent bytes. Other shared provenance/error/"
            "oracle metadata and backend allocation NOT_MEASURED. Not complete deployed storage."
        ),
        fairness=(
            "All engines process identical canonical full-row wrapper from independently accepted "
            "normalized evidence. Source parsing and production state/group finalization excluded "
            "in every mode. Same codec and two writer partitions; row-group layouts differ and "
            "are not a lossless-representation preference experiment."
        ),
        timing=(
            "Command-start-to-observed-exit includes startup/shutdown and <=2-second polling "
            "resolution. Spark transformation_seconds includes S3A write; B0 separately records "
            "local write/upload. Only all-in boundaries are comparable. Exact verification "
            "reported separately but retained in all-in wall."
        ),
        H3=("NOT_RUN: queued availableNow is not physical 1x replay/drain acceptance"),
        new_fault_injection_tests=0,
        phase21="NOT_STARTED",
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--private-root", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(summarize(args.private_root), indent=2))


if __name__ == "__main__":
    main()
