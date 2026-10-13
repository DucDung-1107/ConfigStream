"""Compact Phase 20 summaries; never substitutes historical runs for matched trials."""

import argparse
import json
from pathlib import Path
from statistics import median

from benchmarks.evaluation import preference, statistics


def load_trials(root):
    trials = [json.loads(path.read_text()) for path in sorted(root.glob("*/result.json"))]
    if len(trials) != 9 or any(trial["status"] != "PASS" for trial in trials):
        raise ValueError("Incomplete or incorrect trials; no accepted aggregate")
    if len({(trial["mode"], trial["repetition"]) for trial in trials}) != 9:
        raise ValueError("Missing or duplicate trial repetition")
    return trials


def summarize_rpki(root):
    trials = load_trials(root)
    configurations = {}
    digests = {}
    for mode in ("R1", "R2", "R3"):
        selected = sorted(
            (trial for trial in trials if trial["mode"] == mode),
            key=lambda item: item["repetition"],
        )
        query_ids = {query["query_id"] for trial in selected for query in trial["queries"]}
        if query_ids != {
            "all_days",
            "selected_day",
            "membership",
            "daily_transitions",
            "asn_transitions",
        }:
            raise ValueError("Required query omitted from preference evaluation")
        by_query = {}
        for query_id in sorted(query_ids):
            values = [
                [query["seconds"] for query in trial["queries"] if query["query_id"] == query_id]
                for trial in selected
            ]
            if any(len(cycle) != 3 for cycle in values):
                raise ValueError("Missing registered query cycle")
            outputs = {
                query["output_sha256"]
                for trial in selected
                for query in trial["queries"]
                if query["query_id"] == query_id
            }
            if len(outputs) != 1 or (
                query_id in digests and digests[query_id] != next(iter(outputs))
            ):
                raise ValueError("Matched configurations produce different exact query results")
            digests[query_id] = next(iter(outputs))
            by_query[query_id] = dict(
                warm_cycles_by_build=values,
                build_median_statistics=statistics([median(cycle) for cycle in values]),
                output_sha256=digests[query_id],
                exact_correctness="PASS",
            )
        sizes = {trial["analytical_bytes"] for trial in selected}
        if len(sizes) != 1:
            raise ValueError("Physical byte denominator varies across equivalent builds")
        configurations[mode] = dict(
            analytical_bytes=next(iter(sizes)),
            queries=by_query,
            build_seconds=statistics([trial["build_seconds"] for trial in selected]),
            elapsed_seconds=statistics([trial["elapsed_seconds"] for trial in selected]),
            parquet_files=selected[0]["parquet_files"],
            retained_scratch_bytes=[trial["scratch_retained_bytes"] for trial in selected],
        )
    decisions = {}
    full_queries = {
        key: item["build_median_statistics"]["individual"]
        for key, item in configurations["R1"]["queries"].items()
    }
    for mode in ("R2", "R3"):
        candidate_queries = {
            key: item["build_median_statistics"]["individual"]
            for key, item in configurations[mode]["queries"].items()
        }
        decisions[mode] = preference(
            configurations["R1"]["analytical_bytes"],
            configurations[mode]["analytical_bytes"],
            full_queries,
            candidate_queries,
            correct=True,
        )
        decisions[mode]["paired_query_ratios_by_repetition"] = {
            query: [
                candidate / full
                for candidate, full in zip(candidate_queries[query], values, strict=True)
            ]
            for query, values in full_queries.items()
        }
    return dict(
        evidence="MEASURED_REAL_SOURCE_NORMALIZED",
        experiment="E2R",
        trials=trials,
        configurations=configurations,
        preference_decisions=decisions,
        correctness="exact ordered comparison every day/cycle; all query outputs agree",
        matched_processing_rank="NOT_APPLICABLE: representation comparison, not B0 versus B3",
        query_timing="Parquet reads + SQLite materialization + exact output/independent validation",
        cache="OS uncontrolled, three warm cycles per fresh-process build; build warms inputs",
        process_import_startup_seconds="NOT_MEASURED; worker elapsed excludes module import",
        input_manifest="source-manifest-validation.json",
        policy="experiment-registration.json",
    )


def summarize_pilot(root):
    trials = load_trials(root)
    return dict(
        evidence="MEASURED_REAL_SOURCE_NORMALIZED",
        trials=trials,
        comparison="UNMATCHED to previous source-parsing service trials; do not rank engines",
        modes={
            mode: dict(
                rows=next(trial["rows"] for trial in trials if trial["mode"] == mode),
                write_seconds=statistics(
                    [trial["write_seconds"] for trial in trials if trial["mode"] == mode]
                ),
                validation_seconds=statistics(
                    [trial["validation_seconds"] for trial in trials if trial["mode"] == mode]
                ),
            )
            for mode in ("rpki", "ris", "dns")
        },
        new_kafka_transport="NOT_RUN",
        new_spark_processing="NOT_RUN",
        parser_cost="EXCLUDED; pinned normalized input reused",
        process_import_startup_seconds="NOT_MEASURED",
    )


def resource_summary(roots):
    rows = []
    for root in roots:
        for path in sorted(root.glob("*/resources.json")):
            value = json.loads(path.read_text())
            host, guest = value["host_samples"], value["guest_samples"]
            if not host or not guest or value["host_failure"] or value["guest_failure"]:
                raise ValueError("Missing or failed resource sampling")
            rows.append(
                dict(
                    run=path.parent.name,
                    suite=root.name,
                    peak_sampled_process_tree_rss_bytes=max(
                        item["process_tree_rss_bytes"] for item in host
                    ),
                    minimum_host_free_disk_bytes=min(item["free_disk_bytes"] for item in host),
                    minimum_guest_free_disk_bytes=min(
                        item["guest_free_disk_bytes"] for item in guest
                    ),
                    minimum_guest_available_ram_bytes=min(
                        item["guest_available_memory_bytes"] for item in guest
                    ),
                    peak_sampled_output_bytes=max(item["experiment_bytes"] for item in host),
                    host_sample_count=len(host),
                    guest_sample_count=len(guest),
                    failures=[],
                )
            )
    return dict(
        evidence="MEASURED_LOCAL",
        runs=rows,
        allocation_changes=False,
        task_spill_cpu="NOT_MEASURED: these workers use PyArrow/SQLite, no Spark JVM",
        peak_scope="sampled worker tree only; container RSS separately in environment manifest",
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--private", type=Path, default=Path(".cache/phase20"))
    args = parser.parse_args()
    reports = dict(
        rpki=summarize_rpki(args.private / "rpki"),
        pilot=summarize_pilot(args.private / "pilot-v2"),
        resources=resource_summary([args.private / "rpki", args.private / "pilot-v2"]),
    )
    print(json.dumps(reports, sort_keys=True))


if __name__ == "__main__":
    main()
