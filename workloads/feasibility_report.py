import argparse
import csv
import html
import json
import math
from collections import Counter
from pathlib import Path

from workloads.feasibility import write_json

RESULTS = Path("workloads/results/dataset-feasibility")
SPECIFICATION = Path("workloads/manifests/dataset-feasibility.json")
ACCESS = {
    "VERIFIED_ACCESSIBLE",
    "VERIFIED_METADATA_ONLY",
    "ACCESS_REQUIRES_REGISTRATION",
    "ACCESS_RESTRICTED",
    "RAW_DATA_UNAVAILABLE",
    "BROKEN_OR_WITHDRAWN",
    "NOT_YET_VERIFIED",
}
TEMPORAL = {"T0", "T1", "T2", "T3", "T4", "T5", "TX"}


def read_json(path):
    return json.loads(Path(path).read_text())


def validate_registry(specification):
    expected = (
        {f"A{index}" for index in range(1, 16)}
        | {f"B{index}" for index in range(1, 4)}
        | {"C1", "C2", "D1", "D2", "D3"}
    )
    candidates = specification["candidates"]
    if len(candidates) != len(expected) or {row["id"] for row in candidates} != expected:
        raise ValueError("All 23 mandatory candidates must appear exactly once")
    for candidate in candidates:
        if candidate["access"] not in ACCESS or not set(candidate["temporal"]) <= TEMPORAL:
            raise ValueError("Unknown access or temporal classification")
        if not candidate["sources"] or candidate["fit"] not in {"F1", "F2", "F3", "F4"}:
            raise ValueError("Evidence and architecture classification are required")
        for score, maximum in zip(candidate["scores"], specification["score_maxima"], strict=True):
            if type(score) is not int or not 0 <= score <= maximum:
                raise ValueError("Score exceeds rubric")
    return candidates


def write_csv(path, rows):
    if not rows:
        return
    columns = list(dict.fromkeys(key for row in rows for key in row))
    with Path(path).open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def bars(path, title, rows, *, logarithmic=False):
    width, height = 1150, 85 + len(rows) * 26
    maximum = max((value for label, value in rows), default=1) or 1
    scale = math.log1p(maximum) if logarithmic else maximum
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}"><rect width="100%" height="100%" fill="white"/>',
        f'<text x="20" y="26" font-family="sans-serif" font-size="16">{html.escape(title)}</text>',
    ]
    for index, (label, value) in enumerate(rows):
        top = 50 + index * 26
        length = 530 * (math.log1p(value) if logarithmic else value) / scale
        parts.extend(
            [
                f'<text x="15" y="{top + 15}" font-family="sans-serif" font-size="12">'
                f"{html.escape(str(label))}</text>",
                f'<rect x="380" y="{top}" width="{length:.2f}" height="18" fill="#2166ac"/>',
                f'<text x="{390 + length:.2f}" y="{top + 14}" font-family="sans-serif" '
                f'font-size="11">{value:,.0f}</text>',
            ]
        )
    if logarithmic:
        parts.append(
            f'<text x="20" y="{height - 10}" font-size="12">'
            "Bar length uses log(1 + value); labels are actual values.</text>"
        )
    Path(path).write_text("\n".join(parts) + "</svg>\n")


def complementarity(left, right):
    identities = {left["id"], right["id"]}
    if left["id"] == right["id"] or "D3" in identities and identities <= {"D1", "D2", "D3"}:
        return 0
    if identities == {"D1", "D2"}:
        return 2
    if identities <= {"A1", "A2", "A13"}:
        return 2
    if left["fit"] == "F4" or right["fit"] == "F4":
        return 3
    if left["fit"] == right["fit"]:
        return 5 if left["fit"] == "F2" else 3
    return 9 if "T5" in left["temporal"] + right["temporal"] else 8


def report(results=RESULTS, specification=SPECIFICATION):
    specification = read_json(specification)
    candidates = validate_registry(specification)
    results.mkdir(parents=True, exist_ok=True)
    plots = results / "plots"
    plots.mkdir(exist_ok=True)
    rpki, bgp, dns = [read_json(results / (name + ".json")) for name in ("rpki", "bgp", "dns")]
    if len(rpki["days"]) < 2 or len(dns["days"]) != 2 or bgp["prefix_events"] <= 0:
        raise ValueError("Shortlist requires successful temporal policy, DNS and event probes")
    artifacts = read_json(results / "artifacts.json")
    history = read_json("workloads/results/terrads/history-summary.json")
    static = read_json("workloads/results/terrads/static-summary.json")
    phase11 = read_json("workloads/results/real-history/observed-summary.json")["combined"]
    measured = {
        "A1": {
            "unit": "current .tf files",
            "entities": static["terraform_files"],
            "logical_bytes": static["logical_terraform_bytes"],
            "changes": None,
        },
        "A2": {
            "unit": "file incarnations / Git adjacent pairs",
            "entities": history["assets"],
            "records": history["revisions"],
            "changes": history["revisions"] - history["assets"],
            "logical_bytes": history["logical_content_bytes"],
            "start": history["start"],
            "end": history["end"],
        },
        "A3": {
            "unit": "file incarnations / Git adjacent pairs",
            "entities": phase11["assets"],
            "records": phase11["revisions"],
            "changes": phase11["revisions"] - phase11["assets"],
            "logical_bytes": phase11["logical_content_bytes"],
            "start": phase11["start"],
            "end": phase11["end"],
        },
        "B1": {
            "unit": "canonical authorization tuples / added+removed transitions",
            "entities": rpki["unique_entities_union"],
            "records": sum(day["rows"] for day in rpki["days"]),
            "changes": sum(day.get("added", 0) + day.get("removed", 0) for day in rpki["days"]),
            "logical_bytes": rpki["logical_bytes"],
            "artifact_bytes": sum(item["artifact_bytes"] for item in rpki["artifacts"]),
            "start": "2026-09-01",
            "end": "2026-09-14",
            "natural_event_rate": None,
        },
        "D1": {
            "unit": "unique prefixes / peer-specific NLRI events",
            "entities": bgp["unique_prefixes"],
            "records": bgp["prefix_events"],
            "changes": None,
            "logical_bytes": bgp["logical_mrt_bytes"],
            "artifact_bytes": sum(item["artifact_bytes"] for item in bgp["artifacts"]),
            "start": "2026-09-01T00:00:00Z",
            "end": "2026-09-01T01:00:00Z",
            "natural_event_rate": bgp["events_per_second_fixed_hour"],
        },
        "C1": {
            "unit": "query names/day; changed A/AAAA groups among common groups",
            "entities": dns["days"][0]["query_names"],
            "records": sum(day["rows"] for day in dns["days"]),
            "changes": dns["days"][-1]["comparison"]["changed_groups"],
            "logical_bytes": sum(day["parquet_uncompressed_column_bytes"] for day in dns["days"]),
            "logical_byte_definition": "uncompressed Parquet column bytes, not DNS wire bytes",
            "canonical_address_json_bytes": sum(
                day["canonical_address_json_bytes"] for day in dns["days"]
            ),
            "artifact_bytes": sum(day["artifact"]["artifact_bytes"] for day in dns["days"]),
            "start": "2026-09-01",
            "end": "2026-09-02",
            "natural_event_rate": None,
        },
    }
    for candidate in candidates:
        candidate["total_score"] = sum(candidate["scores"])
        candidate["score_uncertainty"] = (
            "Judgmental rubric, +/- 5 total points; not a statistical CI"
        )
        candidate["measured"] = measured.get(candidate["id"])
        candidate["collection_date"] = specification["collection_date"]
        candidate["historical_content_type"] = candidate["temporal"]
        candidate["raw_artifact_bytes"] = [
            record["artifact"]["artifact_bytes"]
            for record in artifacts["records"]
            if record["candidate"] == candidate["id"]
        ]
        candidate["event_identity"] = {
            "F1": "asset incarnation + pinned revision/blob hash; extraction ordinal",
            "F2": "source/scope + observation date + canonical entity state hash",
            "F3": "source timestamp/peer/resource identity and payload hash; retain multiplicity",
            "F4": "No native ConfigStream event identity proposed; discovery/archive service only",
        }[candidate["fit"]]
        candidate["causal_limit"] = (
            "No deployment, operator command, incident or production-rate inference"
        )
    write_json(results / "candidate-registry.json", {"schema_version": 1, "candidates": candidates})
    write_json(
        results / "probe-summary.json",
        {
            "schema_version": 1,
            "measured": measured,
            "new_probe_candidates": [
                "B1",
                "C1",
                "D1",
                "C2",
                "A5",
                "A6",
                "A7",
                "A8",
                "A10",
                "A11",
                "A13",
            ],
            "reused_candidates": ["A1", "A2", "A3", "A4"],
            "warning": "Units differ; do not sum rows/events as configuration changes",
        },
    )
    manifest_artifacts = (
        rpki["artifacts"]
        + bgp["artifacts"]
        + [day["artifact"] for day in dns["days"]]
        + [record["artifact"] for record in artifacts["records"]]
    )
    write_json(
        results / "probe-manifest.json",
        {
            "schema_version": 1,
            "artifacts": manifest_artifacts,
            "unique_artifact_bytes": sum(
                item["artifact_bytes"]
                for item in {item["url"]: item for item in manifest_artifacts}.values()
            ),
            "limits": {
                "per_artifact_bytes": 500000000,
                "new_download_target_bytes": 2000000000,
                "disk_reserve_bytes": 20 * 1024**3,
            },
            "scope": "Bounded inspection only; no source adapters or pipeline integration",
        },
    )
    rows = [
        {
            "id": candidate["id"],
            "candidate": candidate["name"],
            "temporal": ";".join(candidate["temporal"]),
            "access": candidate["access"],
            "fit": candidate["fit"],
            "decision": candidate["decision"],
            "license": candidate["license"],
            "coverage": candidate["coverage"],
            "primary_url": candidate["url"],
            "confidence": candidate["confidence"],
        }
        for candidate in candidates
    ]
    write_csv(results / "candidate-matrix.csv", rows)
    write_csv(
        results / "semantic-classification.csv",
        [
            {
                "id": candidate["id"],
                "entity": candidate["entity"],
                "timestamp": candidate["timestamp"],
                "event_identity": candidate["event_identity"],
                "causal_limit": candidate["causal_limit"],
            }
            for candidate in candidates
        ],
    )
    comparisons = [
        {
            "id": candidate["id"],
            **(
                candidate["measured"]
                or {
                    "unit": "N/A: no comparable real-state/event probe",
                    "entities": None,
                    "changes": None,
                    "logical_bytes": None,
                }
            ),
        }
        for candidate in candidates
    ]
    for name in ["temporal-comparison", "volume-comparison", "velocity-comparison"]:
        write_csv(results / (name + ".csv"), comparisons)
    scores = [
        {
            "id": candidate["id"],
            **dict(zip(specification["score_order"], candidate["scores"], strict=True)),
            "total": candidate["total_score"],
            "confidence": candidate["confidence"],
            "uncertainty": candidate["score_uncertainty"],
        }
        for candidate in candidates
    ]
    write_csv(results / "scoring.csv", scores)
    matrix = [
        {"id": left["id"], **{right["id"]: complementarity(left, right) for right in candidates}}
        for left in candidates
    ]
    write_csv(results / "complementarity-matrix.csv", matrix)
    shortlist = {
        "preferred": ["A2", "B1", "C1", "D1"],
        "primary_new_workload": "D1 for streaming velocity; B1 for policy-state evolution",
        "configuration_centered_alternative": ["A2", "B1", "C1"],
        "blocked_alternative": "PeeringDB only after legitimate access and substantive-field pilot",
        "scope_gate": "Instructor must approve infrastructure state/event scope before Round 2",
        "rankings": {
            "direct_configuration_history": ["A2", "A3", "A7"],
            "policy_state": ["B1", "B2"],
            "high_velocity": ["D1", "D2", "C2"],
            "long_history": ["D1", "B1", "B2", "C1"],
            "overall_feasibility": [
                row["id"]
                for row in sorted(candidates, key=lambda row: (-row["total_score"], row["id"]))
            ],
        },
        "no_automatic_integration": True,
    }
    write_json(results / "shortlist.json", shortlist)
    bars(
        plots / "accessibility.svg",
        "Candidate access classifications (23 candidate artifacts)",
        sorted(Counter(candidate["access"] for candidate in candidates).items()),
    )
    bars(
        plots / "temporal-types.svg",
        "Temporal classes (multi-label counts; do not sum as candidates)",
        sorted(Counter(kind for candidate in candidates for kind in candidate["temporal"]).items()),
    )
    bars(
        plots / "scores.svg",
        "Judgmental evidence rubric /100; legal/access blockers override score",
        [(candidate["id"], candidate["total_score"]) for candidate in candidates],
    )
    bars(
        plots / "sample-bytes.svg",
        "Measured logical bytes: text / CSV / MRT / Parquet columns differ",
        [(identity, row["logical_bytes"]) for identity, row in measured.items()],
        logarithmic=True,
    )
    bars(
        plots / "sample-records.svg",
        "Measured records (units differ; static files / observations / events)",
        [(identity, row.get("records", row["entities"])) for identity, row in measured.items()],
        logarithmic=True,
    )
    bars(
        plots / "observations-changes.svg",
        "Different units: repeated observations versus state differences",
        [
            ("RPKI daily rows", measured["B1"]["records"]),
            ("RPKI tuple additions/removals", measured["B1"]["changes"]),
            ("DNS daily rows, all record types", measured["C1"]["records"]),
            ("DNS changed common A/AAAA groups", measured["C1"]["changes"]),
        ],
        logarithmic=True,
    )
    bars(
        plots / "rpki-daily-counts.svg",
        "RPKI unique authorizations per daily observation",
        [(day["date"], day["unique_authorizations"]) for day in rpki["days"]],
    )
    bars(
        plots / "rpki-daily-changes.svg",
        "RPKI daily tuple additions + removals (baseline omitted)",
        [(day["date"], day["added"] + day["removed"]) for day in rpki["days"] if "added" in day],
    )
    bars(
        plots / "bgp-minute-events.svg",
        "RIS rrc00 unicast peer-prefix events/minute, source time",
        [(str(index), value) for index, (timestamp, value) in enumerate(bgp["events_per_minute"])],
    )
    bars(
        plots / "dns-observed-differences.svg",
        "DNS address values in common observed groups only",
        list(dns["days"][-1]["comparison"].items()),
    )
    parts = [
        '<svg xmlns="http://www.w3.org/2000/svg" width="850" height="850">',
        '<rect width="100%" height="100%" fill="white"/>',
        '<text x="10" y="25" font-size="16">'
        "Conceptual complementarity 0–9 (judgment, not data)</text>",
    ]
    for index, candidate in enumerate(candidates):
        position = 80 + index * 30
        parts.extend(
            [
                f'<text x="{position}" y="65" font-size="11">{candidate["id"]}</text>',
                f'<text x="40" y="{position + 18}" font-size="11">{candidate["id"]}</text>',
            ]
        )
        for other_index, other in enumerate(candidates):
            score = complementarity(candidate, other)
            parts.extend(
                [
                    f'<rect x="{80 + other_index * 30}" y="{position}" width="29" '
                    f'height="29" fill="rgb({240 - score * 14},240,{240 - score * 9})"/>',
                    f'<text x="{90 + other_index * 30}" y="{position + 20}" '
                    f'font-size="11">{score}</text>',
                ]
            )
    (plots / "complementarity.svg").write_text("\n".join(parts) + "</svg>\n")
    lines = [
        "# Phase 13 measured dataset feasibility",
        "",
        "23 mandatory candidates; no architecture change. Null = not measured/not applicable.",
        "The Phase 12 decision B remains unchanged for direct configuration histories.",
        "",
        "## All-candidate inventory",
        "",
        "| ID | Candidate | Temporal | Access | Fit | Decision | Score |",
        "|---|---|---|---|---|---|---:|",
    ]
    for candidate in candidates:
        lines.append(
            f"| {candidate['id']} | [{candidate['name']}]({candidate['url']}) | "
            f"{', '.join(candidate['temporal'])} | {candidate['access']} | "
            f"{candidate['fit']} | {candidate['decision']} | {candidate['total_score']} |"
        )
    lines += [
        "",
        "## Actual measured samples (not population estimates)",
        "",
        "| ID | Unit | Entities | Records | Changes | Logical bytes | Natural events/s |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for identity, row in measured.items():
        lines.append(
            "| "
            + " | ".join(
                str(value) if value is not None else "N/A"
                for value in [
                    identity,
                    row["unit"],
                    row["entities"],
                    row.get("records"),
                    row["changes"],
                    row["logical_bytes"],
                    row.get("natural_event_rate"),
                ]
            )
            + " |"
        )
    lines += [
        "",
        "## Per-candidate strength, limitation and effort",
        "",
        "| ID | Temporal | Main strength | Main limitation / decision reason | Effort |",
        "|---|---|---|---|---|",
    ]
    for candidate in candidates:
        lines.append(
            f"| {candidate['id']} | {', '.join(candidate['temporal'])} | "
            f"{candidate['role']} | {candidate['reason']} | {candidate['effort']} |"
        )
    lines += [
        "",
        "## Full scoring comparison",
        "",
        "All scores judgmental; other components and uncertainty are in scoring.csv.",
        "",
        "| ID | Temporal /20 | Scale /20 | Access /15 | Fit /10 | "
        "Complementarity /10 | Total /100 |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for candidate in candidates:
        scores = candidate["scores"]
        lines.append(
            f"| {candidate['id']} | {scores[0]} | {scores[1]} | {scores[2]} | "
            f"{scores[4]} | {scores[5]} | {candidate['total_score']} |"
        )
    lines += [
        "",
        "Preferred: **A2 + B1 + C1 + D1**; alternative: **A2 + B1 + C1**.",
        "BGP events provide velocity, not proof of high-frequency configuration edits.",
        "See the methodology and shortlist docs for filtering, parser failures and scope gate.",
    ]
    (results / "summary.md").write_text("\n".join(lines) + "\n")
    return {"candidates": len(candidates), "preferred": shortlist["preferred"]}


def main():
    parser = argparse.ArgumentParser(
        description="Regenerate tables and SVGs from measured artifacts"
    )
    parser.add_argument("--results", type=Path, default=RESULTS)
    args = parser.parse_args()
    print(json.dumps(report(args.results)))


if __name__ == "__main__":
    main()
