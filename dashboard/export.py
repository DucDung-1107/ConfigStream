"""Export bounded public aggregates from immutable accepted research receipts."""

import argparse
import gzip
import json
import tempfile
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

ROOT = Path(__file__).resolve().parents[1]
BINDING = "9c2520c06aa3b455577ead9ef16f0a820e1e804d"


class Catalog(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: str
    generated_at: str
    artifact_commit: str
    artifacts: dict[str, dict[str, str | None]] = Field(max_length=20)
    domains: list[dict[str, JsonValue]] = Field(max_length=4)
    rpki_days: list[dict[str, JsonValue]] = Field(max_length=14)
    ris_minutes: list[dict[str, JsonValue]] = Field(max_length=60)
    ris_summary: dict[str, JsonValue]
    ris_fixture: list[dict[str, JsonValue]] = Field(max_length=4)
    dns_days: list[dict[str, JsonValue]] = Field(max_length=2)
    dns_comparison: dict[str, JsonValue]
    representations: list[dict[str, JsonValue]] = Field(max_length=3)
    queries: list[dict[str, JsonValue]] = Field(max_length=15)
    processing: list[dict[str, JsonValue]] = Field(max_length=27)
    hypotheses: dict[str, JsonValue]
    negative_findings: list[dict[str, JsonValue]] = Field(max_length=4)
    operations: dict[str, JsonValue]
    attribution: list[dict[str, str]] = Field(max_length=4)

    @model_validator(mode="after")
    def semantic_boundaries(self):
        if self.schema_version != "dashboard-1" or self.artifact_commit != BINDING:
            raise ValueError("Unknown dashboard source binding")
        if any(day.get("status") != "COMPLETE" for day in self.rpki_days):
            raise ValueError("Partial RPKI observations cannot be displayed as complete")
        if self.dns_comparison.get("strict_matched_vantage_eligible") != 0:
            raise ValueError("Unverified DNS matched-vantage qualification")
        if self.hypotheses.get("H3", {}).get("decision") != "NOT_RUN":
            raise ValueError("Unexecuted H3 cannot be promoted")
        if sum(row["occurrences"] for row in self.ris_minutes) != self.ris_summary["occurrences"]:
            raise ValueError("RIS minute completeness mismatch")
        if len({row["id"] for row in self.ris_fixture}) != len(self.ris_fixture):
            raise ValueError("Distinct owned RIS source occurrences required")
        return self


def build():
    artifacts = {}
    bindings = json.loads((ROOT / "dashboard/source-bindings.json").read_text())

    def source(relative):
        path = ROOT / "workloads/results" / relative
        raw = path.read_bytes()
        data = json.loads(raw)
        public = "workloads/results/" + relative
        if bindings.get(public) != sha256(raw).hexdigest():
            raise ValueError("Pinned research artifact missing or modified")
        artifacts[public] = {
            "sha256": sha256(raw).hexdigest(),
            "code_commit": data.get("code_commit")
            or data.get("runtime_commit")
            or data.get("evidence_code_commit"),
        }
        return data, public

    terraform, terraform_ref = source("phase20/terraform-reference.json")
    rpki, rpki_ref = source("phase17-closure/daily-pipeline-validation.json")
    transitions, transition_ref = source("phase17-closure/transition-pipeline-validation.json")
    ris, ris_ref = source("phase18-finalization/full-hour-validation.json")
    rates, rate_ref = source("phase18-closure/event-time-characterization.json")
    multiplicity, multiplicity_ref = source("phase18-closure/multiplicity-validation.json")
    dns, dns_ref = source("phase19/independent-oracle.json")
    dns_storage, dns_storage_ref = source("phase19/representation-costs.json")
    representations, representation_ref = source("phase20/rpki-representations.json")
    all_in, all_in_ref = source("phase20/rpki-all-in-storage.json")
    processing, processing_ref = source("phase20/matched-processing-repetitions.json")
    hypotheses, hypothesis_ref = source("phase20/hypothesis-decisions.json")
    application, application_ref = source("phase20-k8s/closure-application-validation.json")
    resources, resource_ref = source("phase20-k8s/closure-resource-measurements.json")
    recovery, recovery_ref = source("phase20-k8s/closure-recovery-validation.json")
    readback, readback_ref = source("phase20-k8s/closure-final-readback.json")
    if (
        any(day["status"] != "PASS" for day in rpki["days"])
        or ris["status"] != "PASS_RETAINED_HISTORY_FINALIZATION"
    ):
        raise ValueError("Accepted complete observations/occurrences required")
    if not dns["exact_private_ledgers"] or not dns["independent_normalization"]:
        raise ValueError("DNS independent source evidence unavailable")

    def domain(name, role, count, unit, scope, artifact, qualifier, field):
        return dict(
            name=name,
            role=role,
            count=count,
            unit=unit,
            scope=scope,
            artifact=artifact,
            field=field,
            qualifier=qualifier,
            evidence="ACCEPTED REAL-SOURCE AGGREGATE",
            status="ACCEPTED",
        )

    domains = [
        domain(
            "Terraform",
            "VersionedConfiguration",
            terraform["accepted_versions"],
            "accepted Git versions",
            "375 repositories; accepted TerraDS selection",
            terraform_ref,
            "Git revisions do not establish deployments.",
            "accepted_versions",
        ),
        domain(
            "RPKI",
            "StateObservation",
            rpki["complete_days"],
            "complete daily observations",
            rpki["scope"],
            rpki_ref,
            "Complete scoped observed membership; no operator-operation claim.",
            "complete_days",
        ),
        domain(
            "RIS",
            "EventOccurrence",
            ris["logical_occurrences"],
            "native event occurrences",
            "rrc00 · 2026-09-01 00:00–01:00 UTC",
            ris_ref,
            "Accepted through retained-history recovery; "
            "UPDATEs do not establish a complete routing table.",
            "logical_occurrences",
        ),
        domain(
            "DNS",
            "MeasurementObservation",
            dns["raw_source_rows"],
            "raw measurements",
            "Two selected .li zonefile-basis objects · 2026-09-01–02",
            dns_ref,
            "Source completeness leaves entity coverage and resolver/vantage unknown.",
            "raw_source_rows",
        ),
    ]
    days = []
    for index, day in enumerate(rpki["days"]):
        boundary = transitions["boundaries"][index - 1] if index else None
        days.append(
            dict(
                date=day["date"],
                members=day["members"],
                status="COMPLETE",
                additions=boundary["additions"] if boundary else None,
                removals=boundary["removals"] if boundary else None,
                observation_id=day["observation_id"],
                set_digest=day["set_digest"],
                reconstruction_seconds=day["reconstruction_seconds"],
                artifact=rpki_ref,
                transition_artifact=transition_ref,
            )
        )
    minute_rows = [
        dict(
            time=datetime.fromtimestamp(row["minute"], UTC).isoformat(),
            announcement=row["announcement"],
            withdrawal=row["withdrawal"],
            occurrences=row["occurrences"],
            artifact=rate_ref,
        )
        for row in rates["minutes"]
    ]
    if sum(row["occurrences"] for row in minute_rows) != ris["logical_occurrences"]:
        raise ValueError("Source minute totals disagree with accepted history")
    ris_summary = dict(
        occurrences=ris["logical_occurrences"],
        announcements=sum(row["announcement"] for row in minute_rows),
        withdrawals=sum(row["withdrawal"] for row in minute_rows),
        exclusions=ris["source_exclusions"],
        repeated_fingerprints=multiplicity["repeats_beyond_first"],
        mean_source_rate=rates["mean_occurrences_per_source_second"],
        artifact=ris_ref,
        fingerprint_artifact=multiplicity_ref,
    )
    fixture = owned_ris_fixture(
        application["four_roles"]["ris"]["oracle"]["source_order_projection_sha256"]
    )
    dns_days = [
        dict(
            date=row["date"],
            rows=row["summary"]["source_rows"],
            selected_rows=row["summary"]["selected_address_rows"],
            groups=row["summary"]["address_groups"],
            statuses=row["summary"]["status_codes"],
            workers=row["distinct_recorded_workers"],
            source_precision=row["source_precision"],
            entity_coverage=row["summary"]["entity_coverage"],
            proven_empty=row["summary"]["real_proven_empty_rows"],
            artifact=dns_ref,
        )
        for row in dns["sources"]
    ]
    representation_rows = []
    queries = []
    for mode, config in representations["configurations"].items():
        components = [
            row["accounted_component_content_bytes"]
            for row in all_in["rows"]
            if row["mode"] == mode
        ]
        representation_rows.append(
            dict(
                mode=mode,
                bytes=config["analytical_bytes"],
                all_in_bytes=sorted(components)[len(components) // 2],
                artifact=representation_ref,
                all_in_artifact=all_in_ref,
            )
        )
        for name, query in config["queries"].items():
            statistics = query["build_median_statistics"]
            queries.append(
                dict(
                    mode=mode,
                    query=name,
                    median=statistics["median"],
                    minimum=statistics["minimum"],
                    maximum=statistics["maximum"],
                    status=query["exact_correctness"],
                    output_digest=query["output_sha256"],
                    artifact=representation_ref,
                )
            )
    trial_rows = []
    for trial in processing["trials"]:
        if (
            trial["status"] != "PASS"
            or trial["exact"]["exact_membership_and_full_projection"] != "PASS"
        ):
            raise ValueError("Unqualified processing trial")
        trial_rows.append(
            dict(
                workload=trial["workload"],
                mode=trial["mode"],
                repetition=trial["repetition"],
                rows=trial["exact"]["logical_rows"],
                seconds=trial["command_start_to_observed_exit_seconds"],
                validated_commit_seconds=trial["to_validated_commit_seconds"],
                artifact=processing_ref,
                status="PASS",
                scope="Same accepted normalized projection; "
                "parser and production finalization excluded",
            )
        )
    operations = dict(
        recorded_date="2026-10-14",
        evidence="RECORDED KUBERNETES RUN",
        node_count=application["node_count"],
        k3s=application["node_info"]["kubeletVersion"],
        architecture=application["node_info"]["architecture"],
        vm=resources["vm"],
        peak_sampled_working_set_bytes=resources["pod_peak_sampled_working_set_bytes"],
        measurement=resources["metric_definition"],
        selected_lag=recovery["source_replay"]["final_selected_lag"],
        lag_query_id=application["configuration"]["query_id"],
        fixture_versions=application["configuration"]["snapshots"],
        fixture_diffs=application["configuration"]["adjacent_diffs"],
        native_pods=readback["pods"],
        recovery="PASS · owned callback/pod fixtures",
        limits="One node; RF1; local-path volumes; internal HTTP/PLAINTEXT; "
        "no node-loss or HA claim",
        artifact=application_ref,
        resource_artifact=resource_ref,
        recovery_artifact=recovery_ref,
        native_artifact=readback_ref,
    )
    negatives = [
        dict(
            label="DNS value reuse increased retained components",
            value=dns_storage["reused_component_bytes"] - dns_storage["full_component_bytes"],
            unit="additional bytes",
            qualifier=dns_storage["representation_evaluation"],
            artifact=dns_storage_ref,
        ),
        dict(
            label="RIS global aggregation exhausted Spark execution memory",
            value=None,
            unit="recorded failure",
            qualifier="Earlier full ingestion failed at finalization; "
            "bounded reconciliation subsequently recovered retained history.",
            artifact="workloads/results/phase18-finalization/root-cause-analysis.json",
        ),
        dict(
            label="RPKI analytical savings exceed all-in savings",
            value=None,
            unit="qualified comparison",
            qualifier=all_in["all_in_definition"],
            artifact=all_in_ref,
        ),
        dict(
            label="RIS 1× paced replay H3",
            value=None,
            unit="NOT_RUN",
            qualifier=hypotheses["H3"]["finding"],
            artifact=hypothesis_ref,
        ),
    ]
    root_cause = ROOT / negatives[1]["artifact"]
    if bindings.get(negatives[1]["artifact"]) != sha256(root_cause.read_bytes()).hexdigest():
        raise ValueError("Pinned negative evidence modified")
    artifacts[negatives[1]["artifact"]] = dict(
        sha256=sha256(root_cause.read_bytes()).hexdigest(), code_commit=None
    )
    return Catalog(
        schema_version="dashboard-1",
        generated_at=datetime.now(UTC).isoformat(),
        artifact_commit=BINDING,
        artifacts=artifacts,
        domains=domains,
        rpki_days=days,
        ris_minutes=minute_rows,
        ris_summary=ris_summary,
        ris_fixture=fixture,
        dns_days=dns_days,
        dns_comparison={**dns["comparison"], "artifact": dns_ref},
        representations=representation_rows,
        queries=queries,
        processing=trial_rows,
        hypotheses={
            name: {**hypotheses[name], "artifact": hypothesis_ref} for name in ("H1", "H2", "H3")
        },
        negative_findings=negatives,
        operations=operations,
        attribution=[
            dict(
                name="TerraDS accepted Git selection",
                scope="Public source provenance and accepted filtering",
                url="https://github.com/kongwoang/ConfigStream/blob/"
                + BINDING
                + "/docs/terrads.md",
            ),
            dict(
                name="RIPE NCC RPKI",
                scope="ripencc.tal observed authorization memberships",
                url="https://www.ripe.net/",
            ),
            dict(
                name="RIPE RIS",
                scope="rrc00 selected UPDATE archives; aggregate-only",
                url="https://ris.ripe.net/",
            ),
            dict(
                name="OpenINTEL",
                scope="CC BY-NC-SA 4.0; selected .li objects; aggregate-only",
                url="https://openintel.nl/",
            ),
        ],
    )


def owned_ris_fixture(expected_projection):
    import pyarrow.parquet as parquet

    from schemas.temporal.provenance import Artifact
    from workloads.ris_oracle import build as oracle

    data = json.loads((ROOT / "tests/fixtures/phase16/mrt.json").read_text())
    raw = bytes.fromhex(data["one_update_hex"]) * 2 + bytes.fromhex(data["malformed_bgp_body_hex"])
    with tempfile.TemporaryDirectory() as temporary:
        directory = Path(temporary)
        path = directory / "owned.gz"
        compressed = bytearray(gzip.compress(raw, mtime=0))
        compressed[9] = 3
        path.write_bytes(compressed)
        artifact = Artifact(
            sha256=sha256(path.read_bytes()).hexdigest(),
            bytes=path.stat().st_size,
            safe_reference="fixture:20k1/owned.gz",
            revision="owned-v1",
        )
        summary = oracle(path, artifact, directory / "oracle", source_id="owned")
        if summary["source_order_projection_sha256"] != expected_projection:
            raise ValueError("Owned occurrence fixture differs from accepted Kubernetes source")
        rows = parquet.read_table(directory / "oracle/events.parquet").to_pylist()
    return [
        dict(
            id=row["logical_record_id"],
            offset=row["byte_offset"],
            ordinal=row["expansion_ordinal"],
            fingerprint=row["content_fingerprint"],
            prefix=row["prefix"],
            action=row["action"],
            evidence="PROJECT-OWNED DEMO FIXTURE",
            artifact="tests/fixtures/phase16/mrt.json",
        )
        for row in rows
    ]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output", type=Path, default=ROOT / "frontend/public/research/v1/catalog.json"
    )
    args = parser.parse_args()
    catalog = build()
    raw = catalog.model_dump_json(indent=2).encode()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(raw)
    digest = sha256(raw).hexdigest()
    (ROOT / "frontend/lib/evidence-digest.ts").write_text(
        f'export const evidenceDigest = "{digest}";\n'
    )
    print(
        json.dumps(
            dict(
                bytes=len(raw),
                sha256=digest,
                artifacts=len(catalog.artifacts),
                scope="approved aggregates and owned fixtures only",
            )
        )
    )


if __name__ == "__main__":
    main()
