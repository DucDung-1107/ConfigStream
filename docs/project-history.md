# Project history and acceptance records

This page preserves the milestone trail separately from the [project overview](../README.md).
For setup instructions, use the [documentation index](README.md). Each acceptance
record applies to its recorded source revision, data selection and environment.

## Current validation boundary

The project has implemented all four domain pipelines, completed selected local
replays, and demonstrated a single-node Kubernetes fixture deployment with a
research dashboard. The comparative evaluation is conditionally accepted within
its measured scope; full production and paced-replay cells remain open.

The later configuration-integrity changes passed local regressions, but the
corrected-image Kubernetes recovery trial stopped at the resource guard before
commit. It did not complete deployment validation. See the
[integrity amendment](pipeline-integrity-amendment.md),
[repair receipt](../workloads/results/pipeline-closure/validation-20261016.json)
and [current architecture status](architecture.md#scope-and-status).

## Milestones

| Stage | Delivered work | Details and evidence |
| --- | --- | --- |
| Foundations, phases 0–3 | Typed asset/snapshot/change contracts, deterministic generator, Kafka transport and CI | [Data model](data-model.md), [generator](generator.md), [Kafka topics](kafka-topics.md) |
| Configuration processing, phases 4–6 | Direct Spark Kafka input, validation, bounded version state, normalization, adjacent diffs and rule alerts | [Spark streaming](spark-streaming.md), [architecture](architecture.md) |
| Storage and serving, phases 7–9 | Committed Parquet lake, rebuildable Elasticsearch metadata and read-only FastAPI | [Data lake](data-lake.md), [Elasticsearch](elasticsearch.md), [API](api.md) |
| Controlled evaluation, phase 10 | Public baseline provenance, synthetic histories, batch analytics and replay harness; recorded one-million-snapshot stress run | [Acceptance](phase10-acceptance.md), [benchmark methodology](benchmark.md) |
| Observed histories, phase 11 | Public Git history, empirical calibration and a 100,518-snapshot controlled run | [Acceptance](phase11-acceptance.md), [external validity](external-validity.md) |
| Terraform, phase 12 | TerraDS characterization, sampled file histories, 10,188-version replay and 3,683 verified adjacent diffs | [Acceptance](phase12-acceptance.md), [methodology](phase12-methodology.md) |
| Scope and contracts, phases 13–16 | Source feasibility survey, four distinct temporal roles and evidence/provenance contracts | [Survey](phase13-dataset-survey.md), [shortlist](phase13-shortlist-decision.md), [contract acceptance](phase16-acceptance.md) |
| RPKI, phase 17 | Complete scoped daily membership, verified transitions and reconstruction | [Initial acceptance](phase17-acceptance.md), [closure](phase17-closure-acceptance.md) |
| RIS, phase 18 | Native event identity, committed history and full-hour acceptance through retained-history recovery | [Initial acceptance](phase18-acceptance.md), [finalization](phase18-finalization-acceptance.md) |
| DNS, phase 19 | Measurement preservation, qualified grouping/comparison and explicit coverage uncertainty | [Acceptance](phase19-acceptance.md), [uncertainty](phase19-dns-uncertainty.md) |
| Comparative evaluation, phase 20 | Matched RPKI storage/query experiments and 27 repeated normalized-core engine trials | [Conditional acceptance](phase20-acceptance.md), [storage](phase20-storage-query.md), [processing](phase20-processing.md) |
| Kubernetes, phase 20K.1 | One-node deployment, native Spark driver/executor, lake/API readback and owned-fixture recovery | [Acceptance](phase20-k8s-acceptance.md) |
| Dashboard, phase 20V | Research evidence views, configuration metadata, responsive UI and an executed backend rehearsal | [Acceptance](phase20v-acceptance.md), [demo](phase20v-demo-script.md) |
| Configuration integrity follow-up | Versioned Parquet inventories, exact replay checks, source-identity conflict detection and checkpoint compatibility | [Amendment](pipeline-integrity-amendment.md), [current status](architecture.md#scope-and-status) |
| Checkpoint presentation | English SimplePlus slides, Vietnamese notes, four-domain diagrams and measured RPKI comparisons | [Presentation bundle](presentations/checkpoint-2026-10-26/README.md) |

## How to interpret the record

- Generated and calibrated histories are controlled workloads; public Git histories are observations of file evolution.
- A Git commit does not establish a deployment. RPKI states, RIS events and DNS measurements have different temporal semantics.
- The accepted RIS hour used retained-history recovery; it is not a fresh clean-ingestion benchmark.
- Local storage/query results and the single-node deployment do not establish horizontal scalability, HA or end-to-end exactly-once delivery.
- Later repairs do not retroactively change older measurements. Keep negative outcomes and incomplete evaluation cells visible in their original records.

For source units and accepted totals, see the [evidence catalog](../frontend/public/research/v1/catalog.json).
The previous long README remains available in [Git history](https://github.com/kongwoang/ConfigStream/blob/e65d2dd/README.md).
