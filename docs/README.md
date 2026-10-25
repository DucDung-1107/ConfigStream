# ConfigStream documentation

Start with the [project overview](../README.md) or the
[processing demo](../README.md#quick-start). This index groups guides by task;
chronological decisions and acceptance records are in [project history](project-history.md).

## Run the system

| Task | Guide |
| --- | --- |
| Understand the components and current validation status | [Architecture](architecture.md) |
| Generate controlled JSONL or Kafka inputs | [Generator](generator.md) |
| Configure the broker and topics | [Kafka topics](kafka-topics.md) |
| Run Spark with Java 21 | [Spark streaming](spark-streaming.md) |
| Store committed history in MinIO | [Data lake](data-lake.md) |
| Initialize or rebuild configuration search | [Elasticsearch](elasticsearch.md) |
| Serve configuration metadata | [API](api.md) |
| Build and preview the dashboard | [Frontend README](../frontend/README.md) |
| Deploy on Kubernetes | [Infrastructure README](../infra/k8s/README.md) |
| Operate or recover a deployment | [Operations](kubernetes-operations.md), [recovery](kubernetes-recovery.md) |
| Rehearse the integrated presentation | [Seven-step demonstration](phase20v-demo-script.md) |

The processing demo needs no services. Service-based examples require the stack
specified in their guide; Kubernetes demonstrations additionally require the
cluster and selected deployment to be running.

## Data and domain contracts

| Topic | Read next |
| --- | --- |
| Configuration entities, versions and diffs | [Data model](data-model.md), [architecture](architecture.md) |
| Baseline files and controlled histories | [Datasets](datasets.md), [workload calibration](workload-calibration.md) |
| Public Git histories | [Real-history sources](real-history-sources.md) |
| Terraform / TerraDS | [Source and scope](terrads.md), [sampling and replay](phase12-methodology.md) |
| RPKI observed states | [Architecture](phase17-rpki-architecture.md), [complete-day integration](phase17-closure-integration.md) |
| RIS event occurrences | [Architecture](phase18-ris-architecture.md), [identity and correctness](phase18-ris-identity-and-correctness.md) |
| DNS measurements | [Architecture](phase19-dns-architecture.md), [uncertainty](phase19-dns-uncertainty.md), [OpenINTEL attribution](phase19-source-attribution.md) |
| Dashboard evidence and API boundaries | [Data contracts](phase20v-data-contracts.md) |
| Current configuration integrity and checkpoint compatibility | [Integrity amendment](pipeline-integrity-amendment.md) |

## Results and methodology

- [Historical Spark analytics](historical-analytics.md) and [replay methodology](benchmark.md).
- [Storage/query comparison](phase20-storage-query.md): RPKI layouts, repeated warm-query timings and accounting boundaries.
- [Processing comparisons](phase20-processing.md): measured normalized-core trials and unexecuted full-pipeline cells.
- [Cross-domain interpretation](phase20-cross-domain-analysis.md) and [threats to validity](phase20-threats-to-validity.md).
- [Evaluation acceptance](phase20-acceptance.md), [Kubernetes acceptance](phase20-k8s-acceptance.md) and [dashboard acceptance](phase20v-acceptance.md).
- [Evidence catalog](../frontend/public/research/v1/catalog.json) and [versioned result receipts](../workloads/results/).

Recorded experiments describe their own code revision, source scope and runtime.
Check [current architecture status](architecture.md#scope-and-status) before using
an older acceptance record as evidence for a new deployment.

## Development and validation

| Area | Entry point |
| --- | --- |
| Python environment and core checks | [Root quick start](../README.md#quick-start), [Makefile](../Makefile) |
| Unit and service-dependent test suites | [Tests](../tests/), [pytest markers](../pyproject.toml) |
| Spark setup and local tests | [Spark streaming](spark-streaming.md) |
| API tests and integration prerequisites | [API](api.md) |
| Frontend type, unit and browser checks | [Frontend README](../frontend/README.md) |
| Kubernetes manifest validation | [Infrastructure README](../infra/k8s/README.md) |
| Automated CI jobs | [GitHub Actions workflow](../.github/workflows/ci.yml) |

## Presentations

[Checkpoint 1 — 26 October 2026](presentations/checkpoint-2026-10-26/README.md):
English slides, Vietnamese presenter notes, editable diagrams and reproducible
result charts. The supplied [SimplePlus template](presentations/SimplePlus_Blank_Template/)
is preserved alongside the presentation.


### Contributed reference materials

The checkpoint linked above remains the primary presentation. Additional materials
are retained for editing and reference:

- [Report drafts and figures](../doc/README.md) — PR #1, Nguyễn Đức Dũng.
- [Architecture package](presentations/configstream-architecture-package/README.md) — PR #2, Mai Vũ Hoàng.
- [Experimental slide template and evidence tools](slides/README.md) — PR #1; generic template content, not the checkpoint deck.
