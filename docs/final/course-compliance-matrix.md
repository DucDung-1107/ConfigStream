# Initial course compliance matrix

Stage A audit precedes new exercises and report writing. Source/status boundaries
are frozen in [requirements](course-requirements.md). Implementation paths and
accepted receipts were inspected, not inferred from a README. Report/slide/demo
references are planned locations until the final closure receipt verifies them.
No old acceptance or scientific data is changed.

| ID / official requirement | Implementation / test | Accepted evidence | Report / slide / demo | Status | Residual gap |
|---|---|---|---|---|---|
| C01: Real-world Big Data problem | workloads/feasibility_report.py / tests/unit/test_workloads.py | workloads/results/phase20/oracle-consistency.json | I.3 / 3 / 2 | VERIFIED_FROM_ACCEPTED_REAL_SOURCE | Selected scopes only; incompatible units are not pooled |
| C02: Lambda or Kappa Architecture | infra/k8s/application.py / tests/kubernetes/test_application.py | workloads/results/phase20-k8s/closure-application-validation.json | II.1 / 4 / 1 | DEMONSTRATED_WITH_OWNED_FIXTURE | Lambda-style executed speed/batch/serving; not universal four-domain search |
| C03: End-to-end ingestion to visualization | frontend/components/dashboard/dashboard.tsx / frontend/tests/dashboard.spec.ts | workloads/results/phase20v/final-delivery-validation.json | III.7 / 11 / 2 | IMPLEMENTED_AND_EXECUTED | Owned configuration API plus accepted external aggregates |
| C04: Apache Spark | spark/streaming/stateful.py / tests/spark/test_stateful.py | workloads/results/phase20-k8s/closure-application-validation.json | III.3 / 6 / 8 | IMPLEMENTED_AND_EXECUTED | Native Kubernetes driver/executor and local service-free tests differ |
| C05: HDFS or equivalent distributed storage | lake/client.py / tests/spark/test_lake.py | workloads/results/phase20-k8s/closure-final-readback.json | II.3 / 5 / 8 | DOCUMENTED_WITH_LIMITATION | S3A MinIO interface; one local-path node is not replicated distributed storage; instructor equivalence unverified |
| C06: Distributed messaging | messaging/kafka_producer.py / tests/kubernetes/test_application.py | workloads/results/phase20-k8s/closure-recovery-validation.json | III.2 / 5 / 1 | DEMONSTRATED_WITH_OWNED_FIXTURE | Strimzi/KRaft RF1 has no broker failover |
| C07: NoSQL | search/mappings.py / tests/spark/test_search.py | workloads/results/phase20-k8s/closure-final-readback.json | III.6 / 5 / 3 | IMPLEMENTED_AND_EXECUTED | Configuration metadata only; rebuildable, not historical authority |
| C08: Kubernetes or Cloud deployment | infra/k8s/base/namespaces.yaml / tests/kubernetes/test_platform.py | workloads/results/phase20-k8s/closure-application-validation.json | III.5 / 7 / 1 | IMPLEMENTED_AND_EXECUTED | One ARM64 K3s node; no HA or horizontal-scaling proof |
| C09: FastAPI integration | api/service.py / tests/api/test_api.py | workloads/results/phase20v/final-delivery-validation.json | III.6 / 11 / 3 | IMPLEMENTED_AND_EXECUTED | Bounded read-only metadata API, no all-domain raw search |
| C10: Persistent storage | infra/k8s/base/minio.yaml / tests/kubernetes/test_application.py | workloads/results/phase20-k8s/closure-recovery-validation.json | III.5 / 7 / 8 | DEMONSTRATED_WITH_OWNED_FIXTURE | Pod restart persistence; node-loss durability not tested |
| C11: Source identity and replay recovery | spark/ris.py / tests/spark/test_ris_reconciliation.py | workloads/results/phase18-finalization/full-hour-validation.json | III.8 / 8 / 5 | VERIFIED_FROM_ACCEPTED_REAL_SOURCE | RIS full-hour acceptance via retained-history recovery; callback faults are not power loss |
| C12: Resource monitoring | infra/k8s/scripts/deploy_dashboard.py / tests/unit/test_kubernetes.py | workloads/results/phase20v/resource-measurements.json | III.9 / 7 / 1 | IMPLEMENTED_AND_EXECUTED | Sampled working set/RSS, not instantaneous peak; no Prometheus/Grafana claim |
| C13: Security and data rights | search/mappings.py / tests/unit/test_dashboard_evidence.py | workloads/results/phase20v/evidence-binding.json | II.5 / 13 / 9 | DOCUMENTED_WITH_LIMITATION | No secrets/raw rows published; internal HTTP/PLAINTEXT/local-only, instructor rights review unresolved |
| C14: Complete scoped RPKI states | spark/rpki.py / tests/spark/test_rpki_transformations.py | workloads/results/phase17-closure/daily-pipeline-validation.json | I.4 / 3 / 4 | VERIFIED_FROM_ACCEPTED_REAL_SOURCE | One trust-anchor scope; observed memberships, not operator actions |
| C15: RIS native multiplicity | processing/ris.py / tests/spark/test_ris_reconciliation.py | workloads/results/phase18-finalization/full-hour-validation.json | I.4 / 3 / 5 | VERIFIED_FROM_ACCEPTED_REAL_SOURCE | One collector/hour; no RIB/incident inference; parser disposition qualified |
| C16: DNS measurement uncertainty | spark/dns.py / tests/spark/test_dns_history.py | workloads/results/phase19/independent-oracle.json | I.4 / 3 / 6 | VERIFIED_FROM_ACCEPTED_REAL_SOURCE | Two objects, unknown entity/vantage coverage; no deletion/causality claim |
| S01: Complex Aggregations | analytics/jobs.py / tests/spark/test_analytics.py | workloads/results/phase20v/ci-closure-validation.json | III.10 / 6 / 7 | IMPLEMENTED_AND_EXECUTED | Window/statistics exist; pivot/unpivot/custom aggregate evidence needs bounded closure |
| S02: Advanced Transformations | spark/streaming/parser.py / tests/spark/test_transformations.py | workloads/results/phase20v/ci-closure-validation.json | III.10 / 6 / 7 | IMPLEMENTED_AND_EXECUTED | Chaining exists; deterministic Spark UDF/native equivalence demonstration pending |
| S03: Join Operations | analytics/jobs.py / tests/spark/test_analytics.py | workloads/results/phase20v/ci-closure-validation.json | III.10 / 6 / 7 | IMPLEMENTED_AND_EXECUTED | Joins execute; broadcast/sort-merge physical strategy not established by .join alone |
| S04: Performance Optimization | spark/ris.py / tests/spark/test_ris_reconciliation.py | workloads/results/phase18-finalization/full-hour-validation.json | III.10 / 6 / 7 | IMPLEMENTED_AND_EXECUTED | Bounded reconciliation/persistence executed; explicit pruning/bucketing plan demo pending |
| S05: Streaming Processing | spark/streaming/stateful.py / tests/spark/test_stateful.py | workloads/results/phase20-k8s/closure-recovery-validation.json | III.10 / 6 / 8 | DEMONSTRATED_WITH_OWNED_FIXTURE | Watermark/state/restart executed; modes applicability and bounded guarantees need explicit explanation |
| S06: Advanced Analytics | analytics/jobs.py / tests/spark/test_analytics.py | workloads/results/phase20v/ci-closure-validation.json | III.10 / 6 / 7 | IMPLEMENTED_AND_EXECUTED | Statistics/time-series exist; MLlib/GraphFrames are not implemented or justified by a predictive/graph question |
| A01: Current-semester syllabus and rubric | Not yet implemented / Not yet executed | No completed output | I.6 / 1 / 10 | UNVERIFIED | 2020 course page; Teams disconnected; no verified current semester announcement |
| A02: Authors, group, student identifiers | Not yet implemented / Not yet executed | No completed output | I.1 / 1 / 10 | UNVERIFIED | Owner asked for exact roster; repository handle alone does not establish institutional identity |
| R01: Four report sections | Not yet implemented / Not yet executed | No completed output | I-IV / 14 / 10 | NOT_RUN | Final source/PDF must be written and rendered after this audit |
| R02: Eleven structured Lessons Learned | Not yet implemented / Not yet executed | No completed output | IV / 12 / 10 | NOT_RUN | Authentic experiences, four required subheadings per lesson |
| R03: Presentation and actual defense | Not yet implemented / Not yet executed | No completed output | III.11 / 11 / 1 | NOT_RUN | Editable slides, notes/PDF render and fresh rehearsal required |
| E01: Phase20 comparative evaluation | benchmarks/phase20.py / tests/unit/test_evaluation.py | workloads/results/phase20/hypothesis-decisions.json | III.11 / 10 / 7 | DOCUMENTED_WITH_LIMITATION | H1/H2 within tested scope; H3 NOT_RUN; core normalized comparisons are not full production rate benchmarks |

## Targeted closure decision

Keep all existing production paths and local-only dashboard. Add a bounded owned
Spark educational suite for pivot/unpivot, actual grouped aggregate, scalar UDF
versus native SQL, physical joins, pruning, supported catalog bucketing and
materialized persistence. Reuse existing real stateful streaming/late-data tests;
do not introduce fabricated routing/DNS state. MLlib/GraphFrames applicability
is discussed, not invented. Write report/slides only after these gaps are audited.
H3 remains NOT_RUN; a pacing proposal may be documented but no long replay starts.
Final statuses are recorded additively in the final acceptance/compliance closure
receipt, preserving this pre-implementation audit.
