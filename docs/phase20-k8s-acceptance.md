# Phase 20K.1 Kubernetes application and recovery acceptance

## Decision

**PASS — REAL KUBERNETES DEPLOYMENT VALIDATED**

Scope: one local ARM64 node, owned fixtures, native driver/executor execution,
persistent pod restarts and single-writer callback/checkpoint recovery.
This is not HA, node-loss durability, a full private-data replay, a completed
dashboard or blanket course acceptance.

The original Phase 20K result was **PARTIAL** after its build crossed the old
15-GiB growth guard. Original [acceptance receipt](../workloads/results/phase20-k8s/acceptance.json),
[resource failure](../workloads/results/phase20-k8s/resource-failure.json) and prior
measurements remain immutable. The owner subsequently authorized resource
adjustments through a [dated amendment](../workloads/results/phase20-k8s/closure-policy-amendment.json).

## Exact executed scope

Dedicated Lima `configstream-k8s`: 3 CPUs / 7 GiB / 48 GiB, K3s v1.35.9+k3s1,
one Ready ARM64 node. Original Lima `k` remains 2 CPUs / 6 GiB / 64 GiB.
Strimzi 0.51.0/Kafka 4.1.1 RF1, pinned MinIO, Spark 4.0.2/Java 21/Python 3.12.13,
Elasticsearch 9.5.5 and existing FastAPI ran on Kubernetes.

Accepted application source: `d24e7c460905d0d01f3edd8c119d0440a681ff8c`;
image manifest `sha256:59e650a218b1f4e6f76459fb15ee56bbbe18963f5d61020dcec2de8d23889192`.
Delivery documentation/test SHA is distinct from the executable image SHA.

Configuration: exact two normalized EventRow projections, one adjacent diff.
Three initial deliveries plus three replay deliveries produced six physical
rows but still two logical versions; selected final Kafka lag zero.
Native Spark batch validated all normalized declared fields, not just row counts.

Actual Terraform/RPKI/RIS/DNS owned workflows passed: 8/7/4/6 logical records
respectively. RIS retained four occurrences despite only two fingerprints and
published its one source exclusion/window accounting. DNS retained unknown entity
coverage. Fixture scope is not another full real-source validation.

## Persistence, integrity and resources

Kafka/MinIO pod replacement preserved twenty selected object SHA-256s and offset
three; ES replacement preserved API query results. Commit-before-checkpoint
failure/restart passed. Missing/corrupt/uncommitted cloned history failed closed;
conditional conflicting commit was rejected. See [recovery](kubernetes-recovery.md).

Accepted campaign sampled `cs-*` working-set peak 4,699,242,496 bytes, CPU peak
1.385 cores, zero new host swap-out, guest available RAM minimum 1,164,099,584
bytes, free disk minimum 26,092,679,168 bytes. Observed campaign through reporting
4,524.371 seconds; not an isolated throughput benchmark. Caches/earlier attempts
are included in accounted allocation. No production-memory/scalability inference.

## Validation and remaining gates

Local service-free regression: 1,121 unit / 80 API tests; actual read-only
Kubernetes acceptance: three platform plus six application tests, all passed.
Full Spark regression and delivery CI are bound separately in
[closure validation](../workloads/results/phase20-k8s/closure-validation.json).
Skipped opt-in tests are never counted as passed.

Final delivery CI is service-free; it does not execute Kubernetes or private data.
Final SHA/run URL/conclusion are verified after pushing and reported in the final
agent report/private CI receipt; a file cannot encode its own Git commit SHA.
Dashboard, instructor review, production TLS/least privilege, multi-node topology,
remote storage and full course-guideline acceptance remain open. No Phase 21.

## Evidence and runbooks

- [Application](../workloads/results/phase20-k8s/closure-application-validation.json)
- [Recovery](../workloads/results/phase20-k8s/closure-recovery-validation.json)
- [Resources](../workloads/results/phase20-k8s/closure-resource-measurements.json)
- [Negative attempts](../workloads/results/phase20-k8s/closure-negative-attempts.json)
- [Closure decision](../workloads/results/phase20-k8s/closure-acceptance.json)
- [Architecture](kubernetes-architecture.md), [deployment](kubernetes-deployment.md)
- [Operations](kubernetes-operations.md), [security](kubernetes-security.md)
- [Demonstration](kubernetes-demo.md)

**Vietnamese:** Máy hiện tại chạy được stack Kubernetes thật, Spark có driver/
executor riêng, pipeline/batch/API và recovery fixture đạt. Không gọi single-node
là HA; chưa có dashboard và chưa chạy lại dữ liệu nghiên cứu đầy đủ.
