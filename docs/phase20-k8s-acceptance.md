# Phase 20K acceptance

## Decision

**PARTIAL — MANIFESTS OR PLATFORM ONLY**

A real single-node K3s/Kafka/MinIO core runs, but native Kubernetes Spark
ingestion/batch, API serving and restart/checkpoint recovery remain unverified.
The build campaign crossed its allocated-growth guard. A successful image export
during graceful shutdown does not satisfy the budget or prove application operation.

Phase 20 was fully pushed first at `787dec817657c14944997429f4b27a28ab3d2245`;
[exact Phase 20 CI](https://github.com/kongwoang/ConfigStream/actions/runs/38483498938)
succeeded with 1,104 unit, 80 API and 109 Spark tests. Its comparative decision
remains conditional; no unexecuted rate matrix is promoted to PASS.

## Measured boundary

- Dedicated node `configstream-k8s`, K3s v1.35.9+k3s1, Linux ARM64.
- VM 3 CPUs / 6 GiB / 48 GiB; original 2-CPU/6-GiB/64-GiB VM unchanged.
- Strimzi 0.51.0 / Kafka 4.1.1 RF1, MinIO source-built pinned release.
- Kafka, MinIO, operator/entity operator, CoreDNS, metrics-server/local-path Ready.
- Two infrastructure PVCs Bound; fixture PVC awaiting a scheduled consumer.
- Internal DNS/MinIO health and namespaced least-privilege checks passed.
- Three opt-in **read-only platform** tests passed; zero executed application
  Kubernetes pipeline/recovery tests. Skipped opt-in tests are not passed tests.
- Server-side manifest/CRD dry-run, Kustomize build and Helm lint passed.
- Service-free local regression: 1,112 unit and 80 API tests passed; runtime/guard-focused
  tests passed. Post-change full Spark regression and final CI must be verified
  against the exact delivery workflow; they are not local Kubernetes acceptance.

Initial socket-path bootstrap failure: 14.842 seconds, no VM created. Resume used
a short separate Lima home and retained the original clock. Platform became Ready
at 711.224 aggregate seconds. At 1,103.094 seconds growth was 16,136,364,032 bytes
against a 16,106,127,360-byte limit. Guest memory/disk floors did not fail.

## Open gates

Explicit owner approval for one bounded build/test retry with an additive growth
amendment is pending. The original 7,200-second cap is not reset or extended.
The Dockerfile has a proposed cache/layer reduction and the native submit adapter
has an offline-tested JVM-config fix; the corrected image is not built.

Required future checks: actual driver/executor lifecycle, exact Kafka-to-MinIO
history, batch output, transport offsets/lag, persistence/recovery/corruption,
four-role owned fixtures, ES/API, resource evidence and exact final CI.
Visualization, multi-node/HA and instructor approval remain separate open gates.

## Evidence

- [Execution policy](../workloads/results/phase20-k8s/execution-policy.json)
- [Platform inventory](../workloads/results/phase20-k8s/platform-validation.json)
- [Resource failure](../workloads/results/phase20-k8s/resource-failure.json)
- [Architecture](kubernetes-architecture.md), [deployment](kubernetes-deployment.md)
- [Operations](kubernetes-operations.md), [recovery](kubernetes-recovery.md)
- [Security](kubernetes-security.md), [demo](kubernetes-demo.md)

Previous accepted evidence is preserved. No private dataset benchmark, dashboard
or Phase 21 work has begun. **Vietnamese:** Đã có cụm K3s thật và Kafka/MinIO chạy;
chưa nghiệm thu pipeline Spark/API. Guard storage đã dừng build, chờ duyệt budget
trước retry; không gọi đây là PASS hay triển khai HA.
