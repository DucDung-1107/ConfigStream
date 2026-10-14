# Kubernetes architecture and evidence boundary

## Course-level decision

The intended deployment is Lambda-style: an executed speed path must consume
Kafka with native Kubernetes Structured Streaming and commit immutable Parquet;
a separately executed batch path must read that committed history and build a
verified analytical/metadata projection; FastAPI/Elasticsearch serve that safe
projection. MinIO/Parquet, not search indexes, remains historical source of truth.
The visualization frontend is a future read-only client, not FastAPI's Swagger UI.

**Only the platform/core portion is currently accepted.** Both Spark paths and the
serving path are not yet executed on Kubernetes. The presence of adapter code or
an architecture diagram does not establish a functioning Lambda application.

## Actual boundary

- Dedicated `configstream-k8s` Lima VM: one ARM64 node, K3s `v1.35.9+k3s1`.
- `cs-infra`: Strimzi 0.51.0, one Kafka 4.1.1 KRaft broker/controller, MinIO.
- `cs-processing`: Spark ServiceAccount/Role and owned fixture checkpoint PVC.
  Driver/executor pods are planned, not observed.
- `cs-app`: reserved serving namespace; Elasticsearch/API/dashboard not deployed.
- K3s metrics-server/CoreDNS/local-path provisioner are observed running.

The previous Lima `k` VM and all accepted Terraform/RPKI/RIS/DNS evidence remain
outside this deployment. No private source mount or migration is performed.

## Semantic and persistence contracts

Configuration snapshots retain adjacent-version semantics. RPKI retains scoped
complete observations; RIS retains source-coordinate occurrence multiplicity;
DNS retains measurement status, contributors and uncertainty. The adapter calls
existing processing functions; it does not create a universal configuration diff.

Local-path PVCs preserve data through a tested pod replacement only when such a
test has actually run. They do not establish node-loss durability. RF1 has no
broker failover; one Kubernetes node is not horizontal scalability or HA.

See [acceptance](phase20-k8s-acceptance.md), [diagram](figures/kubernetes-architecture.drawio)
and [machine-readable platform inventory](../workloads/results/phase20-k8s/platform-validation.json).
