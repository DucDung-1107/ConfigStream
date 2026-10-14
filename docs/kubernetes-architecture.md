# Kubernetes architecture and evidence boundary

## Executed Lambda-style paths

Speed: Kafka → native Kubernetes Spark Structured Streaming → immutable MinIO/
Parquet commits. Batch: a separate native Spark application reads committed
history, validates the independently specified fixture projection, and builds
verified metadata. Serving: Elasticsearch/FastAPI expose configuration metadata.
MinIO/Parquet remains the historical source of truth, not the search index.
All three paths were executed; frontend visualization remains absent.

## Actual deployment

One ARM64 K3s v1.35.9+k3s1 node: dedicated Lima `configstream-k8s`, 3 CPUs /
7 GiB RAM / 48 GiB disk. `cs-infra` hosts Strimzi 0.51.0/Kafka 4.1.1 KRaft RF1
and MinIO; `cs-processing` hosts separate Spark drivers/executors and the owned
RWO checkpoint/evidence PVC; `cs-app` hosts Elasticsearch 9.5.5 and FastAPI.
CoreDNS, metrics-server and local-path storage provide the lightweight platform.
The original Lima `k`, private sources and all accepted evidence remain separate.

Actual owned workflows preserve Terraform Git versions, scoped RPKI complete
states, RIS occurrence multiplicity and DNS measurement uncertainty using existing
processors. They do not establish another private full-source acceptance.

Single-node execution is not horizontal scaling or HA. RF1 has no broker failover;
local-path pod restart persistence is not node-loss durability. The shared local
checkpoint PVC is not a portable multi-node checkpoint design. No external server
was needed. See [diagram](figures/kubernetes-architecture.drawio),
[PNG](figures/kubernetes-architecture.png), [acceptance](phase20-k8s-acceptance.md)
and [executed evidence](../workloads/results/phase20-k8s/closure-application-validation.json).
