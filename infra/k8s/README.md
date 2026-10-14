# ConfigStream Kubernetes

Status: real single-node K3s and Kafka/MinIO core are running. **Application
streaming/batch, API and recovery acceptance are not yet established.** See
[acceptance](../../docs/phase20-k8s-acceptance.md) before running anything.

- `platform/lima.yaml`: separate owner-approved 3-CPU/6-GiB/48-GiB VM, no source mounts.
- `base/`: namespaces, namespaced Spark RBAC, isolated MinIO, owned checkpoint PVC.
- `overlays/local/`: single-node Strimzi KRaft and explicit configuration topic.
- `overlays/multinode/`: gated requirements, not an accepted multi-node overlay.
- `values/strimzi.yaml`: pinned operator chart values and bounded resources.
- `versions.json`: verified ARM64 image references and software versions.
- `scripts/`: checksum-verified bootstrap, fail-closed sampling and native fixture submission.
- `Dockerfile`: locked Python application and pre-resolved Spark connectors.

All private credentials, kubeconfig, image/source exports and operational logs live
outside Git. Never copy `.cache`, old topics/PVCs or external-source ledgers into an
image. Native cluster-mode submission is mandatory; local image-build SparkPi only
prepares connectors and is **not** deployment evidence.

The failed build campaign crossed its 15-GiB allocated-growth guard. It is stopped
pending an explicit budget amendment. Do not reset the original 7,200-second receipt
or silently retry. Read [deployment](../../docs/kubernetes-deployment.md) and
[operations](../../docs/kubernetes-operations.md) for safe commands and limitations.
