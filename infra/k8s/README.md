# ConfigStream Kubernetes

**PASS: real single-node owned-fixture application deployment.** Native Spark
driver/executor, Kafka→MinIO exact history, batch, ES/API and recovery passed.
[Acceptance](../../docs/phase20-k8s-acceptance.md) limits claims to tested scope;
dashboard, HA/multinode and instructor/course acceptance remain separate.

- `platform/lima.yaml`: original approved3CPU/6GiB/48GiB, no source mounts.
- `platform/lima-closure.yaml`: accepted3CPU/7GiB/48GiB demonstration profile.
- `base/`: namespaces/RBAC, MinIO and owned evidence PVC.
- `overlays/local/`: RF1 Strimzi KRaft and topic.
- `overlays/closure/`: isolated owned pipeline configuration.
- `overlays/demo/`: persistent ES and read-only FastAPI.
- `overlays/multinode/`: gated requirements, not deployed HA.
- `scripts/`: bootstrap/build, exclusive safety guard, native submit, fixture
  environments, serving secrets, pod metrics and persistence inventory.
- `Dockerfile`/`versions.json`: locked application and pinned ARM64 software.

No private source/archive/checkpoint/credential is included in images or Git.
Old growth failure stays recorded; the owner authorized new20K.1 campaigns.
See [deployment](../../docs/kubernetes-deployment.md),
[operations](../../docs/kubernetes-operations.md) and
[recovery](../../docs/kubernetes-recovery.md). Never reset old receipts/PVCs.
