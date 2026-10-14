# Kubernetes20K — preflight and execution authorization

Baseline clean main `787dec817657c14944997429f4b27a28ab3d2245`, after all completed
Phase20 code/evidence was normally pushed. [Exact CI](https://github.com/kongwoang/ConfigStream/actions/runs/38483498938)
passed1104unit/80API/109Spark; this is service-free CI, not Kubernetes acceptance.
Phase20 remains conditional for unexecuted full benchmarks. Prior evidence stays
immutable; no full datasets or Phase21 work are authorized here.

## Measured existing environment

Apple Silicon host19327352832bytes RAM, about594GiB free disk; memory-pressure
reports39percent free after quota wait. Existing workspace Lima2.2.0 `k` uses
VZ/ARM64,2CPU/6GiB/64GiB; guest free30334242816bytes, available RAM4622168064bytes.
Kafka4.1.2 and source-built MinIO remain healthy. Their named persistent Docker
volumes `configstream_kafka-data`/`configstream_minio-data` live in the original
guest rootless Docker storage. They are not new Kubernetes PVCs and must never
be mounted, migrated, deleted or reset by this experiment.

No kubectl, Helm, Kustomize, K3s, Kind or Minikube was installed. Existing Docker
CLI lacks buildx. New builds must not commandeer the original VM. A separate
K3s/containerd environment with pinned BuildKit is planned.

## Owner-approved bounded scope

Owner: **“Duyệt VM riêng và bounded fixtures.”**
[Policy](../workloads/results/phase20-k8s/execution-policy.json): a new
`configstream-k8s` VM3CPU/6GiB/48GiB,7200s installation/build/fixture cap,
15GiB newly allocated growth,20GiB host/guest disk reserve,512MiB guest available
floor, original VM unchanged. Quota was allowed to refill **before** the cap starts.
Core Kafka/MinIO/native Spark first; Elasticsearch/API advance only after measured
memory/request budget. No paid cloud or automatic capacity/timeout increases.

The [K3s requirements](https://docs.k3s.io/installation/requirements) support ARM64
and distinguish control-plane overhead from application requirements. Thus6GiB is
a bounded pilot allocation, not a guarantee that the whole demonstration fits.
Full-stack scheduling/usage must be measured, not inferred from manifests.

## Verified image/platform choices

[Pins](../infra/k8s/versions.json) record verified LinuxARM64 manifest digests for
Spark4.0.2/Java21, Strimzi0.51.0, supported Kafka4.1.1 and Elasticsearch9.5.5.
The requested Strimzi0.51.0/Kafka4.1.2 image returned **no such manifest**; this
new fixture cluster uses a declared4.1.1 environment, not a downgrade of accepted
experiments. Kafka client/serialization compatibility is still an execution gate.
Existing MinIO image is ARM64, built from the pinned source commit using
`infra/docker/minio/Dockerfile`; retain source/license attribution and do not claim
vendor-supported HA. Application dependencies remain locked by `uv.lock`.

Native Spark must create Kubernetes driver/executor pods. The current runtime
explicitly forces a local master; a narrowly tested submit-aware path is required,
while the default local research runtime remains unchanged. See
[Spark Kubernetes deployment](https://dist.apache.org/repos/dist/release/spark/docs/4.0.2/running-on-kubernetes.html).

## Course and serving boundaries

The [official guideline](https://datalab.vn/teaching/it4043e-big-data-storage-and-processing)
requires an actual data pipeline and visualization, not merely manifests. Existing
FastAPI provides configuration-specific metadata queries via Elasticsearch, not
four-domain APIs and not a dashboard. Visualization remains an explicit gate;
no dashboard implementation or fabricated real-source demo is authorized.
This is a single-node local deployment, not broker failover, node-loss durability,
multi-node HA, horizontal scalability or full course acceptance by itself.
