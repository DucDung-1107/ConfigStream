# Security and licensing

## Verified local controls

Separate Lima home/no private mounts; encrypted K3s Secrets; private mode-0600
kubeconfig. Services are ClusterIP, accessed through localhost port-forward.
Credentials are generated privately and applied via stdin, absent from Git,
images and ConfigMaps. API never receives administrative credentials.

Spark's namespaced Role manages necessary pods/services/configmaps only;
cleanup requires service `deletecollection`, not PVC deletion.
`spark.kubernetes.driver.ownPersistentVolumeClaim=false`. No app cluster-admin.
Infra Pod Security is baseline; processing/app restricted v1.35, nonroot and
capability drop. NetworkPolicy restricts namespace ingress.

API `cs-api` has `read`/`view_index_metadata` on `cs20k1-*` and cluster
`monitor` for health. Bootstrap/index writer is separate from the API.

## Limitations and publication

Owned Kafka is trusted internal PLAINTEXT; S3/ES internal HTTP. Processing has
administrative fixture MinIO credentials; index bootstrap is privileged.
Production TLS/SASL, per-bucket users, narrow egress and credential rotation
require separate validation. This is not comprehensive zero trust.

CLI/chart checksums and ARM64 images are pinned in `versions.json`. Source-built
MinIO is AGPLv3: preserve corresponding-source/license obligations on distribution.
Elasticsearch terms differ from Apache Spark/Kafka. Builds use a clean Git archive
and locked dependencies, never private caches/checkpoints/third-party rows.

Public outputs contain owned fixtures, code, aggregates and digests only.
Instructor approval/source redistribution rights are not inferred from Kubernetes
acceptance. The future UI must use approved read-only projections.
