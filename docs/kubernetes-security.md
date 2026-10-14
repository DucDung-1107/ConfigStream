# Security and licensing

## Verified local controls

- All application services are ClusterIP; local K3s access is 127.0.0.1:16443.
- A separate Lima home and no host source mounts isolate prior private evidence.
- K3s is started with secrets encryption and kubeconfig mode 0600.
- Credentials are generated privately, sent to Kubernetes via stdin and never
  included in Git, images or ConfigMaps. Administrative kubeconfig remains private.
- Spark Role is namespaced to `cs-processing`, limited to pods/services/configmaps.
  Real impersonated checks permit processing pod creation and deny infrastructure
  Secret reads/default-namespace pod creation.
- Namespace Pod Security: infra baseline; processing/application restricted v1.35.
- MinIO drops capabilities, runs nonroot, has no ServiceAccount token.
- NetworkPolicy restricts ingress to labeled internal namespaces; this is not
  fine-grained egress isolation or a production zero-trust claim.

## Explicit local limitations

Kafka uses trusted internal PLAINTEXT without client authentication in the owned,
single-node fixture profile. MinIO uses internal HTTP. Neither is public. Production
TLS/SASL, per-bucket users/policies, narrow egress and read-only serving credentials
need separate validation. The fixture processing Secret currently carries MinIO
administrative credentials; it is not a general production least-privilege claim.
No application pod has cluster-admin. Only the private bootstrap operator uses
cluster administration for installation.

Serving Secrets/RBAC and Elasticsearch/API controls are not accepted because
those services have not been deployed. A dashboard must consume approved read-only
projections, not administrative S3/Kafka access.

## Supply chain and publication

Use checksum-verified pinned CLI releases, exact Strimzi chart hash and recorded
Linux ARM64 image references. The MinIO image is source-built from the pinned
release/commit already used by the project; MinIO is AGPLv3, not an unrestricted
proprietary redistribution artifact. Preserve source/license and corresponding
source obligations when distributing an image. Elasticsearch licensing is not
equivalent to Apache Spark/Kafka; record distribution terms before publishing images.

No private third-party raw files, normalized rows, checkpoints, payload logs or
credentials are committed. Owned synthetic fixtures are distinguished from real
source results. Instructor approval and full course acceptance are not assumed.
