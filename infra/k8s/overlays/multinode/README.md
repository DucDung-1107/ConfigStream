# Gated multi-node profile

No multi-node deployment or availability acceptance exists.

Before an authorized deployment, replace local-path RWO volumes with a tested
durable storage class; use an independent shared/S3 checkpoint design rather
than the owned-fixture single-node PVC; pin application images in an authenticated
registry; configure ingress TLS, network egress, resource quotas and node placement.

Kafka requires separately placed KRaft controllers and replicated brokers,
replication factor at least three, appropriate ISR policy and executed failover
tests. MinIO needs a separately justified distributed/durable storage deployment.
Spark executor count alone does not prove horizontal scalability. Neither these
notes nor the local manifests establish HA or node-loss durability.

No cloud resources, paid services or additional nodes are authorized automatically.
