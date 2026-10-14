# Demonstration contract

Show one Ready ARM64 K3s node, Ready Kafka/MinIO/ES/API, Bound PVCs, completed
native Spark drivers and separate executors, real streaming/batch results,
selected offsets/immutable commits and verified API queries. All values are
clearly labeled owned fixtures, not advertised as private real-source traffic.

```sh
export KUBECONFIG="$PWD/.cache/phase20-k8s/kubeconfig"
.tools/k8s/kubectl port-forward -n cs-app svc/api 18800:8000 --address 127.0.0.1
# Another shell with the same explicit kubeconfig:
CONFIGSTREAM_K8S_TESTS=read-only .venv/bin/pytest tests/kubernetes/test_platform.py
CONFIGSTREAM_K8S_TESTS=closure .venv/bin/pytest tests/kubernetes/test_application.py
curl -fsS 'http://127.0.0.1:18800/api/v1/snapshots?asset_id=k8s-owned-router'
curl -fsS 'http://127.0.0.1:18800/api/v1/assets/k8s-owned-router/latest-snapshot'
```

These tests assert previously executed workflows; they do not themselves inject
faults. See [deployment](kubernetes-deployment.md) and [recovery](kubernetes-recovery.md).

Four owned roles: eight Terraform Git versions (not deployments), seven RPKI rows/
two complete states, four RIS occurrences/two fingerprints/one exclusion, six DNS
measurements/two groups/entity coverage unknown. No unified config-change claim.

## Open visualization/course gate

No dashboard/frontend is implemented. Swagger is not end-to-end visualization.
The future independently deployed read-only UI must expose scope, provenance,
source time and completeness/uncertainty qualifiers. Existing configuration API
does not imply all four domain histories are served.

The [IT4043E guideline](https://datalab.vn/teaching/it4043e-big-data-storage-and-processing)
also requires visualization. Instructor review and full course acceptance remain
open. No dashboard, private benchmark or Phase 21 begins automatically.
