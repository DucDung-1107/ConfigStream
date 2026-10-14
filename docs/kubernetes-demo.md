# Demonstration contract

## Current truthful demonstration

Show one Ready K3s node, CoreDNS/metrics-server/local-path, the Strimzi KRaft
broker/controller, MinIO and Bound infrastructure PVCs. Show internal DNS/MinIO
health and namespaced RBAC checks. **Do not present this as a completed data pipeline.**

The platform read-only tests are reproducible without private sources:

```sh
export KUBECONFIG="$PWD/.cache/phase20-k8s/kubeconfig"
CONFIGSTREAM_K8S_TESTS=read-only .venv/bin/pytest tests/kubernetes
```

They do not inject faults, publish source events or count Spark success.

## Intended application demonstration, not executed

Seed independently specified owned snapshots plus an identical retry. Execute
native cluster-mode streaming with separate driver/executor pods. Validate exact
committed snapshots and batch reconstruction. Then validate configuration metadata
in Elasticsearch and an actual read-only FastAPI business query. Repeat bounded
RPKI/RIS/DNS fixtures without replacing their domain semantics.

All private/full datasets remain excluded. Synthetic fixtures are clearly labeled;
no synthetic values may be advertised as a real-source dashboard.

## Visualization gate

No dashboard/frontend is implemented or deployed. Swagger is not end-to-end
visualization. The future service contract is a separately deployed read-only
client of verified metadata/projections with explicit configuration, authorization
observation, routing occurrence and DNS measurement labels. It must display scope,
provenance, completeness/uncertainty and timing qualifiers and must not expose
raw private rows or imply DNS/RIS configuration causality.

The [IT4043E guideline](https://datalab.vn/teaching/it4043e-big-data-storage-and-processing)
requires visualization as well as ingestion/processing/storage. That course gate
remains open. No dashboard implementation or Phase 21 is authorized by this phase.
