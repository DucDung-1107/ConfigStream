# Operations and resource safety

Kafka/MinIO, ES/API and native Spark ran on the dedicated 3-CPU/7-GiB/48-GiB node.
Stateful PVCs are Bound; completed driver/executor pods remain evidence.
Metrics-server, structured results, private logs and five-second pod sampling
replace a heavier Prometheus/Grafana stack in this local profile.

```sh
export KUBECONFIG="$PWD/.cache/phase20-k8s/kubeconfig"
export PATH="$PWD/.tools/k8s:$PATH"
kubectl get nodes,pods,pvc -A
kubectl top nodes
kubectl top pods -A
kubectl get events -A --sort-by=.metadata.creationTimestamp
kubectl logs -n cs-processing DRIVER --tail=100
kubectl describe pod -n cs-processing POD
```

## Fail-closed safety

The dated [20K.1 amendment](../workloads/results/phase20-k8s/closure-policy-amendment.json)
lifts old runtime/growth caps for new campaigns, not previous receipts.
`scripts/safety.py` uses exclusive receipts/fresh samples: 20 GiB host/guest disk,
512 MiB guest available RAM, 15% host memory-pressure free; three consecutive
samples above 16 MiB/s new host swap-out stop owned jobs/builds. It never deletes
PVCs or prior datasets. Old storage and initial swap failures remain negative.

Accepted 7-GiB campaign: zero new swap-out; minimum guest available memory
1,164,099,584 bytes and free disk 26,092,679,168 bytes. Sampled summed `cs-*`
pod working-set peak 4,699,242,496 bytes and CPU peak 1.385 cores. These are not
total process RSS or instantaneous peaks. Accounted allocated storage includes
build caches and earlier attempts. See [measurements](../workloads/results/phase20-k8s/closure-resource-measurements.json).

## Diagnosis

| Symptom | Inspect first |
|---|---|
| Pending | Allocatable vs requests, events, PVC/node placement |
| CrashLoopBackOff | Previous logs, command, Secrets and endpoints |
| OOMKilled | Exit reason, limits and actual guest/host memory; stop before resizing |
| FailedMount | Bound PVC/PV, fsGroup, RWO topology |
| Kafka unavailable | Ready condition, DNS, topic status, selected offsets |
| Executor failure | Driver logs, image, namespaced RBAC, pod templates |
| API unready | ES endpoint/auth/index readiness, API logs |

Never reset brokers or overwrite commits to repair a demo. Inspect resources
before redeployment; private logs must not expose credentials or raw payloads.
