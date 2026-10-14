# Operations and bounded resources

## Observed platform

One ARM64 K3s node is Ready. Node allocatable CPU is 2,500m and memory
4,471,488 KiB after system/kube reserves. Kafka and MinIO PVCs are Bound; the
fixture PVC uses delayed binding until a consumer is scheduled. Current evidence
is in [platform validation](../workloads/results/phase20-k8s/platform-validation.json).

Core requests exclude future Spark/ES/API. Inspect actual usage before admitting
any additional profile; requests alone are not observed memory. No Prometheus or
Grafana is installed. K3s metrics-server plus structured logs are the lightweight
profile. Dashboard visualization is absent.

```sh
kubectl get nodes,pods,pvc -A
kubectl top nodes
kubectl top pods -A
kubectl describe pod -n cs-infra POD
kubectl logs -n cs-infra POD --tail=100
kubectl get events -A --sort-by=.metadata.creationTimestamp
```

Logs remain private; publish compact counters/diagnostics, not credentials or
raw payloads. Preserve imageIDs, pod UIDs/restarts and executor lifecycle per run.

## Fail-closed campaign guard

`scripts/watch.py` uses the original receipt, host memory-pressure/disk, allocated
new VM/tool/cache bytes and guest disk/available RAM. Floors are 20 GiB free disk,
512 MiB guest available RAM and 15% host memory-pressure free. Growth includes
the pinned Ubuntu host image cache; guest blocks are not added a second time.

On failure the watcher requests deletion only of labeled owned processing
jobs/pods and SIGTERM of the new VM's BuildKit daemon. It does not delete PVCs,
Kafka/MinIO pods, old experiment data or checkpoints. BuildKit's graceful export
finished after the stop request; this is **not** a passing budget result.

## Diagnosis

| Symptom | Inspect before acting |
|---|---|
| Pending | Node allocatable vs requests, events, delayed/local-path PVC binding |
| CrashLoopBackOff | Previous container logs, command/runtime, Secret references |
| OOMKilled | Termination reason, limits and actual pod/guest memory; no implicit heap increase |
| FailedMount | PVC/PV Bound state, node placement, permissions and fsGroup |
| Kafka unavailable | Kafka Ready condition, endpoint/DNS, topic Ready and selected offsets |
| Executor failure | Driver logs, namespaced SA/RBAC, image availability, pod template and PVC |
| API unready | Verified ES endpoint/auth/index readiness; API not yet deployed here |

Never reset a broker or overwrite committed evidence to repair a demonstration.
If resources fail, stop the affected job, retain diagnostics and seek the smallest
explicit amendment. Multi-node requirements are separately gated.
