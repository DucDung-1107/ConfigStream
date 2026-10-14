# Dashboard deployment on the existing K3s node

Static Next.js export is served by pinned ARM64 Nginx unprivileged. There is no
additional Node runtime in the serving pod. Next16.4.0/React19.3.0/Tailwind4.3.3,
Recharts3.8.0, TanStack Table8.21.3 and other dependencies are exactly locked.
Node24 and Nginx1.31.1 image indexes are pinned by digest in the Dockerfile.

The dedicated VM now uses3CPU/6GiB/48GiB after the recorded host-memory amendment.
Frontend requests50mCPU/32MiB and limits
500mCPU/128MiB. It runs UID101, restricted capabilities, read-only root filesystem,
bounded writable `/tmp`, no SA token, Secrets or private source mount. Service
`dashboard` is ClusterIP in `cs-app`. Existing Kafka/Spark/MinIO/ES are reused.

From a clean committed source and an idle resource-safe host:

```sh
export PATH="$PWD/.tools/lima/bin:$PWD/.tools/k8s:$PATH"
export LIMA_HOME="$HOME/.configstream-k8s-lima"
export KUBECONFIG="$PWD/.cache/phase20-k8s/kubeconfig"
.venv/bin/python infra/k8s/scripts/deploy_dashboard.py --host-static-build
kubectl rollout status deployment/dashboard -n cs-app --timeout=120s
kubectl get pods -n cs-app
kubectl port-forward -n cs-app svc/dashboard 13080:8080 --address 127.0.0.1
```

The build script registers
an exclusive new receipt, source SHA and1800-second build cap; checks20GiB disk,
512MiB available guest memory, host pressure and sustained new swap-out. It builds
the clean frontend source. The accepted path validates/builds static export on
host Node26.5.0 and packages it in pinned ARM64 Nginx with a dedicated Dockerignore.
The full Node24 container build is available in `frontend/Dockerfile`; its first
attempt stopped on sustained host swap-out during image download. The script patches
the rendered Deployment image to that exact SHA. A failed guard/build stops the
attempt and retains diagnostics; it never deletes old data or evidence.

```sh
DASHBOARD_URL=http://127.0.0.1:13080 node frontend/tests/review.mjs
cd frontend
DASHBOARD_URL=http://127.0.0.1:13080 npm run e2e
```

E2E mocks approved API responses for deterministic interface fault tests; the
review/rehearsal path uses the actual same-origin configuration API. No full
source ingestion is needed. Inspect imageIDs, current probes and metrics before
calling deployment accepted. Local-only port-forward is the approved access;
public exposure/authentication requires separate review. Stop/restart only the
dedicated VM as in the Kubernetes runbook; preserve PVCs and prior namespaces.
