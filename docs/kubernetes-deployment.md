# Reproducible Kubernetes deployment

## Policy and versions

The initial platform campaign failed its old growth guard. Its receipts are
preserved. The dated [20K.1 amendment](../workloads/results/phase20-k8s/closure-policy-amendment.json)
authorizes measured adjustments of the dedicated VM only. Initial installation
uses `platform/lima.yaml` (6 GiB); accepted demonstration uses
`platform/lima-closure.yaml` (7 GiB). Never point these commands at Lima `k`.
All software/image pins are in `infra/k8s/versions.json`.

## Fresh platform

On an equivalent ARM64 Mac with pinned Lima 2.2.0, from a clean repository:

```sh
export PATH="$PWD/.tools/lima/bin:$PWD/.tools/k8s:$PATH"
python3 infra/k8s/scripts/bootstrap.py --install
export LIMA_HOME="$HOME/.configstream-k8s-lima"
export KUBECONFIG="$PWD/.cache/phase20-k8s/kubeconfig"
kubectl get nodes -o wide
kubectl apply -k infra/k8s/base
.venv/bin/python infra/k8s/scripts/create_credentials.py
helm pull strimzi-kafka-operator --repo https://strimzi.io/charts --version 0.51.0
# Verify chart hash from versions.json before install:
shasum -a 256 strimzi-kafka-operator-helm-3-chart-0.51.0.tgz
helm install strimzi strimzi-kafka-operator-helm-3-chart-0.51.0.tgz \
  -n cs-infra -f infra/k8s/values/strimzi.yaml --wait --timeout 5m
kubectl apply -k infra/k8s/overlays/local
kubectl wait -n cs-infra kafka/configstream --for=condition=Ready --timeout=300s
```

Bootstrap uses a short private Lima home (workspace socket paths exceeded UNIX
limits). Its initial safety receipt is exclusive; do not reset old receipts.
MinIO must be locally imported before its StatefulSet is Ready. Build the pinned
public source with `infra/docker/minio/Dockerfile`; copying an image is permitted,
copying old volumes is not. The accepted image/config digest is in versions.json.

Install checksum-verified ARM64 BuildKit v0.25.1 in the dedicated VM using its
official release; its worker must target `/run/k3s/containerd/containerd.sock`,
namespace `k8s.io`, not old Docker. With repository source copied to the guest,
a fresh MinIO build uses:

```sh
sudo buildctl build --frontend dockerfile.v0 \
  --local context=/tmp/configstream-source \
  --local dockerfile=/tmp/configstream-source/infra/docker/minio \
  --output type=image,name=docker.io/library/configstream-minio:RELEASE.2025-10-15T17-29-55Z,push=false
```

Use the same containerd worker configuration as `scripts/build.py`. Confirm
MinIO readiness before any seed. Preserve AGPL corresponding-source obligations.

## New authorized campaign and image

At an idle boundary inspect host pressure/disk and stop the dedicated VM, then
`limactl edit configstream-k8s --memory 7 --cpus 3` and restart. This is the
accepted measured local profile, not permission to overcommit another host.
Start the guard before builds/jobs, in a separate terminal:

```sh
.venv/bin/python infra/k8s/scripts/safety.py --campaign YOUR-NEW-ID
# Another terminal, same PATH/LIMA_HOME/KUBECONFIG:
CAMPAIGN=".cache/phase20-k8s-closure/YOUR-NEW-ID"
.venv/bin/python infra/k8s/scripts/build.py --campaign-directory "$CAMPAIGN"
IMAGE="configstream-k8s:$(git rev-parse HEAD)"
kubectl apply -k infra/k8s/overlays/closure
```

A clean Git source archive and locked dependencies are mandatory. The accepted
executable is SHA d24e7c460905d0d01f3edd8c119d0440a681ff8c; a fresh revision/image
requires its own exact fixture validation. Local SparkPi in the Dockerfile only
prepares connectors; native jobs below are deployment proof.

## Owned application and recovery

```sh
.venv/bin/python infra/k8s/scripts/submit.py seed --image "$IMAGE" --run-id seed01 --campaign-directory "$CAMPAIGN" --environment configstream-closure-env
.venv/bin/python infra/k8s/scripts/submit.py stream --image "$IMAGE" --run-id stream01 --campaign-directory "$CAMPAIGN" --environment configstream-closure-env
.venv/bin/python infra/k8s/scripts/submit.py batch --image "$IMAGE" --run-id batch01 --campaign-directory "$CAMPAIGN" --environment configstream-closure-env
```

Wait for each Job **and its real native driver** result before the next command:
`kubectl wait -n cs-processing job/cs-batch-batch01 --for=condition=complete --timeout=900s`.
Jobs have no automatic retries. Inspect `spark-role=driver` and `executor`
pods and independently checked `/evidence/closure-20k1/batch-result.json`.

Prepare a separate recovery prefix with the helper (existing conflicting configs
are rejected, not overwritten):

```sh
.venv/bin/python infra/k8s/scripts/fixture_environment.py --campaign-directory "$CAMPAIGN" --prefix closure-20k1-recovery --environment closure-recovery-env
```

Seed that environment, then submit `fail-stream` (expected failed driver), prove
commit present/checkpoint ACK absent, submit `stream` with the SAME environment
and checkpoint, then `batch` and `integrity`. Replay the main `seed`/`stream`/
`batch` sequence: physical delivery count grows, logical identities must not.
For four roles use a fresh `closure-...` prefix/helper environment and submit
`roles`; native existing processors perform oracle/replay/source finalization.
Never reuse another scientific source/checkpoint namespace.

## Serving and readback

```sh
.venv/bin/python infra/k8s/scripts/serving_secrets.py
kubectl apply -k infra/k8s/overlays/demo
kubectl rollout status statefulset/elasticsearch -n cs-app --timeout=300s
kubectl port-forward -n cs-app svc/elasticsearch 19200:9200 --address 127.0.0.1
# Another shell initializes private credentials, reader role and indices:
.venv/bin/python infra/k8s/scripts/serving_secrets.py --initialize-url http://127.0.0.1:19200
```

Prepare a main-prefix environment with `fixture_environment.py --search`; run
`batch --search-writer` using it. Existing batch indexes verified configuration
metadata only. Deployment image is explicitly pinned; adjust it to a newly built
validated image before apply if reproducing from another clean SHA. Wait for API
readiness and query through localhost as in [demo](kubernetes-demo.md).

For read-only evidence access use helper `--inspector` (one-hour disposable pod).
On expiry recreate only that diagnostic pod, never its evidence PVC.
Record inventories before safe pod replacement; replace only the dedicated
owned broker/MinIO/ES pods, wait Ready, compare offsets/object digests/API again.
Faults belong exclusively in disposable owned prefixes.

## Stop/start/redeploy

```sh
LIMA_HOME="$HOME/.configstream-k8s-lima" limactl stop configstream-k8s
LIMA_HOME="$HOME/.configstream-k8s-lima" limactl start configstream-k8s
kubectl diff -k infra/k8s/overlays/demo
kubectl apply -k infra/k8s/overlays/demo
```

Never use volume deletion, broker reset or `limactl delete` for ordinary cleanup.
Guard stop now preserves completed/failed pod evidence and targets active work
only; the initial late reporting cleanup defect remains documented.
