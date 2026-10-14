# Reproducible deployment

## Approval and current stop

The approved campaign is 3 CPUs, 6 GiB RAM, 48 GiB disk on a **separate** Lima
VM, 7,200 aggregate seconds and 15 GiB allocated growth. It crossed the growth
guard during image export. Do not rerun installation/build/jobs or reset its
receipt without the requested owner amendment. No VM/heap increase is approved.

Tools are installed in `.tools/k8s`. Versions and ARM64 references are in
`infra/k8s/versions.json`. The original Kafka 4.1.2 environment is unchanged;
Strimzi 0.51.0 supplies Kafka 4.1.1 in the new fixture-only environment.

## Fresh authorized installation

From the repository root on an equivalent ARM64 Mac with Lima 2.2.0:

```sh
export PATH="$PWD/.tools/lima/bin:$PWD/.tools/k8s:$PATH"
python3 infra/k8s/scripts/bootstrap.py --install
export LIMA_HOME="$HOME/.configstream-k8s-lima"
export KUBECONFIG="$PWD/.cache/phase20-k8s/kubeconfig"
kubectl get nodes -o wide
kubectl apply -k infra/k8s/base
python3 infra/k8s/scripts/create_credentials.py
helm pull strimzi-kafka-operator --repo https://strimzi.io/charts --version 0.51.0
helm install strimzi strimzi-kafka-operator-helm-3-chart-0.51.0.tgz \
  -n cs-infra -f infra/k8s/values/strimzi.yaml --wait --timeout 5m
kubectl apply -k infra/k8s/overlays/local
kubectl wait -n cs-infra kafka/configstream --for=condition=Ready --timeout=300s
```

Verify the chart SHA-256 against `scripts/validate.sh` before installation. Import
the pinned source-built MinIO image into **new** K3s containerd; never copy old
volumes. Its source and AGPL obligations are recorded in the preflight.

Bootstrap uses a short private Lima home because the previous workspace path
exceeded UNIX socket limits. `--resume-before-vm` preserves the original receipt
and is only for the recorded pre-VM bootstrap failure; it cannot reinstall an
existing VM or grant a fresh timeout.

## Application image and native jobs: pending acceptance

Build only a clean source archive, not the entire workspace/cache. The Dockerfile
locks dependencies, Java/Spark/Scala and pre-resolves connectors. BuildKit 0.25.1
uses the new K3s containerd worker; the old VM is never an image build worker.
The optimized Dockerfile after the guard failure is **not yet built/validated**.

When separately authorized and the watcher is fresh/healthy:

```sh
python3 infra/k8s/scripts/submit.py seed --image configstream-k8s:SOURCE_SHA --run-id UUID
python3 infra/k8s/scripts/submit.py stream --image configstream-k8s:SOURCE_SHA --run-id UUID
python3 infra/k8s/scripts/submit.py batch --image configstream-k8s:SOURCE_SHA --run-id UUID
```

Wait for each job's real result before the next. These are intended commands, not
executed acceptance evidence. Jobs have zero automatic retries. The streaming
driver must create a separately scheduled executor pod; image-build local SparkPi
is merely connector preparation. The batch adapter checks independently specified
owned version IDs/content/hashes. Four-domain fixture/recovery coverage is still open.

## Stop/start without deleting evidence

```sh
LIMA_HOME="$HOME/.configstream-k8s-lima" limactl stop configstream-k8s
LIMA_HOME="$HOME/.configstream-k8s-lima" limactl start configstream-k8s
```

Never use `limactl delete`, PVC deletion or Helm uninstall as a routine cleanup.
Do not point these commands at the original `k` VM. Redeployment is reviewed
`kubectl diff` followed by `kubectl apply -k`; private Secrets are not rendered into Git.
