#!/usr/bin/env bash
set -euo pipefail
root=$(git rev-parse --show-toplevel)
tools="$root/.tools/k8s-validation"
mkdir -p "$tools"
cd "$tools"
curl -fLsS --connect-timeout 15 --max-time 120 -o kustomize_v5.7.1_linux_amd64.tar.gz \
  https://github.com/kubernetes-sigs/kustomize/releases/download/kustomize%2Fv5.7.1/kustomize_v5.7.1_linux_amd64.tar.gz
curl -fLsS --connect-timeout 15 --max-time 60 -o kustomize-checksums \
  https://github.com/kubernetes-sigs/kustomize/releases/download/kustomize%2Fv5.7.1/checksums.txt
grep ' kustomize_v5.7.1_linux_amd64.tar.gz$' kustomize-checksums | sha256sum -c -
tar -xzf kustomize_v5.7.1_linux_amd64.tar.gz kustomize
curl -fLsS --connect-timeout 15 --max-time 120 -o helm-v3.19.2-linux-amd64.tar.gz \
  https://get.helm.sh/helm-v3.19.2-linux-amd64.tar.gz
curl -fLsS --connect-timeout 15 --max-time 60 -o helm-checksums \
  https://get.helm.sh/helm-v3.19.2-linux-amd64.tar.gz.sha256sum
sha256sum -c helm-checksums
tar -xzf helm-v3.19.2-linux-amd64.tar.gz linux-amd64/helm
curl -fLsS --connect-timeout 15 --max-time 120 -o kubeconform-linux-amd64.tar.gz \
  https://github.com/yannh/kubeconform/releases/download/v0.7.0/kubeconform-linux-amd64.tar.gz
curl -fLsS --connect-timeout 15 --max-time 60 -o kubeconform-checksums \
  https://github.com/yannh/kubeconform/releases/download/v0.7.0/CHECKSUMS
grep ' kubeconform-linux-amd64.tar.gz$' kubeconform-checksums | sha256sum -c -
tar -xzf kubeconform-linux-amd64.tar.gz kubeconform
./kustomize build "$root/infra/k8s/base" > base.yaml
./kustomize build "$root/infra/k8s/overlays/local" > local.yaml
./kubeconform -strict -summary -kubernetes-version 1.35.0 base.yaml
./kustomize build "$root/infra/k8s/overlays/frontend" > frontend.yaml
./kubeconform -strict -summary -kubernetes-version 1.35.0 frontend.yaml
./linux-amd64/helm pull strimzi-kafka-operator --repo https://strimzi.io/charts --version 0.51.0
echo '1ec8bbffe5e40d645635ace9c439ac54b39639af243bae6ce5b1f9eabfc07e9b  strimzi-kafka-operator-helm-3-chart-0.51.0.tgz' | sha256sum -c -
./linux-amd64/helm lint strimzi-kafka-operator-helm-3-chart-0.51.0.tgz -f "$root/infra/k8s/values/strimzi.yaml"
