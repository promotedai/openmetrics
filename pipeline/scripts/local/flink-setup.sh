#!/usr/bin/env bash

source "$(dirname "$0")/../../../config/config.sh"

printf "Starting setup of Flink...\n"
eval $(minikube -p minikube docker-env)

printf "Flink install using Riskfocus helm chart..."
kubectl apply --namespace $LOCAL_K8S_NAMESPACE -f $PROMOTED_DIR/metrics/pipeline/kubernetes/common/flink-cli-config.yaml
helm repo add riskfocus https://riskfocus.github.io/helm-charts-public
helm repo update
helm install --namespace $LOCAL_K8S_NAMESPACE flink riskfocus/flink -f $PROMOTED_DIR/metrics/pipeline/kubernetes/common/flink-values.yaml -f $PROMOTED_DIR/metrics/pipeline/kubernetes/local/flink-values.yaml
if [ $? -ne 0 ]; then
  printf "Failed!\n"
  exit 1
else
  printf "Complete!\n"
fi

printf "Verifying access to Flink"
SECONDS=0
until kubectl rollout status statefulset/flink-jobmanager -n $LOCAL_K8S_NAMESPACE; do
  sleep 1
  if [ $SECONDS -gt 90 ]; then
    echo >&2 "Flink setup is taking too long(>90s). Timing out!."
    exit 1
  fi
done
printf "Success!\n"

echo "Now, go setup some local jobs: "
echo "  make local-raw-output-job-setup"
echo "  make local-flat-output-job-setup"
echo "  make local-counter-job-setup"
