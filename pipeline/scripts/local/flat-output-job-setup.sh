#!/usr/bin/env bash

source "$(dirname "$0")/../../../config/config.sh"

printf "Creating Flink FlatOutputJob..."
eval $(minikube -p minikube docker-env)
bazel run src/main/java/ai/promoted/metrics/logprocessor/job/join:FlatOutputJob_image -- --norun
if [ $? -ne 0 ]; then
  printf "Failed!\n"
  exit 1
else
  printf "Success!\n"
fi

printf "Creating Flink FlatOutputJob jobs..."
kubectl apply --namespace $LOCAL_K8S_NAMESPACE -f $PROMOTED_DIR/metrics/pipeline/kubernetes/local/flat-output-job.yaml
if [ $? -ne 0 ]; then
  printf "Failed!\n"
  exit 1
else
  printf "Done!\n"
fi
