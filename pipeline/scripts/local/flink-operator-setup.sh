#!/usr/bin/env bash

source "$(dirname "$0")/../../../config/config.sh"

printf "Starting setup of Flink Operator...\n"

helm repo remove flink-operator-repo
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.6.1/
if [ $? -ne 0 ]; then
  printf "Failed!\n"
  exit 1
fi

helm install -n $LOCAL_K8S_NAMESPACE --set "webhook.create=false,operatorPod.resources.requests.memory=1024Mi,operatorPod.resources.limits.memory=1024Mi,watchNamespaces[0]=metrics" flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator
if [ $? -ne 0 ]; then
  printf "Failed!\n"
  exit 1
fi

printf "Success!\n"

echo "Now, go setup some local jobs: "
echo "  make local-validate-enrich-job-setup"
echo "  make local-raw-output-job-setup"
echo "  make local-flat-output-job-setup"
echo "  make local-counter-job-setup"
