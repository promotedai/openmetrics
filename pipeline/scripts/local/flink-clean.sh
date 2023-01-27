#!/usr/bin/env bash

source "$(dirname "$0")/../../../config/config.sh"

./scripts/local/raw-output-job-clean.sh
./scripts/local/flat-output-job-clean.sh
./scripts/local/counter-job-clean.sh
./scripts/local/content-metrics-job-clean.sh

printf "Deleting Flink deployment..."
helm uninstall flink --namespace $LOCAL_K8S_NAMESPACE || true
(kubectl delete --namespace $LOCAL_K8S_NAMESPACE -f $PROMOTED_DIR/metrics/pipeline/kubernetes/common/flink-cli-config.yaml) || true
printf "Done!\n"
printf "Cleaning up pvc and pv used by Flink..."
(kubectl get pvc -n $LOCAL_K8S_NAMESPACE | awk 'NR>1{print $1}' | grep flink | xargs kubectl delete pvc -n $LOCAL_K8S_NAMESPACE) || true
# Cleanup PV in case reclaim policy is different than Delete
(kubectl get pv -n $LOCAL_K8S_NAMESPACE | grep flink | awk 'NR>1{print $1}' | xargs kubectl delete pv -n $LOCAL_K8S_NAMESPACE) || true
printf "Done!\n"
