#!/usr/bin/env bash

source "$(dirname "$0")/../../config/config.sh"

printf "Deleting Metrics Kafka-ui helm chart..."
helm delete kafka-ui -n $LOCAL_K8S_NAMESPACE || true
printf "Deleting Metrics Kafka helm chart..."
helm delete kafka -n $LOCAL_K8S_NAMESPACE || true
printf "Done!\n"
printf "Deleting pvc and pv used by Metrics Kafka..."
kubectl get pvc -n $LOCAL_K8S_NAMESPACE | awk 'NR>1{print $1}' | egrep '(kafka)' | xargs kubectl delete pvc -n $LOCAL_K8S_NAMESPACE
# Cleanup PV in case reclaim policy is different than Delete
kubectl get pv -n $LOCAL_K8S_NAMESPACE | egrep '(kafka)' | awk '{print $1}' | xargs kubectl delete pv -n $LOCAL_K8S_NAMESPACE
printf "Done!\n"
