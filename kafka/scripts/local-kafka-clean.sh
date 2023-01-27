#!/usr/bin/env bash

source "$(dirname "$0")/../../config/config.sh"

printf "Deleting Metrics Kafka and Zookeeper helm chart..."
helm delete kafka -n $LOCAL_K8S_NAMESPACE || true
helm delete zookeeper -n $LOCAL_K8S_NAMESPACE || true
printf "Done!\n"
printf "Deleting pvc and pv used by Metrics Kafka and Zookeeper..."
kubectl get pvc -n $LOCAL_K8S_NAMESPACE | awk 'NR>1{print $1}' | egrep '(zookeeper|kafka)' | xargs kubectl delete pvc -n $LOCAL_K8S_NAMESPACE
# Cleanup PV in case reclaim policy is different than Delete
kubectl get pv -n $LOCAL_K8S_NAMESPACE | egrep '(zookeeper|kafka)' | awk '{print $1}' | xargs kubectl delete pv -n $LOCAL_K8S_NAMESPACE
printf "Done!\n"
