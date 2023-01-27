#!/usr/bin/env bash

source "$(dirname "$0")/../../../config/config.sh"
eval $(minikube -p minikube docker-env)

printf "Deleting Redis..."
helm uninstall -n ${LOCAL_K8S_NAMESPACE} redis
kubectl get pvc -n ${LOCAL_K8S_NAMESPACE} | grep redis |  awk 'NR>0{print $1}' | xargs kubectl delete pvc -n ${LOCAL_K8S_NAMESPACE}
kubectl get pv -n ${LOCAL_K8S_NAMESPACE} | grep redis | awk 'NR>0{print $1}' | xargs kubectl delete pv -n ${LOCAL_K8S_NAMESPACE}
printf "Done!\n"
