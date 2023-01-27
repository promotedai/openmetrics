#!/usr/bin/env bash

source "$(dirname "$0")/../../../config/config.sh"

printf "Deleting MinIO..."
# Remove pod
helm uninstall --namespace ${LOCAL_K8S_NAMESPACE} minio
# Remove pvc & pv
kubectl get pvc -n $LOCAL_K8S_NAMESPACE | grep minio | awk 'NR>0{print $1}' | xargs kubectl delete pvc -n $LOCAL_K8S_NAMESPACE
printf "Done!\n"
