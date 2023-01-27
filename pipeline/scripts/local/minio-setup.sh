#!/usr/bin/env bash

source "$(dirname "$0")/../../../config/config.sh"

eval $(minikube -p minikube docker-env)
printf "Creating MinIO...\n"
helm repo add minio https://charts.min.io/
helm install --namespace ${LOCAL_K8S_NAMESPACE} --set rootUser=YOURACCESSKEY,rootPassword=YOURSECRETKEY -f ${PROMOTED_DIR}/metrics/pipeline/kubernetes/local/minio-values.yaml minio minio/minio
printf "Done!\n"
