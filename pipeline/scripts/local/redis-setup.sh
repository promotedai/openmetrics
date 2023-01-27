#!/usr/bin/env bash

rootdir="$(dirname "$0")/../../.."
source "${rootdir}/config/config.sh"
eval $(minikube -p minikube docker-env)

printf "Creating Redis..."
helm repo add bitnami https://charts.bitnami.com/bitnami
helm install -n ${LOCAL_K8S_NAMESPACE} -f "${rootdir}/pipeline/kubernetes/local/redis.yaml" redis bitnami/redis
printf "Done!\n"
