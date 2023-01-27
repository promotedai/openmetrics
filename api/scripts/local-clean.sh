#!/usr/bin/env bash
source "$(dirname "$0")/../../config/config.sh"
kubectl delete pod event-api -n $LOCAL_K8S_NAMESPACE --ignore-not-found
kubectl delete deployment event-api -n $LOCAL_K8S_NAMESPACE --ignore-not-found
kubectl delete service event-api -n $LOCAL_K8S_NAMESPACE --ignore-not-found
