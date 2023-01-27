#!/usr/bin/env bash

source "$(dirname "$0")/../../../config/config.sh"

printf "Deleting fake LogGenerator deployment..."
(kubectl delete -n $LOCAL_K8S_NAMESPACE -f $PROMOTED_DIR/metrics/pipeline/kubernetes/local/log-generator-deployment.yaml) || true
(kubectl delete -n $LOCAL_K8S_NAMESPACE -f $PROMOTED_DIR/metrics/pipeline/kubernetes/common/log-generator-config.yaml) || true
printf "Done!\n"
