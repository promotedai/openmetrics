#!/usr/bin/env bash

source "$(dirname "$0")/../../../config/config.sh"

printf "Deleting fake ContentGenerator deployment..."
(kubectl delete -n $LOCAL_K8S_NAMESPACE -f $PROMOTED_DIR/metrics/pipeline/kubernetes/local/fake-content-generator.yaml) || true
(kubectl delete -n $LOCAL_K8S_NAMESPACE -f $PROMOTED_DIR/metrics/pipeline/kubernetes/common/log-generator-config.yaml) || true
printf "Done!\n"
