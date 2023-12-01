#!/usr/bin/env bash

source "$(dirname "$0")/../../../config/config.sh"

printf "Deleting Flink Operator..."
helm uninstall -n $LOCAL_K8S_NAMESPACE flink-kubernetes-operator
