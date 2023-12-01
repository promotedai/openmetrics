#!/usr/bin/env bash

KAFKA_BITNAMI_VERSION=25.1.8
KAFKA_UI_VERSION=0.7.0

source "$(dirname "$0")/../../config/config.sh"

helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo add kafka-ui https://provectus.github.io/kafka-ui-charts/
helm repo update

printf "Starting installation of Kafka helm chart...\n"
helm install -n $LOCAL_K8S_NAMESPACE -f $PROMOTED_DIR/metrics/kafka/kubernetes/kafka-local.yaml kafka bitnami/kafka --version ${KAFKA_BITNAMI_VERSION}
SECONDS=0
until kubectl rollout status statefulset/kafka-controller -n $LOCAL_K8S_NAMESPACE; do
  printf "."
  sleep 1
  if [ $SECONDS -gt 60 ]; then
    echo >&2 "Kafka setup is taking too long(>60s). Timing out!."
    exit 1
  fi
done
printf "Starting installation of Kafka-ui chart...\n"
helm install -n $LOCAL_K8S_NAMESPACE -f $PROMOTED_DIR/metrics/kafka/kubernetes/kafka-ui-local.yaml kafka-ui kafka-ui/kafka-ui --version ${KAFKA_UI_VERSION}
printf "Success!\n"
