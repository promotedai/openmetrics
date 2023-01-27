#!/usr/bin/env bash

source "$(dirname "$0")/../../config/config.sh"

printf "Starting installation of Zookeeper helm chart...\n"
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update
helm install -n $LOCAL_K8S_NAMESPACE --set replicaCount=1,volumePermissions.enabled=true \
  zookeeper bitnami/zookeeper
SECONDS=0
until kubectl rollout status statefulset/zookeeper -n $LOCAL_K8S_NAMESPACE; do
  printf "."
  sleep 1
  if [ $SECONDS -gt 60 ]; then
    echo >&2 "Zookeeper setup is taking too long(>60s). Timing out!."
    exit 1
  fi
done
printf "Success!\n"

printf "Starting installation of Kafka helm chart...\n"
if [[ "$OSTYPE" == "darwin"* ]]; then
  gsed -i "s/host.docker.internal/$MINIKUBE_IP/" $PROMOTED_DIR/metrics/kafka/kubernetes/kafka-local.yaml
  gsed -i "s/31090/$LOCAL_KAFKA_FIRST_PORT/" $PROMOTED_DIR/metrics/kafka/kubernetes/kafka-local.yaml
else
  sed -i "s/host.docker.internal/$MINIKUBE_IP/" $PROMOTED_DIR/metrics/kafka/kubernetes/kafka-local.yaml
  sed -i "s/31090/$LOCAL_KAFKA_FIRST_PORT/" $PROMOTED_DIR/metrics/kafka/kubernetes/kafka-local.yaml
fi
helm install -n $LOCAL_K8S_NAMESPACE --set "externalAccess.enabled=true,externalAccess.service.nodePorts={$LOCAL_KAFKA_FIRST_PORT},updateStrategy.type=RollingUpdate,externalZookeeper.servers={zookeeper}" -f $PROMOTED_DIR/metrics/kafka/kubernetes/kafka-local.yaml kafka bitnami/kafka
SECONDS=0
until kubectl rollout status statefulset/kafka -n $LOCAL_K8S_NAMESPACE; do
  printf "."
  sleep 1
  if [ $SECONDS -gt 60 ]; then
    echo >&2 "Kafka setup is taking too long(>60s). Timing out!."
    exit 1
  fi
done
printf "Success!\n"
