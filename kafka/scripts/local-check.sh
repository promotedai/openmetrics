#!/usr/bin/env bash

source "$(dirname "$0")/../../config/config.sh"

function ephemeral_port() {
  # Source - https://unix.stackexchange.com/questions/55913/whats-the-easiest-way-to-find-an-unused-local-port
  local low_bounce=49152
  local range=16384
  while true; do
    candidate=$(($low_bound + ($RANDOM % $range)))
    (echo "" >/dev/tcp/127.0.0.1/${candidate}) >/dev/null 2>&1
    if [ $? -ne 0 ]; then
      echo $candidate
      break
    fi
  done
}

printf "Verifying if Metrics Zookeeper is accessible and healthy..."
zk_temp_port="$(ephemeral_port)"
(kubectl port-forward service/zookeeper -n $LOCAL_K8S_NAMESPACE $zk_temp_port:2181 >/dev/null 2>&1) &
kubectl_pid=$!
sleep 5 # to avoid race

zk_health_check=$(echo ruok | nc localhost $zk_temp_port)

if [[ $zk_health_check == "imok" ]]; then
  printf "Success!\n"
else
  printf "Failed. You may want to clean and set up again!\n"
  exit 1
fi
sleep 1
#Kill temporary Zookeeper port forward
kill $kubectl_pid

printf "Verifying if Metrics Kafka is accessible..."

nc -z $MINIKUBE_IP $LOCAL_KAFKA_FIRST_PORT

if [[ $? -eq 0 ]]; then
  printf "Success!\n"
else
  printf "Failed. You may want to clean and set up again!\n"
  exit 1
fi
