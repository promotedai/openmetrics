#!/usr/bin/env bash

source "$(dirname "$0")/../../config/config.sh"

# Metrics API
printf "Starting setup of \033[1mMetrics API\033[0m..."
[ $DEBUG ] && set -x
cd $PROMOTED_DIR/metrics/api
npm install
kubectl apply -f $PROMOTED_DIR/metrics/api/kubernetes/metrics-api-local-deployment.yaml -n $LOCAL_K8S_NAMESPACE
kubectl apply -f $PROMOTED_DIR/metrics/api/kubernetes/metrics-api-local-service.yaml -n $LOCAL_K8S_NAMESPACE
sleep 5
# Quick test with a fake insertion ID
output=$(curl -X POST -d '{ "body": "{\"platformId\": \"1\", \"userId\": \"1\", \"impression\": [{ \"insertionId\": \"73f26a37-0b41-4c83-a679-645a9eff140b\" }]}" }' http://$(minikube ip):30001/2015-03-31/functions/main/invocations)
printf "$output"
set +x

response_code=$( echo "$output" | grep statusCode | jq ".statusCode" )
if [[ $response_code -eq 200 ]]; then
  printf "\033[1msuccessful!\033[0m\n"
else
  printf "\033[1mfailed\033[0m. Exiting!\n"
  exit 1
fi
