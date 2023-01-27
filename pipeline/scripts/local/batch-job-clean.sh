#!/usr/bin/env bash

source "$(dirname "$0")/../../../config/config.sh"

printf "Deleting Flink BatchLogProcessor job..."
kubectl delete --namespace $LOCAL_K8S_NAMESPACE -f $PROMOTED_DIR/metrics/pipeline/kubernetes/local/batch-log-processor-job.yaml || true
printf "Done!\n"
printf "Deleting Flink Job..."
JOBMANAGER=`kubectl get pods --namespace $LOCAL_K8S_NAMESPACE | grep flink-jobmanager-0 | awk '{print $1}'`
if [ -z "$JOBMANAGER" ]
then
      printf "No Flink jobmanager running\n"
      printf "Done!\n"
      exit 0
fi
JOB_ID=`kubectl exec -it $JOBMANAGER --namespace $LOCAL_K8S_NAMESPACE -- flink list --jobmanager localhost:8081 | grep BatchLogProcessor | awk '{print $4}'`
if [ -z "$JOB_ID" ]
then
      printf "No Flink Job running\n"
      printf "Done!\n"
      exit 0
fi
kubectl exec -it $JOBMANAGER --namespace $LOCAL_K8S_NAMESPACE -- flink cancel $JOB_ID --jobmanager localhost:8081 || true
printf "Done!\n"
