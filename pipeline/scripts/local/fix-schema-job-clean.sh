#!/usr/bin/env bash

source "$(dirname "$0")/../../../config/config.sh"

printf "Deleting Flink FixSchemaJob..."
kubectl delete --namespace $LOCAL_K8S_NAMESPACE -f $PROMOTED_DIR/metrics/pipeline/kubernetes/local/fix-schema-job.yaml || true
printf "Done!\n"

printf "Deleting Flink FixSchemaJob jobs..."
JOBMANAGER=`kubectl get pods --namespace $LOCAL_K8S_NAMESPACE | grep flink-jobmanager-0 | awk '{print $1}'`
if [ -z "$JOBMANAGER" ]
then
      printf "No Flink JobManager running\n"
      printf "Done!\n"
      exit 0
fi

# $1 = the job ID
delete_flink_job () {
  JOB_ID=`kubectl exec -it $JOBMANAGER --namespace $LOCAL_K8S_NAMESPACE --request-timeout=5s -- flink list --jobmanager localhost:8081 | grep $1 | awk '{print $4}'`
  if [ -z "$JOB_ID" ]
  then
        printf "No '$1' Flink job running\n"
  else
        # Using `stop` fails when the job is regularly restarting.
        kubectl exec -it $JOBMANAGER --namespace $LOCAL_K8S_NAMESPACE --request-timeout=5s -- flink cancel $JOB_ID --jobmanager localhost:8081 || true
  fi
}

delete_flink_job fix-schema-delivery-log || true

printf "Done!\n"
