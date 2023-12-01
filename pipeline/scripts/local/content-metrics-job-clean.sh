#!/usr/bin/env bash

source "$(dirname "$0")/../../../config/config.sh"

printf "Deleting Flink ContentMetricsJob..."
kubectl delete -k kubernetes/_envs/prm/content-metrics/local/blue
printf "Done!\n"

printf "Deleting Flink ContentMetricsJob jobs..."
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

# TODO - fix this to have the correct job name.  Flink SQL creates a dynamic job name.
delete_flink_job content-metrics || true

printf "Done!\n"
