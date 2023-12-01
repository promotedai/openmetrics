#!/usr/bin/env bash
#
# Expects setup-terminal.sh to already have been run.
#
# To run:
# ~/promotedai/metrics/pipeline/scripts/flink/delete-flink-tm-pvcs.sh

set -eux

if [[ -z "${NAMESPACE}" ]]; then
  echo "NAMESPACE must be set"
  exit 1
fi

PODS=$(kubectl -n "${NAMESPACE}" get pods --no-headers -o custom-columns=":metadata.name" | grep flink-taskmanager)

for PVC in $(kubectl -n "${NAMESPACE}" get pvc --no-headers -o custom-columns=":metadata.name" | grep flink-taskmanager); do
  POD_PVC=${PVC/taskmanager-data-/}

  if [[ $PODS == *"$POD_PVC"* ]]; then
    echo "Skipping pvc \"${PVC}\" since the pod \"$POD_PVC\" still exists"
  else
    kubectl -n "${NAMESPACE}" delete pvc/"${PVC}" --wait=false
  fi
done
