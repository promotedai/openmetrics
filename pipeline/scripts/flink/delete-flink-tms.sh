#!/usr/bin/env bash
#
# Expects setup-terminal.sh to already have been run.
#
# To run:
# ~/promotedai/metrics/pipeline/scripts/flink/delete-flink-tms.sh

set -eux

if [[ -z "${NAMESPACE}" ]]; then
  echo "NAMESPACE must be set"
  exit 1
fi

for POD in $(kubectl -n "${NAMESPACE}" get pods --no-headers -o custom-columns=":metadata.name" | grep flink-taskmanager); do
  kubectl -n "${NAMESPACE}" delete pod/"${POD}" --wait=false
done
