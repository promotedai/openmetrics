#!/usr/bin/env bash
#
# Copies jobmanager logs to local tmp disk.
#
# Supported args:
# -t <num> = Only get the last X lines of a file.  This is encouraged since 
#
# Warning: when trying to copy the whole file, you will probably hit "file changed as we read it".

# For debugging: set -eux
set -eu

flink_pods=$(kubectl get pods --no-headers -o custom-columns=":metadata.name"  | grep flink-)

echo "$flink_pods" | while IFS= read -r flink_pod
do
  echo "*****************************************"
  echo "kubectl exec $flink_pod -- df"
  echo "$(kubectl exec $flink_pod -- df)"
done