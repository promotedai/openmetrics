#!/usr/bin/env bash

source "$(dirname "$0")/../../../config/config.sh"

printf "Deleting Flink SqlRunnerJob..."
kubectl delete -k kubernetes/_envs/prm/sql-runner/local/blue
printf "Done!\n"
