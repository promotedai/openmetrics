#!/usr/bin/env bash

source "$(dirname "$0")/../../../config/config.sh"

printf "Deleting Flink Sql-gateway-session cluster..."
kubectl delete -k kubernetes/_envs/prm/sql-gateway-session/local/_base
printf "Done!\n"
