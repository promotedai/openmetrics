#!/usr/bin/env bash

# Args
# 1 = service name.
# 2 = image name.
# 3 = env vars json path.

source "$(dirname "$0")/../../config/config.sh"

if [[ -z "$NAMESPACE" ]]; then
    echo "The Kubernetes NAMESPACE env variable must be set" 1>&2
    exit 1
fi

cmd="kubectl delete -n $NAMESPACE pod/event-api"
echo $cmd
# This is dangerous since it could allow args to execute code.  It's pretty safe given that we only have one argument which is a path.
eval $cmd

# This creates an image
env_vars=$(jq '.main | to_entries | map("\(.key)=\(.value|tostring)")|.[]' ${3} | tr -d '"' | sed 's/^/--env="/' | sed 's/$/"/')
cmd="kubectl run --rm -i --namespace $NAMESPACE --expose=true --port=9001 --image=${2} --env=\"DOCKER_LAMBDA_STAY_OPEN=1\" ${env_vars[@]} --restart=Never  ${1} main"

echo $cmd
# This is dangerous since it could allow args to execute code.  It's pretty safe given that we only have one argument which is a path.
eval $cmd
