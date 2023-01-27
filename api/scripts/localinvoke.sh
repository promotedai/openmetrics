#!/usr/bin/env bash

# Args
# 1 = service name.
# 2 = image name.
# 3 = env vars json path.
# 4 = (optional) event path.

source "$(dirname "$0")/../../config/config.sh"

if [[ -z "$NAMESPACE" ]]; then
    echo "The Kubernetes NAMESPACE env variable must be set" 1>&2
    exit 1
fi

if [[ -z "${4}" ]]; then
    event="main/event.json"
else
    event=${4}
fi

event_request=$(cat ${event})

# This creates an image
env_vars=$(jq '.main | to_entries | map("\(.key)=\(.value|tostring)")|.[]' ${3} | tr -d '"' | sed 's/^/--env="/' | sed 's/$/"/')
cmd="kubectl run -i --namespace $NAMESPACE --image=${2} ${env_vars[@]} --restart=Never  ${1} main '$event_request'"

echo $cmd
# This is dangerous since it could allow args to execute code.  It's pretty safe given that we only have one argument which is a path.
eval $cmd
