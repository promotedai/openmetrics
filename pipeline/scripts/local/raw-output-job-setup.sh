#!/usr/bin/env bash

source "$(dirname "$0")/../../../config/config.sh"

printf "Creating Flink RawOutputJob..."
eval $(minikube -p minikube docker-env)

build_arm_opt=""
ARCH=$(uname -m)
if [[ "$ARCH" == "arm"* ]]; then
  printf "\nBuilding for $ARCH\n"
  build_arm_opt="--//pipeline/src/main/java/ai/promoted/metrics/logprocessor/job/raw:image-arch=arm"
fi

bazel run src/main/java/ai/promoted/metrics/logprocessor/job/raw:RawOutputJob_image ${build_arm_opt} -- --norun
if [ $? -ne 0 ]; then
  printf "Failed!\n"
  exit 1
else
  printf "Success!\n"
fi

printf "Creating Flink RawOutputJob jobs..."
kubectl apply -k kubernetes/_envs/prm/raw-output/local/blue
if [ $? -ne 0 ]; then
  printf "Failed!\n"
  exit 1
else
  printf "Done!\n"
fi
