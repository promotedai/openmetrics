#!/usr/bin/env bash
source "$(dirname "$0")/../../config/config.sh"
export GO111MODULE=on
bazel run main:EventAPI_image
