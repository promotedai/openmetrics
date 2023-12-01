#!/usr/bin/env bash


./scripts/local/raw-output-job-clean.sh
./scripts/local/flat-output-job-clean.sh
./scripts/local/counter-job-clean.sh
./scripts/local/content-metrics-job-clean.sh

./scripts/local/flink-operator-clean.sh
./scripts/local/redis-clean.sh
./scripts/local/minio-clean.sh
