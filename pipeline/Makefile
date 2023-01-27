#!make
MAKEFLAGS += --silent
export SHELL=/bin/bash

local: local-setup local-check

local-setup:
	bash scripts/local/setup.sh

local-check:
	bash scripts/local/check.sh

clean-local:
	bash scripts/local/clean.sh

local-minio-setup:
	bash scripts/local/minio-setup.sh

local-minio-clean:
	bash scripts/local/minio-clean.sh

local-flink-setup:
	bash scripts/local/flink-setup.sh

local-flink-clean:
	bash scripts/local/flink-clean.sh

local-raw-output-job-setup:
	bash scripts/local/raw-output-job-setup.sh

local-raw-output-job-clean:
	bash scripts/local/raw-output-job-clean.sh

local-flat-output-job-setup:
	bash scripts/local/flat-output-job-setup.sh

local-flat-output-job-clean:
	bash scripts/local/flat-output-job-clean.sh

local-counter-job-setup:
	bash scripts/local/counter-job-setup.sh

local-counter-job-clean:
	bash scripts/local/counter-job-clean.sh

local-content-metrics-job-setup:
	bash scripts/local/content-metrics-job-setup.sh

local-content-metrics-job-clean:
	bash scripts/local/content-metrics-job-clean.sh

local-fake-log-generator-setup:
	bash scripts/local/fake-log-generator-setup.sh

# Used to stop the fake LogGenerator job.
local-fake-log-generator-clean:
	bash scripts/local/fake-log-generator-clean.sh

local-fake-content-generator-setup:
	bash scripts/local/fake-content-generator-setup.sh

# Used to stop the fake ContentGenerator job.
local-fake-content-generator-clean:
	bash scripts/local/fake-content-generator-clean.sh

local-batch-job-setup:
	bash scripts/local/batch-job-setup.sh

# Used to stop the running Flink BatchLogIngestion job and delete the kubectl job.
local-batch-job-clean:
	bash scripts/local/batch-job-clean.sh

local-redis-setup:
	bash scripts/local/redis-setup.sh

local-redis-clean:
	bash scripts/local/redis-clean.sh
