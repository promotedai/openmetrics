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

local-flink-operator-setup:
	bash scripts/local/flink-operator-setup.sh

local-flink-operator-clean:
	bash scripts/local/flink-operator-clean.sh

local-validate-enrich-job-setup:
	bash scripts/local/validate-enrich-job-setup.sh

local-validate-enrich-job-clean:
	bash scripts/local/validate-enrich-job-clean.sh

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

local-content-query-metrics-job-setup:
	bash scripts/local/content-query-metrics-job-setup.sh

local-content-query-metrics-job-clean:
	bash scripts/local/content-query-metrics-job-clean.sh

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

local-sql-runner-job-setup:
	bash scripts/local/sql-runner-job-setup.sh

local-sql-runner-job-clean:
	bash scripts/local/sql-runner-job-clean.sh

local-sql-gateway-session-setup:
	bash scripts/local/sql-gateway-session-setup.sh

local-sql-gateway-session-clean:
	bash scripts/local/sql-gateway-session-clean.sh

local-redis-setup:
	bash scripts/local/redis-setup.sh

local-redis-clean:
	bash scripts/local/redis-clean.sh

local-redis-cluster-setup:
	bash scripts/local/redis-cluster-setup.sh

local-redis-cluster-clean:
	bash scripts/local/redis-cluster-clean.sh
