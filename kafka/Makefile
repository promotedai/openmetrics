#!make
MAKEFLAGS += --silent
export SHELL=/bin/bash

local: local-setup local-check

local-setup:
	bash scripts/local-setup.sh

local-check:
	bash scripts/local-check.sh

clean-local:
	bash scripts/local-clean.sh

# Setup Zookeeper and Kafka.
local-kafka-setup:
	bash scripts/local-kafka-setup.sh

local-kafka-clean:
	bash scripts/local-kafka-clean.sh
