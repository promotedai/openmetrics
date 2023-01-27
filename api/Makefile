.PHONY: build deploy

# Description of Go Serverless integration.
# https://docs.google.com/document/d/14Xt-xGxWNAao4Is033n2dlO7fOJ9zjxV9GLZS1Je2qw/edit#heading=h.m9b2f6ohmg4t

build-local:
	./scripts/build-local.sh

build-deploy:
	export GO111MODULE=on; \
	if [ -a .serverless/event-api.zip ]; then rm -rf .serverless/event-api.zip; fi; \
	mkdir -p .serverless; \
	rm -rf bin; mkdir -p bin; \
	cp "$(shell bazel run --run_under "echo" main:main)" bin/main

access:
	chmod -R u+x ./scripts/

api: local-api

local-invoke: access build-local
	./scripts/localinvoke.sh 'event-api' 'bazel/api/main:EventAPI_image' '../config/local.json' '$(event)'

local-keep-running: access build-local
	./scripts/localkeeprunning.sh 'event-api' 'bazel/api/main:EventAPI_image' '../config/local.json'

test:
	bazel test ... --test_output=errors

testwatch:
	ibazel test ... --test_output=errors

benchmark:
	bazel run main:go_default_test -- --test.bench=.

local: local-setup

local-setup: access build-local
	bash scripts/local-setup.sh

local-check:
	bash scripts/local-check.sh

clean-local:
	bash scripts/local-clean.sh
