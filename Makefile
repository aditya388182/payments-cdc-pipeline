# Payments CDC pipeline + on-call console. Targets that need the stack use oncall/tools/stack.sh.
SHELL := /bin/bash
PATH  := $(HOME)/.local/bin:$(HOME)/go/bin:$(PATH)
PY    := venv/bin/python
export PYTHONPATH := $(CURDIR)
export PYSPARK_PYTHON := $(CURDIR)/$(PY)
export PYSPARK_DRIVER_PYTHON := $(CURDIR)/$(PY)

.PHONY: help env oncall-up oncall-down oncall-status oncall-e2e proto ws-load bullets results

help:
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | sed 's/:.*##/ -/'

env: ## generate a git-ignored .env with random local dev values (never overwrites)
	oncall/tools/gen_env.sh

oncall-up: ## bring up the data plane, replay-svc, mock GitHub and the Go API (loopback only)
	oncall/tools/stack.sh up

oncall-down: ## stop the console and pipeline processes (add ARGS=--all to stop containers too)
	oncall/tools/stack.sh down $(ARGS)

oncall-status: ## what is running
	oncall/tools/stack.sh status

oncall-e2e: ## full local end-to-end run: mock OAuth + Playwright on the Expo web build, screenshots, results/oncall/e2e.json
	oncall/e2e/run.sh

proto: ## regenerate the Python and Go protobuf stubs
	cd oncall/proto && buf lint && buf generate --template buf.gen.go.yaml
	$(PY) -m grpc_tools.protoc -I oncall/proto --python_out=oncall/replay/gen --grpc_python_out=oncall/replay/gen oncall/v1/replay.proto

ws-load: ## k6 WebSocket load test, 1,000 clients (+ slow-client variant), results/oncall/ws_load.json
	oncall/loadtest/run.sh

bullets: ## print the resume bullets with numbers from results/oncall/*.json (MISSING for absent keys)
	$(PY) oncall/tools/bullets.py

results: ## regenerate the results files that do not need the full stack (tests.json, schema_gate.json, audit)
	$(PY) scripts/check_schema_compat.py --self-test --json results/oncall/schema_gate.json $(if $(REGISTRY),--registry $(REGISTRY))
	$(PY) oncall/tools/make_tests_json.py
