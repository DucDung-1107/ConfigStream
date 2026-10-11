SHELL := /bin/sh
PYTHON ?= python3
UV := .tools/uv/bin/uv
DOCKER ?= docker
KAFKA_CONSUME_MAX_MESSAGES ?= 1
export UV_CACHE_DIR := $(CURDIR)/.cache/uv
export UV_PYTHON_INSTALL_DIR := $(CURDIR)/.tools/python
export TMPDIR := $(CURDIR)/.cache/tmp

.PHONY: setup test lint format check generator processing-demo clean
.PHONY: kafka-up kafka-down kafka-topics kafka-status kafka-demo integration-test
.PHONY: kafka-consume-assets kafka-consume-events kafka-consume-snapshots kafka-consume-alerts phase6-demo
.PHONY: spark-setup spark-test spark-stream spark-integration-test
.PHONY: lake-setup lake-env lake-up lake-down lake-init lake-stream lake-inspect lake-integration-test
.PHONY: search-setup search-up search-down search-init search-stream search-inspect search-query search-reindex search-integration-test
.PHONY: api-setup api-run api-test api-integration-test
SPARK_ARGS ?=
PHASE6_ARGS ?=

setup:
	mkdir -p .cache/tmp .tools
	test -x "$(UV)" || ($(PYTHON) -m venv .tools/uv && .tools/uv/bin/python -m pip install --no-cache-dir 'uv==0.12.21')
	"$(UV)" sync --locked --extra kafka

api-setup: setup
	"$(UV)" sync --locked --extra kafka --extra spark --extra lake --extra search --extra api

api-run:
	.venv/bin/python -m api.main

api-test:
	.venv/bin/python -m pytest tests/api

api-integration-test:
	.venv/bin/python -m pytest -m api_integration tests/api_integration

test:
	.venv/bin/python -m pytest tests/unit

spark-setup: setup
	"$(UV)" sync --locked --extra kafka --extra spark

lake-setup: setup
	"$(UV)" sync --locked --extra kafka --extra spark --extra lake

lake-env:
	.venv/bin/python -m scripts.lake_dev_credentials

lake-up:
	.venv/bin/python -m lake.main --check-config
	$(DOCKER) compose --profile lake up -d --build --wait --wait-timeout 180

lake-down:
	$(DOCKER) compose --profile lake down

lake-init:
	.venv/bin/python -m lake.main

lake-stream:
	CONFIGSTREAM_LAKE_ENABLED=true .venv/bin/python -m spark.streaming.main $(SPARK_ARGS)

lake-inspect:
	.venv/bin/python -m lake.inspect $(LAKE_ARGS)

lake-integration-test:
	.venv/bin/python -m pytest -m lake_integration tests/lake_integration

search-setup: setup
	"$(UV)" sync --locked --extra kafka --extra spark --extra lake --extra search

search-up:
	.venv/bin/python -m lake.main --check-config
	$(DOCKER) compose --profile lake --profile search up -d --build --wait --wait-timeout 240

search-down:
	$(DOCKER) compose --profile lake --profile search down

search-init:
	.venv/bin/python -m search.main init

search-stream:
	CONFIGSTREAM_LAKE_ENABLED=true CONFIGSTREAM_SEARCH_ENABLED=true .venv/bin/python -m spark.streaming.main $(SPARK_ARGS)

search-inspect:
	.venv/bin/python -m search.main inspect

search-query:
	.venv/bin/python -m search.main query $(SEARCH_ARGS)

search-reindex:
	.venv/bin/python -m search.reindex $(SEARCH_ARGS)

search-integration-test:
	.venv/bin/python -m pytest -m search_integration tests/search_integration

spark-test:
	.venv/bin/python -m pytest tests/spark

spark-stream:
	.venv/bin/python -m spark.streaming.main $(SPARK_ARGS)

spark-integration-test:
	.venv/bin/python -m pytest -m spark_integration tests/spark_integration

lint:
	.venv/bin/ruff check .
	.venv/bin/ruff format --check .

format:
	.venv/bin/ruff format .
	.venv/bin/ruff check --fix .

check: lint test

generator:
	@.venv/bin/python -m generator.main --assets 5 --events 10 --seed 42 --no-sleep

processing-demo:
	@.venv/bin/python -m processing.demo

phase6-demo:
	.venv/bin/python -m scripts.kafka_stateful_demo $(PHASE6_ARGS)

kafka-up:
	$(DOCKER) compose up -d --wait --wait-timeout 180 kafka

kafka-down:
	$(DOCKER) compose down

kafka-topics:
	.venv/bin/python -m messaging.topics

kafka-status:
	$(DOCKER) compose ps

kafka-demo:
	.venv/bin/python -m generator.main --assets 3 --events 8 --rate 100 --seed 42 --no-sleep --sink kafka

integration-test:
	.venv/bin/python -m pytest -m integration tests/integration

kafka-consume-assets kafka-consume-events kafka-consume-snapshots kafka-consume-alerts:
	$(DOCKER) compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server kafka:29092 --topic config.$(patsubst kafka-consume-%,%,$@) \
		--from-beginning --timeout-ms 10000 --max-messages $(KAFKA_CONSUME_MAX_MESSAGES) \
		--property print.key=true

clean:
	rm -rf .pytest_cache .ruff_cache build dist htmlcov .coverage
