SHELL := /bin/sh
PYTHON ?= python3
UV := .tools/uv/bin/uv
DOCKER ?= docker
KAFKA_CONSUME_MAX_MESSAGES ?= 1
export UV_CACHE_DIR := $(CURDIR)/.cache/uv
export UV_PYTHON_INSTALL_DIR := $(CURDIR)/.tools/python
export TMPDIR := $(CURDIR)/.cache/tmp

.PHONY: setup test lint format check generator processing-demo clean
.PHONY: feasibility-test feasibility-report
.PHONY: phase16-test phase16-coordinate-proof
.PHONY: rpki-oracle rpki-local rpki-publish rpki-stream rpki-reconstruct phase17-integration-test

phase16-test:
	.venv/bin/python -m pytest tests/unit/test_temporal_contracts.py

phase16-coordinate-proof:
	.venv/bin/python -m scripts.phase16_coordinate_probe --parser-dir .cache/phase16/parser --output .cache/phase16/coordinate-proof.json

rpki-oracle rpki-local rpki-publish rpki-stream rpki-reconstruct:
	.venv/bin/python -m workloads.rpki_main $(patsubst rpki-%,%,$@) $(RPKI_ARGS)

phase17-integration-test:
	.venv/bin/python -m pytest tests/phase17_integration

feasibility-test:
	.venv/bin/python -m pytest tests/unit/test_feasibility*.py

feasibility-report:
	.venv/bin/python -m workloads.feasibility_report
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
	"$(UV)" sync --locked --extra kafka --extra ris --extra dns

.PHONY: dataset-fetch dataset-inspect workload-generate
.PHONY: observed-fetch observed-inspect observed-replay empirical-report calibration-generate phase11-integration-test
observed-fetch:
	.venv/bin/python -m workloads.observed_main fetch $(OBSERVED_ARGS)

.PHONY: terrads-fetch terrads-verify terrads-inspect terrads-sample terrads-history terrads-replay terrads-report terrads-audit terrads-validate terrads-smoke phase12-integration-test
terrads-fetch terrads-verify terrads-inspect terrads-sample terrads-history terrads-replay terrads-report terrads-audit terrads-validate terrads-smoke:
	.venv/bin/python -m workloads.terrads_main $(patsubst terrads-%,%,$@) $(TERRADS_ARGS)

phase12-integration-test:
	.venv/bin/python -m pytest -m phase12_integration tests/phase12_integration

observed-inspect:
	.venv/bin/python -m workloads.observed_main inspect $(OBSERVED_ARGS)

observed-replay:
	.venv/bin/python -m workloads.observed_main replay $(OBSERVED_ARGS)

empirical-report:
	.venv/bin/python -m workloads.observed_main report $(OBSERVED_ARGS)

calibration-generate:
	.venv/bin/python -m workloads.observed_main generate --profile observed-calibrated $(OBSERVED_ARGS)

phase11-integration-test:
	.venv/bin/python -m pytest -m phase11_integration tests/phase11_integration

dataset-fetch:
	.venv/bin/python -m workloads.main fetch $(DATASET_ARGS)

dataset-inspect:
	.venv/bin/python -m workloads.main inspect $(DATASET_ARGS)

workload-generate:
	.venv/bin/python -m workloads.main generate $(WORKLOAD_ARGS)

.PHONY: analytics-run analytics-inspect analytics-test phase10-integration-test
analytics-run:
	CONFIGSTREAM_LAKE_ENABLED=true .venv/bin/python -m analytics.main $(ANALYTICS_ARGS)

analytics-inspect:
	CONFIGSTREAM_LAKE_ENABLED=true .venv/bin/python -m analytics.main --inspect $(ANALYTICS_ARGS)

analytics-test:
	.venv/bin/python -m pytest tests/spark/test_analytics.py tests/unit/test_analytics_config.py

phase10-integration-test:
	.venv/bin/python -m pytest -m phase10_integration tests/phase10_integration

.PHONY: benchmark-smoke benchmark-main benchmark-stress benchmark-report
benchmark-smoke:
	.venv/bin/python -m benchmarks.main --benchmark-id smoke --assets 500 --versions-per-asset 20 $(BENCHMARK_ARGS)

benchmark-main:
	.venv/bin/python -m benchmarks.main --benchmark-id main --assets 10000 --versions-per-asset 100 --rate 1000 --driver-memory 2g --max-offsets 50000 $(BENCHMARK_ARGS)

benchmark-stress:
	.venv/bin/python -m benchmarks.main --benchmark-id stress --assets 50000 --versions-per-asset 100 --rate 1000 --driver-memory 2g --max-offsets 50000 --max-seconds 7200 $(BENCHMARK_ARGS)

benchmark-report:
	.venv/bin/python -m benchmarks.report $(BENCHMARK_REPORT_ARGS)

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
	"$(UV)" sync --locked --extra kafka --extra ris --extra spark

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
