# ematix-flow — developer Makefile
#
# Wraps the commands you'd otherwise type by hand so the
# testcontainers cleanup, ruff, bandit, and rust-test gates all run
# in the right order with the right post-conditions.
#
# Targets are designed to be safe to run repeatedly; tests are
# idempotent and the Docker prune is targeted by label so it can't
# touch unrelated containers on your machine.

.PHONY: help test test-python test-rust test-integration test-e2e \
        clean-testcontainers fmt lint security \
        up down logs demo-deps \
        demo-streaming-init demo-streaming-producer demo-streaming-pipeline \
        demo-workflow-scheduler demo-workflow-status \
        demo-s3-init demo-s3-seed demo-s3-pipeline

help:  ## Show this help.
	@awk 'BEGIN {FS=":.*##"; printf "Targets:\n"} \
	     /^[a-zA-Z0-9_-]+:.*##/ {printf "  %-25s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Use the venv's interpreter when one exists; otherwise fall back to
# system python3. Override with `make PYTHON=/path/to/python <target>`.
PYTHON ?= $(shell test -x .venv/bin/python && echo .venv/bin/python || echo python3)
FLOW   ?= $(shell test -x .venv/bin/flow && echo .venv/bin/flow || echo flow)

# ---- fast test lanes (no Docker) ---------------------------------

test: test-python test-rust  ## Run both fast suites (no Docker).

test-python:  ## Python test suite (default markers, no integration).
	pytest -q

test-rust:  ## Rust workspace lib tests (no testcontainers).
	cargo test --workspace --lib

# ---- integration lane (Docker-gated, auto-cleanup) --------------

test-e2e: ## E2E demo suite (needs `make up` for postgres+kafka+minio).
	$(PYTHON) -m pytest tests/e2e/ --e2e -v

test-integration: ## Full integration suite incl. testcontainers; always cleans up after.
	@echo "==> Running integration tests (testcontainers will spin up postgres/redis/minio/kafka/etc)"
	cargo test --workspace -- --ignored ; STATUS=$$? ; \
	$(MAKE) clean-testcontainers ; \
	exit $$STATUS

clean-testcontainers: ## Remove leaked testcontainers containers + dangling volumes (safe; label-scoped).
	@echo "==> Sweeping testcontainers-labeled containers"
	@COUNT=$$(docker ps -aq -f label=org.testcontainers.managed-by=testcontainers | wc -l | tr -d ' ') ; \
	if [ "$$COUNT" -gt 0 ]; then \
	  docker rm -f $$(docker ps -aq -f label=org.testcontainers.managed-by=testcontainers) >/dev/null ; \
	  echo "   removed $$COUNT containers" ; \
	else \
	  echo "   nothing to remove" ; \
	fi
	@echo "==> Pruning dangling volumes"
	@docker volume prune -f 2>&1 | tail -1

# ---- code-quality gates ------------------------------------------

fmt:  ## cargo fmt + check.
	cargo fmt --all
	cargo fmt --all -- --check

lint:  ## ruff (Python) + clippy (Rust, the strict CI gate).
	ruff check python tests
	cargo clippy --workspace --all-targets -- -D warnings

security:  ## bandit (Python) + cargo-audit (Rust).
	bandit -r python -ll -c pyproject.toml
	cargo audit

# ---- demo-stack lifecycle ----------------------------------------

demo-deps:  ## Install Python deps the demos need (confluent-kafka, boto3, pyarrow).
	$(PYTHON) -m pip install 'confluent-kafka>=2.0' 'boto3>=1.30' 'pyarrow>=14'

up:  ## Bring up the demo docker stack (postgres + kafka + minio).
	docker compose -f examples/docker-compose.yml up -d
	@echo "==> waiting for services to be healthy"
	@docker compose -f examples/docker-compose.yml ps

down:  ## Tear down the demo docker stack (preserves volumes).
	docker compose -f examples/docker-compose.yml down

logs:  ## Tail logs from the demo docker stack.
	docker compose -f examples/docker-compose.yml logs -f

# ---- demo 09: streaming clickstream (Kafka → Postgres) ----------

PG_EXEC = docker exec -i ematix-flow-pg psql -U postgres

demo-streaming-init:  ## Demo 09: create analytics.clicks table.
	$(PG_EXEC) -f - < examples/09_streaming_clickstream/init.sql

demo-streaming-producer:  ## Demo 09: run the synthetic producer (Ctrl+C to stop).
	$(PYTHON) examples/09_streaming_clickstream/producer.py

demo-streaming-pipeline:  ## Demo 09: run the streaming pipeline (Ctrl+C to stop).
	cd examples/09_streaming_clickstream && PYTHONPATH=. \
		$(abspath $(FLOW)) consume --module pipeline clicks-to-pg

# ---- demo 10: workflow DAG + central scheduler ------------------

DEMO10_MOD := examples.10_workflow_dag.pipelines
DEMO10_RUNS := sqlite:///tmp/ematix-demo-10-runs.db

demo-workflow-scheduler:  ## Demo 10: run flow scheduler against the DAG (Ctrl+C to stop).
	cd examples/10_workflow_dag && PYTHONPATH=. $(abspath $(FLOW)) scheduler \
		--module pipelines \
		--executor "subprocess+python://" \
		--run-log-url "$(DEMO10_RUNS)" \
		--poll-interval 5 \
		--interval 60

demo-workflow-status:  ## Demo 10: per-pipeline status snapshot.
	cd examples/10_workflow_dag && PYTHONPATH=. $(abspath $(FLOW)) status \
		--module pipelines \
		--run-log-url "$(DEMO10_RUNS)"

# ---- demo 11: S3 (MinIO) parquet → Postgres ---------------------

demo-s3-init:  ## Demo 11: create analytics.events table.
	$(PG_EXEC) -f - < examples/11_s3_parquet_to_postgres/init.sql

demo-s3-seed:  ## Demo 11: upload 3 parquet files to MinIO bucket.
	$(PYTHON) examples/11_s3_parquet_to_postgres/seed.py

demo-s3-pipeline:  ## Demo 11: run the S3 → Postgres pipeline (Ctrl+C to stop).
	$(PYTHON) examples/11_s3_parquet_to_postgres/pipeline.py
