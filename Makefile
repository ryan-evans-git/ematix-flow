# ematix-flow — developer Makefile
#
# Wraps the commands you'd otherwise type by hand so the
# testcontainers cleanup, ruff, bandit, and rust-test gates all run
# in the right order with the right post-conditions.
#
# Targets are designed to be safe to run repeatedly; tests are
# idempotent and the Docker prune is targeted by label so it can't
# touch unrelated containers on your machine.

.PHONY: help test test-python test-rust test-integration \
        clean-testcontainers fmt lint security

help:  ## Show this help.
	@awk 'BEGIN {FS=":.*##"; printf "Targets:\n"} \
	     /^[a-zA-Z_-]+:.*##/ {printf "  %-22s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# ---- fast test lanes (no Docker) ---------------------------------

test: test-python test-rust  ## Run both fast suites (no Docker).

test-python:  ## Python test suite (default markers, no integration).
	pytest -q

test-rust:  ## Rust workspace lib tests (no testcontainers).
	cargo test --workspace --lib

# ---- integration lane (Docker-gated, auto-cleanup) --------------

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
