"""Streaming pipeline runner — Python facade over the Rust ``flow consume`` runtime.

This module gives Python callers the same ``flow consume <toml>``
behavior as the CLI binary, but invokable in-process without
shelling out. Useful for orchestrators (Airflow, Dagster, Argo)
that run a Python wrapper task and want to call the runner
directly.

The pipeline runs on the Rust side (multi-thread tokio) — no
records cross the Python FFI boundary. The function blocks the
calling Python thread until the pipeline exits (clean or
SIGTERM/SIGINT). Errors raise ``ValueError``.

Example::

    from ematix_flow import streaming

    metrics = streaming.run_pipeline(
        config=\"\"\"
            pipeline_name = "events-to-pg"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"
            group_id = "ematix-flow"

            [target]
            kind = "postgres"
            url = "postgres://localhost/mydb"

            [target.table]
            schema = "public"
            name = "events"
        \"\"\",
        metrics_port=9100,
    )
    print(metrics["total_rows"], metrics["shutdown_triggered"])
"""

from __future__ import annotations

import os
from typing import Any, Mapping

from ematix_flow._core import (
    PipelineMetrics,
    run_pipeline_from_path,
    run_pipeline_from_toml_str,
)

__all__ = ["run_pipeline", "PipelineMetrics"]


def run_pipeline(
    *,
    config: str | os.PathLike[str] | None = None,
    config_str: str | None = None,
    metrics_port: int | None = None,
) -> PipelineMetrics:
    """Run a streaming pipeline.

    Exactly one of ``config`` (path) or ``config_str`` (TOML
    contents) must be provided. Returns a dict with
    ``total_rows``, ``iterations``, and ``shutdown_triggered``.
    Blocks until SIGTERM / SIGINT or clean exit.

    ``metrics_port``: optional Prometheus ``/metrics`` endpoint
    port. When set, the server binds ``127.0.0.1:<port>`` for
    the pipeline's lifetime; otherwise no metrics server runs.
    """
    if config is None and config_str is None:
        raise ValueError("run_pipeline: either `config` (path) or `config_str` is required")
    if config is not None and config_str is not None:
        raise ValueError("run_pipeline: pass `config` OR `config_str`, not both")
    if config_str is not None:
        return run_pipeline_from_toml_str(config_str, metrics_port)
    assert config is not None
    return run_pipeline_from_path(os.fspath(config), metrics_port)
