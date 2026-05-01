"""Type stubs for the Rust extension module `ematix_flow._core`."""

from typing import Any, TypedDict

def core_version() -> str: ...

class PipelineMetrics(TypedDict):
    """Result dict returned by `run_pipeline_from_*` on clean exit."""

    total_rows: int
    iterations: int
    shutdown_triggered: bool

def run_pipeline_from_toml_str(
    toml_str: str, metrics_port: int | None = None
) -> PipelineMetrics:
    """Run a streaming pipeline from a TOML config string.

    Blocks the calling Python thread until SIGTERM / SIGINT is
    received or the pipeline exits cleanly. Errors raise
    ``ValueError``. When ``metrics_port`` is set, the Prometheus
    ``/metrics`` HTTP endpoint runs on ``127.0.0.1:<port>`` for
    the pipeline's lifetime.
    """
    ...

def run_pipeline_from_path(
    path: str, metrics_port: int | None = None
) -> PipelineMetrics:
    """Run a streaming pipeline from a TOML config file path.

    Same return shape and error semantics as
    ``run_pipeline_from_toml_str``.
    """
    ...
