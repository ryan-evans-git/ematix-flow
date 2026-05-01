"""Type stubs for the Rust extension module `ematix_flow._core`."""

from typing import Any, Iterable, TypedDict

import pyarrow as pa

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

class KafkaBackend:
    """Streaming Kafka backend with Arrow IO + manual offset commits.

    Constructed via :py:meth:`open` (kwarg-style — no chained
    builder calls). Methods are blocking; the Rust side runs the
    work on its tokio runtime and releases the GIL.
    """

    @staticmethod
    def open(
        bootstrap_servers: str,
        *,
        group_id: str | None = None,
        payload_format: str | None = None,
        schema_registry_url: str | None = None,
        delivery_semantics: str | None = None,
        transactional_id: str | None = None,
        sasl_plain_username: str | None = None,
        sasl_plain_password: str | None = None,
        sasl_scram_username: str | None = None,
        sasl_scram_password: str | None = None,
        sasl_scram_mechanism: str | None = None,
        tls_cert_path: str | None = None,
        tls_key_path: str | None = None,
        tls_ca_path: str | None = None,
        msk_iam_region: str | None = None,
        batch_size: int | None = None,
        batch_bytes: int | None = None,
        batch_window_ms: int | None = None,
        idle_timeout_ms: int | None = None,
    ) -> "KafkaBackend":
        """Build a KafkaBackend.

        ``payload_format``: ``"json"`` (default), ``"raw_bytes"``,
        ``"avro"``, or ``"protobuf"``. Avro/Protobuf require
        ``schema_registry_url``.

        ``delivery_semantics``: ``"at_least_once"`` (default) or
        ``"exactly_once"`` (requires ``transactional_id``).

        Auth providers — pass at most one set:
          - ``sasl_plain_username`` + ``sasl_plain_password``
          - ``sasl_scram_username`` + ``sasl_scram_password`` +
            ``sasl_scram_mechanism`` (``"sha-256"`` or ``"sha-512"``)
          - ``tls_cert_path`` + ``tls_key_path`` + ``tls_ca_path``
          - ``msk_iam_region`` (AWS MSK IAM auth)

        ``batch_size`` / ``batch_bytes`` / ``batch_window_ms`` /
        ``idle_timeout_ms`` tune the consumer drain limits — first
        trigger flushes.
        """
        ...

    def ping(self) -> None:
        """Validate connectivity. Raises ``ValueError`` on failure."""
        ...

    def read_arrow_stream(self, query: str) -> list[pa.RecordBatch]:
        """Drain ``query`` (the topic) into a list of PyArrow RecordBatches.

        Returns the accumulated batches when the source goes idle or
        the size/byte limits in the batch config fire. Auto-commit
        is off; call :py:meth:`commit_offsets` after a durable
        downstream write for at-least-once semantics.
        """
        ...

    def write_arrow_stream(
        self, topic: str, batches: Iterable[pa.RecordBatch]
    ) -> int:
        """Produce each row of every PyArrow batch to ``topic``.

        Returns the total row count produced.
        """
        ...

    def commit_offsets(self) -> None:
        """Commit the consumer's pending offsets. No-op for producer-only."""
        ...
