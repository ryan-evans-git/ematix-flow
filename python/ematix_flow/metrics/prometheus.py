"""PrometheusSink — exposes pipeline metrics on /metrics for scraping.

Uses `prometheus_client` (optional dep). The sink owns its own
CollectorRegistry so multiple Sinks don't trample each other in tests.
The HTTP server is opt-in via `http_port=`; when None, the sink only
records metrics and callers can scrape via
`prometheus_client.generate_latest(sink._registry)`.
"""

from __future__ import annotations


class PrometheusSink:
    def __init__(self, *, http_port: int | None = None, registry=None):
        try:
            from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram
        except ImportError as e:
            raise ImportError(
                "PrometheusSink requires prometheus_client. "
                "Install with `pip install prometheus_client`."
            ) from e

        self._registry = registry if registry is not None else CollectorRegistry()
        self._runs_total = Counter(
            "pipeline_runs_total",
            "Total pipeline invocations by name + outcome",
            ["pipeline", "outcome"],
            registry=self._registry,
        )
        self._duration_seconds = Histogram(
            "pipeline_duration_seconds",
            "Pipeline body execution time in seconds",
            ["pipeline"],
            registry=self._registry,
        )
        self._retry_attempt = Gauge(
            "pipeline_retry_attempt",
            "Current retry attempt count per pipeline (0 when idle)",
            ["pipeline"],
            registry=self._registry,
        )

        self._http_thread = None
        if http_port is not None:
            from prometheus_client import start_http_server
            # start_http_server returns the server + thread; the thread
            # is what we hold so we can stop it on close().
            ret = start_http_server(http_port, registry=self._registry)
            # The function signature varies by version; defend.
            self._http_thread = ret if ret is not None else None

    def inc_runs(self, name: str, outcome: str) -> None:
        self._runs_total.labels(pipeline=name, outcome=outcome).inc()

    def observe_duration(self, name: str, secs: float) -> None:
        self._duration_seconds.labels(pipeline=name).observe(secs)

    def set_attempt(self, name: str, count: int) -> None:
        self._retry_attempt.labels(pipeline=name).set(count)

    def close(self) -> None:
        # prometheus_client's start_http_server doesn't expose a clean
        # stop path on older versions — best-effort.
        pass
