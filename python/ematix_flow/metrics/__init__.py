"""Pipeline metrics — Prometheus + OpenTelemetry export.

Three metrics every operator wants from a pipeline orchestrator:
  - pipeline_runs_total{name, outcome}    counter
                                          outcome ∈ {success, failure, skipped}
  - pipeline_duration_seconds{name}       histogram
  - pipeline_retry_attempt{name}          gauge (current attempt; 0 when idle)

Sinks satisfy a narrow Protocol:

    class MetricsSink(Protocol):
        def inc_runs(self, name: str, outcome: str) -> None: ...
        def observe_duration(self, name: str, secs: float) -> None: ...
        def set_attempt(self, name: str, count: int) -> None: ...
        def close(self) -> None: ...

Concretes:
  - NullSink         no-op default
  - StdoutSink       prints to stderr (debugging)
  - InMemorySink     test double; counts everything in dicts
  - PrometheusSink   optional dep `prometheus_client`
  - OtelSink         optional dep `opentelemetry-sdk` + exporter

`run_due_with_dag_detailed(..., metrics=sink)` updates the sink as
pipelines fire / fail / skip. A buggy sink can't poison the loop —
exceptions are swallowed-and-logged like the alerter path.

URL factory schemes:
  null://                            NullSink
  stdout://                          StdoutSink
  memory://                          InMemorySink
  prometheus://[:port]               PrometheusSink (HTTP /metrics)
  otlp://collector.example:4317      OtelSink (OTLP gRPC)
  otlp+http://collector:4318         OtelSink (OTLP HTTP)
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class MetricsSink(Protocol):
    def inc_runs(self, name: str, outcome: str) -> None: ...
    def observe_duration(self, name: str, secs: float) -> None: ...
    def set_attempt(self, name: str, count: int) -> None: ...
    def close(self) -> None: ...


class NullSink:
    """Default no-op sink. Cheap to construct, ignores everything."""

    def inc_runs(self, name: str, outcome: str) -> None:
        pass

    def observe_duration(self, name: str, secs: float) -> None:
        pass

    def set_attempt(self, name: str, count: int) -> None:
        pass

    def close(self) -> None:
        pass


class StdoutSink:
    """Prints one line per metric event to stderr. Useful for local
    debugging — confirms metric calls are happening without spinning
    up a Prometheus or OTel collector."""

    def __init__(self, stream=None):
        import sys
        self._stream = stream if stream is not None else sys.stderr

    def inc_runs(self, name: str, outcome: str) -> None:
        print(f"[METRIC] pipeline_runs_total{{name={name},outcome={outcome}}} +1",
              file=self._stream, flush=True)

    def observe_duration(self, name: str, secs: float) -> None:
        print(f"[METRIC] pipeline_duration_seconds{{name={name}}} {secs:.4f}",
              file=self._stream, flush=True)

    def set_attempt(self, name: str, count: int) -> None:
        print(f"[METRIC] pipeline_retry_attempt{{name={name}}} {count}",
              file=self._stream, flush=True)

    def close(self) -> None:
        pass


class InMemorySink:
    """Counts in plain dicts. The test fixture for everything that
    integrates with the metrics layer."""

    def __init__(self):
        # (name, outcome) -> count
        self.counters: dict[tuple[str, str], int] = {}
        # name -> [secs, ...]
        self.durations: dict[str, list[float]] = {}
        # name -> latest attempt count
        self.attempts: dict[str, int] = {}

    def inc_runs(self, name: str, outcome: str) -> None:
        key = (name, outcome)
        self.counters[key] = self.counters.get(key, 0) + 1

    def observe_duration(self, name: str, secs: float) -> None:
        self.durations.setdefault(name, []).append(secs)

    def set_attempt(self, name: str, count: int) -> None:
        self.attempts[name] = count

    def close(self) -> None:
        pass


# ---- URL factory --------------------------------------------------------


def from_url(url: str) -> MetricsSink:
    """Pick the right metrics sink for a URL. See module docstring."""
    from urllib.parse import urlparse

    if not url:
        raise ValueError("Metrics URL must not be empty")

    parsed = urlparse(url)
    scheme = parsed.scheme.lower()

    if scheme == "null":
        return NullSink()
    if scheme == "stdout":
        return StdoutSink()
    if scheme == "memory":
        return InMemorySink()

    if scheme == "prometheus":
        from .prometheus import PrometheusSink
        # prometheus://:9090 → port 9090; prometheus:// → no http server
        port = parsed.port
        return PrometheusSink(http_port=port)

    if scheme in ("otlp", "otlp+grpc", "otlp+http"):
        from .otel import OtelSink
        # Strip our custom prefix, hand the SDK what it expects.
        endpoint = url.replace("otlp+grpc://", "").replace(
            "otlp+http://", ""
        ).replace("otlp://", "")
        return OtelSink(endpoint=endpoint)

    raise ValueError(
        f"unknown metrics URL scheme {scheme!r} in {url!r}. "
        f"Supported: null, stdout, memory, prometheus, otlp, otlp+grpc, otlp+http."
    )


__all__ = [
    "InMemorySink",
    "MetricsSink",
    "NullSink",
    "OtelSink",
    "PrometheusSink",
    "StdoutSink",
    "from_url",
]


def __getattr__(name: str):
    if name == "PrometheusSink":
        from .prometheus import PrometheusSink
        return PrometheusSink
    if name == "OtelSink":
        from .otel import OtelSink
        return OtelSink
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
