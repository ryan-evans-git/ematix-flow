"""OtelSink — OpenTelemetry metrics export.

Uses `opentelemetry-sdk` + the OTLP exporter (optional deps).
The OTel collector pattern means one sink → many downstreams
(Prometheus, Cortex, Datadog, etc.) without re-instrumenting flow.

Tests inject an in-memory reader via `OtelSink._with_in_memory_reader()`
so they don't need a running collector.
"""

from __future__ import annotations


class OtelSink:
    def __init__(self, *, endpoint: str = "localhost:4317", _reader=None):
        try:
            from opentelemetry.sdk.metrics import MeterProvider
            from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        except ImportError as e:
            raise ImportError(
                "OtelSink requires opentelemetry-sdk. Install with "
                "`pip install opentelemetry-sdk opentelemetry-exporter-otlp`."
            ) from e

        if _reader is None:
            try:
                from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
                    OTLPMetricExporter,
                )
            except ImportError as e:
                raise ImportError(
                    "OtelSink with OTLP/gRPC requires "
                    "opentelemetry-exporter-otlp-proto-grpc. Install with "
                    "`pip install opentelemetry-exporter-otlp`."
                ) from e
            exporter = OTLPMetricExporter(endpoint=endpoint, insecure=True)
            _reader = PeriodicExportingMetricReader(exporter, export_interval_millis=5000)

        self._reader = _reader
        self._provider = MeterProvider(metric_readers=[_reader])
        self._meter = self._provider.get_meter("ematix_flow")

        self._runs_counter = self._meter.create_counter(
            "pipeline_runs_total",
            description="Total pipeline invocations by name + outcome",
        )
        self._duration_histogram = self._meter.create_histogram(
            "pipeline_duration_seconds",
            description="Pipeline body execution time in seconds",
            unit="s",
        )
        # OTel doesn't have a native synchronous gauge until 1.20+.
        # ObservableGauge with a callback is the portable form; we
        # keep a side-table of latest values for the callback to read.
        self._attempt_state: dict[str, int] = {}
        try:
            self._meter.create_observable_gauge(
                "pipeline_retry_attempt",
                callbacks=[self._observe_attempts],
                description="Current retry attempt count per pipeline (0 when idle)",
            )
        except AttributeError:
            # Very old OTel SDK without create_observable_gauge — skip
            # the gauge silently; counters + histograms still work.
            pass

    def _observe_attempts(self, _options):
        from opentelemetry.metrics import Observation

        for name, count in self._attempt_state.items():
            yield Observation(count, {"pipeline": name})

    @classmethod
    def _with_in_memory_reader(cls):
        """Construct a sink backed by an in-memory reader. Used by
        tests so they can introspect metrics without an OTLP
        collector."""
        from opentelemetry.sdk.metrics.export import InMemoryMetricReader

        reader = InMemoryMetricReader()
        sink = cls.__new__(cls)
        OtelSink.__init__(sink, _reader=reader)
        return sink

    def inc_runs(self, name: str, outcome: str) -> None:
        self._runs_counter.add(1, {"pipeline": name, "outcome": outcome})

    def observe_duration(self, name: str, secs: float) -> None:
        self._duration_histogram.record(secs, {"pipeline": name})

    def set_attempt(self, name: str, count: int) -> None:
        self._attempt_state[name] = count

    def close(self) -> None:
        try:
            self._provider.shutdown()
        except Exception:
            pass
