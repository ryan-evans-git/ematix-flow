"""Phase Ω.M1 — Metrics export (Prometheus + OpenTelemetry).

Three orchestrator-level metrics every operator wants:
  - pipeline_runs_total{name, outcome}    (counter)
  - pipeline_duration_seconds{name}       (histogram)
  - pipeline_retry_attempt{name}          (gauge)

Sinks satisfy a narrow Protocol — anything with the four methods
(inc_runs, observe_duration, set_attempt, close) plugs in. Concretes:
  - NullSink        no-op default
  - StdoutSink      debugging, prints to stderr
  - InMemorySink    tests
  - PrometheusSink  optional dep `prometheus_client`
  - OtelSink        optional dep `opentelemetry-sdk`
"""

from __future__ import annotations

import datetime as _dt
import pytest

from ematix_flow import pipeline as p
from ematix_flow.metrics import (
    InMemorySink,
    MetricsSink,
    NullSink,
    StdoutSink,
    from_url as metrics_from_url,
)


_SIDE_TABLES = (
    "_REGISTRY", "_DEPENDS_ON", "_UPSTREAM_FRESHNESS",
    "_LAST_RUN", "_RETRY_POLICY", "_ATTEMPT_STATE",
)


@pytest.fixture(autouse=True)
def _clean_registry():
    for tbl in _SIDE_TABLES:
        d = getattr(p, tbl, None)
        if d is not None:
            d.clear()
    yield
    for tbl in _SIDE_TABLES:
        d = getattr(p, tbl, None)
        if d is not None:
            d.clear()


# ---- Protocol ------------------------------------------------------


def test_null_sink_satisfies_protocol():
    assert isinstance(NullSink(), MetricsSink)


def test_stdout_sink_satisfies_protocol():
    assert isinstance(StdoutSink(), MetricsSink)


def test_in_memory_sink_satisfies_protocol():
    assert isinstance(InMemorySink(), MetricsSink)


# ---- InMemorySink ---------------------------------------------------


def test_in_memory_sink_records_counters():
    sink = InMemorySink()
    sink.inc_runs("alpha", "success")
    sink.inc_runs("alpha", "success")
    sink.inc_runs("alpha", "failure")
    assert sink.counters[("alpha", "success")] == 2
    assert sink.counters[("alpha", "failure")] == 1


def test_in_memory_sink_records_durations():
    sink = InMemorySink()
    sink.observe_duration("alpha", 0.5)
    sink.observe_duration("alpha", 1.2)
    sink.observe_duration("beta", 0.1)
    assert sink.durations["alpha"] == [0.5, 1.2]
    assert sink.durations["beta"] == [0.1]


def test_in_memory_sink_records_attempt_gauge():
    sink = InMemorySink()
    sink.set_attempt("flaky", 1)
    sink.set_attempt("flaky", 2)  # latest wins
    sink.set_attempt("ok", 0)
    assert sink.attempts["flaky"] == 2
    assert sink.attempts["ok"] == 0


# ---- run_due_with_dag_detailed integration -------------------------


def test_run_due_records_success_counter():
    @p.register(name="ok", schedule="@hourly")
    def _ok():
        return {}

    sink = InMemorySink()
    p.run_due_with_dag_detailed(["ok"], metrics=sink)
    assert sink.counters[("ok", "success")] == 1


def test_run_due_records_failure_counter():
    @p.register(name="boom", schedule="@hourly")
    def _boom():
        raise RuntimeError("nope")

    sink = InMemorySink()
    p.run_due_with_dag_detailed(["boom"], metrics=sink)
    assert sink.counters[("boom", "failure")] == 1


def test_run_due_records_skipped_counter():
    @p.register(name="root", schedule="@hourly")
    def _root():
        raise RuntimeError("upstream broken")

    @p.register(name="leaf", schedule="@hourly", depends_on=["root"])
    def _leaf():
        return {}

    sink = InMemorySink()
    p.run_due_with_dag_detailed(["root", "leaf"], metrics=sink)
    assert sink.counters[("leaf", "skipped")] == 1


def test_run_due_records_duration():
    """A pipeline's duration is observed for histogram bucketing."""
    @p.register(name="quick", schedule="@hourly")
    def _quick():
        return {}

    sink = InMemorySink()
    p.run_due_with_dag_detailed(["quick"], metrics=sink)
    assert len(sink.durations["quick"]) == 1
    assert sink.durations["quick"][0] >= 0


def test_run_due_updates_retry_attempt_gauge():
    @p.register(
        name="flaky",
        schedule="@hourly",
        retry={"max_attempts": 5, "backoff": "fixed", "base_secs": 0},
    )
    def _flaky():
        raise RuntimeError("boom")

    sink = InMemorySink()
    t = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.timezone.utc)
    p.run_due_with_dag_detailed(["flaky"], now=t, metrics=sink)
    assert sink.attempts["flaky"] == 1
    p.run_due_with_dag_detailed(["flaky"], now=t + _dt.timedelta(seconds=1), metrics=sink)
    assert sink.attempts["flaky"] == 2


def test_run_due_clears_attempt_gauge_on_recovery():
    calls = []

    @p.register(
        name="flaky",
        schedule="@hourly",
        retry={"max_attempts": 5, "backoff": "fixed", "base_secs": 0},
    )
    def _flaky():
        calls.append("attempt")
        if len(calls) < 2:
            raise RuntimeError("not yet")
        return {}

    sink = InMemorySink()
    t = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.timezone.utc)
    p.run_due_with_dag_detailed(["flaky"], now=t, metrics=sink)
    assert sink.attempts["flaky"] == 1
    p.run_due_with_dag_detailed(["flaky"], now=t + _dt.timedelta(seconds=1), metrics=sink)
    # Recovered → gauge reset to 0.
    assert sink.attempts["flaky"] == 0


def test_broken_sink_does_not_crash_run_due():
    """A buggy sink must not poison the orchestrator loop."""
    @p.register(name="ok", schedule="@hourly")
    def _ok():
        return {}

    class _Broken:
        def inc_runs(self, *a, **k): raise RuntimeError("broken")
        def observe_duration(self, *a, **k): raise RuntimeError("broken")
        def set_attempt(self, *a, **k): raise RuntimeError("broken")
        def close(self): pass

    # Must not raise.
    result = p.run_due_with_dag_detailed(["ok"], metrics=_Broken())
    assert [e.name for e in result.fired] == ["ok"]


# ---- URL factory ---------------------------------------------------


def test_from_url_null():
    sink = metrics_from_url("null://")
    assert isinstance(sink, NullSink)


def test_from_url_stdout():
    sink = metrics_from_url("stdout://")
    assert isinstance(sink, StdoutSink)


def test_from_url_memory():
    sink = metrics_from_url("memory://")
    assert isinstance(sink, InMemorySink)


def test_from_url_unknown_scheme_errors():
    with pytest.raises(ValueError):
        metrics_from_url("graphite://localhost:2003")


# ---- Prometheus (gated on optional dep) ----------------------------


def test_prometheus_sink_optional():
    """If `prometheus_client` is installed, PrometheusSink works.
    If not, the import path should fail with a helpful message."""
    try:
        import prometheus_client  # noqa: F401
    except ImportError:
        pytest.skip("prometheus_client not installed")

    from ematix_flow.metrics import PrometheusSink

    sink = PrometheusSink()  # uses the default registry
    sink.inc_runs("alpha", "success")
    sink.observe_duration("alpha", 0.123)
    sink.set_attempt("flaky", 2)
    # Scrape the registry; check our metric names appear.
    output = prometheus_client.generate_latest(sink._registry).decode("utf-8")
    assert "pipeline_runs_total" in output
    assert "pipeline_duration_seconds" in output
    assert "pipeline_retry_attempt" in output
    assert 'pipeline="alpha"' in output
    assert 'outcome="success"' in output
    sink.close()


# ---- OpenTelemetry (gated on optional dep) -------------------------


def test_otel_sink_optional():
    try:
        from opentelemetry.sdk.metrics import MeterProvider  # noqa: F401
    except ImportError:
        pytest.skip("opentelemetry-sdk not installed")

    from ematix_flow.metrics import OtelSink

    # Construct with an in-memory reader so we don't need an OTLP
    # collector running in tests.
    sink = OtelSink._with_in_memory_reader()
    sink.inc_runs("alpha", "success")
    sink.observe_duration("alpha", 0.5)
    sink.set_attempt("flaky", 3)
    metrics = sink._reader.get_metrics_data()
    # OTel SDK gives us ResourceMetrics → ScopeMetrics → MetricData.
    # Walk the tree and assert we see all three of our metric names.
    names = set()
    for rm in metrics.resource_metrics:
        for sm in rm.scope_metrics:
            for m in sm.metrics:
                names.add(m.name)
    assert "pipeline_runs_total" in names
    assert "pipeline_duration_seconds" in names
    assert "pipeline_retry_attempt" in names
    sink.close()
