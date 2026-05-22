"""OTEL tracing — pipeline_run_span wraps pipeline executions."""
from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

from ematix_flow import pipeline
from ematix_flow.tracing import (
    get_global_tracer,
    pipeline_run_span,
    set_global_tracer,
)


@pytest.fixture(autouse=True)
def _reset_tracer():
    saved = get_global_tracer()
    set_global_tracer(None)
    yield
    set_global_tracer(saved)


@pytest.fixture(autouse=True)
def _reset_registry():
    pipeline._REGISTRY.clear()
    pipeline._DEPENDS_ON.clear()
    pipeline._UPSTREAM_FRESHNESS.clear()
    pipeline._LAST_RUN.clear()
    pipeline._RETRY_POLICY.clear()
    pipeline._ATTEMPT_STATE.clear()
    yield
    pipeline._REGISTRY.clear()
    pipeline._DEPENDS_ON.clear()
    pipeline._UPSTREAM_FRESHNESS.clear()
    pipeline._LAST_RUN.clear()
    pipeline._RETRY_POLICY.clear()
    pipeline._ATTEMPT_STATE.clear()


class FakeTracer:
    """Duck-typed Tracer for tests. Records every span created."""

    def __init__(self):
        self.spans: list[dict] = []

    @contextmanager
    def start_as_current_span(self, name: str, attributes: dict | None = None):
        span = MagicMock()
        record = {"name": name, "attributes": attributes or {}, "span": span}
        self.spans.append(record)
        try:
            yield span
        except Exception:
            record["exception_recorded"] = True
            raise


class TestPipelineRunSpan:
    def test_no_op_without_tracer(self) -> None:
        # When no global tracer is set, the helper is a pass-through.
        with pipeline_run_span("orders") as span:
            assert span is None

    def test_creates_span_with_attributes(self) -> None:
        tracer = FakeTracer()
        with pipeline_run_span("orders", attempt=2, tracer=tracer):
            pass
        assert len(tracer.spans) == 1
        rec = tracer.spans[0]
        assert rec["name"] == "flow.pipeline.run"
        assert rec["attributes"]["flow.pipeline.name"] == "orders"
        assert rec["attributes"]["flow.pipeline.attempt"] == 2

    def test_global_tracer_consulted_by_default(self) -> None:
        tracer = FakeTracer()
        set_global_tracer(tracer)
        with pipeline_run_span("orders"):
            pass
        assert len(tracer.spans) == 1

    def test_exception_is_recorded(self) -> None:
        tracer = FakeTracer()
        with (
            pytest.raises(RuntimeError, match="boom"),
            pipeline_run_span("orders", tracer=tracer),
        ):
            raise RuntimeError("boom")
        assert tracer.spans[0].get("exception_recorded") is True


class TestPipelineExecutorEmitsSpan:
    """run_due_with_dag_detailed wraps each pipeline run in a span."""

    def test_successful_pipeline_emits_one_span(self) -> None:
        tracer = FakeTracer()
        set_global_tracer(tracer)

        @pipeline.register(name="orders_sync", schedule="0 * * * *")
        def _fn():
            return {"ok": True}

        result = pipeline.run_due_with_dag_detailed(["orders_sync"])
        assert len(result.fired) == 1
        assert len(tracer.spans) == 1
        assert tracer.spans[0]["attributes"]["flow.pipeline.name"] == "orders_sync"

    def test_failing_pipeline_records_exception(self) -> None:
        tracer = FakeTracer()
        set_global_tracer(tracer)

        @pipeline.register(name="bad", schedule="0 * * * *")
        def _fn():
            raise RuntimeError("kapow")

        pipeline.run_due_with_dag_detailed(["bad"])
        assert tracer.spans[0].get("exception_recorded") is True

    def test_no_spans_emitted_when_tracer_unset(self) -> None:
        # Tracer-free run path must remain unaffected (no perf cost on
        # the hot path when nobody's listening).
        @pipeline.register(name="orders_sync", schedule="0 * * * *")
        def _fn():
            return {"ok": True}

        # No set_global_tracer here.
        result = pipeline.run_due_with_dag_detailed(["orders_sync"])
        assert len(result.fired) == 1
