"""OpenTelemetry tracing — per-pipeline-run spans.

Wraps each pipeline execution in a ``flow.pipeline.run`` span so
traces land in Tempo / Datadog / Honeycomb / any OTLP-compatible
backend. The dependency on ``opentelemetry-sdk`` is opt-in: callers
either pass an existing ``Tracer`` instance or one of the URL-shaped
factory helpers here.

URL forms:

  otel://stdout                   — ConsoleSpanExporter (dev)
  otel+otlp+grpc://host:port      — OTLP gRPC exporter
  otel+otlp+http://host:port      — OTLP HTTP exporter

The runner is expected to call :func:`configure_tracer_from_url` once
at process start and then pass the resulting tracer to
:func:`pipeline.run_due_with_dag_detailed`. The pipeline module
itself takes a duck-typed ``Tracer`` (anything with a
``start_as_current_span`` context manager) so testing doesn't need
the real OTel SDK.
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Protocol


class Tracer(Protocol):
    """Minimal Tracer interface: anything with
    ``start_as_current_span(name, attributes=...)`` satisfies this.
    Matches the public OpenTelemetry SDK's Tracer."""

    def start_as_current_span(self, name: str, attributes: dict | None = ...): ...


_FLOW_TRACER: Any | None = None


def set_global_tracer(tracer: Any | None) -> None:
    """Process-global tracer. ``None`` disables tracing."""
    global _FLOW_TRACER
    _FLOW_TRACER = tracer


def get_global_tracer() -> Any | None:
    return _FLOW_TRACER


@contextmanager
def pipeline_run_span(
    pipeline_name: str,
    attempt: int = 1,
    tracer: Any | None = None,
):
    """Open a ``flow.pipeline.run`` span around a pipeline execution.

    Records the pipeline name, attempt count, and (on exception) the
    error type + message. Falls through as a no-op when no tracer is
    configured — callers can wrap unconditionally and let the helper
    decide.
    """
    active_tracer = tracer if tracer is not None else _FLOW_TRACER
    if active_tracer is None:
        yield None
        return
    attrs = {
        "flow.pipeline.name": pipeline_name,
        "flow.pipeline.attempt": attempt,
    }
    with active_tracer.start_as_current_span(
        "flow.pipeline.run", attributes=attrs,
    ) as span:
        try:
            yield span
        except Exception as exc:
            # Record error on the span before re-raising. Tracer
            # implementations vary — wrap in try/except so a missing
            # method doesn't double-fault the caller.
            try:
                if span is not None and hasattr(span, "record_exception"):
                    span.record_exception(exc)
                if span is not None and hasattr(span, "set_status"):
                    from opentelemetry.trace import Status, StatusCode

                    span.set_status(Status(StatusCode.ERROR, str(exc)))
            except Exception:
                pass
            raise


def configure_tracer_from_url(url: str) -> Any:
    """Build + register a tracer from a URL.

    See module docstring for supported URL forms. Imports
    ``opentelemetry-sdk`` lazily so callers without the extra still
    get an ImportError pointing to the right pip install.
    """
    try:
        from opentelemetry import trace
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import (
            BatchSpanProcessor,
            ConsoleSpanExporter,
        )
    except ImportError as exc:
        raise ImportError(
            "OTEL tracing requires opentelemetry-sdk. "
            "Install with `pip install ematix-flow[metrics-otel]`."
        ) from exc

    resource = Resource.create({"service.name": "ematix-flow"})
    provider = TracerProvider(resource=resource)

    if url == "otel://stdout":
        provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
    elif url.startswith("otel+otlp+grpc://"):
        try:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
                OTLPSpanExporter,
            )
        except ImportError as exc:
            raise ImportError(
                "OTLP gRPC exporter needs opentelemetry-exporter-otlp. "
                "Install with `pip install ematix-flow[metrics-otel]`."
            ) from exc
        endpoint = url[len("otel+otlp+grpc://"):]
        # Prepend http:// scheme because the OTLP gRPC exporter wants
        # a full URL even for insecure endpoints.
        if not endpoint.startswith(("http://", "https://")):
            endpoint = f"http://{endpoint}"
        provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True))
        )
    elif url.startswith("otel+otlp+http://"):
        try:
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
                OTLPSpanExporter,
            )
        except ImportError as exc:
            raise ImportError(
                "OTLP HTTP exporter needs opentelemetry-exporter-otlp. "
                "Install with `pip install ematix-flow[metrics-otel]`."
            ) from exc
        endpoint = url[len("otel+otlp+http://"):]
        if not endpoint.startswith(("http://", "https://")):
            endpoint = f"http://{endpoint}"
        # Strip a trailing /v1/traces if the user supplied it; the
        # OTLP HTTP exporter appends it itself.
        endpoint = endpoint.rstrip("/")
        provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint + "/v1/traces"))
        )
    else:
        raise ValueError(
            f"unsupported tracer URL {url!r}; "
            f"expected otel://stdout, otel+otlp+grpc://host:port, or "
            f"otel+otlp+http://host:port"
        )

    trace.set_tracer_provider(provider)
    tracer = trace.get_tracer("ematix-flow")
    set_global_tracer(tracer)
    return tracer
