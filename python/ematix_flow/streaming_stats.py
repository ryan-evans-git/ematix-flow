"""Streaming-pipeline live-stats recorder.

Streaming pipelines (the daemons launched via ``flow consume``) used
to be invisible to the Web UI: a single "running" row with no
``duration_ms`` makes the "Median duration" footer meaningless and
"Next: <ts>" doesn't apply. This module closes that gap by snapshotting
the counters the Rust streaming runner already exports on its
``/metrics`` endpoint (``ematix_streaming_rows_consumed_total`` etc.)
into the RunLog record's ``extras`` field roughly every 30 seconds. The
``/api/pipelines`` aggregator surfaces these fields, and ``Pipelines.svelte``
swaps the "Median duration" footer for throughput + cycle-time when
``kind == "streaming"``.

Snapshots are scraped at ``http://localhost:<metrics_port>/metrics``
(the daemon binds it). Counter deltas over a rolling 1m and 5m window
become throughput; the 1m batch rate becomes an approximate cycle
time. A real p50/p95 batch-duration histogram is a v0.5.1 follow-up —
that needs a new Rust histogram exporter, intentionally out of scope
for this slice.

Opt-in via ``run_log_url=`` to :func:`ematix_flow.streaming.run_pipeline`
or via ``flow consume --run-log-url …`` (or the ``EMATIX_FLOW_RUN_LOG_URL``
env var). Default behavior — no RunLog, no snapshots — is unchanged.
"""
from __future__ import annotations

import re
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen

from ematix_flow.run_log.history import RunHistoryStore, RunRecord

__all__ = [
    "StreamingStatsRecorder",
    "make_streaming_run_id",
    "scrape_counters",
    "summarize_window",
]


# Counter names we care about. Pinned in sync with
# `crates/ematix-flow-core/src/streaming.rs::StreamingPipelineMetricsCounters`.
_COUNTERS = (
    "ematix_streaming_rows_consumed_total",
    "ematix_streaming_rows_written_total",
    "ematix_streaming_batches_total",
    "ematix_streaming_errors_total",
)


# Prometheus exposition format is line-oriented:
#   ematix_streaming_rows_consumed_total{pipeline="orders"} 12345
# We don't fully parse it — we just match the metric name + numeric tail
# for the pipeline we care about. Multi-pipeline endpoints (one process
# hosting several streaming pipelines on one port) work because each
# row has the pipeline label as a const label.
_LINE_RE = re.compile(
    r"^(?P<name>[a-zA-Z_][a-zA-Z0-9_]*)"
    r"(?:\{(?P<labels>[^}]*)\})?\s+"
    r"(?P<value>-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)\s*$",
)


def make_streaming_run_id(pipeline_name: str, started_at: datetime) -> str:
    """Stable run_id for a streaming daemon instance.

    Format: ``<pipeline>-stream-<UTC ts>-<short uuid>``. The trailing
    UUID disambiguates same-second restarts; the ``-stream-`` infix
    distinguishes streaming records from batch records at a glance.
    """
    ts = started_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{pipeline_name}-stream-{ts}-{uuid.uuid4().hex[:8]}"


@dataclass(frozen=True)
class _Sample:
    """One scrape result. Times are seconds since epoch."""

    ts: float
    rows_consumed: int
    rows_written: int
    batches: int
    errors: int


def scrape_counters(metrics_url: str, pipeline_name: str) -> _Sample | None:
    """Scrape one snapshot from a Prometheus ``/metrics`` endpoint.

    Returns None when the endpoint is unreachable (e.g. before the
    Rust daemon has bound its port). Robust to extra metrics + label
    sets — only the four counters we care about are read.
    """
    try:
        with urlopen(metrics_url, timeout=2.0) as resp:
            body = resp.read().decode("utf-8", errors="replace")
    except (URLError, OSError, TimeoutError):
        return None

    values: dict[str, int] = {n: 0 for n in _COUNTERS}
    for line in body.splitlines():
        if not line or line.startswith("#"):
            continue
        m = _LINE_RE.match(line)
        if m is None:
            continue
        name = m.group("name")
        if name not in values:
            continue
        labels = m.group("labels") or ""
        # Filter by pipeline label — multi-pipeline endpoints share
        # one port. The label form is ``pipeline="foo"``; we match
        # exactly so a "foobar" pipeline doesn't accidentally swallow
        # "foo"'s counter.
        if f'pipeline="{pipeline_name}"' not in labels:
            continue
        # Counters are integers; the .0 suffix Prometheus sometimes
        # adds is harmless.
        try:
            values[name] = int(float(m.group("value")))
        except ValueError:
            continue
    return _Sample(
        ts=time.time(),
        rows_consumed=values["ematix_streaming_rows_consumed_total"],
        rows_written=values["ematix_streaming_rows_written_total"],
        batches=values["ematix_streaming_batches_total"],
        errors=values["ematix_streaming_errors_total"],
    )


def summarize_window(
    samples: deque[_Sample],
    *,
    now: float,
    window_seconds: float,
) -> dict[str, float | None]:
    """Compute throughput / batch rate over the trailing ``window_seconds``.

    Returns a dict with ``rows_consumed_per_sec``, ``rows_written_per_sec``,
    ``batches_per_sec``, ``errors_per_sec``, ``avg_batch_cycle_ms``,
    ``span_seconds``. All values None if the window has fewer than two
    samples to delta from.
    """
    cutoff = now - window_seconds
    # Find oldest sample still in the window. ``samples`` is appended
    # newest-last, so linear scan from the left.
    oldest: _Sample | None = None
    for s in samples:
        if s.ts >= cutoff:
            oldest = s
            break
    newest = samples[-1] if samples else None

    if oldest is None or newest is None or oldest is newest:
        return {
            "rows_consumed_per_sec": None,
            "rows_written_per_sec": None,
            "batches_per_sec": None,
            "errors_per_sec": None,
            "avg_batch_cycle_ms": None,
            "span_seconds": None,
        }
    span = newest.ts - oldest.ts
    if span <= 0:
        return {
            "rows_consumed_per_sec": None,
            "rows_written_per_sec": None,
            "batches_per_sec": None,
            "errors_per_sec": None,
            "avg_batch_cycle_ms": None,
            "span_seconds": None,
        }

    rows_in = (newest.rows_consumed - oldest.rows_consumed) / span
    rows_out = (newest.rows_written - oldest.rows_written) / span
    batch_rate = (newest.batches - oldest.batches) / span
    error_rate = (newest.errors - oldest.errors) / span
    # Avg cycle time = how often a batch completes. Honest as a
    # "loop-cadence" metric; calling it "latency" would overclaim
    # (it includes idle waits between source polls). UI labels it
    # "avg batch cycle".
    cycle_ms = (1000.0 / batch_rate) if batch_rate > 0 else None

    return {
        "rows_consumed_per_sec": round(rows_in, 2),
        "rows_written_per_sec": round(rows_out, 2),
        "batches_per_sec": round(batch_rate, 4),
        "errors_per_sec": round(error_rate, 4),
        "avg_batch_cycle_ms": round(cycle_ms, 2) if cycle_ms is not None else None,
        "span_seconds": round(span, 2),
    }


class StreamingStatsRecorder:
    """Background snapshotter for one streaming pipeline.

    Lifecycle:

    1. ``__init__`` opens nothing — just stashes config.
    2. ``start()`` writes the initial ``status="running"`` record to
       the RunLog and spawns the background thread.
    3. The thread loops every ``interval_seconds`` (default 30s):
       scrape the metrics endpoint, update rolling 1m + 5m windows,
       re-write the record with refreshed ``extras`` (same ``run_id``
       so the RunLog deduplicates in place).
    4. ``close(success, error)`` stops the thread (with a final
       snapshot attempt) and writes the terminal
       ``status="succeeded"`` / ``"failed"`` record.

    Use as a context manager:

        with StreamingStatsRecorder(
            run_log=rl, pipeline_name="orders", metrics_port=9100,
        ) as rec:
            run_pipeline_from_toml_str(toml, metrics_port=9100)

    The CM exits clean → ``status="succeeded"``; an exception
    propagating → ``status="failed"`` with the exception repr.
    """

    def __init__(
        self,
        *,
        run_log: RunHistoryStore,
        pipeline_name: str,
        metrics_port: int,
        metrics_host: str = "127.0.0.1",
        interval_seconds: float = 30.0,
        window_1m_seconds: float = 60.0,
        window_5m_seconds: float = 300.0,
        # Max samples retained — caps memory growth on long-running
        # daemons. At 30s cadence we need ~10 for 5m + a little slack.
        max_samples: int = 32,
    ) -> None:
        self._run_log = run_log
        self._pipeline_name = pipeline_name
        self._metrics_url = f"http://{metrics_host}:{metrics_port}/metrics"
        self._interval = interval_seconds
        self._w1 = window_1m_seconds
        self._w5 = window_5m_seconds
        self._samples: deque[_Sample] = deque(maxlen=max_samples)

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._started_at: datetime | None = None
        self._run_id: str | None = None

    # ---- public API -----------------------------------------------

    @property
    def run_id(self) -> str | None:
        return self._run_id

    def start(self) -> str:
        """Write the initial record and spawn the snapshot thread.

        Returns the generated ``run_id`` so callers can log it.
        Idempotent only in the sense that calling twice is a programming
        error — raises ``RuntimeError``.
        """
        if self._thread is not None:
            raise RuntimeError("StreamingStatsRecorder.start() called twice")
        self._started_at = datetime.now(timezone.utc)
        self._run_id = make_streaming_run_id(self._pipeline_name, self._started_at)
        self._write_record(status="running", extras=self._initial_extras())
        self._thread = threading.Thread(
            target=self._loop,
            name=f"streaming-stats[{self._pipeline_name}]",
            daemon=True,
        )
        self._thread.start()
        return self._run_id

    def close(self, *, success: bool, error: str | None = None) -> None:
        """Stop the snapshotter and write the terminal record."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None
        # One last scrape so the terminal record reflects what really
        # shipped — not the snapshot from 30s ago. Best-effort.
        self._maybe_scrape()
        finished_at = datetime.now(timezone.utc)
        extras = self._build_extras(now=time.time())
        if error:
            extras["error"] = error
        self._write_record(
            status=("succeeded" if success else "failed"),
            extras=extras,
            finished_at=finished_at,
            error_summary=error,
        )

    def __enter__(self) -> StreamingStatsRecorder:
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc is None:
            self.close(success=True)
        else:
            self.close(success=False, error=f"{exc_type.__name__}: {exc}")

    # ---- background loop ------------------------------------------

    def _loop(self) -> None:
        # First sample immediately so a graceful exit within the
        # 30s window still leaves one data point.
        self._maybe_scrape()
        while not self._stop_event.wait(self._interval):
            self._maybe_scrape()
            self._write_record(status="running", extras=self._build_extras(now=time.time()))

    def _maybe_scrape(self) -> None:
        sample = scrape_counters(self._metrics_url, self._pipeline_name)
        if sample is not None:
            self._samples.append(sample)

    # ---- record assembly ------------------------------------------

    def _initial_extras(self) -> dict[str, Any]:
        return {
            "snapshot_at": None,
            "rows_consumed_total": 0,
            "rows_written_total": 0,
            "batches_total": 0,
            "errors_total": 0,
            "stats_1m": None,
            "stats_5m": None,
        }

    def _build_extras(self, *, now: float) -> dict[str, Any]:
        latest = self._samples[-1] if self._samples else None
        stats_1m = (
            summarize_window(self._samples, now=now, window_seconds=self._w1)
            if latest is not None
            else None
        )
        stats_5m = (
            summarize_window(self._samples, now=now, window_seconds=self._w5)
            if latest is not None
            else None
        )
        return {
            "snapshot_at": datetime.now(timezone.utc).isoformat(),
            "rows_consumed_total": latest.rows_consumed if latest else 0,
            "rows_written_total": latest.rows_written if latest else 0,
            "batches_total": latest.batches if latest else 0,
            "errors_total": latest.errors if latest else 0,
            "stats_1m": stats_1m,
            "stats_5m": stats_5m,
        }

    def _write_record(
        self,
        *,
        status: str,
        extras: dict[str, Any],
        finished_at: datetime | None = None,
        error_summary: str | None = None,
    ) -> None:
        assert self._started_at is not None and self._run_id is not None
        rec = RunRecord(
            run_id=self._run_id,
            pipeline=self._pipeline_name,
            status=status,
            started_at=self._started_at,
            finished_at=finished_at,
            attempt=1,
            error_summary=error_summary,
            kind="streaming",
            extras=extras,
        )
        try:
            self._run_log.record_run_record(rec)
        except Exception:
            # The snapshotter must never break the streaming daemon.
            # A transient RunLog outage just means the UI's stats
            # become stale — the data path keeps flowing.
            pass
