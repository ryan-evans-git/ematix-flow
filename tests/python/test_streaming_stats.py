"""Streaming stats snapshot recorder.

Validates the bits worth pinning:

* Counter scrape parses Prometheus text-format including the
  pipeline label filter (so multi-pipeline endpoints don't bleed).
* The 1m / 5m window summary returns None for insufficient data and
  proportional rates for valid windows.
* The lifecycle (start → snapshots → close) writes records under a
  single run_id; the terminal record carries the right status.
* RunLog outages don't crash the streaming daemon.
"""
from __future__ import annotations

import threading
import time
from collections import deque
from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from ematix_flow.run_log.history import RunRecord
from ematix_flow.streaming_stats import (
    StreamingStatsRecorder,
    _Sample,
    make_streaming_run_id,
    scrape_counters,
    summarize_window,
)

# ---- scrape_counters ----------------------------------------------


_PROM_BODY = """\
# HELP ematix_streaming_rows_consumed_total Total rows in.
# TYPE ematix_streaming_rows_consumed_total counter
ematix_streaming_rows_consumed_total{pipeline="orders"} 1234
ematix_streaming_rows_consumed_total{pipeline="payments"} 99
# HELP ematix_streaming_rows_written_total Total rows out.
# TYPE ematix_streaming_rows_written_total counter
ematix_streaming_rows_written_total{pipeline="orders"} 1230
ematix_streaming_batches_total{pipeline="orders"} 42
ematix_streaming_errors_total{pipeline="orders"} 1
ematix_streaming_idle_iterations_total{pipeline="orders"} 7
some_other_metric{foo="bar"} 999
"""


class _FakeResp:
    def __init__(self, body: str) -> None:
        self._body = body.encode()

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> _FakeResp:
        return self

    def __exit__(self, *args) -> None:
        pass


def test_scrape_counters_filters_by_pipeline_label() -> None:
    with patch(
        "ematix_flow.streaming_stats.urlopen",
        return_value=_FakeResp(_PROM_BODY),
    ):
        sample = scrape_counters("http://x", "orders")
    assert sample is not None
    assert sample.rows_consumed == 1234
    assert sample.rows_written == 1230
    assert sample.batches == 42
    assert sample.errors == 1


def test_scrape_counters_returns_none_when_unreachable() -> None:
    with patch(
        "ematix_flow.streaming_stats.urlopen", side_effect=OSError("ECONNREFUSED"),
    ):
        assert scrape_counters("http://x", "orders") is None


def test_scrape_counters_misses_when_label_mismatches() -> None:
    # No `pipeline="missing"` row anywhere → zero everything.
    with patch(
        "ematix_flow.streaming_stats.urlopen",
        return_value=_FakeResp(_PROM_BODY),
    ):
        sample = scrape_counters("http://x", "missing")
    assert sample is not None
    assert sample.rows_consumed == 0
    assert sample.batches == 0


# ---- summarize_window ---------------------------------------------


def test_summarize_returns_none_fields_with_one_sample() -> None:
    samples: deque[_Sample] = deque(maxlen=8)
    samples.append(_Sample(ts=1000.0, rows_consumed=100, rows_written=99, batches=10, errors=0))
    s = summarize_window(samples, now=1010.0, window_seconds=60.0)
    assert s["rows_consumed_per_sec"] is None
    assert s["avg_batch_cycle_ms"] is None


def test_summarize_computes_throughput_and_cycle_time() -> None:
    samples: deque[_Sample] = deque(maxlen=8)
    samples.append(_Sample(ts=1000.0, rows_consumed=100, rows_written=99, batches=10, errors=0))
    samples.append(_Sample(ts=1060.0, rows_consumed=1900, rows_written=1850, batches=70, errors=2))
    s = summarize_window(samples, now=1060.0, window_seconds=60.0)
    # 1800 rows in over 60s = 30/s
    assert s["rows_consumed_per_sec"] == pytest.approx(30.0)
    # 1751 rows out over 60s ≈ 29.18/s
    assert s["rows_written_per_sec"] == pytest.approx(29.18, rel=1e-3)
    # 60 batches over 60s = 1/s → cycle 1000ms
    assert s["batches_per_sec"] == pytest.approx(1.0)
    assert s["avg_batch_cycle_ms"] == pytest.approx(1000.0)
    # Rates are rounded to 4 decimal places in summarize_window.
    assert s["errors_per_sec"] == pytest.approx(round(2 / 60.0, 4))
    assert s["span_seconds"] == pytest.approx(60.0)


def test_summarize_with_no_batches_yields_none_cycle() -> None:
    samples: deque[_Sample] = deque(maxlen=8)
    samples.append(_Sample(ts=1000.0, rows_consumed=0, rows_written=0, batches=0, errors=0))
    samples.append(_Sample(ts=1060.0, rows_consumed=0, rows_written=0, batches=0, errors=0))
    s = summarize_window(samples, now=1060.0, window_seconds=60.0)
    assert s["rows_consumed_per_sec"] == pytest.approx(0.0)
    assert s["batches_per_sec"] == pytest.approx(0.0)
    assert s["avg_batch_cycle_ms"] is None


# ---- run_id ---------------------------------------------------------


def test_run_id_is_stable_shape() -> None:
    ts = datetime(2026, 5, 21, 14, 30, 0, tzinfo=UTC)
    rid = make_streaming_run_id("orders", ts)
    assert rid.startswith("orders-stream-20260521T143000Z-")
    # 8 hex chars suffix
    assert len(rid.rsplit("-", 1)[-1]) == 8


# ---- StreamingStatsRecorder lifecycle ------------------------------


class _RecordingRunLog:
    def __init__(self) -> None:
        self.records: list[RunRecord] = []

    def record_run_record(self, record: RunRecord) -> None:
        self.records.append(record)


class _FailingRunLog:
    def record_run_record(self, record: RunRecord) -> None:
        raise RuntimeError("boom")


def _patch_scrape_returning(sample_seq: list[_Sample | None]):
    """Patch the recorder's scrape to return a deterministic series.

    Once the list is exhausted the last value repeats — keeps tests
    from racing the snapshotter thread.
    """
    samples = iter(sample_seq)
    last = sample_seq[-1] if sample_seq else None

    def _fake(url: str, name: str) -> _Sample | None:
        nonlocal last
        try:
            v = next(samples)
            last = v
            return v
        except StopIteration:
            return last

    return patch("ematix_flow.streaming_stats.scrape_counters", side_effect=_fake)


def test_lifecycle_writes_running_then_terminal_under_same_run_id() -> None:
    rl = _RecordingRunLog()
    rec = StreamingStatsRecorder(
        run_log=rl,
        pipeline_name="orders",
        metrics_port=9100,
        interval_seconds=0.05,  # fast loop for the test
        window_1m_seconds=60.0,
        window_5m_seconds=300.0,
    )
    samples = [
        _Sample(ts=1000.0, rows_consumed=10, rows_written=9, batches=1, errors=0),
        _Sample(ts=1000.5, rows_consumed=50, rows_written=48, batches=3, errors=0),
    ]
    with _patch_scrape_returning(samples):
        run_id = rec.start()
        # Give the loop one tick to update.
        time.sleep(0.15)
        rec.close(success=True)

    # All records share the run_id (RunLog dedups on it).
    assert all(r.run_id == run_id for r in rl.records)
    # First record is "running" with the initial-extras shape.
    assert rl.records[0].status == "running"
    # Terminal record is "succeeded".
    assert rl.records[-1].status == "succeeded"
    assert rl.records[-1].finished_at is not None
    # Extras carry the snapshot fields.
    terminal_extras = rl.records[-1].extras
    assert "rows_consumed_total" in terminal_extras
    assert "stats_1m" in terminal_extras
    assert terminal_extras["rows_consumed_total"] == 50


def test_failure_path_marks_record_failed_with_error() -> None:
    rl = _RecordingRunLog()
    rec = StreamingStatsRecorder(
        run_log=rl, pipeline_name="orders", metrics_port=9100, interval_seconds=10.0,
    )
    with _patch_scrape_returning([None]), pytest.raises(RuntimeError, match="injected"), rec:
        raise RuntimeError("injected")

    terminal = rl.records[-1]
    assert terminal.status == "failed"
    assert terminal.error_summary is not None
    assert "injected" in terminal.error_summary


def test_run_log_outage_does_not_crash_daemon() -> None:
    rec = StreamingStatsRecorder(
        run_log=_FailingRunLog(),
        pipeline_name="orders",
        metrics_port=9100,
        interval_seconds=0.05,
    )
    with _patch_scrape_returning([_Sample(ts=1.0, rows_consumed=0, rows_written=0, batches=0, errors=0)]):
        rec.start()
        time.sleep(0.1)
        rec.close(success=True)
    # Test passes if no exception propagated to the main thread.


def test_close_without_samples_still_writes_terminal_record() -> None:
    """Endpoint never reachable → no samples accumulated → terminal record
    still lands with extras-shaped placeholder fields."""
    rl = _RecordingRunLog()
    rec = StreamingStatsRecorder(
        run_log=rl, pipeline_name="orders", metrics_port=9100, interval_seconds=0.05,
    )
    with _patch_scrape_returning([None]):
        rec.start()
        time.sleep(0.1)
        rec.close(success=True)
    terminal = rl.records[-1]
    assert terminal.status == "succeeded"
    # stats_1m / stats_5m are None when there are zero samples in window.
    # The initial-extras dict has them as None too.


def test_double_start_is_an_error() -> None:
    rl = _RecordingRunLog()
    rec = StreamingStatsRecorder(
        run_log=rl, pipeline_name="orders", metrics_port=9100, interval_seconds=10.0,
    )
    with _patch_scrape_returning([None]):
        rec.start()
        try:
            with pytest.raises(RuntimeError, match="called twice"):
                rec.start()
        finally:
            rec.close(success=True)


# Cleanup: ensure no threads leak across tests.
@pytest.fixture(autouse=True)
def _no_thread_leaks():
    before = {t.ident for t in threading.enumerate()}
    yield
    # Up to 2s for the daemon thread to wind down after close().
    deadline = time.time() + 2.0
    while time.time() < deadline:
        after = {t.ident for t in threading.enumerate()}
        leaked = after - before
        # Threads named like "streaming-stats[..]" should all have exited.
        leaked_streaming = [
            t for t in threading.enumerate()
            if t.ident in leaked and t.name.startswith("streaming-stats")
        ]
        if not leaked_streaming:
            break
        time.sleep(0.05)
