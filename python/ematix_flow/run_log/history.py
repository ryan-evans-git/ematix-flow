"""Rich per-run history for the Web UI (Phase 4a-2).

The existing :class:`ematix_flow.run_log.protocol.RunLog` is a
**latest-run summary** — ``record_run(name, ts, success)``
overwrites a per-pipeline timestamp + boolean. That shape is fine
for the scheduler's "should this pipeline fire now?" question but
can't power a list view of historical runs.

This module adds a **separate Protocol** (kept distinct from
``RunLog`` so legacy backends don't have to implement it) plus a
data class describing each run:

- :class:`RunRecord` — one row per (pipeline, run_id) attempt
  family. Carries enough to render the Web UI's list + detail
  pages: ids, timestamps, status, attempt count, failed step
  (batch) / watermark (streaming), and any error summary.
- :class:`RunHistoryStore` — query Protocol with ``list_runs``,
  ``get_run``, and the ``record_run_record`` writer. Backends
  opt in by implementing it alongside ``RunLog``.
- :class:`InMemoryRunHistory` — process-local store that
  satisfies the Protocol. Used by tests and by
  ``flow web --run-log memory://`` for ad-hoc inspection.

Backends that don't implement ``RunHistoryStore`` (S3, Azure, GCS —
blob stores can't do range queries cheaply) still work with the
scheduler; the Web UI just refuses to start against them with a
clear error pointing operators at a SQL backend.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol, runtime_checkable

__all__ = [
    "InMemoryRunHistory",
    "RunHistoryStore",
    "RunRecord",
]


_VALID_STATUS = frozenset(
    {
        "running",
        "paused",
        "succeeded",
        "failed",
        "cancelled",
    }
)


@dataclass(frozen=True)
class RunRecord:
    """A single run's worth of history for the Web UI.

    Field semantics:

    - ``run_id`` is a stable per-attempt-family id (ULID-shaped in
      the suggested impl, but any unique string works).
    - ``pipeline`` is the registered name.
    - ``status`` ∈ {``running``, ``paused``, ``succeeded``,
      ``failed``, ``cancelled``}.
    - ``started_at`` is the UTC timestamp the first attempt began.
    - ``finished_at`` is None while ``status`` is ``running`` or
      ``paused``.
    - ``attempt`` is 1-based; bumps on each retry within a single
      run family.
    - ``failed_step`` (batch) — the name of the step that errored
      out, or None for streaming / succeeded runs.
    - ``failed_watermark`` (streaming) — the last committed
      watermark when the run failed, or None.
    - ``error_summary`` — single-line error message, or None.
    - ``kind`` ∈ {``"batch"``, ``"streaming"``} — drives the
      "Resume from watermark" vs "Restart from failed step" UI
      action label.
    - ``extras`` — open-ended metadata. Backends are free to use
      this for store-specific fields without schema changes
      (e.g. K8s job id, Lambda request id).
    """

    run_id: str
    pipeline: str
    status: str
    started_at: datetime
    finished_at: datetime | None = None
    attempt: int = 1
    failed_step: str | None = None
    failed_watermark: str | None = None
    error_summary: str | None = None
    kind: str = "batch"
    extras: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.status not in _VALID_STATUS:
            raise ValueError(
                f"RunRecord.status must be one of {sorted(_VALID_STATUS)}, "
                f"got {self.status!r}"
            )
        if self.kind not in {"batch", "streaming"}:
            raise ValueError(
                f"RunRecord.kind must be 'batch' or 'streaming', got {self.kind!r}"
            )
        if self.attempt < 1:
            raise ValueError(f"RunRecord.attempt must be >= 1, got {self.attempt}")
        if self.status in {"running", "paused"} and self.finished_at is not None:
            raise ValueError(
                f"RunRecord with status {self.status!r} must have "
                "finished_at=None (the run hasn't ended yet)"
            )

    @property
    def duration_ms(self) -> int | None:
        """End-to-end runtime in milliseconds. Returns None for
        in-flight or paused runs."""
        if self.finished_at is None:
            return None
        delta = self.finished_at - self.started_at
        return int(delta.total_seconds() * 1000)

    def to_summary_dict(self) -> dict[str, Any]:
        """JSON-serialisable shape for the ``GET /api/runs`` response."""
        return {
            "run_id": self.run_id,
            "pipeline": self.pipeline,
            "status": self.status,
            "started_at": _iso(self.started_at),
            "finished_at": _iso(self.finished_at) if self.finished_at else None,
            "duration_ms": self.duration_ms,
            "attempt": self.attempt,
            "failed_step": self.failed_step,
            "kind": self.kind,
        }

    def to_detail_dict(self) -> dict[str, Any]:
        """JSON-serialisable shape for the ``GET /api/runs/:id`` response.

        Mirrors :meth:`to_summary_dict` and adds the fields the
        detail page renders: error summary, watermark, extras.
        """
        return {
            **self.to_summary_dict(),
            "failed_watermark": self.failed_watermark,
            "error_summary": self.error_summary,
            "extras": dict(self.extras),
        }


def _iso(ts: datetime) -> str:
    """Render a datetime in ISO 8601 UTC. Mirrors the existing
    iso utility in `_iso.py` but kept inline so this module has
    zero internal cross-imports."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


# ---- Protocol ------------------------------------------------------


@runtime_checkable
class RunHistoryStore(Protocol):
    """Query + write surface for the Web UI's run-history view.

    Implementations should be idempotent on ``record_run_record``
    (re-recording the same run_id replaces the prior copy).
    ``list_runs`` and ``get_run`` are query-only.
    """

    def record_run_record(self, record: RunRecord) -> None:
        """Persist a single record. Idempotent — re-recording the
        same ``run_id`` replaces the prior copy in place."""

    def list_runs(
        self,
        *,
        pipeline: str | None = None,
        status: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[RunRecord], int]:
        """Return ``(records, total)`` matching the filters.

        ``records`` is the slice ``[offset : offset + limit]`` of
        the filtered list, sorted by ``started_at`` descending.
        ``total`` is the unfiltered-by-page count, used by the UI
        for pagination.
        """

    def get_run(self, run_id: str) -> RunRecord | None:
        """Return the record for ``run_id`` or None if unknown."""


# ---- In-memory impl ------------------------------------------------


class InMemoryRunHistory:
    """Process-local :class:`RunHistoryStore` implementation.

    Used by tests and by ``flow web --run-log memory://`` for ad-hoc
    inspection. Records are kept in a list ordered by
    ``started_at`` descending; ``record_run_record`` is idempotent
    via run_id.

    Not thread-safe — wrap in :class:`threading.RLock` if you need
    concurrent writers.
    """

    def __init__(self) -> None:
        # Insert order is *write* order, not started_at order. We
        # re-sort lazily in list_runs.
        self._by_id: dict[str, RunRecord] = {}

    def record_run_record(self, record: RunRecord) -> None:
        self._by_id[record.run_id] = record

    def list_runs(
        self,
        *,
        pipeline: str | None = None,
        status: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[RunRecord], int]:
        rows = list(self._by_id.values())
        if pipeline:
            rows = [r for r in rows if r.pipeline == pipeline]
        if status:
            rows = [r for r in rows if r.status == status]
        rows.sort(key=lambda r: r.started_at, reverse=True)
        total = len(rows)
        return rows[offset : offset + limit], total

    def get_run(self, run_id: str) -> RunRecord | None:
        return self._by_id.get(run_id)

    # Convenience helpers --------------------------------------------

    def __len__(self) -> int:
        return len(self._by_id)

    def clear(self) -> None:
        """Drop all records. Useful for tests."""
        self._by_id.clear()
