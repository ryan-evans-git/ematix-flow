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
        "requested",  # enqueued via Phase 4b restart/rerun, awaiting pickup
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
        if (
            self.status in {"requested", "running", "paused"}
            and self.finished_at is not None
        ):
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
    """Query + write + mutating-action surface for the Web UI.

    Methods split into three layers:

    **Query**: ``list_runs``, ``get_run``.
    **Write**: ``record_run_record`` (idempotent on ``run_id``).
    **Actions** (Phase 4b): ``enqueue_restart``, ``enqueue_rerun``,
    ``set_pause`` — implementations record the user's intent into
    the store; the scheduler claim loop picks the new rows up on
    its next tick and dispatches them.

    Actions write **new rows** for restart / rerun (they're new
    run families with status ``"requested"``) and mutate the
    existing row's ``extras["pause_requested"]`` flag for pause /
    resume (the same run, asked to stop or continue).
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

    # ---- Mutating actions (Phase 4b) ------------------------------

    def enqueue_restart(self, prior_run_id: str, from_step: str | None) -> str:
        """Enqueue a restart of ``prior_run_id``.

        Writes a new :class:`RunRecord` with status
        ``"requested"`` and ``extras`` containing
        ``{"restart_from_step": <step>, "prior_run_id": <id>}``.
        The scheduler picks the new row up and resumes the
        pipeline at ``from_step`` (batch) or from the prior
        watermark (when ``from_step`` is None).

        Returns the new run id.
        """

    def enqueue_rerun(self, prior_run_id: str) -> str:
        """Enqueue a full re-run of ``prior_run_id``.

        Writes a new :class:`RunRecord` with status
        ``"requested"`` and ``extras`` containing
        ``{"rerun_full": true, "prior_run_id": <id>}``. The
        scheduler picks the new row up and runs the pipeline from
        scratch.

        Returns the new run id.
        """

    def set_pause(self, run_id: str, pause: bool) -> None:
        """Request a pause (or resume) on a running/paused run.

        Sets ``extras["pause_requested"] = pause`` on the existing
        row. The worker checks this at the next step / watermark
        boundary and transitions to ``"paused"`` (or back to
        ``"running"``). Idempotent — setting pause=True on an
        already-paused run is a no-op.
        """

    # ---- Scheduler-integration hooks (Phase 4b-2) -----------------

    def pending_actions(self) -> list[RunRecord]:
        """Return rows the scheduler should act on this tick.

        Two row classes qualify:

        1. ``status == "requested"`` — a restart or rerun enqueued
           via :meth:`enqueue_restart` / :meth:`enqueue_rerun` that
           the scheduler hasn't dispatched yet.
        2. Rows with ``extras["pause_requested"]`` set whose status
           doesn't yet match (running with pause_requested=True or
           paused with pause_requested=False). These are
           transitions the worker needs to apply at the next
           boundary.

        Implementations should be idempotent: returning the same
        row twice across ticks is harmless because the consumer
        (``consume_requested_run`` or the worker's transition
        logic) is itself idempotent.
        """

    def consume_requested_run(self, run_id: str) -> bool:
        """Atomically transition ``run_id`` from ``"requested"`` to
        ``"running"``. Returns True if the transition fired, False
        if the row wasn't in ``"requested"`` state (another worker
        already claimed it, or the user cancelled). Idempotent.
        """


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

    # ---- Mutating actions (Phase 4b) ------------------------------

    def enqueue_restart(
        self, prior_run_id: str, from_step: str | None
    ) -> str:
        prior = self._by_id.get(prior_run_id)
        if prior is None:
            raise KeyError(
                f"enqueue_restart: prior run_id {prior_run_id!r} not found"
            )
        from datetime import datetime as _dt, timezone as _tz
        import uuid as _uuid

        new_id = f"req-{_uuid.uuid4().hex}"
        new_record = RunRecord(
            run_id=new_id,
            pipeline=prior.pipeline,
            status="requested",
            started_at=_dt.now(_tz.utc),
            kind=prior.kind,
            extras={
                "restart_from_step": from_step,
                "prior_run_id": prior_run_id,
            },
        )
        self._by_id[new_id] = new_record
        return new_id

    def enqueue_rerun(self, prior_run_id: str) -> str:
        prior = self._by_id.get(prior_run_id)
        if prior is None:
            raise KeyError(
                f"enqueue_rerun: prior run_id {prior_run_id!r} not found"
            )
        from datetime import datetime as _dt, timezone as _tz
        import uuid as _uuid

        new_id = f"req-{_uuid.uuid4().hex}"
        new_record = RunRecord(
            run_id=new_id,
            pipeline=prior.pipeline,
            status="requested",
            started_at=_dt.now(_tz.utc),
            kind=prior.kind,
            extras={
                "rerun_full": True,
                "prior_run_id": prior_run_id,
            },
        )
        self._by_id[new_id] = new_record
        return new_id

    def set_pause(self, run_id: str, pause: bool) -> None:
        record = self._by_id.get(run_id)
        if record is None:
            raise KeyError(f"set_pause: run_id {run_id!r} not found")
        # RunRecord is frozen; build a new one with updated extras.
        new_extras = dict(record.extras)
        new_extras["pause_requested"] = pause
        # Use dataclasses.replace via __dataclass_fields__ — frozen
        # but mutable through replace.
        from dataclasses import replace as _replace

        self._by_id[run_id] = _replace(record, extras=new_extras)

    # ---- Scheduler-integration hooks (Phase 4b-2) -----------------

    def pending_actions(self) -> list[RunRecord]:
        out: list[RunRecord] = []
        for r in self._by_id.values():
            if r.status == "requested":
                out.append(r)
                continue
            pause_req = r.extras.get("pause_requested")
            if pause_req is True and r.status == "running":
                out.append(r)
            elif pause_req is False and r.status == "paused":
                out.append(r)
        return out

    def consume_requested_run(self, run_id: str) -> bool:
        rec = self._by_id.get(run_id)
        if rec is None or rec.status != "requested":
            return False
        from dataclasses import replace as _replace

        self._by_id[run_id] = _replace(rec, status="running")
        return True

    # Convenience helpers --------------------------------------------

    def __len__(self) -> int:
        return len(self._by_id)

    def clear(self) -> None:
        """Drop all records. Useful for tests."""
        self._by_id.clear()
