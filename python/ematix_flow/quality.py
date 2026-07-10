"""Data-quality expectations + freshness SLOs.

This is a thin integration layer over the published ``ematix-probe``
library (``pip install "ematix-flow[quality]"``). It:

- maps a pipeline *target connection* to an ``ematix_probe`` source,
- adapts an ematix-flow :class:`~ematix_flow.table.ManagedTable` to the
  probe's declarative-table protocol (PK → ``unique``,
  non-nullable → ``not_null``),
- runs expectations as a **post-write** stage and reduces the result to
  a :class:`QualityOutcome` with an explicit ``warn`` / ``fail`` policy,
- evaluates **freshness SLOs** — both at run-end and (via
  :class:`FreshnessEvaluator`) on a schedule, so a *stalled* pipeline
  that stops running still breaches its SLO.

Everything here is **opt-in**: a pipeline that declares neither
``expectations=`` nor ``freshness_sla=`` never imports ``ematix_probe``
and behaves exactly as before.

Verdict vocabulary::

    pass   every assertion passed
    warn   no hard failures, but at least one assertion could not be
           evaluated (``error`` from the probe — e.g. an unsupported
           column type) — surfaced, never blocks
    fail   at least one assertion found a real data violation
    error  the stage itself could not run (kept internal; callers see
           ``warn``)

The ``error``→``warn`` demotion is deliberate: an engine limitation
(the probe can't check ``unique`` on an Int32 key, say) must not turn a
pipeline red the way a genuine data violation does.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

# Policy values for ``on_quality_failure=``.
POLICY_WARN = "warn"
POLICY_FAIL = "fail"
_VALID_POLICIES = (POLICY_WARN, POLICY_FAIL)


class QualityError(RuntimeError):
    """Base class for data-quality errors."""


class QualityDependencyMissing(QualityError):
    """Raised when a pipeline declares expectations/freshness but the
    ``ematix-probe`` extra isn't installed."""


class QualityCheckFailed(QualityError):
    """Raised from the quality stage when expectations fail and the
    pipeline's ``on_quality_failure`` policy is ``"fail"``. Propagates
    like any pipeline error, so the run is recorded as failed."""

    def __init__(self, outcome: QualityOutcome):
        self.outcome = outcome
        failed = ", ".join(
            a.name for a in outcome.assertions if a.verdict == "fail"
        )
        super().__init__(
            f"data-quality check failed for {outcome.qualified_table}: "
            f"{outcome.checks_failed}/{outcome.checks_total} assertion(s) "
            f"failed [{failed}]"
        )


# --------------------------------------------------------------------------
# Result types
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class QualityAssertion:
    """One assertion's outcome, flattened from the probe's
    ``AssertionResult``."""

    name: str
    verdict: str  # pass | fail | error
    message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "verdict": self.verdict, "message": self.message}


@dataclass(frozen=True)
class QualityOutcome:
    """Reduced result of running a suite of expectations against a table."""

    pipeline: str
    table: str
    schema: str | None
    verdict: str  # pass | warn | fail
    assertions: tuple[QualityAssertion, ...] = ()
    duration_seconds: float = 0.0
    started_at: datetime | None = None
    finished_at: datetime | None = None

    @property
    def qualified_table(self) -> str:
        return f"{self.schema}.{self.table}" if self.schema else self.table

    @property
    def checks_total(self) -> int:
        return len(self.assertions)

    @property
    def checks_failed(self) -> int:
        return sum(1 for a in self.assertions if a.verdict == "fail")

    @property
    def checks_errored(self) -> int:
        return sum(1 for a in self.assertions if a.verdict == "error")

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline": self.pipeline,
            "table": self.table,
            "schema": self.schema,
            "verdict": self.verdict,
            "checks_total": self.checks_total,
            "checks_failed": self.checks_failed,
            "checks_errored": self.checks_errored,
            "assertions": [a.to_dict() for a in self.assertions],
            "duration_seconds": self.duration_seconds,
        }


@dataclass(frozen=True)
class FreshnessState:
    """Result of a freshness evaluation for one pipeline."""

    pipeline: str
    sla_seconds: int
    lag_seconds: int | None  # None when there has never been a success
    state: str  # healthy | warning | breached | unknown
    last_success: datetime | None = None
    evaluated_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline": self.pipeline,
            "sla_seconds": self.sla_seconds,
            "lag_seconds": self.lag_seconds,
            "state": self.state,
            "last_success": self.last_success.isoformat()
            if self.last_success
            else None,
            "evaluated_at": self.evaluated_at.isoformat()
            if self.evaluated_at
            else None,
        }


# --------------------------------------------------------------------------
# Duration parsing
# --------------------------------------------------------------------------


def parse_duration(value: str | int | float | timedelta) -> timedelta:
    """Parse a freshness/SLA duration into a :class:`timedelta`.

    Accepts a ``timedelta``, a number of seconds, or a compact string
    like ``"90s"``, ``"15m"``, ``"6h"``, ``"2d"`` (or bare ``"3600"``
    for seconds).
    """
    if isinstance(value, timedelta):
        return value
    if isinstance(value, (int, float)):
        return timedelta(seconds=float(value))
    s = str(value).strip().lower()
    if not s:
        raise ValueError("empty duration")
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    if s[-1] in units:
        num = s[:-1]
        try:
            return timedelta(seconds=float(num) * units[s[-1]])
        except ValueError as exc:
            raise ValueError(f"invalid duration {value!r}") from exc
    try:
        return timedelta(seconds=float(s))
    except ValueError as exc:
        raise ValueError(
            f"invalid duration {value!r}; use e.g. '6h', '30m', '2d', or seconds"
        ) from exc


# --------------------------------------------------------------------------
# Probe wiring
# --------------------------------------------------------------------------


def _require_probe():
    """Import ematix-probe lazily; raise a helpful error if the extra
    isn't installed."""
    try:
        import ematix_probe  # noqa: F401
        from ematix_probe import source as probe_source
        from ematix_probe.flow import probe_from_table
        from ematix_probe.probe import DataProbe, Tester

        return probe_source, probe_from_table, DataProbe, Tester
    except ImportError as exc:  # pragma: no cover - exercised via monkeypatch
        raise QualityDependencyMissing(
            "data-quality checks require the ematix-probe library; install it "
            'with:  pip install "ematix-flow[quality]"'
        ) from exc


def source_for_connection(conn: Any):
    """Map a pipeline *target connection* to an ``ematix_probe`` source.

    Returns ``None`` for connection kinds the probe can't read (MySQL,
    SQLite, warehouses, object stores) — the caller skips the check with
    a warning rather than failing the run.
    """
    probe_source, _, _, _ = _require_probe()
    kind = getattr(conn, "kind", None)
    if kind == "postgres":
        return probe_source.postgres(conn.url)
    if kind == "duckdb":
        return probe_source.duckdb(conn.path)
    return None


class _ColumnView:
    """Adapts an ematix-flow ``Column`` to the probe's ``_ColumnLike``
    protocol (``name`` / ``nullable`` / ``primary_key``)."""

    __slots__ = ("name", "nullable", "primary_key")

    def __init__(self, name: str, nullable: bool, primary_key: bool):
        self.name = name
        self.nullable = nullable
        self.primary_key = primary_key


class _TableView:
    """Adapts a ``ManagedTable`` subclass to the probe's ``_TableLike``
    protocol so ``probe_from_table`` can auto-derive not_null/unique."""

    def __init__(self, table_cls: Any):
        self.__name__ = table_cls.__name__
        self.__tablename__ = table_cls.__tablename__
        self.__schema__ = getattr(table_cls, "__schema__", None)
        self.columns = tuple(
            _ColumnView(col.name, col.nullable, col.primary_key)
            for _, col in table_cls._columns()
        )


def _reduce_verdict(assertions: Sequence[QualityAssertion]) -> str:
    """pass/warn/fail from per-assertion verdicts (error → warn)."""
    if any(a.verdict == "fail" for a in assertions):
        return "fail"
    if any(a.verdict == "error" for a in assertions):
        return POLICY_WARN
    return "pass"


def run_expectations(
    *,
    target_connection: Any,
    pipeline_name: str,
    target_cls: Any | None = None,
    expectations: Callable[[Any], None] | None = None,
    freshness_column: str | None = None,
    freshness_sla: str | int | float | timedelta | None = None,
) -> QualityOutcome | None:
    """Build and run a data-quality probe against the pipeline's target.

    ``target_cls`` (a ``ManagedTable``) contributes auto-derived
    not_null/unique checks; ``expectations`` layers explicit checks on
    top using the probe's fluent ``Tester`` API. When ``freshness_sla``
    and ``freshness_column`` are both set, an at-run freshness assertion
    is appended.

    Returns ``None`` (and logs a warning) when the target connection kind
    is unsupported by the probe — the run proceeds without the check.
    """
    if target_cls is None and expectations is None and freshness_column is None:
        return None

    source = source_for_connection(target_connection)
    if source is None:
        logger.warning(
            "data-quality checks skipped for %r: target connection kind %r is "
            "not supported by ematix-probe (supported: postgres, duckdb)",
            pipeline_name,
            getattr(target_connection, "kind", "?"),
        )
        return None

    _, probe_from_table, DataProbe, _Tester = _require_probe()

    fresh_within: str | None = None
    if freshness_sla is not None and freshness_column is not None:
        secs = int(parse_duration(freshness_sla).total_seconds())
        fresh_within = f"{secs}s"

    def _extend(t: Any) -> None:
        if expectations is not None:
            expectations(t)
        if fresh_within is not None:
            t.freshness(freshness_column, within=fresh_within)

    if target_cls is not None:
        table_view = _TableView(target_cls)
        probe = probe_from_table(table_view, source=source, extend=_extend)
    else:
        # No ManagedTable (source_table / raw-SQL pipeline): build a bare
        # probe. We still need a table name for the probe to target.
        table_name = getattr(target_connection, "table", None) or pipeline_name

        def _build(t: Any) -> None:
            _extend(t)

        _build.__name__ = pipeline_name
        probe = DataProbe(_build, source=source, table=table_name, schema=None)

    report = probe.run()
    assertions = tuple(
        QualityAssertion(
            name=a.name or f"assertion[{a.assertion_index}]",
            verdict=a.verdict,
            message=a.message,
        )
        for a in report.assertions
    )
    return QualityOutcome(
        pipeline=pipeline_name,
        table=report.table,
        schema=report.schema,
        verdict=_reduce_verdict(assertions),
        assertions=assertions,
        duration_seconds=getattr(report, "duration_seconds", 0.0),
        started_at=getattr(report, "started_at", None),
        finished_at=getattr(report, "finished_at", None),
    )


# --------------------------------------------------------------------------
# Freshness
# --------------------------------------------------------------------------

# A pipeline is "warning" once it passes this fraction of its SLA window
# without a success, and "breached" past 100%.
_FRESHNESS_WARN_FRACTION = 0.8


def evaluate_freshness(
    *,
    pipeline: str,
    last_success: datetime | None,
    sla: str | int | float | timedelta,
    now: datetime | None = None,
) -> FreshnessState:
    """Pure freshness computation. ``breached`` when the pipeline has
    never succeeded or the lag exceeds the SLA; ``warning`` in the last
    20% of the window; else ``healthy``."""
    now = now or datetime.now(UTC)
    sla_delta = parse_duration(sla)
    sla_seconds = int(sla_delta.total_seconds())

    if last_success is None:
        return FreshnessState(
            pipeline=pipeline,
            sla_seconds=sla_seconds,
            lag_seconds=None,
            state="breached",
            last_success=None,
            evaluated_at=now,
        )

    if last_success.tzinfo is None:
        last_success = last_success.replace(tzinfo=UTC)
    lag_seconds = int((now - last_success).total_seconds())
    if lag_seconds > sla_seconds:
        state = "breached"
    elif lag_seconds >= sla_seconds * _FRESHNESS_WARN_FRACTION:
        state = "warning"
    else:
        state = "healthy"
    return FreshnessState(
        pipeline=pipeline,
        sla_seconds=sla_seconds,
        lag_seconds=lag_seconds,
        state=state,
        last_success=last_success,
        evaluated_at=now,
    )


@dataclass
class SlaSpec:
    """A pipeline's freshness SLA, registered at decoration time so the
    scheduled :class:`FreshnessEvaluator` knows what to watch."""

    pipeline: str
    sla_seconds: int


# Registry of freshness SLAs, populated by the decorator. Keyed by
# pipeline name. The scheduled evaluator reads this.
_SLA_REGISTRY: dict[str, SlaSpec] = {}


def register_freshness_sla(
    pipeline: str, sla: str | int | float | timedelta | None
) -> None:
    """Record (or clear) a pipeline's freshness SLA."""
    if sla is None:
        _SLA_REGISTRY.pop(pipeline, None)
        return
    _SLA_REGISTRY[pipeline] = SlaSpec(
        pipeline=pipeline, sla_seconds=int(parse_duration(sla).total_seconds())
    )


def registered_slas() -> dict[str, SlaSpec]:
    return dict(_SLA_REGISTRY)


class FreshnessEvaluator:
    """Evaluates freshness SLOs against a run-history store, on a
    schedule. This is the piece that catches a pipeline that has *stopped
    running* — a plain failure alert never fires because no run happens.

    ``last_success_fn(pipeline) -> datetime | None`` supplies the most
    recent successful completion; wire it to a
    :class:`~ematix_flow.run_log.history.RunHistoryStore`.

    Alerts fire only on the **healthy→breached edge** (de-duped via
    ``_last_state``) so a wall-clock breach doesn't page every tick.
    """

    def __init__(
        self,
        *,
        last_success_fn: Callable[[str], datetime | None],
        on_breach: Callable[[FreshnessState], None] | None = None,
        on_recover: Callable[[FreshnessState], None] | None = None,
        store_writer: Callable[[FreshnessState], None] | None = None,
    ):
        self._last_success_fn = last_success_fn
        self._on_breach = on_breach
        self._on_recover = on_recover
        self._store_writer = store_writer
        self._last_state: dict[str, str] = {}

    def prime_state(self, mapping: dict[str, str]) -> None:
        """Seed the per-pipeline last-known state (e.g. from the persisted
        ``freshness_state`` table) so edge de-dup survives across separate
        one-shot ``flow freshness-check`` invocations."""
        self._last_state.update(mapping)

    def evaluate_all(
        self, now: datetime | None = None
    ) -> list[FreshnessState]:
        """Evaluate every registered SLA. Returns the fresh states and
        fires breach/recover callbacks on state-edges."""
        now = now or datetime.now(UTC)
        states: list[FreshnessState] = []
        for spec in registered_slas().values():
            last = self._last_success_fn(spec.pipeline)
            state = evaluate_freshness(
                pipeline=spec.pipeline,
                last_success=last,
                sla=spec.sla_seconds,
                now=now,
            )
            states.append(state)
            if self._store_writer is not None:
                try:
                    self._store_writer(state)
                except Exception:  # pragma: no cover - best effort
                    logger.warning(
                        "freshness store write failed for %s", spec.pipeline
                    )
            self._fire_edges(state)
        return states

    def _fire_edges(self, state: FreshnessState) -> None:
        prev = self._last_state.get(state.pipeline)
        self._last_state[state.pipeline] = state.state
        if state.state == "breached" and prev != "breached":
            if self._on_breach is not None:
                self._on_breach(state)
        elif (
            state.state in ("healthy", "warning")
            and prev == "breached"
            and self._on_recover is not None
        ):
            self._on_recover(state)


# --------------------------------------------------------------------------
# Durable store (best-effort) + orchestration
# --------------------------------------------------------------------------

# Env var naming the shared analytics sqlite DB the web UI reads. When
# set, the quality stage records results there; when unset, results are
# still attached to the run result but not persisted for the UI.
_ANALYTICS_DB_ENV = "EMATIX_FLOW_ANALYTICS_DB"
# Kill switch — skip the whole quality stage regardless of kwargs.
_SKIP_ENV = "EMATIX_SKIP_QUALITY"


def _analytics_store_or_none():
    path = os.environ.get(_ANALYTICS_DB_ENV)
    if not path:
        return None
    try:
        from ematix_flow.web.analytics_store import AnalyticsStore

        return AnalyticsStore(path)
    except Exception:  # pragma: no cover - best effort
        logger.warning("could not open analytics store at %s", path)
        return None


def _record_quality(outcome: QualityOutcome, run_id: str | None) -> None:
    store = _analytics_store_or_none()
    if store is None:
        return
    try:
        store.record_quality_run(outcome, run_id=run_id)
    except Exception:  # pragma: no cover - best effort
        logger.warning("failed to persist quality result for %s", outcome.pipeline)
    finally:
        store.close()


def _fire_freshness_alert(alerters: Any, kind: str, state: FreshnessState) -> None:
    """Notify each alerter of a freshness state-edge."""
    if not alerters:
        return
    try:
        from ematix_flow.alerters import AlertEvent
    except Exception:  # pragma: no cover
        return
    if state.lag_seconds is None:
        detail = "no successful run on record"
    else:
        detail = (
            f"{state.lag_seconds}s since last success "
            f"(SLA {state.sla_seconds}s)"
        )
    event = AlertEvent(
        kind=kind,
        pipeline=state.pipeline,
        timestamp=state.evaluated_at or datetime.now(UTC),
        error_message=detail,
    )
    for alerter in alerters:
        try:
            alerter.notify(event)
        except Exception:  # pragma: no cover
            logger.warning("alerter %r failed on %s", alerter, kind)


def evaluate_freshness_once(
    *,
    last_success_fn: Callable[[str], datetime | None],
    analytics_store: Any | None = None,
    alerters: Any | None = None,
    now: datetime | None = None,
) -> list[FreshnessState]:
    """Evaluate every registered freshness SLA once.

    Persists each state to ``analytics_store`` (if given), seeds prior
    state from it so ``sla_breached`` fires only on the healthy→breached
    edge across separate invocations, and notifies ``alerters`` on the
    breach and recover edges. This is the body of ``flow freshness-check``
    and the per-tick scheduler hook.
    """
    prior: dict[str, str] = {}
    writer = None
    if analytics_store is not None:
        try:
            prior = {
                r["pipeline"]: r["state"] for r in analytics_store.list_freshness()
            }
        except Exception:  # pragma: no cover - best effort
            prior = {}
        writer = analytics_store.upsert_freshness_state

    evaluator = FreshnessEvaluator(
        last_success_fn=last_success_fn,
        on_breach=lambda s: _fire_freshness_alert(alerters, "sla_breached", s),
        on_recover=lambda s: _fire_freshness_alert(alerters, "recovered", s),
        store_writer=writer,
    )
    evaluator.prime_state(prior)
    return evaluator.evaluate_all(now=now)


def _emit_alert(kind: str, pipeline: str, message: str) -> None:
    """Fire a quality/SLA alert through the configured alerters, if any."""
    try:
        from ematix_flow.alerters import AlertEvent, active_alerters
    except Exception:  # pragma: no cover - alerters optional
        return
    alerters = active_alerters()
    if not alerters:
        return
    event = AlertEvent(
        kind=kind,
        pipeline=pipeline,
        timestamp=datetime.now(UTC),
        error_message=message,
    )
    for alerter in alerters:
        try:
            alerter.notify(event)
        except Exception:  # pragma: no cover - never let alerting break a run
            logger.warning("alerter %r failed on %s", alerter, kind)


def run_quality_stage(
    *,
    target_connection: Any,
    pipeline_name: str,
    target_cls: Any | None = None,
    expectations: Callable[[Any], None] | None = None,
    on_quality_failure: str = POLICY_WARN,
    freshness_column: str | None = None,
    freshness_sla: str | int | float | timedelta | None = None,
    sync_result: dict[str, Any] | None = None,
    alert_on_warn: bool = False,
) -> QualityOutcome | None:
    """Post-write quality stage invoked from the pipeline decorator.

    Runs expectations, attaches the summary to ``sync_result["quality"]``,
    persists it (best-effort), fires alerts, and — when the policy is
    ``"fail"`` and a real violation was found — raises
    :class:`QualityCheckFailed` so the run is recorded as failed.
    """
    if os.environ.get(_SKIP_ENV):
        logger.info("EMATIX_SKIP_QUALITY set — skipping quality stage")
        return None
    if on_quality_failure not in _VALID_POLICIES:
        raise ValueError(
            f"on_quality_failure must be one of {_VALID_POLICIES}, "
            f"got {on_quality_failure!r}"
        )

    outcome = run_expectations(
        target_connection=target_connection,
        pipeline_name=pipeline_name,
        target_cls=target_cls,
        expectations=expectations,
        freshness_column=freshness_column,
        freshness_sla=freshness_sla,
    )
    if outcome is None:
        return None

    if sync_result is not None:
        sync_result["quality"] = outcome.to_dict()

    run_id = sync_result.get("run_id") if sync_result else None
    _record_quality(outcome, run_id)

    if outcome.verdict == "fail":
        msg = (
            f"{outcome.checks_failed}/{outcome.checks_total} checks failed: "
            + "; ".join(
                f"{a.name}: {a.message}"
                for a in outcome.assertions
                if a.verdict == "fail"
            )
        )
        if on_quality_failure == POLICY_FAIL:
            # Raising records the run as failed; the orchestrator's own
            # "failed" alert covers notification — don't double-fire here.
            raise QualityCheckFailed(outcome)
        # policy == "warn": the run still succeeds, so nothing else will
        # notify — emit a quality_failed alert so it isn't silent.
        _emit_alert("quality_failed", pipeline_name, msg)
        logger.warning("data-quality WARN (policy=warn) for %s: %s", pipeline_name, msg)
    elif outcome.verdict == POLICY_WARN and outcome.checks_errored:
        logger.warning(
            "data-quality: %d check(s) could not be evaluated for %s",
            outcome.checks_errored,
            pipeline_name,
        )
        if alert_on_warn:
            _emit_alert(
                "quality_failed",
                pipeline_name,
                f"{outcome.checks_errored} check(s) could not be evaluated",
            )

    return outcome
