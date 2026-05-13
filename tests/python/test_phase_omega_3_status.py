"""Phase Ω.3 — operator-facing status view.

Tests `pipeline.status_snapshot()` + `pipeline.render_status_table()`,
the data + rendering primitives behind the `flow status` CLI.

Scope (in-process only — durable run-history is Ω.D1a):

  - Snapshot lists every registered pipeline with name, schedule,
    depends_on list, last-run timestamp + outcome, retry policy +
    current attempt state.
  - Pipelines that have never run show last_run=None.
  - Pipelines with a default RetryPolicy (max_attempts=1) show as
    "no retry"; richer policies show their parameters.
  - In-flight retry cycles surface attempt_count, next_eligible_at,
    and the gave_up flag.
  - Renderer produces one row per pipeline plus a header line; the
    text is fixed-width-aligned so a wide terminal renders cleanly.
"""

from __future__ import annotations

import datetime as _dt
import pytest

from ematix_flow import pipeline as p


_SIDE_TABLES = (
    "_REGISTRY",
    "_DEPENDS_ON",
    "_UPSTREAM_FRESHNESS",
    "_LAST_RUN",
    "_RETRY_POLICY",
    "_ATTEMPT_STATE",
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


# ---- snapshot ------------------------------------------------------


def test_snapshot_empty_when_no_pipelines():
    assert p.status_snapshot() == []


def test_snapshot_includes_every_registered_pipeline():
    @p.register(name="a", schedule="@hourly")
    def _a():
        return {}

    @p.register(name="b", schedule="@daily")
    def _b():
        return {}

    snap = p.status_snapshot()
    names = sorted(row["name"] for row in snap)
    assert names == ["a", "b"]


def test_snapshot_for_pipeline_with_no_runs():
    @p.register(name="fresh", schedule="@hourly")
    def _fresh():
        return {}

    [row] = p.status_snapshot()
    assert row["name"] == "fresh"
    assert row["schedule"] == "@hourly"
    assert row["depends_on"] == []
    assert row["last_run"] is None
    # Default retry policy = no retries.
    assert row["retry_policy"]["max_attempts"] == 1
    assert row["attempt_state"] is None


def test_snapshot_records_depends_on():
    @p.register(name="root", schedule="@hourly")
    def _root():
        return {}

    @p.register(name="leaf", schedule="@hourly", depends_on=["root"])
    def _leaf():
        return {}

    by_name = {r["name"]: r for r in p.status_snapshot()}
    assert by_name["leaf"]["depends_on"] == ["root"]
    assert by_name["root"]["depends_on"] == []


def test_snapshot_records_last_run_after_invocation():
    @p.register(name="ok", schedule="@hourly")
    def _ok():
        return {}

    now = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.timezone.utc)
    p.run_due_with_dag(["ok"], now=now)
    [row] = p.status_snapshot()
    assert row["last_run"] is not None
    ts, success = row["last_run"]
    assert success is True
    assert isinstance(ts, _dt.datetime)


def test_snapshot_surfaces_in_flight_retry_cycle():
    @p.register(
        name="flaky",
        schedule="@hourly",
        retry={"max_attempts": 3, "backoff": "fixed", "base_secs": 30},
    )
    def _flaky():
        raise RuntimeError("boom")

    t = _dt.datetime(2026, 5, 13, 12, 0, 0, tzinfo=_dt.timezone.utc)
    p.run_due_with_dag(["flaky"], now=t)

    [row] = p.status_snapshot()
    assert row["retry_policy"]["max_attempts"] == 3
    assert row["retry_policy"]["backoff"] == "fixed"
    st = row["attempt_state"]
    assert st is not None
    assert st["attempt_count"] == 1
    assert st["gave_up"] is False
    # next_eligible_at = last_attempt_at + 30s
    assert st["next_eligible_at"] == t + _dt.timedelta(seconds=30)


def test_snapshot_marks_gave_up():
    @p.register(
        name="dies",
        schedule="@hourly",
        retry={"max_attempts": 1, "backoff": "fixed", "base_secs": 0},
    )
    def _dies():
        raise RuntimeError("boom")

    p.run_due_with_dag(["dies"])
    [row] = p.status_snapshot()
    assert row["attempt_state"]["gave_up"] is True


# ---- rendering -----------------------------------------------------


def test_render_empty():
    txt = p.render_status_table([])
    # Should at least produce a header and a "no pipelines" hint.
    assert "no pipelines" in txt.lower() or "pipeline" in txt.lower()


def test_render_includes_pipeline_names_and_schedule():
    @p.register(name="alpha", schedule="@hourly")
    def _a():
        return {}

    @p.register(name="beta", schedule="@daily")
    def _b():
        return {}

    snap = p.status_snapshot()
    txt = p.render_status_table(snap)
    assert "alpha" in txt
    assert "beta" in txt
    assert "@hourly" in txt
    assert "@daily" in txt


def test_render_includes_retry_summary_when_in_flight():
    @p.register(
        name="r",
        schedule="@hourly",
        retry={"max_attempts": 5, "backoff": "exponential", "base_secs": 1},
    )
    def _r():
        raise RuntimeError("boom")

    p.run_due_with_dag(["r"])
    txt = p.render_status_table(p.status_snapshot())
    # Surface the attempt count and the max so an operator immediately
    # sees how many tries are left.
    assert "1/5" in txt or "1 / 5" in txt


def test_render_marks_gave_up_pipelines():
    @p.register(
        name="dead",
        schedule="@hourly",
        retry={"max_attempts": 1, "backoff": "fixed", "base_secs": 0},
    )
    def _d():
        raise RuntimeError("boom")

    p.run_due_with_dag(["dead"])
    txt = p.render_status_table(p.status_snapshot())
    assert "gave up" in txt.lower() or "gave_up" in txt.lower()


def test_render_marks_never_run():
    @p.register(name="virgin", schedule="@hourly")
    def _v():
        return {}

    txt = p.render_status_table(p.status_snapshot())
    # Something should signal "no last run" — either a literal "never",
    # a dash, or an explicit "never run" phrase.
    assert "never" in txt.lower() or "—" in txt or "-" in txt
