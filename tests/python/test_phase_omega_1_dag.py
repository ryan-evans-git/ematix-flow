"""Phase Ω.1 — pipeline dependencies (DAG).

Tests `depends_on=` argument on the registration decorator + the
`run_due` topo-ordering it enables. Covers:

  - `depends_on=` annotates the registry entry with upstream names.
  - Registration rejects cycles with a clear error message naming
    both endpoints.
  - Registration rejects unknown upstream names with a clear error.
  - `run_due_with_dag` runs upstreams strictly before downstreams.
  - Downstream skips when its upstream's most-recent run failed.
  - Downstream skips when its upstream's most-recent run is stale
    (older than `upstream_freshness`).
  - `upstream_freshness=None` (default) treats any successful run as
    fresh.

In-process scope only. Cross-process (durable) run-history is Ω.D1a.
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
    # Ω.2 retry tables — must also clear so a prior test's failure
    # doesn't leave `gave_up=True` state behind on a same-named pipeline.
    "_RETRY_POLICY",
    "_ATTEMPT_STATE",
)


@pytest.fixture(autouse=True)
def _clean_registry():
    """Reset the process-global pipeline registry before each test."""
    for tbl in _SIDE_TABLES:
        d = getattr(p, tbl, None)
        if d is not None:
            d.clear()
    yield
    for tbl in _SIDE_TABLES:
        d = getattr(p, tbl, None)
        if d is not None:
            d.clear()


# ---- Registration --------------------------------------------------


def test_depends_on_annotates_registry():
    @p.register(name="root", schedule="@hourly")
    def _root():
        return {"status": "ok"}

    @p.register(name="leaf", schedule="@hourly", depends_on=["root"])
    def _leaf():
        return {"status": "ok"}

    assert p.depends_on_of("leaf") == ["root"]
    assert p.depends_on_of("root") == []


def test_depends_on_with_freshness_seconds():
    @p.register(name="root", schedule="@hourly")
    def _root():
        return {"status": "ok"}

    @p.register(
        name="leaf",
        schedule="@hourly",
        depends_on=["root"],
        upstream_freshness_secs=300,  # upstream must have succeeded < 5min ago
    )
    def _leaf():
        return {"status": "ok"}

    assert p.upstream_freshness_secs_of("leaf") == 300


def test_unknown_upstream_rejected_with_clear_error():
    with pytest.raises(ValueError) as ei:
        @p.register(name="leaf", schedule="@hourly", depends_on=["does_not_exist"])
        def _leaf():
            return {}

    msg = str(ei.value)
    assert "does_not_exist" in msg
    assert "leaf" in msg


def test_self_dependency_rejected():
    with pytest.raises(ValueError) as ei:
        @p.register(name="alpha", schedule="@hourly", depends_on=["alpha"])
        def _alpha():
            return {}

    msg = str(ei.value)
    # Self-loop is a cycle of length 1; error should name the offender.
    assert "alpha" in msg
    assert "cycle" in msg.lower() or "self" in msg.lower()


def test_two_node_cycle_rejected():
    @p.register(name="a", schedule="@hourly")
    def _a():
        return {}

    # Now try to register `b` depending on `a`, then mutate `a` to
    # depend on `b`. We achieve this by manually re-registering `a`.
    @p.register(name="b", schedule="@hourly", depends_on=["a"])
    def _b():
        return {}

    # Re-register `a` with a back-edge — should be rejected.
    p._REGISTRY.pop("a")
    p._DEPENDS_ON.pop("a", None)
    with pytest.raises(ValueError) as ei:
        @p.register(name="a", schedule="@hourly", depends_on=["b"])
        def _a_v2():
            return {}

    msg = str(ei.value)
    assert "a" in msg and "b" in msg
    assert "cycle" in msg.lower()


def test_three_node_cycle_rejected():
    @p.register(name="x", schedule="@hourly")
    def _x():
        return {}

    @p.register(name="y", schedule="@hourly", depends_on=["x"])
    def _y():
        return {}

    @p.register(name="z", schedule="@hourly", depends_on=["y"])
    def _z():
        return {}

    # Now try x → z (closing x→y→z→x). Re-register x.
    p._REGISTRY.pop("x")
    p._DEPENDS_ON.pop("x", None)
    with pytest.raises(ValueError) as ei:
        @p.register(name="x", schedule="@hourly", depends_on=["z"])
        def _x_v2():
            return {}

    assert "cycle" in str(ei.value).lower()


# ---- Topo order + freshness gating --------------------------------


def test_topo_order_runs_upstream_first():
    """`order_for_run_due(names)` returns names in topo order."""
    @p.register(name="raw", schedule="@hourly")
    def _raw():
        return {}

    @p.register(name="cleaned", schedule="@hourly", depends_on=["raw"])
    def _cleaned():
        return {}

    @p.register(name="agg", schedule="@hourly", depends_on=["cleaned"])
    def _agg():
        return {}

    # Caller passes them in arbitrary order; topo sort restores
    # raw → cleaned → agg.
    ordered = p.order_for_run_due(["agg", "raw", "cleaned"])
    assert ordered == ["raw", "cleaned", "agg"]


def test_run_due_runs_upstream_before_downstream():
    """End-to-end: a downstream pipeline only fires after its
    upstream's just-completed run succeeded (within this process)."""
    calls: list[str] = []

    @p.register(name="root", schedule="@hourly")
    def _root():
        calls.append("root")
        return {"status": "ok"}

    @p.register(name="leaf", schedule="@hourly", depends_on=["root"])
    def _leaf():
        calls.append("leaf")
        return {"status": "ok"}

    fired = p.run_due_with_dag(["leaf", "root"])
    assert calls == ["root", "leaf"]
    assert fired == ["root", "leaf"]


def test_downstream_skipped_when_upstream_failed():
    calls: list[str] = []

    @p.register(name="root", schedule="@hourly")
    def _root():
        calls.append("root")
        raise RuntimeError("boom")

    @p.register(name="leaf", schedule="@hourly", depends_on=["root"])
    def _leaf():
        calls.append("leaf")
        return {}

    fired = p.run_due_with_dag(["leaf", "root"])
    # Root attempted, raised; leaf must not have been invoked.
    # `fired` includes only successful invocations.
    assert "root" in calls
    assert "leaf" not in calls
    assert "root" not in fired
    assert "leaf" not in fired


def test_downstream_runs_when_upstream_freshness_inf():
    """Default upstream_freshness=None means any prior successful
    run counts as fresh. If the upstream isn't in the `due` set but
    had a successful prior run earlier in the process, downstream
    can still run."""
    calls: list[str] = []

    @p.register(name="root", schedule="@hourly")
    def _root():
        calls.append("root")
        return {}

    @p.register(name="leaf", schedule="@hourly", depends_on=["root"])
    def _leaf():
        calls.append("leaf")
        return {}

    # Fire root first (e.g. earlier tick).
    p.run_due_with_dag(["root"])
    assert calls == ["root"]

    # Now only leaf is due; root's last run was successful, no
    # freshness window → leaf eligible.
    fired = p.run_due_with_dag(["leaf"])
    assert calls == ["root", "leaf"]
    assert fired == ["leaf"]


def test_downstream_skipped_when_upstream_stale():
    """When `upstream_freshness_secs=N` is declared, downstream
    only runs if upstream's last success was within N seconds."""
    calls: list[str] = []

    @p.register(name="root", schedule="@hourly")
    def _root():
        calls.append("root")
        return {}

    @p.register(
        name="leaf",
        schedule="@hourly",
        depends_on=["root"],
        upstream_freshness_secs=1,
    )
    def _leaf():
        calls.append("leaf")
        return {}

    # Set root's last-success to 10 seconds ago (stale).
    p._LAST_RUN["root"] = (
        _dt.datetime.now(_dt.UTC) - _dt.timedelta(seconds=10),
        True,
    )

    fired = p.run_due_with_dag(["leaf"])
    assert "leaf" not in calls
    assert fired == []


def test_no_dependencies_runs_independently():
    """Pipelines without `depends_on=` work exactly as before."""
    calls: list[str] = []

    @p.register(name="solo", schedule="@hourly")
    def _solo():
        calls.append("solo")
        return {}

    fired = p.run_due_with_dag(["solo"])
    assert calls == ["solo"]
    assert fired == ["solo"]
