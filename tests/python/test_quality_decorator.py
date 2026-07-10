"""Decorator-integration tests: `@ematix.pipeline(expectations=, ...)`
runs the quality stage after the write, threads the policy, and raises
on fail. The engine write (`pipeline.sync`) and connection resolution
are stubbed so the test needs no live database, but the quality stage
itself runs for real against a DuckDB file."""

from __future__ import annotations

import pytest

pytest.importorskip("ematix_probe")
duckdb = pytest.importorskip("duckdb")

from ematix_flow import decorators as _dec  # noqa: E402
from ematix_flow import ematix  # noqa: E402
from ematix_flow import pipeline as _p  # noqa: E402
from ematix_flow import quality as _q  # noqa: E402
from ematix_flow.connections import DuckDBConnection  # noqa: E402
from ematix_flow.table import ManagedTable  # noqa: E402
from ematix_flow.types import BigInt, Column, String  # noqa: E402


class Customers(ManagedTable):
    __schema__ = "main"
    __tablename__ = "customers"

    id = Column(BigInt(), nullable=False, primary_key=True)
    email = Column(String(256), nullable=False)


@pytest.fixture()
def wired(tmp_path, monkeypatch):
    """DuckDB target + stubbed write/connection resolution."""
    db = str(tmp_path / "wh.duckdb")

    def _make(rows):
        con = duckdb.connect(db)
        con.execute("CREATE TABLE customers(id BIGINT, email VARCHAR)")
        con.executemany("INSERT INTO customers VALUES (?, ?)", rows)
        con.close()

    conn = DuckDBConnection(name="wh", path=db)
    monkeypatch.setattr(_dec, "_resolve_named_connection", lambda _name: conn)
    monkeypatch.setattr(
        _p, "sync", lambda **kw: {"run_id": "r1", "rows_inserted": 1}
    )
    _p._REGISTRY.clear()
    return _make


def test_decorator_pass_attaches_quality(wired):
    wired([(1, "a@b.com"), (2, "c@d.com")])

    @ematix.pipeline(
        target=Customers,
        mode="merge",
        expectations=lambda t: t.row_count(at_least=1),
    )
    def load(c):
        return "SELECT id, email FROM src"

    result = load()
    assert result["quality"]["verdict"] == "pass"
    assert result["quality"]["checks_total"] >= 3


def test_decorator_fail_policy_raises(wired):
    wired([(1, None)])  # null email violates not_null

    @ematix.pipeline(
        target=Customers,
        mode="merge",
        on_quality_failure="fail",
        expectations=lambda t: t.row_count(at_least=1),
    )
    def load(c):
        return "SELECT id, email FROM src"

    with pytest.raises(_q.QualityCheckFailed):
        load()


def test_decorator_warn_policy_survives(wired):
    wired([(1, None)])

    @ematix.pipeline(
        target=Customers,
        mode="merge",
        on_quality_failure="warn",
        expectations=lambda t: t.row_count(at_least=1),
    )
    def load(c):
        return "SELECT id, email FROM src"

    result = load()  # does not raise
    assert result["quality"]["verdict"] == "fail"


def test_decorator_no_kwargs_skips_stage(wired, monkeypatch):
    wired([(1, "a@b.com")])
    called = {"n": 0}
    real = _q.run_quality_stage

    def _spy(**kw):
        called["n"] += 1
        return real(**kw)

    monkeypatch.setattr(_q, "run_quality_stage", _spy)

    @ematix.pipeline(target=Customers, mode="merge")
    def load(c):
        return "SELECT id, email FROM src"

    load()
    assert called["n"] == 0  # stage not invoked without expectations/freshness


def test_decorator_registers_freshness_sla(wired):
    @ematix.pipeline(
        target=Customers,
        mode="merge",
        freshness_sla="6h",
        name="fresh_pipe",
    )
    def load(c):
        return "SELECT id, email FROM src"

    try:
        assert "fresh_pipe" in _q.registered_slas()
        assert _q.registered_slas()["fresh_pipe"].sla_seconds == 21600
    finally:
        _q.register_freshness_sla("fresh_pipe", None)
