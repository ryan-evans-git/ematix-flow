"""Integration tests for the expectations engine against a real DuckDB
target. Exercises run_expectations + run_quality_stage (warn vs fail),
auto-derivation from a ManagedTable, and the unsupported-connection skip.

Requires the ematix-probe extra + duckdb."""

from __future__ import annotations

import pytest

pytest.importorskip("ematix_probe")
duckdb = pytest.importorskip("duckdb")

from ematix_flow import quality as q  # noqa: E402
from ematix_flow.connections import (  # noqa: E402
    DuckDBConnection,
    SQLiteConnection,
)
from ematix_flow.table import ManagedTable  # noqa: E402
from ematix_flow.types import BigInt, Column, String  # noqa: E402


class Customers(ManagedTable):
    __schema__ = "main"
    __tablename__ = "customers"

    id = Column(BigInt(), nullable=False, primary_key=True)
    email = Column(String(256), nullable=False)


def _duckdb_with(rows: list[tuple], path: str) -> None:
    con = duckdb.connect(path)
    con.execute("CREATE TABLE customers(id BIGINT, email VARCHAR)")
    con.executemany("INSERT INTO customers VALUES (?, ?)", rows)
    con.close()


@pytest.fixture()
def clean_db(tmp_path):
    return str(tmp_path / "wh.duckdb")


def test_expectations_pass(clean_db):
    _duckdb_with([(1, "a@b.com"), (2, "c@d.com")], clean_db)
    conn = DuckDBConnection(name="wh", path=clean_db)
    out = q.run_expectations(
        target_connection=conn,
        pipeline_name="load_customers",
        target_cls=Customers,
        expectations=lambda t: t.row_count(at_least=1),
    )
    assert out is not None
    assert out.verdict == "pass"
    assert out.checks_total >= 3  # id.not_null, id.unique, email.not_null, row_count


def test_expectations_detect_null_violation(clean_db):
    _duckdb_with([(1, "a@b.com"), (2, None)], clean_db)
    conn = DuckDBConnection(name="wh", path=clean_db)
    out = q.run_expectations(
        target_connection=conn,
        pipeline_name="load_customers",
        target_cls=Customers,
    )
    assert out.verdict == "fail"
    assert out.checks_failed >= 1
    assert any(a.name.startswith("email") and a.verdict == "fail" for a in out.assertions)


def test_stage_warn_policy_does_not_raise(clean_db):
    _duckdb_with([(1, None)], clean_db)
    conn = DuckDBConnection(name="wh", path=clean_db)
    sync_result: dict = {"run_id": "r1"}
    # policy="warn": returns the outcome, attaches to result, no raise.
    out = q.run_quality_stage(
        target_connection=conn,
        pipeline_name="load_customers",
        target_cls=Customers,
        on_quality_failure="warn",
        sync_result=sync_result,
    )
    assert out.verdict == "fail"
    assert sync_result["quality"]["verdict"] == "fail"


def test_stage_fail_policy_raises(clean_db):
    _duckdb_with([(1, None)], clean_db)
    conn = DuckDBConnection(name="wh", path=clean_db)
    with pytest.raises(q.QualityCheckFailed) as ei:
        q.run_quality_stage(
            target_connection=conn,
            pipeline_name="load_customers",
            target_cls=Customers,
            on_quality_failure="fail",
        )
    assert ei.value.outcome.verdict == "fail"


def test_stage_invalid_policy_rejected(clean_db):
    _duckdb_with([(1, "a@b.com")], clean_db)
    conn = DuckDBConnection(name="wh", path=clean_db)
    with pytest.raises(ValueError):
        q.run_quality_stage(
            target_connection=conn,
            pipeline_name="p",
            target_cls=Customers,
            on_quality_failure="explode",
        )


def test_unsupported_connection_skips(clean_db):
    # SQLite has no ematix-probe source → skip (returns None), no crash.
    conn = SQLiteConnection(name="lite", path=clean_db)
    out = q.run_expectations(
        target_connection=conn,
        pipeline_name="p",
        target_cls=Customers,
    )
    assert out is None


def test_kill_switch_skips_stage(clean_db, monkeypatch):
    _duckdb_with([(1, None)], clean_db)
    conn = DuckDBConnection(name="wh", path=clean_db)
    monkeypatch.setenv("EMATIX_SKIP_QUALITY", "1")
    out = q.run_quality_stage(
        target_connection=conn,
        pipeline_name="p",
        target_cls=Customers,
        on_quality_failure="fail",  # would otherwise raise
    )
    assert out is None


class Events(ManagedTable):
    __schema__ = "main"
    __tablename__ = "events"

    id = Column(BigInt())  # nullable — isolate the freshness assertion
    updated_at = Column(String(64))


def test_freshness_column_check_passes_when_fresh(clean_db):
    con = duckdb.connect(clean_db)
    con.execute("CREATE TABLE events(id BIGINT, updated_at TIMESTAMP)")
    con.execute("INSERT INTO events VALUES (1, now())")
    con.close()
    conn = DuckDBConnection(name="wh", path=clean_db)
    out = q.run_expectations(
        target_connection=conn,
        pipeline_name="load_events",
        target_cls=Events,
        freshness_column="updated_at",
        freshness_sla="24h",
    )
    assert out is not None
    assert any("freshness" in a.name for a in out.assertions)
    assert out.verdict == "pass"
