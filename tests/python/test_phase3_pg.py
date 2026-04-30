"""Phase 3: Postgres adapter — connect, ping, execute, transactions.

Marked `integration`; opt in with `pytest -m integration`. Requires Docker.
"""

from __future__ import annotations

import pytest

from ematix_flow import _core

pytestmark = pytest.mark.integration


def test_same_database_unit_does_not_need_db() -> None:
    # Sanity: the pure-logic helper is exposed and round-trips with
    # the Rust unit tests.
    assert _core.same_database("postgres://u@h/d", "postgres://u@h:5432/d") is True
    assert _core.same_database("postgres://u@h/d", "postgres://u@h/other") is False


def test_connect_returns_connection(pg_url: str) -> None:
    conn = _core.connect(pg_url)
    assert conn is not None


def test_ping_returns_one(pg_url: str) -> None:
    conn = _core.connect(pg_url)
    assert conn.ping() == 1


def test_execute_runs_ddl_and_dml(pg_url: str) -> None:
    conn = _core.connect(pg_url)
    conn.execute("DROP TABLE IF EXISTS phase3_smoke")
    conn.execute("CREATE TABLE phase3_smoke (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
    rows = conn.execute("INSERT INTO phase3_smoke (id, name) VALUES (1, 'a'), (2, 'b')")
    assert rows == 2
    count = conn.fetch_scalar_int("SELECT count(*)::int FROM phase3_smoke")
    assert count == 2


def test_transaction_commits_all(pg_url: str) -> None:
    conn = _core.connect(pg_url)
    conn.execute("DROP TABLE IF EXISTS phase3_tx_commit")
    conn.execute("CREATE TABLE phase3_tx_commit (id INTEGER PRIMARY KEY)")
    conn.execute_in_transaction(
        [
            "INSERT INTO phase3_tx_commit VALUES (1)",
            "INSERT INTO phase3_tx_commit VALUES (2)",
        ]
    )
    assert conn.fetch_scalar_int("SELECT count(*)::int FROM phase3_tx_commit") == 2


def test_transaction_rolls_back_on_error(pg_url: str) -> None:
    conn = _core.connect(pg_url)
    conn.execute("DROP TABLE IF EXISTS phase3_tx_rollback")
    conn.execute("CREATE TABLE phase3_tx_rollback (id INTEGER PRIMARY KEY)")
    with pytest.raises(ValueError):
        conn.execute_in_transaction(
            [
                "INSERT INTO phase3_tx_rollback VALUES (1)",
                "INSERT INTO phase3_tx_rollback VALUES (1)",  # PK violation
            ]
        )
    assert conn.fetch_scalar_int("SELECT count(*)::int FROM phase3_tx_rollback") == 0


def test_invalid_url_raises_value_error() -> None:
    with pytest.raises(ValueError):
        _core.connect("postgres://nobody@127.0.0.1:1/nope")


def test_malformed_url_raises_value_error() -> None:
    with pytest.raises(ValueError):
        _core.connect("not a url at all")
