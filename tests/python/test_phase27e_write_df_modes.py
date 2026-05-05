"""Phase 27e enhancement: write_df inferred path beyond mode='append'.

Without a ManagedTable target, write_df now also supports:
  - mode='truncate' — TRUNCATE the table, then COPY in.
  - mode='merge' / 'scd1' — INSERT ... ON CONFLICT (keys) DO UPDATE SET ...
    (requires explicit keys=).
  - mode='scd2' — still requires target=ManagedTable (needs SCD2 metadata
    columns the inferred path can't synthesize).
"""

import pytest

from ematix_flow import _core

pytestmark = pytest.mark.integration


def _setup_pg(pg_url: str):
    conn = _core.connect(pg_url)
    conn.execute("DROP SCHEMA IF EXISTS warehouse CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS ematix_flow CASCADE")
    conn.execute("CREATE SCHEMA warehouse")
    return conn


# --- truncate -----------------------------------------------------------


def test_inferred_write_df_truncate_replaces_rows(pg_url):
    import polars as pl

    import ematix_flow.df  # noqa: F401

    conn = _setup_pg(pg_url)
    df_initial = pl.DataFrame({"id": [1, 2, 3], "label": ["a", "b", "c"]})
    conn.write_df(df_initial, "warehouse.adhoc", mode="append")
    assert conn.fetch_scalar_int("SELECT count(*)::int FROM warehouse.adhoc") == 3

    df_replace = pl.DataFrame({"id": [10, 20], "label": ["x", "y"]})
    conn.write_df(df_replace, "warehouse.adhoc", mode="truncate")
    assert conn.fetch_scalar_int("SELECT count(*)::int FROM warehouse.adhoc") == 2
    assert (
        conn.fetch_scalar_int(
            "SELECT count(*)::int FROM warehouse.adhoc WHERE id IN (1, 2, 3)"
        )
        == 0
    )


# --- merge / scd1 -------------------------------------------------------


def test_inferred_write_df_merge_upserts_with_explicit_keys(pg_url):
    import polars as pl

    import ematix_flow.df  # noqa: F401

    conn = _setup_pg(pg_url)
    conn.execute(
        "CREATE TABLE warehouse.users (id BIGINT PRIMARY KEY, name TEXT)"
    )
    conn.execute("INSERT INTO warehouse.users VALUES (1, 'old1'), (2, 'old2')")

    df = pl.DataFrame({"id": [2, 3], "name": ["new2", "new3"]})
    result = conn.write_df(
        df, "warehouse.users", mode="merge", keys=["id"]
    )
    assert result["path"] == "inferred_merge"
    # id=1 still 'old1', id=2 updated to 'new2', id=3 inserted.
    assert (
        conn.fetch_scalar_int("SELECT count(*)::int FROM warehouse.users") == 3
    )
    assert (
        conn.fetch_scalar_int(
            "SELECT count(*)::int FROM warehouse.users WHERE id = 2 AND name = 'new2'"
        )
        == 1
    )
    assert (
        conn.fetch_scalar_int(
            "SELECT count(*)::int FROM warehouse.users WHERE id = 1 AND name = 'old1'"
        )
        == 1
    )


def test_inferred_write_df_merge_requires_keys(pg_url):
    import polars as pl

    import ematix_flow.df  # noqa: F401

    conn = _setup_pg(pg_url)
    df = pl.DataFrame({"id": [1], "name": ["a"]})
    with pytest.raises(ValueError, match="keys"):
        conn.write_df(df, "warehouse.users", mode="merge")


def test_inferred_write_df_scd1_alias_works(pg_url):
    import polars as pl

    import ematix_flow.df  # noqa: F401

    conn = _setup_pg(pg_url)
    conn.execute(
        "CREATE TABLE warehouse.dim (id BIGINT PRIMARY KEY, attr TEXT)"
    )
    conn.execute("INSERT INTO warehouse.dim VALUES (1, 'old')")
    df = pl.DataFrame({"id": [1, 2], "attr": ["new", "fresh"]})
    conn.write_df(df, "warehouse.dim", mode="scd1", keys=["id"])
    assert (
        conn.fetch_scalar_int(
            "SELECT count(*)::int FROM warehouse.dim WHERE id = 1 AND attr = 'new'"
        )
        == 1
    )


def test_inferred_write_df_merge_unknown_key_raises(pg_url):
    import polars as pl

    import ematix_flow.df  # noqa: F401

    conn = _setup_pg(pg_url)
    df = pl.DataFrame({"id": [1], "name": ["a"]})
    with pytest.raises(ValueError, match="not_a_col"):
        conn.write_df(df, "warehouse.t", mode="merge", keys=["not_a_col"])


# --- scd2 (still needs target=) ----------------------------------------


def test_inferred_write_df_scd2_rejected(pg_url):
    import polars as pl

    import ematix_flow.df  # noqa: F401

    conn = _setup_pg(pg_url)
    df = pl.DataFrame({"id": [1], "name": ["a"]})
    with pytest.raises((NotImplementedError, ValueError), match="scd2"):
        conn.write_df(df, "warehouse.t", mode="scd2", keys=["id"])


# --- pandas parity check -----------------------------------------------


def test_inferred_write_df_truncate_pandas(pg_url):
    import pandas as pd

    import ematix_flow.df  # noqa: F401

    conn = _setup_pg(pg_url)
    conn.write_df(pd.DataFrame({"a": [1, 2]}), "warehouse.t", mode="append")
    conn.write_df(pd.DataFrame({"a": [99]}), "warehouse.t", mode="truncate")
    assert conn.fetch_scalar_int("SELECT count(*)::int FROM warehouse.t") == 1
    assert conn.fetch_scalar_int("SELECT a::int FROM warehouse.t") == 99
