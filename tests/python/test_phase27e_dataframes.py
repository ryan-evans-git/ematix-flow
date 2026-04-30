"""Phase 27e: DataFrame interop — Connection.read_df / write_df.

Importing `ematix_flow.df` monkey-patches `_core.Connection` with
`read_df(sql, *, prefer="auto")` and `write_df(df, qualified_name, *,
mode, target=None, keys=None, ...)`. Auto-detect rules:
  - read_df prefer="auto" → polars when both polars+pandas installed,
    else pandas. Forced choice raises ImportError if missing.
  - write_df dispatches on isinstance.
Transport: stage to a TEMP table via psycopg2 + COPY CSV, then route
through the strategy executor for full mode support.
"""

from typing import Annotated

import pytest

from ematix_flow import _core, ematix, pk
from ematix_flow.types import BigInt, Text

pytestmark = pytest.mark.integration


def _setup_pg(pg_url: str):
    conn = _core.connect(pg_url)
    conn.execute("DROP SCHEMA IF EXISTS warehouse CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS src CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS ematix_flow CASCADE")
    conn.execute("CREATE SCHEMA src")
    conn.execute("CREATE SCHEMA warehouse")
    conn.execute("CREATE TABLE src.users (id BIGINT PRIMARY KEY, name TEXT)")
    conn.execute("INSERT INTO src.users VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
    return conn


# --- read_df --------------------------------------------------------------


def test_read_df_auto_prefers_polars_when_available(pg_url):
    import ematix_flow.df  # noqa: F401 — installs monkey-patch

    conn = _setup_pg(pg_url)
    df = conn.read_df("SELECT id, name FROM src.users ORDER BY id")
    import polars as pl

    assert isinstance(df, pl.DataFrame)
    assert df.shape == (3, 2)
    assert df.columns == ["id", "name"]
    assert df["name"].to_list() == ["alice", "bob", "carol"]


def test_read_df_force_pandas(pg_url):
    import ematix_flow.df  # noqa: F401

    conn = _setup_pg(pg_url)
    df = conn.read_df(
        "SELECT id, name FROM src.users ORDER BY id", prefer="pandas"
    )
    import pandas as pd

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["id", "name"]
    assert df["name"].tolist() == ["alice", "bob", "carol"]


def test_read_df_force_polars(pg_url):
    import ematix_flow.df  # noqa: F401

    conn = _setup_pg(pg_url)
    df = conn.read_df(
        "SELECT id, name FROM src.users ORDER BY id", prefer="polars"
    )
    import polars as pl

    assert isinstance(df, pl.DataFrame)


def test_read_df_invalid_prefer_raises(pg_url):
    import ematix_flow.df  # noqa: F401

    conn = _setup_pg(pg_url)
    with pytest.raises(ValueError):
        conn.read_df("SELECT 1", prefer="numpy")


# --- write_df: ManagedTable path -----------------------------------------


def test_write_df_polars_with_managed_table_merge(pg_url):
    import ematix_flow.df  # noqa: F401
    import polars as pl

    conn = _setup_pg(pg_url)

    @ematix.table(schema="warehouse")
    class UserDim:
        id: Annotated[BigInt, pk()]
        name: Text

    df = pl.DataFrame({"id": [10, 20, 30], "name": ["x", "y", "z"]})
    result = conn.write_df(
        df, "warehouse.user_dim", mode="merge", target=UserDim
    )
    assert result["rows_inserted"] == 3
    assert (
        conn.fetch_scalar_int("SELECT count(*)::int FROM warehouse.user_dim") == 3
    )
    # Calling write_df again with overlapping IDs should upsert.
    df2 = pl.DataFrame({"id": [20, 40], "name": ["y2", "w"]})
    result2 = conn.write_df(
        df2, "warehouse.user_dim", mode="merge", target=UserDim
    )
    assert (
        conn.fetch_scalar_int("SELECT count(*)::int FROM warehouse.user_dim") == 4
    )
    assert (
        conn.fetch_scalar_int(
            "SELECT count(*)::int FROM warehouse.user_dim "
            "WHERE id = 20 AND name = 'y2'"
        )
        == 1
    )


def test_write_df_pandas_with_managed_table_append(pg_url):
    import ematix_flow.df  # noqa: F401
    import pandas as pd

    conn = _setup_pg(pg_url)

    @ematix.table(schema="warehouse")
    class UserDim:
        id: Annotated[BigInt, pk()]
        name: Text

    df = pd.DataFrame({"id": [100, 101], "name": ["pa", "pb"]})
    result = conn.write_df(
        df, "warehouse.user_dim", mode="append", target=UserDim
    )
    assert result["rows_inserted"] == 2


def test_write_df_managed_table_truncate_replaces_all(pg_url):
    import ematix_flow.df  # noqa: F401
    import polars as pl

    conn = _setup_pg(pg_url)

    @ematix.table(schema="warehouse")
    class UserDim:
        id: Annotated[BigInt, pk()]
        name: Text

    # Seed with a first write, then replace with truncate.
    conn.write_df(
        pl.DataFrame({"id": [999], "name": ["old"]}),
        "warehouse.user_dim",
        mode="append",
        target=UserDim,
    )
    df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
    conn.write_df(df, "warehouse.user_dim", mode="truncate", target=UserDim)
    assert (
        conn.fetch_scalar_int("SELECT count(*)::int FROM warehouse.user_dim") == 2
    )
    assert (
        conn.fetch_scalar_int("SELECT count(*)::int FROM warehouse.user_dim WHERE id = 999")
        == 0
    )


# --- write_df: inferred (no ManagedTable) path ---------------------------


def test_write_df_inferred_creates_table_from_polars(pg_url):
    import ematix_flow.df  # noqa: F401
    import polars as pl

    conn = _setup_pg(pg_url)

    df = pl.DataFrame({"id": [1, 2, 3], "label": ["a", "b", "c"]})
    conn.write_df(df, "warehouse.adhoc_polars", mode="append")
    rows = conn.fetch_scalar_int(
        "SELECT count(*)::int FROM warehouse.adhoc_polars"
    )
    assert rows == 3


def test_write_df_inferred_no_metadata_columns_added(pg_url):
    """Q8.2 γ: inferred path takes the df as-is — no _loaded_at / _batch_id."""
    import ematix_flow.df  # noqa: F401
    import pandas as pd

    conn = _setup_pg(pg_url)
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    conn.write_df(df, "warehouse.adhoc_pandas", mode="append")
    cols = conn.fetch_scalar_int(
        "SELECT count(*)::int FROM information_schema.columns "
        "WHERE table_schema = 'warehouse' AND table_name = 'adhoc_pandas' "
        "AND column_name IN ('_loaded_at', '_batch_id')"
    )
    assert cols == 0


# --- write_df: invalid input ---------------------------------------------


def test_write_df_unsupported_type_raises(pg_url):
    import ematix_flow.df  # noqa: F401

    conn = _setup_pg(pg_url)
    with pytest.raises(TypeError):
        conn.write_df(
            [{"id": 1}], "warehouse.x", mode="append"
        )
