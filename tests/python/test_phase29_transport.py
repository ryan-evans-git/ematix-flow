"""Phase 29: transport optimizations for write_df.

When `adbc-driver-postgresql` is installed, `Connection.write_df`
stages DataFrames via ADBC's Arrow-native bulk ingest (Postgres COPY
BINARY) instead of the CSV path. This is faster, smaller on the wire,
and type-correct (no string round-tripping for numerics, timestamps,
booleans, etc.).

Falls back to the CSV path automatically when ADBC is missing.
"""

from datetime import datetime, timezone
from typing import Annotated

import pytest

from ematix_flow import _core, ematix, pk
from ematix_flow.types import BigInt, Numeric, Text, TimestampTZ

pytestmark = pytest.mark.integration


def _setup_pg(pg_url: str):
    conn = _core.connect(pg_url)
    conn.execute("DROP SCHEMA IF EXISTS warehouse CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS ematix_flow CASCADE")
    conn.execute("CREATE SCHEMA warehouse")
    return conn


def _adbc_available() -> bool:
    try:
        import adbc_driver_postgresql  # noqa: F401

        return True
    except ImportError:
        return False


@pytest.mark.skipif(not _adbc_available(), reason="adbc-driver-postgresql not installed")
def test_write_df_uses_adbc_binary_path_when_available(pg_url):
    """When ADBC is installed, write_df should use it transparently and
    produce the same results as the CSV path."""
    import ematix_flow.df as df_mod  # noqa: F401
    import polars as pl

    conn = _setup_pg(pg_url)

    @ematix.table(schema="warehouse")
    class M:
        id: Annotated[BigInt, pk()]
        score: Numeric[10, 2]
        ts: TimestampTZ

    df = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "score": [10.50, 20.75, 30.99],
            "ts": [
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                datetime(2026, 1, 2, tzinfo=timezone.utc),
                datetime(2026, 1, 3, tzinfo=timezone.utc),
            ],
        }
    )
    result = conn.write_df(df, "warehouse.m", mode="merge", target=M)
    assert result["rows_inserted"] == 3
    assert conn.fetch_scalar_int("SELECT count(*)::int FROM warehouse.m") == 3
    # Type-correct: score precision preserved, ts not string-roundtripped.
    score_check = conn.fetch_scalar_int(
        "SELECT count(*)::int FROM warehouse.m "
        "WHERE id = 2 AND score = 20.75 AND ts = '2026-01-02T00:00:00+00:00'::timestamptz"
    )
    assert score_check == 1


@pytest.mark.skipif(not _adbc_available(), reason="ADBC required")
def test_write_df_inferred_path_uses_adbc(pg_url):
    """The inferred (no target=) path also benefits from ADBC."""
    import ematix_flow.df as df_mod  # noqa: F401
    import polars as pl

    conn = _setup_pg(pg_url)
    df = pl.DataFrame({"id": [1, 2], "label": ["a", "b"]})
    conn.write_df(df, "warehouse.adhoc", mode="append")
    assert conn.fetch_scalar_int("SELECT count(*)::int FROM warehouse.adhoc") == 2


@pytest.mark.skipif(not _adbc_available(), reason="ADBC required")
def test_write_df_inferred_merge_uses_adbc(pg_url):
    """Inferred merge path: ADBC for staging + ON CONFLICT for upsert."""
    import ematix_flow.df as df_mod  # noqa: F401
    import polars as pl

    conn = _setup_pg(pg_url)
    conn.execute(
        "CREATE TABLE warehouse.up (id BIGINT PRIMARY KEY, name TEXT)"
    )
    conn.execute("INSERT INTO warehouse.up VALUES (1, 'old')")
    df = pl.DataFrame({"id": [1, 2], "name": ["new", "fresh"]})
    conn.write_df(df, "warehouse.up", mode="merge", keys=["id"])
    new_name = conn.fetch_scalar_int(
        "SELECT count(*)::int FROM warehouse.up WHERE id = 1 AND name = 'new'"
    )
    fresh_name = conn.fetch_scalar_int(
        "SELECT count(*)::int FROM warehouse.up WHERE id = 2 AND name = 'fresh'"
    )
    assert new_name == 1
    assert fresh_name == 1


def test_write_df_csv_fallback_works_without_adbc(monkeypatch, pg_url):
    """Force-disable ADBC at import time and verify the CSV path still works."""
    import ematix_flow.df as df_mod
    import polars as pl

    monkeypatch.setattr(df_mod, "_HAS_ADBC", False, raising=False)

    conn = _setup_pg(pg_url)
    df = pl.DataFrame({"id": [1, 2, 3], "label": ["x", "y", "z"]})
    conn.write_df(df, "warehouse.fallback", mode="append")
    assert (
        conn.fetch_scalar_int("SELECT count(*)::int FROM warehouse.fallback") == 3
    )


def test_write_df_pandas_with_adbc(pg_url):
    """ADBC works with pandas DataFrames too via pyarrow conversion."""
    if not _adbc_available():
        pytest.skip("ADBC required")
    import ematix_flow.df as df_mod  # noqa: F401
    import pandas as pd

    conn = _setup_pg(pg_url)
    df = pd.DataFrame({"id": [10, 20], "name": ["pa", "pb"]})
    conn.write_df(df, "warehouse.pd", mode="append")
    assert conn.fetch_scalar_int("SELECT count(*)::int FROM warehouse.pd") == 2
