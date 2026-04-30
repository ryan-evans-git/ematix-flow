"""Phase 28: Spark DataFrame interop (ematix-flow[spark]).

Importing `ematix_flow.spark` monkey-patches `_core.Connection` with:

  conn.read_spark_df(spark_session, sql) -> pyspark.sql.DataFrame
  conn.write_spark_df(df, qualified_name, *, mode, target=, keys=, ...)

Unit tests below pin URL construction, dispatch, and error paths
without requiring a JVM. The `@pytest.mark.spark` integration test
spins up a local SparkSession and validates end-to-end — opt-in via
`pytest -m spark`, requires `pip install ematix-flow[spark]` plus
ability to download the Postgres JDBC jar at runtime.
"""

from __future__ import annotations

import pytest

import ematix_flow.spark as spark_mod


# --- DSN → JDBC URL conversion -------------------------------------------


def test_postgres_dsn_to_jdbc_url_basic():
    jdbc, props = spark_mod._dsn_to_jdbc(
        "postgres://alice:secret@localhost:5432/mydb"
    )
    assert jdbc == "jdbc:postgresql://localhost:5432/mydb"
    assert props["user"] == "alice"
    assert props["password"] == "secret"


def test_postgres_dsn_to_jdbc_url_no_port():
    jdbc, props = spark_mod._dsn_to_jdbc(
        "postgres://alice:secret@db.local/mydb"
    )
    # Default Postgres port.
    assert jdbc == "jdbc:postgresql://db.local:5432/mydb"


def test_postgres_dsn_to_jdbc_url_passes_extra_query_params():
    jdbc, props = spark_mod._dsn_to_jdbc(
        "postgres://u:p@h:5432/d?sslmode=require&application_name=ematix"
    )
    assert "sslmode" in props
    assert props["sslmode"] == "require"
    assert props.get("application_name") == "ematix"


def test_dsn_to_jdbc_rejects_non_postgres_scheme():
    with pytest.raises(ValueError):
        spark_mod._dsn_to_jdbc("mysql://u:p@h/d")


def test_dsn_to_jdbc_requires_host_user_dbname():
    with pytest.raises(ValueError):
        spark_mod._dsn_to_jdbc("postgres:///mydb")


# --- import-time monkey-patch --------------------------------------------


def test_import_attaches_methods_to_connection():
    from ematix_flow import _core

    assert hasattr(_core.Connection, "read_spark_df")
    assert hasattr(_core.Connection, "write_spark_df")


# --- error path when pyspark missing -------------------------------------


def test_read_spark_df_without_pyspark_raises_clear_error(monkeypatch):
    """Even when pyspark is installed, calling with a non-SparkSession
    object should raise a clear TypeError (not a generic AttributeError).
    """
    from ematix_flow import _core

    conn = _core.connect.__call__  # placeholder — we don't actually connect
    # We can't easily build a Connection without a DB, so just verify
    # the function rejects an obviously-wrong session arg.
    fn = spark_mod._read_spark_df_impl

    class _FakeConn:
        def dsn(self):
            return "postgres://u:p@h:5432/d"

    with pytest.raises(TypeError, match="SparkSession"):
        fn(_FakeConn(), "not-a-spark-session", "SELECT 1")


def test_write_spark_df_without_pyspark_dispatches_on_type():
    """write_spark_df should raise TypeError on a non-Spark DataFrame."""
    fn = spark_mod._write_spark_df_impl

    class _FakeConn:
        def dsn(self):
            return "postgres://u:p@h:5432/d"

    with pytest.raises(TypeError, match="Spark DataFrame"):
        fn(_FakeConn(), "not a df", "schema.table", mode="append")


# --- live Spark integration (opt-in, slow) -------------------------------


@pytest.mark.spark
def test_read_spark_df_end_to_end(pg_url):
    """Requires `pip install pyspark` and downloads the Postgres JDBC jar."""
    from ematix_flow import _core

    conn = _core.connect(pg_url)
    conn.execute("DROP SCHEMA IF EXISTS src CASCADE")
    conn.execute("CREATE SCHEMA src")
    conn.execute("CREATE TABLE src.t (id BIGINT, name TEXT)")
    conn.execute("INSERT INTO src.t VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    pyspark = pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("ematix-flow-test")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
        .getOrCreate()
    )
    try:
        df = conn.read_spark_df(spark, "SELECT id, name FROM src.t ORDER BY id")
        rows = df.collect()
        assert [r.id for r in rows] == [1, 2, 3]
        assert [r.name for r in rows] == ["a", "b", "c"]
    finally:
        spark.stop()
