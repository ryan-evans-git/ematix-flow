"""Phase 28: Spark DataFrame interop (ematix-flow[spark]).

Importing `ematix_flow.spark` monkey-patches `_core.Connection` with:

  conn.read_spark_df(spark_session, sql) -> pyspark.sql.DataFrame
  conn.write_spark_df(df, qualified_name, *, mode, target=, keys=, ...)

Unit tests below pin URL construction, dispatch, and error paths
without requiring a JVM. `@pytest.mark.spark` integration tests spin
up a local SparkSession and validate end-to-end against a real
Postgres container.

To run the live tests:

    pip install pyspark
    # macOS with Homebrew JDK 21+:
    JAVA_HOME=/opt/homebrew/opt/openjdk \\
    PATH="/opt/homebrew/opt/openjdk/bin:$PATH" \\
    _JAVA_OPTIONS="-Djava.security.manager=allow" \\
    pytest -m spark

The `_JAVA_OPTIONS` flag is needed because newer JDKs (21+) ship with
SecurityManager removed; Spark's Hadoop layer still queries it.
"""

from typing import Annotated

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


def _spark_session():
    """Build a local SparkSession with the Postgres JDBC jar."""
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("ematix-flow-test")
        .master("local[1]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def _seed_and_setup(pg_url):
    from ematix_flow import _core

    conn = _core.connect(pg_url)
    conn.execute("DROP SCHEMA IF EXISTS src CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS warehouse CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS ematix_flow CASCADE")
    conn.execute("CREATE SCHEMA src")
    conn.execute("CREATE SCHEMA warehouse")
    conn.execute("CREATE TABLE src.t (id BIGINT, name TEXT)")
    conn.execute("INSERT INTO src.t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    return conn


@pytest.mark.spark
def test_read_spark_df_end_to_end(pg_url):
    """Requires `pip install pyspark` and downloads the Postgres JDBC jar."""
    conn = _seed_and_setup(pg_url)
    spark = _spark_session()
    try:
        df = conn.read_spark_df(spark, "SELECT id, name FROM src.t ORDER BY id")
        rows = df.collect()
        assert [r.id for r in rows] == [1, 2, 3]
        assert [r.name for r in rows] == ["a", "b", "c"]
    finally:
        spark.stop()


@pytest.mark.spark
def test_read_spark_df_arbitrary_subquery(pg_url):
    """A SQL fragment with WHERE / aggregations works because the framework
    wraps it in `(<sql>) AS subq` for Spark JDBC."""
    conn = _seed_and_setup(pg_url)
    spark = _spark_session()
    try:
        df = conn.read_spark_df(
            spark,
            "SELECT id FROM src.t WHERE id > 1 ORDER BY id",
        )
        ids = [r.id for r in df.collect()]
        assert ids == [2, 3]
    finally:
        spark.stop()


@pytest.mark.spark
def test_write_spark_df_inferred_append_round_trip(pg_url):
    """Round-trip: read a Spark DF, write it back to a new Postgres table."""
    conn = _seed_and_setup(pg_url)
    spark = _spark_session()
    try:
        df = conn.read_spark_df(spark, "SELECT id, name FROM src.t")
        result = conn.write_spark_df(df, "warehouse.copied", mode="append")
        assert result["path"] == "inferred"
        assert (
            conn.fetch_scalar_int("SELECT count(*)::int FROM warehouse.copied")
            == 3
        )
    finally:
        spark.stop()


@pytest.mark.spark
def test_write_spark_df_managed_table_merge(pg_url):
    """ManagedTable path: write_spark_df runs through the strategy executor
    so merge mode works identically to the polars/pandas helper."""
    from ematix_flow import ematix, pk
    from ematix_flow import pipeline as p
    from ematix_flow.types import BigInt, Text

    conn = _seed_and_setup(pg_url)
    p._REGISTRY.clear()
    p._FEATURE_VIEWS_REGISTRY.clear()

    @ematix.table(schema="warehouse")
    class Copied:
        id: Annotated[BigInt, pk()]
        name: Text

    spark = _spark_session()
    try:
        df = conn.read_spark_df(spark, "SELECT id, name FROM src.t")
        result = conn.write_spark_df(
            df, "warehouse.copied", mode="merge", target=Copied
        )
        assert result["rows_inserted"] == 3
        # Re-write with a partial overlap → second invocation upserts.
        df2 = conn.read_spark_df(
            spark, "SELECT 2::bigint AS id, 'b-updated'::text AS name"
        )
        conn.write_spark_df(
            df2, "warehouse.copied", mode="merge", target=Copied
        )
        # id=2 reflects the updated value; id=1 and id=3 unchanged.
        updated = conn.fetch_scalar_int(
            "SELECT count(*)::int FROM warehouse.copied "
            "WHERE id = 2 AND name = 'b-updated'"
        )
        assert updated == 1
    finally:
        spark.stop()
