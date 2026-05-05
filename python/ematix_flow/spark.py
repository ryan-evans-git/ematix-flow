"""Phase 28: Spark DataFrame interop.

Importing this module monkey-patches `_core.Connection` with two
methods:

  conn.read_spark_df(spark_session, sql) -> pyspark.sql.DataFrame
  conn.write_spark_df(df, qualified_name, *, mode, target=, keys=, ...)

Pyspark is optional — `pip install ematix-flow[spark]`. The user owns
the SparkSession (Spark is heavy: JVM, JDBC jar, cluster config). This
module just wires Postgres ↔ Spark via the JDBC driver, and routes
write_spark_df through the existing strategy executor (same pattern
as the polars/pandas write_df in Phase 27e).

The Postgres JDBC jar is not bundled. Users typically wire it in via
`SparkSession.builder.config("spark.jars.packages",
"org.postgresql:postgresql:42.7.x")`. Document the lift, don't hide it.
"""

from __future__ import annotations

import uuid
from typing import Any
from urllib.parse import parse_qsl, urlparse

from ematix_flow import _core

# --- DSN → JDBC URL --------------------------------------------------------


def _dsn_to_jdbc(dsn: str) -> tuple[str, dict[str, str]]:
    """Convert a tokio-postgres DSN into a JDBC URL + properties dict.

    Returns (jdbc_url, properties) where `properties` carries `user`,
    `password`, and any extra query parameters (sslmode, application_name)
    so they pass through to the JDBC driver.
    """
    if not dsn.startswith(("postgres://", "postgresql://")):
        raise ValueError(
            f"Spark interop requires a Postgres DSN; got {dsn[:32]!r}"
        )
    parsed = urlparse(dsn)
    host = parsed.hostname
    port = parsed.port or 5432
    dbname = (parsed.path or "").lstrip("/")
    user = parsed.username
    password = parsed.password
    if not host or not dbname or not user:
        raise ValueError(
            f"DSN must include host, user, and dbname; parsed "
            f"host={host!r} user={user!r} dbname={dbname!r}"
        )
    jdbc = f"jdbc:postgresql://{host}:{port}/{dbname}"
    props: dict[str, str] = {"user": user}
    if password is not None:
        props["password"] = password
    for k, v in parse_qsl(parsed.query):
        if k not in props:
            props[k] = v
    return jdbc, props


# --- read_spark_df --------------------------------------------------------


def _is_spark_session(obj: Any) -> bool:
    cls = type(obj)
    mod = getattr(cls, "__module__", "") or ""
    return mod.startswith("pyspark.sql") and cls.__name__ == "SparkSession"


def _is_spark_dataframe(obj: Any) -> bool:
    cls = type(obj)
    mod = getattr(cls, "__module__", "") or ""
    return mod.startswith("pyspark.sql") and cls.__name__ == "DataFrame"


def _read_spark_df_impl(self, spark_session: Any, sql: str) -> Any:
    """Execute `sql` via Spark JDBC, return a pyspark.sql.DataFrame."""
    if not _is_spark_session(spark_session):
        raise TypeError(
            f"read_spark_df expects a pyspark.sql.SparkSession; got "
            f"{type(spark_session).__name__}. `pip install ematix-flow[spark]`"
        )
    jdbc, props = _dsn_to_jdbc(self.dsn())
    # Wrap arbitrary SQL via `(<sql>) AS subq` — Spark JDBC requires a
    # table-shaped reference, but a parenthesized subquery counts.
    return (
        spark_session.read.format("jdbc")
        .option("url", jdbc)
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", f"({sql}) AS ematix_subq")
        .options(**props)
        .load()
    )


# --- write_spark_df -------------------------------------------------------


def _write_spark_df_impl(
    self,
    df: Any,
    qualified_name: str,
    *,
    mode: str,
    target: Any = None,
    keys: tuple[str, ...] | None = None,
    update_columns: tuple[str, ...] | None = None,
    compare_columns: tuple[str, ...] | None = None,
    event_timestamp_column: str | None = None,
) -> dict[str, Any]:
    """Stage a Spark DataFrame to a real Postgres table via JDBC, then
    route through the strategy executor (same pattern as 27e write_df).
    """
    if not _is_spark_dataframe(df):
        raise TypeError(
            f"write_spark_df expects a Spark DataFrame; got "
            f"{type(df).__name__}"
        )
    if "." not in qualified_name:
        raise ValueError(
            f"write_spark_df requires schema-qualified name; got {qualified_name!r}"
        )
    schema, _, table = qualified_name.partition(".")

    jdbc, props = _dsn_to_jdbc(self.dsn())
    staging_name = f"_ematix_spark_{uuid.uuid4().hex[:12]}"
    staging_dbtable = f'"{staging_name}"'

    # Stage via JDBC append into a uuid-named table in the public schema.
    (
        df.write.format("jdbc")
        .option("url", jdbc)
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", staging_dbtable)
        .options(**props)
        .mode("overwrite")  # creates fresh, no append-to-old-staging
        .save()
    )

    try:
        if target is not None:
            from ematix_flow import pipeline as _p
            from ematix_flow.source import Source as _Source

            declared = [name for name, _col in target._columns()]
            cols_sql = ", ".join(f'"{c}"' for c in declared)
            source_sql = f'SELECT {cols_sql} FROM "{staging_name}"'
            source_obj = _Source.postgres_query(self, source_sql)
            return _p.sync(
                target=target,
                source=source_obj,
                target_connection=self,
                mode=mode,
                pipeline_name=f"write_spark_df:{target.__schema__}.{target.__tablename__}",
                keys=keys,
                update_columns=update_columns,
                compare_columns=compare_columns,
                event_timestamp_column=event_timestamp_column,
            )
        # Inferred path: rename the staging table into the requested
        # destination. mode='append' creates if missing; other modes
        # would need declared keys, which we don't have.
        if mode != "append":
            raise NotImplementedError(
                f"inferred write_spark_df currently only supports mode='append'; "
                f"declare a @ematix.table class and pass target= for mode={mode!r}"
            )
        self.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        # Copy rows over rather than rename — staging is in public.
        self.execute(
            f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" '
            f'(LIKE "{staging_name}" INCLUDING DEFAULTS)'
        )
        self.execute(
            f'INSERT INTO "{schema}"."{table}" SELECT * FROM "{staging_name}"'
        )
        return {"path": "inferred"}
    finally:
        try:
            self.execute(f'DROP TABLE IF EXISTS "{staging_name}"')
        except Exception:
            pass


# --- monkey-patch on import ----------------------------------------------


_core.Connection.read_spark_df = _read_spark_df_impl  # type: ignore[attr-defined]
_core.Connection.write_spark_df = _write_spark_df_impl  # type: ignore[attr-defined]


__all__ = []
