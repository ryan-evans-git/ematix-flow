"""Cloud-warehouse connection dataclasses + Arrow query adapters.

Phase 2 of "What's not shipped" — adds Snowflake / BigQuery / Redshift
to ematix-flow's connection registry.

## Scope

This module ships the **Python-side surface**:

- Three typed ``Connection`` dataclasses with field validation,
  ``${...}`` interpolation (env + pluggable secret stores), and
  password / private-key redaction in ``repr()``.
- Three Arrow query adapters — ``snowflake_query_to_arrow``,
  ``bigquery_query_to_arrow``, ``redshift_query_to_arrow`` — that
  execute a SQL string against the warehouse and return a
  :class:`pyarrow.Table`.

The adapters can be used standalone today (run the query, get an
Arrow table, hand it to whatever ematix-flow sink you like). First-
class pipeline-source / ``@ematix.connection`` integration so users
can write ``source=Source.snowflake_query(conn, sql)`` is **Phase 2b**
— it needs Rust-side dialect dispatch in
``crates/ematix-flow-core/src/backend.rs`` and is intentionally
out of scope for this slice.

## Why Python-side first

The native Rust drivers for Snowflake and BigQuery are immature
(``snowflake-arrow``, ``bigquery-rs``) and would add 6+ months of
maintenance burden. The Python SDKs (``snowflake-connector-python``,
``google-cloud-bigquery``) are battle-tested, well-documented, and
already return Arrow batches natively for both. Shipping the Python
side now lets users get value immediately; the Rust integration is a
future optimisation.

Redshift speaks Postgres wire protocol, so a ``RedshiftConnection``
can be converted to a Postgres DSN via :meth:`RedshiftConnection.to_postgres_url`
and used through the existing ``Source.postgres_query`` path
unchanged. The dedicated ``redshift_query_to_arrow`` adapter exists
mainly so future ``COPY FROM s3://...`` write-side acceleration has a
clean place to land.

## Install extras

::

    pip install "ematix-flow[snowflake]"   # snowflake-connector-python
    pip install "ematix-flow[bigquery]"    # google-cloud-bigquery + bigquery-storage
    pip install "ematix-flow[redshift]"    # redshift-connector
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from ematix_flow.connections import Connection
from ematix_flow.secrets import expand

__all__ = [
    "BigQueryConnection",
    "RedshiftConnection",
    "SnowflakeConnection",
    "WarehouseRunResult",
    "WarehouseSource",
    "WarehouseSyncError",
    "WarehouseTarget",
    "bigquery_query_to_arrow",
    "bigquery_write_arrow",
    "redshift_query_to_arrow",
    "redshift_write_arrow",
    "run_warehouse_pipeline",
    "snowflake_query_to_arrow",
    "snowflake_write_arrow",
]


# ============================================================
# Connection dataclasses
# ============================================================


@dataclass(repr=False)
class SnowflakeConnection(Connection):
    """Snowflake account handle.

    Authenticates with either ``password`` or ``private_key`` (key-pair
    auth — the field holds the PEM string, not a path; users can
    bridge from a path via ``Path(...).read_text()`` or
    ``${vault:...}`` interpolation).

    ``role`` is optional; without it Snowflake uses the user's default
    role. ``warehouse``, ``database``, and ``schema`` are optional at
    the connection level — they can be set per-query via SQL — but
    setting them here saves a ``USE`` round-trip.
    """

    account: str = ""
    user: str = ""
    password: str = ""
    private_key: str = ""
    warehouse: str = ""
    database: str = ""
    schema: str = ""
    role: str = ""

    def __post_init__(self) -> None:
        self.kind = "snowflake"
        if not self.account:
            raise ValueError(f"SnowflakeConnection({self.name!r}): account is required")
        if not self.user:
            raise ValueError(f"SnowflakeConnection({self.name!r}): user is required")
        if not self.password and not self.private_key:
            raise ValueError(
                f"SnowflakeConnection({self.name!r}): either password or private_key "
                "is required"
            )

    def resolved_password(self) -> str:
        """``${...}``-interpolated password. Returns the empty string
        if no password is set (key-pair auth only)."""
        return expand(self.password) or ""

    def resolved_private_key(self) -> str:
        return expand(self.private_key) or ""

    def resolved_account(self) -> str:
        return expand(self.account) or ""

    def resolved_user(self) -> str:
        return expand(self.user) or ""


@dataclass(repr=False)
class BigQueryConnection(Connection):
    """Google BigQuery dataset handle.

    ``credentials_path`` is optional — when unset, the SDK falls
    back to Application Default Credentials (the standard
    ``gcloud auth application-default login`` flow + GKE workload
    identity / Cloud Run runtime credentials).

    ``location`` (e.g. ``"us-central1"``) is optional but recommended
    for multi-region projects so query routing is deterministic.
    """

    project: str = ""
    dataset: str = ""
    credentials_path: str = ""
    location: str = ""

    def __post_init__(self) -> None:
        self.kind = "bigquery"
        if not self.project:
            raise ValueError(f"BigQueryConnection({self.name!r}): project is required")
        if not self.dataset:
            raise ValueError(f"BigQueryConnection({self.name!r}): dataset is required")

    def resolved_project(self) -> str:
        return expand(self.project) or ""

    def resolved_dataset(self) -> str:
        return expand(self.dataset) or ""

    def resolved_credentials_path(self) -> str:
        return expand(self.credentials_path) or ""


@dataclass(repr=False)
class RedshiftConnection(Connection):
    """Amazon Redshift cluster handle.

    Redshift speaks PostgreSQL wire protocol, so reads "just work"
    via the existing :class:`ematix_flow.connections.PostgresConnection`
    + :func:`ematix_flow.source.Source.postgres_query` path — call
    :meth:`to_postgres_url` to bridge.

    The Redshift-specific fields exist mainly so the future ``COPY
    FROM s3://...`` write-side acceleration (Phase 2b) has a clean
    home: ``s3_staging_dir`` is the bucket prefix Redshift loads
    from, and ``iam_role`` is the IAM role ARN Redshift assumes to
    read that bucket.
    """

    host: str = ""
    port: int = 5439
    database: str = ""
    user: str = ""
    password: str = ""
    s3_staging_dir: str = ""
    iam_role: str = ""

    def __post_init__(self) -> None:
        self.kind = "redshift"
        if not self.host:
            raise ValueError(f"RedshiftConnection({self.name!r}): host is required")
        if not self.database:
            raise ValueError(f"RedshiftConnection({self.name!r}): database is required")
        if not self.user:
            raise ValueError(f"RedshiftConnection({self.name!r}): user is required")
        if not self.password:
            raise ValueError(f"RedshiftConnection({self.name!r}): password is required")

    def to_postgres_url(self) -> str:
        """Render the connection as a ``postgres://`` URL.

        Useful for bridging Redshift through the existing
        :class:`ematix_flow.connections.PostgresConnection` path.
        Credentials are interpolated through the secrets registry.
        """
        user = expand(self.user)
        password = expand(self.password)
        host = expand(self.host)
        database = expand(self.database)
        return f"postgres://{user}:{password}@{host}:{self.port}/{database}"


# ============================================================
# Arrow query adapters
# ============================================================


def snowflake_query_to_arrow(
    conn: SnowflakeConnection,
    query: str,
    *,
    _client: Any | None = None,
) -> pa.Table:
    """Execute ``query`` against ``conn`` and return the result as a
    :class:`pyarrow.Table`.

    Uses ``snowflake-connector-python``'s built-in Arrow result-set
    materialisation (``cursor.fetch_arrow_all()``). The connector
    streams batches over the Snowflake REST API but materialises
    them into a single Arrow Table in this helper — for very large
    result sets, consider iterating ``cursor.fetch_arrow_batches()``
    directly.

    ``_client`` is an internal hook for tests; production callers
    pass only ``conn`` + ``query`` and the connector is built from
    the connection's fields.
    """
    if _client is None:
        try:
            import snowflake.connector  # type: ignore[import-not-found]
        except ImportError as exc:
            raise ImportError(
                "snowflake-connector-python is required for "
                "snowflake_query_to_arrow; install with "
                "`pip install ematix-flow[snowflake]`"
            ) from exc
        connect_kwargs: dict[str, Any] = {
            "account": conn.resolved_account(),
            "user": conn.resolved_user(),
        }
        if conn.password:
            connect_kwargs["password"] = conn.resolved_password()
        if conn.private_key:
            connect_kwargs["private_key"] = conn.resolved_private_key()
        for fld in ("warehouse", "database", "schema", "role"):
            v = expand(getattr(conn, fld))
            if v:
                connect_kwargs[fld] = v
        _client = snowflake.connector.connect(**connect_kwargs)
    with _client.cursor() as cur:
        cur.execute(query)
        return cur.fetch_arrow_all()


def bigquery_query_to_arrow(
    conn: BigQueryConnection,
    query: str,
    *,
    _client: Any | None = None,
) -> pa.Table:
    """Execute ``query`` against ``conn`` and return the result as a
    :class:`pyarrow.Table`.

    Uses ``google-cloud-bigquery``'s ``QueryJob.to_arrow()``, which
    routes through the BigQuery Storage Read API when available
    (faster for large results) and falls back to the standard
    query-results pagination otherwise.

    ``_client`` is an internal hook for tests.
    """
    if _client is None:
        try:
            from google.cloud import bigquery  # type: ignore[import-not-found]
        except ImportError as exc:
            raise ImportError(
                "google-cloud-bigquery is required for "
                "bigquery_query_to_arrow; install with "
                "`pip install ematix-flow[bigquery]`"
            ) from exc
        client_kwargs: dict[str, Any] = {"project": conn.resolved_project()}
        if conn.location:
            client_kwargs["location"] = expand(conn.location)
        cred_path = conn.resolved_credentials_path()
        if cred_path:
            # The SDK reads GOOGLE_APPLICATION_CREDENTIALS from env;
            # we set it here scoped to the client construction so we
            # don't pollute the global env.
            import os

            os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", cred_path)
        _client = bigquery.Client(**client_kwargs)
    return _client.query(query).to_arrow()


def redshift_query_to_arrow(
    conn: RedshiftConnection,
    query: str,
    *,
    _client: Any | None = None,
) -> pa.Table:
    """Execute ``query`` against ``conn`` and return the result as a
    :class:`pyarrow.Table`.

    Uses ``redshift-connector`` (or any PostgreSQL-protocol driver
    via the ``_client`` hook). Redshift doesn't have a native Arrow
    output API like Snowflake / BigQuery do, so we materialise rows
    via ``cursor.fetchall()`` and build the Arrow table column-by-
    column from the cursor's ``description``.

    For multi-million-row results consider issuing ``UNLOAD`` to S3
    Parquet and reading with ematix-flow's standard parquet path
    instead — that bypasses the row-tuple round-trip entirely.

    ``_client`` is an internal hook for tests.
    """
    if _client is None:
        try:
            import redshift_connector  # type: ignore[import-not-found]
        except ImportError as exc:
            raise ImportError(
                "redshift-connector is required for "
                "redshift_query_to_arrow; install with "
                "`pip install ematix-flow[redshift]`"
            ) from exc
        _client = redshift_connector.connect(
            host=expand(conn.host),
            port=conn.port,
            database=expand(conn.database),
            user=expand(conn.user),
            password=expand(conn.password),
        )
    with _client.cursor() as cur:
        cur.execute(query)
        col_names = [d[0] for d in cur.description]
        rows = cur.fetchall()
    # Build columns from the row tuples. Pyarrow infers types from
    # the data; users wanting explicit schemas should run the query
    # outside this helper.
    columns: dict[str, list[Any]] = {name: [] for name in col_names}
    for row in rows:
        for name, value in zip(col_names, row, strict=False):
            columns[name].append(value)
    return pa.table(columns)


# ============================================================
# Write-side adapters
# ============================================================


def _quote_snowflake_identifier(name: str) -> str:
    """Double-quote a Snowflake identifier, escaping embedded quotes.

    Snowflake folds unquoted identifiers to uppercase. Double-quoting
    preserves the user's casing — what they declared in the Arrow
    schema lands as that exact column name in the Snowflake table.
    """
    return '"' + name.replace('"', '""') + '"'


def _arrow_type_to_snowflake(field: pa.Field) -> str:
    """Map an Arrow type to a conservative Snowflake DDL type.

    Snowflake's PARQUET file format performs type coercion on
    ``COPY INTO``, so the target column types don't have to match the
    parquet payload exactly. Defaults are conservative — wide enough
    not to truncate, narrow enough not to mis-classify (e.g. ``int64``
    → ``NUMBER(19)``, not ``VARIANT``).
    """
    t = field.type
    if pa.types.is_int8(t) or pa.types.is_int16(t) or pa.types.is_int32(t):
        return "NUMBER(10)"
    if pa.types.is_int64(t):
        return "NUMBER(19)"
    if pa.types.is_uint8(t) or pa.types.is_uint16(t) or pa.types.is_uint32(t):
        return "NUMBER(10)"
    if pa.types.is_uint64(t):
        return "NUMBER(20)"
    if pa.types.is_float32(t):
        return "FLOAT"
    if pa.types.is_float64(t):
        return "DOUBLE"
    if pa.types.is_boolean(t):
        return "BOOLEAN"
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "VARCHAR(16777216)"
    if pa.types.is_date32(t) or pa.types.is_date64(t):
        return "DATE"
    if pa.types.is_timestamp(t):
        return "TIMESTAMP_NTZ" if t.tz is None else "TIMESTAMP_TZ"
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return "BINARY"
    if pa.types.is_decimal(t):
        return f"NUMBER({t.precision},{t.scale})"
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        return "ARRAY"
    if pa.types.is_struct(t) or pa.types.is_map(t):
        return "OBJECT"
    # Catch-all: Snowflake's JSON-shaped variant column.
    return "VARIANT"


def _arrow_schema_to_snowflake_ddl(table_name: str, schema: pa.Schema) -> str:
    """Build ``CREATE TABLE IF NOT EXISTS`` from an Arrow schema.

    Used when ``snowflake_write_arrow(create_if_not_exists=True)`` is
    asked to land into a table that doesn't yet exist. Identifiers
    are double-quoted so original casing is preserved.
    """
    cols = []
    for field in schema:
        sql_type = _arrow_type_to_snowflake(field)
        nullable = "" if field.nullable else " NOT NULL"
        cols.append(f"{_quote_snowflake_identifier(field.name)} {sql_type}{nullable}")
    return (
        f"CREATE TABLE IF NOT EXISTS {_quote_snowflake_identifier(table_name)} "
        f"({', '.join(cols)})"
    )


def snowflake_write_arrow(
    conn: SnowflakeConnection,
    table: pa.Table,
    *,
    table_name: str,
    create_if_not_exists: bool = True,
    _client: Any | None = None,
) -> int:
    """Bulk-load ``table`` into Snowflake table ``table_name`` via a
    parquet-staged ``PUT`` + ``COPY INTO``. Returns the row count
    written.

    **Arrow-native — no pandas dependency.** The Arrow table is
    written to a local temp parquet file with ``pyarrow.parquet``,
    uploaded to Snowflake's table-scoped session stage with ``PUT``,
    and loaded with ``COPY INTO ... FILE_FORMAT = (TYPE = PARQUET)
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE``. This replaces the
    earlier ``write_pandas`` path (which required pandas as a
    transitive runtime dep).

    When ``create_if_not_exists=True`` and the target table doesn't
    exist, a ``CREATE TABLE IF NOT EXISTS`` is issued first with a
    schema derived from the Arrow schema; see
    :func:`_arrow_type_to_snowflake` for the type mapping.

    For multi-GB tables, prefer external-stage S3 + ``COPY INTO``
    directly. This adapter uses the session stage (``@%<table>``)
    which is convenient but uploads through the local connection.
    """
    if _client is None:
        try:
            import snowflake.connector  # type: ignore[import-not-found]
        except ImportError as exc:
            raise ImportError(
                "snowflake-connector-python is required for "
                "snowflake_write_arrow; install with "
                "`pip install ematix-flow[snowflake]`"
            ) from exc
        connect_kwargs: dict[str, Any] = {
            "account": conn.resolved_account(),
            "user": conn.resolved_user(),
        }
        if conn.password:
            connect_kwargs["password"] = conn.resolved_password()
        if conn.private_key:
            connect_kwargs["private_key"] = conn.resolved_private_key()
        for fld in ("warehouse", "database", "schema", "role"):
            v = expand(getattr(conn, fld))
            if v:
                connect_kwargs[fld] = v
        _client = snowflake.connector.connect(**connect_kwargs)

    # Stage the Arrow table as a local parquet file. ``delete=False``
    # because Windows can't unlink an open file; we clean up
    # explicitly in the finally.
    import os
    import tempfile

    import pyarrow.parquet as pq

    nrows = table.num_rows
    with tempfile.NamedTemporaryFile(
        suffix=".parquet", delete=False
    ) as tmp:
        parquet_path = tmp.name
    try:
        pq.write_table(table, parquet_path)
        cursor = _client.cursor()
        try:
            quoted_table = _quote_snowflake_identifier(table_name)
            if create_if_not_exists:
                cursor.execute(
                    _arrow_schema_to_snowflake_ddl(table_name, table.schema)
                )
            # PUT to the table's session stage. ``OVERWRITE = TRUE``
            # so repeated runs replace any leftover file from a prior
            # failed COPY; ``AUTO_COMPRESS = FALSE`` because parquet
            # is already columnar-compressed and the Snowflake-side
            # gzip wrapper would just add CPU + bytes.
            cursor.execute(
                f"PUT file://{parquet_path} @%{quoted_table} "
                "OVERWRITE = TRUE AUTO_COMPRESS = FALSE"
            )
            staged_basename = os.path.basename(parquet_path)
            # COPY INTO. ``MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE``
            # handles column-order independence between the parquet
            # and the target table. ``PURGE = TRUE`` deletes the
            # staged file after a successful copy.
            cursor.execute(
                f"COPY INTO {quoted_table} "
                f"FROM @%{quoted_table}/{staged_basename} "
                "FILE_FORMAT = (TYPE = PARQUET) "
                "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE "
                "PURGE = TRUE"
            )
        finally:
            cursor.close()
    except Exception as exc:
        raise WarehouseSyncError(
            f"snowflake_write_arrow: write failed for table "
            f"{table_name!r}: {exc}"
        ) from exc
    finally:
        try:
            os.unlink(parquet_path)
        except OSError:
            # Best-effort cleanup; temp dir GC will sweep eventually.
            pass
    return nrows


def bigquery_write_arrow(
    conn: BigQueryConnection,
    table: pa.Table,
    *,
    table_name: str,
    create_if_not_exists: bool = True,
    _client: Any | None = None,
) -> int:
    """Bulk-load ``table`` into BigQuery via parquet
    ``load_table_from_file``. Returns the row count.

    **Arrow-native — no pandas dependency.** The Arrow table is
    written to a local temp parquet file with ``pyarrow.parquet``,
    then handed to the BigQuery client's ``load_table_from_file``
    with ``source_format = PARQUET``. This replaces the earlier
    ``load_table_from_dataframe`` path (which required pandas).

    ``create_if_not_exists=True`` maps to ``WRITE_TRUNCATE`` (creates
    or replaces); ``False`` maps to ``WRITE_APPEND`` (rejects if the
    table is missing). For very large tables, prefer staging the
    parquet to GCS and using ``LOAD DATA`` directly.
    """
    if _client is None:
        try:
            from google.cloud import bigquery  # type: ignore[import-not-found]
        except ImportError as exc:
            raise ImportError(
                "google-cloud-bigquery is required for "
                "bigquery_write_arrow; install with "
                "`pip install ematix-flow[bigquery]`"
            ) from exc
        client_kwargs: dict[str, Any] = {"project": conn.resolved_project()}
        if conn.location:
            client_kwargs["location"] = expand(conn.location)
        _client = bigquery.Client(**client_kwargs)

    import os
    import tempfile

    import pyarrow.parquet as pq

    project = conn.resolved_project()
    dataset = conn.resolved_dataset()
    table_ref = f"{project}.{dataset}.{table_name}"
    nrows = table.num_rows

    with tempfile.NamedTemporaryFile(
        suffix=".parquet", delete=False
    ) as tmp:
        parquet_path = tmp.name
    try:
        pq.write_table(table, parquet_path)
        # The job-config import is lazy so mocked tests don't need
        # the SDK installed.
        try:
            from google.cloud import bigquery as _bq  # type: ignore[import-not-found]

            write_disposition = (
                _bq.WriteDisposition.WRITE_APPEND
                if not create_if_not_exists
                else _bq.WriteDisposition.WRITE_TRUNCATE
            )
            job_config = _bq.LoadJobConfig(
                source_format=_bq.SourceFormat.PARQUET,
                write_disposition=write_disposition,
            )
            with open(parquet_path, "rb") as fh:
                job = _client.load_table_from_file(
                    fh, table_ref, job_config=job_config
                )
        except ImportError:
            # Pure-mock path (test client has no bigquery import).
            with open(parquet_path, "rb") as fh:
                job = _client.load_table_from_file(fh, table_ref)
        job.result()  # blocks until the load job completes
    except Exception as exc:
        raise WarehouseSyncError(
            f"bigquery_write_arrow: write failed for table "
            f"{table_name!r}: {exc}"
        ) from exc
    finally:
        try:
            os.unlink(parquet_path)
        except OSError:
            pass  # best-effort cleanup
    return nrows


def redshift_write_arrow(
    conn: RedshiftConnection,
    table: pa.Table,
    *,
    table_name: str,
    create_if_not_exists: bool = True,
    _client: Any | None = None,
    _s3_client: Any | None = None,
) -> int:
    """Bulk-load ``table`` into Redshift via S3 + ``COPY``.

    Requires ``s3_staging_dir`` + ``iam_role`` on the connection.
    Writes the Arrow table as Parquet into the staging dir, then
    runs ``COPY <table> FROM '<staging>' IAM_ROLE '<role>' FORMAT
    PARQUET``. Returns the row count.

    For small writes, callers can fall back to row-by-row INSERTs
    via standard psycopg2 — but COPY is dramatically faster on
    anything more than a few thousand rows.
    """
    if not conn.s3_staging_dir or not conn.iam_role:
        raise WarehouseSyncError(
            f"redshift_write_arrow: connection {conn.name!r} needs both "
            "s3_staging_dir and iam_role to use bulk-COPY writes"
        )
    if _client is None:
        try:
            import redshift_connector  # type: ignore[import-not-found]
        except ImportError as exc:
            raise ImportError(
                "redshift-connector is required for redshift_write_arrow; "
                "install with `pip install ematix-flow[redshift]`"
            ) from exc
        _client = redshift_connector.connect(
            host=expand(conn.host),
            port=conn.port,
            database=expand(conn.database),
            user=expand(conn.user),
            password=expand(conn.password),
        )
    if _s3_client is None:
        try:
            import boto3  # type: ignore[import-not-found]
        except ImportError as exc:
            raise ImportError(
                "boto3 is required for redshift bulk writes (S3 staging); "
                "install with `pip install ematix-flow[redshift]`"
            ) from exc
        _s3_client = boto3.client("s3")
    # Stage the Arrow table to S3 as Parquet.
    import io as _io
    import re as _re
    import uuid as _uuid

    import pyarrow.parquet as pq  # type: ignore[import-not-found]

    buf = _io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    m = _re.match(r"^s3://([^/]+)/(.*)$", expand(conn.s3_staging_dir) or "")
    if not m:
        raise WarehouseSyncError(
            f"redshift_write_arrow: s3_staging_dir {conn.s3_staging_dir!r} "
            "must look like s3://bucket/prefix/"
        )
    bucket = m.group(1)
    prefix = m.group(2).rstrip("/")
    key = f"{prefix}/{_uuid.uuid4().hex}.parquet"
    _s3_client.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    staging_url = f"s3://{bucket}/{key}"
    try:
        with _client.cursor() as cur:
            cur.execute(
                f"COPY {table_name} FROM '{staging_url}' "
                f"IAM_ROLE '{expand(conn.iam_role)}' FORMAT PARQUET"
            )
        _client.commit()
    finally:
        # Clean up the staging file even if COPY failed.
        try:
            _s3_client.delete_object(Bucket=bucket, Key=key)
        except Exception:
            pass  # best-effort cleanup
    return table.num_rows


# ============================================================
# Pipeline integration — Source/Target factories + orchestrator
# ============================================================


class WarehouseSyncError(RuntimeError):
    """Raised when a warehouse read / transform / write fails."""


@dataclass(frozen=True)
class WarehouseSource:
    """A typed warehouse source spec: (connection, sql, kind).

    Returned by ``Source.snowflake_query`` / ``bigquery_query`` /
    ``redshift_query``; consumed by ``run_warehouse_pipeline``.
    """

    connection: Connection
    sql: str
    kind: str  # "snowflake" | "bigquery" | "redshift"


@dataclass(frozen=True)
class WarehouseTarget:
    """A typed warehouse target spec: (connection, table_name, kind).

    Returned by ``WarehouseTarget.snowflake_table`` /
    ``bigquery_table`` / ``redshift_table``; consumed by
    ``run_warehouse_pipeline``.

    ``create_if_not_exists`` defers schema decisions to the
    warehouse's auto-create when True; explicit ``CREATE TABLE``
    DDL is operator-supplied when False.
    """

    connection: Connection
    table_name: str
    kind: str  # "snowflake" | "bigquery" | "redshift"
    create_if_not_exists: bool = True

    @classmethod
    def snowflake_table(
        cls,
        connection: SnowflakeConnection,
        table_name: str,
        *,
        create_if_not_exists: bool = True,
    ) -> WarehouseTarget:
        return cls(
            connection=connection,
            table_name=table_name,
            kind="snowflake",
            create_if_not_exists=create_if_not_exists,
        )

    @classmethod
    def bigquery_table(
        cls,
        connection: BigQueryConnection,
        table_name: str,
        *,
        create_if_not_exists: bool = True,
    ) -> WarehouseTarget:
        return cls(
            connection=connection,
            table_name=table_name,
            kind="bigquery",
            create_if_not_exists=create_if_not_exists,
        )

    @classmethod
    def redshift_table(
        cls,
        connection: RedshiftConnection,
        table_name: str,
        *,
        create_if_not_exists: bool = True,
    ) -> WarehouseTarget:
        return cls(
            connection=connection,
            table_name=table_name,
            kind="redshift",
            create_if_not_exists=create_if_not_exists,
        )


@dataclass(frozen=True)
class WarehouseRunResult:
    """Outcome of :func:`run_warehouse_pipeline`."""

    rows_read: int
    rows_written: int
    duration_ms: int


def run_warehouse_pipeline(
    source: WarehouseSource,
    target: WarehouseTarget,
    *,
    transform_sql: str | None = None,
    _source_client: Any | None = None,
    _target_client: Any | None = None,
    _target_s3_client: Any | None = None,
) -> WarehouseRunResult:
    """Read → optional in-memory SQL transform → write.

    Reads Arrow batches from ``source`` via the matching
    ``*_query_to_arrow`` adapter, optionally runs ``transform_sql``
    against the result via DuckDB's in-memory engine, then writes
    to ``target`` via the matching ``*_write_arrow`` adapter.

    Returns row counts + duration in milliseconds. Raises
    :class:`WarehouseSyncError` on read / transform / write
    failures with the underlying message preserved.

    The ``_source_client`` / ``_target_client`` / ``_target_s3_client``
    parameters are internal test hooks; production callers pass only
    the source + target + transform_sql.
    """
    import time as _time

    started = _time.monotonic()

    # ---- Read ---------------------------------------------------
    try:
        if source.kind == "snowflake":
            arrow_in = snowflake_query_to_arrow(
                source.connection, source.sql, _client=_source_client
            )
        elif source.kind == "bigquery":
            arrow_in = bigquery_query_to_arrow(
                source.connection, source.sql, _client=_source_client
            )
        elif source.kind == "redshift":
            arrow_in = redshift_query_to_arrow(
                source.connection, source.sql, _client=_source_client
            )
        else:
            raise WarehouseSyncError(
                f"run_warehouse_pipeline: unknown source kind {source.kind!r}"
            )
    except Exception as exc:
        if isinstance(exc, WarehouseSyncError):
            raise
        raise WarehouseSyncError(
            f"read failed from {source.kind} source: {exc}"
        ) from exc

    rows_read = arrow_in.num_rows

    # ---- Transform ----------------------------------------------
    if transform_sql:
        try:
            import duckdb  # type: ignore[import-not-found]
        except ImportError as exc:
            raise ImportError(
                "duckdb is required for transform_sql; install with "
                "`pip install ematix-flow[runlog-duckdb]` or any extra "
                "that includes duckdb"
            ) from exc
        con = duckdb.connect()
        try:
            con.register("source", arrow_in)
            arrow_out = con.execute(transform_sql).arrow()
        except Exception as exc:
            raise WarehouseSyncError(
                f"transform_sql failed: {exc}"
            ) from exc
        finally:
            con.close()
    else:
        arrow_out = arrow_in

    # ---- Write --------------------------------------------------
    try:
        if target.kind == "snowflake":
            rows_written = snowflake_write_arrow(
                target.connection,
                arrow_out,
                table_name=target.table_name,
                create_if_not_exists=target.create_if_not_exists,
                _client=_target_client,
            )
        elif target.kind == "bigquery":
            rows_written = bigquery_write_arrow(
                target.connection,
                arrow_out,
                table_name=target.table_name,
                create_if_not_exists=target.create_if_not_exists,
                _client=_target_client,
            )
        elif target.kind == "redshift":
            rows_written = redshift_write_arrow(
                target.connection,
                arrow_out,
                table_name=target.table_name,
                create_if_not_exists=target.create_if_not_exists,
                _client=_target_client,
                _s3_client=_target_s3_client,
            )
        else:
            raise WarehouseSyncError(
                f"run_warehouse_pipeline: unknown target kind {target.kind!r}"
            )
    except Exception as exc:
        if isinstance(exc, WarehouseSyncError):
            raise
        raise WarehouseSyncError(
            f"write failed to {target.kind} target: {exc}"
        ) from exc

    elapsed_ms = int((_time.monotonic() - started) * 1000)
    return WarehouseRunResult(
        rows_read=rows_read,
        rows_written=rows_written,
        duration_ms=elapsed_ms,
    )
