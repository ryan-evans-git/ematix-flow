"""Phase 27e: DataFrame interop.

Importing this module monkey-patches `_core.Connection` with two methods:

  conn.read_df(sql, *, prefer="auto") -> polars.DataFrame | pandas.DataFrame
  conn.write_df(df, qualified_name, *, mode, target=None, keys=None, ...)

Auto-detect rules (Q7 → A): `prefer="auto"` returns polars when both
polars + pandas are installed; falls back to pandas when only pandas
is installed. Forced `prefer="polars"` / `"pandas"` raises ImportError
if missing.

Transport (Q9 → A): write_df stages the df to a TEMP table via
psycopg2 + COPY CSV, then routes through `pipeline.sync` for the
ManagedTable path so every strategy mode (append/truncate/merge/
scd1/scd2) works identically. Inferred path (no `target=`) takes the
df as-is — no metadata columns added (Q8.2 γ).

Requires the `[df]` extra: `pip install ematix-flow[df]`.
"""

from __future__ import annotations

import csv
import io
import json
import uuid
from typing import Any, Literal

from ematix_flow import _core


def _try_import(name: str) -> Any | None:
    try:
        return __import__(name)
    except ImportError:
        return None


# Phase 29: ADBC + pyarrow availability check. When both are present we
# stage DataFrames via Arrow → COPY BINARY (faster + type-correct) instead
# of CSV → COPY TEXT. The CSV path remains as fallback so users on older
# `[df]` extras keep working.
def _adbc_available() -> bool:
    try:
        import adbc_driver_postgresql.dbapi  # noqa: F401
        import pyarrow  # noqa: F401

        return True
    except ImportError:
        return False


_HAS_ADBC = _adbc_available()


def _df_to_arrow_table(df: Any, kind: str) -> Any:
    """Convert a polars / pandas DataFrame to a pyarrow Table with the
    column order preserved. Used by the ADBC ingest path.
    """
    import pyarrow as pa

    if kind == "polars":
        return df.to_arrow()
    return pa.Table.from_pandas(df, preserve_index=False)


def _adbc_ingest(
    dsn: str,
    schema: str,
    table: str,
    arrow_table: Any,
    *,
    mode: str = "create_append",
) -> None:
    """Stage an Arrow Table to `<schema>.<table>` via ADBC's bulk_ingest
    (Postgres COPY BINARY under the hood). `mode` is the ADBC ingest mode
    keyword: "create" / "append" / "create_append" / "replace".
    """
    import adbc_driver_postgresql.dbapi as adbc_pg

    conn = adbc_pg.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.adbc_ingest(
                table,
                arrow_table,
                mode=mode,
                db_schema_name=schema,
            )
        conn.commit()
    finally:
        conn.close()


def _require_psycopg2():
    psycopg2 = _try_import("psycopg2")
    if psycopg2 is None:
        raise ImportError(
            "ematix_flow.df requires psycopg2-binary; install with "
            "`pip install ematix-flow[df]`"
        )
    return psycopg2


def _detect_df_kind(df: Any) -> Literal["polars", "pandas"]:
    polars = _try_import("polars")
    pandas = _try_import("pandas")
    if polars is not None and isinstance(df, polars.DataFrame):
        return "polars"
    if pandas is not None and isinstance(df, pandas.DataFrame):
        return "pandas"
    raise TypeError(
        f"write_df expected a polars or pandas DataFrame; got "
        f"{type(df).__name__}. Install with `pip install ematix-flow[df]` "
        "and pass either polars or pandas."
    )


def _read_df_impl(self, sql: str, *, prefer: str = "auto") -> Any:
    if prefer not in ("auto", "polars", "pandas"):
        raise ValueError(
            f"prefer must be 'auto', 'polars', or 'pandas'; got {prefer!r}"
        )
    polars = _try_import("polars")
    pandas = _try_import("pandas")

    if prefer == "polars" and polars is None:
        raise ImportError(
            "prefer='polars' but polars is not installed; "
            "`pip install polars` or use prefer='pandas'"
        )
    if prefer == "pandas" and pandas is None:
        raise ImportError(
            "prefer='pandas' but pandas is not installed; "
            "`pip install pandas` or use prefer='polars'"
        )
    if polars is None and pandas is None:
        raise ImportError(
            "ematix_flow.df.read_df requires either polars or pandas; "
            "install via `pip install polars` or `pip install pandas`"
        )

    chosen = prefer
    if chosen == "auto":
        chosen = "polars" if polars is not None else "pandas"

    psycopg2 = _require_psycopg2()
    pg = psycopg2.connect(self.dsn())
    try:
        with pg.cursor() as cur:
            cur.execute(sql)
            cols = [d.name for d in cur.description]
            rows = cur.fetchall()
        if chosen == "pandas":
            return pandas.DataFrame(rows, columns=cols)
        return polars.DataFrame(rows, schema=cols, orient="row")
    finally:
        pg.close()


# --- write_df helpers -----------------------------------------------------


def _df_to_csv_bytes_polars(df: Any, columns: list[str]) -> bytes:
    """Serialize a polars DataFrame to CSV with given column order."""
    sub = df.select(columns)
    buf = io.BytesIO()
    sub.write_csv(buf, include_header=False)
    return buf.getvalue()


def _df_to_csv_bytes_pandas(df: Any, columns: list[str]) -> bytes:
    """Serialize a pandas DataFrame to CSV with given column order."""
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, columns=columns, quoting=csv.QUOTE_MINIMAL)
    return buf.getvalue().encode("utf-8")


_INFER_PG_TYPES = {
    # polars
    "Int8": "SMALLINT",
    "Int16": "SMALLINT",
    "Int32": "INTEGER",
    "Int64": "BIGINT",
    "UInt8": "SMALLINT",
    "UInt16": "INTEGER",
    "UInt32": "BIGINT",
    "UInt64": "NUMERIC",
    "Float32": "REAL",
    "Float64": "DOUBLE PRECISION",
    "Boolean": "BOOLEAN",
    "String": "TEXT",
    "Utf8": "TEXT",
    "Date": "DATE",
    "Datetime": "TIMESTAMPTZ",
    # pandas (mapped from str(dtype))
    "int8": "SMALLINT",
    "int16": "SMALLINT",
    "int32": "INTEGER",
    "int64": "BIGINT",
    "float32": "REAL",
    "float64": "DOUBLE PRECISION",
    "bool": "BOOLEAN",
    "object": "TEXT",
    "datetime64[ns]": "TIMESTAMPTZ",
    "datetime64[us]": "TIMESTAMPTZ",
    "datetime64[ns, UTC]": "TIMESTAMPTZ",
}


def _polars_schema_to_pg(df: Any) -> list[tuple[str, str]]:
    return [(name, _INFER_PG_TYPES.get(str(dtype), "TEXT")) for name, dtype in df.schema.items()]


def _pandas_schema_to_pg(df: Any) -> list[tuple[str, str]]:
    return [(name, _INFER_PG_TYPES.get(str(df[name].dtype), "TEXT")) for name in df.columns]


def _write_df_impl(
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
    if "." not in qualified_name:
        raise ValueError(
            f"write_df requires a schema-qualified name (e.g., 'public.t'); "
            f"got {qualified_name!r}"
        )
    schema, _, table = qualified_name.partition(".")
    kind = _detect_df_kind(df)

    if target is not None:
        return _write_df_managed(
            self,
            df=df,
            kind=kind,
            target=target,
            mode=mode,
            keys=keys,
            update_columns=update_columns,
            compare_columns=compare_columns,
            event_timestamp_column=event_timestamp_column,
        )
    return _write_df_inferred(
        self,
        df=df,
        kind=kind,
        schema=schema,
        table=table,
        mode=mode,
        keys=keys,
    )


def _stage_df_to_temp_table(
    conn_dsn: str,
    df: Any,
    kind: str,
    columns: list[str],
    pg_types: list[str],
) -> str:
    """Stage df to a uuid-named real table in the public schema. Returns
    the table name; caller drops.

    Uses ADBC (Arrow → COPY BINARY) when available — faster, smaller on
    the wire, type-correct (no string round-tripping for timestamps,
    numerics, booleans). Falls back to CSV `COPY` when ADBC isn't
    installed.
    """
    temp_name = f"_ematix_df_{uuid.uuid4().hex[:12]}"

    if _HAS_ADBC:
        # ADBC's create_append mode infers the table schema from the
        # Arrow table; we don't need to spell out column types.
        arrow_table = _df_to_arrow_table(df.select(columns) if kind == "polars" else df[columns], kind)
        _adbc_ingest(conn_dsn, "public", temp_name, arrow_table, mode="create_append")
        return temp_name

    # Fallback: CSV COPY via psycopg2.
    psycopg2 = _require_psycopg2()
    cols_decl = ", ".join(f'"{c}" {t}' for c, t in zip(columns, pg_types))
    pg = psycopg2.connect(conn_dsn)
    try:
        pg.autocommit = True
        with pg.cursor() as cur:
            cur.execute(f'CREATE TABLE "{temp_name}" ({cols_decl})')
            csv_bytes = (
                _df_to_csv_bytes_polars(df, columns)
                if kind == "polars"
                else _df_to_csv_bytes_pandas(df, columns)
            )
            cur.copy_expert(
                f'COPY "{temp_name}" ({", ".join(chr(34) + c + chr(34) for c in columns)}) '
                f"FROM STDIN WITH (FORMAT CSV)",
                io.BytesIO(csv_bytes),
            )
    except Exception:
        pg.close()
        raise
    pg.close()
    return temp_name


def _write_df_managed(
    self,
    *,
    df: Any,
    kind: str,
    target: Any,
    mode: str,
    keys: tuple[str, ...] | None,
    update_columns: tuple[str, ...] | None,
    compare_columns: tuple[str, ...] | None,
    event_timestamp_column: str | None,
) -> dict[str, Any]:
    """Q8.1 C / Q8.2 γ: validate df shape against ManagedTable, stage
    via temp table, run the strategy executor for full mode support."""
    declared = [name for name, _col in target._columns()]
    if kind == "polars":
        df_cols = list(df.columns)
        pg_types = [t for _, t in _polars_schema_to_pg(df)]
    else:
        df_cols = list(df.columns)
        pg_types = [t for _, t in _pandas_schema_to_pg(df)]

    missing = [c for c in declared if c not in df_cols]
    if missing:
        raise ValueError(
            f"DataFrame is missing columns declared on {target.__name__}: {missing}"
        )

    # Stage to a real (uuid-named) table. Must drop after.
    types_for_declared = [pg_types[df_cols.index(c)] for c in declared]
    temp_name = _stage_df_to_temp_table(
        self.dsn(), df, kind, declared, types_for_declared
    )
    try:
        from ematix_flow import pipeline as _p
        from ematix_flow.source import Source as _Source

        source_sql = f'SELECT {", ".join(chr(34) + c + chr(34) for c in declared)} FROM "{temp_name}"'
        source_obj = _Source.postgres_query(self, source_sql)
        result = _p.sync(
            target=target,
            source=source_obj,
            target_connection=self,
            mode=mode,
            pipeline_name=f"write_df:{target.__schema__}.{target.__tablename__}",
            keys=keys,
            update_columns=update_columns,
            compare_columns=compare_columns,
            event_timestamp_column=event_timestamp_column,
        )
        return result
    finally:
        try:
            self.execute(f'DROP TABLE IF EXISTS "{temp_name}"')
        except Exception:
            pass


def _write_df_inferred(
    self,
    *,
    df: Any,
    kind: str,
    schema: str,
    table: str,
    mode: str,
    keys: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    """Q8.2 γ: inferred path — no metadata columns.

    Supported modes:
      - append: COPY into the existing/created table.
      - truncate: TRUNCATE then COPY.
      - merge / scd1: requires explicit keys=. Stages via uuid temp table,
        then INSERT...ON CONFLICT (keys) DO UPDATE SET non_keys.
      - scd2: rejected — needs SCD2 metadata columns the inferred path
        can't synthesize. Use target=ManagedTable for SCD2.
    """
    if mode == "scd2":
        raise NotImplementedError(
            "inferred write_df does not support mode='scd2' — declare a "
            "@ematix.table class and pass target= for SCD2"
        )
    if mode not in ("append", "truncate", "merge", "scd1"):
        raise ValueError(
            f"inferred write_df: unsupported mode={mode!r}; "
            "expected 'append', 'truncate', 'merge', or 'scd1'"
        )

    psycopg2 = _require_psycopg2()
    if kind == "polars":
        cols_with_types = _polars_schema_to_pg(df)
    else:
        cols_with_types = _pandas_schema_to_pg(df)
    cols = [c for c, _ in cols_with_types]
    decls = ", ".join(f'"{c}" {t}' for c, t in cols_with_types)

    if mode in ("merge", "scd1"):
        if not keys:
            raise ValueError(
                f"inferred write_df mode={mode!r} requires keys=[...]; "
                "without a target= the framework can't infer the upsert key"
            )
        unknown = [k for k in keys if k not in cols]
        if unknown:
            raise ValueError(
                f"keys={list(keys)} reference column(s) not in the DataFrame: "
                f"{unknown} (have: {cols})"
            )

    self.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    self.execute(
        f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" ({decls})'
    )

    if mode == "truncate":
        self.execute(f'TRUNCATE TABLE "{schema}"."{table}"')

    # append / truncate: ingest directly into the destination.
    if mode in ("append", "truncate"):
        if _HAS_ADBC:
            sub_df = df.select(cols) if kind == "polars" else df[cols]
            arrow_table = _df_to_arrow_table(sub_df, kind)
            _adbc_ingest(self.dsn(), schema, table, arrow_table, mode="append")
        else:
            pg = psycopg2.connect(self.dsn())
            try:
                pg.autocommit = True
                csv_bytes = (
                    _df_to_csv_bytes_polars(df, cols)
                    if kind == "polars"
                    else _df_to_csv_bytes_pandas(df, cols)
                )
                with pg.cursor() as cur:
                    cur.copy_expert(
                        f'COPY "{schema}"."{table}" '
                        f'({", ".join(chr(34) + c + chr(34) for c in cols)}) '
                        f"FROM STDIN WITH (FORMAT CSV)",
                        io.BytesIO(csv_bytes),
                    )
            finally:
                pg.close()
        return {"rows_inserted": len(df), "path": f"inferred_{mode}"}

    # merge / scd1: stage to a uuid table → INSERT ... ON CONFLICT.
    staging = f"_ematix_df_{uuid.uuid4().hex[:12]}"
    if _HAS_ADBC:
        sub_df = df.select(cols) if kind == "polars" else df[cols]
        arrow_table = _df_to_arrow_table(sub_df, kind)
        _adbc_ingest(self.dsn(), "public", staging, arrow_table, mode="create_append")
    else:
        pg_stage = psycopg2.connect(self.dsn())
        try:
            pg_stage.autocommit = True
            csv_bytes = (
                _df_to_csv_bytes_polars(df, cols)
                if kind == "polars"
                else _df_to_csv_bytes_pandas(df, cols)
            )
            with pg_stage.cursor() as cur:
                cur.execute(f'CREATE TABLE "{staging}" ({decls})')
                cur.copy_expert(
                    f'COPY "{staging}" '
                    f'({", ".join(chr(34) + c + chr(34) for c in cols)}) '
                    f"FROM STDIN WITH (FORMAT CSV)",
                    io.BytesIO(csv_bytes),
                )
        finally:
            pg_stage.close()

    pg = psycopg2.connect(self.dsn())
    try:
        pg.autocommit = True
        with pg.cursor() as cur:
            non_keys = [c for c in cols if c not in keys]
            target_cols = ", ".join(f'"{c}"' for c in cols)
            conflict_cols = ", ".join(f'"{k}"' for k in keys)
            if non_keys:
                set_clause = ", ".join(
                    f'"{c}" = EXCLUDED."{c}"' for c in non_keys
                )
                upsert = (
                    f'INSERT INTO "{schema}"."{table}" ({target_cols}) '
                    f'SELECT {target_cols} FROM "{staging}" '
                    f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {set_clause}"
                )
            else:
                upsert = (
                    f'INSERT INTO "{schema}"."{table}" ({target_cols}) '
                    f'SELECT {target_cols} FROM "{staging}" '
                    f"ON CONFLICT ({conflict_cols}) DO NOTHING"
                )
            cur.execute(upsert)
            cur.execute(f'DROP TABLE IF EXISTS "{staging}"')
    finally:
        pg.close()
    return {"rows_inserted": len(df), "path": "inferred_merge"}


# --- monkey-patch on import ----------------------------------------------


_core.Connection.read_df = _read_df_impl  # type: ignore[attr-defined]
_core.Connection.write_df = _write_df_impl  # type: ignore[attr-defined]


__all__ = []
