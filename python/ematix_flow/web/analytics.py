"""Ad-hoc SQL query surface for the web SQL editor (SQL Lab spine).

Phase 0 of the SQL Lab + Dashboards feature (see
``docs/plans/SQL_LAB_DASHBOARDS.md``). Provides:

- a **datasource registry** (name -> connection URL) resolved to a
  safe, credential-free listing for the UI;
- a **read-only guard** that rejects DML/DDL and multi-statement input
  before anything touches the engine;
- **query execution** through the ematix engine (``Connection.query``
  -> ``Backend::read_arrow_stream``) with a row cap, returning a
  JSON-safe ``{columns, rows, stats}`` payload.

Read-only enforcement, the row cap, and (later) statement timeouts are
applied here, in front of the engine — never trust the client SQL.
"""
from __future__ import annotations

import operator
import os
import re
import threading
import time
from dataclasses import dataclass
from typing import Any

# Default cap on rows returned to the browser. Large result sets are
# truncated (``stats.truncated == True``); pushing an explicit LIMIT
# down into the engine is a Phase 1 follow-up.
DEFAULT_MAX_ROWS = 10_000
MAX_ROWS_CEILING = 100_000

# Statement kinds permitted through the ad-hoc editor. Everything else
# (INSERT/UPDATE/DELETE/DROP/CREATE/ALTER/TRUNCATE/MERGE/GRANT/ATTACH/…)
# is rejected before reaching the engine.
_READONLY_LEADING_KEYWORDS = frozenset({"SELECT", "WITH", "EXPLAIN"})

_LINE_COMMENT = re.compile(r"--[^\n]*")
_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
_LEADING_WORD = re.compile(r"[A-Za-z]+")

# Engine functions that read the filesystem or the network — the real
# residual hole once only SELECT is allowed: e.g. DuckDB
# `SELECT * FROM read_csv('/etc/passwd')` or `read_parquet('s3://…')`.
# Blocking these closes arbitrary file-read and SSRF from inside an
# otherwise-valid SELECT. Matched as function calls (`name(`) so a
# column literally named `glob` isn't a false positive.
_BLOCKED_FUNCTIONS = (
    "read_csv", "read_csv_auto", "read_parquet", "parquet_scan",
    "read_json", "read_json_auto", "read_json_objects", "read_ndjson",
    "read_ndjson_auto", "read_ndjson_objects", "read_text", "read_blob",
    "read_xlsx", "csv_scan", "sniff_csv", "glob", "load_extension",
    "st_read", "delta_scan", "iceberg_scan",
    "postgres_scan", "postgres_scan_pushdown", "mysql_scan", "sqlite_scan",
)
# Keywords that open external databases / load extensions. These are
# statement-leading (already blocked by the leading-keyword + single-
# statement checks), but denied here too as defense in depth (e.g.
# `EXPLAIN ATTACH …`).
_BLOCKED_KEYWORDS = ("attach", "install")

_DANGEROUS_SQL = re.compile(
    "|".join(
        [rf"\b{re.escape(f)}\s*\(" for f in _BLOCKED_FUNCTIONS]
        + [rf"\b{re.escape(k)}\b" for k in _BLOCKED_KEYWORDS]
    ),
    re.IGNORECASE,
)

# Wall-clock ceiling for a single query, overridable per deployment.
DEFAULT_QUERY_TIMEOUT_S = 30.0

# Result cache (used for repeated dashboard-tile queries so a refresh
# doesn't re-run every chart). Keyed on (url, sql, cap); TTL from
# ``EMATIX_FLOW_CACHE_TTL_S`` (default 0 = disabled). Ad-hoc editor
# queries are never cached (always fresh).
_RESULT_CACHE: dict[tuple, tuple[float, dict]] = {}
_CACHE_LOCK = threading.Lock()


def _cache_ttl() -> float:
    try:
        return float(os.environ.get("EMATIX_FLOW_CACHE_TTL_S", "0"))
    except (TypeError, ValueError):
        return 0.0


def _cache_get(key: tuple) -> dict | None:
    with _CACHE_LOCK:
        entry = _RESULT_CACHE.get(key)
        if entry is None:
            return None
        expiry, value = entry
        if expiry < time.monotonic():
            _RESULT_CACHE.pop(key, None)
            return None
        return value


def _cache_put(key: tuple, value: dict, ttl: float) -> None:
    with _CACHE_LOCK:
        _RESULT_CACHE[key] = (time.monotonic() + ttl, value)


def clear_result_cache() -> None:
    with _CACHE_LOCK:
        _RESULT_CACHE.clear()


class QueryError(ValueError):
    """Raised for a rejected or malformed query (maps to HTTP 400)."""


class QueryTimeout(QueryError):
    """Raised when a query exceeds the wall-clock limit (HTTP 504).

    Subclasses :class:`QueryError` so existing handlers degrade safely;
    the ``/api/query`` endpoint maps it to 504 specifically.
    """


class DatasourceNotFound(KeyError):
    """Raised when a requested datasource id is not registered (HTTP 404)."""


# ---- Datasource registry -------------------------------------------


@dataclass(frozen=True)
class Datasource:
    id: str
    url: str

    @property
    def dialect(self) -> str:
        return dialect_from_url(self.url)

    def public_dict(self) -> dict[str, str]:
        """Listing shape for the UI. Deliberately omits ``url`` so
        connection strings (which may embed credentials) never reach
        the browser."""
        return {"id": self.id, "dialect": self.dialect}


def dialect_from_url(url: str) -> str:
    """Best-effort dialect label from a connection URL scheme."""
    scheme = url.split("://", 1)[0].lower() if "://" in url else url.lower()
    if scheme in ("postgres", "postgresql"):
        return "postgres"
    if scheme.startswith("mysql"):
        return "mysql"
    if scheme == "sqlite" or url == ":memory:":
        return "sqlite"
    if scheme == "duckdb":
        return "duckdb"
    return scheme or "unknown"


class DatasourceRegistry:
    """Maps datasource ids to connection URLs.

    Phase 0 takes a plain ``{id: url}`` dict (typically from server
    config). Phase 1 adds the built-in in-process ematix engine entry
    and object-store sources.
    """

    def __init__(self, datasources: dict[str, str] | None = None):
        self._by_id: dict[str, Datasource] = {
            id_: Datasource(id=id_, url=url)
            for id_, url in (datasources or {}).items()
        }

    def list(self) -> list[Datasource]:
        return list(self._by_id.values())

    def get(self, datasource_id: str) -> Datasource:
        try:
            return self._by_id[datasource_id]
        except KeyError as exc:
            raise DatasourceNotFound(datasource_id) from exc


# ---- Read-only guard -----------------------------------------------


def _strip_comments(sql: str) -> str:
    return _LINE_COMMENT.sub(" ", _BLOCK_COMMENT.sub(" ", sql))


def guard_readonly(sql: str) -> str:
    """Validate that ``sql`` is a single read-only statement.

    Returns the cleaned (comment-stripped, semicolon-trimmed) SQL, or
    raises :class:`QueryError`. This is a defensive syntactic guard,
    not a full parser: it blocks the obvious write/DDL paths and
    multi-statement injection. Stronger AST-level validation is a
    follow-up.
    """
    if sql is None:
        raise QueryError("query is required")
    stripped = _strip_comments(sql).strip()
    if not stripped:
        raise QueryError("query is empty")

    # Trim a single trailing statement terminator, then reject any
    # remaining semicolon (multi-statement).
    core = stripped.rstrip(";").rstrip()
    if not core:
        raise QueryError("query is empty")
    if ";" in core:
        raise QueryError("multiple statements are not allowed; run one query at a time")

    match = _LEADING_WORD.match(core)
    keyword = match.group(0).upper() if match else ""
    if keyword not in _READONLY_LEADING_KEYWORDS:
        raise QueryError(
            f"only read-only queries are allowed (SELECT / WITH / EXPLAIN); "
            f"got {keyword or 'an unrecognized statement'!s}"
        )

    # A valid SELECT can still reach the filesystem/network via engine
    # table functions (read_csv, read_parquet, *_scan, …) or open other
    # databases (ATTACH). Reject those.
    hit = _DANGEROUS_SQL.search(core)
    if hit is not None:
        token = hit.group(0).rstrip("( ").strip()
        raise QueryError(
            f"'{token}' is not permitted here — filesystem / network / "
            "cross-database access is blocked for security"
        )
    return core


# ---- Row cap -------------------------------------------------------


def _clamp_max_rows(max_rows: int | None) -> int:
    if max_rows is None:
        return DEFAULT_MAX_ROWS
    try:
        n = int(max_rows)
    except (TypeError, ValueError) as exc:
        raise QueryError("max_rows must be an integer") from exc
    if n <= 0:
        raise QueryError("max_rows must be positive")
    return min(n, MAX_ROWS_CEILING)


def _query_timeout_s() -> float:
    try:
        return float(os.environ.get("EMATIX_FLOW_QUERY_TIMEOUT_S", DEFAULT_QUERY_TIMEOUT_S))
    except (TypeError, ValueError):
        return DEFAULT_QUERY_TIMEOUT_S


def _run_with_timeout(fn, timeout_s: float):
    """Run ``fn`` on a daemon thread and give up after ``timeout_s``.

    Bounds how long the request handler blocks. On timeout the native
    query keeps running on the (daemon) thread until it finishes — true
    mid-query cancellation is backend-specific (sqlite ``interrupt()`` /
    Postgres ``statement_timeout``) and is a follow-up — but the handler
    returns promptly with :class:`QueryTimeout`.
    """
    if timeout_s <= 0:
        return fn()
    box: dict[str, Any] = {}

    def target() -> None:
        try:
            box["ok"] = fn()
        except BaseException as exc:  # noqa: BLE001 - relayed to caller
            box["err"] = exc

    thread = threading.Thread(target=target, name="ematix-query", daemon=True)
    thread.start()
    thread.join(timeout_s)
    if thread.is_alive():
        raise QueryTimeout(f"query exceeded the {timeout_s:g}s time limit")
    if "err" in box:
        raise box["err"]
    return box["ok"]


def run_query(
    url: str, sql: str, max_rows: int | None = None, use_cache: bool = False
) -> dict[str, Any]:
    """Execute a validated read-only query and return a JSON payload.

    ``sql`` must already be the cleaned string from :func:`guard_readonly`.

    The engine (``Connection.query``) converts the Arrow result to native
    Python objects in Rust and returns ``{columns, rows, truncated}``
    directly — no pyarrow / pandas in the server process. That keeps a
    second mimalloc (pyarrow bundles its own; ``_core`` installs one as
    Rust's global allocator) out of the process, which is what caused the
    native segfaults, and keeps the API path lean.

    The engine call runs under a wall-clock timeout
    (``EMATIX_FLOW_QUERY_TIMEOUT_S``, default 30s). When ``use_cache`` and
    a TTL is configured, an identical recent result is served from cache
    (``stats.cached == True``).
    """
    cap = _clamp_max_rows(max_rows)
    ttl = _cache_ttl() if use_cache else 0.0
    cache_key = (url, sql, cap)
    if ttl > 0:
        hit = _cache_get(cache_key)
        if hit is not None:
            return {**hit, "stats": {**hit["stats"], "cached": True}}

    started = time.perf_counter()

    def _call():
        from ematix_flow import _core

        return _core.connect(url).query(sql, cap)

    try:
        result = _run_with_timeout(_call, _query_timeout_s())
    except QueryError:
        raise
    except Exception as exc:  # engine/connection error -> 400 for the caller
        raise QueryError(str(exc)) from exc
    elapsed_ms = int((time.perf_counter() - started) * 1000)

    rows = result["rows"]
    payload = {
        "columns": result["columns"],
        "rows": rows,
        "stats": {
            "row_count": len(rows),
            "truncated": result["truncated"],
            "elapsed_ms": elapsed_ms,
            "cached": False,
        },
    }
    if ttl > 0:
        _cache_put(cache_key, payload, ttl)
    return payload


def execute_query_request(
    registry: DatasourceRegistry,
    datasource_id: str,
    sql: str,
    max_rows: int | None = None,
    use_cache: bool = False,
) -> dict[str, Any]:
    """Full path for one ``POST /api/query``: resolve datasource,
    guard the SQL, execute. Raises :class:`DatasourceNotFound` (404) or
    :class:`QueryError` (400)."""
    datasource = registry.get(datasource_id)  # may raise DatasourceNotFound
    cleaned = guard_readonly(sql)  # may raise QueryError
    return run_query(datasource.url, cleaned, max_rows, use_cache=use_cache)


# ---- Catalog / schema browsing -------------------------------------
#
# Introspection runs internal, fixed queries through the same engine
# path (`run_query`, which does not apply the read-only guard — these
# SELECTs are ours, not client input). Schema / table names taken from
# the request path are interpolated as SQL string *literals*, so they
# are escaped via :func:`_sql_literal` to prevent injection.

# System schemas hidden from the browser across the SQL-standard dialects.
_SYSTEM_SCHEMAS = (
    "information_schema",
    "pg_catalog",
    "pg_toast",
    "sys",
    "mysql",
    "performance_schema",
)


def _sql_literal(value: str) -> str:
    """Quote a string as a SQL literal, escaping embedded quotes."""
    return "'" + value.replace("'", "''") + "'"


def list_schemas(url: str, dialect: str) -> list[str]:
    if dialect == "sqlite":
        return ["main"]
    exclusions = ", ".join(_sql_literal(s) for s in _SYSTEM_SCHEMAS)
    sql = (
        "SELECT schema_name FROM information_schema.schemata "
        f"WHERE schema_name NOT IN ({exclusions}) ORDER BY schema_name"
    )
    return [row[0] for row in run_query(url, sql)["rows"]]


def list_tables(url: str, dialect: str, schema: str) -> list[dict[str, str]]:
    if dialect == "sqlite":
        sql = (
            "SELECT name, type FROM sqlite_master "
            "WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' "
            "ORDER BY name"
        )
    else:
        sql = (
            "SELECT table_name, table_type FROM information_schema.tables "
            f"WHERE table_schema = {_sql_literal(schema)} ORDER BY table_name"
        )
    out = []
    for row in run_query(url, sql)["rows"]:
        kind = "view" if str(row[1] or "").upper().find("VIEW") >= 0 else "table"
        out.append({"name": row[0], "kind": kind})
    return out


def list_columns(
    url: str, dialect: str, schema: str, table: str
) -> list[dict[str, Any]]:
    if dialect == "sqlite":
        sql = (
            'SELECT name, type, "notnull" '
            f"FROM pragma_table_info({_sql_literal(table)}) ORDER BY cid"
        )
        return [
            {
                "name": row[0],
                "type": row[1] or "",
                "nullable": int(row[2]) == 0,
            }
            for row in run_query(url, sql)["rows"]
        ]
    sql = (
        "SELECT column_name, data_type, is_nullable "
        "FROM information_schema.columns "
        f"WHERE table_schema = {_sql_literal(schema)} "
        f"AND table_name = {_sql_literal(table)} ORDER BY ordinal_position"
    )
    return [
        {
            "name": row[0],
            "type": row[1],
            "nullable": str(row[2]).upper() == "YES",
        }
        for row in run_query(url, sql)["rows"]
    ]


def catalog_schemas(registry: DatasourceRegistry, datasource_id: str) -> list[str]:
    ds = registry.get(datasource_id)
    return list_schemas(ds.url, ds.dialect)


def catalog_tables(
    registry: DatasourceRegistry, datasource_id: str, schema: str
) -> list[dict[str, str]]:
    ds = registry.get(datasource_id)
    return list_tables(ds.url, ds.dialect, schema)


def catalog_columns(
    registry: DatasourceRegistry, datasource_id: str, schema: str, table: str
) -> list[dict[str, Any]]:
    ds = registry.get(datasource_id)
    return list_columns(ds.url, ds.dialect, schema, table)


# ---- Dashboard filters ---------------------------------------------
#
# Filters are applied on a chart's OUTPUT columns by wrapping its query
# in a subquery: `SELECT * FROM (<chart_sql>) _sub WHERE <preds>`. This
# works for any SELECT (including GROUP BY / window queries) and only
# includes predicates for columns the chart actually produces, so a
# dashboard-wide filter naturally applies to the charts that share that
# dimension and is ignored by the ones that don't. Cross-filtering (a
# click on a bar) emits the same {column, values} shape.


def _sql_value(value: Any) -> str:
    """Quote a filter value as a SQL literal."""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


def build_filtered_sql(
    cleaned_sql: str,
    filters: list[dict[str, Any]],
    available_columns: list[str],
) -> str | None:
    """Wrap ``cleaned_sql`` so the applicable ``filters`` restrict its
    output. Returns ``None`` when no filter applies (leave the query
    unchanged). ``cleaned_sql`` must already be a guarded SELECT."""
    have = {c.lower() for c in available_columns}
    clauses: list[str] = []
    for f in filters or []:
        col = f.get("column")
        values = f.get("values") or []
        if not col or str(col).lower() not in have or not values:
            continue
        ident = '"' + str(col).replace('"', '""') + '"'
        lits = ", ".join(_sql_value(v) for v in values)
        clauses.append(f"{ident} IN ({lits})")
    if not clauses:
        return None
    return (
        f"SELECT * FROM (\n{cleaned_sql}\n) AS _emat_sub WHERE "
        + " AND ".join(clauses)
    )


def execute_chart_for_dashboard(
    registry: DatasourceRegistry,
    chart: dict[str, Any],
    filters: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Run a chart's query for a dashboard tile, applying dashboard
    filters where the chart's output has the filtered column. Raises
    :class:`DatasourceNotFound` / :class:`QueryError`."""
    data = execute_query_request(
        registry, chart["datasource_id"], chart["sql"], use_cache=True
    )
    if filters:
        colnames = [c["name"] for c in data["columns"]]
        cleaned = guard_readonly(chart["sql"])
        wrapped = build_filtered_sql(cleaned, filters, colnames)
        if wrapped is not None:
            data = execute_query_request(
                registry, chart["datasource_id"], wrapped, use_cache=True
            )
    return data


# ---- Alerts --------------------------------------------------------

_ALERT_OPS = {
    ">": operator.gt,
    "<": operator.lt,
    ">=": operator.ge,
    "<=": operator.le,
    "==": operator.eq,
    "!=": operator.ne,
}


def evaluate_alert(
    registry: DatasourceRegistry,
    chart: dict[str, Any],
    column: str,
    op: str,
    threshold: float,
) -> dict[str, Any]:
    """Run ``chart`` and test its ``column`` against ``op threshold``.
    Triggers if any row satisfies the condition. Raises
    :class:`QueryError` for a bad operator / missing column."""
    cmp = _ALERT_OPS.get(op)
    if cmp is None:
        raise QueryError(f"unsupported operator {op!r} (use one of {', '.join(_ALERT_OPS)})")
    data = execute_query_request(registry, chart["datasource_id"], chart["sql"])
    col_idx = next(
        (i for i, c in enumerate(data["columns"]) if c["name"] == column), None
    )
    if col_idx is None:
        raise QueryError(f"column {column!r} is not in the chart's output")
    matched = [
        row[col_idx]
        for row in data["rows"]
        if isinstance(row[col_idx], (int, float)) and cmp(row[col_idx], threshold)
    ]
    return {
        "triggered": bool(matched),
        "matched_values": matched,
        "rows_evaluated": len(data["rows"]),
    }
