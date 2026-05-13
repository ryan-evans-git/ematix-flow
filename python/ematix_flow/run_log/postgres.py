"""PostgresRunLog — multi-host backend backed by PostgreSQL.

Use this when more than one host runs `flow run-due` and they need to
agree on the freshness gate and retry state. SQLite can't be safely
shared across hosts; Postgres is the right tool.

Schema mirrors SqliteRunLog's two tables. Conflict handling uses
`INSERT ... ON CONFLICT (...) DO UPDATE` so the call sites stay the
same as SQLite's REPLACE INTO.

Optional dep: `psycopg` (psycopg 3, install via `pip install psycopg[binary]`).
"""

from __future__ import annotations

from datetime import datetime

from ._iso import iso_utc, parse_iso


class PostgresRunLog:
    """PostgreSQL-backed run history.

    `dsn` is a libpq-style connection string ("postgresql://user@host/db",
    "host=... dbname=...", or a service name). All four backends accept
    a single string for the location; for Postgres that string is the DSN.

    `schema` controls which Postgres schema the tables live in
    (default "public"). Useful for keeping orchestrator state out of
    your application data namespace.
    """

    _DDL = (
        # The schema is created first so a non-default schema name works
        # without requiring the operator to pre-create it. The role used
        # in `dsn` needs CREATE privilege on the database for this; if
        # not, see `create_tables=False` below.
        'CREATE SCHEMA IF NOT EXISTS "{schema}";'
        'CREATE TABLE IF NOT EXISTS "{schema}".run_log ('
        "  pipeline_name TEXT PRIMARY KEY,"
        "  last_run_at   TEXT NOT NULL,"
        "  success       BOOLEAN NOT NULL"
        ");"
        'CREATE TABLE IF NOT EXISTS "{schema}".attempt_state ('
        "  pipeline_name   TEXT PRIMARY KEY,"
        "  attempt_count   INTEGER NOT NULL,"
        "  last_attempt_at TEXT NOT NULL,"
        "  gave_up         BOOLEAN NOT NULL"
        ");"
    )

    def __init__(
        self,
        dsn: str,
        *,
        schema: str = "public",
        create_tables: bool = True,
    ):
        """Connect to Postgres and (optionally) create the schema + tables.

        Args:
            dsn: libpq connection string (postgresql://user@host/db, etc.).
            schema: namespace for the two orchestrator tables. Default
                "public", which always exists. Custom schemas are
                auto-created with `CREATE SCHEMA IF NOT EXISTS` — this
                needs CREATE-on-database privilege.
            create_tables: when True (default), the schema + two tables
                are created on first connect via `IF NOT EXISTS` DDL.
                Set to False if your role lacks DDL privilege; an
                operator (DBA, migration script) must have already
                created them with the matching layout.

        Permission notes:
          - To use `create_tables=True` with `schema="public"`:
              GRANT USAGE, CREATE ON SCHEMA public TO <role>.
          - To use a custom schema: that schema must either already
            exist (with USAGE granted) OR the role must have CREATE
            ON DATABASE.
          - After first start, only INSERT/UPDATE/DELETE/SELECT on the
            two tables are needed; the role can be downgraded.
        """
        try:
            import psycopg
        except ImportError as e:
            raise ImportError(
                "PostgresRunLog requires psycopg. Install with "
                "`pip install psycopg[binary]`."
            ) from e

        # autocommit=True keeps the semantics aligned with SQLite's
        # isolation_level=None — each statement is its own transaction,
        # mirroring the in-memory dict-assignment shape.
        self._conn = psycopg.connect(dsn, autocommit=True)
        self._schema = schema
        if create_tables:
            with self._conn.cursor() as cur:
                cur.execute(self._DDL.format(schema=schema))

    def close(self) -> None:
        self._conn.close()

    def record_run(self, name: str, ts: datetime, success: bool) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                f'INSERT INTO "{self._schema}".run_log '
                "(pipeline_name, last_run_at, success) VALUES (%s, %s, %s) "
                "ON CONFLICT (pipeline_name) DO UPDATE SET "
                "last_run_at = EXCLUDED.last_run_at, "
                "success     = EXCLUDED.success",
                (name, iso_utc(ts), success),
            )

    def record_attempt(self, name: str, state) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                f'INSERT INTO "{self._schema}".attempt_state '
                "(pipeline_name, attempt_count, last_attempt_at, gave_up) "
                "VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (pipeline_name) DO UPDATE SET "
                "attempt_count   = EXCLUDED.attempt_count, "
                "last_attempt_at = EXCLUDED.last_attempt_at, "
                "gave_up         = EXCLUDED.gave_up",
                (
                    name,
                    state.attempt_count,
                    iso_utc(state.last_attempt_at),
                    state.gave_up,
                ),
            )

    def clear_attempt_state(self, name: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                f'DELETE FROM "{self._schema}".attempt_state WHERE pipeline_name = %s',
                (name,),
            )

    def restore_into_process(self) -> None:
        from ematix_flow import pipeline as _p

        with self._conn.cursor() as cur:
            cur.execute(
                f'SELECT pipeline_name, last_run_at, success FROM "{self._schema}".run_log'
            )
            for name, ts_s, ok in cur.fetchall():
                _p._LAST_RUN[name] = (parse_iso(ts_s), bool(ok))
            cur.execute(
                f'SELECT pipeline_name, attempt_count, last_attempt_at, gave_up '
                f'FROM "{self._schema}".attempt_state'
            )
            for name, count, ts_s, gave_up in cur.fetchall():
                _p._ATTEMPT_STATE[name] = _p.AttemptState(
                    attempt_count=count,
                    last_attempt_at=parse_iso(ts_s),
                    gave_up=bool(gave_up),
                )
