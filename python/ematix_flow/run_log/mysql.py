"""MySQLRunLog — MySQL / MariaDB backend.

Use this when your stack already has MySQL provisioned. Schema mirrors
the Postgres backend (two tables, upsert semantics) but uses MySQL's
`INSERT ... ON DUPLICATE KEY UPDATE` instead of the Postgres
`ON CONFLICT` clause.

The MySQL "database" already provides namespace isolation (you connect
to a specific one), so this backend takes a `table_prefix` instead of
a Postgres-style schema name. Pass `table_prefix="orchestrator_"` to
isolate state from other tables when sharing a database.

Optional dep: `PyMySQL` (pure-Python; install via `pip install PyMySQL`).
"""

from __future__ import annotations

from datetime import datetime
from urllib.parse import urlparse

from ._iso import iso_utc, parse_iso


class MySQLRunLog:
    """MySQL (and MariaDB) backend.

    Args:
        url: Connection URL of the form
            `mysql://user:password@host:port/database`. Either this or
            `connect_kwargs=` must be provided.
        connect_kwargs: dict of kwargs passed directly to
            `pymysql.connect(...)`. Use when you need finer control
            (SSL options, charset, unix_socket, etc.) than the URL
            shorthand allows.
        table_prefix: prepended to the two table names. Default "" =
            "run_log" + "attempt_state". Set to e.g.
            "orchestrator_" to coexist with application tables.
        create_tables: when True (default), CREATE TABLE IF NOT EXISTS
            is run on connect. Set False if the role lacks DDL
            privilege; an operator must pre-create the tables.

    Permission notes:
      - `create_tables=True` needs CREATE on the database. After first
        start, the role only needs SELECT/INSERT/UPDATE/DELETE on the
        two tables.
      - Unlike Postgres, MySQL has no schema-level namespace within a
        database; isolation between environments (prod/staging/...)
        needs separate databases OR distinct `table_prefix` values.
    """

    def __init__(
        self,
        url: str | None = None,
        *,
        connect_kwargs: dict | None = None,
        table_prefix: str = "",
        create_tables: bool = True,
    ):
        try:
            import pymysql
        except ImportError as e:
            raise ImportError(
                "MySQLRunLog requires PyMySQL. Install with "
                "`pip install PyMySQL`."
            ) from e

        if url is None and connect_kwargs is None:
            raise ValueError(
                "MySQLRunLog needs either url= or connect_kwargs=."
            )
        if url is not None and connect_kwargs is not None:
            raise ValueError("pass url= OR connect_kwargs=, not both.")

        if url is not None:
            kwargs = _parse_mysql_url(url)
        else:
            kwargs = dict(connect_kwargs or {})

        kwargs.setdefault("autocommit", True)
        self._conn = pymysql.connect(**kwargs)
        self._prefix = table_prefix
        self._run_log_table = f"{table_prefix}run_log"
        self._attempt_table = f"{table_prefix}attempt_state"

        if create_tables:
            with self._conn.cursor() as cur:
                cur.execute(
                    f"CREATE TABLE IF NOT EXISTS `{self._run_log_table}` ("
                    "  pipeline_name VARCHAR(255) NOT NULL PRIMARY KEY,"
                    "  last_run_at   VARCHAR(64)  NOT NULL,"
                    "  success       TINYINT(1)   NOT NULL"
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
                )
                cur.execute(
                    f"CREATE TABLE IF NOT EXISTS `{self._attempt_table}` ("
                    "  pipeline_name   VARCHAR(255) NOT NULL PRIMARY KEY,"
                    "  attempt_count   INT          NOT NULL,"
                    "  last_attempt_at VARCHAR(64)  NOT NULL,"
                    "  gave_up         TINYINT(1)   NOT NULL"
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
                )

    def close(self) -> None:
        self._conn.close()

    # ---- writes --------------------------------------------------------

    def record_run(self, name: str, ts: datetime, success: bool) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO `{self._run_log_table}` "
                "(pipeline_name, last_run_at, success) VALUES (%s, %s, %s) "
                "ON DUPLICATE KEY UPDATE "
                "last_run_at = VALUES(last_run_at), "
                "success     = VALUES(success)",
                (name, iso_utc(ts), 1 if success else 0),
            )

    def record_attempt(self, name: str, state) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO `{self._attempt_table}` "
                "(pipeline_name, attempt_count, last_attempt_at, gave_up) "
                "VALUES (%s, %s, %s, %s) "
                "ON DUPLICATE KEY UPDATE "
                "attempt_count   = VALUES(attempt_count), "
                "last_attempt_at = VALUES(last_attempt_at), "
                "gave_up         = VALUES(gave_up)",
                (
                    name,
                    state.attempt_count,
                    iso_utc(state.last_attempt_at),
                    1 if state.gave_up else 0,
                ),
            )

    def clear_attempt_state(self, name: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM `{self._attempt_table}` WHERE pipeline_name = %s",
                (name,),
            )

    # ---- restore -------------------------------------------------------

    def restore_into_process(self) -> None:
        from ematix_flow import pipeline as _p

        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT pipeline_name, last_run_at, success "
                f"FROM `{self._run_log_table}`"
            )
            for name, ts_s, ok in cur.fetchall():
                _p._LAST_RUN[name] = (parse_iso(ts_s), bool(ok))
            cur.execute(
                f"SELECT pipeline_name, attempt_count, last_attempt_at, gave_up "
                f"FROM `{self._attempt_table}`"
            )
            for name, count, ts_s, gave_up in cur.fetchall():
                _p._ATTEMPT_STATE[name] = _p.AttemptState(
                    attempt_count=count,
                    last_attempt_at=parse_iso(ts_s),
                    gave_up=bool(gave_up),
                )


def _parse_mysql_url(url: str) -> dict:
    """Crack a `mysql://user:pass@host:port/db?param=value` URL into
    the kwargs `pymysql.connect` expects."""
    u = urlparse(url)
    if u.scheme not in ("mysql", "mariadb"):
        raise ValueError(
            f"MySQLRunLog url must use scheme mysql:// or mariadb://, got {u.scheme!r}"
        )
    out: dict = {}
    if u.hostname:
        out["host"] = u.hostname
    if u.port:
        out["port"] = u.port
    if u.username:
        out["user"] = u.username
    if u.password:
        out["password"] = u.password
    if u.path and u.path != "/":
        out["database"] = u.path.lstrip("/")
    return out
