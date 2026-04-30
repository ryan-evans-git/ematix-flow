"""`Source` factory functions.

Phase 5 covers `Source.postgres_query(conn, query)`; Phase 9 adds
`Source.postgres_table(...)` sugar.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Source:
    """A source of rows for a Pipeline.

    `connection` is an `_core.Connection`; `query` is an arbitrary SELECT
    that produces the columns the target declares (matching by name).
    """

    connection: Any
    query: str

    @classmethod
    def postgres_query(cls, connection: Any, query: str) -> Source:
        return cls(connection=connection, query=query)

    @classmethod
    def postgres_table(
        cls,
        connection: Any,
        schema: str,
        table: str,
        columns: Iterable[str] | None = None,
    ) -> Source:
        """Sugar over `postgres_query` for a plain `SELECT ... FROM s.t`.

        With `columns=None` we emit `SELECT * FROM schema.table`; the
        Rust-side outer SELECT projects to the target's user columns
        anyway, but explicit columns are useful when the source table is
        wide and you want projection to push down at parse time.
        """
        if columns is None:
            query = f"SELECT * FROM {schema}.{table}"
        else:
            cols = ", ".join(columns)
            query = f"SELECT {cols} FROM {schema}.{table}"
        return cls(connection=connection, query=query)
