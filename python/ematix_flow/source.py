"""`Source` factory functions.

Phase 5 covers `Source.postgres_query(conn, query)`. Phase 9 will add
`Source.postgres_table(...)` sugar and projection helpers.
"""

from __future__ import annotations

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
