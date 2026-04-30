"""Phase 17/18: FeatureView base class with point-in-time + asof-join helpers.

`FeatureView` is a `ManagedTable` subclass whose `@ematix.feature_view`
decoration attaches ML dunders. This module adds the query-time
classmethods:

  Cls.point_in_time(conn, entity_keys, as_of) -> dict | None
  Cls.historical_features(conn, spine, columns=None) -> list[dict]

The SQL is the standard SCD2 PIT pattern:

  SELECT cols FROM schema.table
  WHERE entity_keys MATCH
    AND valid_from <= as_of
    AND (valid_to IS NULL OR valid_to > as_of)
  ORDER BY valid_from DESC
  LIMIT 1

For `historical_features`, a single LATERAL JOIN against a VALUES spine
gives the standard "asof join" idiom — one query, one round trip.

Requires `pip install ematix-flow[df]` for psycopg2 (DictCursor support).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, ClassVar

from ematix_flow.table import ManagedTable

# Metadata columns that should never be in the user-visible feature set.
_META_COLS = ("_loaded_at", "_batch_id", "valid_from", "valid_to", "is_current", "row_hash")


def _require_psycopg2():
    try:
        import psycopg2  # noqa: F401
        from psycopg2.extras import RealDictCursor  # noqa: F401
    except ImportError as e:
        raise ImportError(
            "FeatureView point_in_time / historical_features require psycopg2; "
            "install with `pip install ematix-flow[df]`"
        ) from e
    import psycopg2

    return psycopg2


def _validate_entity_keys(cls, entity_keys: dict[str, Any]) -> list[str]:
    declared = cls._primary_keys()
    if not declared:
        raise TypeError(
            f"{cls.__name__}.point_in_time requires the class to declare "
            "primary-key entity columns"
        )
    for k in declared:
        if k not in entity_keys:
            raise ValueError(
                f"point_in_time entity_keys missing required key {k!r}; "
                f"declared keys: {declared}"
            )
    for k in entity_keys:
        if k not in declared:
            raise ValueError(
                f"point_in_time entity_keys carries {k!r}, which is not a "
                f"declared entity (primary key) on {cls.__name__}; "
                f"declared keys: {declared}"
            )
    return declared


def _resolve_columns(cls, columns: list[str] | None) -> list[str]:
    if columns is not None:
        return list(columns)
    return [n for n, _col in cls._columns() if n not in _META_COLS]


class FeatureView(ManagedTable):
    """Base class for `@ematix.feature_view`-decorated tables.

    Provides PIT / asof-join classmethods on top of the standard
    `ManagedTable` schema-management surface.
    """

    __abstract__: ClassVar[bool] = True
    __is_feature_view__: ClassVar[bool] = True

    @classmethod
    def point_in_time(
        cls,
        conn: Any,
        *,
        entity_keys: dict[str, Any],
        as_of: datetime,
        columns: list[str] | None = None,
    ) -> dict[str, Any] | None:
        """Return the row valid at `as_of` for the given entity_keys, or None."""
        keys = _validate_entity_keys(cls, entity_keys)
        cols = _resolve_columns(cls, columns)

        psycopg2 = _require_psycopg2()
        from psycopg2.extras import RealDictCursor

        cols_sql = ", ".join(f'"{c}"' for c in cols)
        where_keys = " AND ".join(f'"{k}" = %s' for k in keys)
        sql = (
            f"SELECT {cols_sql} "
            f'FROM "{cls.__schema__}"."{cls.__tablename__}" '
            f"WHERE {where_keys} "
            f"  AND valid_from <= %s "
            f"  AND (valid_to IS NULL OR valid_to > %s) "
            f"ORDER BY valid_from DESC "
            f"LIMIT 1"
        )
        params = [entity_keys[k] for k in keys] + [as_of, as_of]
        pg = psycopg2.connect(conn.dsn())
        try:
            with pg.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
        finally:
            pg.close()
        return dict(row) if row else None

    @classmethod
    def online_features(
        cls,
        conn: Any,
        *,
        entity_keys: dict[str, Any],
        columns: list[str] | None = None,
    ) -> dict[str, Any] | None:
        """Phase 19: query the `<schema>.<table>__online` materialized view
        for sub-millisecond inference reads against the current versions.

        Requires the FeatureView to have been declared with `online=True`
        and synced at least once (the MV is created/refreshed at sync time).
        """
        if not getattr(cls, "__online__", False):
            raise RuntimeError(
                f"{cls.__name__}.online_features requires online=True on the "
                "@ematix.feature_view decoration"
            )
        keys = _validate_entity_keys(cls, entity_keys)
        cols = _resolve_columns(cls, columns)

        psycopg2 = _require_psycopg2()
        from psycopg2.extras import RealDictCursor

        cols_sql = ", ".join(f'"{c}"' for c in cols)
        where_keys = " AND ".join(f'"{k}" = %s' for k in keys)
        sql = (
            f"SELECT {cols_sql} "
            f'FROM "{cls.__schema__}"."{cls.__tablename__}__online" '
            f"WHERE {where_keys} "
            f"LIMIT 1"
        )
        params = [entity_keys[k] for k in keys]
        pg = psycopg2.connect(conn.dsn())
        try:
            with pg.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
        finally:
            pg.close()
        return dict(row) if row else None

    @classmethod
    def historical_features(
        cls,
        conn: Any,
        *,
        spine: list[dict[str, Any]],
        columns: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Asof-join the FeatureView against a spine of (entity_keys, as_of)
        records. Returns one dict per spine entry, with feature columns
        populated to the version valid at that entry's `as_of` (or NULL).

        Each spine entry must include every entity-key column plus an
        `as_of` (datetime). Missing as_of raises ValueError.
        """
        keys = cls._primary_keys()
        cols = _resolve_columns(cls, columns)
        if not keys:
            raise TypeError(
                f"{cls.__name__}.historical_features requires the class to "
                "declare primary-key entity columns"
            )
        if not spine:
            return []
        for i, e in enumerate(spine):
            for k in keys:
                if k not in e:
                    raise ValueError(
                        f"spine[{i}] missing entity key {k!r}; required keys: {keys}"
                    )
            if "as_of" not in e:
                raise ValueError(f"spine[{i}] missing 'as_of' (datetime)")

        psycopg2 = _require_psycopg2()
        from psycopg2.extras import RealDictCursor

        # Build a VALUES-shaped spine table. A leading "_s_idx" preserves
        # caller-supplied order so the result rows align 1:1 with `spine`.
        spine_cols = keys + ["as_of"]
        all_cols = ["_s_idx"] + spine_cols
        placeholders_per_row = "(" + ", ".join(["%s"] * len(all_cols)) + ")"
        values_sql = ", ".join([placeholders_per_row] * len(spine))
        spine_alias_cols = ", ".join(
            ['"_s_idx"'] + [f'"_s_{c}"' for c in spine_cols]
        )
        feat_cols = ", ".join(f'feat."{c}" AS "{c}"' for c in cols)

        where_join = " AND ".join(
            f'"{k}" = spine."_s_{k}"' for k in keys
        )
        sql = (
            f"SELECT {feat_cols} "
            f"FROM (VALUES {values_sql}) AS spine ({spine_alias_cols}) "
            f"LEFT JOIN LATERAL ("
            f'  SELECT {", ".join(chr(34) + c + chr(34) for c in cols)} '
            f'  FROM "{cls.__schema__}"."{cls.__tablename__}" '
            f"  WHERE {where_join} "
            f'    AND valid_from <= spine."_s_as_of" '
            f'    AND (valid_to IS NULL OR valid_to > spine."_s_as_of") '
            f"  ORDER BY valid_from DESC "
            f"  LIMIT 1"
            f") AS feat ON true "
            f'ORDER BY spine."_s_idx"'
        )
        params: list[Any] = []
        for i, e in enumerate(spine):
            params.append(i)
            for c in spine_cols:
                params.append(e[c])

        pg = psycopg2.connect(conn.dsn())
        try:
            with pg.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
        finally:
            pg.close()
        return [dict(r) for r in rows]


__all__ = ["FeatureView"]
