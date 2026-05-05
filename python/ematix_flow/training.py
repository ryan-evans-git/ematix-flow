"""Phase 20: training-dataset builder.

`ematix.training_set(conn, spine, feature_views, columns=None, prefer="auto")`
joins multiple FeatureViews against a shared spine via per-FV LATERAL
asof-joins and returns a polars (default) or pandas DataFrame.

The SQL pattern is one LATERAL JOIN per FV against a VALUES-shaped
spine table — single round trip. Column-name collisions across FVs
are disambiguated by prefixing with `<tablename>__<col>`.

Requires the `[df]` extra: psycopg2 plus polars or pandas.
"""

from __future__ import annotations

from typing import Any

# Metadata columns that should never be returned to the user as features.
_META_COLS = (
    "_loaded_at",
    "_batch_id",
    "valid_from",
    "valid_to",
    "is_current",
    "row_hash",
)


def _try_import(name: str):
    try:
        return __import__(name)
    except ImportError:
        return None


def _require_psycopg2():
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor  # noqa: F401
    except ImportError as e:
        raise ImportError(
            "ematix.training_set requires psycopg2; install with "
            "`pip install ematix-flow[df]`"
        ) from e
    import psycopg2

    return psycopg2


def _resolve_dataframe_lib(prefer: str):
    polars = _try_import("polars")
    pandas = _try_import("pandas")
    if prefer not in ("auto", "polars", "pandas"):
        raise ValueError(
            f"prefer must be 'auto', 'polars', or 'pandas'; got {prefer!r}"
        )
    if prefer == "polars" and polars is None:
        raise ImportError(
            "prefer='polars' but polars is not installed"
        )
    if prefer == "pandas" and pandas is None:
        raise ImportError(
            "prefer='pandas' but pandas is not installed"
        )
    if polars is None and pandas is None:
        raise ImportError(
            "training_set requires polars or pandas — install at least one"
        )
    if prefer == "auto":
        return ("polars", polars) if polars is not None else ("pandas", pandas)
    return (prefer, polars if prefer == "polars" else pandas)


def training_set(
    conn: Any,
    *,
    spine: list[dict[str, Any]],
    feature_views: list[type],
    columns: dict[type, list[str]] | None = None,
    prefer: str = "auto",
):
    """Build a training dataset by asof-joining feature_views against
    spine. Returns a polars / pandas DataFrame.
    """
    if not feature_views:
        raise ValueError("training_set requires at least one entry in feature_views")

    # Validate every entry is a FeatureView.
    for fv in feature_views:
        if not getattr(fv, "__is_feature_view__", False):
            raise TypeError(
                f"training_set: {fv.__name__} is not a FeatureView "
                "(decorate with @ematix.feature_view)"
            )

    chosen, lib = _resolve_dataframe_lib(prefer)

    if not spine:
        # Build an empty df with the spine columns + per-FV column names so
        # downstream code can union etc.
        result_cols = _resolve_result_columns(feature_views, columns, [])
        empty_rows: list[dict[str, Any]] = []
        if chosen == "polars":
            return lib.DataFrame(empty_rows, schema=result_cols)
        return lib.DataFrame(empty_rows, columns=result_cols)

    # Determine spine entity-key set + as_of presence.
    spine_keys = set(spine[0].keys())
    if "as_of" not in spine_keys:
        raise ValueError("spine entries must include an 'as_of' datetime")

    # Each FV's primary keys must be a subset of spine keys.
    for fv in feature_views:
        pks = fv._primary_keys()
        missing = [k for k in pks if k not in spine_keys]
        if missing:
            raise ValueError(
                f"FeatureView {fv.__name__} requires entity keys {missing} "
                f"that are not present in the spine (have: {sorted(spine_keys)})"
            )

    # Resolve final column lists per FV (default = all non-meta non-key cols).
    columns = columns or {}
    fv_columns: dict[type, list[str]] = {}
    for fv in feature_views:
        if fv in columns:
            fv_columns[fv] = list(columns[fv])
        else:
            pks = set(fv._primary_keys())
            fv_columns[fv] = [
                n for n, _col in fv._columns() if n not in _META_COLS and n not in pks
            ]

    # Detect collisions and prefix where needed.
    counts: dict[str, int] = {}
    for cols in fv_columns.values():
        for c in cols:
            counts[c] = counts.get(c, 0) + 1
    aliases: dict[type, dict[str, str]] = {}
    for fv, cols in fv_columns.items():
        aliases[fv] = {}
        for c in cols:
            if counts[c] > 1:
                aliases[fv][c] = f"{fv.__tablename__}__{c}"
            else:
                aliases[fv][c] = c

    # Build the SQL — one LATERAL per FV.
    spine_cols_ordered = sorted(spine_keys)  # deterministic ordering
    spine_values_cols = ["_s_idx", *spine_cols_ordered]
    placeholders_per_row = "(" + ", ".join(["%s"] * len(spine_values_cols)) + ")"
    values_sql = ", ".join([placeholders_per_row] * len(spine))
    spine_alias_cols_sql = ", ".join(
        ['"_s_idx"'] + [f'"_s_{c}"' for c in spine_cols_ordered]
    )

    # Output: spine columns (raw names) + each FV's columns (aliased).
    spine_select = ", ".join(
        f'spine."_s_{c}" AS "{c}"' for c in spine_cols_ordered if c != "as_of"
    )
    spine_select += (', ' if spine_select else '') + 'spine."_s_as_of" AS "as_of"'

    feat_select_parts = []
    lateral_parts = []
    for fv in feature_views:
        cols = fv_columns[fv]
        ali = aliases[fv]
        cte_alias = f"_lat_{fv.__tablename__}"
        for c in cols:
            feat_select_parts.append(f'{cte_alias}."{c}" AS "{ali[c]}"')
        pks = fv._primary_keys()
        where_join = " AND ".join(f'"{k}" = spine."_s_{k}"' for k in pks)
        cols_sql = ", ".join(f'"{c}"' for c in cols) if cols else "1"
        lateral_parts.append(
            f"LEFT JOIN LATERAL ("
            f"  SELECT {cols_sql} "
            f'  FROM "{fv.__schema__}"."{fv.__tablename__}" '
            f"  WHERE {where_join} "
            f'    AND valid_from <= spine."_s_as_of" '
            f'    AND (valid_to IS NULL OR valid_to > spine."_s_as_of") '
            f"  ORDER BY valid_from DESC "
            f"  LIMIT 1"
            f") AS {cte_alias} ON true"
        )

    select_sql = spine_select
    if feat_select_parts:
        select_sql += ", " + ", ".join(feat_select_parts)

    sql = (
        f"SELECT {select_sql} "
        f"FROM (VALUES {values_sql}) AS spine ({spine_alias_cols_sql}) "
        + " ".join(lateral_parts)
        + ' ORDER BY spine."_s_idx"'
    )

    params: list[Any] = []
    for i, e in enumerate(spine):
        params.append(i)
        for c in spine_cols_ordered:
            if c not in e:
                raise ValueError(f"spine[{i}] missing column {c!r}")
            params.append(e[c])

    psycopg2 = _require_psycopg2()
    from psycopg2.extras import RealDictCursor

    pg = psycopg2.connect(conn.dsn())
    try:
        with pg.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = [dict(r) for r in cur.fetchall()]
    finally:
        pg.close()

    if chosen == "polars":
        return lib.DataFrame(rows, infer_schema_length=max(1, len(rows)))
    return lib.DataFrame(rows)


def _resolve_result_columns(
    feature_views: list[type],
    columns: dict[type, list[str]] | None,
    spine_keys_ordered: list[str],
) -> list[str]:
    """Compute the column-name list a training_set DataFrame will have.
    Used for the empty-spine case to construct an empty-but-shaped df."""
    columns = columns or {}
    counts: dict[str, int] = {}
    fv_cols: dict[type, list[str]] = {}
    for fv in feature_views:
        if fv in columns:
            fv_cols[fv] = list(columns[fv])
        else:
            pks = set(fv._primary_keys())
            fv_cols[fv] = [
                n for n, _col in fv._columns() if n not in _META_COLS and n not in pks
            ]
        for c in fv_cols[fv]:
            counts[c] = counts.get(c, 0) + 1
    out = list(spine_keys_ordered) + (["as_of"] if "as_of" not in spine_keys_ordered else [])
    for fv in feature_views:
        for c in fv_cols[fv]:
            out.append(
                f"{fv.__tablename__}__{c}" if counts[c] > 1 else c
            )
    return out
