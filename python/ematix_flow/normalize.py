"""Phase 26: per-column normalization markers + chain composition.

Each marker is a frozen dataclass with a `to_sql(col)` method that
returns the SQL expression for `col` after this normalizer applies.
Markers compose left-to-right via `compose_chain`: `[lower(), trim()]`
produces `trim(lower(col))`.

`derive(expression)` is a special marker that produces a new column
from an arbitrary expression (rather than wrapping an existing one);
it must appear first in the chain when present.

`sql(expression)` is the documented escape hatch — substitutes `col`
in the user-supplied expression. No quoting, no validation. The user
owns the SQL-injection risk.

See `docs/NORMALIZATION_TRANSFORMS_PLAN.md` §3.3 for the catalogue.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Literal

from ematix_flow.markers import _NaturalKeyMarker, _NullableMarker, _PkMarker

# Markers that aren't normalizers — filtered out before composing the chain.
_NON_NORMALIZER_MARKERS = (_PkMarker, _NaturalKeyMarker, _NullableMarker)


def _quote_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _quote_value(value: Any) -> str:
    """Render `value` as a Postgres literal with appropriate cast.

    Used by `default()` and `nullif()`. Raises for None or unsupported
    types — punt to `sql()` for anything else.
    """
    if value is None:
        raise ValueError(
            "default(None) is not allowed — use `T | None` for nullability"
        )
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, str):
        return _quote_string(value)
    if isinstance(value, datetime):
        return f"{_quote_string(value.isoformat())}::timestamptz"
    if isinstance(value, date):
        return f"{_quote_string(value.isoformat())}::date"
    raise ValueError(
        f"unsupported default() value of type {type(value).__name__}; "
        "use sql(...) for arbitrary expressions"
    )


# --- string ---------------------------------------------------------------


@dataclass(frozen=True)
class _Trim:
    def to_sql(self, col: str) -> str:
        return f"trim({col})"


def trim() -> _Trim:
    return _Trim()


@dataclass(frozen=True)
class _Lower:
    def to_sql(self, col: str) -> str:
        return f"lower({col})"


def lower() -> _Lower:
    return _Lower()


@dataclass(frozen=True)
class _Upper:
    def to_sql(self, col: str) -> str:
        return f"upper({col})"


def upper() -> _Upper:
    return _Upper()


@dataclass(frozen=True)
class _EmptyToNull:
    def to_sql(self, col: str) -> str:
        return f"NULLIF({col}, '')"


def empty_to_null() -> _EmptyToNull:
    return _EmptyToNull()


@dataclass(frozen=True)
class _WhitespaceToNull:
    def to_sql(self, col: str) -> str:
        return f"NULLIF(trim({col}), '')"


def whitespace_to_null() -> _WhitespaceToNull:
    return _WhitespaceToNull()


@dataclass(frozen=True)
class _Replace:
    old: str
    new: str

    def to_sql(self, col: str) -> str:
        return f"replace({col}, {_quote_string(self.old)}, {_quote_string(self.new)})"


def replace(old: str, new: str) -> _Replace:
    return _Replace(old=old, new=new)


@dataclass(frozen=True)
class _RegexReplace:
    pattern: str
    replacement: str
    flags: str

    def to_sql(self, col: str) -> str:
        return (
            f"regexp_replace({col}, {_quote_string(self.pattern)}, "
            f"{_quote_string(self.replacement)}, {_quote_string(self.flags)})"
        )


def regex_replace(pattern: str, replacement: str, flags: str = "g") -> _RegexReplace:
    return _RegexReplace(pattern=pattern, replacement=replacement, flags=flags)


@dataclass(frozen=True)
class _Truncate:
    n: int

    def to_sql(self, col: str) -> str:
        return f"left({col}, {self.n})"


def truncate(n: int) -> _Truncate:
    return _Truncate(n=n)


# --- canonicalization -----------------------------------------------------


@dataclass(frozen=True)
class _EmailNormalize:
    def to_sql(self, col: str) -> str:
        return f"lower(trim({col}))"


def email_normalize() -> _EmailNormalize:
    return _EmailNormalize()


@dataclass(frozen=True)
class _PhoneNormalize:
    def to_sql(self, col: str) -> str:
        return f"regexp_replace({col}, '[^0-9+]', '', 'g')"


def phone_normalize() -> _PhoneNormalize:
    return _PhoneNormalize()


# --- date / time ----------------------------------------------------------

# Postgres `to_timestamp` format codes mapped to a regex that
# pre-validates the column value before casting. If the value doesn't
# match the regex, the CASE returns NULL (or the user's chosen
# on_failure default). Without the pre-filter Postgres raises on a
# malformed value, which would tank the entire load.
_FORMAT_REGEXES: dict[str, str] = {
    "YYYY-MM-DD": r"^\d{4}-\d{2}-\d{2}$",
    "YYYY-MM-DD HH24:MI:SS": r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$",
    "YYYY-MM-DD HH24:MI:SS.MS": r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+$",
    "YYYY-MM-DDTHH24:MI:SS": r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$",
    "YYYY-MM-DDTHH24:MI:SSOF": r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}(:?\d{2})?$",
    "MM/DD/YYYY": r"^\d{2}/\d{2}/\d{4}$",
    "DD/MM/YYYY": r"^\d{2}/\d{2}/\d{4}$",
    "MM-DD-YYYY": r"^\d{2}-\d{2}-\d{4}$",
    "DD-MM-YYYY": r"^\d{2}-\d{2}-\d{4}$",
    "YYYYMMDD": r"^\d{8}$",
}

OnFailure = Literal["null", "error", "default"]


# Phase 26 follow-up (Q3.5 γ): default catalogues used when callers
# pass no `format=` / `formats=`. Order matters — first-match-wins on
# ambiguous inputs. Curated to common ISO + US conventions; rare or
# locale-specific formats stay opt-in via explicit `formats=[...]`.
DEFAULT_TIMESTAMP_FORMATS: tuple[str, ...] = (
    "YYYY-MM-DD HH24:MI:SS.MS",
    "YYYY-MM-DDTHH24:MI:SSOF",
    "YYYY-MM-DDTHH24:MI:SS",
    "YYYY-MM-DD HH24:MI:SS",
    "YYYY-MM-DD",
    "MM/DD/YYYY",
)
DEFAULT_DATE_FORMATS: tuple[str, ...] = (
    "YYYY-MM-DD",
    "YYYYMMDD",
    "MM/DD/YYYY",
)


def _validate_on_failure_pair(on_failure: OnFailure, default: Any) -> None:
    if on_failure == "default" and default is None:
        raise ValueError(
            'on_failure="default" requires default= to be set to a non-None value'
        )
    if on_failure != "default" and default is not None:
        raise ValueError(
            'default= is only valid with on_failure="default"'
        )


def _resolve_formats(
    format: str | None,
    formats: Sequence[str] | None,
    *,
    default_catalogue: Sequence[str] | None = None,
) -> tuple[str, ...]:
    if format is not None and formats is not None:
        raise ValueError("pass either format= or formats=, not both")
    if format is not None:
        return (format,)
    if formats is not None:
        if not formats:
            raise ValueError("formats= must contain at least one format")
        return tuple(formats)
    if default_catalogue is not None:
        return tuple(default_catalogue)
    raise ValueError("format= or formats= is required")


def _format_regex(fmt: str) -> str:
    """Return the regex that pre-validates `fmt`, or a permissive '.*' fallback."""
    return _FORMAT_REGEXES.get(fmt, ".*")


def _build_parse_case(
    col: str,
    formats: tuple[str, ...],
    cast_fn: str,
    on_failure: OnFailure,
    default: Any,
) -> str:
    if on_failure == "error":
        # Single-format error mode skips the regex guard entirely so
        # Postgres raises naturally on a bad value.
        if len(formats) != 1:
            raise ValueError('on_failure="error" requires a single format')
        return f"{cast_fn}({col}, {_quote_string(formats[0])})"

    if on_failure not in ("null", "default"):
        raise ValueError(f"invalid on_failure={on_failure!r}")

    whens = "\n        ".join(
        f"WHEN {col} ~ {_quote_string(_format_regex(fmt))} "
        f"THEN {cast_fn}({col}, {_quote_string(fmt)})"
        for fmt in formats
    )
    case_expr = f"CASE\n        {whens}\n        ELSE NULL\n    END"
    if on_failure == "default":
        return f"COALESCE({case_expr}, {_quote_value(default)})"
    return case_expr


@dataclass(frozen=True)
class _ParseTimestamp:
    formats: tuple[str, ...]
    on_failure: OnFailure
    default: Any

    def to_sql(self, col: str) -> str:
        return _build_parse_case(
            col, self.formats, "to_timestamp", self.on_failure, self.default
        )


def parse_timestamp(
    format: str | None = None,
    *,
    formats: Sequence[str] | None = None,
    on_failure: OnFailure = "null",
    default: Any = None,
) -> _ParseTimestamp:
    resolved = _resolve_formats(
        format, formats, default_catalogue=DEFAULT_TIMESTAMP_FORMATS
    )
    _validate_on_failure_pair(on_failure, default)
    return _ParseTimestamp(formats=resolved, on_failure=on_failure, default=default)


@dataclass(frozen=True)
class _ParseDate:
    formats: tuple[str, ...]
    on_failure: OnFailure
    default: Any

    def to_sql(self, col: str) -> str:
        return _build_parse_case(
            col, self.formats, "to_date", self.on_failure, self.default
        )


def parse_date(
    format: str | None = None,
    *,
    formats: Sequence[str] | None = None,
    on_failure: OnFailure = "null",
    default: Any = None,
) -> _ParseDate:
    resolved = _resolve_formats(
        format, formats, default_catalogue=DEFAULT_DATE_FORMATS
    )
    _validate_on_failure_pair(on_failure, default)
    return _ParseDate(formats=resolved, on_failure=on_failure, default=default)


@dataclass(frozen=True)
class _ToTimezone:
    tz: str

    def to_sql(self, col: str) -> str:
        return f"{col} AT TIME ZONE {_quote_string(self.tz)}"


def to_timezone(tz: str) -> _ToTimezone:
    return _ToTimezone(tz=tz)


@dataclass(frozen=True)
class _DateTrunc:
    precision: str

    def to_sql(self, col: str) -> str:
        return f"date_trunc({_quote_string(self.precision)}, {col})"


def date_trunc(precision: str) -> _DateTrunc:
    return _DateTrunc(precision=precision)


# --- numeric --------------------------------------------------------------

_INT_REGEX = r"^-?\d+$"
_NUMERIC_REGEX = r"^-?\d+(\.\d+)?$"


def _build_numeric_case(
    col: str,
    cast: str,
    regex: str,
    on_failure: OnFailure,
    default: Any,
) -> str:
    if on_failure == "error":
        return f"{col}::{cast}"
    if on_failure not in ("null", "default"):
        raise ValueError(f"invalid on_failure={on_failure!r}")
    case_expr = (
        f"CASE WHEN {col} ~ {_quote_string(regex)} "
        f"THEN {col}::{cast} ELSE NULL END"
    )
    if on_failure == "default":
        return f"COALESCE({case_expr}, {_quote_value(default)})"
    return case_expr


@dataclass(frozen=True)
class _ParseInt:
    on_failure: OnFailure
    default: Any

    def to_sql(self, col: str) -> str:
        return _build_numeric_case(
            col, "bigint", _INT_REGEX, self.on_failure, self.default
        )


def parse_int(on_failure: OnFailure = "null", default: Any = None) -> _ParseInt:
    _validate_on_failure_pair(on_failure, default)
    return _ParseInt(on_failure=on_failure, default=default)


@dataclass(frozen=True)
class _ParseNumeric:
    precision: int
    scale: int
    on_failure: OnFailure
    default: Any

    def to_sql(self, col: str) -> str:
        cast = f"numeric({self.precision}, {self.scale})"
        return _build_numeric_case(
            col, cast, _NUMERIC_REGEX, self.on_failure, self.default
        )


def parse_numeric(
    precision: int,
    scale: int,
    on_failure: OnFailure = "null",
    default: Any = None,
) -> _ParseNumeric:
    _validate_on_failure_pair(on_failure, default)
    return _ParseNumeric(
        precision=precision, scale=scale, on_failure=on_failure, default=default
    )


@dataclass(frozen=True)
class _Round:
    precision: int

    def to_sql(self, col: str) -> str:
        return f"round({col}, {self.precision})"


def round(precision: int) -> _Round:
    return _Round(precision=precision)


@dataclass(frozen=True)
class _Clamp:
    low: Any
    high: Any

    def to_sql(self, col: str) -> str:
        return f"least(greatest({col}, {self.low}), {self.high})"


def clamp(low: Any, high: Any) -> _Clamp:
    return _Clamp(low=low, high=high)


# --- boolean --------------------------------------------------------------


@dataclass(frozen=True)
class _ParseBool:
    truthy: tuple[str, ...]
    falsy: tuple[str, ...]

    def to_sql(self, col: str) -> str:
        truthy_list = ", ".join(_quote_string(v) for v in self.truthy)
        falsy_list = ", ".join(_quote_string(v) for v in self.falsy)
        return (
            f"CASE WHEN lower({col}) IN ({truthy_list}) THEN true "
            f"WHEN lower({col}) IN ({falsy_list}) THEN false END"
        )


def parse_bool(
    truthy: Sequence[str] = ("true", "1", "yes", "y"),
    falsy: Sequence[str] = ("false", "0", "no", "n"),
) -> _ParseBool:
    return _ParseBool(truthy=tuple(truthy), falsy=tuple(falsy))


# --- NULL handling --------------------------------------------------------


@dataclass(frozen=True)
class _Default:
    rendered: str

    def to_sql(self, col: str) -> str:
        return f"COALESCE({col}, {self.rendered})"


def default(value: Any) -> _Default:
    return _Default(rendered=_quote_value(value))


@dataclass(frozen=True)
class _NullIf:
    rendered: str

    def to_sql(self, col: str) -> str:
        return f"NULLIF({col}, {self.rendered})"


def nullif(value: Any) -> _NullIf:
    return _NullIf(rendered=_quote_value(value))


def not_null_or(value: Any) -> _Default:
    """Alias for `default(value)` when "must never be null" reads better."""
    return default(value)


# --- escape hatch ---------------------------------------------------------


@dataclass(frozen=True)
class _Sql:
    expression: str

    def to_sql(self, col: str) -> str:
        return self.expression.replace("col", col)


def sql(expression: str) -> _Sql:
    """User-supplied Postgres expression; `col` is the column placeholder.

    No validation, no escaping. The user owns SQL-injection risk.
    `preview()` flags every column carrying a `sql()` marker for audit.
    """
    return _Sql(expression=expression)


# --- derive ---------------------------------------------------------------


@dataclass(frozen=True)
class _Derive:
    expression: str

    def to_sql(self, col: str) -> str:
        # `col` is ignored for derived columns — the expression IS the value.
        return self.expression


def derive(expression: str) -> _Derive:
    """Produce a new column from an arbitrary SQL expression.

    Must be the first marker in the chain when present. Subsequent
    normalizers wrap the derived expression naturally.
    """
    return _Derive(expression=expression)


# --- chain composition ----------------------------------------------------


def is_normalizer(marker: Any) -> bool:
    """True if `marker` participates in the normalization chain."""
    if isinstance(marker, _NON_NORMALIZER_MARKERS):
        return False
    return hasattr(marker, "to_sql")


def compose_chain(markers: Sequence[Any], col: str) -> str:
    """Compose normalizers left-to-right into a single SQL expression.

    `[lower(), trim()]` produces `trim(lower(col))`. Non-normalizer
    markers (`pk()`, `natural_key()`, `nullable()`) are filtered out.
    `derive()` must come first when present.
    """
    chain = [m for m in markers if is_normalizer(m)]
    if not chain:
        return col
    for i, marker in enumerate(chain):
        if isinstance(marker, _Derive) and i != 0:
            raise ValueError(
                "derive() must be the first marker in the chain when present"
            )
    expr = col
    for i, marker in enumerate(chain):
        if i == 0 and isinstance(marker, _Derive):
            expr = marker.expression
        else:
            expr = marker.to_sql(expr)
    return expr


# --- pipeline-level transforms_pre ----------------------------------------


@dataclass(frozen=True)
class _DeduplicateBy:
    keys: tuple[str, ...]
    order_by: str | None

    def to_sql_wrap(self, inner: str) -> str:
        keys_sql = ", ".join(self.keys)
        order = f", {self.order_by}" if self.order_by else ""
        return (
            f"SELECT DISTINCT ON ({keys_sql}) * FROM {inner} "
            f"ORDER BY {keys_sql}{order}"
        )


def deduplicate_by(*keys: str, order_by: str | None = None) -> _DeduplicateBy:
    if not keys:
        raise ValueError("deduplicate_by requires at least one key column")
    return _DeduplicateBy(keys=tuple(keys), order_by=order_by)


@dataclass(frozen=True)
class _FilterWhere:
    expr: str

    def to_sql_wrap(self, inner: str) -> str:
        return f"SELECT * FROM {inner} WHERE {self.expr}"


def filter_where(expr: str) -> _FilterWhere:
    return _FilterWhere(expr=expr)


@dataclass(frozen=True)
class _Limit:
    n: int

    def to_sql_wrap(self, inner: str) -> str:
        return f"SELECT * FROM {inner} LIMIT {self.n}"


def limit(n: int) -> _Limit:
    if n < 0:
        raise ValueError("limit(n) must be non-negative")
    return _Limit(n=n)


@dataclass(frozen=True)
class _SamplePct:
    p: float

    def to_sql_wrap(self, inner: str) -> str:
        return f"SELECT * FROM {inner} WHERE random() < {self.p}"


def sample_pct(p: float) -> _SamplePct:
    if not 0.0 <= p <= 1.0:
        raise ValueError("sample_pct(p) requires 0.0 <= p <= 1.0")
    return _SamplePct(p=p)


def is_pipeline_transform(obj: Any) -> bool:
    return hasattr(obj, "to_sql_wrap")


# --- source SQL synthesis -------------------------------------------------


def apply_normalization(
    target_cls: Any,
    base_sql: str,
    transforms_pre: Sequence[Any] = (),
) -> str:
    """Wrap `base_sql` with normalization + transforms_pre CTEs.

    Layers, in order:
      1. `_src_user`        — the raw user source.
      2. `_src_normalized`  — per-column normalizers applied (skipped if no
                              column has normalizers and no `derive()` columns).
      3. `_src_pre_1..N`    — one CTE per pipeline-level transform_pre.

    The final SELECT pulls from the last layer.
    """
    columns = list(target_cls._columns())
    has_normalizers = any(col.normalizers for _, col in columns)

    if not has_normalizers and not transforms_pre:
        return base_sql

    ctes: list[tuple[str, str]] = [("_src_user", base_sql)]
    last_cte = "_src_user"

    if has_normalizers:
        # Build a SELECT that applies each column's chain. Columns without
        # normalizers pass through bare; `derive()` columns generate from
        # the expression rather than a source column.
        select_terms: list[str] = []
        for name, col in columns:
            if col.normalizers:
                expr = compose_chain(col.normalizers, name)
                if expr == name:
                    select_terms.append(name)
                else:
                    select_terms.append(f"{expr} AS {name}")
            else:
                select_terms.append(name)
        norm_sql = (
            f"SELECT {', '.join(select_terms)} FROM {last_cte}"
        )
        ctes.append(("_src_normalized", norm_sql))
        last_cte = "_src_normalized"

    for i, t in enumerate(transforms_pre, start=1):
        if not is_pipeline_transform(t):
            raise TypeError(
                f"transforms_pre entry {t!r} is not a pipeline transform "
                "(missing to_sql_wrap)"
            )
        cte_name = f"_src_pre_{i}"
        ctes.append((cte_name, t.to_sql_wrap(last_cte)))
        last_cte = cte_name

    cte_clauses = ",\n     ".join(f"{name} AS (\n        {sql}\n     )" for name, sql in ctes)
    return f"WITH {cte_clauses}\nSELECT * FROM {last_cte}"


__all__ = [
    "DEFAULT_DATE_FORMATS",
    "DEFAULT_TIMESTAMP_FORMATS",
    "OnFailure",
    "apply_normalization",
    "clamp",
    "compose_chain",
    "date_trunc",
    "deduplicate_by",
    "default",
    "derive",
    "email_normalize",
    "empty_to_null",
    "filter_where",
    "is_normalizer",
    "is_pipeline_transform",
    "limit",
    "lower",
    "not_null_or",
    "nullif",
    "parse_bool",
    "parse_date",
    "parse_int",
    "parse_numeric",
    "parse_timestamp",
    "phone_normalize",
    "regex_replace",
    "replace",
    "round",
    "sample_pct",
    "sql",
    "to_timezone",
    "trim",
    "truncate",
    "upper",
    "whitespace_to_null",
]
