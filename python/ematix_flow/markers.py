"""Phase 24a: column markers used inside `Annotated[T, marker()]`.

Pydantic-Field style — markers are always called (`pk()`, not `pk`)
so future params land cleanly (`pk(autoincrement=True)` etc.).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class _PkMarker:
    """Marker for a primary-key column."""

    # Stored as a frozenset of (key, value) tuples so the dataclass stays
    # hashable even with arbitrary kwargs. Forward-compatible: we accept
    # `autoincrement=True` etc. today even though the framework currently
    # ignores them.
    options: frozenset[tuple[str, Any]] = field(default_factory=frozenset)

    def __repr__(self) -> str:
        if not self.options:
            return "pk()"
        kwargs = ", ".join(f"{k}={v!r}" for k, v in sorted(self.options))
        return f"pk({kwargs})"


def pk(**options: Any) -> _PkMarker:
    """Mark a column as a primary key.

    Composite primary keys: place `pk()` on multiple columns; their
    declaration order becomes the PK column order.

    Pipelines automatically use the PK columns as their merge keys
    when `keys=` is omitted on `@ematix.pipeline` / `pipeline.sync`,
    so you typically don't need to repeat the key list. The
    resolution order is:

      1. explicit `keys=("col_a", "col_b")` on the pipeline
      2. ``__merge_keys__ = ("col_a", "col_b")`` class dunder on
         the table
      3. first ``natural_key()`` group on the table
      4. ``pk()`` columns (this marker)

    When a `natural_key()` or `__merge_keys__` resolution differs
    from the declared PK, the pipeline emits a `UserWarning`. Pass
    explicit `keys=` to silence.
    """
    return _PkMarker(options=frozenset(options.items()))


@dataclass(frozen=True)
class _NaturalKeyMarker:
    """Marker for a column participating in a UNIQUE constraint."""

    # Group label. Empty string is the default group; any string-labeled
    # group joins separate columns into a distinct UNIQUE constraint.
    group: str = ""

    def __repr__(self) -> str:
        if self.group:
            return f"natural_key({self.group!r})"
        return "natural_key()"


def natural_key(group: str = "") -> _NaturalKeyMarker:
    """Mark a column as part of a composite UNIQUE constraint.

    All `natural_key()` columns (no arg) join one constraint; columns
    sharing a group label join a separate constraint named after the
    label.

    Two ways to use `natural_key()` in practice:

    1. **As an additional UNIQUE constraint** on a non-PK column —
       e.g. an external `email` field that must be unique even
       though `customer_id` is the PK:

           class Customer:
               customer_id: Annotated[BigInt, pk()]
               email: Annotated[String[256], natural_key()]

    2. **As the SCD2 merge-key driver** when you want to be
       explicit about which column the SCD2 merge keys off of
       (rather than letting the framework infer from `pk()`).
       For SCD2 the typical PK on the business-key column is
       sufficient — augmentation adds `valid_from` automatically,
       and the merge key inference picks `pk()`. Use
       `natural_key()` here only when you have a non-PK business
       key you want SCD2 to merge against.

    When a pipeline omits `keys=`, the resolution order picks the
    natural-key group ahead of `pk()` columns. See `pk()` for the
    full resolution chain.
    """
    return _NaturalKeyMarker(group=group)


@dataclass(frozen=True)
class _NullableMarker:
    """Explicit nullability marker. `T | None` is preferred but this
    sentinel is sometimes clearer for explicit cases."""

    def __repr__(self) -> str:
        return "nullable()"


def nullable() -> _NullableMarker:
    return _NullableMarker()


__all__ = [
    "pk",
    "natural_key",
    "nullable",
    "_PkMarker",
    "_NaturalKeyMarker",
    "_NullableMarker",
]
