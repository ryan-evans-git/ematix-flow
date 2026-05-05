"""Column type catalogue + `Column` descriptor.

Each `ColumnType` subclass emits a `to_spec()` dict that the Rust core
deserializes into a `ColumnType` enum variant via the `kind` tag.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class ColumnType:
    """Base class. Subclasses must override `to_spec()`."""

    def to_spec(self) -> dict[str, Any]:
        raise NotImplementedError

    def __or__(self, other: Any) -> Any:
        # Phase 24a: support `String[256] | None` syntax in type
        # annotations. Returns a detectable wrapper the table decorator
        # treats as nullable. For non-`None` operands, defer to the
        # default behavior so unrelated `|` usage isn't silently coerced.
        if other is None or other is type(None):
            return _Nullable(self)
        return NotImplemented


class _Nullable:
    """Internal: result of `ColumnType_instance | None`. The table
    decorator detects this and produces a nullable column."""

    __slots__ = ("inner",)

    def __init__(self, inner: ColumnType) -> None:
        self.inner = inner

    def __repr__(self) -> str:
        return f"{self.inner!r} | None"


def _kind(name: str) -> type[ColumnType]:
    """Build a parameter-less `ColumnType` subclass with kind `name`."""

    spec = {"kind": name}

    class _Type(ColumnType):
        def to_spec(self) -> dict[str, Any]:
            return dict(spec)

        def __repr__(self) -> str:  # pragma: no cover - cosmetic
            return f"{type(self).__name__}()"

    _Type.__name__ = name
    return _Type


SmallInt = _kind("small_int")
Integer = _kind("integer")
BigInt = _kind("big_int")
Float = _kind("float")
Double = _kind("double")
Boolean = _kind("boolean")
Text = _kind("text")
Date = _kind("date")
Timestamp = _kind("timestamp")
TimestampTZ = _kind("timestamp_tz")
JSON = _kind("json")
JSONB = _kind("jsonb")
UUID = _kind("uuid")
Bytes = _kind("bytes")


class String(ColumnType):
    def __init__(self, length: int) -> None:
        if length <= 0:
            raise ValueError("String(length=) must be positive")
        self.length = length

    def to_spec(self) -> dict[str, Any]:
        return {"kind": "string", "length": self.length}

    def __class_getitem__(cls, length: int) -> String:
        # Phase 24a: `String[256]` evaluates to `String(256)` so the type
        # reads cleanly inside `Annotated[...]`.
        return cls(length)


class Numeric(ColumnType):
    def __init__(self, precision: int, scale: int) -> None:
        if precision <= 0 or scale < 0 or scale > precision:
            raise ValueError("Numeric requires precision > 0 and 0 <= scale <= precision")
        self.precision = precision
        self.scale = scale

    def to_spec(self) -> dict[str, Any]:
        return {"kind": "numeric", "precision": self.precision, "scale": self.scale}

    def __class_getitem__(cls, params: tuple[int, int]) -> Numeric:
        # Phase 24a: `Numeric[12, 2]` evaluates to `Numeric(12, 2)`.
        if not isinstance(params, tuple) or len(params) != 2:
            raise TypeError(
                "Numeric[...] requires two arguments: Numeric[precision, scale]"
            )
        precision, scale = params
        return cls(precision=precision, scale=scale)


@dataclass
class Column:
    type: ColumnType
    nullable: bool = True
    primary_key: bool = False
    name: str = field(default="", repr=False)
    # Phase 26: ordered tuple of per-column normalizer markers. Filtered
    # at decoration time so pk() / natural_key() / nullable() never appear.
    normalizers: tuple[Any, ...] = field(default=(), repr=False)

    def to_spec(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "type": self.type.to_spec(),
            "nullable": self.nullable,
            "primary_key": self.primary_key,
        }
