"""Phase 24a: parameterized SQL types via `__class_getitem__`.

`String[256]` should evaluate to the same thing as `String(256)`, and
`Numeric[12, 2]` to `Numeric(precision=12, scale=2)`. Both forms are
useful; the subscript form is what users will mostly write inside
`Annotated[...]` because it reads as a clean type annotation.
"""

from __future__ import annotations

from ematix_flow.types import Numeric, String


def test_string_subscript_equivalent_to_call() -> None:
    s_call = String(256)
    s_sub = String[256]
    assert s_call.to_spec() == s_sub.to_spec()
    assert s_sub.length == 256


def test_string_subscript_with_different_lengths() -> None:
    assert String[64].to_spec() == {"kind": "string", "length": 64}
    assert String[1024].to_spec() == {"kind": "string", "length": 1024}


def test_numeric_subscript_equivalent_to_call() -> None:
    n_call = Numeric(precision=12, scale=2)
    n_sub = Numeric[12, 2]
    assert n_call.to_spec() == n_sub.to_spec()
    assert n_sub.precision == 12
    assert n_sub.scale == 2


def test_numeric_subscript_with_different_dimensions() -> None:
    assert Numeric[10, 4].to_spec() == {
        "kind": "numeric",
        "precision": 10,
        "scale": 4,
    }


def test_numeric_subscript_requires_two_args() -> None:
    """Numeric[12] (missing scale) should raise — both must be supplied."""
    import pytest

    with pytest.raises((TypeError, ValueError)):
        Numeric[12]
