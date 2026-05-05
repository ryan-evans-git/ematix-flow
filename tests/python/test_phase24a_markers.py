"""Phase 24a: pk / natural_key / nullable markers."""

from __future__ import annotations

from ematix_flow import natural_key, nullable, pk


def test_pk_returns_marker_object() -> None:
    m = pk()
    assert m is not None
    # Equality between markers of the same kind with same args.
    assert pk() == pk()


def test_pk_accepts_kwargs_for_forward_compat() -> None:
    """pk(autoincrement=True) etc. — kwargs allowed even if framework ignores them."""
    pk(autoincrement=True)


def test_natural_key_default_group() -> None:
    m = natural_key()
    assert m == natural_key()
    # The default group is None (or some sentinel) — what matters is two
    # default-grouped markers equal one another.


def test_natural_key_with_string_group() -> None:
    a = natural_key("legacy")
    b = natural_key("legacy")
    c = natural_key("primary")
    assert a == b
    assert a != c


def test_default_natural_key_differs_from_grouped() -> None:
    assert natural_key() != natural_key("legacy")


def test_nullable_marker_is_singleton() -> None:
    assert nullable() == nullable()


def test_markers_have_repr() -> None:
    assert "pk" in repr(pk())
    assert "natural_key" in repr(natural_key())
    assert "nullable" in repr(nullable())


def test_markers_are_hashable() -> None:
    """Markers go into Annotated[T, marker] which Python compares structurally."""
    {pk(), natural_key(), nullable()}
