"""Phase 0 smoke tests: package imports and the Rust extension is wired up."""

import re

import ematix_flow

# A SemVer-compatible version string. Locks down the shape of
# `__version__` without hardcoding the literal — that way a release
# bump only needs to touch Cargo.toml + pyproject.toml, not this
# file. (The previous hardcoded "0.1.0" assertion silently broke
# CI on every bump until someone remembered to update it.)
_SEMVER = re.compile(r"^\d+\.\d+\.\d+(?:[-+].+)?$")


def test_package_version() -> None:
    assert _SEMVER.match(ematix_flow.__version__), (
        f"__version__ must be SemVer-shaped; got {ematix_flow.__version__!r}"
    )


def test_rust_extension_loads() -> None:
    assert _SEMVER.match(ematix_flow._core.core_version()), (
        f"core_version() must be SemVer-shaped; got "
        f"{ematix_flow._core.core_version()!r}"
    )


def test_python_and_rust_versions_agree() -> None:
    """Catches packaging bugs where the Python wheel ships a different
    version than the bundled Rust extension (e.g., a partial bump
    that forgot one Cargo.toml or pyproject.toml). Both sides read
    from their respective `version` fields, which the workspace
    keeps in lockstep."""
    assert ematix_flow.__version__ == ematix_flow._core.core_version()
