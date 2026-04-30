"""Phase 0 smoke tests: package imports and the Rust extension is wired up."""

import ematix_flow


def test_package_version() -> None:
    assert ematix_flow.__version__ == "0.1.0"


def test_rust_extension_loads() -> None:
    assert ematix_flow._core.core_version() == "0.1.0"
