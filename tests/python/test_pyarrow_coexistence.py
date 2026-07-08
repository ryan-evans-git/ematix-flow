"""Regression tests: _core must coexist with pyarrow's bundled mimalloc.

pyarrow's libarrow ships its own statically linked mimalloc (v2.x). Until
the fix on this branch, `_core` also installed mimalloc (v3.x) as the Rust
`#[global_allocator]` on every platform. On macOS (arm64 verified) two
mimalloc instances in one process corrupt each other's thread-local heap
metadata: importing `_core` *after* pyarrow crashed with SIGSEGV either
mid-call inside `_core` or at interpreter shutdown, in
`libarrow.dylib::mi_process_done -> _mi_theap_collect_retired` (via
`__cxa_finalize_ranges`). Linux was unaffected.

Each scenario runs in a *subprocess* so a SIGSEGV fails the test instead of
killing the pytest run. The child exercises both crash modes: a call into
`_core` after the imports (mid-call mode) and a normal exit (shutdown mode).
"""

import subprocess
import sys

import pytest

pytest.importorskip("pyarrow")
pytest.importorskip("pandas")

# `parse_spec` after the imports covers the mid-call crash mode;
# the interpreter exiting normally covers the shutdown crash mode.
_SPEC = (
    '{"name": "n", "source": {"connection": "c", "query": "q"},'
    ' "target": {"connection": "c", "schema": "s", "table": "t"},'
    ' "mode": "append", "keys": []}'
)
_CALL_CORE = (
    "from ematix_flow import _core; "
    f"_core.parse_spec('{_SPEC}'); "
    "print(_core.core_version())"
)

_SCENARIOS = {
    # The historical segfault: pyarrow (and pandas, which loads pyarrow)
    # initialize libarrow's mimalloc first, then _core loads.
    "pyarrow_then_core": "import pandas, pyarrow.parquet; " + _CALL_CORE,
    "pyarrow_only_then_core": "import pyarrow.parquet; " + _CALL_CORE,
    # Control: the reverse order never crashed; pin it so a future
    # allocator change can't regress it either.
    "core_then_pyarrow": _CALL_CORE + "; import pandas, pyarrow.parquet",
}


@pytest.mark.parametrize("code", _SCENARIOS.values(), ids=_SCENARIOS.keys())
def test_core_and_pyarrow_coexist(code: str) -> None:
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, (
        f"child python exited {proc.returncode} (SIGSEGV is -11/139: "
        f"allocator clash with pyarrow's bundled mimalloc)\n"
        f"stdout: {proc.stdout}\nstderr: {proc.stderr}"
    )
    assert proc.stdout.strip(), "expected core_version() output"
