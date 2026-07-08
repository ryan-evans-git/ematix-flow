# macOS: `_core` + pyarrow dual-mimalloc segfault

**Status:** fixed on `fix/macos-pyarrow-segv` — `_core` no longer sets a
mimalloc `#[global_allocator]` on macOS. Linux is unchanged (keeps mimalloc
and benchmark-allocator parity).

## Symptom

On macOS arm64, with the repo `.venv` (pyarrow 24.0.0, pandas 3.0.2,
CPython 3.14):

```bash
python -c "import pandas, pyarrow.parquet; from ematix_flow import _core; print(_core.core_version())"
# prints the version, then exits 139 (SIGSEGV)
```

Two crash modes, same root cause:

- **Interpreter shutdown:** the repro above prints the version, then
  segfaults during `exit()`.
- **Mid-call:** the pytest suite crashed inside `_core.parse_spec` in
  `tests/python/test_phase1_spec.py` after earlier tests had imported
  pandas — the whole macOS suite could not run.

Linux CI was always green: the clash is macOS-specific.

## Root cause

`_core` (the Rust cdylib) set mimalloc **v3.x** (`libmimalloc-sys` 0.1.49
default) as the Rust `#[global_allocator]`. pyarrow's `libarrow` statically
bundles its **own** mimalloc (v2.x) and uses it as the default Arrow memory
pool on macOS. With both instances live in one process on macOS, the
mimalloc heap metadata gets corrupted; the process dies either mid-call in
`_core` or in libarrow's `atexit` handler.

Faulting frame (lldb, macOS 15.x arm64):

```
* thread #2, queue = 'com.apple.main-thread', stop reason = EXC_BAD_ACCESS (code=1, address=0x17)
  * frame #0: libarrow.2400.dylib`_mi_theap_collect_retired + 124
    frame #1: libarrow.2400.dylib`mi_theap_collect_ex + 72
    frame #2: libarrow.2400.dylib`mi_process_done + 76
    frame #3: libsystem_c.dylib`__cxa_finalize_ranges + 416
    frame #4: libsystem_c.dylib`exit + 44
```

There is **no dyld symbol interposition** involved: `_core` exports zero
`mi_*` symbols (`nm -gU _core.*.so`), and `libarrow` imports none (`nm -u
libarrow.2400.dylib`). Both copies are internally linked; the collision is
via a process-wide resource (thread-local heap teardown at
`__cxa_finalize` / TLS-destructor time), not via one library calling the
other's functions.

## Evidence matrix

All with the pre-fix `_core` (exit codes; 139 = SIGSEGV):

| # | Repro (`python -c ...`) | Exit |
|---|---|---|
| A | `import pandas, pyarrow.parquet` (no `_core`) | 0 |
| B | `from ematix_flow import _core` only | 0 |
| C | `_core` **first**, then `pandas, pyarrow.parquet` | 0 |
| D | `import pandas` → `_core` | 139 |
| E | `import pyarrow.parquet` → `_core` | 139 |
| F | E with `ARROW_DEFAULT_MEMORY_POOL=system` | 139 |
| G | E with pyarrow **22.0.0** (throwaway venv, same broken `_core`) | 0 |

Reading:

- Import order flips the crash (C vs D/E): whichever mimalloc initializes
  second poisons the pairing. pandas 3.x imports pyarrow, so `import
  pandas` alone is enough to arm it.
- `ARROW_DEFAULT_MEMORY_POOL=system` does **not** help (F): libarrow's
  bundled mimalloc still initializes and registers its `atexit` handler
  even when it isn't the selected pool, so there is no env-var workaround.
- pyarrow 22.0.0 also bundles mimalloc (v2, default pool `mimalloc`) but
  does not crash against our v3 (G) — pyarrow 24 upgraded its bundled
  mimalloc to a version that collides with mimalloc v3 in-process on
  macOS.

## Fix

Feature-gate the allocator off for macOS **in the Python extension only**
(`crates/ematix-flow-py`):

- `Cargo.toml`: the `mimalloc` dependency moved under
  `[target.'cfg(not(target_os = "macos"))'.dependencies]`.
- `src/lib.rs`: `#[cfg(not(target_os = "macos"))]` on the
  `#[global_allocator]` static.

macOS builds of `_core` fall back to the system allocator. The `flow` CLI
and `flow-worker` binaries keep mimalloc everywhere — they never share a
process with pyarrow. Linux wheels keep mimalloc, preserving
production-allocator parity with the benchmark harness where it is
actually measured.

Regression pin: `tests/python/test_pyarrow_coexistence.py` runs the repro
in a subprocess (so a SIGSEGV fails the test instead of killing pytest)
for both import orders.

## Alternatives considered

- **Pin our mimalloc to v2** (`mimalloc` crate `v2` feature) to match
  Arrow's generation. Not chosen: coexistence would still depend on
  whatever mimalloc version each new pyarrow wheel bundles (the 22 → 24
  flip above shows how fragile that is). Not verified experimentally.
- **`ARROW_DEFAULT_MEMORY_POOL=system`**: does not work (row F).
- **Import-order guard** (`import ematix_flow` before pandas/pyarrow):
  works (row C) but cannot be enforced on downstream users, and pandas
  3.x makes accidental pyarrow-first imports the norm.
