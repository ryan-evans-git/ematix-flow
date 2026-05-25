# PGO (Profile-Guided Optimization) build pipeline

Phase 1 of `docs/plans/CURRENT.md` — Σ.T V5 Tier 1 L3 lever.

The PGO binary is the **release-bench binary** going forward. The
optimized binary is what produces the published 22q SF=10 numbers.

## TL;DR

```sh
# One-time: install cargo-pgo and the llvm-tools-preview component.
# (rust-toolchain.toml already pins llvm-tools-preview; cargo will
# fetch it on first invocation.)
cargo install --locked cargo-pgo

# Three-step pipeline:
scripts/pgo/build-instrumented.sh    # 1. instrumented build
scripts/pgo/train.sh                  # 2. training run (22q SF=10, single iter)
scripts/pgo/optimize.sh               # 3. merge profiles + optimized build

# Clean profile data between training iterations:
scripts/pgo/clean.sh
```

The PGO-optimized `tpch_triangulation_bench` binary lands at:

```
target/aarch64-apple-darwin/release/examples/tpch_triangulation_bench
```

(or the equivalent host-triple path on other architectures.)

## What gets PGO'd

The training workload is the `tpch_triangulation_bench` example. Even
though the example binary is what gets instrumented and run,
`ematix-flow-core` is compiled into that binary as a library dep — so
the engine's hot paths (parquet scan, hash agg, hash join, filter
pushdown) are the code that benefits from the optimized build.

## Open-question resolutions

These resolve the OQ-PGO-A and OQ-PGO-B questions in `docs/plans/CURRENT.md`.

### OQ-PGO-A: `cargo-pgo` over raw rustflags

We use `cargo-pgo` (the published cargo subcommand) rather than
hand-rolling `-Cprofile-generate` / `-Cprofile-use` rustflags. Why:

- Lower toolchain risk. `cargo-pgo` tracks rustc PGO conventions
  across nightly/stable rotations; raw rustflags would break on flag
  renames.
- CI-friendly. Single invocation per pipeline step; cacheable artifacts.
- Cross-platform. We bench on M3 Pro (canonical) plus minipc x86;
  `cargo-pgo` handles the host-triple paths.

Drop to raw rustflags only if `cargo-pgo` doesn't support a needed
flag (e.g. cross-compilation we're not doing today).

### OQ-PGO-B: 22q SF=10 single iteration as the training workload

The training run is one pass over 22 TPC-H SF=10 queries (with
`tpch_triangulation_bench` configured to `TPCH_TRIALS=1` and skip
DuckDB / Polars, since we only want ematix-flow's hot paths
represented in the profile).

- **Why SF=10:** that's the strategic-target scale per V5; SF=1 hot
  paths are largely a subset of SF=10's at the codegen-shape level
  (same operators, same kernel calls, just smaller batches).
- **Why single iteration:** 22 queries × ~1-15s = ~3-5 minutes total
  training time. Multi-iteration would re-bias the profile toward
  long-running queries (Q21, Q05) without changing the codegen
  decisions PGO actually needs.
- **SF=1 regression catcher:** Story 1.3 includes a 22q SF=1
  regression check (PGO vs non-PGO) to verify that training-on-SF=10
  doesn't hurt SF=1 shapes by more than ±1%.

## Files

- `build-instrumented.sh` — Step 1. Builds the instrumented bench
  binary at `target/<host_triple>/release/examples/tpch_triangulation_bench`.
- `train.sh` — Step 2. Runs the bench against SF=10 data; `.profraw`
  files land under `target/pgo-profiles/`.
- `optimize.sh` — Step 3. Merges `.profraw` → `.profdata`, then
  rebuilds with the merged profile applied.
- `clean.sh` — Drops `target/pgo-profiles/` to start a fresh training run.
- `test_pgo_build_smoke.sh` — Story 1.1 smoke test. Runs
  `build-instrumented.sh`, asserts the binary exists and is
  instrumented (looks for `__llvm_profile` symbols).
- `test_training_run.sh` — Story 1.2 smoke test. Runs `train.sh`
  briefly (single query) and asserts `.profraw` files exist.
- `test_profile_merge.sh` — Story 1.2 smoke test. Runs `optimize.sh`
  and asserts the optimized binary is distinct from the instrumented
  one (different size).

## Platform support

| Platform              | Build (Story 1.1) | Training (Story 1.2) | Notes                                            |
|-----------------------|-------------------|----------------------|--------------------------------------------------|
| Linux x86_64          | ✓                 | ✓                    | Primary PGO target (CI box).                     |
| Linux aarch64         | ✓                 | ✓ (untested)         | Should work; not validated yet.                  |
| macOS aarch64 (M-series) | ✓               | ✗ blocked             | Instrumented binary segfaults in dyld init.       |

### macOS instrumented-binary crash (2026-05-25)

On macOS aarch64, the PGO-instrumented binary crashes with EXC_BAD_ACCESS
inside `__llvm_profile_instrument_target` while dyld runs C++ static
constructors — specifically `_GLOBAL__sub_I_http_util.cpp` from the
`openssl-src` crate (vendored OpenSSL, pulled in by `rdkafka` via the
`ssl-vendored` feature). The PGO instrumentation runtime is not yet
initialized when the C++ ctor runs, so the counter-write call segfaults.

This is a Mach-O dyld init-ordering issue, not a cargo-pgo bug.

**Workaround for now: run Story 1.2 (training) and Story 1.3
(release-bench reproduction) on Linux.** The Story 1.1 build pipeline
(this script + `build-instrumented.sh`) works on macOS so contributors
can validate the toolchain locally; the actual PGO-optimized release
binary is produced on the Linux bench host.

Longer-term fix (out of scope for Story 1.1): feature-gate the cloud /
streaming deps (rdkafka, aws-sdk-*, google-cloud-*) behind a
`cloud-backends` feature in `ematix-flow-core/Cargo.toml`, so a
PGO-instrumented release build doesn't link the C++ initializers.
Tracked as a follow-up item once Linux PGO numbers prove the lever.
BOLT (post-link sample-based optimization) was considered but does
not have aarch64-macOS support either, so it's a Linux-only path
regardless.

## Re-training cadence

For TPC-H 22q the workload shape is stable; the SOP is **re-train on
every major release-candidate build**. Story 1.4 (CI hook) wires this
into the SF=10 release-bench CI workflow with profile caching keyed on
`Cargo.lock` hash.

Re-train sooner if:
- The 22q SQL changes (new query added, existing query rewritten).
- Major engine surface change (new operator, optimizer rule, codec).
- Rust toolchain bump.
