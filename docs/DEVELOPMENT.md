# Developer setup

Quick guide for contributors. For project background see
`docs/BENCHMARKS.md`, `docs/PHASE_SIGMA_T_SF10_WEAKNESS_CLOSURE_V5.md`,
and the active plan at `docs/plans/CURRENT.md`.

## Toolchain

The workspace is on Rust **stable** with MSRV `1.85`. A
`rust-toolchain.toml` at the repo root pins the channel and required
components, so a fresh checkout + `cargo build` will pull the right
toolchain automatically.

Required components (auto-installed by rustup on first invocation):

| Component             | Used for                                        |
|-----------------------|-------------------------------------------------|
| `llvm-tools-preview`  | `cargo pgo optimize` (merges `.profraw`)         |
| `rustfmt`             | `cargo fmt` gate in CI                           |
| `clippy`              | `cargo clippy` gate in CI                        |

## Build + test

```sh
# Plain release build (does NOT require cargo-pgo).
cargo build --release

# Workspace tests.
cargo test --release

# Lints (matches CI).
cargo fmt --check
cargo clippy --release --all-targets -- -D warnings
```

## TPC-H benchmark workflow

The canonical 22-query bench is `tpch_triangulation_bench`, which
runs ematix-flow + DuckDB + Polars in the same process for
apples-to-apples comparison.

```sh
# SF=1, default trials/warmups, all 22 queries.
cargo run --release -p ematix-flow-core \
    --example tpch_triangulation_bench --features triangulation

# SF=10, 5 trials × 2 warmups, ematix-flow only.
TPCH_DATA_DIR=examples/tpch/data/sf10 \
TPCH_TRIALS=5 TPCH_WARMUPS=2 \
TPCH_SKIP_DUCKDB=1 TPCH_SKIP_POLARS=1 \
EMAT_RG_DECODE_CACHE=1 EMAT_RH_SUM_F64=1 \
    cargo run --release -p ematix-flow-core \
        --example tpch_triangulation_bench --features triangulation
```

The full bench-env checklist (env flags that the 0.80 SF=10 baseline
assumes) lives in memory `feedback_full_bench_env_checklist.md`.

## Profile-Guided Optimization (PGO)

The release-bench binary is produced via a profile-guided build —
the optimized `tpch_triangulation_bench` binary is what generates
the published 22q SF=10 numbers going forward (per V5 Tier 1 L3).

One-time setup:

```sh
cargo install --locked cargo-pgo
```

Per-release pipeline:

```sh
scripts/pgo/build-instrumented.sh    # instrumented build
scripts/pgo/train.sh                  # training run (22q SF=10, single iter, ~3-5 min)
scripts/pgo/optimize.sh               # merge profiles + optimized build
```

The optimized binary lands at:

```
target/<host_triple>/release/examples/tpch_triangulation_bench
```

See `scripts/pgo/README.md` for the full design — including the
OQ-PGO-A (cargo-pgo over rustflags) and OQ-PGO-B (22q SF=10 single
iter) resolutions.

To clean and re-train (e.g. after a Cargo.lock change or a major
release-candidate rebuild):

```sh
scripts/pgo/clean.sh
scripts/pgo/build-instrumented.sh
scripts/pgo/train.sh
scripts/pgo/optimize.sh
```

## Repository layout

```
crates/
  ematix-flow-core/         engine (SQL planner, operators, kernels)
  ematix-flow-cli/          `flow` binary
  ematix-flow-distributed/  Arrow Flight peer mesh + distributed exec
  ematix-flow-py/           PyO3 bindings — published as `ematix-flow` on PyPI
docs/
  PHASE_*.md                phase plans (closure work, refactors)
  plans/CURRENT.md          active work plan (one at a time)
  progress/CURRENT.md       active progress log
scripts/
  pgo/                      PGO pipeline (Story 1.1 / 1.2)
  tpch-bench-multi.sh       cluster-mode benchmark driver (Σ.C)
  bench-tpch-polars.py      Polars 22q reference numbers
  bench-tpch-pyspark.py     PySpark 22q reference numbers
bench-results/              published bench artifacts per release
examples/tpch/              TPC-H queries + SF=1 data
```

## Common contributor workflows

| Task                                  | Command                                          |
|---------------------------------------|--------------------------------------------------|
| New feature + tests                   | `cargo test -p ematix-flow-core <test_name>`     |
| Lint fix                              | `cargo clippy --fix --release --all-targets`     |
| Format                                | `cargo fmt`                                      |
| Q-N performance loop                  | `cargo run --release -p ematix-flow-core --example tpch_q14_late_mat_bench` |
| Σ.O.c decode-cache hit-rate logging   | `EMAT_RG_DECODE_CACHE=1` env flag                |
| Robin Hood SUM(f64) on hot agg paths  | `EMAT_RH_SUM_F64=1` env flag                     |

## See also

- `CLAUDE.md` — agent-facing project rules (TDD, fewer-bigger PRs, no
  pandas in warehouse path, etc.).
- `docs/PHASE_SIGMA_T_SF10_WEAKNESS_CLOSURE_V5.md` — current
  performance roadmap.
- `docs/plans/CURRENT.md` — active plan (Phase 1 PGO is here).
