# TPC-H benchmark harness

Σ.A1's data + queries directory. The data is regenerated locally; only
the queries + this README are committed.

See [`docs/PHASE_SIGMA_PLAN.md`](../../docs/PHASE_SIGMA_PLAN.md) for the
surrounding plan + acceptance criteria.

## Generate data

```sh
# SF=1 → ~1 GB; ~3s on M3, ~10s on a typical CI runner.
cargo run --release -p ematix-flow-core --example tpch_generate -- \
    --sf 1 --out examples/tpch/data/sf1
```

Output: one Parquet file per TPC-H table under `data/sf<N>/`:

```
examples/tpch/data/sf1/
├── customer.parquet
├── lineitem.parquet
├── nation.parquet
├── orders.parquet
├── part.parquet
├── partsupp.parquet
├── region.parquet
└── supplier.parquet
```

Snappy-compressed; matches the format the upstream TPC-H reference
publishes. **Idempotent** — re-running skips tables whose Parquet
file already exists. Delete `data/sfN/` to regenerate.

The data directory is in `.gitignore`. Never commit it (SF=1 is 1 GB).

## Run the smoke test

```sh
cargo test -p ematix-flow-core --test tpch_smoke --release
```

Hermetic: generates LineItem in-process, registers it with DataFusion,
runs Q6, asserts revenue matches the TPC-H reference value. Doesn't
read from `data/sfN/` — those are for the criterion benches.

## Run the criterion benches

Reads from generated Parquet under `data/sf1/` (must run the generator
above first) and benchmarks Q1 / Q3 / Q6 / Q19:

```sh
# Full suite (~90s wall-clock at default measurement_time=20s).
cargo bench -p ematix-flow-core --bench tpch

# Single query for fast iteration.
cargo bench -p ematix-flow-core --bench tpch -- q06

# Save / compare against a named baseline.
cargo bench -p ematix-flow-core --bench tpch -- --save-baseline mine
cargo bench -p ematix-flow-core --bench tpch -- --baseline mine
```

Tunable env vars:
- `TPCH_DATA_DIR` — path to the SF=1 directory if you've put it
  somewhere other than `examples/tpch/data/sf1`.
- `TPCH_MEASUREMENT_TIME_S` — override criterion's per-query
  measurement window (default 20s/30s/60s depending on query).
  Useful for fast iteration (`=5`) or CI noise mitigation (`=120`).

Σ.A1 baseline numbers + acceptance criteria + when-to-re-run guidance
live in [`docs/BENCHMARKS.md`](../../docs/BENCHMARKS.md).

## Queries

The 22 TPC-H spec queries are bundled into the `tpchgen` crate (used by
the smoke test + future benches via `tpchgen::q_and_a::queries::Q*`).
Only the queries we actually exercise are checked into `queries/` as
human-readable `.sql`:

| File | Use site |
|---|---|
| `q01.sql` | Σ.A1 PR 2 criterion bench |
| `q03.sql` | Σ.A1 PR 2 criterion bench |
| `q06.sql` | Σ.A1 PR 1 smoke test + PR 2 criterion bench |
| `q19.sql` | Σ.A1 PR 2 criterion bench |

The remaining 18 queries live in `tpchgen::q_and_a::queries::Q*` until
Σ.A2 / Σ.C exercises the full TPC-H suite.

## Reference answers

`tpchgen::q_and_a::answer(N)` returns `Some(&'static str)` with the
SF=1 reference value for query N (1-22), exactly as published by
TPC.org. Used in the smoke test for the SUM correctness assertion.

## Why not `dbgen`?

The canonical [TPC-H `dbgen`](https://github.com/electrum/tpch-dbgen) is
the C reference implementation. We use the pure-Rust
[`tpchgen-rs`](https://github.com/clflushopt/tpchgen-rs) crate instead:

- Byte-for-byte equal to `dbgen` (verified per-checkin in tpchgen-rs's
  upstream CI).
- ~10× faster than `dbgen` (no text parsing, no fork/exec overhead).
- No TPC.org access request needed.
- No C toolchain in the build environment.

Apache 2.0 licensed; matches the workspace.

### Arrow ABI note

`tpchgen-arrow` 2.0.2 pins `arrow ^57.1`; this workspace is on
`arrow 58` (driven by orc-rust 0.8 / deltalake 0.32 / parquet 58).
We use the core `tpchgen` crate (no arrow deps) and route through CSV
→ `arrow_csv::ReaderBuilder` to land in the workspace's pinned ABI.
~10–20s overhead at SF=1, acceptable for tests + benches. Re-evaluate
when tpchgen-arrow ships an arrow-58 release.
