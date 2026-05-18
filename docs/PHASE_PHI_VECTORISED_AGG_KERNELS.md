# Φ — Vectorised aggregate kernels

**Goal:** push the hot loops of `FusedAggregateExec` (from [[PHASE_SIGMA_G_GENERIC_FUSED_AGGREGATE]])
down to type × agg-op kernels parameterised at compile time, using
the same NEON / AVX-512 specialisation discipline that
`ematix-parquet` already uses for decode.

**Why this is its own phase:** Σ.G dispatches shapes to a single
operator. The operator's *body* is still mostly type-erased Arrow
kernels — a `SUM` over `i64` uses `arrow-arith::aggregate::sum`,
which is good but not SIMD-fused with the upstream filter/group.
Φ collapses the filter → group → accumulate path into hand-vectorised
kernels per (type, op) combo, the way ematix-parquet already does
decode + predicate fusion.

**Non-goal:** match Photon's full breadth (thousands of kernels).
Φ targets the **6-8 most-common type × op combos** that cover ≥80%
of real-world aggregates, plus a stub for falling back to Arrow
kernels for the long tail.

## What the kernel matrix actually looks like

Compile-time-specialised hot loops parameterised by:

| Axis | Values |
|---|---|
| Input type | `i32`, `i64`, `f64`, `Decimal128`, `Date32`, `Dictionary<UInt32, Utf8>` |
| Agg op | `SUM`, `COUNT`, `COUNT(DISTINCT)`, `MIN/MAX`, `AVG`, `conditional COUNT (CASE WHEN)` |
| Group cardinality | 0 (no group), small (≤256), medium (≤64K), large (hash) |
| Filter | none, dense bitmap, sparse selection vector |

Full Cartesian = 6 × 6 × 4 × 3 = ~430. Photon ships them all.
**We ship the 30-40 that appear in our hand-coded shapes + are
common in real workloads** — let cargo monomorphise the rest from a
generic baseline.

The 30-40 priority set (informed by the SF=1 BENCHMARKS.md from the
2026-05-17 campaign):

```
[i64, f64] × [SUM, COUNT, conditional COUNT] × [small group, medium group]
                                              × [dense bitmap filter, no filter]
+ [Dictionary<UInt32, Utf8>] × [COUNT] × [small group, medium group] × any filter
```

Covers Q1 (`SUM/COUNT over 4-group lineitem`), Q3 (`SUM over orders+customer`),
Q5 (`SUM over 6-way join with region group`), Q6 (`SUM(disc*price) lineitem`),
Q12 (`conditional COUNT lineitem`), Q14 (`conditional SUM lineitem`).

## Where the kernels live

New crate: `crates/ematix-flow-kernels/`

```
crates/ematix-flow-kernels/
├── Cargo.toml          # workspace dep, no_std-compatible
├── src/
│   ├── lib.rs           # public API
│   ├── sum.rs           # SUM kernels per type × group × filter
│   ├── count.rs         # COUNT + conditional COUNT
│   ├── minmax.rs        # MIN/MAX
│   ├── dict_count.rs    # Dictionary group-key COUNT
│   ├── arch/
│   │   ├── neon.rs       # AArch64 NEON (M-series local dev)
│   │   ├── avx2.rs       # x86_64 baseline (CI + most cloud)
│   │   └── avx512.rs     # x86_64 Sapphire Rapids+ (the c7i target)
│   └── runtime_dispatch.rs  # std::is_x86_feature_detected!
```

Reuses ematix-parquet's existing `arch/` discipline. The decode-side
kernels (in `ematix-parquet-codec`) consume the column chunks; the
aggregate-side kernels (this crate) consume the resulting Arrow
arrays. No cross-dependency between them.

## Phases

### Φ.1 — `SUM(i64)` + `SUM(f64)` over small groups (~2 wk)

Cheapest viable kernel set: the inner loop of Q1, Q3, Q5, Q6.
Group cardinality ≤256, no filter (filter applied upstream).

- Implement `sum_i64_small_groups_neon` / `_avx2` / `_avx512`
- Implement `sum_f64_*` variants
- Wire into `FusedAggregateExec`'s hot loop via the runtime
  dispatch trait
- **Gate:** Q1/Q3/Q6 SF=1 numbers improve ≥10% on c7i (the
  AVX-512 path); no regression on Q1/Q3/Q6 on M-series (NEON path)

### Φ.2 — `conditional COUNT (CASE WHEN)` + `SUM` with bitmap filter (~2 wk)

Q12's two `COUNT(CASE WHEN ... THEN 1 END)`. Q14's
`SUM(CASE WHEN type LIKE 'PROMO%' THEN price * (1-discount) END)`.

The filter is the inner CASE predicate; the kernel evaluates the
predicate + accumulates in one pass instead of building a separate
mask vector.

- `count_conditional_i64_*` (predicate over an i32/i64 input)
- `sum_conditional_f64_*` (predicate over a Utf8 LIKE pattern that's
  been pre-resolved to a row bitmap)
- Wire to Q12 + Q14 fused operators
- **Gate:** Q12 + Q14 improve ≥15% on c7i; M-series neutral or better

### Φ.3 — Dictionary-keyed COUNT for [[dict-arrival-blocker]] follow-up (~2 wk)

The Σ.E3b `DictGroupCountExec` operator's hot loop runs in plain
Rust today. With Φ.3 it gets a vectorised kernel that:
- Reads the `keys: UInt32` array
- Looks up into a `counts: [u64; N]` accumulator where N = dict size
- Uses gather instructions (AVX-512 VPGATHERDD) for the count update
- NEON fallback uses scalar gather (no NEON gather instruction, but
  the loop is tight enough to be competitive)

- **Gate:** kernel bench shows ≥2× on c7i vs current scalar
  `DictGroupCountExec` (the 2.17× already achieved on M-series is
  the baseline; we want to keep it on x86 + ideally improve)

### Φ.4 — `MIN/MAX` + `AVG` + `COUNT(DISTINCT)` (~3 wk)

Round out the common aggregate suite. `MIN/MAX` is straightforward
SIMD horizontal-reduce. `AVG` is `SUM/COUNT` — already covered.
`COUNT(DISTINCT)` is the hard one — uses HyperLogLog. Worth checking
if Arrow's existing `hash_aggregate` is competitive enough that we
skip HLL for now.

- Defer HLL if Arrow's path is within 1.5× of the win we'd get from
  doing our own.

### Φ.5 — Codegen path for new combos (stretch, ~4 wk)

Some kernels (per-bitwidth × per-type combos) explode combinatorially
if we keep hand-writing them. Borrow the `seq-macro` /
`const-generic` pattern from ematix-parquet's bit-unpacker:

```rust
seq_macro::seq!(N in 1..=64 {
    pub fn sum_i64_filtered_bw_#N(values: &[i64], filter: &[u64]) -> i64 {
        // monomorphised for each bitwidth
    }
});
```

Lets us cover the matrix without hand-writing 50+ versions of the
same loop. The cost is compile time — ematix-parquet already pays
this for its decode jumptable; one more crate doing it is bounded.

- **Gate:** total `cargo build --release -p ematix-flow-kernels`
  stays under 60 sec on c7i.2xlarge.

## Risks

1. **Kernel bug = wrong result** — vectorised arithmetic is famously
   easy to get subtly wrong (lane-masking, overflow, NaN handling).
   Mitigation: every kernel has a fuzz test vs the equivalent
   scalar loop; CI runs the fuzz harness on PR.
2. **Compile time explosion** — each new monomorphisation adds to
   codegen. Mitigation: keep `cargo check` benchmarks on every kernel
   PR; if `-p ematix-flow-kernels` build exceeds 60 sec, refactor
   towards trait dispatch (slower runtime, faster compile).
3. **Runtime dispatch overhead** — `std::is_x86_feature_detected!`
   has a one-time cost per call site. Mitigation: detect once per
   `ExecutionContext` startup, cache the function pointer table on
   the context. Same pattern as ematix-parquet's
   [parquet_dispatch.rs](https://github.com/...).
4. **Φ doesn't move SF=10 numbers** — if the SF=10 SF-scaling
   regression observed in the 2026-05-17 campaign (Q01: ematix-flow
   2.5× slower than DuckDB at SF=10, vs 1.7× at SF=1) is *not*
   kernel-bound but planner/cardinality-bound, Φ won't fix it.
   Mitigation: profile SF=10 Q01 + Q03 on c7i.2xlarge BEFORE
   starting Φ.1 — confirm hot path is the aggregate inner loop, not
   join build or partition repartition.

## What we publish

- `docs/BENCHMARKS-KERNELS.md` — per-kernel microbenches at the same
  format as ematix-parquet's `BENCHMARKS.md`. NEON vs AVX-2 vs
  AVX-512, scalar baseline, expected speedup table.
- TPC-H BENCHMARKS.md updates from each gate
- A README section: "How ematix-flow's aggregate path stays
  vectorised: from decode through group through accumulate." This is
  the doc that addresses "you're tuned to TPC-H" honestly — Φ-track
  kernels are *generic per type × op*, not per query.

## Decisions to lock before starting

| # | Question | Default |
|---|---|---|
| 1 | New crate vs adding to ematix-flow-core | **New crate.** Keeps the kernel set independent of datafusion/Arrow versions, and lets ematix-parquet eventually depend on it (today the dep direction is one-way: flow → parquet). |
| 2 | NEON-first or AVX-first | **NEON first** for local-dev iteration; AVX-2 + AVX-512 immediately after. Match ematix-parquet's pattern. |
| 3 | Test discipline | Fuzz vs scalar oracle, ≥1M random inputs per kernel, run in CI. Same as ematix-parquet. |
| 4 | Naming convention | `<op>_<type>_<group-shape>_<filter-shape>_<arch>` — e.g. `sum_i64_small_dense_filter_avx512`. Verbose but unambiguous. |
| 5 | Where AVX-512 detection happens | At `ExecutionContext` construction. Plumb a `KernelDispatch` handle through the operator tree. |

## Engineering total

Φ.1 + Φ.2 + Φ.3 = **~6 wk** for the deterministic part.
Φ.4 (mostly mechanical) = **~3 wk**.
Φ.5 codegen path = **~4 wk** but it's a stretch enabler.

Total core: **~9 wk** to ship the kernel matrix that covers our
top-6 TPC-H shapes + the [[dict-arrival-blocker]] follow-up.

## What we DON'T do in Φ

- **String-side aggregate** (e.g. `MIN(varchar)`) beyond the
  dictionary key path. Real string aggregation is its own can of
  worms (UTF-8 collation, locale, etc.). Stays Arrow-default.
- **Floating-point reproducibility** — SIMD reduction order isn't
  associative for f64. Document the non-determinism; don't try to
  fix it. Real users hitting this need Kahan summation, which is a
  separate kernel set (Φ.6 if there's demand).
- **Cross-NUMA scaling** — kernels stay single-thread per call site;
  parallelism is the operator's job (which already uses rayon per
  Π.15). Cross-NUMA pinning is a separate kernel concern.

## What this unblocks

After Φ ships:

- "ematix-flow beats DuckDB/Polars on **non-TPC-H** aggregate
  workloads" becomes a credible claim
- Σ.G.4's shape-detection rule can fire on broader patterns because
  it no longer needs to recognise a hand-coded operator — the kernel
  is generic
- Lambda/EKS worker pods (Phase Z distributed campaign) inherit the
  same kernel set transparently — no per-worker tuning needed

## Sequencing relative to other phases

| Phase | When | Why |
|---|---|---|
| Σ.G | Now (after AWS campaign) | Hygiene + maintainability; no perf change |
| **Φ.1-Φ.3** | **Immediately after Σ.G** | Actual perf wins on existing + new workloads |
| Π (spilling) | Parallel to Φ if separate engineer | Independent surface; production-readiness |
| Ψ (stats) | After Φ.3 | Φ.3 + better stats unlock Σ.G.4's cost-based dispatch |
| Distributed campaign | Parallel to Φ | Independent surface; per-shard work benefits from Φ |
| Φ.4-Φ.5 | Polish, lower priority | Diminishing returns vs Π and Ψ |
