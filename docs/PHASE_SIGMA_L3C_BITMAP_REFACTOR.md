# Σ.L.3.c — sequential-AND bitmap refactor for adaptive predicate reorder

## Why this plan exists

Σ.L.3 ships the `AdaptiveFilterOrder` data structure that tracks per-
predicate pass rates across row groups and computes a selectivity-sorted
order. Σ.L.3.b wires this into `BridgeFilter` as an opt-in field with
`observe_row_group` / `applied_order` accessors.

**But the data structure is information-only today.** The actual filter
hot path in `BridgeFilter::build_bitmap` (`crates/ematix-flow-core/src/
ematix_fast_parquet.rs:177`) builds **every** per-predicate bitmap in
parallel and AND-s them at the end. Reordering doesn't change the CPU
work — every predicate's column gets decoded fully even when an earlier
predicate would have eliminated the row.

To turn `AdaptiveFilterOrder` into a **real** perf win, the bitmap
construction has to switch from parallel-AND to one of two architectures:

1. **Short-circuit AND** — keep parallelism but cancel late predicates
   once the running AND of completed predicates approaches zero.
2. **Sequential masked-AND** — apply the most-selective predicate first,
   use its bitmap as a *decode mask* for subsequent columns so they
   only materialize rows that already passed.

This document is the implementation plan for landing one or both.

## Background — current hot path

```rust
// ematix_fast_parquet.rs line 177
pub fn build_bitmap(&self, path: &Path, rg: usize) -> DfResult<(Vec<u8>, usize)> {
    let mut combined: Option<(Vec<u8>, usize)> = None;
    for p in &self.predicates {                  // <-- iterates ALL predicates
        let (b, total) = match p { ... };        // <-- builds full bitmap each
        combined = Some(and(combined, (b, total))); // <-- AND incrementally
    }
    ...
}
```

The streaming reader's filter path then masked-decodes each *projection*
column against `combined`. Decode work for the projection columns is
already gated by the bitmap. The waste is in the **predicate columns**
themselves — every predicate column gets fully decoded even if rows are
about to be eliminated.

### Where the waste shows up

For Q19 (3-branch OR of (string Eq + range)):
- `p_brand` (string Eq) selectivity ~4% per branch — very selective
- `l_quantity` (BETWEEN) selectivity ~33% per branch — much less

If we built the `p_brand` bitmap first, then used it as a decode mask
when reading `l_quantity`, we'd skip 96% of `l_quantity` decode work
per branch.

Q06 (l_shipdate range + l_discount range + l_quantity range): all three
predicates contribute to filtering, but the cheapest+most-selective
applied first eliminates the most rows before the expensive decoders
even start.

## Architecture options

### Option A: short-circuit parallel AND

**Idea:** keep the existing parallel `tokio::spawn` setup but install a
shared `Cancellation` token. After each predicate finishes its bitmap,
update a running AND estimate; if the population of the running AND
falls below some threshold (e.g., 0.1% of input rows), cancel the
remaining predicates' decode tasks.

**Pros:**
- Minimal disruption to the existing parallel-decode infrastructure.
- Recovers some of the AdaptiveFilterOrder win without giving up
  parallelism.

**Cons:**
- Cancellation only helps after the first few predicates have already
  done work. Doesn't address the "decode the WHOLE selective column
  before AND-ing" problem.
- Adds tokio cancellation plumbing to a hot path. Easy to leak.
- The estimator that decides "running AND is too sparse" has to be
  cheap — every bit count of a multi-MB bitmap is overhead.

**Expected win:** 5-15% on Q06/Q19; minimal elsewhere.

### Option B: sequential masked-AND

**Idea:** apply predicates in the order `applied_order()` returns. The
first predicate decodes its column fully and produces a bitmap. The
second predicate uses that bitmap as a decode mask — we already have
`masked_decode_i32` / `masked_decode_f64` / `masked_decode_string` in
`ematix_parquet_bridge`. Subsequent predicates skip-decode rows that
have already been eliminated.

**Pros:**
- The win scales with cumulative selectivity. If the first predicate
  keeps 5% of rows, the second predicate decodes 5% of its column —
  20× less work.
- Cleanly composes with `AdaptiveFilterOrder`: predicate ordering is
  the central choice.
- Mirrors what Velox / Photon do publicly.

**Cons:**
- Gives up parallelism across predicates. Single-threaded predicate
  evaluation.
- "Cheapest column" and "most selective predicate" can conflict.
  Decoding a 32-byte string column once is cheaper than decoding a
  4-byte i32 column once, regardless of pass rate. Need a cost model.
- masked_decode kernels have higher per-row overhead than full decode
  + AND; only wins when the mask has low density.

**Expected win:** 20-40% on filter-heavy queries (Q06/Q19/Q14); 0%
on queries with no/single predicate.

### Option C (recommended): hybrid driven by selectivity prediction

**Idea:** at plan time, predict cumulative selectivity. If predicted
cumulative selectivity after the first predicate is below threshold T
(say, 30%), use Option B; otherwise use today's Option-0 (parallel
all-decode). The shape catalog already has Σ.E5 Phase 1.8's
selectivity predictor; reuse it.

The `AdaptiveFilterOrder` then refines the choice as queries run —
if observed selectivity disagrees with prediction, the predictor's
calibration shifts.

**Pros:**
- Pays the sequential-AND cost only when the win is real.
- Existing parallel path stays for cases where it's better.
- Naturally folds into the auto-pick framework that already gates
  page-streaming vs eager-decode.

**Cons:**
- Two code paths to maintain.
- Auto-pick threshold tuning needs bench data per query class.

**Expected win:** 15-30% geomean on filter-heavy subset; ~0% on
already-fast queries. **No regressions** because the auto-pick
guarantees we only switch when predicted win.

## Implementation plan — Option C

### Σ.L.3.c.1 — masked predicate evaluators

Today's `ematix_parquet_bridge` has `masked_decode_i32` etc. for
*projection columns*. Add masked variants for predicate evaluation:

- `filter_i32_column_masked(path, rg, col, mask, pred) -> bitmap`
- `filter_f64_column_masked(path, rg, col, mask, pred) -> bitmap`
- `filter_byte_array_masked(path, rg, col, mask, pred) -> bitmap`

Input `mask` is the AND-so-far bitmap. Output bitmap has bits ONLY at
positions that were in `mask` AND passed the predicate.

Test: equivalence with `filter_*` followed by AND with `mask`.

Effort: ~150 LOC of bridge code + ~6 tests.

### Σ.L.3.c.2 — sequential-AND path in BridgeFilter

New method `build_bitmap_sequential(path, rg)` that:

1. Iterates `applied_order()` (most-selective first).
2. First predicate: full decode + bitmap (current path).
3. Subsequent predicates: masked decode using accumulated bitmap.
4. Returns final bitmap + observe stats.

Test: produces identical bitmap to `build_bitmap()` on the same input.

Effort: ~200 LOC + ~8 equivalence tests.

### Σ.L.3.c.3 — auto-pick in build_bitmap

`build_bitmap` becomes:

```rust
pub fn build_bitmap(&self, path, rg) -> ... {
    if self.should_use_sequential() {
        self.build_bitmap_sequential(path, rg)
    } else {
        self.build_bitmap_parallel(path, rg)  // current impl
    }
}

fn should_use_sequential(&self) -> bool {
    self.adaptive.is_some()
        && self.predicates.len() >= 2
        && self.predicted_pass_rate() <= 0.30
}
```

Auto-pick threshold tuned via the 22q bench. Per-query telemetry to
`workload_log` lets Σ.L.5 surface "switching to sequential-AND saved
X% on this query" recommendations.

Effort: ~50 LOC.

### Σ.L.3.c.4 — bench gate

New example `adaptive_filter_22q_gate.rs` mirroring
`dict_arrival_22q_gate`. Two configurations: parallel-only baseline
vs adaptive-enabled. Pass criteria:

- geomean ≤ 0.97 (3% improvement over noise floor)
- No single query regresses > 2%
- Filter-heavy subset (Q06, Q14, Q19) shows ≥ 10% improvement each

Effort: ~150 LOC.

## Sequencing + dependencies

- L.3.c.1 (masked evaluators) — blocks .c.2.
- L.3.c.2 (sequential path) — blocks .c.3.
- L.3.c.3 (auto-pick) — must land *with* the bench gate.
- L.3.c.4 (bench gate) — gates merge.

Total effort estimate: **2-3 days of focused work**.

## Risks + mitigations

- **Risk**: masked decoders are slower per-row than full decoders, so
  if the mask is dense (>50% pass), sequential-AND loses.
  - **Mitigation**: auto-pick threshold; never engage when predicted
    cumulative selectivity > 30%.

- **Risk**: tokio runtime fragmentation as the parallel path becomes
  one-of-two instead of always.
  - **Mitigation**: keep parallel path as default; sequential is an
    explicit opt-in via the same `with_adaptive_reordering` flag that
    Σ.L.3.b already gates on.

- **Risk**: predicate evaluation order affects predicate fingerprint
  (Σ.L.4's `filter_fingerprint`).
  - **Mitigation**: fingerprint already sorts predicates before hashing
    (Σ.L.4 scan_cache.rs), so order changes don't invalidate the cache.

- **Risk**: codegen tax from adding a method branch (per
  [[optimizer-codegen-sensitivity]]).
  - **Mitigation**: this is inside the bitmap-build function, not a new
    optimizer rule. The 7% tax applies to optimizer-pass additions; not
    to method-internal branches.

## When to land this

**Not before tomorrow's distributed bench.** Reason: Σ.L.3.c only
helps single-node and filter-heavy queries; distributed bench is the
priority window. Land Σ.J.2 (cross-stage bloom) wiring first if time
on a distributed-bench day; circle back to Σ.L.3.c when single-node
gate becomes the bottleneck again.

## Connection to broader Σ.L roadmap

| Phase | Status | This plan's impact |
|-------|--------|--------------------|
| Σ.L.1 | done | unchanged |
| Σ.L.2 | done | gains a new metric: per-query sequential-vs-parallel chosen path |
| Σ.L.3.b | done | this is the wire-in target |
| Σ.L.3.c | **THIS DOC** | turns Σ.L.3.b into a real perf win |
| Σ.L.4 | scaffold | unaffected |
| Σ.L.5 | scaffold | gains a "consider larger row groups + bloom on column X" rule when sequential-AND is the picked path |

[[sigma-l-adaptive-runtime]] supersedes the Σ.L.3 entry once this lands.
