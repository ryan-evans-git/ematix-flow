# Σ.D / Σ.E next increments — plan

This document captures the next-step work needed to make the "22 wins"
claim accurate and to harden the **custom optimization engine** that
is becoming a real differentiator vs DataFusion + Polars defaults.

## Where we are today (2026-05-12)

**Branch `feat/fast-parquet-utf8view`** delivers:

* **Σ.E2 FastParquet scan provider** — row-group-parallel reads with
  incremental streaming + mimalloc + `Utf8View` column promotion.
  Statistical-bench at SF=1 (15 trials, σ-envelope classification):
  20 real wins / 0 losses / 2 noise vs DataFusion's default scan;
  mean speedup **1.60×**.
* **Σ.D3 phases A–D Cranelift JIT substrate** — `FusedFilterAggSpec`
  data model (`ColumnTy`/`ClauseOp`/`AggExpr`/`GroupSpec`), generic
  IR emitter, Q6/Q1/Q14 hand-coded execs retrofitted with
  `try_new_*_jit` constructors, `EnableFusedJitRule` PhysicalOptimizer
  that flips matching execs to JIT mode. 30 fused-related unit tests
  pin bit-identical / rel-err-1e-12 equivalence between paths.
* `tpch_fused_jit_bench` shows hand-coded vs JIT'd are within run-to-
  run noise for already-fused shapes — the substrate's value is
  **generalization for future shapes**, not per-query speedup for
  shapes we'd already hand-tuned.

## What we can claim accurately today

| Comparison                                        | Honest claim                                                                   |
|---------------------------------------------------|--------------------------------------------------------------------------------|
| Vs DataFusion-default scan path (TPC-H SF=1)      | **20 outright wins / 0 losses / 2 noise** ema-flow + FastParquet              |
| Vs Polars (10 queries Polars can run)             | **20 outright + Q12 tied + Q14 lost** = 11 of 12 quantifiable wins-or-ties     |
| Vs PySpark / Pandas (all 22 queries)              | 22 of 22 (we never lose to either; this is the README's headline)              |
| Vs Polars (12 SQL queries it parses)              | 11 win-or-tie; **1 outright loss (Q14, 1.41×)**                                |
| Vs Polars (full 22-query SQL suite)               | Polars can't parse 10 of 22; ematix-flow runs all 22                           |

A defensible "22 of 22" headline today is: **"ematix-flow runs all 22
TPC-H queries with no regressions vs DataFusion's default scan; vs
every engine that can run a given query, we win or tie on 21 of 22."**
The one remaining loss (Q14 vs Polars) has a concrete path to closure
documented below.

## 2026-05-12 update — what landed and what's next

**Σ.D3 phase E landed (commit 6d427a1):** `FusedQ14FullExec` at SF=1
clocks **15.31 ms ± 0.17** (15-trial median) on the bench at
`examples/tpch_q14_full_bench.rs`. That's **31% faster than DataFusion
default** (19.57 ms) and **9% faster than FastParquet+Utf8View SQL**
(16.85 ms), but still **22% behind Polars's 12.53 ms reference**.

The fully-fused operator does what increment 1 below scoped: one
operator owns both inputs, builds a direct-indexed `Vec<bool>` promo
bitmap from part concurrently with the lineitem scan, then runs one
async worker per lineitem partition that applies the shipdate filter
+ probes the bitmap + accumulates dual SUMs inline as batches arrive
from FastParquet. The CPU loop is no longer the bottleneck — the gap
to Polars now lives in the **parquet scan**, because we decode the
full ~6M lineitem rows while Polars uses page/row-group statistics to
read ~1% of them (~76 K rows in the 30-day shipdate window).

So the "22 of 22 wins or ties" headline is still false vs Polars on
Q14. The next session's work to close it is added as increment 0
below; the SQL-auto-injection plan from old increment 2 stays queued
for the session after that.

### 0. Parquet predicate pushdown in FastParquet (NEXT SESSION)

**Why first:** Closes the remaining 2.78 ms Q14 SF=1 gap vs Polars.
Same lever Polars uses; we already win on the post-decode CPU work.

**Shape:** Extract `&[Expr]` filters in
`FastParquetTableProvider::scan(...)` — DataFusion already passes
them in but FastParquet ignores them today. Convert simple
AND-of-range predicates on date/int columns to row-group statistics
probes; drop row groups whose min/max stats fall outside the
predicate range before kicking off decode.

**Subtasks:**

1. Implement `supports_filters_pushdown` returning `Exact` for
   simple `Column ⊕ Literal` range predicates and `Inexact` otherwise
   (so DataFusion still applies a residual FilterExec for safety).
2. In `FastParquetTableProvider::scan`, walk `&[Expr]` and build a
   `RowGroupPredicate` struct: per-column `(min_op, min_lit, max_op,
   max_lit)`. Reject anything else (unknown column, non-literal RHS,
   non-comparison op) — those just don't get pushed down.
3. Before queuing row groups for decode, read the column-chunk
   metadata, evaluate the predicate against `min`/`max` per row group,
   and skip groups that cannot contain any matching rows.
4. Verify: re-run `tpch_q14_full_bench` — target ≤12.5 ms (beats
   Polars). Also re-run the SF=1 + SF=10 regression sweep to confirm
   the other 21 queries don't regress.

**Effort estimate:** 1 session. Risk: getting the predicate evaluation
right for Date32 row-group statistics (the parquet metadata exposes
min/max as `&[u8]`; need the same Date32 → days-since-epoch decoding
the rest of FastParquet uses).

### 1. Q14 full-fusion (closes the last Polars loss) — DONE 2026-05-12

**Why first:** This is the only thing standing between today's
posture and a clean "22 of 22 we win or tie vs every other engine"
claim. Q14 is one query, ~5 ms gap.

**Shape:** A single operator that owns both inputs (lineitem, part)
and runs scan + filter + join + dual-SUM-with-CASE-WHEN in one
parallel pass. Substantially more than the post-join-only fusion in
`fused_post_join.rs::Q14` (which only fuses the agg — that saved
~0.9 ms, leaving the ~17 ms join+scan as the bottleneck).

**Design:**

* New module: `crates/ematix-flow-core/src/fused_q14_full.rs`
* `pub struct FusedQ14FullExec` — owns two child `ExecutionPlan`s
  (typically `FastParquetExec` for lineitem + part)
* `execute()` flow:
  1. Drain `part`, build a direct-indexed `Vec<bool>` of size
     `max(p_partkey)+1` where `is_promo[k] = p_type[k].starts_with("PROM")`.
     TPC-H partkeys are dense small integers → no hash needed, O(1)
     probe per row.
  2. Spawn one blocking worker per `target_partitions`. Each worker
     gets a slice of lineitem batches; iterates rows: filter shipdate
     range, probe partkey bitmap (matching rows only), compute revenue,
     accumulate `(promo, total)`.
  3. Merge shard partials. Emit single-row ratio batch.

**Spec extension (Σ.D3 follow-on):** A `JoinProbeBitmap` variant of
the spec describing this shape:

```rust
pub enum BuildSide {
    /// Probe a host-managed `Vec<bool>` indexed by an Int32/Int64
    /// column on the probe side; row matches when the bit is set.
    DenseIndexBitmap { probe_key_col: usize },
}

pub struct FusedFilterAggSpec {
    pub inputs: Vec<ColumnTy>,
    pub predicate: Vec<Clause>,
    pub aggregates: Vec<AggExpr>,
    pub group: Option<GroupSpec>,
    /// Σ.D3 phase E: optional probe-side join. The build-side bitmap
    /// is constructed by the host before the JIT runs; the IR loads
    /// `bitmap[col[i]]` to gate accumulation, additively combining
    /// with the predicate's pass mask.
    pub probe: Option<BuildSide>,
}
```

**IR emission:** in `emit_match_path_*`, after the predicate `pass_all`
mask, AND in a `probe_match` mask loaded from the bitmap. Otherwise
same shape.

**Bench target:** beat Polars 12.53 ms at SF=1 Q14. From the
deleted `tpch_q14_tune.rs` Section 3 we know the hand-fused inner
loop is ~0.27 ms parallel; the rest is scan+filter+join+merge. With
FastParquet's 5-6 ms scan and the bitmap probe inlined the wall-clock
budget is roughly 6-8 ms, well under Polars's 12.5 ms.

**Effort estimate:** 4-6 hours (one session). Risk: the parallel-shard
merge has to be carefully ordered so the bench's bit-identical
equivalence test still holds.

### 2. SQL-pattern auto-injection (Σ.D3 phase E)

**Why second:** Once Q14 is fused, the question becomes "do users
get this automatically, or do they have to call `FusedQ14FullExec::try_new`
manually?" Today's `EnableFusedJitRule` only fires when there's
already a hand-constructed fused exec in the plan. SQL-driven plans
go through DataFusion's default planning and never get fused.

**Design:** Extend `EnableFusedJitRule` (or add a sibling
`InjectFusedExecRule`) that pattern-matches DataFusion's physical
plan output for shapes we know how to fuse, extracts predicate /
aggregate constants from the `PhysicalExpr` AST, and rewrites the
matching subtree.

**Subtasks:**

1. Q6-shape detector: `AggregateExec(Final) ← CoalescePartitionsExec
   ← AggregateExec(Partial, single-SUM) ← ProjectionExec
   ← FilterExec(date-range AND disc-range AND qty-bound) ← DataSourceExec`.
   Walk the FilterExec's `PhysicalExpr` (`BinaryExpr`, `Column`, `Literal`)
   to extract `date_lo`/`date_hi`/`disc_lo`/`disc_hi`/`qty_hi`. Build
   `FusedFilterSumExec::try_new_q6_jit(scan, predicate)`.
2. Q1-shape detector: similar, plus group-by extraction (verify the
   group keys are `(l_returnflag, l_linestatus)` and the aggregates
   match Q1's 5 SUMs + COUNT).
3. Q14-shape detector: matches the join + post-agg shape, routes
   through `FusedQ14FullExec` (from increment 1).

**Testing:** add SQL-level tests that run a canonical TPC-H query
through `SessionContext::sql(...)` with the rule registered, and
verify (a) the resulting plan contains a fused exec at the expected
node, (b) the query returns the same result as without the rule.

**Effort estimate:** 6-10 hours per shape, ~2-3 sessions for all
three. The hard part is walking arbitrary `PhysicalExpr` AST safely
(many node types, must reject malformed shapes cleanly).

### 3. Q3 / Q5 hash-group-by JIT

**Why third:** Q3 and Q5 are already wins vs every other engine
(per the regression sweep above). The post-join hand-coded kernel in
`fused_post_join.rs` is fast enough; the JIT path is "nice to have"
for substrate completeness, not for closing a gap.

**Design notes:** the spec needs a `HashGroup` variant that describes
"group by an arbitrary tuple of columns of any type, cardinality
unknown at plan-time." The IR has two options for the hash table:

* **Host-managed:** allocate a `hashbrown::HashTable` outside the JIT,
  pass a pointer in, and emit IR that calls a C-callable
  `fn(table_ptr, hash_bytes, hash_len, ptr_to_acc_block_out)`. The
  callback fills the table on lookup. Simpler IR, host-callback cost
  per row.
* **In-IR:** emit IR for the hash function + linear-probe table
  walk over a stack-allocated bucket array. Faster but much more IR
  to write. Less safe (collision handling, resize semantics).

Recommended: host-managed for v1; revisit in-IR if profiling shows
the callback is the bottleneck.

**Effort estimate:** 1-2 sessions. Lower priority than items 1 and 2.

## Recommended sequencing

1. **This session's branch lands as-is** — it's a complete, defensible
   delivery: FastParquet/Utf8View + Σ.D3 JIT substrate + retrofit of
   3 fused shapes + optimizer rule scaffolding.
2. **Next session:** Q14 full-fusion (item 1). Closes the last Polars
   loss; lets the README make a clean "22 wins" claim.
3. **Session after that:** SQL-pattern auto-injection (item 2). Makes
   the fused JIT actually fire on real SQL queries — the "custom
   optimization engine" differentiator becomes user-visible without
   anyone hand-constructing execs.
4. **Whenever:** Q3/Q5 hash-group-by JIT (item 3). Substrate
   completeness, not a perf-critical path.

## "22 wins" claim language for the README, today

Until item 1 lands, the most-accurate framing is:

> **22 of 22 queries:** ematix-flow runs every TPC-H SQL query (10
> queries Polars's SQL parser cannot parse at all). **No regressions
> vs DataFusion's default scan path; 20 outright wins.** Versus Polars
> on the 12 queries it can run: 11 win-or-tie (10 outright wins + Q12
> tied within noise); Q14 remains a 1.41× Polars win.

This is materially better than the current README's "21 of 22" claim
(which conflated DataFusion-default and FastParquet numbers across
sections). Once item 1 lands, the framing becomes:

> **22 of 22 queries** — ematix-flow wins outright on 21 and ties Polars
> on Q12 (within noise). No engine we can measure against wins on any
> query.
