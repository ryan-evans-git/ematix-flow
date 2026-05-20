# Σ.H — Filter + HashJoin + Aggregate

**Status:** kicked off 2026-05-20 on `feat/sigma-f-shape-catalog`.

**One-line goal:** let `InjectFilterMultiAggRule` route the
aggregate stack over a `HashJoinExec` body, not just `Filter > Scan`,
so the existing `FilterMultiAggSpec` consumes joined rows instead of
DataFusion's default `AggregateExec`. Covers 80+ queries across TPC-H
and TPC-DS where the join sits between the aggregate and the scan.

## Background

Σ.G inventory: **121/125** TPC-H + TPC-DS queries have a
`HashJoinExec`. The current Σ.F catalog rules only fire on the 16
queries where there's no join between the aggregate and the scan.
Σ.G concluded the join is the dominant gating shape; this phase is
the response.

Per Σ.G's recommendation, Σ.H ships in two variants:

- **Σ.H.1 (cheap, this PR):** weaken the rule's "single-child chain
  to leaf" rejection so it also accepts `HashJoinExec` as the body.
  The `FilterMultiAggSpec` consumes batches from the join's output
  unchanged. No new executor.
- **Σ.H.2 (aggressive, follow-up phase):** dict-aware probe-side
  join — compose with dict-arrival decode → dict-coded probe key →
  translate-once cache. ~2-3 weeks. Gated on Σ.H.1 numbers.

## Discovery from real plan dumps (Q3 / Q5 / Q10)

`crates/ematix-flow-core/examples/sigma_h_plan_dump.rs` emits each
target query's actual physical plan via DataFusion's standard
displayable. Findings:

- All three have the same outer wrapper as Q01:
  `SortPreservingMergeExec > SortExec > ProjectionExec >
  AggregateExec(FinalPartitioned) > RepartitionExec(Hash) >
  AggregateExec(Partial) > [opt ProjectionExec] > HashJoinExec`.
- The optional `ProjectionExec` between `Partial` and the join is
  a column-reorder, not a CSE alias. The aggregate references its
  inputs by index into that projection's output schema, which in
  turn names columns from the join's output. Column resolution by
  *name* works through the projection cleanly.
- The catalog matcher (`filter_multi_agg_shape` post-Σ.I.1) already
  matches this entire wrapper. The only gate that rejects is
  `is_passthrough_chain_to_leaf(&scan)` in
  `try_build_replacement` — it returns `false` at the join's first
  multi-child node.

## The cheap-variant change (Σ.H.1)

Single helper edit in `fused_aggregate_filter_multi_agg_rule.rs`:

```rust
// Before: rejects on any multi-child node.
fn is_passthrough_chain_to_leaf(node) -> bool { ... }

// After: also accepts HashJoinExec as a multi-child stopping point,
// recursing into both sides. NestedLoopJoinExec / CrossJoinExec /
// UnionExec still reject — they have semantics or perf shapes the
// current spec wasn't designed for.
fn is_supported_body(node) -> bool { ... }
```

That's the entirety of the source change. Everything else (column
resolution, aggregate extraction, group-key resolution, JIT spec
construction) works through the join because every input column the
spec needs has a name in the join's output schema.

## Risk and guard rails

- **Schema resolution may fail on edge cases.** If the join's
  output schema doesn't contain a column the spec needs by name,
  `FilterMultiAggSpec::try_new` returns Err and the rule bails to
  DataFusion's default. Safe — never wrong-answer.
- **Performance might not move much on Q3/Q5/Q10.** The bottleneck
  in join-heavy queries is the join itself, not the aggregate. If
  the aggregate is 10% of total runtime, even doubling its speed
  is a 5% end-to-end win. The bench gate decides whether Σ.H.1 is
  worth shipping standalone or only as scaffolding for Σ.H.2.
- **CSE projection misinterpretation.** The catalog captures
  `cse_projection` whenever a Projection appears between the
  Partial aggregate and its body. For Q10 that's a column-reorder
  Projection, not a CSE. `extract_aggregates(..., cse_projection)`
  may need a defensive check — if the projection's aliases don't
  match the `__common_expr_N` pattern, treat it as a no-op
  passthrough.

## Bench gate

3-run multi-bench (Σ.F.2 methodology), Σ.H.1 head-to-head vs
v0.3.0 (current main):

**Pass criteria:**
- Per-query: target queries (Q3 / Q5 / Q7 / Q8 / Q10 / Q11 / Q21)
  improve by ≥ 5% on the median, OR all stay within ±3%.
- 22-query geomean: stays within ±2% of v0.3.0 (no broad regression).

**Fail mode:** if Σ.H.1 regresses any TPC-H query by > 5%, dig
into the plan diff for that query and either narrow the rule's
acceptance condition or revert.

## What follows if Σ.H.1 passes

- **Σ.H.2 — dict-aware probe-side join exec.** Compose with the
  dict-arrival substrate from Σ.E5 (currently single-threaded,
  decode-time). New `DictHashJoinExec` (or extension to
  `FusedAggregateExec` that consumes a join shape natively).
  2-3 weeks, new bench gate.
- **Σ.J — `WindowAgg` shape.** Σ.G flagged 17/103 TPC-DS queries
  with windows. Independent of Σ.H.

## What follows if Σ.H.1 fails

- Document why each regressing query regressed (most likely:
  CSE-projection misinterpretation, or per-row work added by the
  fused spec exceeds DataFusion's per-batch aggregate cost on
  small post-join row counts).
- Decide whether to ship a narrowed version (e.g. only fire when
  estimated post-join row count is > N) or skip directly to Σ.H.2.

## Non-goals

- Not a new executor. `FusedAggregateExec<FilterMultiAggSpec>`
  unchanged; only the rule's matcher acceptance loosens.
- Not a join optimisation. The HashJoin runs unchanged; we just
  consume its output more efficiently.
- Not a dict-aware lever. Σ.H.2 is where dict-coded probe keys
  land. Σ.H.1 routes Utf8View / Dictionary group keys (whatever
  arrives from the join) through the existing spec templates.
