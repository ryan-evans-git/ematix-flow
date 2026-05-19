# Σ.E5 — Per-Filter Exact Pushdown Design

**Status:** design draft, 2026-05-19. No code yet.
**Author:** Claude (collab with Ryan).
**Predecessor work:** [[sigma-e5-streaming-late-mat-landed]], [[sigma-e5-q13-root-cause]], [[sigma-e5-like-kernel]], [[sigma-e5-compact-helper]] (in memory).

## 1. Motivation

`EmatixFastParquetTableProvider::supports_filters_pushdown` currently returns
`TableProviderFilterPushDown::Inexact` for every filter shape that
`predicate_from_expr_with_dict` accepts. Inexact preserves DataFusion's residual
`FilterExec`, which re-evaluates the same predicate against rows that already
passed emat's bitmap. Three downstream consequences:

1. **Selectivity-gate fallback is correct but slow.** When the bitmap is
   high-selectivity (`popcount * 3 > total`), the reader drops the bitmap, dense-
   decodes the full RG, and lets FilterExec re-filter. That works but it doubles
   the predicate cost.

2. **Filter columns stay in projection.** FilterExec needs the filter columns
   to evaluate; DataFusion keeps them in the scan projection. For the high-
   selectivity case where the filter column is otherwise unused downstream
   (Q13's `o_comment`, Q16's `p_type`), that decode is pure waste — we
   established the gap empirically in the Q13 root-cause investigation.

3. **The `compact_decoded_column` and `LikeMatcher` levers are blocked.**
   Both helpers landed dead-code in this session. Their unlock is a path where
   FilterExec is *gone* and the reader is the only filter step.

Exact pushdown removes FilterExec for the marked filters AND elides their
columns from the scan projection when not otherwise needed. Per-filter Exact
lets us mark only the filters where our bitmap eval is provably equivalent to
DataFusion's eval, keeping a safety net for the rest.

## 2. Safety Requirements

For a filter declared Exact, emat's evaluation MUST produce the same row set
as DataFusion's evaluation. Any divergence is a correctness bug.

### 2.1 Predicate-semantic equivalence

For each `ColumnPredicate` variant we accept today:

| Predicate            | Exact-safe? | Notes                                                    |
|----------------------|:-----------:|----------------------------------------------------------|
| `I32Range`           | yes         | integer comparison is unambiguous                        |
| `I32In`              | yes         | discrete membership                                      |
| `StringEq`/`NotEq`   | yes         | byte-equality matches Arrow's `eq_utf8`                  |
| `StringIn`           | yes         | byte-equality across N literals                          |
| `StringLike`         | conditional | only when `LikeMatcher::compile` accepts the pattern     |
| `F64Range`           | refused for pushdown today  | NaN/Inf semantics need vetting                                          |
| `I32ColumnPair`      | refused for pushdown today  | double-decode trap                                       |

`StringLike` is conditional because:
- `_` (single-char wildcard) — `LikeMatcher::compile` returns `None`. Already
  refused at predicate-extraction time.
- `\` escape character — refused (`Like::escape_char.is_some()` → `None`).
- Case-insensitive (`ILIKE`) — refused.
- Byte-oriented `LikeMatcher` is correct for ASCII patterns and any UTF-8
  pattern that doesn't straddle a multi-byte boundary at pattern edges. TPC-H
  uses ASCII so the bench corpus is unaffected. For a general production
  guarantee, we'd add a UTF-8-safe variant or refuse non-ASCII patterns.

### 2.2 Null handling

Parquet stores nulls separately from values via definition levels. Emat's
bitmap kernels currently assume all values are present — they emit predicate
results against a single contiguous value buffer. For columns with nulls, the
mapping between row index and value-buffer index breaks.

**Resolution: Exact only when the column has no nulls in the row group.**
Parquet metadata exposes `ColumnMetaData.statistics.null_count` per RG.

- All RGs report `null_count == 0` → safe for Exact.
- Any RG reports `null_count > 0` (or stats missing) → fall back to Inexact.

TPC-H tables are all non-null, so every filter eligible for Exact will pass
this check on the bench. For real-world data, the null-count check is an
explicit guard.

### 2.3 Selectivity-fallback correctness

The current selectivity-gate fallback (`popcount * 3 > total`) drops the
bitmap. With Exact pushdown, FilterExec is gone — dropping the bitmap means
the rows are NOT filtered. The fallback must instead **apply the bitmap via
`compact_decoded_column`** after the dense decode.

The compact is also correct when pushdown was Inexact (FilterExec on already-
filtered rows is a no-op against zero rows — cheap), so the fallback can
unconditionally apply the bitmap regardless of pushdown declaration. This
removes the existing branching.

### 2.4 Mixed Exact + Inexact filters

DataFusion drops Exact filters from FilterExec but keeps Inexact ones. Emat's
`BridgeFilter::build_bitmap` AND-combines all pushed predicates. So:

- Emat applies the AND of (Exact ∪ Inexact pushed predicates) via bitmap.
- DataFusion's residual FilterExec applies only the Inexact ones (no-op on the
  rows that already passed both because emat's predicate is a subset of the
  total).

Net: correct, no duplicated Exact predicate work. The Inexact predicate work
duplicates between emat and FilterExec — that's acceptable since marking
something Inexact already implies we don't trust it as the sole filter.

## 3. Implementation Plan

### Phase 1 — Low-risk Exact shapes (target: 1 PR)

1. Add `ColumnPredicate::is_exact_safe(&self) -> bool` returning true for
   `I32Range`, `I32In`, `StringEq`, `StringNotEq`, `StringIn`, and `StringLike`
   when `LikeMatcher::compile(pattern).is_some()`.
2. Add helper `column_has_no_nulls(col_idx) -> bool` cached on
   `EmatixFastParquetTableProvider` (read at open time from per-RG stats).
3. Modify `supports_filters_pushdown`:
   ```rust
   Some(pred) if pred.is_exact_safe() && self.column_has_no_nulls(pred.col_idx()) =>
       TableProviderFilterPushDown::Exact,
   Some(_) => TableProviderFilterPushDown::Inexact,
   None => TableProviderFilterPushDown::Unsupported,
   ```
4. Replace the selectivity-gate fallback drop-bitmap with unconditional
   `compact_decoded_column` after dense decode. Land the call site that was
   reverted in `ee67bde`.
5. Bench gate: SF=1 22-query geomean must not regress vs the
   `0.85` baseline. Specifically watch Q03, Q21 (the queries that regressed
   in the previous attempt — they should be fine now because the residual
   FilterExec is GONE for filters that match `is_exact_safe`).

### Phase 2 — LikeMatcher in dense byte_array bitmap kernel

1. Pre-compile `LikeMatcher` once per `ColumnPredicate::StringLike` at
   predicate construction time. Store as `Arc<LikeMatcher>`.
2. Wire into `filter_byte_array_to_bitmap_dense` via a typed variant that
   takes a `&LikeMatcher` instead of the generic `Fn(&[u8])`.
3. Lift the dict-only gate on `StringLike` in `predicate_from_expr_with_dict`.
   With Phase 1's Exact declaration + Phase 2's faster eval, PLAIN-LIKE
   pushdown is unblocked.
4. Bench Q13 specifically. Predicted: -10 to -20% improvement (down from
   +25% baseline) — savings come from elided `o_comment` projection.

### Phase 3 — Optional future shapes

- `F64Range` — needs NaN/Inf semantic vetting before Exact.
- `I32ColumnPair` — currently refused for pushdown (double-decode trap on
  Q12). With Phase 1's compact path, the trap may be resolved — worth
  re-testing.

## 4. Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Null-count stats missing or wrong → silent wrong results | Treat missing stats as "may have nulls" → Inexact |
| `LikeMatcher` edge case (UTF-8 boundary) produces wrong matches on non-ASCII | Refuse non-ASCII patterns at compile time, or add a UTF-8-safe slow path |
| Compact path is slower than expected → Q03/Q21 regression repeats | Bench gate Phase 1 before landing; if regression appears, drop the unconditional compact and keep it Exact-only-conditional |
| DataFusion plans differ in subtle ways with Exact (e.g. ordering, partition_statistics) | Audit Q13 and Q01 EXPLAIN ANALYZE before/after Phase 1 |

## 5. Bench Gate

A change to pushdown semantics affects many queries. The gate:

- **SF=1 22-query geomean** within ±2pp of the pre-change baseline.
- **No new regression** above +10% on any query that was within ±5% pre-change.
- **Correctness**: every query returns the same row count and the same
  result-set hash as the FastParquet reference.

Run before merging Phase 1, again before Phase 2.

## 6. Out of Scope

- Updating SF=10 expectations — SF=1 is the primary benchmark.
- Per-RG dynamic Exact decisions — the null-count check is static at open
  time. If a query touches RGs with mixed null-counts, we conservatively pick
  Inexact for the whole filter.
- Push down into the streaming page reader (not just eager) — already done
  via `with_filter` in #516; the change is at the predicate-declaration layer.

## 7. Decision Points for Review

1. **Is the byte-oriented `LikeMatcher` safe enough for production, or do we
   gate to ASCII patterns?**
2. **Compact unconditional vs Exact-conditional in the fallback?** The doc
   recommends unconditional for simplicity; Exact-conditional is a fallback
   if Phase 1 bench gate fails.
3. **Phase 2 sequencing — separate PR or bundle with Phase 1?** Per the
   "bundle related work" feedback, bundling is preferred. Risk: Phase 2's
   PLAIN-LIKE pushdown is the most novel piece and benefits from isolated
   bench data.
