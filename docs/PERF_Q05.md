# PERF_Q05 — Q05 SF=10 stage profile

Status: profiled 2026-05-25.

## Wall time

| Engine | Median ms | σ | Rows |
|--------|----------:|----:|----:|
| ematix-flow | 201.48 | 5.74 | 5 |
| DuckDB | 157.38 | 5.84 | 5 |
| Polars | — | — | failed (bigidx) |

**28% behind DuckDB**. Q05 is one of the explicit gaps.

## Physical plan

6-way join: region → nation → supplier → (cust ⋈ orders ⋈ lineitem), then sum by n_name. The 2-key supplier-nation = customer-nation constraint creates a large intermediate.

```
SortPreservingMergeExec [revenue DESC]
  ...
  AggregateExec FinalPartitioned gby=[n_name] sum
    AggregateExec Partial
      HashJoinExec CollectLeft Inner (r_regionkey, n_regionkey)         -- region filter ASIA
        region
        HashJoinExec CollectLeft Inner (n_nationkey, s_nationkey)       -- nation
          nation
          HashJoinExec CollectLeft Inner ON 2 KEYS:                     -- supplier (HOT)
              (s_suppkey, l_suppkey)
              (s_nationkey, c_nationkey)                                -- ← this 2-key shape
            supplier                                                    -- 100k rows
            HashJoinExec Partitioned Inner (o_orderkey, l_orderkey)
              HashJoinExec Partitioned Inner (c_custkey, o_custkey)
                customer                                                -- 1.5M rows
                orders (filter o_orderdate ∈ 1994-01-01..1995-01-01)   -- 2.3M rows
              lineitem                                                  -- 60M rows, no filter
```

## Per-stage breakdown (top 8)

| Rank | Operator | Depth | Median ms | Out rows |
|-----:|:---------|------:|----------:|---------:|
| 1 | HashJoinExec (supplier + 2-key constraint) | 11 | 676.14 | 9,103,367 |
| 2 | HashJoinExec (cust+orders ⋈ lineitem) | 10 | 152.54 | 364,380 |
| 3 | EmatixFastParquetExec (lineitem) | 13 | 109.73 | 59,986,052 |
| 4 | HashJoinExec (cust ⋈ orders) | 13 | 62.31 | 2,275,919 |
| 5 | EmatixFastParquetExec (customer) | 15 | 15.37 | 1,500,000 |
| 6 | RepartitionExec (lineitem 60M Hash(l_orderkey)) | 12 | 9.05 | 2,275,919 |
| 7 | RepartitionExec | 12 | 5.50 | 59,986,052 |
| 8 | EmatixFastParquetExec (orders) | 16 | 2.72 | 2,275,919 |

Σ median compute: 1040 ms. Wall median 193 ms. Parallel speedup ≈ 5.39× of 14 cores.

## Theoretical floor

| Stage | Floor (ms) |
|-------|-----------:|
| lineitem scan + decode 60M × 4 cols | 12 |
| orders scan + filter (15M → 2.3M) | 5 |
| customer scan (1.5M × 2 cols) | 1 |
| supplier/nation/region | <1 |
| HashJoin cust ⋈ orders (build 1.5M, probe 2.3M × 12 ns / 14) | 2 |
| HashJoin (cust+orders 1.5M build) ⋈ lineitem 60M probe × 12 ns / 14 | 51 |
| 2-key join supplier (100k build) × (cust+orders+lineitem 364k probe) × 12 ns | 0.3 |
| Hash agg 5 groups | <1 |
| **Floor** | **~72 ms** |
| **Actual** | **193 ms** |
| **Waste ratio** | **2.7×** |

## Waste candidates

### 1. The 9.1M-row output from the 2-key supplier join — DuckDB doesn't build this

The dominant operator at 676 ms compute (~50 ms wall). The 2-key constraint `s_nationkey = c_nationkey` is functionally equivalent to a per-row check that "the supplier and customer share a nation" — this should be evaluated as a *filter*, not a join expansion.

DuckDB likely reorders the joins so that the nation-match check happens after the (cust ⋈ orders ⋈ lineitem) intermediate is already small. Or it uses sideways information passing (runtime bloom on s_nationkey) to skip rows.

Memory [[sigma-qm-slice4-spike-rejected]] notes that hand-rolling static-redundant-semi didn't work because of double-build. Memory [[q18-sf10-duckdb-plan-diff]] is the same shape — DuckDB wins by reordering joins.

**This is the structural gap for Q05.** No simple lever fixes it without join-reorder logic.

### 2. lineitem scan 110 ms compute on 60M rows with NO filter pushed

Q05 has no lineitem-side predicate (no `where l_*`), so all 60M rows are touched. Floor: 12 ms parallel. We're at 110 ms parallel = 7.86 ms wall. That's ~4× over floor — likely the same Snappy decode rate gap noted in [[sigma-e5-q19-root-cause-orchestration]] and [[q06-sf10-polars-gap-wall]].

Q05 can't avoid touching all lineitem (no predicate), but a runtime bloom from (cust ⋈ orders) filtered keys could skip lineitem rows that won't join. Memory [[sigma-q-l9-bloom-consumer-findings]] confirms L9 fires on this exact shape (small-dim → fact). The L9 rule should be putting a bloom on lineitem against orderkeys from (cust ⋈ orders).

**Check:** is L9 firing on the (cust ⋈ orders) → lineitem edge? If yes but lineitem still scans 60M rows, the bloom is being installed but its pass rate is too high (all orderkeys of 2.3M filtered orders × 6 ratio of lineitem-to-orders ≈ 14M lineitem rows pass the bloom, only ~77% reduction).

### 3. cust ⋈ orders ⋈ lineitem at 152 ms compute on 364k output

The middle-of-plan 3-way join produces 364k rows from a 60M probe. Most lineitem rows don't survive the join. This is correct semantics but the path is expensive because lineitem isn't pre-filtered.

### 4. Q05 was previously flagged for fixing — see memory

Memory [[tpch-correctness-gaps]] mentions Q05 was correctness-fixed via rule narrowing (PR #143). Memory [[sigma-q-l13-to-l16-session]] explicitly says "Q05 needs structural work (join reordering)". This profile confirms that diagnosis.

## Findings to capture as memories

- Q05 SF=10: 2.7× over floor, primarily due to the supplier-nation 2-key join producing a 9.1M intermediate that DuckDB doesn't materialise. **Structural fix = join reorder, not a tunable lever.**
- The L9 bloom on (cust+orders) → lineitem should be firing — verify in plan dump if 60M lineitem rows still get touched.

## Next levers from Q05

1. **Verify L9 is firing** on Q05 — if it's there but lineitem still scans 60M, raise the bloom selectivity or push to scan-time predicate. If L9 isn't there at all, that's a rule-guard miss.
2. **Q05 join-reorder** is the structural lever; deferred (multi-month effort to add cost-based join reorder).
