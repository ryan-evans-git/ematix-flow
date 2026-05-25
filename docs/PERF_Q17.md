# PERF_Q17 — Q17 SF=10 stage profile

## Wall time

| Engine | Median ms | σ | Rows |
|--------|----------:|----:|----:|
| ematix-flow | 207.34 | 18.21 | 1 |
| DuckDB | 170.18 | 7.42 | 1 |
| Polars | 493.01 | 29.01 | 1 |

**22% behind DuckDB.** Q17 is an explicit known gap (memory [[sigma-q-l13-to-l16-session]] notes the gap as structural).

## Physical plan

Decorrelated correlated subquery: `quantity < 0.2 × avg(quantity) where l_partkey = part.partkey`. Two lineitem scans:
1. Main scan with bloom-emitter pushdown (filtered to part-matching rows)
2. Avg-subquery scan over **the full lineitem** (no bloom on this side)

```
ProjectionExec: sum / 7
  AggregateExec Final no-gby sum(extprice)
    HashJoinExec Partitioned Inner (p_partkey, l_partkey) filter=l_quantity < 0.2 * avg
      BuildSideBloomEmitterExec key_col=p_partkey target=l_partkey                 -- ← L9 fires
        HashJoinExec Partitioned Inner (p_partkey, l_partkey)
          part (filter p_brand=Brand#23 AND p_container='MED BOX')                 -- ~2k matching parts
          lineitem                                                                 -- 60M → 61k via bloom
      ProjectionExec 0.2 * avg(l_quantity), l_partkey
        AggregateExec FinalPartitioned gby=[l_partkey] avg(l_quantity)
          AggregateExec Partial
            lineitem                                                               -- 60M, NO bloom
```

## Per-stage breakdown (top 6)

| Rank | Operator | Depth | Median ms | Out rows |
|-----:|:---------|------:|----------:|---------:|
| 1 | EmatixFastParquetExec (lineitem main, L9 bloom on l_partkey) | 9 | 1102.25 | 61,385 |
| 2 | HashJoinExec (main+avg join) | 4 | 492.34 | 5,526 |
| 3 | EmatixFastParquetExec (lineitem for avg subquery, NO bloom) | 9 | 246.90 | 59,986,052 |
| 4 | HashJoinExec (part_filt ⋈ lineitem main) | 7 | 150.73 | 61,385 |
| 5 | EmatixFastParquetExec (part) | 11 | 13.05 | 2,044 |
| 6 | RepartitionExec | 8 | 6.50 | 59,986,052 |

Σ median compute: ~2010 ms. Wall median 177 ms. Parallel speedup ≈ 11.3×.

## Theoretical floor

| Stage | Floor (ms) |
|-------|-----------:|
| part scan + filter (1.5M → 2k) | 2 |
| lineitem scan main with l_partkey-bloom (still decodes all 60M to check bloom) | 8 |
| lineitem scan for avg (60M × 2 cols) | 7 |
| HashJoin part ⋈ lineitem (build 2k × probe 60M × 8 ns / 14) | 34 |
| Hash agg (200k partkey groups, avg = sum+count f64) | 8 |
| HashJoin main_rows × avg_per_partkey (build avg, probe 61k) | <1 |
| Final agg sum (no group) | <1 |
| **Floor** | **~60 ms** |
| **Actual** | **177 ms** |
| **Waste ratio** | **2.9×** |

## Waste candidates

### 1. L9 fires on the main lineitem scan but NOT on the avg-subquery lineitem scan

Plan shows `BuildSideBloomEmitterExec` only on the main lineitem path. The avg subquery scans the full 60M lineitem unfiltered to compute avg(l_quantity) PER PARTKEY. But after the `where p_brand AND p_container` filter at the outer level, **only ~2k partkeys are relevant**. The avg subquery shouldn't compute avg for the 198k other partkeys.

DuckDB likely propagates the partkey filter into the avg subquery via runtime-filter sideways info.

**Expected impact:** the avg-subquery lineitem scan drops from 60M to 60k (1000× fewer rows). Compute drops from 247 ms to ~3 ms. Wall: 177 → ~150 ms (~15% improvement). Would put us within 12% of DuckDB.

Concrete lever: the L9 rule looks at the **immediate** HashJoinExec's build side. The avg subquery's lineitem scan doesn't have a direct part-filtered build adjacent — the partkey filter is several levels up the plan. Memory [[sigma-sb-cascade-neg]] explored cascading L9 across FK chains; was neutral at the time. Q17's specific shape (decorrelated subquery on the same large fact table) is a textbook target for **cascading L9 down into the subquery scan**.

### 2. HashJoinExec join filter `l_quantity < 0.2 * avg`

Filter pushed INTO the join (visible in plan as `filter=l_quantity@0 < ...`). This is fine — applied per probe-row during join. Not a waste.

### 3. The 1102 ms compute on lineitem-main even with bloom

Even with bloom pushdown narrowing OUTPUT to 61k, the scan still decodes all 60M l_partkey values to evaluate the bloom membership. This is structural — the bloom check happens after decode of the key column. Wall ~78 ms is what 60M l_partkey decode costs. The bloom saves the OTHER columns' decode (extprice, quantity) — those only get decoded for the 61k passing rows.

That's why the L9 win is "60M lineitem rows scanned, but only 3 cols × 61k rows materialised for downstream". The decode of l_partkey for all 60M is the residual cost.

## Findings

- **Q17's residual gap to DuckDB is the avg-subquery side's missing L9 bloom**. Same decorrelated-subquery pattern as Q15 + a cross-subquery sideways info pass.
- L9 design currently looks at the immediate HashJoin's build side only — it doesn't propagate into "the OTHER aggregate's input scan even though it references the same table". This is a generalised lever, not a bandage.

## Next levers

1. **Extend L9 to cross-subquery sideways info pass** — if the same large fact table is scanned in both the main join and a decorrelated aggregate, propagate the build-side bloom from the main join into the aggregate's scan. Specifically targets Q17 and likely Q20.
