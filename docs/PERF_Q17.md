# PERF_Q17 — Q17 SF=10 stage profile

Status: **re-verified 2026-05-26** (Σ.AH B.17).

## Wall time (20×3 canonical 2026-05-26)

| Engine | Median ms | σ |
|--------|----------:|----:|
| ematix-flow | **175.26** | 6.99 |
| DuckDB | 165.93 | 5.47 |

**6% behind DuckDB** (was 22% — large improvement, we closed most of the gap). Stage profile 5-trial: 169.63 ms.

## Per-stage decomposition

Σ compute 1958.92 ms / wall 169.63 ms = **11.55× parallelism = 83%** — third best in sweep.

| Stage | Floor | Actual | Status |
|-------|-------|--------|--------|
| EmatixFastParquetExec depth 9 (lineitem MAIN scan, L9 bloom from part → 61k rows out) | ~5 ms parallel for filtered scan | 1093.26 | **WAY over what 61k output suggests** — but this is the full 60M scan with bloom-applied output |
| HashJoinExec depth 4 (main lineitem ⋈ avg-subquery output) | 61k probe × ~30 ns | ~30 ms | 506.15 | **17× over** — needs investigation |
| EmatixFastParquetExec depth 9 (lineitem AVG-SUBQUERY scan, no bloom, full 60M) | ~600 ms full scan | ~600 | 182.89 | sub-floor (async) |
| HashJoinExec depth 7 (part_filt ⋈ lineitem-main) | 61k probe × 12 ns = 1 ms | ~10 | 155.15 | mild over |
| EmatixFastParquetExec part (2M → 2044) | small | 13.04 | at-floor ✓ |
| RepartitionExec | small | 5.15 | at-floor ✓ |
| AggregateExec Partial (60M → 25k l_partkey groups, avg) | ~30 ms | 2.48 | sub-floor (async) |
| AggregateExec Final | small | 0.30 | at-floor ✓ |
| BuildSideBloomEmitterExec | tiny | 0 | confirmed firing ✓ |

**Wait — the depth-9 1093 ms on a 61k output is suspicious.** That's the MAIN lineitem scan with L9 bloom applied; output is 61k rows after bloom filter. But the scan must STILL DECODE THE FULL 60M to evaluate the bloom! Then drop 99.9% of rows.

The L9 bloom is filtering OUTPUT but not skipping DECODE. The 1093 ms parallel = full lineitem 60M decode + bloom probe per row, then output 61k.

**This is the L9 architectural limit:** the bloom is currently consumed at the operator level (HashJoinExec probe-side filtering), not at the scan-level (BridgeFilter equivalent). For Q17 we'd need to push the bloom INTO the EmatixFastParquetExec to skip decoding rows whose l_partkey isn't in the bloom.

## Σ.AH waste candidate ranking

| Rank | Candidate | Wall savings | Confidence |
|-----:|-----------|-------------:|:----------:|
| 1 | **L9 bloom → in-scan filter (push into EmatixFastParquetExec BridgeFilter)** | Drops lineitem main scan 60M → 61k decoded. ~80 ms wall. | medium |
| 2 | **HashJoinExec depth 4 17× over floor** (506 ms vs 30 ms) | Needs samply to diagnose. Could be the 2-key (avg + l_partkey) comparison or batch projection. | low (no clear mechanism) |
| 3 | **L9 cascade on AVG subquery side** | The full 60M scan on the avg-subquery side doesn't have a bloom. Adding one (same part-keys) would drop that scan too. | medium (~30 ms wall) |

## Findings

- **Q17 closed half its gap to DuckDB** since 2026-05-25 (was 22% behind, now 6%).
- **L9 bloom fires on Q17 (BuildSideBloomEmitterExec visible)** but filters at OPERATOR level not SCAN level. The biggest remaining Q17 lever: push L9 bloom into the scan's BridgeFilter to skip decoding (60M → 61k decoded). Similar to the Σ.S.B fact-table cascade idea but operator-to-scan within a single join.
- Q17 parallelism 83% — third-best in sweep. The query is structurally well-aligned for parallel execution; the remaining gap is the decode-then-filter pattern.

**Next:** B.18 (Q18 — 243.70 ms, −6% behind DuckDB).

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
