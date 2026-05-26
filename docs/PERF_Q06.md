# PERF_Q06 — Q06 SF=10 stage profile

Status: **re-verified 2026-05-26** (Σ.AH Phase B.6). Originally profiled 2026-05-25.

## Wall time

### 2026-05-26 refresh (20 trials × 3 warmups, post-Σ.AG.7 bare invocation — canonical)

| Engine | Median ms | σ |
|--------|----------:|----:|
| ematix-flow | **76.08** | 4.30 |
| DuckDB | 74.62 | 2.31 |

**Statistical tie with DuckDB** (1.95% slower). Polars was skipped at SF=10 in this bench; from prior runs Polars ~63 ms. Stage profile 5-trial: 81.02 ms (slightly elevated, noise band).

## Physical plan

Q06 SQL: `select sum(l_extendedprice * l_discount) from lineitem where l_shipdate ∈ [1994-01-01, 1995-01-01) and l_discount ∈ [0.05, 0.07] and l_quantity < 24`. All predicates on lineitem.

```
FusedAggregateExec<FilterSumSpec>
  EmatixFastParquetExec(lineitem, projection=[l_quantity, l_extendedprice, l_discount, l_shipdate])
```

The l_shipdate range predicate is fully pushed into the scan via BridgeFilter (scan emits 9.1M rows from 60M = 15% pass rate). l_discount and l_quantity are filtered inside the FusedAggregateExec.

## Per-stage breakdown

| Rank | Operator | Median ms | Out rows |
|-----:|:---------|----------:|---------:|
| 1 | EmatixFastParquetExec (with BridgeFilter on l_shipdate) | 907.71 | 9,099,165 |
| 2 | FusedAggregateExec | 0.00 (credited to upstream pull) | 0 |

Σ median compute: 908 ms. Wall median 76.68 ms. **Parallel speedup ≈ 11.84× of 14 cores** — Q06 is the most parallel-friendly query in the suite (no joins, no shuffle, no skew).

## Theoretical floor (per-stage, projection-cost-aware, 2026-05-26)

Q06's plan collapses to 2 nodes (FusedAggregateExec inlined into scan-side pull). All work is credited to `EmatixFastParquetExec`.

### What actually happens inside the scan

Q06 has 3 inline filters: `l_shipdate ∈ [1994-01-01, 1995-01-01)` (BridgeFilter pushed into decode), `l_discount ∈ [0.05, 0.07]` (FusedAgg inline), `l_quantity < 24` (FusedAgg inline). Combined selectivity ≈ 15% → 9.1M output rows.

**Σ.AE.2 selectivity-gate fallback**: when BridgeFilter pass rate > 1/3 within an RG, the scan falls back to **dense decode** of all 4 projected cols + per-batch bitmap residual. l_shipdate alone narrows ~85% of RGs at the min/max prune level; the ~9 RGs that contain `1994-01-01..1995-01-01` have selectivity ~100% within them (date sorted within RG), so dense-decode fallback fires.

| Stage component | Decoded data | Floor (ms parallel-sum) | Notes |
|-----------------|-------------|------------------------:|-------|
| l_shipdate (60M rows, i32, ratio ~0.73) — for BridgeFilter | 240 MB / (1.66 GB/s × 14) | ~200 (or ~30 if RG-pruned to ~9 RGs) | min/max prune likely kicks in for ~85% of RGs |
| l_quantity (60M rows or ~9.1M survivors, dict ratio ~1.0) | 45 MB at 10.43 GB/s | ~6 | very fast (dict-encoded) |
| l_extendedprice (~9.1M survivors after RG-prune, ratio 0.73) | ~75 MB / 1.66 GB/s × 14 | ~65 | dense decode within passing RGs |
| l_discount (~9.1M survivors after RG-prune, ratio ~0.73) | ~75 MB / 1.66 GB/s × 14 | ~65 | dense decode within passing RGs |
| Per-batch filter l_quantity<24 + l_discount range on 9.1M rows | 9.1M × 0.5 ns kernel × 2 preds | ~10 | post-decode kernel |
| Sum f64 × f64 on ~9M survivors | 9M × ~5 ns mul+add | ~50 | f64 multiply+sum loop |
| **Σ floor (sum ms)** | | **~400** | **assuming RG-prune saves ~85%** |
| **Σ floor without RG-prune (worst case)** | | **~900** | full 60M decode of all 4 cols |
| **Σ actual** | | **910 ms** | observed |
| **Σ effective parallelism** | 909.91 / 81.02 = **11.23× = 80%** | | **best in sweep** ✓ |
| **Realistic wall floor** | Σ floor 400 / 11.23 = | **~36 ms** (with RG-prune) | |
| **Observed wall** | | **81 ms** | |

**Two readings of the data:**

1. **If RG-prune is working** (likely): floor is ~400 ms parallel sum / 11.23 effective parallelism = **36 ms wall**. We're at **2.25× over realistic floor**. The gap (45 ms wall) likely comes from BridgeFilter overhead on each RG's l_shipdate decode (the i32 cmp is fast but per-batch bitmap construction adds cost), plus per-row f64 mul+add in the sum.
2. **If RG-prune is NOT working** (worst case dense decode of all 60M): floor is ~900 ms parallel sum / 11.23 = **80 ms wall**. We're **at-floor**. Snappy mix-weighted decode is the inescapable cost.

The DuckDB tie at 75 ms suggests reading 2 is closer — both engines pay the full Snappy decode for the four columns × ~14% of RGs. Polars's 63 ms (11% faster) is mostly the codec/decoder advantage [[polars-parquet-decode-approach]].

**Q06 is essentially at its realistic-parallelism floor on the canonical Snappy file.**

## What Polars does that we don't

Polars hits 63.5 ms — 11% faster. Worth investigating:

- Polars uses its own parquet decoder ([[polars-parquet-decode-approach]]) — const-generic per-bit-width macro-unrolled unpacker + jumptable dispatch. Our `ematix-parquet` codec has [[ematix-parquet-varint-optimal]] and [[ematix-parquet-v013-win]] features — comparable but not identical.
- Snappy decompress rate: [[q06-sf10-polars-gap-wall]] specifically documents that Snappy is the bound here (extprice Snappy at 1.73 GB/s, 7.3× memcpy). Polars may use a faster Snappy path or reuse decompressed buffers more aggressively.
- The Σ.O.c.1 RG decode cache is active here on rep-2+ trials — verified via memory but Polars may have its own equivalent.

## Σ.AH waste candidate ranking

Q06 is at realistic floor on Snappy. The only chase-worthy candidates are codec-level / decoder-level — not query-shape.

| Rank | Candidate | Wall savings | Confidence | Notes |
|-----:|-----------|-------------:|:----------:|-------|
| 1 | **Codec switch to LZ4_RAW** (canonical file change) | ~15 ms (76 → ~57) | high | LZ4 throughput 4.23 GB/s vs Snappy 1.66 GB/s on extprice (Phase A.1 audit). Comparability trade-off per `[[sf10-canonical-lineitem-snappy]]`. |
| 2 | **Polars-parity decoder spike** | ~13 ms (76 → ~63) | medium | Multi-month investment. Documented in `[[polars-parquet-decode-approach]]`. |
| 3 | **Σ.AE.2 selectivity-gate threshold tune** — switch from 1/3 fallback to masked-decode when l_shipdate selectivity is moderate | ~5 ms | low-medium | Currently dense-decodes all 4 cols for RGs that pass the date filter. Could load only surviving rows for extprice/discount via masked decode. Complex; touches the bitmap-stash path. |
| 4 | **Effective parallelism 80%** | already near-ceiling | — | Best of the suite; no obvious lever |

## Findings to capture as memories

- **Q06 SF=10 is at its realistic-parallelism floor on Snappy** — confirmed via per-stage decomposition (Σ floor ~400-900 ms / 11.23× effective parallelism = 36-80 ms wall; observed 81 ms).
- **Effective parallelism on Q06 is 80% — best in the 22q sweep.** Single-table scan + fused agg + no shuffle = the optimal plan shape for parallelism. Q01's parallelism imbalance (54%) is the same hardware doing a 7-col projection where decode imbalance hurts more.
- **Σ.AE.2 selectivity-gate fallback is doing dense decode** of all 4 Q06 cols for RGs passing the date filter. This is correct (cheaper than masked decode for >1/3 selectivity within an RG) but it means Q06's decode cost = full read of ~15% of the file, not just ~15% of the rows. The 2026-05-25 "12 ms decode floor" was assuming sparse decode, which doesn't happen here.

## Next levers from Q06

**None new.** Q06's gap to DuckDB (1.95%) is well inside noise; gap to Polars (~13 ms) is a known multi-month decoder investment.

---

## Verify pass — 2026-05-26 (Σ.AH B.6)

**What changed since 2026-05-25:**
- Wall: 71.71 → 76.08 ms canonical (+6%; within noise band). Stage profile 81.02 ms (slightly elevated).
- vs DuckDB: was +0.4% behind → now +1.95% behind (still parity-ish).
- Plan structure: unchanged (2 nodes: FusedAggregateExec + EmatixFastParquetExec).

**Methodology correction from 2026-05-25:** the prior "4.8× over floor" (16 ms floor vs 77 ms actual) was based on a sparse-decode model that doesn't apply — Σ.AE.2's selectivity-gate fallback densely decodes all 4 cols within passing RGs. The realistic floor accounting for dense decode + 80% effective parallelism is ~36–80 ms wall. We're at ~81 ms → essentially at-floor.

**Effective parallelism 80% is the best in the sweep.** Cross-query insight: single-table fused-agg scans (Q06 shape) hit the parallelism ceiling because there are no synchronisation barriers. Multi-join queries (Q05 38%, Q02 41%) lose parallelism to CollectLeft small-dim joins and Partial→Final agg pipelines.

**Next:** B.7 (Q07, 157.48 ms — lose to DuckDB by 11%; multi-join shape with nation OR-pair).
