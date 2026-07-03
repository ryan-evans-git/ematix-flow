# Benchmarks

Measurement notes + canonical baselines for ematix-flow's performance
gates. Subsequent PRs that touch `transform.rs` or upgrade DataFusion
re-run the relevant suite + diff against the baseline; any regression
≥10% needs a justification in the PR body.

Plan: [`docs/PHASE_SIGMA_PLAN.md`](PHASE_SIGMA_PLAN.md).

---

## TL;DR — v0.12.0 release benchmark, 2026-07-03, Apple M4 Max (14-core, 36 GB)

**ematix-flow vs DuckDB, strict protocol, one session, the exact
`v0.12.0` code**: solo-engine passes, per-query process isolation,
plan cache off, thermal gating, 10 trials × 2 warmups × 4 invocations
(first discarded), medians, 2σ verdict bars, env.json provenance in
`bench-results/release-v0.12.0/`. Production defaults only — no
bench-only flags; the harness session is constructed from the shipped
preset and pinned to it by parity tests.

**Zero clear DuckDB wins at any scale.**

| Scale | Clear ematix wins | Clear DuckDB wins | Noise/tie | Σ medians (ematix vs DuckDB) |
|---|---|---|---|---|
| SF=1 | **22 / 22** | 0 | 0 | 307 ms vs 806 ms (+163%) |
| SF=10 | **17 / 22** | 0 | 5 | 2,411 ms vs 3,246 ms (+35%) |
| SF=100 | **20 / 22** | 0 | 2 | 30,598 ms vs 41,270 ms (+35%) |

### Concurrent-stream throughput ("tph"), QPH — 2026-07-03, same engine code

Strict throughput protocol (seeded 22-query permutations per stream,
solo engines, 4 batches first-discarded, memory-guarded). ematix runs
its concurrency-aware defaults (`EMAT_TARGET_PARTITIONS` auto —
cross-process registry sizes partitions, reader decode fan-out, and
the rayon pool from one core share). Results:
`bench-results/sched-arc-2026-07-03/`.

| Config | ematix QPH | DuckDB QPH | ratio |
|---|---:|---:|---|
| SF=10, 1 stream | **27,462** | 21,405 | 1.28× |
| SF=10, 10 streams | **28,333** | 26,463 | 1.07× |
| SF=10, 100 streams | **26,073** | 25,824 | 1.01× |
| SF=100, 1 stream | **2,212** | 1,814 | 1.22× |
| SF=100, 10 streams | **1,581** | 1,118 | 1.41× |

> Polars / PySpark / Postgres columns are NOT re-measured in this
> release; see the historical 2026-06-21 five-engine section below
> (engine-stable carried baselines). The strict apples-to-apples
> comparison is ematix-flow vs DuckDB, co-measured above.

### SF=1 — ~1 GB · fits in cache

**22/22 clear wins.**

| Query | ematix-flow ms | DuckDB ms | Δ ms | verdict (2σ) |
|------:|---------------:|----------:|-----:|:------------|
| Q01 | **19.48** | 49.32 | +29.84 | **ematix** |
| Q02 | **7.53** | 18.28 | +10.75 | **ematix** |
| Q03 | **13.10** | 32.78 | +19.68 | **ematix** |
| Q04 | **11.31** | 23.31 | +12.00 | **ematix** |
| Q05 | **12.05** | 31.42 | +19.37 | **ematix** |
| Q06 | **1.73** | 13.39 | +11.66 | **ematix** |
| Q07 | **25.18** | 34.30 | +9.12 | **ematix** |
| Q08 | **14.68** | 39.44 | +24.76 | **ematix** |
| Q09 | **18.74** | 56.33 | +37.59 | **ematix** |
| Q10 | **20.66** | 41.99 | +21.33 | **ematix** |
| Q11 | **5.58** | 9.82 | +4.24 | **ematix** |
| Q12 | **14.80** | 25.44 | +10.64 | **ematix** |
| Q13 | **9.04** | 142.94 | +133.90 | **ematix** |
| Q14 | **11.49** | 23.03 | +11.54 | **ematix** |
| Q15 | **11.31** | 14.86 | +3.55 | **ematix** |
| Q16 | **9.31** | 21.94 | +12.63 | **ematix** |
| Q17 | **14.17** | 25.77 | +11.60 | **ematix** |
| Q18 | **18.27** | 45.81 | +27.54 | **ematix** |
| Q19 | **17.60** | 36.36 | +18.76 | **ematix** |
| Q20 | **12.35** | 30.41 | +18.06 | **ematix** |
| Q21 | **34.27** | 77.90 | +43.63 | **ematix** |
| Q22 | **4.20** | 11.16 | +6.96 | **ematix** |

### SF=10 — ~10 GB · production scale

**17 clear wins, 5 within the noise bar, 0 losses.** The 5 ties (Q01/Q03/Q05/Q08/Q09) all resolve inside the 2σ bar; ematix is nominally ahead in 3 of the 5.

| Query | ematix-flow ms | DuckDB ms | Δ ms | verdict (2σ) |
|------:|---------------:|----------:|-----:|:------------|
| Q01 | **248.21** | 246.15 | -2.06 | ~tie |
| Q02 | **19.27** | 39.23 | +19.96 | **ematix** |
| Q03 | **129.27** | 135.99 | +6.72 | ~tie |
| Q04 | **58.45** | 84.97 | +26.52 | **ematix** |
| Q05 | **137.33** | 132.06 | -5.27 | ~tie |
| Q06 | **25.40** | 75.54 | +50.14 | **ematix** |
| Q07 | **118.35** | 135.24 | +16.89 | **ematix** |
| Q08 | **146.69** | 157.32 | +10.63 | ~tie |
| Q09 | **271.20** | 287.92 | +16.72 | ~tie |
| Q10 | **192.72** | 221.48 | +28.76 | **ematix** |
| Q11 | **11.41** | 25.41 | +14.00 | **ematix** |
| Q12 | **86.45** | 108.99 | +22.54 | **ematix** |
| Q13 | **102.40** | 232.85 | +130.45 | **ematix** |
| Q14 | **82.01** | 125.52 | +43.51 | **ematix** |
| Q15 | **61.76** | 81.11 | +19.35 | **ematix** |
| Q16 | **44.22** | 57.40 | +13.18 | **ematix** |
| Q17 | **80.29** | 147.09 | +66.80 | **ematix** |
| Q18 | **177.73** | 199.20 | +21.47 | **ematix** |
| Q19 | **122.03** | 188.93 | +66.90 | **ematix** |
| Q20 | **91.88** | 136.71 | +44.83 | **ematix** |
| Q21 | **180.33** | 374.52 | +194.19 | **ematix** |
| Q22 | **23.74** | 52.65 | +28.91 | **ematix** |

### SF=100 — ~100 GB · out-of-core

**20 clear wins, 2 within the noise bar (Q01, Q16), 0 losses.**

| Query | ematix-flow ms | DuckDB ms | Δ ms | verdict (2σ) |
|------:|---------------:|----------:|-----:|:------------|
| Q01 | **2460.74** | 2748.43 | +287.69 | ~tie |
| Q02 | **195.20** | 337.23 | +142.03 | **ematix** |
| Q03 | **1501.23** | 1859.57 | +358.34 | **ematix** |
| Q04 | **850.24** | 1010.53 | +160.29 | **ematix** |
| Q05 | **1434.49** | 1821.57 | +387.08 | **ematix** |
| Q06 | **518.62** | 857.97 | +339.35 | **ematix** |
| Q07 | **1476.11** | 1967.90 | +491.79 | **ematix** |
| Q08 | **1654.67** | 2368.44 | +713.77 | **ematix** |
| Q09 | **3494.58** | 4863.26 | +1368.68 | **ematix** |
| Q10 | **1952.52** | 2412.38 | +459.86 | **ematix** |
| Q11 | **184.46** | 245.59 | +61.13 | **ematix** |
| Q12 | **1032.64** | 1357.39 | +324.75 | **ematix** |
| Q13 | **1968.37** | 2885.50 | +917.13 | **ematix** |
| Q14 | **826.95** | 1204.21 | +377.26 | **ematix** |
| Q15 | **820.55** | 1156.65 | +336.10 | **ematix** |
| Q16 | **404.19** | 467.95 | +63.76 | ~tie |
| Q17 | **1483.75** | 1841.77 | +358.02 | **ematix** |
| Q18 | **2150.23** | 2790.67 | +640.44 | **ematix** |
| Q19 | **1226.71** | 1855.94 | +629.23 | **ematix** |
| Q20 | **1144.17** | 1492.15 | +347.98 | **ematix** |
| Q21 | **3467.07** | 5024.93 | +1557.86 | **ematix** |
| Q22 | **350.17** | 699.76 | +349.59 | **ematix** |
---

## Historical TL;DR — five-engine tables (SF=1 2026-06-12; SF=10/SF=100 2026-06-21), Apple M4 Max

Five engines, all 22 TPC-H queries, three scale factors, the same machine
and the same Parquet files. **ematix-flow numbers are production-faithful**:
the harness (`examples/tpch_preset_rebench.rs`) builds a **fresh
`SessionContext` per measurement** through the shipped preset — no plan
cache, no context reuse, no bench-only optimizer rules — so every number is
what `pip install ematix-flow` reproduces on a cold query. ematix-flow and
DuckDB run in **isolated** (not interleaved) passes — co-scheduling the two
engines in one process penalized whichever ran second by up to 60% on small
queries — with 20 timed trials after 3 warmups at SF=1 / SF=10 and 5 trials
after 2 warmups at SF=100, medians. SF=10 verdicts are the median of **two
same-session pairs with alternating engine order** on a settled machine
(per-query agreement between the pairs: under 2%); **SF=100 verdicts are the
median of three independent order-alternated per-query-isolated sweeps** —
necessary because SF=100 per-query times drift ±10-30% on cache/thermal state,
so a single sweep over-counts losses. Polars is the in-process harness; PySpark is
`local[*]` on the JVM; Postgres 14 is `EXPLAIN ANALYZE` Execution Time
(B-tree indexed + `ANALYZE`d). The fastest engine per query is **bold**.
All 22 row counts + sums match DuckDB at every scale.

> This refresh replaces the 2026-06-11 tables. Two engine changes since:
> the tight-cardinality runtime-bloom rescue (Q08's wrap — its first
> publish-protocol win, below) and a fix to a self-inflicted harness
> regression (an unconditional between-query heap release that taxed
> small-working-set queries up to +25%; it now fires only above a 6 GB
> peak-RSS threshold, preserving its −5% out-of-core win). DuckDB and
> Polars also ran faster than their prior columns in these sessions —
> several ±5%-margin verdicts (Q07/Q09/Q18 at SF=10) moved to the
> competitor as a result. That is what same-session honesty
> looks like: queries within ~5% should be read as parity, and the
> stable signal is the geomean.

### SF=1 — ~1 GB · fits in cache

- **Wins (fastest median per query):** ematix-flow **22 / 22** — the full sweep.
- **Geomean ematix-flow speedup:** 2.36× vs DuckDB · 4.22× vs Polars · 18.25× vs PySpark · 16.78× vs Postgres.

| Query | ematix-flow | DuckDB | Polars | PySpark | Postgres |
|---|--:|--:|--:|--:|--:|
| Q01 | **17.1** | 45.6 | 38.6 | 167 | 411 |
| Q02 | **7.1** | 16.5 | 47.9 | 190 | 123 |
| Q03 | **12.8** | 30.9 | 46.5 | 257 | 163 |
| Q04 | **10.5** | 21.2 | 23.8 | 184 | 94.6 |
| Q05 | **15.4** | 29.7 | 8949 | 335 | 226 |
| Q06 | **1.7** | 12.0 | 10.5 | 41.5 | 218 |
| Q07 | **24.3** | 31.1 | 118 | 260 | 1262 |
| Q08 | **13.3** | 36.8 | 96.7 | 182 | 99.7 |
| Q09 | **17.4** | 51.9 | 47.5 | 583 | 820 |
| Q10 | **24.6** | 56.8 | 111 | 362 | 355 |
| Q11 | **5.3** | 9.0 | 8.9 | 119 | 36.3 |
| Q12 | **13.9** | 23.5 | 19.2 | 269 | 361 |
| Q13 | **9.0** | 128 | 118 | 684 | 871 |
| Q14 | **11.4** | 20.8 | 12.3 | 114 | 69.8 |
| Q15 | **10.9** | 12.9 | 11.5 | 127 | 140 |
| Q16 | **9.0** | 20.3 | 21.2 | 205 | 113 |
| Q17 | **15.0** | 24.2 | 39.0 | 233 | 398 |
| Q18 | **18.1** | 43.1 | 56.6 | 560 | 1154 |
| Q19 | **17.3** | 32.1 | 105 | 86.0 | 31.9 |
| Q20 | **11.9** | 26.7 | 22.4 | 106 | 147 |
| Q21 | **32.2** | 69.9 | 721 | 628 | 609 |
| Q22 | **8.1** | 19.6 | 13.6 | 354 | 24.1 |

### SF=10 — ~10 GB · production scale

- **Wins (ematix-flow vs DuckDB):** ematix-flow **22 / 22** — ematix + DuckDB re-measured 2026-06-21 per-query-isolated (each query in its own process; warm cache, 15 timed trials after 3 warmups, medians — the fair protocol, the way competitors are measured). **Q05 flips to a win** (135 vs 139) via the Q05 transitive-dim-semi splice (PR #159, default-on + scale-gated to SF≤10); **Q07 and Q18 also flip** from the prior table; **Q09** — the last parity-class query — confirmed a win by a 12-iteration thermally-paired re-measure (258 vs 269, 11/12 iters). The prior **16/22** used a trial-interleaved protocol that over-penalizes ematix's allocator (mimalloc page-cache starvation), depressing the parity-class verdicts (Q07/Q09/Q18); per-query-isolated removes that artifact. All 22 row counts + sums still match DuckDB.
- **Geomean ematix-flow speedup:** **1.58× vs DuckDB** (per-query-isolated, 2026-06-21). The Polars / PySpark / Postgres columns below are **carried from the 2026-06-12/18 runs** (not co-measured today), so the 5-engine "fastest" marks on the Polars-competitive rows (Q14/Q15) are cross-run/approximate; the apples-to-apples claim is ematix-flow vs DuckDB.

| Query | ematix-flow | DuckDB | Polars | PySpark | Postgres |
|---|--:|--:|--:|--:|--:|
| Q01 | **226** | 234 | 547 | 732 | 4306 |
| Q02 | **18.6** | 38.8 | 417 | 599 | 2222 |
| Q03 | **119** | 141 | 572 | 2722 | 3488 |
| Q04 | **53.3** | 86.3 | 264 | 1711 | 905 |
| Q05 | **135** | 139 | — | 4589 | 3233 |
| Q06 | **24.8** | 73.8 | 56.3 | 205 | 1373 |
| Q07 | **116** | 134 | 1287 | 3737 | 2188 |
| Q08 | **148** | 162 | 1172 | 940 | 1340 |
| Q09 | **258** | 269 | 445 | 2187 | 7431 |
| Q10 | **187** | 365 | 3145 | 2355 | 3362 |
| Q11 | **11.2** | 24.8 | 39.7 | 197 | 578 |
| Q12 | **82.4** | 107 | 116 | 826 | 3542 |
| Q13 | **97.9** | 224 | 419 | 2069 | 10989 |
| Q14 | **80.9** | 122 | 84.4 | 379 | 813 |
| Q15 | **59.6** | 79.9 | 62.1 | 645 | 1606 |
| Q16 | **43.0** | 57.0 | 169 | 638 | 1098 |
| Q17 | **75.7** | 142 | 473 | 3956 | 5387 |
| Q18 | **172** | 195 | 588 | 6953 | 19846 |
| Q19 | **119** | 182 | 1264 | 493 | 148 |
| Q20 | **88.3** | 133 | 265 | 419 | 3279 |
| Q21 | **202** | 361 | 34184 | 7523 | 6952 |
| Q22 | **22.7** | 114 | 122 | 628 | 203 |
<!-- SF=10 ematix+DuckDB: 2026-06-21 per-query-isolated, 15 trials/3 warmups, medians, production-defaults binary (PR #159: Q05 transitive-dim-semi default-on + scale-gate). Q09 from a 12-iter thermally-paired re-measure (parity-class). Polars/PySpark/Postgres carried from 2026-06-12/18; Q14/Q15 ematix-vs-Polars is cross-run. -->

### SF=100 — ~100 GB · out-of-core

- **Wins (ematix-flow vs DuckDB, 3 independent per-query-isolated sweeps, 2026-06-21):** **14 / 22** reproduce as ematix wins across *all three* sweeps (17/22 by 3-round median); **2 consistent DuckDB wins** — Q10 (~0.73×) and Q8 (~0.95×), the high-RSS wide-string queries; the remaining **6** (Q3, Q5, Q9, Q16, Q18, Q21 — marked †) sit *within run-to-run cache/thermal noise* at this scale (per-query times drift ±10-30% round-to-round on a 36 GB box where the ~30 GB working set exceeds RAM). The prior "18/22, 1.58×" was measured *in-sweep*, which inflates ematix's easy-query margins.
- **Geomean ematix-flow speedup:** **1.18× vs DuckDB** (3-round per-query-isolated median, the honest "both engines warm" number). Polars/PySpark/Postgres columns are **carried 2026-05-29 baselines** (older protocol) — ematix leads each by a wide margin, but those are not a fresh head-to-head.

| Query | ematix-flow | DuckDB | Polars | PySpark | Postgres |
|---|--:|--:|--:|--:|--:|
| Q01 | **2,250** | 2,375 | 79,084 | 5,184 | — |
| Q02 | **225** | 393 | 53,109 | 6,697 | 26,054 |
| Q03 † | 1,532 | **1,528** | 30,103 | 26,128 | — |
| Q04 | **821** | 895 | 6,550 | 10,543 | — |
| Q05 † | 1,644 | **1,620** | — | 38,145 | — |
| Q06 | **461** | 824 | 540 | 1,142 | — |
| Q07 | **1,480** | 1,668 | 95,946 | 12,775 | — |
| Q08 | 2,198 | **2,085** | — | 23,610 | — |
| Q09 † | 6,839 | **6,369** | 21,438 | 67,105 | — |
| Q10 | 3,560 | **2,585** | — | 24,590 | — |
| Q11 | **213** | 226 | 412 | 5,639 | 60,108 |
| Q12 | **982** | 1,197 | 1,112 | 6,482 | — |
| Q13 | **1,983** | 2,772 | 5,114 | 14,442 | 86,674 |
| Q14 | **784** | 2,320 | 895 | 2,623 | — |
| Q15 | **790** | 1,022 | 871 | 5,142 | — |
| Q16 † | **383** | 388 | 1,809 | 5,012 | 29,144 |
| Q17 | **1,368** | 1,529 | 9,536 | 47,823 | — |
| Q18 † | **2,655** | 2,674 | 15,572 | 54,206 | — |
| Q19 | **1,160** | 1,617 | — | 3,101 | 52,107 |
| Q20 | **1,183** | 1,385 | 6,692 | 5,682 | — |
| Q21 † | **4,896** | 5,326 | — | 55,583 | — |
| Q22 | **446** | 602 | 1,255 | 5,107 | 16,805 |

_Bold = faster of **ematix-flow / DuckDB** (both 2026-06-21, 3-round per-query-isolated median). **†** = within SF=100 run-to-run noise — did not reproduce the same winner across all 3 sweeps. Polars / PySpark / Postgres are **carried 2026-05-29 baselines** on the same machine (older protocol), shown for rough reference, not bolded against the freshly-measured pair._

> **Provenance.** The **ematix-flow + DuckDB** columns are fresh
> per-query-isolated runs on this Apple M4 Max through
> `examples/tpch_preset_rebench.rs` — each query in its own process, each
> engine solo (no cross-engine RAM contention): **SF=10** 2026-06-21, 15
> trials / 3 warmups; **SF=100** 2026-06-21, **median of 3 independent
> order-alternated sweeps** (5 trials / 2 warmups each), engine commit
> `4e7393c` (post-#159). Both are taken on a settled machine, so neither
> column carries another day's thermal/page-cache state. Polars Q14/Q15 at SF=10 (the queries where the verdict is
> within a few percent) were re-measured 2026-06-12 at the same 20×3
> trial counts; the rest of the Polars column is the 2026-06-10 run
> (SF=10) / 2026-05-29-31 baselines (SF=1, SF=100). PySpark and
> Postgres are carried from 2026-05-29/31 on the **same machine** —
> engine-stable, and Spark needs a JVM not currently installed. Polars
> can't run several canonical TPC-H shapes (we feed it
> semantically-identical `q??.polars.sql` variants); Q05 overflows its
> default 32-bit row index at SF >= 10 (`—`), and at SF=100 Polars OOMs
> on several queries. Postgres ran with a 90 s per-query cap at SF=100 —
> most heavy queries timed out (`—`), so its SF=100 geomean covers only
> the 6 that finished.

> **SF=100 measurement — refreshed 2026-06-21 (3 independent sweeps).**
> ematix-flow and DuckDB are timed **each in its own process** (co-running both
> in one process makes the ~30 GB working set contend for the 36 GB box's RAM and
> understates the higher-memory engine). Across **3 independent per-query-isolated
> sweeps**, ematix-flow wins **14/22 reproducibly** (in every sweep) and **17/22 by
> 3-round median**, geomean **1.18× vs DuckDB**. **Q10** (~0.73×) and **Q8** (~0.95×)
> are consistent losses — the high-RSS wide-string queries. Six queries
> (Q3/Q5/Q9/Q16/Q18/Q21) swing ±10-30% on cache/thermal state at this scale and are
> not individually decidable. The earlier **"18/22, 1.58×"** came from a single
> *in-sweep* run, which over-states ematix by rewarding its cache behaviour on the
> easy queries. The [README](../README.md#benchmarks) carries the headline (SF=1
> 22/22, SF=10 **22/22 vs DuckDB**, SF=100 **14/22 robust · 1.18×**).

> **Config.** ematix-flow runs the production preset, no env vars:
> `target_partitions = cores` (14 here) plus the fused-aggregate,
> dict-group-count, push-LeftSemi, transitive-semi (Q20-shape),
> runtime-bloom (right-sized overflow blooms + probe-order fused eval),
> cardinality-gated Robin-Hood, scale-relative-broadcast (anti-aware),
> and cluster-key single-phase aggregation rules — exactly what
> `pip install ematix-flow` gives you, one fresh `SessionContext` per
> query. DuckDB runs at defaults (in-memory `read_parquet`).

> **Thermal band (SF=10 / SF=100).** Back-to-back runs drift ±5-20% as the
> M4 Max heats. The isolated passes here run under `caffeinate -i` with
> 3 warmups + 20 trials (medians), which bounds within-pass drift; for
> lever-level A/B effects use the strict harness
> (`scripts/bench/strict_ab.sh`: interleaved A/B/A/B + `taskpolicy -a` +
> discard-first, median CV ~1.3-2.0%).

---

## Historical TL;DR — TPC-H head-to-head, Apple M4 Max (2026-05-05, 5-trial)

Single-node DataFusion (via ematix-flow) is faster than PySpark
`local[*]` on every TPC-H query benched, at every scale factor
benched, on the same hardware:

- **Full 22-query TPC-H suite at SF=1: geomean 5.87× faster**
  (range 1.78× to 16.74×). All 22 queries plan + execute on
  DataFusion 53.1 with no SQL-surface gaps.
- **Representative set (Q1/Q3/Q6/Q19) at SF=10: geomean 3.3×
  faster** (range 1.7× to 9.2×). Gap narrows at SF=10 vs SF=1
  — Spark amortises better as input scales — but DataFusion
  still wins every query.

> The 5.87× number is the *5-trial / 1-warmup* baseline from 2026-05-05.
> The 20-trial refresh above supersedes it; per-query medians shifted
> ±10% under the longer trial window.

The 22-query SF=1 numbers fully ground the headline claim that
single-node DataFusion outperforms single-node Spark on TPC-H:

| Engine                                    | Geomean DF speedup | Range          | Coverage           |
|---|---|---|---|
| **DataFusion** (single-node)              | —                  | —              | 22/22 queries pass |
| PySpark (`local[*]`, 12 cores, 4 GB heap) | **5.87× (SF=1) / 3.3× (SF=10)** | 1.78× – 16.74× | 22/22 queries pass |

Per-query SF=10 representative-set medians:

| Engine                                           | Q1       | Q3        | Q6      | Q19     | DF/Spark    |
|---|---|---|---|---|---|
| **DataFusion** (single-node)                     | 323 ms   |  233 ms   | 116 ms  | 212 ms  | —           |
| **ematix-flow distributed** (3 in-process workers) | 315 ms |  228 ms   | 109 ms  | 202 ms  | —           |
| PySpark (`local[*]`, 12 cores, 4 GB heap)        | 1318 ms  | 2146 ms   | 200 ms  | 483 ms  | **3.3× geomean** |

(Full 22-query SF=1 per-query table in
[Σ.C extension — 22-query SF=1 head-to-head](#σc-extension--22-query-sf1-single-node-head-to-head-2026-05-05)
below.)

(median wall-clock; criterion bootstrap CI for DataFusion +
distributed columns, min/max of 3 trials for PySpark; full
method + per-query CIs in
[Σ.C PR 2](#σc-pr-2--sf10-multi-process-baseline-2026-05-05) below.)

**What this is.** A regression-canary baseline + a credible
single-node DataFusion-vs-PySpark comparison on identical
hardware. Reproducible in ~30 minutes from a clean checkout
(see one-liner below).

**What this isn't.** Proof of cross-host scaling. Every number
above is **single-host** (loopback network, shared kernel /
CPU / memory). The 3-worker ematix-flow-distributed config does
not show meaningful gains over the single-node config here —
that's expected on one machine; cross-host wins require
hardware-isolated workers (homelab k3s, rented bare-metal) and
remain a deferred item. We do *not* claim "scales as well as
PySpark on a real cluster" from these numbers.

**Repro** (~30 min wall-clock on M-class hardware):

```sh
# Generate SF=10 (~3.2 GB, ~1 min)
cargo run --release -p ematix-flow-core --example tpch_generate -- \
    --sf 10 --out examples/tpch/data/sf10

# DataFusion + ematix-flow-distributed
TPCH_DATA_DIR=examples/tpch/data/sf10 TPCH_MEASUREMENT_TIME_S=10 \
    cargo bench -p ematix-flow-core --bench tpch
TPCH_DATA_DIR=examples/tpch/data/sf10 TPCH_MEASUREMENT_TIME_S=10 \
    cargo bench -p ematix-flow-distributed --bench tpch_distributed

# PySpark (needs JDK 17+ + pyspark in venv)
JAVA_HOME=/opt/homebrew/opt/openjdk PATH="$JAVA_HOME/bin:$PATH" \
    .venv/bin/python scripts/bench-tpch-pyspark.py \
    --data-dir examples/tpch/data/sf10 --trials 3
```

**Honest gaps:**

- ~~Only 4 of TPC-H's 22 queries benched~~ — **closed.** Full 22
  benched at SF=1; SF=10 still 4-query because the SF=10 22-query
  bench cycle is ~30 min Rust + ~15 min Spark on Apple M4 Max.
- Cross-host distributed numbers: deferred (no real cluster
  hardware in this project's runway). The `infra/` recipe still
  works for AWS-equipped users.
- Polars beats DataFusion on Q6 by 1.82× at SF=1, **but the
  2026-05-05 deep-dive (Σ.A1 PR 4 follow-up below) localised
  the gap to parquet decode, not aggregate compute**: against
  pre-decoded MemTable, DataFusion runs Q6 in 8.89 ms — faster
  than Polars's 10.0 ms (which includes parquet decode).
  Production streaming pipelines hit the MemTable path, not
  parquet, so this gap doesn't show up in real workloads.
- PySpark used JDK 23 (not the officially supported JDK 17 / 21)
  with Py4J reflection-warning noise that's harmless. Numbers
  should be within run-to-run variance of a JDK 17 baseline.

For paste-into-Hacker-News-or-Twitter:

> ematix-flow (Rust, DataFusion 53) runs the full 22-query TPC-H
> suite at SF=1 5.87× faster than PySpark single-node (geomean;
> 1.78×–16.74× per-query). At SF=10 on the representative set the
> geomean is 3.3× faster. Same Apple M4 Max, same Parquet, same SQL.
> Single-host only — not claiming cross-host scaling. Repro +
> per-query CIs:
> github.com/ryan-evans-git/ematix-flow/blob/main/docs/BENCHMARKS.md

---

## Σ.C extension — 22-query SF=1 single-node head-to-head (2026-05-05)

Closes the "only 4 queries benched" honest-gap from Σ.A1 PR 4.
All 22 official TPC-H spec queries (Q1..Q22) extracted from
`tpchgen 2.0.2` with the canonical validation-set parameters
substituted in, written to `examples/tpch/queries/q01.sql`..
`q22.sql` via
`cargo run --release -p ematix-flow-core --example tpch_extract_queries`,
audited by `tpch_22_audit` (22/22 pass — no SQL-surface gaps in
DataFusion 53.1), then benched against PySpark `local[*]`.

Method: same Apple M4 Max / SF=1 Snappy Parquet / criterion
`sample_size = 10`, `measurement_time = 10s` (2-second-per-query
warm-up handled internally; longer windows on Q1/Q3/Q6/Q19 to
preserve the existing baselines). PySpark via 1 discarded
warm-up + 3 timed trials, median reported.

| Q   | DataFusion (ms) | PySpark median (ms) | PySpark min/max | Spark/DF |
|---|---|---|---|---|
| Q1  |  49.4 |  214.8 | 196 / 278 | **4.34×** |
| Q2  |  31.0 |  299.1 | 234 / 310 | **9.65×** |
| Q3  |  38.7 |  307.8 | 274 / 338 | **7.95×** |
| Q4  |  27.9 |  215.8 | 211 / 293 | **7.73×** |
| Q5  |  52.5 |  367.6 | 347 / 396 | **7.00×** |
| Q6  |  22.3 |   62.8 |  48 /  73 | **2.81×** |
| Q7  |  55.3 |  280.8 | 276 / 467 | **5.08×** |
| Q8  |  53.2 |  213.4 | 191 / 270 | **4.01×** |
| Q9  |  65.5 |  545.1 | 477 / 572 | **8.32×** |
| Q10 |  66.4 |  437.8 | 401 / 477 | **6.59×** |
| Q11 |  23.2 |  217.1 | 176 / 315 | **9.37×** |
| Q12 |  45.5 |  308.9 | 297 / 339 | **6.79×** |
| Q13 |  93.3 |  684.8 | 663 / 736 | **7.34×** |
| Q14 |  28.2 |  140.7 | 124 / 157 | **4.99×** |
| Q15 |  38.6 |  165.8 | 151 / 214 | **4.29×** |
| Q16 |  27.6 |  255.2 | 214 / 261 | **9.26×** |
| Q17 |  68.0 |  246.7 | 245 / 317 | **3.63×** |
| Q18 | 109.1 |  545.1 | 537 / 594 | **5.00×** |
| Q19 |  57.5 |  101.7 |  94 / 178 | **1.77×** |
| Q20 |  35.9 |  119.6 | 119 / 255 | **3.33×** |
| Q21 |  76.9 |  603.8 | 591 / 776 | **7.85×** |
| Q22 |  17.2 |  281.6 | 268 / 338 | **16.36×** |

(DataFusion: criterion median of 10 samples. PySpark: median of
3 timed trials after 1 discarded warm-up. Slight DF deltas vs
the original Σ.A1 PR 2 baseline reflect run-to-run variance —
the new bench uses tighter `measurement_time = 10s` per-query
windows to fit 22 queries in a single run.)

**Geomean DataFusion speedup over single-node PySpark: 5.87×.**
Range 1.78× (Q19; Spark's broadcast-join optimiser closes the
gap on the disjunctive-WHERE shape) to 16.74× (Q22; correlated
NOT EXISTS + IN-list, where Spark's optimiser stalls). 21 of 22
queries see a ≥3× DataFusion win; Q19 is the only sub-3× result.

**Findings:**

1. **Q22 is the headline.** 16.74× DataFusion win on a
   correlated NOT EXISTS query. Spark's Catalyst can't prune
   the customer-orders correlation efficiently at this scale;
   DataFusion's planner ships a single-pass anti-join that
   crushes it. This is the kind of query where the "DataFusion
   beats Spark single-node" claim originates.
2. **Q22, Q11, Q16, Q2, Q9 (≥8.3×)** all involve aggregations
   with subqueries / IN-lists — DataFusion's vectorized hash
   build/probe pulls a clear win on the join-heavy queries.
3. **Q19 (1.78×) is the closest** — disjunctive `WHERE (... OR
   ... OR ...)` over a 2-way join. Spark's Catalyst recognises
   the pattern; DataFusion still wins, just by less. (Polars
   *loses* on this same query, see Σ.A1 PR 4 follow-up below.)
4. **No SQL-surface gaps.** All 22 queries plan + execute
   cleanly. The TPC-H `interval ':1' day (3)` leading-precision
   form rejected by DataFusion's planner is dropped at extract
   time — see `tpch_extract_queries.rs` Q1 substitution comment.
   Q15's spec-form `CREATE VIEW revenue:s ... ; SELECT ... ;
   DROP VIEW` is rewritten as a single CTE for the same reason
   (the colon in `revenue:s` isn't a valid identifier and
   `ctx.sql()` is single-statement only).

**Reproducer** (~10 min wall-clock on Apple M4 Max):

```sh
# 1. Generate SF=1 if you haven't (~10 s, 320 MB).
cargo run --release -p ematix-flow-core --example tpch_generate -- \
    --sf 1 --out examples/tpch/data/sf1

# 2. Extract canonical 22 SQL files (idempotent).
cargo run --release -p ematix-flow-core --example tpch_extract_queries

# 3. Audit — confirms all 22 plan + execute, prints per-query rows.
cargo run --release -p ematix-flow-core --example tpch_22_audit

# 4. DataFusion bench (~5 min wall-clock).
cargo bench -p ematix-flow-core --bench tpch

# 5. PySpark bench (~6-8 min wall-clock; needs JDK 17+ in PATH).
JAVA_HOME=/opt/homebrew/opt/openjdk PATH="$JAVA_HOME/bin:$PATH" \
    .venv/bin/python scripts/bench-tpch-pyspark.py \
    --data-dir examples/tpch/data/sf1 --trials 3
```

**What this doesn't change.** The cross-host distributed
question is still gated on real cluster hardware — see
[Σ.C PR 2](#σc-pr-2--sf10-multi-process-baseline-2026-05-05) for
the multi-process baseline + deferred items. SF=10 22-query
numbers would take ~30 min Rust + ~15 min Spark on Apple M4 Max and
aren't published yet; the SF=10 4-query result above is a
representative sample.

---

## Σ.A1 — TPC-H representative set at SF=1

The Σ.A1 audit-by-running anchor: Q1 / Q3 / Q6 / Q19 against
single-node DataFusion 53.1, reading Snappy Parquet from
`examples/tpch/data/sf1/`. Materializes results into `Vec<RecordBatch>`
so we measure execution wall-time, not just plan-building.

### Σ.A1 PR 2 baseline (2026-05-05)

Hardware: Apple M4 Max, 36 GB RAM, macOS 15. SF=1 dataset = 320 MB
across 8 Parquet files (lineitem.parquet 202 MB, orders.parquet
52 MB, partsupp.parquet 39 MB, ...).

Method: criterion `sample_size = 10`, `measurement_time = 20s`,
warm-up 3s. Median of 10 samples reported with [low, high] from
criterion's bootstrap CI. Saved as baseline `sigma_a1_sf1_m3pro`.

| Query | Median | Range | Iterations | Workload shape |
|---|---|---|---|---|
| Q1  | **48.7 ms** | [48.5, 48.8]  | 440  | scan + 10-aggregate group-by |
| Q3  | **34.6 ms** | [34.4, 34.7]  | 605  | 3-way hash join + ORDER + LIMIT 10 |
| Q6  | **18.2 ms** | [18.2, 18.3]  | 1155 | scan + filter + single sum |
| Q19 | **38.0 ms** | [37.4, 38.4]  | 605  | 2-way join + complex disjunctive WHERE |

### Σ.C PR 1 — distributed batch SQL on the same TPC-H reps (2026-05-05)

First public numbers for the `DistributedBackend` path.
`crates/ematix-flow-distributed/benches/tpch_distributed.rs` runs
the same Q1/Q3/Q6/Q19 reps as the Σ.A1 single-node bench, but
through `DistributedBackend::read_arrow_stream`. Two
configurations:

- **distributed-of-one** (`peers = []`): degenerate single-worker
  cluster. Vanilla DataFusion under the hood; isolates the trait-
  surface cost of going through `DistributedBackend` vs raw
  `SessionContext`.
- **3 in-process workers**: spawns 3 tonic-served `Worker`s on
  free localhost ports, points the coordinator at them via
  `StaticWorkerResolver`. Exercises `DistributedPhysicalOptimizerRule`
  + cross-"pod" Arrow Flight at the floor RPC cost.

Apple M4 Max / SF=1 / criterion `sample_size = 10`,
`measurement_time = 15s`:

| Query | DF (1-node) | distributed-of-one | 3 in-process workers | of_one overhead | 3-worker overhead |
|---|---|---|---|---|---|
| Q1  | 48.7 ms | **48.9 ms** | **53.9 ms** | +0.4% | +10.7% |
| Q3  | 34.6 ms | **34.8 ms** | **37.1 ms** | +0.6% | +7.2% |
| Q6  | 18.2 ms | **18.5 ms** | **19.9 ms** | +1.6% | +9.3% |
| Q19 | 38.0 ms | **38.3 ms** | **43.9 ms** | +0.8% | +15.5% |

**Findings:**

1. **Trait-surface overhead is ≤1.6%.** Going through
   `DistributedBackend` instead of constructing a raw
   `SessionContext` adds essentially no cost. Validates the Σ.B
   design — the distributed code path costs nothing when no peers
   are configured.

2. **3-worker RPC overhead at SF=1 is 7–15%** — RPC + plan-
   serialization cost dominates the work for queries that finish
   in <50ms. This is the *floor* of distributed cost, not a real
   workload result. Expected crossover (where distributed wins) is
   at SF=10+ where per-query work scales past the fixed RPC cost.

3. **Cross-host numbers require EC2.** This bench runs all 3
   workers on the same machine, so RPC latency is loopback (~50µs).
   Real cross-host m6i.4xlarge × 4 numbers add network latency
   (~500µs intra-AZ) + I/O contention. They go in Σ.C PR 2 once
   we have the cluster provisioned.

**Reproducer:**

```sh
TPCH_MEASUREMENT_TIME_S=15 \
  cargo bench -p ematix-flow-distributed --bench tpch_distributed
```

Reads from `examples/tpch/data/sf1/`. Generate first via
`cargo run --release -p ematix-flow-core --example tpch_generate
-- --sf 1 --out examples/tpch/data/sf1`.

**What's still open in Σ.C:**

- ~~**PR 2 — multi-host comparison**~~: superseded by the SF=10
  multi-process numbers below. AWS path retained in `infra/` for
  anyone with cluster access; this project's deliverable is the
  honest single-host bench.
- **PR 3 — repro script + announcement-ready artifact**:
  `scripts/tpch-bench.sh` that brings up the compose stack + runs
  the suite + emits the comparison table.

### Σ.C PR 2 — SF=10 multi-process baseline (2026-05-05)

> **Honest framing.** Single-host run. The "3 in-process workers"
> configuration uses tonic over loopback inside one process; the
> docker-compose stack uses tonic over loopback between containers
> sharing the host kernel/CPU/memory/disk. Neither is cross-host.
> These numbers are useful as a **regression canary** + evidence
> the distributed planner runs correctly at scale. They are *not*
> proof of cross-host scaling, and shouldn't be cited as such.
> Real cross-host numbers stay deferred (homelab k3s across
> multiple boxes, or rented bare-metal). See
> [`docs/PHASE_SIGMA_PLAN.md`](PHASE_SIGMA_PLAN.md) Σ.C scope-shift
> note.

Same Apple M4 Max / criterion (`sample_size = 10`,
`measurement_time = 10s`, warm-up 3s); SF=10 dataset (3.2 GB
across 8 Snappy Parquet files; lineitem 2.1 GB / 60 M rows).

| Query | DF (1-node) | distributed-of-one | 3 in-process workers | PySpark | DF / PySpark |
|---|---|---|---|---|---|
| Q1  | **323.0 ms** [313.9 / 331.4] | **282.5 ms** [279.7 / 284.9] | **314.9 ms** [310.9 / 318.9] | **1318.2 ms** [1172 / 1909] | **0.245** (DF 4.08× faster) |
| Q3  | **233.2 ms** [228.8 / 238.5] | **217.8 ms** [215.3 / 220.0] | **228.1 ms** [224.3 / 231.5] | **2146.4 ms** [2118 / 2687] | **0.109** (DF 9.21× faster) |
| Q6  | **116.1 ms** [115.0 / 121.4] | **109.3 ms** [108.5 / 112.3] | **109.3 ms** [106.3 / 112.0] | **200.4 ms**  [195 / 269]   | **0.579** (DF 1.73× faster) |
| Q19 | **211.7 ms** [207.4 / 223.4] | **206.3 ms** [198.1 / 209.3] | **201.5 ms** [194.4 / 204.0] | **482.7 ms**  [399 / 611]   | **0.439** (DF 2.28× faster) |

(DataFusion / distributed columns: median, [low / high] from
criterion's bootstrap CI. PySpark column: median of 3 trials,
[min / max] across trials. Software: DataFusion 53.1, PySpark
4.1.1, JDK 23, Spark `local[*]` (12 logical cores), 4 GB driver
heap, `shuffle.partitions = 8`.)

**Findings:**

1. **All three configurations are within ~12% of each other.**
   At SF=10 on a single host, the distributed plan adds essentially
   no measured cost; the 3-worker config is a *touch* faster on
   Q19 and a touch slower on Q1. The spread is mostly run-to-run
   variance. None of the runs show distributed *winning* by a
   meaningful margin — that's expected single-host behaviour
   because all three configurations are CPU-bound on the same
   physical cores. Distributed wins require independent hardware.

2. **Distributed-of-one is consistently 3–13% faster than the
   raw single-node bench**, which is *surprising* — we expected
   trait-surface overhead to add 1–2%, not subtract. The most
   likely explanation is small differences in how the two benches
   build their `SessionContext`:
   `with_distributed_planner()` may default to a slightly different
   set of physical optimizer rules. Worth investigating in a
   follow-up; not a blocker for these numbers.

3. **3-worker overhead at SF=10 is ≤5%** vs distributed-of-one,
   down from 7–15% at SF=1. Per-query work has scaled past the
   fixed RPC cost. The crossover where distributed *beats*
   single-process is past SF=10 *and* requires hardware-isolated
   workers — neither condition holds here.

**Acceptance gate (single-host regression canary):** all four
distributed numbers within ±30% of single-node DataFusion on the
same hardware → **all four within ±13%, gate cleared with
substantial margin.**

**DataFusion vs PySpark (SF=10, single-host).** Geomean speedup
of **3.3×** across the four queries (range: 1.73× on Q6 to 9.21×
on Q3). Compared to the SF=1 head-to-head in the Σ.A1 PR 4
section above (geomean ~4.3× DF wins), the gap narrowed at SF=10
on Q1 (4.08× vs 4.0×, ~unchanged) and Q19 (2.28× vs 3.4×, gap
narrowed) but *widened* on Q3 (9.21× vs 6.8×) and on Q6 (1.73×
vs 3.5×; Spark closed half the gap there). Spark's optimizer is
amortising better as input size grows on Q3/Q6, but DataFusion
still wins comfortably on every query, even single-node-vs-
single-node. Per-trial PySpark variance is wide (Q1: 1172–1909 ms,
Q3: 2118–2687 ms) — Spark's JIT + GC tax shows up clearly even
after the discarded warm-up. DataFusion's bootstrap CIs are
tight by comparison (within ±5% of median).

The "scales as well as PySpark" claim is now grounded for SF=10
single-host: DataFusion *beats* single-node Spark by 1.7–9.2× on
the rep set. The independent question of whether the distributed
plan adds real value at scale is still gated on cross-host
hardware (deferred — see below).

**Reproducer:**

```sh
# Generate SF=10 (~3.2 GB on disk; ~minute on M-class).
cargo run --release -p ematix-flow-core --example tpch_generate -- \
    --sf 10 --out examples/tpch/data/sf10

# Single-node DataFusion (~5 min wall-clock @ 10s measurement).
TPCH_DATA_DIR=examples/tpch/data/sf10 TPCH_MEASUREMENT_TIME_S=10 \
    cargo bench -p ematix-flow-core --bench tpch

# Distributed-of-one + 3 in-process workers (~10 min wall-clock).
TPCH_DATA_DIR=examples/tpch/data/sf10 TPCH_MEASUREMENT_TIME_S=10 \
    cargo bench -p ematix-flow-distributed --bench tpch_distributed

# PySpark single-node baseline (~5 min wall-clock; needs JDK 17+).
JAVA_HOME=/opt/homebrew/opt/openjdk PATH="$JAVA_HOME/bin:$PATH" \
    .venv/bin/python scripts/bench-tpch-pyspark.py \
    --data-dir examples/tpch/data/sf10 --trials 3

# Optional: against the docker-compose stack on the same host
# (multi-process via container loopback, still NOT cross-host).
export TPCH_HOST_DATA_DIR=$PWD/examples/tpch/data
docker compose -f examples/distributed-cluster/docker-compose.yml up --build -d
EMATIX_DISTRIBUTED_PEERS=http://localhost:50051,http://localhost:50052,http://localhost:50053 \
TPCH_DATA_DIR=$PWD/examples/tpch/data/sf10 TPCH_MEASUREMENT_TIME_S=10 \
    cargo bench -p ematix-flow-distributed --bench tpch_distributed
docker compose -f examples/distributed-cluster/docker-compose.yml down
```

**What still needs real cluster hardware (deferred):**

- Headline "scales as well as PySpark" claim — needs network-
  separated nodes (homelab k3s + multiple boxes, or rented bare-
  metal at Hetzner / OVH). The `infra/` recipe still works for
  anyone with AWS access; this project no longer publishes AWS
  numbers as primary.
- SF=100 / SF=1000 — laptop-class hardware can't honestly
  produce these (data alone is 100 GB / 1 TB; the bench would be
  paging-bound, not compute-bound).

### Σ.A2 PR 5 — DuckDB-dialect audit (2026-05-05)

DuckDB's SQL surface is closer to DataFusion's than Spark's (both
inherit Postgres-style syntax + arrow-rs types). The translator's
expected scope was a handful of function-name aliases; in practice
the remap table has one entry: `list_value → make_array`.

**Acceptance: ≥90% pass rate on TPC-H + curated set.**

- TPC-H Q1/Q3/Q6/Q19 e2e through `dialect = "duckdb"` → DataFusion:
  **4/4 PASS** (revenue + row count match TPC-H reference).
- DuckDB unit tests (`tests/dialect_duckdb.rs`): **13/13 PASS** —
  pass-through (basic SELECT, aggregates, joins, window, CTE, INTERVAL
  literal), `list_value → make_array` remap, error paths.
- TPC-DS suite under `dialect = "duckdb"` (same 103 Spark-canonical
  queries from PR 4): **103/103 PASS**. DuckDB's parser is
  permissive enough to accept Spark-style queries; the only DuckDB-
  specific function (`list_value`) doesn't appear in TPC-DS.

```sh
cargo run --release -p ematix-flow-core --example tpcds_dialect_audit -- duckdb
# === Σ.A2 dialect audit (DuckDb) ===
# total:           103
# PASS:            103 (100.0%)
```

**Implication**: DuckDB→DataFusion is essentially a no-op for the
canonical query surface. Translator value-add is in cases where
users explicitly use DuckDB-isms (currently just `list_value`); add
new entries to `DUCKDB_TO_DF` as real-world queries surface gaps.

Σ.A2 status after PR 5: all 5 sub-PRs done. Acceptance gates met
(or vastly exceeded). Ready to start Σ.B (distributed-execution
trait refactor) or pause to ship v0.1.0.

### Σ.A2 PR 4 — TPC-DS Spark-dialect audit (2026-05-05)

Plan-only translator audit: each of the 103 official Apache Spark
TPC-DS queries (99 numbered + 4 a/b variants like `q14a`, `q23a`)
fed through `dialect::translate(_, Dialect::Spark)`, output handed
to a `SessionContext` registered with the canonical TPC-DS schema
(24 tables, types translated `CHAR(N) → VARCHAR`). DataFusion's
`create_physical_plan` exercises the planner. No data; no execution.

```text
=== Σ.A2 PR 4 audit ===
total:           103
PASS:            103 (100.0%)
TRANSLATE_FAIL:  0
PLAN_FAIL:       0

acceptance gate: ≥80% PASS — MET
```

**Headline: 100% PASS, no findings.** PR 2's function-name remap +
PR 3's LATERAL VIEW EXPLODE rewrite + DataFusion 53's native SQL
surface together cover the entire TPC-DS Spark dialect. The plan's
speculative concerns (correlated subqueries, complex-type literals,
window-frame INTERVAL syntax) all just work — either the queries
don't lean on those constructs, or DataFusion accepts them as-is.

**Audit reproducer:**

```sh
cargo run --release -p ematix-flow-core --example tpcds_dialect_audit
```

Reads `examples/tpcds/queries/spark/q*.sql` (Apache-Spark-canonical)
and `examples/tpcds/schema.sql` (auto-generated from
`apache/spark`'s `TPCDSSchema.scala`). Per-query failures land on
stderr with the underlying error; clusters by error-message-prefix
in the summary so common root causes are obvious.

**What this implies for Σ.A2:**

- PR 4's "iterate on translator gaps until ≥80%" task is a no-op
  for the canonical TPC-DS suite. Any future translator work
  triggers from real-world user queries that surface idioms the
  audit didn't catch.
- The 103 .sql files stay in the repo as a regression suite — any
  future change to the dialect translator or DataFusion bump gets
  re-checked against them via `tpcds_dialect_audit`.
- Σ.A2 acceptance is met. PR 5 (DuckDB dialect) is the next
  remaining sub-phase work item.

### Σ.A1 audit findings

**None.** All four queries execute cleanly through DataFusion 53.1
with no SQL surface gaps. The Σ.A1 plan reserved a PR 3 for "fix
audit gaps surfaced by the benches" — that PR will be a no-op for
the representative set. Σ.A2 (dialect translator) + Σ.C (TPC-H full
22-query suite) will surface gaps the four representative queries
don't reach (correlated subqueries, certain CTEs, complex-type
literal forms).

### Σ.A1 PR 4 head-to-head: DataFusion vs single-node PySpark (2026-05-05)

Same Apple M4 Max host, same SF=1 Parquet under `examples/tpch/data/sf1/`,
same `.sql` files. DataFusion via the criterion bench above; PySpark
via `scripts/bench-tpch-pyspark.py` (3 trials per query after a
discarded JIT warm-up; median reported).

Software baseline: DataFusion 53.1 (workspace pin), PySpark 4.1.1,
OpenJDK 23.0.2 (Homebrew). Spark configured `local[*]` (12 logical
cores), 4 GB driver heap, `spark.sql.adaptive.enabled = true`,
`shuffle.partitions = 8`.

| Query | DataFusion (ms) | PySpark (ms) | DataFusion / PySpark | PySpark trials min/max | rows |
|---|---|---|---|---|---|
| Q1  | **48.7**  | 192.6 | **0.253** (DF 4.0× faster) | 191.1 / 324.6 |  4 |
| Q3  | **34.6**  | 235.5 | **0.147** (DF 6.8× faster) | 228.5 / 437.2 | 10 |
| Q6  | **18.2**  |  64.2 | **0.283** (DF 3.5× faster) |  55.1 / 186.8 |  1 |
| Q19 | **38.0**  | 130.8 | **0.290** (DF 3.4× faster) | 112.0 / 342.6 |  1 |

**Geomean speedup: ~4.3×.** Σ.A1 PR 4 acceptance gate was DataFusion
median ≤ PySpark median on all four with geomean ≥1.5×; cleared
comfortably.

Why DataFusion wins by this margin on these workloads:
- No JVM cold-start tax (Spark first-trial is consistently 1.5–3×
  slower than median; DataFusion has none of that).
- Vectorized scan + arrow-rs batch dispatch outperforms Spark's
  Tungsten codegen on small/medium queries because Spark's
  optimizer + planner overhead is amortized over fewer rows.
- No shuffle in any of these four queries (Q3's join is broadcast at
  this scale); both engines run single-stage. SF=10/100 with bigger
  shuffle is where Spark's distributed plan helps + its single-node
  edge narrows.

Row counts match exactly across both engines (Q1: 4, Q3: 10, Q6: 1,
Q19: 1) — confirms the SQL produces equivalent results on both.

### Σ.A1 PR 4 follow-up: vs Polars (2026-05-05)

Polars is the closest peer to DataFusion in positioning (Rust under
Python, in-process, vectorized), so a head-to-head matters even
though both engines are single-node only. Same Apple M4 Max / SF=1 /
Parquet / .sql files. Polars 1.40.1 via `polars.SQLContext`. Median
of 3 trials after 1 discarded warm-up.

| Query | DataFusion (ms) | Polars (ms) | PySpark (ms) | DF/Polars | Polars/PySpark | rows |
|---|---|---|---|---|---|---|
| Q1  | 48.7 | **FAIL**  | 192.6 | — | — | — |
| Q3  | 34.6 |  46.8     | 235.5 | 0.739 (DF 1.35× faster) | 0.199 (Polars 5.0× faster than Spark) | 10 |
| Q6  | 18.2 | **10.0**  |  64.2 | **1.82 (Polars 1.82× faster)** | 0.156 (Polars 6.4× faster than Spark) | 1 |
| Q19 | 38.0 | 366.3     | 130.8 | 0.104 (DF 9.6× faster) | 2.800 (Polars 2.8× **slower** than Spark) | 1 |

**Audit findings — surface for Σ.A2 dialect translator:**

- **Q1 fails on Polars**:
  `SQLSyntaxError: unsupported interval syntax ('INTERVAL '90' DAY')`.
  TPC-H spec uses `INTERVAL` literals; Polars's SQL parser is younger
  and doesn't yet accept them. A Polars-targeted dialect translator
  would rewrite `DATE 'X' - INTERVAL 'N' DAY` → `DATE 'X-N-days'`
  (concrete literal). Σ.A2 future work.
- **Q19 collapses on Polars** (9.6× slower than DataFusion, 2.8×
  slower than PySpark). The 3-clause disjunctive `WHERE (... OR ... OR
  ...)` over a 2-way join apparently doesn't simplify into something
  Polars's optimizer can vectorize. Worth a docs.rs investigation
  but not a Σ.A1 blocker — DataFusion handles this cleanly.

**Where Polars wins**: Q6 (vectorized filter + single sum) beats
DataFusion 1.82×. Polars's tight scan-+-aggregate loop is well-tuned
for this workload shape. Same-engine-family Polars-vs-DataFusion is
close (~10–80 ms swings); DataFusion's win on the suite comes from
SQL coverage + complex-query robustness, not raw scan speed.

#### Q6 tuning audit (2026-05-05)

We swept `SessionConfig` knobs to see if any close the Polars gap on
Q6. None do; in fact the in-decoder filter knobs make it worse.
Reproducer: `cargo run --release -p ematix-flow-core --example
tpch_q6_tune`.

| Config | Median (ms) |
|---|---|
| default | 16.9 |
| + `target_partitions=12` | 17.2 |
| + `repartition_file_scans=true` | 17.1 |
| + `parquet.pushdown_filters=true` | **28.3** (worse) |
| + `parquet.reorder_filters=true` | **62.9** (much worse) |

`target_partitions` is already at `num_cpus::get()` by default;
DataFusion already splits the Parquet into 12 byte-range scan groups
automatically (visible in the EXPLAIN as
`file_groups={12 groups: [[…0..17M], …]}`). The Q6 predicates are
cheap enough to evaluate post-decode on Arrow batches; pushing them
into the Parquet decoder pays a per-batch filter-mask cost without
recovering it.

**Implications for the bench harness + Σ.B work:**
- Keep the criterion bench's `SessionContext::new()` (no custom
  config). The defaults are right.
- **Do not** globally enable `pushdown_filters` — it hurts simple-
  aggregate queries like Q6.
- ~~Polars's 1.82× edge on Q6 is hand-tuned vectorized inner
  loops~~ — **the original deep-dive below inverted this, but the
  inversion was itself wrong**: it compared Polars's *parquet*
  number against DataFusion's *MemTable* number. The 2026-05-11
  apples-to-apples re-run below puts Polars at 1.9 ms vs
  DataFusion 5.96 ms on MemTable — Polars wins by 3.13×. The
  edge IS the fused filter-+-sum inner loop, not parquet decode.

#### Q6 deep dive: where does the 1.82× actually go? (2026-05-05)

The original tuning audit above swept `SessionConfig` knobs — all
no-ops or worse — and concluded the gap was in DataFusion's
vectorized aggregate. **That conclusion was wrong.** Two new
investigations land the real story:

**(1) MemTable isolation.** Pre-decode `lineitem.parquet` into
in-memory Arrow batches once at startup (decode is *not* timed),
register as a MemTable, run Q6 against it. Removes parquet I/O
from the hot path entirely:

| Source              | Median (ms) |
|---------------------|-------------|
| Parquet, default    | 17–38 (system-load variance; see notes) |
| **MemTable, default** | **8.89** |
| Polars (parquet, reference) | 10.0 |

**8.89 ms beats Polars's 10.0 ms.** When DataFusion runs Q6 on
already-decoded Arrow data, it's faster than Polars including
parquet decode. The whole gap is parquet decode efficiency, not
the aggregate hot path.

**(2) EXPLAIN ANALYZE per-operator wall-time** (default config,
parquet source):

| Operator                | Wall-time |
|-------------------------|-----------|
| `DataSourceExec` (parquet scan)  | scan_total 123 ms / 12 parallel ≈ **10 ms wall-clock** |
| `FilterExec`            | 49 ms compute / 12 parallel ≈ **4 ms wall-clock** |
| `AggregateExec` (partial × 12)   | 96 µs total — **sub-millisecond** |
| `CoalescePartitionsExec`         | 4 µs                |
| `AggregateExec` (final) | 7 µs                |
| `ProjectionExec`        | 459 ns              |

The aggregate is sub-millisecond. **The 17ish ms of headline
parquet wall-time is ~10ms scan + ~4ms filter + a few ms of plan
overhead.** Polars is faster on the scan/filter portion of that —
likely fused scan-+-filter inner loop with column-aware decode —
but the aggregate compute is already at parity (and arguably ahead
of) Polars on Arrow.

**Implications:**

- **DataFusion's vectorized aggregate is competitive.** No upstream
  PR needed there. Earlier "would need upstream PRs to the
  aggregate path" advice was based on an incorrect attribution
  of the gap.
- **The gap-closing path is parquet decode/scan.** Worth a future
  investigation when more bench coverage shows systematic decode-
  bound losses; not worth chasing for Q6 alone since DataFusion
  still wins the broader suite (5.87× geomean over PySpark across
  22 queries, see Σ.C extension above).
- **Pre-decoded streaming pipelines unaffected.** Production
  ematix-flow streaming pipelines already keep the hot batch in
  memory between transform stages — they hit the MemTable path,
  not the parquet path. Q6 against a Kafka-source pipeline would
  match the 8.89 ms number, not the 17ish ms parquet number.

**Variance note.** The parquet numbers in this run swung 17–38 ms
across system-load levels; the historical baseline of 16.9 ms
(2026-05-05 original audit) was on an idle host. The MemTable
numbers were stable (7.9–10.1 ms range). Both findings stand
regardless: the aggregate is fast; the parquet decode is what
varies.

Reproducer (no change):

```sh
cargo run --release -p ematix-flow-core --example tpch_q6_tune
```

Section 3 of the script's output now prints the EXPLAIN ANALYZE
breakdown so a future investigator can re-validate the per-
operator timings on their hardware.

**Net for the Σ.A1 rep set**:
- DataFusion: clean wins on Q1 (Polars fails), Q3 (1.35×), Q19 (9.6×).
- DataFusion: loses to Polars on Q6 (1.82× the wrong way).
- Both crush PySpark by 3–6× single-node, except Polars's Q19
  collapse where Spark's optimizer gets a rare win.

This makes the case for ematix-flow's positioning: same-class single-
node performance to Polars on hot loops, broader SQL surface,
distributed scaling path via Σ.B (Polars has no distributed story).

### Σ.A1 PR 4 follow-up redux: vs Polars apples-to-apples (2026-05-11)

The 2026-05-05 Polars audit (above) had a methodology bug: it
compared Polars's `scan_parquet().collect()` numbers against
DataFusion's MemTable numbers. The 2026-05-11 re-run pins both
engines to the same source shape per row, also adds the previously-
missing memtable comparison, and unblocks the queries Polars's SQL
parser rejects (Q1 `INTERVAL '90' DAY`; Q3 / Q19 implicit
`FROM a, b, c` cross-joins) via side-by-side `.polars.sql`
variants under `examples/tpch/queries/`. The variants are
semantically identical to the canonical TPC-H text — the audit
trail is in the file headers.

Polars 1.40.1, 5-trial median after 1 warm-up:

| Query | DataFusion parquet | Polars parquet | DataFusion MemTable | Polars MemTable |
|---|---|---|---|---|
| Q1  | 48.7  | **40.9** | n/a   | **35.2** |
| Q3  | **34.6**  | 52.2 | n/a   | 35.6 (≈ parity) |
| Q6  | 17.56 | **10.7** | 5.96  | **1.9** |
| Q19 | **38.0**  | 387.1 | n/a   | 352.4 |

(DataFusion Q1/Q3/Q19 parquet numbers are the May 5 baseline —
deltas across re-runs on Q6 were within ±5% on idle host.)

**Findings, corrected:**

- **Q1 now passes.** A `.polars.sql` variant with the interval
  resolved to a `DATE` literal runs in 40.9 ms parquet / 35.2 ms
  MemTable. Polars wins Q1 by ~20%; close.
- **Q3 / Q19 also need `.polars.sql` variants** (explicit JOINs).
  Both still fall to DataFusion once they parse — Q3 by 1.51×,
  Q19 by 10.2×.
- **Q6 is genuinely Polars's win, and the May 5 deep-dive's
  parquet-vs-MemTable comparison was apples to oranges.** Same
  source on both sides, Polars hits 1.9 ms vs DataFusion 5.96 ms
  — Polars 3.13×. The DataFusion EXPLAIN ANALYZE puts the
  `AggregateExec` at 96 µs total but the `FilterExec` at 35.86 ms
  of compute (across 14 parallel partitions). Polars apparently
  fuses the filter + sum into one pass over the scan.

**Where the engine gap is, concretely:** Q6 is `SUM(a * b) WHERE
predicate(c, d, e)`. DataFusion materializes the filter mask in
`FilterExec`, then `AggregateExec` walks the surviving rows.
Polars (looks like) folds the predicate into the same Arrow
kernel that does the multiply-+-sum, never materializing a
selection vector. A DataFusion `FusedFilterSumExec` physical
operator that does the same thing for the simple-filter-+-single-
aggregate shape would close this gap. Scope is upstream-DataFusion
work; tracked as a future spike.

### JDK note

PySpark 4.x officially supports JDK 17 / 21. Homebrew's `openjdk`
cask installs 23, which works once Spark is started with
`-Djava.security.manager=allow` (JDK 18+ deprecated
`Subject.getSubject` and Spark's UGI shim still calls it). The script
sets that flag automatically. If running on JDK 17 / 21, the flag is
harmless.

### Reproducing the head-to-head

```sh
# 1. Generate data + run DataFusion benches (Σ.A1 PR 1 + 2 above).
cargo run --release -p ematix-flow-core --example tpch_generate -- \
    --sf 1 --out examples/tpch/data/sf1
cargo bench -p ematix-flow-core --bench tpch -- \
    --save-baseline sigma_a1_sf1_<host>

# 2. Activate the venv with PySpark installed; ensure Java is on PATH.
source .venv/bin/activate
export JAVA_HOME=/opt/homebrew/opt/openjdk  # or wherever
export PATH="$JAVA_HOME/bin:$PATH"

# 3. Run the head-to-head.
python scripts/bench-tpch-pyspark.py

# 4. Edit DATAFUSION_BASELINE_M3PRO_SF1_MS in the script to match
#    your host's DataFusion numbers if you've re-baselined; the
#    DF/PySpark ratio in the output table assumes Apple M4 Max DF numbers
#    by default.

# 5. Polars sibling: no Java needed.
python scripts/bench-tpch-polars.py
```

~~If running on Linux x86_64 EC2 m6i.4xlarge for Σ.C, both columns
will need re-running.~~ **Superseded by the 2026-05-05 Σ.C scope
shift** — AWS path retained in `infra/` but no longer canonical;
Apple M4 Max is now the published-numbers reference. See
[`docs/PHASE_SIGMA_PLAN.md`](PHASE_SIGMA_PLAN.md) Σ.C section for
the scope-shift rationale and `Σ.C extension` above for the
22-query SF=1 head-to-head against `datafusion-distributed`-based
ematix-flow.

### Reproducing

```sh
# 1. Generate SF=1 data (~10s, 320 MB).
cargo run --release -p ematix-flow-core --example tpch_generate -- \
    --sf 1 --out examples/tpch/data/sf1

# 2. Run all four benches (~90s wall-clock with the defaults).
cargo bench -p ematix-flow-core --bench tpch

# 3. Run a single query for fast iteration:
cargo bench -p ematix-flow-core --bench tpch -- q06

# 4. Save / compare against a named baseline:
cargo bench -p ematix-flow-core --bench tpch -- --save-baseline mine
cargo bench -p ematix-flow-core --bench tpch -- --baseline mine

# 5. Tighten the measurement window for fast iteration / loose for CI:
TPCH_MEASUREMENT_TIME_S=10 cargo bench -p ematix-flow-core --bench tpch
```

If the data dir lives somewhere else, point the bench at it via
`TPCH_DATA_DIR=/path/to/sf1 cargo bench -p ematix-flow-core
--bench tpch`. Bench panics with a clear message if any expected
Parquet file is missing.

### When to re-run

- **PRs that touch `crates/ematix-flow-core/src/transform.rs`** or
  any of the `LazySqlTransform` / `DataFusionTransform` machinery.
  Compare against `sigma_a1_sf1_m3pro` if running on M3-class
  hardware; otherwise capture a fresh baseline on the PR's host.
- **DataFusion / arrow upgrades.** Any version bump in
  `Cargo.toml`'s `datafusion` or `arrow-*` workspace deps. Diff
  against the baseline; flag regressions ≥10% in the PR body.
- ~~**Σ.B Ballista work.**~~ **Σ.B distributed-execution work
  (shipped 2026-05-05).** `DistributedBackend` lives in
  `crates/ematix-flow-distributed/`; `tpch_distributed.rs`
  bench runs the rep set against it. Σ.C extension lands the
  full 22-query head-to-head against PySpark.

### Hardware variance

Numbers above are from Apple M4 Max (Apple Silicon). Linux x86_64
m6i.4xlarge baseline lands in Σ.A1 PR 4 alongside PySpark numbers;
expected within ~2× of Apple M4 Max on these queries.

### Σ.D cross-engine snapshot (2026-05-11)

Fresh re-baseline with the Σ.D spike operators (issues #44, #45, #46, #47, #48). Same Apple M4 Max / SF=1 / Parquet under `examples/tpch/data/sf1/`, same JDK 23, PySpark 4.1.1, Polars 1.40.1. 5-trial median for the kernel paths, 3-trial median for PySpark (Spark needs a few iterations to warm).

**Head-to-head on the four representative queries:**

| Query | PySpark | DataFusion (today, MemTable) | Polars (today, MemTable) | Σ.D operator | vs PySpark | vs DF |
|---|---|---|---|---|---|---|
| Q6 (single SUM + filter) | 72.4 ms | 6.10 ms | 1.9 ms | **1.24 ms** (Σ.D1 wrapped) | **58.4×** | **4.9×** |
| Q6 (same)                | 72.4 ms | 6.10 ms | 1.9 ms | **0.95 ms** (Σ.D3 JIT)     | **76.2×** | **6.4×** |
| Q1 (8-output multi-agg + group-by + filter) | 198.4 ms | 22.15 ms | 35.2 ms | **3.06 ms** (Σ.D2 wrapped) | **64.8×** | **7.2×** |

The Σ.D operators preserve every bit of DataFusion's prior single-node win over PySpark and **multiply it** by another 5-7× on top via fused-pass kernel emission. Geomean of the four-query rep set when the operators apply to all of them would land at **≈70× faster than single-node PySpark** — vs DataFusion's current 4.1× geomean on the same set today.

**Where the Σ.D operators don't apply yet:**

Today's Σ.D1 + Σ.D2 + Σ.D3 cover the `Aggregate over Filter over Scan` plan shape — one filter, one or more SUM/COUNT/AVG accumulators, optional low-cardinality group-by. That's exactly Q1 and Q6's shape. The remaining 20 TPC-H queries land in shapes the planner rule (issue #45 phase 4) doesn't match yet; their numbers stay at today's DataFusion baseline. Suite-wide geomean shift requires the day-2+ generalization to land.

### Σ.D cross-engine — full 22-query PySpark baseline (2026-05-11)

Useful for spotting which queries are the next optimization targets — anything PySpark spends >300 ms on is a candidate for a custom DataFusion physical operator if the shape generalizes.

| Query | PySpark (ms) | Plan shape | Σ.D potential |
|---|---|---|---|
| Q1  | 198.4 | multi-agg + group-by + filter | ✅ shipped (Σ.D2) |
| Q2  | 303.1 | sub-aggregate + join | ⚠ correlated subquery; needs different operator |
| Q3  | 297.8 | 3-way join + group-by + filter | 🟡 fuse post-join aggregate (Σ.D extension) |
| Q4  | 210.8 | EXISTS subquery + filter + agg | ⚠ SEMI-JOIN operator |
| Q5  | 356.6 | 5-way join + group-by | 🟡 fuse post-join agg over joined rows |
| Q6  | 72.4  | single SUM + filter | ✅ shipped (Σ.D1 / Σ.D3) |
| Q7  | 281.5 | 4-way join + group-by | 🟡 same as Q5 |
| Q8  | 215.5 | 6-way join + CASE-WHEN agg | 🟡 CASE-WHEN-in-SUM is common (Q12 also) |
| Q9  | 541.6 | 5-way join + group-by | 🟡 same as Q5 |
| Q10 | 460.6 | 4-way join + group-by + ORDER LIMIT | 🟡 post-join agg + top-K |
| Q11 | 139.8 | join + HAVING with sub-aggregate | ⚠ correlated SQL |
| Q12 | 285.8 | join + CASE-WHEN agg | 🟡 fuse `SUM(CASE WHEN p THEN x ELSE 0 END)` |
| Q13 | 691.3 | OUTER JOIN + 2-level group-by | ⚠ outer-join + GROUP-OF-GROUP |
| Q14 | 119.2 | join + filter + agg | 🟡 small post-join agg |
| Q15 | 146.5 | view-based aggregate | ⚠ correlated subquery |
| Q16 | 228.9 | filter + group-by COUNT-DISTINCT | ⚠ distinct aggregates |
| Q17 | 340.6 | correlated AVG subquery | ⚠ correlated subquery |
| Q18 | 578.1 | sub-aggregate filter + ORDER LIMIT | ⚠ HAVING + top-K |
| Q19 | 92.5  | join + disjunctive predicate + SUM | 🟡 disjunctive (OR) predicate-fusion |
| Q20 | 148.1 | nested subquery + IN | ⚠ correlated SQL |
| Q21 | 686.2 | EXISTS + NOT EXISTS + ORDER LIMIT | ⚠ multi-semi-join |
| Q22 | 294.9 | UNION + subquery + COUNT-DISTINCT | ⚠ distinct + union |

**Reading the legend:**

- ✅ shipped — Σ.D1 / Σ.D2 already handle this query's shape.
- 🟡 same-pattern extension — the existing Σ.D2 architecture extends naturally once the planner rule recognizes "Aggregate over Filter over **Join**" or "SUM(CASE …)" or "single ORDER + LIMIT N". These are the **next** Σ.D-arc operators to consider; each unlocks 3-6 queries.
- ⚠ different operator — correlated subqueries, semi-joins, distinct aggregates, outer joins, top-K with a non-trivial sort. Separate research directions.

### Σ.D arc next-target ranking (by potential queries covered)

1. **Σ.D4 — fuse post-join aggregate.** Unlocks **Q3, Q5, Q7, Q9, Q10, Q14, ~Q19**. The join itself stays in DataFusion; we replace the post-join `Aggregate → Filter` with a `FusedAggregateExec` that reads the join's output stream directly. Same kernel shape as Σ.D2.
2. **Σ.D5 — `SUM(CASE WHEN p THEN x ELSE 0 END)` rewrite.** Unlocks **Q8, Q12, more**. Effectively predicate-conditional accumulation inside the existing fused loop. Small extension to the day-2+ predicate AST.
3. **Σ.D6 — fused top-K (heap-based ORDER BY + LIMIT).** Unlocks **Q10, Q18, Q21** (also covers many real-world dashboard "top 100" queries). Doesn't require JIT; pure-Rust impl.
4. **Σ.D7 — disjunctive (OR) predicate fusion.** Unlocks **Q19** specifically (already DataFusion-fast at 38 ms; further win modest). Lower priority.
5. **Σ.D8 — SEMI-JOIN / EXISTS rewrite.** Unlocks **Q4, Q11, Q15, Q17, Q20, Q21, Q22** but is a separate operator class (set-membership rather than aggregate). Real work.
