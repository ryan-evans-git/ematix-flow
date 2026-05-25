# Σ.T — SF=10 weakness-closure architecture

**Status:** working architecture doc (not a release artifact)
**Date:** 2026-05-25
**Author:** architect agent (cold-read, no main-thread context)
**Branch:** `perf/sigma-q-single-node-parity` at HEAD `dc2d457`
**Companions:** `docs/plans/CURRENT.md` (sidecar Phase 1+2), `docs/PHASE_SIGMA_Q_M_JOIN_REORDER.md` (rejected Σ.Q.M arc), `docs/SIGMA_Q_SINGLE_NODE_PARITY.md` (Σ.Q closeout).

This doc complements — does **not** replace — the sidecar plan in `docs/plans/CURRENT.md`. The sidecar work is the single largest committed lever; everything below is framed against it: which losses sidecars close, which they don't, and what the next-best lever for the remainder looks like.

---

## 0. Scope + reading order

| | |
|---|---|
| **Target dataset** | `bench-results/release-2026-05-24/BENCHMARKS-sf10-noiseB.md` |
| **Target queries** | Q01, Q05, Q07, Q08, Q17, Q18 (the 6 where DuckDB beats ematix-flow at SF=10) |
| **Baseline geomean** | 22q SF=10 ematix-flow/DuckDB ≈ 0.80 (per `project_sigma_q_l13_to_l16_session.md`); current run shows ratios consistent with a ~0.85 — see §0.1 caveat below |
| **Existing wins** | 14 of 22 queries faster than DuckDB at SF=10; 2 faster than Polars too |
| **Forbidden lever** | Adding new `PhysicalOptimizerRule`s as the primary lever (see `project_optimizer_codegen_sensitivity.md`) |

### 0.1 Bench-noise caveat

The headline numbers in the user-supplied prompt cite `BENCHMARKS-sf10-noiseB.md`. Two related observations from the project's memory:

1. The closeout session (`project_sigma_q_l13_to_l16_session.md`) reported 0.80 geomean with 14 wins.
2. The Σ.S.B cascading-bloom A/B (`project_sigma_sb_cascade_neg.md`) at T=5 reported 0.738 geomean with 17 wins on the **same baseline configuration**, with the explicit lesson: `EMAT_RG_DECODE_CACHE=1 + EMAT_RH_SUM_F64=1 + sideband bloom set` are all required to hit the proper-config baseline.

The 6 queries cited by the prompt (Q01, Q05, Q07, Q08, Q17, Q18) overlap heavily with the queries that flip in and out of "win" status in those benches — Q17 and Q18 in particular hover on the win/loss boundary. Before treating any of these as confirmed losses requiring structural work, the closure plan should re-validate at TRIALS=5 WARMUPS=2 with the full env-checklist (`feedback_full_bench_env_checklist.md`).

That said: Q05 (1.31×), Q07 (1.13×), and Q08 (1.10×) are durable losses across every bench in the repo. Q01, Q17, Q18 are marginal. The remainder of this doc treats the 6 as a single weakness corpus while flagging which are noise-band.

---

## 1. Per-query root cause

Each subsection: (a) where the cost goes, (b) what we already tried, (c) what hasn't been tried, (d) sidecar-index applicability.

### 1.1 Q01 — `SUM/COUNT GROUP BY l_returnflag, l_linestatus` (lineitem only)

**Median:** ematix 247.18 ms / DuckDB 238.45 ms / **1.04× faster**

**Where the cost is.** Q01 is a single-table aggregate over 60M lineitem rows with a low-cardinality 4-group key. There is no filter (or only a tiny date filter that lets most rows through). The cost is dominated by:

1. **Parquet decode** of `l_returnflag`, `l_linestatus`, `l_quantity`, `l_extendedprice`, `l_discount`, `l_tax`. Six columns, full table.
2. **Two-stage aggregate** (`Partial` partitioned across 14 cores → `Final`).
3. A small amount of arithmetic — `SUM(l_extendedprice * (1 - l_discount))`, etc.

There is no join. There is no significant filter. There is no skew. **This is a pure decode + aggregate kernel benchmark**, and the 1.04× gap is the realistic floor for "DuckDB's vectorised pipeline executor vs our DataFusion+Arrow stack on the simplest possible TPC-H query." With `EMAT_RH_SUM_F64=1` enabled we already win on harder shapes (Q21 1.79×, Q22 5.9×). Q01's gap is what's left after kernel parity: codegen and instruction-cache footprint.

**What we've tried** (rejected, in memory):
- `project_sigma_nf3_beats_stock.md` — RobinHood AggregateExec beats DataFusion stock on 10K-200K cardinality COUNT GROUP BY by 1-5%. Q01's 4 groups are ten orders of magnitude smaller — the wins don't translate.
- `project_sigma_r2_rejected.md` — RobinHoodAvgF64Exec on Q17 regressed +40-55%; lesson generalises ("21.6% self-time ≠ 21.6% reclaimable").
- `project_sigma_e5_per_filter_exact.md` (per `project_sigma_e5_exact_collides_inject_rules.md`) — earlier Exact-pushdown work landed +91% Q01 wins on SF=1 but the path doesn't help at SF=10 where decode is already dominant.

**What hasn't been tried.**
- **PGO (profile-guided optimisation) on the release build.** `project_optimizer_codegen_sensitivity.md` lists this as the explicit remediation for the codegen tax; it has never been measured. A PGO build that uses the 22q SF=10 trace as the training workload would tell LLVM which paths are hot for exactly this aggregate shape.
- **Σ.K.2-style pre-planning dict-preservation routing for `l_returnflag` / `l_linestatus`.** Both are very-low-cardinality strings that should arrive as Dictionary, but per `project_dict_arrival_blocker.md` they don't. Forcing dict-preservation would halve the GroupValues intern cost.
- **LLVM-codegen pinning** — the `lld` + `-Ccodegen-units=1` + `-Cpanic=abort` combo for the release profile has never been re-measured at SF=10.

**Sidecar applicability.** **None.** Q01 has no selective predicate. Sidecar indexes don't help full-table aggregates.

### 1.2 Q05 — 5-way fact-and-dim (region × nation × supplier × customer × orders × lineitem)

**Median:** ematix 187.74 ms / DuckDB 143.71 ms / **1.31× — the worst loss in the set**

**Where the cost is.** Q05 is the canonical "star join" query. The pattern:

```
region (r_name = 'ASIA')   →  1 row
   ⋈  nation               →  5 rows
   ⋈  supplier             →  ~50K rows
   ⋈  lineitem             →  ~12M rows (after FK join on l_suppkey)
   ⋈  orders               →  filtered by o_orderdate range
   ⋈  customer             →  FK join on c_custkey
```

DuckDB closes this via **dynamic filters**: once `region.r_name='ASIA'` evaluates to 1 row, it propagates the set of qualifying `n_nationkey` → `s_nationkey` → `l_suppkey` values into the lineitem scan, so lineitem is decoded with a row-group / page filter on `l_suppkey`. Our plan builds the joins in dependency order and the lineitem scan sees no `l_suppkey` filter.

Three specific sources of cost we can name:

1. **Build-side hashing in non-leaf joins** — without dim filter propagation, the `customer ⋈ orders` and `orders ⋈ lineitem` joins build hashes from large unfiltered inputs.
2. **Post-join filtering** — `customer.c_nationkey IN {qualifying nations}` is checked late, after the full Cartesian-style cascade has materialised.
3. **Memory bandwidth** — the intermediate join result before the final aggregate is in the tens-of-millions of rows; DuckDB processes a fraction of that.

**What we've tried** (rejected, in memory):
- `project_sigma_qm_slice2_rejected.md` — Σ.Q.M Slice 2 (depth-1 Inner Join descent in dim detection): geomean 0.80 → 1.02, Q05 188 → 218. Root cause: DataFusion's CSE doesn't share Join outputs, so the joined dim subtree is double-evaluated.
- `project_sigma_qm_slice4_spike_rejected.md` — Σ.Q.M Slice 4 SPIKE (orders → lineitem redundant-semi injection): Q05 188 → 218 (+16%). Root cause: double hash-build of the same keys (semi build + outer Inner build), CSE doesn't share Join build sides.
- `project_sigma_sb_cascade_neg.md` — cascading L9 bloom over filtered dim chain: neutral at T=5; helped Q07 (-5.2 ms) but flipped Q17/Q18 out of "win" column.

**What hasn't been tried.**
- **Real join-order rewriter** (a cost-based optimiser pass that picks build vs probe side based on input cardinality, rather than DataFusion's default left-side-as-probe). This is the gap `project_sigma_qm_slice4_spike_rejected.md` explicitly names: "The correct mechanism is join reordering — replace the Inner Join operator's structure rather than wrapping it with redundant work."
- **Bloom-from-build-side into TableScan column statistics** — currently Σ.J.2.b ships blooms over Flight headers (`project_sigma_j2b_v_landed.md`), but the consumer (`EnableContextBloomRule` per `project_sigma_j2b_vi_landed.md`) wraps scans in `BloomFilterExec` which filters *after* decode. A consumer that pushes the bloom into the parquet row-group eliminator would do real decode skipping for the Q05 lineitem scan.
- **Sidecar Bloom-on-FK** — see §1.7.

**Sidecar applicability.** **High but conditional.** If `lineitem` has a sidecar page-Bloom on `l_suppkey` and `l_orderkey`, the supplier-filtered chain can probe the bloom and skip row groups where no surviving suppkey exists. **Conditional** because (a) Q05's predicate is `r_name = 'ASIA'` which propagates through 4 joins to land on `l_suppkey` — the planner needs to derive the post-region set of suppkey values at plan time, which it currently doesn't; (b) the saved decode work has to beat the bloom probe overhead, and on FK-heavy joins this has historically been net-negative (`project_l9_bloom_consumer_findings.md` — "bloom-on-FK is net-negative").

### 1.3 Q07 — 5-way join (supplier × lineitem × orders × customer × nation × nation)

**Median:** ematix 158.41 ms / DuckDB 139.83 ms / **1.13× — was 1.95× pre-Σ.Q.L13-L16**

**Where the cost is.** Q07 is the volume-shipping query. Two `nation` aliases (one for supplier nation, one for customer nation) with a 2-element filter `(n1.n_name, n2.n_name) IN ((FRANCE, GERMANY), (GERMANY, FRANCE))`. Year-bucketed aggregate at the top. The query already came down from 281 ms to 159 ms during Σ.Q.L15 / L16; the remaining ~20 ms gap to DuckDB is structurally:

1. **Same dim-filter-propagation gap as Q05** — the nation filter is tiny (2 of 25 nations) and DuckDB propagates this through `nation ⋈ supplier ⋈ lineitem` to filter lineitem to the relevant `l_suppkey` set. We probe more rows.
2. **Year-bucket aggregate** — `EXTRACT(YEAR FROM l_shipdate)` is a per-row scalar op evaluated before aggregation. Likely a small contributor.

**What we've tried.**
- Same rejected Σ.Q.M arc as Q05 — Slice 2 regressed Q07 from 159 → 187 ms.
- Σ.Q.L15 (Inner-L9 with ratio=1024) closed the bulk of the gap.
- Cascading bloom (Σ.S.B) gained −5.2 ms but flipped other queries out.

**What hasn't been tried.**
- **Filter-derived bloom on `s_nationkey`** — when the planner sees `nation.n_name IN ('FRANCE', 'GERMANY')`, it can statically derive the set of `n_nationkey` values that survive (still requires reading nation, but nation is 25 rows). That set becomes an in-memory IN-list passed sideband to the supplier scan. Same shape as Σ.J.2.b context blooms but driven by a small-constant-set filter rather than by a hash-build emitter.
- **Sidecar Bloom on `l_suppkey`** — same applicability as Q05.

**Sidecar applicability.** **Medium.** Same logic as Q05 — a sidecar index on `l_suppkey` helps *if* the planner can derive the surviving suppkey set at plan time. The nation predicate makes this cleaner than Q05's region predicate (1 hop to supplier vs 2).

### 1.4 Q08 — 7-way join (part × supplier × lineitem × orders × customer × nation × region × nation)

**Median:** ematix 194.53 ms / DuckDB 176.80 ms / **1.10× — borderline**

**Where the cost is.** Q08 is "national market share" — the most join-heavy TPC-H query. Predicates: `region.r_name = 'AMERICA'`, `part.p_type = 'ECONOMY ANODIZED STEEL'`, `o_orderdate BETWEEN ...`. Part filter is very selective (1 row in ~200K). The cost goes to:

1. **Same dim-filter-propagation gap** as Q05/Q07.
2. **Specifically the part → lineitem propagation**: `p_partkey` after the part filter is a small set; DuckDB pushes it as a dynamic filter into the lineitem scan on `l_partkey`. We do not.

**What we've tried.** Same Σ.Q.M and Σ.S.B arcs as Q05/Q07. Q08 regressed under Slice 2 (202 → 272 ms).

**What hasn't been tried.**
- **Bloom on `l_partkey`** from the filtered part scan into the lineitem scan — directly analogous to Q14 / Q17's `l_partkey` lookup pattern. This is precisely the sidecar-Phase-1 target shape.

**Sidecar applicability.** **Highest of the 6 queries.** Q08 has a single, very-selective `p_type` filter that produces a 1-key `p_partkey` set that joins directly into `lineitem.l_partkey`. A sidecar sorted index on `l_partkey` turns lineitem from a 60M-row scan into a 1-of-N row-group lookup. This is exactly the 26-40× equality lookup case the ematix-parquet bench documents (cited in `docs/plans/CURRENT.md`).

### 1.5 Q17 — `lineitem × part` with scalar subquery `AVG(l_quantity)` per `p_partkey`

**Median:** ematix 182.69 ms / DuckDB 159.95 ms / **1.14× — was 1.69× pre-Σ.Q.L16**

**Where the cost is.** Q17 is the small-quantity-order-revenue query. The "expensive" part:

```sql
SELECT SUM(l_extendedprice) / 7
FROM   lineitem, part
WHERE  p_partkey = l_partkey
  AND  p_brand = 'Brand#23' AND p_container = 'MED BOX'
  AND  l_quantity < ( SELECT 0.2 * AVG(l_quantity)
                      FROM lineitem
                      WHERE l_partkey = p_partkey )
```

The correlated scalar subquery is decorrelated by DataFusion into a join against a per-partkey aggregate. Cost goes to:

1. **Two lineitem scans** — outer (filtered to surviving partkeys) + correlated subquery (full table grouped by `l_partkey`).
2. **AVG per partkey at ~2M cardinality** — table size 64 MB, blows L2 (per `project_sigma_r2_rejected.md`).
3. **Self-join** — Q17 has two lineitem references with a partkey relationship; cascading-bloom logic mis-attaches build-side filters to the wrong scan (`project_sigma_sb_cascade_neg.md` Q17/Q21 self-scan finding).

**What we've tried** (rejected, in memory):
- `project_sigma_r2_rejected.md` — RobinHoodAvgF64Exec replacement of the AVG-by-i64 group: +40-55% regression. DataFusion's split intern → batch-accumulate pipeline beats fused hash+accumulate at 2M cardinality.
- `project_sigma_q_l11_rejected.md` — u32 integer-key compression on Q17 ARROW_CAST: +1.7% regression (CAST per-row overhead beats hash-density gain).
- `project_sigma_q_l12_rejected.md` — SIMD-tagged hash agg (SwissTable-style): −19% on Q18-shape but +15% on Q17-shape (kept as infra for future shape-aware routing).
- `project_sigma_sb_cascade_neg.md` — Q17 regression +15 ms under cascade due to self-scan ambiguity.

**What hasn't been tried.**
- **Scalar-subquery decorrelation v2.** The current decorrelation produces an Inner Join on `l_partkey`. A v2 could (a) materialise the AVG-per-partkey only for the *partkeys surviving the brand/container filter* (single-digit thousands instead of 2M), or (b) push the AVG computation down inside a single combined lineitem scan with grouped accumulators and a hash table sized for the surviving partkey set. (b) is the harder rewrite but eliminates one full lineitem scan.
- **Σ.P SharedSubtreeExec extension** — both lineitem scans project overlapping columns. If the outer lineitem scan and the subquery's lineitem scan could share a single scan + dual projection, the second decode cost goes to zero. Σ.P already exists for subquery CSE (`project_sigma_p_subquery_cse.md`) but didn't fire on Q17.
- **Sidecar index on `l_partkey` for the outer scan** — small surviving partkey set from the part filter → row-group elimination on lineitem outer scan. Helps the outer scan but not the subquery scan (the subquery groups by all partkeys).

**Sidecar applicability.** **Medium.** Helps the outer scan substantially; the subquery scan still pays full decode. If Σ.P-extension fires alongside, the subquery scan can be folded into the outer scan and the sidecar's row-group elim benefits both. Standalone sidecar without Σ.P-extension: ~half the savings.

### 1.6 Q18 — `customer × orders × lineitem` with HAVING `SUM(l_quantity) > 300`

**Median:** ematix 246.64 ms / DuckDB 225.58 ms / **1.09× — was 2.5× pre-Σ.Q.L10**

**Where the cost is.** This is the **most documented** of the six. Per `project_q18_sf10_duckdb_plan_diff.md`:

> The dominant gap is **join order**, not aggregate kernel. DuckDB processes 12M rows at the final aggregate; we process 60M then LeftSemi-filter down to 624. The 60M intermediate alone is 1.6 TB of output_bytes — multi-second memory bandwidth cost.

The post-L10 1.09× residual is what's left after PushDownLeftSemiRule fixes the LeftSemi positioning. The residual likely splits between:

1. **Build-side wrong on LeftSemi** — the memory entry notes "our HashJoinExec has `build_input_rows=59.99M` — DataFusion built from the 60M side instead of the 624-key side." Σ.Q.L2 SwapSemiJoinBuildSideRule was shipped but marked NEUTRAL; needs re-verification.
2. **Aggregate kernel cost** at the inner `GROUP BY l_orderkey HAVING SUM > 300` (still 15M groups even after L10).
3. **Two lineitem scans** — outer + correlated subquery. Σ.P didn't fire here.

**What we've tried** (rejected and partially landed, in memory):
- `project_sigma_q_l10_landed.md` — **the big win**. PushDownLeftSemiRule Q18 SF=10 −54%. Closed the gap from 2.5× to 1.06× initially; it's drifted to 1.09× in the latest bench (within noise).
- `project_sigma_q_l1b` — RobinHoodSumF64 batch-ingest helped Q18 standalone (−4.4%) but regressed in 22q sweep (+7.8%) due to session-state contamination. Kept as opt-in.
- `project_l9_bloom_consumer_findings.md` — sideband bloom is structurally broken for Q18 (orders is on the build side of an inner join; eager poll can't wait for upstream bloom).
- `project_sigma_qm_slice4_spike_rejected.md` — referenced Q18 architectural framework; doesn't directly target it.

**What hasn't been tried.**
- **Build-side swap verification.** `swap_semi_join_build_rule.rs` exists and is marked NEUTRAL; nobody has re-benched it specifically against the post-L10 Q18 plan. If it correctly swaps after L10's pushdown, the 1.09× residual could close to parity.
- **Σ.P-extension to share both lineitem scans** — same as Q17.
- **AggregateExec kernel choice for `GROUP BY l_orderkey`** — 15M groups, integer key. The right kernel here is radix-partitioned + pre-sized; landed in concept via Σ.N.f.3 but only verified on l_suppkey / l_partkey shapes (10K-200K cardinality), not 15M.

**Sidecar applicability.** **Low.** Q18's filter is on `SUM(l_quantity) > 300` after aggregation — there's no static predicate to drive a sidecar lookup. The post-L10 LeftSemi pushdown is already the right structural answer; the remaining gap is hash-build sizing and the 15M-group aggregate kernel.

### 1.7 Cross-query pattern summary

| Query | Primary root cause | Lever family | Sidecar applicability |
|---|---|---|---|
| Q01 | Decode + aggregate kernel floor | PGO / dict-routing / no-op (accept) | **None** |
| Q05 | Dim-filter propagation (no join reorder) | Join reorder / bloom into scan | **High, conditional** (planner must derive l_suppkey set) |
| Q07 | Same as Q05 (smaller magnitude) | Filter-derived static bloom / sidecar | **Medium** |
| Q08 | Same as Q05/Q07 + very-selective part filter | Sidecar on `l_partkey` / static bloom | **Highest** (single-hop, 1-key→1-key) |
| Q17 | Two-scan lineitem (outer + correlated subq) + 2M-cardinality AVG | Σ.P-extension / sidecar on outer scan | **Medium** (helps outer, not subq alone) |
| Q18 | Hash build sizing + 15M-group agg (residual after L10) | Build-side swap / aggregate kernel | **Low** |

Three of the six (Q05, Q07, Q08) share **the same root cause**: dim-filter propagation that produces a small key-set which DuckDB pushes into the fact scan and we don't. Q17 has a partially-related root cause (the outer scan in Q17 is the same shape — a small surviving partkey set from filtered part). **Four of the six (Q05, Q07, Q08, Q17 outer scan) are sidecar-amenable in principle.**

Q01 is a pure kernel-floor. Q18 is a residual after the big L10 win and needs orthogonal levers (build swap + agg kernel).

---

## 2. Ranked lever menu

Scoring axes:

- **Blast radius** — does it risk codegen perturbation on the 16 queries currently faster than DuckDB? Range: 1 (separate crate, no risk) → 5 (new PhysicalOptimizerRule in flow-core, ~5-8% geomean tax).
- **Closure potential** — how much of the 1.04-1.31× gap can it plausibly close on the targeted queries? Quoted as "% of remaining gap closed" if every targeted query hits its theoretical max; "noise" if the gap is within bench σ.
- **Calendar cost** — engineering weeks of focused work. Excludes per-PR review, rebench, and infra changes.

Each lever clears the constraint per `project_optimizer_codegen_sensitivity.md`: lives in a separate crate, lives outside the optimizer rule chain, or comes with a quantified PGO offset.

### Lever L1 — Sidecar Phase 1 (read-side, already planned)

**What:** `EmatixFastParquetTableProvider` discovers and consumes pre-existing `.parquet.idx` sidecars when planner sees a matching predicate. Per `docs/plans/CURRENT.md` Story 1.1–1.4. Phase 1 only (no auto-build).

**Where it lives:** Pre-planning helper + provider-stamped index handle. **NOT** a `PhysicalOptimizerRule`. Per the doc's hard constraints: "No new `PhysicalOptimizerRule` — sidecar planner hook lives in a pre-planning helper (`dict_routing.rs` shape) and/or stamped onto the `EmatixFastParquetTableProvider` itself."

**Blast radius:** **1** (lowest). The hook lives outside the rule chain. Codegen footprint is in `ematix-parquet` (separate crate) for the index reader. The provider modification is in flow-core but does not perturb the optimizer module graph.

**Closure potential:**
- Q08: **~70-90%** of the 1.10× gap (high-confidence target — single-hop predicate, equality lookup on `l_partkey`, exact ematix-parquet bench scenario).
- Q17 outer scan: **~30-50%** of the 1.14× gap (helps half the cost; subquery still pays).
- Q05, Q07: **~10-30%** of their gaps (the predicates need to traverse 2-4 joins to land on `l_suppkey`; the sidecar can't help without a planner that derives the surviving key set, which Phase 1 doesn't include).
- Q01, Q18: **0%** (no selective predicate / post-aggregation predicate).

Aggregate: closes the easiest single loss (Q08) cleanly, makes a dent in Q17, mostly doesn't help Q05/Q07. **Estimated 22q geomean impact: 0.80 → 0.78** (Q08 closure: −1 pp; Q17 partial closure: −1 pp; Q05/Q07 unchanged).

**Calendar cost:** **2 weeks** per `CURRENT.md` Phase 1 estimate. ematix-parquet v0.16.0 is upstream-gated.

**Sequencing note:** This is the cheapest, lowest-blast-radius, highest-closure-per-week lever available. Ship it first.

### Lever L2 — Filter-derived static bloom into `EmatixFastParquetTableProvider`

**What:** When the logical plan contains a small dim filter that resolves to a small set of FK values (via plan-time analysis, *not* runtime), construct an in-memory bloom over that set and pass it as a *plan-time* context bloom into the fact scan's provider. The provider checks the bloom against row-group min/max + (with sidecar) page-Bloom.

Example: `region.r_name = 'ASIA'` → after pre-evaluating region (25 rows, trivial), derive `{r_regionkey: {2}}` → join via nation → derive `{n_regionkey IN {2}} → {n_nationkey: {N1, N2, N3, N4, N5}}` → join via supplier → derive `{s_nationkey IN {5 keys}} → {s_suppkey: ~50K values}` → push bloom over `s_suppkey` into lineitem scan on `l_suppkey`.

**Where it lives:** Pre-planning helper that runs *before* the physical optimizer, similar to Σ.K.2 dict-routing (`project_sigma_k2_dict_routing.md`). Walks the **logical** plan, materialises small dim chains eagerly (a "mini-pipeline" inside the planner), produces the bloom, attaches it to the provider's metadata. The provider already consumes context blooms via Σ.J.2.b infrastructure.

This composes with L1: with the sidecar present, the bloom drives row-group elimination via the sidecar's page-Bloom index. Without the sidecar, it drives row-group skipping via existing parquet statistics + a post-decode `BloomFilterExec` wrap.

**Blast radius:** **2**. Lives outside the rule chain, but it does add new code to flow-core. The Σ.K.2 precedent (`project_sigma_k2_dict_routing.md`: "outside optimizer to avoid codegen tax") confirms this pattern works.

**Closure potential:**
- Q05: **~50-70%** of the 1.31× gap (the canonical use case).
- Q07: **~50-70%** of the 1.13× gap (single-hop from nation to supplier).
- Q08: marginal additional gain on top of L1 (L1 already handles the part predicate).

Aggregate: **estimated 22q geomean impact: 0.78 → 0.74-0.75** (Q05 closure: −2 pp; Q07 closure: −1 pp).

**Calendar cost:** **3-4 weeks.** Pre-evaluating dim chains has correctness pitfalls (NULL handling, scalar-correlation detection, bloom false-positive cost vs decode-saved). Needs careful prototyping and a kill-switch.

**Sequencing note:** Second after L1. Has the largest plausible 22q-geomean impact of any single lever in this menu.

### Lever L3 — PGO release build, trained on the 22q SF=10 trace

**What:** Build the release binary with `-Cprofile-generate=...`, run the 22q SF=10 sweep against it once to produce a `.profdata` file, then re-build with `-Cprofile-use=.profdata`. Compare 22q SF=10 geomean against the non-PGO build.

This is the explicit remediation in `project_optimizer_codegen_sensitivity.md`:

> Future "add a shape" bites need a different mechanism. Options:
> - PGO (profile-guided opt) — would tell LLVM which paths are hot so it stops re-inlining decisions on add.

**Where it lives:** Build system (`Cargo.toml` profile + `release.toml` CI workflow). No source-code changes to flow-core. **Zero source-level blast radius.**

**Blast radius:** **1** (lowest, alongside L1). Build-system change. Per-query variance from PGO is generally a strict win for hot paths and a wash for cold paths; the codegen-perturbation theory predicts ~3-5 pp geomean gain across the board.

**Closure potential:**
- Q01: **~50-70%** of the 1.04× gap (codegen-bound).
- Q05, Q07, Q08: each ~10-20% (smaller share but free).
- Q17, Q18: similar ~10-20%.
- **Side benefit (not directly counted in closure but real)**: unblocks the next 3-4 rule-shaped levers in the pipeline that have been deferred for codegen-tax reasons. The Σ.Q.M / Σ.K.A / Σ.F-T2 rejections might re-bench positive after PGO.

Aggregate: **estimated 22q geomean impact: 0.74 → 0.70-0.72** (Q01 + uniform 1-2pp lift).

**Calendar cost:** **3-5 days**. The only risk is CI-build-time impact (PGO doubles build time) and reproducibility (the trace must be regenerated whenever the binary changes meaningfully — every 2-4 weeks of dev).

**Sequencing note:** Could run in parallel with L1 (different engineer, no source dependency). The **highest engineering ROI in the menu** if it works as predicted. Strongly recommend running an exploratory PGO build *before* committing to L4-L7, since L4-L7 cost estimates assume the codegen tax is still in effect.

### Lever L4 — Σ.P SharedSubtreeExec extension for two-scan queries (Q17, Q18)

**What:** Σ.P's `SharedSubtreeExec` (per `project_sigma_p_subquery_cse.md`) currently shares aggregate subtrees between duplicate consumers via Arc-shared `CachedBatches`. Extend it to detect when two `TableScan` nodes reference the same physical parquet file with overlapping projections, and fold them into a single underlying scan with branching projections.

Specifically:
- Q17 has two `lineitem` scans (outer + correlated subquery for `AVG(l_quantity) per p_partkey`).
- Q18 has two `lineitem` scans (outer + correlated subquery for `SUM(l_quantity) > 300 per l_orderkey`).

If a single scan produces a single decoded record batch which is then fed to both downstream paths, decode cost halves.

**Where it lives:** Extension of the existing `SharedSubtreeExec` operator + its inserter (`dedupe_aggregate_rule.rs` neighbourhood). The inserter is already a rule (counted under codegen-tax constraint), but per `project_optimizer_codegen_sensitivity.md` updated guidance ("for Σ.R and beyond — extend the existing operator, don't add a new rule") *and* per `project_sigma_p_subquery_cse.md` ("dedupe rule landed clean" at 11-trial measurement), extending the existing rule has been shown to be tax-free.

**Blast radius:** **2**. No new rule, but the existing rule's matcher gets a new arm. Per the Σ.P precedent: this pattern lands clean.

**Closure potential:**
- Q17: **~30-40%** of the 1.14× gap (one of two lineitem scans goes away).
- Q18: **~20-30%** of the 1.09× gap (similar).
- No effect on Q01, Q05, Q07, Q08.

Aggregate: **estimated 22q geomean impact: 0.72 → 0.71** (Q17 + Q18, narrow targets).

**Calendar cost:** **2-3 weeks.** The hard part is plan-equivalence checking (the two scans must produce identical decoded values; this is true iff projection + predicate set is identical, which Q17's two scans are NOT — outer has the brand/container predicate joined in, subquery has the partkey grouping). Likely needs a "shared-scan + per-consumer filter" mode.

**Sequencing note:** Hold until after L1/L2/L3. The closure potential is narrow and the implementation is subtle.

### Lever L5 — Scalar-subquery decorrelation v2 (Q17-specific)

**What:** Q17's `l_quantity < (SELECT 0.2 * AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)` decorrelates to an Inner Join on `l_partkey` against `AVG(l_quantity) GROUP BY l_partkey`. A v2 decorrelator could:

- Recognise that `p_partkey` is filtered upstream to a small set by `p_brand = 'Brand#23' AND p_container = 'MED BOX'`.
- Push the surviving partkey set as a *filter* into the AVG-by-partkey computation, so the subquery's aggregate sees only the surviving partkeys.
- This converts a 2M-group aggregate (the expensive one per `project_sigma_r2_rejected.md`) into a single-digit-thousand-group aggregate.

Note: this is logically different from L4 (L4 shares the scan; L5 reshapes the subquery's aggregate domain).

**Where it lives:** Logical-plan rewrite. This is **dangerously close** to a new `PhysicalOptimizerRule` — it's a `LogicalOptimizerRule` in DataFusion terms, which has not been specifically benched against the codegen-tax theory. Two possible homes:

1. As a one-shot transform inside the existing decorrelation pass (extending `decorrelate_predicate_subquery` rather than adding a new pass) — this avoids the new-rule tax per the Σ.P precedent.
2. As a pre-planning helper (L2-style) that detects the pattern in the *parsed* SQL or logical plan and emits the rewritten subtree directly.

**Blast radius:** **3**. Higher than L1-L4 because the rewrite logic is intricate (correlation-detection, NULL semantics, sub-aggregate determinism — see `dedupe_aggregate_rule.rs` for the float-determinism precedent).

**Closure potential:** Q17: **~50-70%** of the 1.14× gap. Cuts the dominant 2M-group AVG cost.

**Calendar cost:** **4-6 weeks.** This is decorrelator territory. High risk of correctness regressions; needs `tpch_validate` + a non-TPCH correctness suite (the project's auto-memory specifically calls out the "no TPC-H-specific hardcoding" rule).

**Sequencing note:** Only pursue if L1+L2+L3+L4 land but Q17 is still > 1.05×. Has the highest blast radius in the menu after L7.

### Lever L6 — Build-side swap re-verification for LeftSemi (Q18-specific)

**What:** `swap_semi_join_build_rule.rs` exists today. Per `project_q18_sf10_duckdb_plan_diff.md`: "our HashJoinExec has `build_input_rows=59.99M` — DataFusion built from the 60M side instead of the 624-key side. Σ.Q.L2 SwapSemiJoinBuildSideRule is shipped (marked NEUTRAL); needs re-check on whether it fires here." After L10's pushdown the LeftSemi is in a different position; the swap may now apply where it didn't before.

This isn't proposing a new rule; it's verifying that an existing rule fires correctly in a post-L10 world. If it doesn't, fix the matcher.

**Where it lives:** Existing rule. Per the optimizer-codegen-sensitivity counter-evidence (Σ.P dedupe landed clean): modifying an existing rule body has not been clearly shown to trigger the tax, only adding new rules. **However**, `project_optimizer_codegen_sensitivity.md` explicitly lists "Σ.H.1d.4 modified existing rule's `try_build_replacement` body" as a tax-incurring example. The evidence is mixed; assume tax-risk.

**Blast radius:** **3**. Existing rule, but tax-risk per the mixed evidence.

**Closure potential:** Q18: **~30-50%** of the residual 1.09× gap. If the swap was the missing piece, this approaches parity.

**Calendar cost:** **1 week.** Mostly: re-bench, examine the post-L10 plan dump, see whether the existing rule's match condition is too narrow, widen if so.

**Sequencing note:** Run alongside L1 (one engineer, independent of sidecar). Low cost, narrow upside.

### Lever L7 — Real join-order rewriter (cost-based, statistics-aware)

**What:** A pass that, for each `Inner Join`, examines the cardinality estimates of left and right subtrees (from logical-plan statistics) and picks the side with fewer rows as the build side. Generalised join-order rewrite. This is what DuckDB has and what the rejected Σ.Q.M arc tried to approximate via static plan rewrites.

**Where it lives:** DataFusion has a `JoinSelection` rule in its default optimizer pipeline that does some of this. The work is extending its statistics inputs (currently row-count estimates from parquet metadata, plus filter selectivity heuristics) and possibly its decision function.

**Blast radius:** **4**. New planner logic in the rule chain. *However*, the existing `JoinSelection` rule is in DataFusion itself, not flow-core — modifications in DataFusion-upstream would have a separate-crate insulation similar to ematix-parquet (`project_ematix_parquet_v013_win.md` precedent). Modifications wrapped as a *flow-core override* of `JoinSelection` would incur the in-tree tax.

**Closure potential:** Q05, Q07, Q08, Q18 all share the join-build-side gap. If done well: **~40-70%** of each remaining gap.

**Calendar cost:** **6-10 weeks** (upstream contribution path) or **4-6 weeks** (in-tree override path with higher blast radius). Cardinality estimation is famously fiddly.

**Sequencing note:** Highest blast radius, longest calendar cost. **Hold for after L1-L4 are landed and measured.** If post-L1-L4 the residual is still >0.85 geomean, this is the right structural lever. If post-L1-L4 the residual is ≤0.75, this becomes lower-ROI.

### 2.x Lever summary table

| Lever | Targets | Blast radius (1-5) | Closure potential (22q geo) | Calendar cost | Sidecar-Phase-2-needed |
|---|---|---:|---:|---:|:---:|
| **L1 sidecar Phase 1 (planned)** | Q08, Q17 outer, partial Q05/Q07 | **1** | ~-2 pp | 2 wk | N (Phase 1 is the lever) |
| **L2 filter-derived static bloom** | Q05, Q07, partial Q08 | 2 | ~-3 pp | 3-4 wk | N (composes with L1) |
| **L3 PGO release build** | all queries; Q01 most | **1** | ~-3 pp | 3-5 d | N |
| **L4 SharedSubtree extension** | Q17, Q18 | 2 | ~-1 pp | 2-3 wk | N |
| **L5 scalar-subq decorrelation v2** | Q17 | 3 | ~-0.5 pp (narrow) | 4-6 wk | N |
| **L6 LeftSemi build-side swap re-verify** | Q18 | 3 | ~-0.3 pp (narrow) | 1 wk | N |
| **L7 real join-order rewriter** | Q05/Q07/Q08/Q18 | 4 | ~-3-5 pp (if it works) | 6-10 wk | N |

Plausible cumulative impact (L1 + L2 + L3 + L4 + L6 done in order, no L5/L7): 22q SF=10 geomean **0.80 → 0.70-0.73** with 6 → 2-3 DuckDB wins.

---

## 3. Recommended sequencing

Given the codegen-tax constraint and the relative blast radii:

### Phase T1 — ship in parallel (weeks 1-2)

| Track | Lever | Engineer-weeks |
|---|---|---:|
| **A** | **L1 Sidecar Phase 1** (per `CURRENT.md`; Phase 1 only — discovery + planner hook, no auto-build) | 2 |
| **B** | **L3 PGO release build** (build system, CI; produces a measurable geomean datapoint) | 0.5-1 |
| **B'** | **L6 LeftSemi build-side swap re-verification** (1 engineer-week; runs alongside L3 since both can be benched on the same matrix) | 1 |

Track A is the user-already-committed sidecar plan. Track B is independent (build system) and provides a baseline data point: **how much of the 22q SF=10 gap is codegen-tax vs structural?** If PGO closes ~3 pp on its own, that's a strong signal that L4-L7 future levers will land cleaner than the optimizer-codegen-sensitivity memory entries imply.

**End of Phase T1, expected state**: Q08 at-or-near parity, Q01 at-or-near parity, Q18 near parity. Q05/Q07/Q17 reduced but still > 1.05×.

### Phase T2 — second-cohort levers (weeks 3-6)

| Lever | Sequence | Why |
|---|---|---|
| **L2 filter-derived static bloom** | First | Largest closure (Q05 / Q07) at moderate blast radius. Composes with L1. |
| **L4 SharedSubtree extension for Q17/Q18** | Parallel to L2, different engineer | Narrow but tractable. |

Phase T2 needs the Phase T1 PGO measurement to inform: if PGO closes >3 pp, the codegen-tax constraint may be relaxed; L2 can proceed with less defensive coding. If PGO closes <1 pp, the constraint remains and L2's pre-planning-helper home is mandatory.

**End of Phase T2, expected state**: 22q SF=10 geomean ~0.70-0.73. Five of six DuckDB-wins flipped to ematix-wins; only Q05 or Q17 remains DuckDB-faster.

### Phase T3 — structural longshots (weeks 7+)

L7 (join-order rewriter) and/or L5 (Q17 scalar-subq v2). **Only commit to either after Phase T2 results.** If post-T2 the residual is ≤0.75 with ≤1 DuckDB win, these become low-ROI ("optimising what's already good"). If post-T2 the residual is still ≥0.80 with multiple DuckDB wins, L7 is the right answer and L5 stays narrow.

### Why this order and not "ship sidecar later, do something else first"

The user's question explicitly asked: "is shipping sidecar Phase 1 first (already planned) the right next step, or does something else unblock Q01/Q05/Q07/Q08 better?"

**Sidecar Phase 1 is the right next step.** Reasoning:

1. **It is already planned and architecturally cleared** — `docs/plans/CURRENT.md` has 360 lines of open-question resolutions, OQ-CACHE, OQ-CATALOG, OQ-SEL-GATE all answered. Starting another large initiative in parallel without that level of prep is a slower path.
2. **It has the lowest blast radius** (1/5) of any non-build-only lever. The pattern is the same as Σ.K.2 dict-routing which landed clean.
3. **It is the only lever that ships Q08 to parity in 2 weeks** — Q08 is the cleanest closure target of the six and the most-favourable bench number.
4. **PGO (L3) can run alongside** at near-zero engineering cost. Bundling them gives the next bench a 2-lever combined measurement.
5. **L2 (filter-derived static bloom) is the larger lever for Q05/Q07/Q08** but requires careful design — pre-evaluating dim chains at plan time has correctness edges (NULL handling, subquery interactions). 3-4 weeks of design + implementation. Sequencing L1 before L2 means we have a known-good fact-scan filtering substrate (the sidecar's `with_filter` API + page-Bloom) for L2 to push blooms into. Without L1 first, L2 has nowhere to push the bloom to *except* a `BloomFilterExec` wrap, which doesn't reduce decode work.

The single-pivot risk: if Phase 1 sidecar bench-gate (Story 1.3 in `CURRENT.md`) fails — neutral or worse 22q geomean — Phase 2 sidecar should pause and the team should pivot to L3 (PGO) + L6 (build-side swap) + L2 to extract value from the existing 0.80 baseline before deciding whether sidecar's auto-build investment is justified.

---

## 4. Open architectural questions

These need investigation before committing engineering time to the corresponding lever. Each is framed as a diagnostic to run, not a decision to make now.

### OQ-1: Q05 cost decomposition — which join contributes most to the 44 ms gap?

The Q05 cost is one of:
- (a) Build-side hash cost in `customer ⋈ orders` and `orders ⋈ lineitem` (large unfiltered build inputs).
- (b) Probe-side hash cost when the lineitem scan can't push the supplier filter.
- (c) Post-join filtering when `customer.c_nationkey IN {5 nations}` evaluates late.

L2 (filter-derived static bloom) addresses (a) and (b) but not (c); L7 (join-order rewriter) addresses all three.

**Diagnostic:** Extend `crates/ematix-flow-core/examples/duckdb_q18_plan_dump.rs` to also dump our Q05 plan with `EMAT_DUMP_PLAN=physical` and `elapsed_compute` per operator (per `tpch_validate: extend EMAT_DUMP_PLAN to include physical plan`, commit 072b92d). Cross-reference against DuckDB's plan dump. The per-operator wall time on each HashJoinExec tells us where the 44 ms goes.

**Estimated diagnostic time:** 2-4 hours.

### OQ-2: Sidecar planner predicate-derivation — what shapes does the Phase 1 hook recognise?

`CURRENT.md` Story 1.2 (planner hook that picks the indexed read path) describes the hook but doesn't fully specify the predicate-derivation rules. The relevant question for §1.2's Q05 estimate:

> Does the Phase 1 hook recognise `region.r_name = 'ASIA' → JOIN → JOIN → JOIN → JOIN → l_suppkey BLOOM`, or only the single-hop case `p_partkey IN {1 key} → l_partkey BLOOM`?

If the latter (single-hop only), Q05 / Q07 / Q08's transitive filter chains don't fire the hook and the sidecar's plan-time benefit is limited to Q08's single-hop part-key chain. L2 then becomes a hard prerequisite for Q05/Q07.

**Diagnostic:** Spec a one-paragraph "predicate-derivation rules" subsection inside Story 1.2 before the story enters TDD. Cite single-hop vs multi-hop coverage.

**Estimated diagnostic time:** Already in scope of Story 1.2; just needs explicit answering.

### OQ-3: PGO-vs-flag-build empirical magnitude

The codegen-sensitivity memory (`project_optimizer_codegen_sensitivity.md`) predicts PGO will close ~3-5 pp because that's the historical regression magnitude per added rule. But the actual PGO win depends on workload symmetry between the training trace and the bench trace.

**Diagnostic:** Build the binary with PGO trained on the 22q SF=10 sweep, then run a different bench (the SF=1 sweep, or a synthetic skewed workload) to measure how much of the PGO gain is genuine codegen win vs trace-overfit. If it generalises across workloads, ship PGO default-on; if it only helps the trained workload, ship as `EMAT_PGO_RELEASE=1` opt-in.

**Estimated diagnostic time:** 1-2 days, mostly bench compute.

### OQ-4: Q17 outer scan vs subquery scan cost split

Q17's two lineitem scans have different effective cost: outer scan has the filtered-partkey set (sidecar-amenable), subquery scan groups by all partkeys (only partly addressable by L4/L5).

**Diagnostic:** Per-scan `elapsed_compute` from the physical plan dump. If outer scan dominates (>60% of Q17 wall time), L1 + sidecar closes most of the gap. If subquery scan dominates, L5 becomes the right lever and L1's benefit is limited.

**Estimated diagnostic time:** 1-2 hours (uses existing plan-dump infrastructure).

### OQ-5: Σ.P SharedSubtreeExec — why didn't it fire on Q17 / Q18?

Σ.P already dedupes aggregate subtrees (`project_sigma_p_subquery_cse.md`). Both Q17 and Q18 have two lineitem scans that should be deduplicable. The memory entry doesn't list Q17 / Q18 among Σ.P's wins — implying it didn't fire.

**Diagnostic:** Read `crates/ematix-flow-core/src/dedupe_aggregate_rule.rs` and trace its match condition against Q17 / Q18's logical plans. Likely outcome: it matches `Aggregate`-shaped duplicates but not `TableScan`-shaped ones — which is the extension L4 codifies.

**Estimated diagnostic time:** 2-4 hours.

### OQ-6: Q18 post-L10 build-side ground truth

`project_q18_sf10_duckdb_plan_diff.md` reported `build_input_rows=59.99M` *before* L10 landed. Post-L10, the LeftSemi has been pushed down — but the residual 1.09× suggests some inefficiency remains. The relevant question is whether L10 fixed the build-side issue too, or only the join-order issue.

**Diagnostic:** Re-run Q18 with the post-L10 binary (current `dc2d457`) and dump `build_input_rows` for each HashJoinExec. If still showing a 60M-row build at the LeftSemi level → L6 (build-side swap re-verify) is justified. If now showing 624-row build → the residual is purely aggregate-kernel-bound and L6 won't help; L4 (SharedSubtree on the two lineitem scans) becomes the right lever.

**Estimated diagnostic time:** 1-2 hours.

### OQ-7: Filter-derived bloom — false-positive cost

L2 (filter-derived static bloom) only wins if `bloom_probe_cost_per_row < decode_cost_per_skipped_row × selectivity`. The historical L9 bloom-on-FK was net-negative (`project_l9_bloom_consumer_findings.md`). The relevant variables:

- **Per-row probe cost** of a bloom built over (say) ~50K `s_suppkey` values: typically 10-30 ns/row with a well-tuned 8-bits-per-element filter.
- **Decode cost saved per skipped row group** for lineitem at SF=10: ~0.5-1.5 ms per skipped row group of ~1M rows.
- **Row-group-level false-positive rate** of the filter: depends on min/max overlap, typically high for non-clustered data.

If the lineitem data is clustered by `l_suppkey` (it isn't in standard TPC-H but might be in production workloads), L2 wins big. If not, it's marginal.

**Diagnostic:** Build a one-off `examples/bloom_overlap_probe.rs` that takes a parquet file + a candidate bloom-filterable column and reports: per-row-group min/max overlap fraction; predicted false-positive rate at 8 bits/element; predicted decode cost saved. If 0% row groups skip, bail before committing L2 to implementation.

**Estimated diagnostic time:** 4-6 hours.

---

## 5. What this doc deliberately does NOT propose

For clarity, the following levers were considered and explicitly **not** included in §2, with reasons:

1. **New optimizer rules in flow-core** — forbidden by the constraint per `project_optimizer_codegen_sensitivity.md`. Σ.Q.M Slice 2, Slice 4, and Σ.S.B cascade were all such rules; all rejected at sub-pp or sub-noise margins.
2. **`PhysicalOptimizerRule`-wrapped CBO** — covered partially by L7, deliberately framed as "DataFusion-upstream contribution or in-tree wrap" rather than "new rule in flow-core."
3. **Σ.R RobinHood AVG variants** — `project_sigma_r2_rejected.md` showed +40-55% regression on Q17. The lesson generalises ("21.6% self-time ≠ 21.6% reclaimable"). Don't re-litigate.
4. **u32-key compression / SIMD-tagged hashing** — `project_sigma_q_l11_rejected.md` and `project_sigma_q_l12_rejected.md`. Shape-blind; don't help the targeted queries.
5. **Hand-rolled Snappy / per-column parallel decode** — `project_hand_rolled_snappy_neg.md`, `project_per_column_parallel_decode.md`. Both lose at SF=10 in real queries.
6. **Pure aggregate-kernel replacement for Q01** — `project_sigma_nf3_beats_stock.md` already wins on the kernel; remaining Q01 gap is decode + codegen. PGO addresses the codegen half; sidecar doesn't apply.
7. **TPC-H-specific rules** — forbidden by `feedback_no_tpch_hardcoding.md`. Σ.Q.M Slice 4 SPIKE crossed this line (hardcoded orders→lineitem); rejected.

---

## 6. References

- `docs/plans/CURRENT.md` — sidecar Phase 1+2 active plan
- `docs/PHASE_SIGMA_Q_M_JOIN_REORDER.md` — rejected Σ.Q.M plan (background)
- `docs/SIGMA_Q_SINGLE_NODE_PARITY.md` — Σ.Q parity closeout
- `bench-results/release-2026-05-24/BENCHMARKS-sf10-noiseB.md` — target bench
- `crates/ematix-flow-core/src/push_down_left_semi_rule.rs` — Σ.Q.L10 (the post-L10 baseline)
- `crates/ematix-flow-core/src/swap_semi_join_build_rule.rs` — Σ.Q.L2 (L6 candidate)
- `crates/ematix-flow-core/src/runtime_bloom_sideband_rule.rs` — Σ.Q.L9 (composable with L1/L2)
- `crates/ematix-flow-core/src/context_bloom_rule.rs` — Σ.J.2.b.vi (template for L2's pre-planning helper)
- `crates/ematix-flow-core/src/dict_routing.rs` — Σ.K.2 (template for outside-optimizer pre-planning)
- `crates/ematix-flow-core/src/dedupe_aggregate_rule.rs` — Σ.P (L4 extension target)
- `crates/ematix-flow-core/src/synthetic_left_semi_rule.rs` — Σ.Q.M Slice 1 infra (still committed, opt-in via `EMAT_SYNTHETIC_LEFT_SEMI=1`)
- Memory `project_q18_sf10_duckdb_plan_diff.md` — Q18 root cause
- Memory `project_sigma_q_l10_landed.md` — Σ.Q.L10 result
- Memory `project_sigma_q_l13_to_l16_session.md` — 0.80 geomean baseline
- Memory `project_optimizer_codegen_sensitivity.md` — the forbidden-lever constraint
- Memory `project_sigma_r2_rejected.md` — Q17 RobinHoodAvg precedent
- Memory `project_sigma_qm_slice2_rejected.md`, `project_sigma_qm_slice4_spike_rejected.md` — Σ.Q.M arc rejections
- Memory `project_sigma_sb_cascade_neg.md` — Σ.S.B cascade neutrality
- Memory `project_sigma_k2_dict_routing.md` — outside-optimizer pre-planning precedent
- Memory `project_sigma_p_subquery_cse.md` — Σ.P precedent for L4
- Memory `feedback_full_bench_env_checklist.md` — required env vars for proper baseline
- Memory `feedback_no_tpch_hardcoding.md` — generalised-pattern constraint
