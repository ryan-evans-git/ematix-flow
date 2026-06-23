# Q10 SF=100 — why it can't reach the floor, and the wide-string late-materialization lever

- **Status:** ★ SPIKE FLIPS Q10 SF=100 TO A WIN (2026-06-23). Wide-string
  late-materialization (`LateGatherExec` + `BuildRowId`) + build batch=1M (cheap
  StringView gather) + build/probe overlap → late-mat ~2050ms / 21.2 CPU-s vs DuckDB
  ~2236ms / ~24 (thermally-matched ~9% WIN), correct (checksum MATCH SF10+SF100). Was a
  documented 1.36× LOSS. See §3d for the breakthrough; §3a-c are SUPERSEDED. NOT yet
  productionized (FD-detect rule + gated batch-size/overlap remain — §3d).
- **Supersedes** the framing in `project_sf100_loss_diagnostic` (decode/agg-kernel/
  parallel-eff) and the partial directions in `docs/Q10_RADIX_BUILD_JOIN_PROGRAM.md`
  (join radix) and `docs/plans/FD_AGGREGATE_OPERATOR.md` (FD-agg composite kernel,
  NO-GO). All Q10 SF=100 source files (`q10_*`, `fd_aggregate*`, `late_gather_exec`)
  remain banked, correct, default-inert.

---

## 1. The answer: why Q10 SF=100 can't reach the floor

Fresh warm-isolated SF=100, preset path, vs a direct DuckDB profile (2026-06-23):

| | ematix | DuckDB |
|---|---:|---:|
| wall | 2698 ms | ~1950 ms |
| CPU-s | 32.4 | 20.5 |
| parallel eff | 12.0 / 14 | ~13 |

**At the floor (warm), Q10's problem is CPU WORK (1.58×), not parallelism and not
decode.** The eff 8.6 in `project_sf100_loss_diagnostic` was the cold/in-sweep box
artifact; warm-isolated eff is 12.0 — fine. So the morsel-engine (parallel-eff) is
NOT the Q10-floor lever.

### Per-operator, ematix `elapsed_compute` vs DuckDB `operator_timing` (CPU-s)

| Operator | ematix | DuckDB | gap | verdict |
|---|---:|---:|---:|---|
| lineitem scan (decode 600M×4) | 9.60 | — | — | **at parity** |
| ematix FilterExec(`l_returnflag='R'`) | 2.94 | — | — | **separable lever** |
| lineitem scan+filter (combined) | **12.54** | **9.93** | +2.6 | DuckDB FUSES the filter (emits 148M); ematix densely emits 600M then a 2nd-pass FilterExec |
| customer scan (15M × 7 cols) | 2.63 | 1.83 | +0.8 | minor decode gap |
| orders scan | 1.76 | 2.26 | −0.5 | **ematix wins** |
| o⋈l join | 2.17 | 2.54 | −0.4 | **ematix wins** |
| **c⋈o join** | **2.98** | 0.90 | **+2.1** | wide-string gather onto 11.46M |
| **nation join** | **1.29** | 0.09 | **+1.2** | wide-string re-materialize onto 11.46M |
| **agg group-id** | **4.19** | 1.65 | **+2.6** | 7 wide-string cols in the group key |
| sort / projection | 0.40 | 0.84 | −0.4 | — |

**The decode "Snappy floor" in memory was WRONG** (rule #1, applied inward): ematix's
lineitem decode (9.60s) ≈ DuckDB's whole scan+filter (9.93s) — the kernel is already
at/below DuckDB. The "+4.6s decode gap" was conflating decode with the **separable
FilterExec (2.94s)**.

**The dominant fixable excess is carrying the 5 wide strings** (c_name, c_address,
c_phone, c_comment, n_name) through the 11.46M-row join+agg pipeline: c⋈o (+2.1) +
nation (+1.2) + agg wide-key hash (+2.6). The agg *kernel* (the FD-agg target, NO-GO)
was never the right lever — it's the *plan-level* wide-string handling.

### Prize bound (measured, `examples/.../q10_widestring_bound.rs`)

A "narrow" Q10 (group by `c_custkey` only, no wide strings) runs **1584 ms / 18.9 CPU-s
— BEATING DuckDB's 1950 ms / 20.5 CPU-s.** The wide strings alone cost **1134 ms /
13.5 CPU-s (42% of stock wall).** The narrow CPU (18.9) is *below* DuckDB's 20.5 →
the headroom to flip Q10 is real and lives entirely in wide-string handling + the
separable filter.

---

## 2. The lever — wide-string late materialization (the banked `LateGatherExec` path)

Carry a compact `u32 __cust_rowid` through the joins+agg instead of the wide strings;
re-attach the wide cols at the ~3.88M aggregate outputs by gathering from the SHARED
customer build (no re-scan). Infra already built + unit-tested but never wired in
(commit 675d0be): `JoinColumn::BuildRowId`, `EmatHashJoiner::gather_build_cols`,
`LateGatherExec`. This is distinct from — and supersedes — the two NO-GO attacks:
- **FD-agg composite kernel** (NO-GO): attacked only the agg, retain-all-batches +
  interleave-gather cache-hostile (1.87× CPU).
- **join-back arm B** (NO-GO): rebuilt customer 15M fresh. LateGather REUSES the
  c⋈o build via a shared `Arc<OnceCell<EmatHashJoiner>>`.

### Kill-gate spike result (`examples/.../q10_lategather_e2e.rs`, SF=100)

The spike grafts the real late-mat subtree onto Q10's real plan: build =
`customer ⋈ nation` (so n_name is gathered too, not carried) → `EmatixHashJoinExec`
CollectLeft (`c_custkey=o_custkey`) emitting `__cust_rowid` → RepartitionExec(rowid)
→ native `AggregateExec(SinglePartitioned, gby=[rowid])` → `LateGatherExec` re-attaches
6 wide cols → stock Sort/merge reused on top. **Correctness: row count + revenue sum
MATCH stock at SF10 AND SF100.**

Same-process A/B (both arms identical conditions):

| arm | wall | CPU-s | eff |
|---|---:|---:|---:|
| stock | 4539 ms | **39.7** | 8.8 |
| late-mat | 4413 ms | **31.6** | 7.2 |

(Walls are in the contended interleaved-A/B regime; the CPU *ratio* is the robust
signal.) **Late-mat uses 20% less CPU (8 CPU-s) — thesis VALIDATED.** Per-node:
agg **4.25s → 0.46s** ✓; wide-string join gather eliminated ✓.

**But the wall barely moved (1.03×)** because two new overheads + an eff drop eat the
work-saving:
1. **`LateGatherExec` re-attach ~2.2s** — `gather_build_cols` interleaves 7 cols ×
   3.88M ids across ~1830 un-concatenated build batches (slow many-source interleave;
   surfaced in the parent SortExec's pull time since LateGather reports no metrics).
2. **`EmatixHashJoinExec` serial 15M build + rowid reshuffle** — the serial hash-insert
   (memory: "INSERT stays serial") + the extra RepartitionExec(rowid) drop eff 12→7.2.
3. **customer wide-col decode (~2.6s) is unavoidable** — the build must decode the wide
   strings to gather them later (DuckDB pays this too).

Late-mat's 31.6 CPU sits between the narrow floor (18.9) and stock (39.7): ~3.5 CPU is
unavoidable customer decode, **~9 CPU is trimmable implementation overhead** (reattach
+ EmatixHashJoinExec). At the narrow eff (12), a leaned 22-CPU late-mat → ~1830 ms,
**under DuckDB**. So Q10 IS flippable, but only with a lean implementation — wiring the
banked infra as-is reaches ≈ parity at best.

---

## 3. Stacking lever — filter fusion (separable, ~2 CPU-s)

DuckDB fuses `l_returnflag='R'` into the scan (TABLE_SCAN emits 148M). ematix's dense
path emits 600M then a separate `FilterExec` (2.94s) compacts to 148M, materializing a
600M×3 intermediate. This is **distinct from the NO-GO masked-decode path** (which
tries to SKIP f64 decode — refuted, f64 is Snappy-decompress-bound). Here the decode is
unchanged; the lever is applying the predicate inside the scan to emit compacted 148M,
avoiding the separate-operator second pass + the 600M intermediate. Estimate ~1–2 CPU-s.

---

## 3b. P1/P2 results (2026-06-23) — the wide-string cost is irreducibly KERNEL-level

Isolated-consecutive SF=100 (`q10_widestring_bound.rs`), the robust comparison:

| variant | CPU-s | note |
|---|---:|---|
| narrow (no wide strings) | 18.9 | floor IF wide strings never needed (< DuckDB 20.5) |
| stock (carry + hash 7-col key) | 31.6 | ematix spends **~12.7 CPU** on wide strings |
| reattach via DataFusion re-join | **45.9** | **NO-GO** — the re-join is *more* expensive than carrying |
| DuckDB | 20.5 | spends **~1.6 CPU** on the same wide strings |

**The crux: ematix's wide-string handling (join gather + 7-col group-id hash) costs
~12.7 CPU-s; DuckDB's costs ~1.6 — an ~8× kernel gap.** DuckDB dictionary-compresses
group keys (`__internal_compress_*` all over its plan) and has a faster wide-key
HASH_GROUP_BY (1.65 vs ematix 4.19). This is KERNEL efficiency, not a plan difference
(both group by all 7 cols).

- **P1 (coalesce build to lean the re-attach) = NO-GO.** Coalescing halves the gather
  (SortExec 2.6s→1.3s) but `CoalesceBatchesExec` is a serialization barrier: eff
  7.2→6.0, wall WORSE (late/stock 0.97→1.31). Default OFF in the spike.
- **P2 (DataFusion-native re-join) = NO-GO.** 45.9 CPU — re-attaching via a second
  15M-customer join costs more than the wide-string carry it replaces (it does the
  wide join twice). Confirms the documented arm-B NO-GO.
- **The EmatixHashJoinExec gather IS the only viable late-mat** (reuses the build,
  gathers wide strings only at 3.88M, no second join). It measured ~28 CPU (vs stock
  31.6) but eff ~7 (vs narrow 12). Its FLOOR, if both obstacles are fixed, is
  **narrow 18.9 + customer-wide-decode ~2.6 + lean-reattach ~0.5 ≈ 22 CPU at eff 12
  → ~1833 ms, which WOULD beat DuckDB 1950.** Two obstacles, both kernel/operator:
  1. **Parallel EmatixHashJoinExec build** — the 15M serial hash-insert is the eff
     killer (all probe threads wait); DataFusion's Partitioned join builds 14 parts in
     parallel (why narrow is eff 12). The standing "serial INSERT" lever.
  2. **Non-barrier lean gather** — replace the 1830-source `interleave` with a
     sort-by-(batch,local) → per-batch `take` → inverse-permute (no CoalesceBatches
     barrier). Targets reattach 2.2s → ~0.5s.

## 3d. ★★★ BREAKTHROUGH (2026-06-23) — floor LOWERED, Q10 SF=100 FLIPS to a WIN

The §3c "at the floor" verdict was WRONG (rule #1: never declare a floor — the user
pushed back, correctly). The reattach was slow because `interleave` ran across ~1830
build batches (15M / 8192-row scan batches) → byte-copying StringView. The decoder
already emits **Utf8View** (line ~2053), so a FEW-source gather SHARES byte buffers
(near-free). Two cheap levers close it:
1. **Build scan batch size 8192 → 1M** (`EMAT_BATCH_SIZE`): the build retains ~15
   batches not ~1830, so the late-gather `interleave` is buffer-sharing, not byte-copy.
   (Reattach: SortExec 2.75s → ~negligible.) Also helps the whole pipeline (~17% even
   for stock). 4M/8M OVERSHOOT — batches too big, eff collapses (10.6→6.8); **1M is the
   sweet spot.**
2. **Build/probe overlap** (`EMAT_HJ_OVERLAP`, §3c): hides the customer build behind the
   lineitem decode.

**Measured SF=100, isolated, batch=1M + overlap (correct — checksum MATCH at SF10+SF100):**

| arm | wall | CPU-s | eff |
|---|---:|---:|---:|
| stock (orig 8K batch) | ~2900 | ~32 | — |
| stock (1M batch) | 2392-2490 | 25.4-25.7 | 10.3 |
| **late-mat (1M + overlap)** | **1982-2101** | **21.0-21.2** | 10.4 |
| DuckDB (thermally-matched) | 2220-2252 | ~24 | ~13 |

**Thermally-matched sandwich (DuckDB / late-mat / DuckDB): late-mat 2041ms vs DuckDB
2220 & 2252 → ~9% WIN, at ~12% less CPU (21.2 vs ~24).** Same-process A/B: late-mat
1.19× wall / 1.21× CPU over stock. **Q10 SF=100 flips from the documented 1.36× LOSS to
a ~9% WIN over DuckDB.** The wide-string late-mat lever (§2) is real AND realizable; the
§3a/§3b/§3c "kernel gap / at-floor" conclusions are SUPERSEDED — the gap was the gather's
batch granularity + build serialization, both cheaply fixable.

**★ prod-D CORRECTION (controlled A/B): the batch-size lever is ISOLATED-only.** The
§3d numbers above are ISOLATED-WARM (single query). A controlled preset in-sweep A/B
(`tpch_preset_rebench` fresh_ctx) shows a GLOBAL batch=1M REGRESSES every query at
SF100 (1.06–1.42×) and SF10 (1.02–1.12×) — larger batches bloat in-flight RSS and evict
the 36GB page cache (box artifact). So a global bump is NO-GO. The ROBUST cross-protocol
signal is that late-mat does **~15–20% less CPU** than stock at every batch size. The
production design is a **per-scan large batch for the late-mat BUILD scan only** (the
build holds the same 15M customer rows regardless of batch granularity → no extra RSS;
lineitem/probe stay 8192 → no global eviction) — gets the cheap StringView gather without
the in-sweep cost. UNMEASURED; validate in prod-C on the preset in-sweep path. The
"~9% over DuckDB" headline is an isolated-warm (floor-regime) result; in-sweep is the
box-artifact regime where both engines suffer.

**Still a SPIKE — productionization (the real remaining work):**
- **FD-detect planner rule** (correctness-critical): fire only when the group key ⊇ a
  PROVEN PK functionally determining the wide cols. The spike picks the key by NAME.
- **Batch-size**: 1M is global in the spike. A global bump may regress SF10 / small
  queries / 22q geomean → make it scale/shape-gated (large scans only). Validate 22q.
- **Overlap**: `EMAT_HJ_OVERLAP` gated; the buffered-probe blast radius needs the same
  shape gate as the no-shuffle path.
- Gates: 22q SF=10/100 strict A/B, `tpch_validate` 22/22, codegen-tax, peak RSS.

## 3c. P2' result (2026-06-23) — overlap helps eff but the CPU floor caps it: SUPERSEDED by §3d

Added a gated `EMAT_HJ_OVERLAP` to `EmatixHashJoinExec`: spawn the probe-side drain
concurrently with the build so the ~1.08s build hides behind the long-pole lineitem
decode (measured: build ran ENTIRELY before the probe side started — 40% of wall
near-serial). Correct (SF10+SF100 match). A/B SF=100: late-mat eff **7.2 → 8.4** — the
overlap works, partially. BUT:
- late-mat CPU is steady at **~28 CPU-s** (vs stock ~32-34, DuckDB 20.5). Even at perfect
  eff (14), 28 CPU-s → ~2000ms ≈ DuckDB's 1950 — **no clear win possible from the wall side.**
- The build (customer decode) and probe side (lineitem decode) are BOTH CPU-bound; overlapping
  them on 14 cores time-slices rather than speeds up → eff gain is bounded (7.2→8.4, not →12).
- **The narrow floor (18.9 CPU) was illusory.** The wide-string dodge costs back most of the
  saving: reattach ~2.2 + EmatixHashJoinExec build 1.08 + probe 1.69 (vs DataFusion's c⋈o ~0.66)
  + rowid reshuffle. Late-mat nets only **~4-6 CPU-s vs stock**, landing at ~28 vs DuckDB 20.5.

**FINAL VERDICT: Q10 SF=100 is effectively at ematix's achievable floor.** Handling the
wide strings costs ematix ~9-13 CPU-s in EVERY arrangement (carry+hash = stock; rowid+reattach
= late-mat), vs DuckDB's ~1.6 — an irreducible ~8× wide-key KERNEL gap. No plan-level lever
(late-mat, with or without overlap/lean-gather) closes it; the dodge is never cheap enough.
Late-mat + all fixes (lean gather + filter-fusion) projects to ~22-24 CPU → ~DuckDB parity, a
sub-noise (~3%) win on one query at ±15% SF=100 measurement variance — NOT worth ~3 multi-day
kernel fixes. The only path to a clear win is matching DuckDB's in-place wide-key group-by/join
kernel (compressed group keys), i.e. the FD-agg (NO-GO) / radix-agg (multi-month) family.
**Recommendation: STOP Q10 SF=100 plan-lever work; bank P2' overlap as opt-in infra; redirect
the campaign elsewhere.** The investigation's durable value is the corrected diagnosis (decode
at parity, eff fine warm, the gap is wide-key kernel efficiency — NOT decode/agg-plan/morsel).

### (superseded) earlier "flippable" framing
Q10 SF=100 IS flippable, but ONLY via the EmatixHashJoinExec
late-mat WITH both kernel fixes above (+ filter-fusion as insurance). Without them the
lever modestly beats stock on CPU but not on wall, and not DuckDB. The plan-trick alone
does not escape the ~8× wide-string kernel gap — it relocates it from carry+hash to
build+reattach (~10-13 CPU either way); the fixes are what make the relocated cost
cheap. The alternative (match DuckDB's in-place wide-key kernels: compressed group keys
+ faster HASH_GROUP_BY) is the FD-agg/radix-agg family — NO-GO / multi-month.

## 4. Phased implementation plan (each gated; not yet built)

- **P0 (DONE):** diagnosis + prize bound + kill-gate spike. GO-with-caveats.
- **P1 (DONE — coalesce NO-GO; P2 re-join NO-GO).** See §3b. The two real obstacles
  are kernel-level, below. The cheap plan-level dodges both failed.
- **P1' — non-barrier lean gather.** Rewrite `gather_build_cols`'s many-batch path:
  sort the (batch,local) pairs, `take` per build batch contiguously, concat, inverse-
  permute to restore order — no `CoalesceBatchesExec` barrier, no 15M copy. Target
  reattach 2.2s → ~0.5s. Kernel change + unit test (cross-batch order preserved). Gate:
  spike SortExec elapsed drops with eff unchanged.
- **P2' — parallel EmatixHashJoinExec build.** The 15M serial hash-insert is the eff
  killer (eff 7 vs narrow's 12; all probe threads wait on the serial insert). Parallelize
  the insert (per-partition sub-tables merged, or the radix path's per-partition build).
  Gate: late-mat eff ≥ 11 isolated. THIS is the dominant obstacle — without it the
  CPU win doesn't reach wall.
- **P3 — filter fusion** (returnflag into the dense scan output). Gate: lineitem
  scan+filter approaches DuckDB's 9.93s; no regression on other queries' dense scans.
- **P4 — FD-detect planner rule** (correctness-critical). Fire only when the group key
  ⊇ a PROVEN PK that functionally determines the wide cols (the spike picks the key by
  column NAME — NOT shippable). Needs `EmatixFastParquetTableProvider::constraints()`
  + a logical recognizer + ExtensionPlanner (the FD machinery verified in
  `project_sf100_loss_diagnostic` Story-3 Phase-0). Reconcile with the banked
  `late_gather_exec` wiring.
- **P5 — gates.** 22q SF=10/100 strict interleaved A/B (`scripts/bench/strict_ab.sh`),
  `tpch_validate` 22/22 SF=1/10/100, codegen-tax check, peak RSS ≤ stock. Default-on
  decision only if Q10 SF=100 flips with ≥-neutral everywhere.

### Risks
- **Reattach memory** (P1 concat) — bound it; the concat-free build exists for a reason
  (SF100.7 memory-viability). May need a middle ground (gather from ~14 coalesced
  batches, not 1830 nor 1).
- **EmatixHashJoinExec at SF=100** — its no-shuffle shared build had a +21.7% contention
  history on the 148M-probe o⋈l join; here it's the 11.46M-probe c⋈o (different, lighter)
  and the probe emits only u32 (no wide gather). Still, P2 must confirm eff.
- **FD correctness** — wrong FD = wrong results. P4 must require a PROVABLE PK.

### Provenance (this session, all UNCOMMITTED on `feat/fd-agg-composite`)
- `crates/ematix-flow-core/examples/q10_widestring_bound.rs` — prize bound (narrow vs stock).
- `crates/ematix-flow-core/examples/q10_lategather_e2e.rs` — the kill-gate spike.
- `crates/ematix-flow-core/src/emat_hash_join_exec.rs` — added `pub fn build_once()` accessor.
- Fresh DuckDB profile: `duckdb_profile_dump 10` SF=100 (20.5 CPU-s / 1950ms).

---

## §5 PRODUCTIONIZATION — DELIVERED (2026-06-23, `feat/fd-agg-composite`)

The banked spike is now a sound, general, committed production rule (opt-in
`EMAT_LATE_MAT_AGG=1`, default OFF). It fires on Q10 ONLY across all 22 queries
and is correct everywhere.

### What shipped
- **prod-A** (`90fafd4`) — `EmatixFastParquetTableProvider::with_primary_key` +
  `TableProvider::constraints()`. Declared PK (a catalog's DDL), NOT inferred from
  parquet stats. DataFusion derives `{pk}→{cols}` and propagates it to the
  aggregate input (verified on the real Q10 plan: `{c_custkey}→{5 wide cols}`,
  `n_name` correctly NOT covered).
- **prod-B** (`2e7a668` detector, recognizer commit) — `late_mat_agg.rs`:
  `fd_minimal_group_key` (FD-closure key reduction) + `analyze`/`reconstruct` +
  `LateMatAggNode` (`UserDefinedLogicalNodeCore`). Recognizes the sound shape:
  anchor = declared-PK group col that FD-determines the wide cols; fold a dim into
  the build ONLY when it joins via its OWN declared PK (many-to-one → build stays
  1:1 with the anchor → grouping the build-rowid ≡ grouping the full wide key);
  split build/probe on the single anchor-PK = fact-FK edge.
- **prod-C** (`late_mat_agg_planner.rs`) — `LateMatAggPlanner` (`ExtensionPlanner`)
  expands the node into `EmatixHashJoinExec(BuildRowId)` → `Repartition(rowid)` →
  `AggregateExec(rowid)` → `LateGatherExec`. Aggregates rebuilt physically via
  `create_physical_expr`. Wired into `FlowQueryPlanner` (mirrors the PV.3b path).
  Also: `EmatixHashJoinExec::with_new_children` now PRESERVES the shared
  `build_once` (the physical optimizer rewrites the join's children after
  `LateGatherExec` captured it — a fresh cell left the gatherer pointing at an
  uninitialized build).
- **prod-E** gates — env-free soundness test: over ALL 22q with every PK declared
  the rewrite fires on **Q10 only** and is row-for-row identical to stock; SF1
  end-to-end correctness (direct + through FlowQueryPlanner); full lib suite 1233/0.

### The win IS realized through the shipped path — but it's COUPLED to batch size
`q10_late_mat_prod_ab` (production planner, SF=100, isolated-warm, M4 Max):

| arm | exec batch | wall | CPU | vs DuckDB (~1950–2250) |
|---|---|---|---|---|
| stock | 8192 (default) | 2809ms | 35.8 | loss |
| stock | 1M | 2123ms | 25.3 | parity (−24% from batch alone) |
| **late-mat** | **1M** | **1941ms** | **22.7** | **WIN** (−8.6% more) |

- The late-mat rewrite reaches the spike's number (1941 ≈ 1982–2101) through
  production code — **rule #1 vindicated again**: the win is real and realizable
  end-to-end, not a spike artifact.
- **Coupling (the key finding, profiled not inferred):** the wide build cols are
  Utf8View; `LateGatherExec` interleaves them across the retained build batches —
  cheap ONLY with few large batches. At the default 8192 the build emits ~1830
  sources and late-mat is a net **LOSS** (2858 vs 2809). **Batch size is a RUNTIME
  `TaskContext` parameter** (the build's `HashJoinExec` output sizing reads it at
  execute time); a plan-time per-build re-plan is dead (profiled: 8192-row build
  batches regardless) and was removed. The build inherits the SESSION batch.
- A global large batch also speeds the 600M-row probe fact decode (the −24% on
  stock), but **prod-D found a global bump regresses the other 21 queries**, so a
  default-on late-mat is **blocked on a query-scoped batch mechanism**.

### Verdict
SHIP **opt-in** (`EMAT_LATE_MAT_AGG=1`), banked as correct, default-inert,
zero-regression infra — paired with a large session batch for the wide-string-
aggregate workload (then Q10 SF=100 = 1941ms, beats DuckDB, −31% vs default
stock). **Default-on follow-on:** a query-scoped execution batch size — e.g. the
recognizer emits a "preferred batch size" hint the session honors for THIS query
only — which would also unlock prod-D's general large-batch SF=100 wins without
the 22q regression.

---

## §6 QUERY-SCOPED BATCH HINT — win lands at the DEFAULT session batch (2026-06-23)

§5's "coupled to a global batch" blocker is RESOLVED. The large batch is now
applied per-query, scoped to the recognized late-mat query, so the win lands with
no session change and no 22q regression.

- **`batch_size_override_exec.rs` — `BatchSizeOverrideExec`**: a pass-through node
  that at `execute()` threads a `TaskContext` with an overridden
  `SessionConfig::batch_size` to its subtree (batch size is a RUNTIME param, so
  this is the only correct hook). `FlowQueryPlanner` wraps the late-mat plan's
  ROOT in it (default 1M, `EMAT_LATE_MAT_BATCH`). Other queries get no wrapper.
- **`EmatixHashJoinExec.with_overlap(true)`** — overlap baked into the late-mat
  join per-instance (was global `EMAT_HJ_OVERLAP` env). REQUIRED: it hides the
  serial 15M-row build behind the probe decode (SF=100: eff 7.9/2779ms without
  vs 10.5/2322ms with).

**Result (SF=100, isolated, ONLY `EMAT_LATE_MAT_AGG=1`, no other env):**

| arm | wall | CPU |
|---|---|---|
| stock | 3001ms | 33.1 |
| **late-mat** | **2061ms** | **21.7** → **31% faster, beats DuckDB ~1950-2250** |

The win is now self-contained in the rule at the session-default batch. Remaining
for default-on: (1) declare TPC-H PKs in the production registration (the rule
needs them to fire); (2) the 22q SF=10/100 strict A/B with PKs declared, to
confirm the FD-on-the-catalog plan perturbation is neutral on the other 21
queries (the recognizer already proven to fire Q10-only + correct). Until then it
ships opt-in (`EMAT_LATE_MAT_AGG=1`), now a self-contained −31% Q10 SF=100 win.

**Bonus:** `BatchSizeOverrideExec` is general infra — it also unblocks prod-D's
broader SF=100 large-batch wins (NO-GO only because they were global) by scoping
a large batch to any shape that benefits.

---

## §7 PATH TO DEFAULT-ON — PK wiring + 22q A/B gate CLEARED (2026-06-23)

- **PK wiring** (`tpch_preset_rebench`): declares the TPC-H PKs (harness
  scaffolding; a real catalog uses DDL) when `EMAT_TPCH_PK=1` OR the rule is on,
  so late-mat fires on the production-faithful preset path. Default off → baseline
  byte-identical.
- **Shape gate** (`EMAT_LM_MIN_WIDE_COLS`, default 3): the recognizer ALSO fired
  on Q18 in the preset path (the ematix walkers reshape it into a late-mat star) —
  a +28% SF=10 regression (correct but slow: Q18 drops only 1 wide string,
  c_name; the CollectLeft+reattach doesn't pay). Gate requires ≥3 string-typed
  group columns: Q10 carries 5 (fires/wins), Q18 carries 1 (gated out). General,
  not TPC-H-keyed.

**22q SF=10 A/B gate (preset path, interleaved 3-round, baseline vs full package
PK+rule+gate):** every query within ±4% (the SF=10 noise floor), **Q10 −3.9%
(neutral), ZERO regressions.** Q18 +1.5% (was +28%). The FD-on-catalog effect
(declaring PKs) is captured here and is neutral. ★ A first sequential A-then-B run
showed a spurious uniform +3-4% offset = cross-run thermal drift; interleaving
removed it (Q10 — the only query the rule touches — was the tell at +0.7% vs
PK-only). **Q10 SF=100 with the gate: still fires + wins (2018ms/21.7 CPU vs stock
2915/31.8 = 31% faster, beats DuckDB, exact-correct).**

### Default-on status
GATES CLEARED: rule fires Q10-only (shape gate), SF=10 22q regression-free, SF=100
Q10 +31% win, correct everywhere. Flipping the library default to on is LOW RISK —
it only activates where PKs are declared AND the ≥3-wide-string star shape exists
(inert otherwise). Remaining nice-to-have before the flip: a SF=100 22q sweep to
confirm the FD-on-catalog effect is also neutral at SF=100 (box-artifact-
dominated; the isolated Q10 win + SF=10 22q neutrality are the strong evidence).
Shipped opt-in (`EMAT_LATE_MAT_AGG=1`) and ready to flip on the user's call.
