# ematix-flow — adaptive & intelligent levers inventory

A catalog of the mechanisms that make ematix-flow more than "DataFusion +
some operators." Organized by intent. Each entry has a one-line hook +
where it lives + what it actually does + measurable evidence.

Aimed at: marketing/dev-site narrative, technical due-diligence,
onboarding a new contributor who needs to understand the surface area.

---

## 1. Adaptive runtime — the engine learns from each query

The Σ.L family. Built on the premise that a query engine should improve
over time as it sees the same workloads, not stay frozen at "best
guess from cost stats."

### Σ.L.1 — Speculative race for ambiguous routing
**Where**: `crates/ematix-flow-core/src/dict_routing.rs` (DictArrivalVerdict probe).
**What**: When the planner can't tell whether dict-preservation will help,
launches both paths concurrently and uses whichever returns first. The
winner trains the next decision via the workload log.
**Why it matters**: No more "wrong guess locks you in for the whole
query" — the engine hedges its plan choice when stats are weak.

### Σ.L.2 — Workload feedback log
**Where**: `~/.ematix/workload.db` (SQLite).
**What**: Every query's per-operator timings, predicate pass rates,
and chosen routing decisions are durably logged across sessions. The
next session reads this log and biases its routing toward what
*actually* won for this workload last time.
**Why it matters**: A 22-query TPC-H bench gets faster the more times
you run it — not from caching results, but from learning which kernel
to pick for each query's shape.

### Σ.L.3 — Per-batch predicate reorder across row groups
**Where**: `crates/ematix-flow-core/src/scan_cache.rs` (AdaptiveFilterOrder).
**What**: Multi-predicate scans reorder filter evaluation per row-group
based on observed early-row pass rates. The cheapest-and-most-selective
predicate runs first; later row groups inherit the order that's
working so far.
**Why it matters**: Static "stats-based" filter ordering picks one
order for the whole query. Real data has skew. L.3 adapts.

### Σ.L.4 — Cross-query work sharing for concurrent scans
**Where**: `crates/ematix-flow-core/src/scan_cache.rs`.
**What**: When two queries are scanning the same parquet file at the
same time (e.g. a dashboard refresh + an ad-hoc query), they share a
single decode stream instead of double-paying.
**Why it matters**: Real multi-tenant workloads have huge overlap;
sharing makes throughput-per-core scale with users, not query count.

### Σ.L.5 — Workload-aware parquet write tuning
**Where**: `crates/ematix-flow-core/src/write_tuner.rs`.
**What**: The writer observes which columns the read-path queries
most filter / project, and on the next write biases dictionary
encoding, row-group size, and column ordering toward those columns.
**Why it matters**: Workload-tuned parquet files read 2-5× faster
than generically-tuned ones. ematix learns the right shape.

---

## 2. Mid-query AQE — capturing data as a side-effect of execution

Adaptive query execution at the operator level — not "between queries"
(Σ.L family) but **during a single query**.

### Σ.Q.L9 — HashJoin → probe-scan runtime sideband ⭐
**Where**: `bridge_filter_sideband.rs`, `build_side_bloom_emitter_exec.rs`,
`runtime_bloom_sideband_rule.rs`.
**What**: When DataFusion's `HashJoinExec` does its regular build
phase (which it has to do anyway — there's no avoiding it), a
pass-through wrapper observes the build-side key column flowing
through, accumulates a per-partition `BloomFilter`, union-merges
them on completion, and publishes the bloom to a `BridgeFilterSideband`
shared with the probe scan. The probe-side `EmatixFastParquetExec`
peeks the sideband at `execute()` time (which happens AFTER the
build is fully consumed, per `HashJoinExec`'s semantics) and merges
the bloom as an `I64InBloom` `BridgeFilter` predicate **before**
masked-decode begins.
**Why it matters**: This is the first "build-side data feeds back
into probe-side scan" lever in ematix. Probe-side rows whose join
key isn't in the build set skip decode entirely. **Q07 SF=10 −4.7%,
Q21 SF=10 −5.9%**. The same sideband channel is reusable for any
future AQE (adaptive skew detection, late-arrival selectivity,
dynamic partition rebalancing).

### Σ.Q05.CHAIN — L9 cascade chains (filtered dim → … → fact)
**Where**: `runtime_bloom_cascade_chain.rs` (second phase of the L9
rule); scan-side extras in `ematix_fast_parquet.rs`
(`with_extra_runtime_sideband`).
**What**: Detects CollectLeft build CHAINS whose top build is
statically filtered and whose probe chain terminates in a large fact
scan (Q05: region(ASIA) → nation → supplier(2-key) → lineitem), then
installs one bloom per link. Runtime poll order sequences the links
for free: each link's build scan is polled only after the parent
link's bloom published, so the emitters sample already-narrowed
builds (supplier 100K → 20K at SF=10). Intermediate links carry an
`apply_when_dense` marker so the reader applies their bitmaps even
above the REV.23 masked→dense discard threshold (dim-sized scans —
negligible cost, and the next link's build sample depends on the
prune). Multi-key equi-joins may emit a single-key superset bloom
(`EMAT_MULTIKEY_BLOOM`). The terminal bloom composes with an existing
wrap via an EXTRA sideband (a scan's primary slot is never displaced).
**Gating**: tri-state `EMAT_L9_CASCADE` (+ `EMAT_MULTIKEY_BLOOM`),
conservative AUTO: chain start filtered, every build CollectLeft ≤ 4M
rows, terminal ≥ 20M rows, ≥ 2 links, bare (non-composed) terminals
only under `EMAT_L9_CASCADE_TERMINAL_APPLY`.

---

## 3. Decode caches — never pay for the same column twice

### Σ.O.c (RowGroupDecodeCache) / Σ.Q.L6′ — per-column LRU cache
**Where**: `crates/ematix-flow-core/src/emat_arrow_reader.rs`.
**What**: Process-scoped cache of decoded parquet columns, keyed by
`(file_path, row_group_idx, leaf_column_idx)` with a byte-bounded
LRU eviction policy (`VecDeque` for O(1) `pop_front`). The L6′ lift
went from per-projection keys (decoded `Vec<DecodedColumn>` per
projection) to per-leaf keys, so multi-scan queries that overlap on
some-but-not-all columns get cache hits.
**Why it matters**: TPC-H Q17 has two `lineitem` scans sharing
`l_partkey, l_quantity`; Q18, Q08, Q09, Q21 have similar overlap.
Opt-in via `EMAT_RG_DECODE_CACHE=1`. **SF=10 wins**: Q21 −14.4%,
Q18 −10.9%, Q17 −6.5%, Q09 −5.1%. SF=1 untouched (default OFF
preserves the small-query no-overhead posture).

### Σ.M — plan cache with semantic dedup
**Where**: `crates/ematix-flow-core/src/plan_cache.rs`.
**What**: Parsed + optimized `LogicalPlan`s cached by SQL hash +
schema. Re-running the same query within a session skips parse and
planning entirely.
**Why it matters**: Dashboard-style workloads (same N queries
hammered repeatedly) cut their per-query parse/plan overhead from
1-3 ms to ~50 μs.

### Σ.P — SharedSubtreeExec (CSE registry)
**Where**: `crates/ematix-flow-core/src/shared_subtree_exec.rs` +
session-scoped registry.
**What**: When the same subtree appears multiple times in a plan
(common-subexpression pattern: SELECT ... WHERE col IN (subquery) ...
where the subquery output is referenced twice), only the first
consumer executes it; the second replays from a session-scoped
`Arc<CachedBatches>`.
**Why it matters**: Q15 SF=1 was +169% over baseline because of
subquery duplication; with SharedSubtreeExec it's at parity. Same
mechanism unlocks future query optimizations that exploit
"compute once, consume many."

---

## 4. Shape-driven specialisation — pick the right kernel for the plan

The Σ.F + Σ.G + Σ.N families. The engine recognises common query
shapes and dispatches to a hand-rolled specialised operator instead
of running DataFusion's general one.

### Σ.F — Shape catalog DSL
**Where**: `crates/ematix-flow-core/src/shape_catalog/`.
**What**: A declarative DSL for "if you see this pattern, emit that
operator." Rules like "dict-encoded GROUP BY i64 → DictGroupCount" or
"single-column AGG with f64 predicate → FilterMultiAggSpec" are
expressed once, matched in one pass.
**Why it matters**: Adding a new specialisation is one shape entry,
not a new full optimizer rule. Less codegen tax (per
[[optimizer-codegen-sensitivity]] every new rule costs ~7% geomean
just from LLVM perturbing how it generates the rest of the chain).

### Σ.G.2f — Photon-style template specialisation
**Where**: `crates/ematix-flow-core/src/fused_aggregate*` + template files.
**What**: TPC-H Q01-shape `SELECT ... SUM(...) FROM t WHERE filter
GROUP BY a, b` gets compiled into a typed-slice + per-group
accumulator inlined together. No virtual dispatch on the hot row
loop; no per-row predicate eval.
**Why it matters**: Q01 SF=1 hits **27 ms vs DuckDB 45 ms**
(1.67× faster) on the same hardware, same data.

### Σ.G.2f.2 — typed-slice cache for predicate + agg eval
**Where**: `fused_aggregate.rs` templates.
**What**: Predicate result + agg input columns get cast/sliced into
typed Vec slices once, then the inner loop is a tight `for i in
0..n { ... }` with no Arrow downcasts.

### Σ.G.2f.2 — PerfectHashAggregate
**Where**: `fused_aggregate.rs`.
**What**: For aggregates where the group key is provably bounded
(e.g. `l_returnflag, l_linestatus` ∈ {0..15}), uses a direct array
indexed by key value — no hash table at all.

### Σ.G.2f.4 — Utf8ViewTwoKeyU16 template
**Where**: `fused_aggregate.rs`.
**What**: Two-string-key GROUP BY case (Q01-shape) using
`StringView`'s 16-byte fixed representation. Direct u128 compare on
the inline-string slice — no hash, no allocation.

### Σ.N — Robin Hood vectorised hash aggregate
**Where**: `robin_hood_agg.rs` (table) + `robin_hood_agg_exec.rs`
(operator) + `robin_hood_agg_rule.rs` (rule).
**What**: Custom Robin Hood open-addressing hash table specialised
for `i64 → u64` (COUNT, SUM(int)). Caller pre-sizes via observed
cardinality estimates; dynamic-resize-after-first-batch handles
under-estimates.
**Why it matters**: Σ.N.f.3 — **wins 1-5% over DataFusion's stock
vectorised AggregateExec** on Q12-shape (10K, 200K cardinality
TPC-H COUNT GROUP BY). Opt-in to avoid the optimizer-codegen-
sensitivity tax.

---

## 5. SIMD-aware kernels (lives in sibling crate `ematix-parquet`)

The decode floor. Hand-written NEON + AVX2 kernels for the
bit-unpacking + decode loops that dominate parquet scan time.

### Per-bit-width unpackers (bw=1..=32)
**Where**: `../ematix-parquet/crates/ematix-parquet-codec/src/unpack*`.
**What**: Const-generic per-bit-width macro-unrolled unpackers with
jumptable dispatch. Special cases: bw=2,3 indices + bw=4,6,8
lookup (gather-friendly), bw=15-18 (TPC-H l_partkey,
l_extendedprice, l_orderkey live here), bw=17 NEON kernel.
**Why it matters**: ematix-parquet v0.13.0 brought 14 wins, 0
regressions to the 22-query TPC-H bench. Q06 −18.7%, Q17 −9.5%.

### Σ.E5 LIKE matcher
**Where**: `crates/ematix-flow-core/src/like_matcher.rs`.
**What**: Photon-style SIMD substring matcher. Pre-compiles the
`LIKE` pattern into shift-and-match microcode; runs 9-14× faster
than `std::str::contains` on TPC-H Q13's o_comment.

### Σ.E6 — fused decode+predicate (F64)
**Where**: `ematix_parquet_bridge.rs` filter_f64_column_to_bitmap.
**What**: For `WHERE l_quantity < 0.2 × avg(...)` shapes, the
parquet f64 decode and the predicate evaluation are fused into a
single pass — the decoder writes directly into a per-row bitmap,
never materialising the f64 column.
**Why it matters**: Q06 SF=1 path; saves the entire memory write
of the decoded column when selectivity is high.

### LZ4_RAW Snappy / ZSTD / GZIP / Brotli decompressors
**Where**: ematix-parquet-codec.
**What**: Full codec coverage with DoS-bounded buffer caps.
**Why it matters**: ematix-parquet v0.14.0 fixed an LZ4_RAW bug
that took Q06 SF=10 from 3109 ms → 57.88 ms (**beats Polars 62 ms
by 6.7%**).

---

## 6. Late materialization & predicate pushdown

Don't decode data you'll throw away.

### Σ.E5 BridgeFilter — multi-column AND in scan
**Where**: `crates/ematix-flow-core/src/ematix_fast_parquet.rs`.
**What**: `Vec<ColumnPredicate>` AND'd into a row-bitmap before any
column is decoded into Arrow form. Predicate variants:
`I32Range`, `I32In`, `F64Range`, `StringEq`/`NotEq`/`In`/`Like`,
`I32ColumnPair`, `I64InBloom` (Σ.Q.L4′ + L9).
**Why it matters**: Q01/Q03/Q06/Q12/Q14/Q19 all benefit. Q06 SF=1:
9-12 ms (Polars-class).

### Σ.E5 dict-preserved Utf8View masked decode
**Where**: `read_column_byte_array_dict_preserved_into` in
ematix-parquet-codec.
**What**: When parquet dictionary-encodes a string column, ematix
preserves the dictionary all the way to Arrow's `StringView` — no
per-row materialisation. Combined with masked decode (`with_filter`):
only rows whose join/predicate keys pass get the dict-translate
step.

### Σ.E5 per-filter Exact pushdown declaration
**Where**: `EmatixFastParquetTableProvider::supports_filters_pushdown`.
**What**: Each predicate variant declares whether its bitmap eval
matches DataFusion's semantic eval exactly. If yes → declared
`Exact`, DataFusion drops the residual `FilterExec`. If FPR > 0
(e.g. `I64InBloom`) → `Inexact`, DataFusion keeps the residual
join/filter for safety.

### Page-level skipping + page-streaming
**Where**: `emat_page_stream.rs` + parquet page indexes.
**What**: For multi-page row groups, only the pages whose min/max
overlap the predicate are decoded.

---

## 7. Bloom filters — three levels

### Σ.J.2.b — Cross-stage Flight bloom (distributed)
**Where**: `crates/ematix-flow-distributed/src/bloom_*.rs` +
`crates/ematix-flow-core/src/bloom.rs`.
**What**: Build-side blooms emitted at the coordinator, shipped via
Flight HTTP headers (`x-ematix-bloom-*`) to every probe-side worker,
applied as `BloomFilterExec` wrappers over the scan.
**Why it matters**: Closes the distributed bloom-pruning loop.
Split-block bloom format, 256-bit cache-line-aligned blocks,
k=8 hashes per block, ≤8 KiB practical header size.

### Σ.J.2.b.vi — EnableContextBloomRule (probe-side wrapper)
**What**: PhysicalOptimizerRule that walks the plan, finds
`EmatixFastParquetExec` scans whose `<table>.<col>` uuid matches an
inbound bloom, and wraps them in `BloomFilterExec`.

### Σ.J.2.b.vii — emit_build_side_blooms (LogicalPlan walker)
**What**: Walks the LogicalPlan for Inner-equijoin candidates,
pre-executes the build side with `LIMIT cap+1`, hashes the join
keys into a `BloomFilter`, returns a `HashMap<column_uuid, Arc<BloomFilter>>`.

### Σ.Q.L4′ — InBloom ColumnPredicate (in-scan pushdown)
**Where**: `ColumnPredicate::I64InBloom` + dense kernel +
`EnableInBloomScanPushdownRule`.
**What**: Blooms pushed INTO `EmatixFastParquetExec`'s
`BridgeFilter` rather than wrapped post-scan. Probe-side rows whose
key isn't in the bloom skip masked-decode entirely.
**Status**: NEG on TPC-H Q07/Q21 because the pre-execution emitter
double-pays cost. Reusable for star-schema (probe = direct
TableScan) and for distributed shipping. Subsumed by L9 for joins.

### Σ.Q.L9 — Mid-query runtime sideband
**See section 2.** This is the AQE-class bloom path that wins
without the L4′ emission cost.

---

## 8. Auto-routing & rule chain

The "smart planner" surface. Every rule below is opt-in or
context-installed; the default chain is intentionally lean to keep
the optimizer-codegen tax low.

### EnableDictGroupCountRule (Σ.E3b)
COUNT(*) GROUP BY i64-dict-column → DictGroupCountExec.

### EnableDictFilterRule (Σ.F)
`WHERE col IN ('a','b','c')` over a dict-encoded string column →
dict-mask kernel (compares against dict entries, not rows).

### InjectFilterMultiAggRule (Σ.G.2f.3)
TPC-H Q01-shape `SELECT ... SUM(...) FROM t WHERE ... GROUP BY a, b`
→ `FilterMultiAggSpec` template specialisation.

### InjectFilterSumRule (Σ.G.2e)
Q06-shape `SELECT SUM(...) FROM t WHERE ...` → fused
filter+sum operator with no intermediate materialisation.

### DedupeAggregateForFloatDeterminism (Σ.P)
Q15-shape duplicate-aggregate elimination + sort-then-sum for f64
determinism.

### SwapSemiJoinBuildSideRule (Σ.Q.L2)
LeftSemi/RightSemi swap to put the agg-bounded side on BUILD. Now
partition-mode-aware (refuses unsafe `CollectLeft` swaps that would
violate `left_partitions == 1`).

### EnableContextBloomRule (Σ.J.2.b.vi)
Probe-side bloom wrap.

### EnableInBloomScanPushdownRule (Σ.Q.L4′)
In-scan bloom pushdown.

### EnableRuntimeBloomSidebandRule (Σ.Q.L9)
Mid-query bloom sideband — the AQE rule.

### EnableRobinHoodAggregateRule (Σ.N.d)
Q12-shape COUNT(*) GROUP BY i64 → RobinHoodAggregateExec.

### EnableRobinHoodSumF64Rule (Σ.Q.L1b)
SUM(f64) GROUP BY i64 → RobinHoodSumF64Exec.

---

## 9. Distributed primitives

### Arrow Flight peer mesh (`engine=distributed`)
Symmetric peer-to-peer; no coordinator/worker asymmetry. Any node
can be the coordinator for one query and a worker for the next.

### Flight LZ4 compression
Default-on. ~3× wire-size reduction with negligible CPU cost.

### Multi-bloom HeaderMap transport
`x-ematix-bloom-<uuid>` HTTP headers passthrough every Flight stage
via DataFusion's `set_distributed_passthrough_headers`.

### BloomSessionBuilder worker adapter
One-liner installation of probe-side rule + ContextBlooms at the
worker's `build_state` callback. Distributed bloom flow is
opt-in-then-zero-config.

---

## Σ.Q Lever scorecard (TPC-H SF=10, 2026-05-23)

| Lever | Status | Headline |
|---|---|---|
| L6′ Per-column RG decode cache | 🟢 WIN (opt-in) | Q21 −14%, Q18 −11%, Q17 −7% |
| L9 HashJoin → probe runtime sideband | 🟢 WIN (opt-in) | Q07 −4.7%, Q21 −5.9%; mechanism reusable for future AQE |
| **L6′ + L9 combined** | **🟢 WIN** | **SF=10 geomean ematix/DuckDB flipped from 1.043 (behind) → 0.970 (ahead)** |
| L2 Semi-join swap | 🟡 plan-hygiene | Now partition-mode-aware; Q20 no longer crashes |
| L4′ In-scan bloom pushdown | 🔴 NEG on TPC-H | Mechanism shipped; subsumed by L9 |
| L1b RobinHoodSumF64 | 🔴 NEG on Q18 | Kernel/operator/rule shipped; needs vectorised batch ingest |

---

## "What's intelligent about it?" — the elevator pitch

**ematix-flow doesn't just execute SQL — it adapts to your workload.**

- The **Σ.L runtime** learns from every query you've ever run and
  routes future queries based on what actually won, not what stats
  predicted.
- **Σ.Q.L9 mid-query AQE** captures data as a side-effect of
  HashJoin's build phase and feeds it back into probe-side scans
  before they decode a single row — no pre-execution pass, no
  extra cost.
- **Σ.G shape catalog** recognises common SQL shapes (Q01-style
  aggs, dict-encoded group-bys, multi-key joins) and dispatches to
  hand-written Photon-class kernels.
- **Σ.O decode cache** never pays for the same column twice across
  related queries — overlap-aware, byte-bounded, OS-cache-friendly.
- **ematix-parquet kernels** beat polars at decode (Q06 SF=10:
  ematix 58 ms vs polars 62 ms) because the bit-unpack + decode +
  predicate eval are fused into one SIMD pass.
- **Σ.J.2.b distributed blooms** ship build-side membership info
  across Flight stages so probe-side workers never scan rows that
  can't possibly join.

Everything's opt-in — defaults are conservative — but a workload
that turns on the right set of levers (today: `EMAT_RG_DECODE_CACHE=1
EMAT_RT_BLOOM_SIDEBAND=1`) gets a real measured win on its first
hot run and keeps improving over time.
