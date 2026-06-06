# ADR: Selection-vector vs. eager compaction for filtered lineitem scans

- **Status:** OPEN (draft for human decision — design exploration, not yet implemented)
- **Date:** 2026-05-31
- **Author:** architect (cold-start design pass)
- **Reviewers:** TBD (perf engineer / owner)
- **Scope:** Q07 / Q08 SF=10 residual gap to DuckDB; generalizes to every
  filtered-lineitem scan (Q06/Q12/Q14/Q19) that routes through the masked
  decode path.

---

## 1. Context & problem

### 1.1 The measured gap

TPC-H Q07 and Q08 at SF=10 lose ~8–11% to DuckDB on an Apple M4 Max (10 P +
4 E cores, 14 logical). All the usual structural levers were measured this
session and ruled out as neutral:

- Join order — neutral.
- Build-side swap (`EMAT_NDV_BUILD_SIDE`) — perf-neutral on Q08, −2.8% on Q07
  (see memory `[[rev19-q08-buildside-swap-rejected]]`).
- Dynamic-filter / bloom pushdown — neutral (`[[rev20-q07-q08-decode-bound]]`;
  the lineitem scan is Snappy-decompress-bound, and the bloom-fused late-mat
  ceiling is ~3%).

The residual gap traces to **one** step: filtered lineitem scans plateau at
~7 cores on the **row-selection / materialization** step, while DuckDB scales
past it.

### 1.2 The decisive measurements (this session, SF=10)

Bench harness:
`cargo run --release -p ematix-flow-core --example tpch_triangulation_bench --features triangulation`
with `PARTITIONS=N TPCH_DATA_DIR=examples/tpch/data/sf10 TPCH_SKIP_DUCKDB=1 TPCH_SKIP_POLARS=1`.

| Scan shape | PARTITIONS=4 | =7 | =14 | Scales past 7? |
|---|---|---|---|---|
| lineitem, 5 cols, **shipdate filter** (60M→18M), fused masked path | ~126 ms | ~116 ms | ~120 ms | **No — plateaus** |
| lineitem, 5 cols, **no filter** (dense decode of all 60M) | ~97 ms | ~75 ms | ~66 ms | **Yes — clean** |
| dense decode + **separate DataFusion `FilterExec`** (`with_streaming_arrow_reader(true)`) | ~125 ms | ~116 ms | ~120 ms | **No — identical to fused** |

Supporting evidence the cores are **busy, not blocked**:

- CPU-seconds ÷ wall ≈ 9–11× on the filtered path (cores are saturated, not
  idle waiting on a lock or I/O).
- `samply` showed the busy frames in the masked-decode kernels
  (`load_row_group_masked_legacy`, `masked_decode_one_column`,
  `masked_decode_i64`, the NEON predicate bitmap) with **no mutex / lock
  frames**.

The dense-decode-then-`FilterExec` row in the table is the load-bearing one:
it proves the plateau is **not** specific to our fused reader. A textbook
DataFusion pipeline (dense scan + standard `FilterExec`) plateaus at exactly
the same ~116–120 ms. So the cap is in the *select-and-compact* work, wherever
it physically runs.

### 1.3 The hypothesis (to confirm, see §2)

- **Dense decode is memory-bandwidth-bound.** Sequential streaming reads
  scale to ~14 cores because aggregate DRAM bandwidth is the limit and more
  cores buy more bandwidth-channels until saturation.
- **The filter take/gather is memory-latency-bound.** Compacting 18M selected
  rows out of 60M across 5 columns into new `RecordBatch`es is a *gather* — a
  stream of data-dependent loads (read a dict index / row offset, then load
  from that address). Dependent loads expose memory **latency**, and a single
  core's memory-level parallelism (outstanding-miss budget, ~tens of in-flight
  loads) saturates well before 14 cores' worth. The observed knee at ~7 cores
  is consistent with latency-bound gather, not bandwidth-bound streaming.

### 1.4 The architectural tension

DataFusion is **pull-based** and every operator boundary hands off a **dense,
compacted `RecordBatch`**. DuckDB (and Velox/Photon) are **push-based** and
carry a **selection vector** — a list of surviving row indices — *alongside*
the still-dense decoded columns, deferring (often eliminating) the physical
compaction. Their filter→gather cost scales because they never pay the gather:
downstream vectorized operators consume `(data, selection)` pairs directly.

The crux: **a scan-local change cannot avoid the take.** The moment our scan
hands a batch to any standard DataFusion operator (`HashJoinExec`,
`AggregateExec`), that operator expects a dense batch. So unless the
*consuming* operator is selection-aware, the compaction must happen at or
before the scan→operator boundary regardless of how clever the reader is.

### 1.5 What already shipped, and why it does not solve this

A related fix shipped for **Q18 SF=10** (commit `af4f88d`, "Σ.E5"):
`EmatInlineStreamingReader` stops decoding a whole row group before emitting,
so decode overlaps a heavy downstream hash-agg. The auto-inline rule fires at
`partition_rows >= 2M AND row_groups.len() > 1`
(`ematix_fast_parquet.rs:3200, 3223–3226`).

But streaming is gated `!has_filter`
(`ematix_fast_parquet.rs:3191, 3223–3241`), with the comment:

> "Σ.E5 (#516): … Inline + page-streaming readers don't have a masked-decode
> branch yet — force them off."

Verified: `EmatInlineStreamingReader::new` and `EmatPageStreamingReader::new`
(`emat_page_stream.rs:706, 950`) take **no** `BridgeFilter` parameter; only the
eager `EmatArrowBatchReader` builder has `.with_filter`
(`ematix_fast_parquet.rs:3293`). So **filtered** scans fall back to the legacy
eager masked reader and are excluded from streaming.

We tested whether finishing #516 (streaming the masked path) would help Q07.
**Full-Q07 A/B was neutral: 155.9 vs 160.2 ms.** Reason: Q07's downstream is
too light to benefit from overlap — a `CollectLeft` broadcast `s⋈l` join +
light joins + a 56-group final agg. Unlike Q18's 150K-group hash-agg, there's
no heavy consumer to hide decode behind. **Conclusion: streaming alone (still
compacting) is neutral; the win requires NOT compacting.**

---

## 2. Confirm the "take/gather is the capped kernel" hypothesis — PREREQUISITE

This confirmation is a **gate**. Do not start any build below until the
profile nails which kernel caps. The §1.2 evidence is strongly suggestive but
"samply showed the busy frames in the masked-decode kernels" lumps three
distinct costs together: (a) predicate evaluation building the bitmap,
(b) Snappy decompression of surviving pages, (c) the gather/compaction itself.
The hypothesis blames (c). It needs to be isolated, because **the chosen
option depends on which one it is**:

- If the cap is the **gather/compaction (c)** → Options A/B/C are on-target.
- If the cap is **predicate eval (a)** → the fix is a faster/parallel predicate
  kernel, not a selection vector (different, smaller project).
- If the cap is **Snappy decompress (b)** → this is the known decompress floor
  (`[[q06-sf10-polars-gap-wall]]`, `[[rev20-q07-q08-decode-bound]]`); a
  selection vector does **not** help and this whole ADR is moot.

### Code-level evidence the kernel is a scalar dependent-load gather

Read this session (file:line), supporting (c):

- `emat_arrow_reader.rs:1199–1281` — `load_row_group_masked_legacy` builds the
  combined predicate bitmap (`build_bitmap`, `ematix_fast_parquet.rs:460`),
  computes `popcount`, then parallel-decodes each column via
  `masked_decode_one_column` (one column per work-stealing thread,
  `emat_arrow_reader.rs:1226–1252`). **Parallelism is across columns (≤5 here),
  not across rows within a column** — so a 5-column scan can use at most 5
  threads for the gather regardless of `PARTITIONS`. *This alone may explain
  the ~7 knee:* 5 columns × a couple partitions' worth of overlap.
- `masked_decode_one_column` → `masked_decode_i64`
  (`ematix_parquet_bridge.rs:808`) → `read_column_i64_masked_into`
  (`ematix-parquet` `crates/ematix-parquet-codec/src/read.rs:224`) →
  `decode_chunk_row_masked_into` (`read.rs:2045`).
- The actual gather is in two primitives, both **scalar, branchy, push-per-row**:
  - PLAIN: `plain_sparse_decode_i64_into` (`plain.rs:250`) — per matched lane,
    `out.push(i64::from_le_bytes(bytes[off..off+8]))`. Data-dependent offset,
    scalar load, `Vec::push` (which is itself an auto-vectorization barrier,
    see `[[rev14-narrow-autovec]]`).
  - DICT: `gather_dict_at_bitmap_into` (`dict.rs:635`) — per matched row,
    `out.push(dict[idx].clone())`: read the index, **then** load `dict[idx]`.
    This is the canonical dependent-load gather.
- Output is a **dense `Vec<T>` of exactly `popcount` rows** wrapped as a
  `DecodedColumn`; `slice_batch` (`emat_arrow_reader.rs:1671`) assembles them
  into a dense `RecordBatch` that leaves the scan via `RecordBatchStreamAdapter`
  (`ematix_fast_parquet.rs:2698`). **There is no selection vector anywhere —
  compaction is total and eager.**

So the code shape is consistent with (c). But "consistent with" is not
"confirmed," and there are two confounders to rule out.

### Cheapest measurement to nail it (do this first, ~1–2 h, no long benches)

1. **Per-stage `EMAT_TIMING` split on the dense+`FilterExec` path** at
   PARTITIONS=7 and 14. `slice_batch` already has an `EMAT_TIMING`
   batch-filter timer (`emat_arrow_reader.rs:1692–1713`). Add equivalent
   per-stage timers (or read the existing samply profile) to attribute wall to
   **predicate-eval+bitmap** vs **decompress** vs **the Arrow `filter`/take**.
   If the *take* time is flat 4→7→14 while decode time keeps dropping, (c) is
   confirmed.
2. **Vary column count, fixed selectivity.** Re-run the filtered scan with 1,
   3, 5, 8 projected columns. If the plateau core-count rises with column count
   (more columns → more gather threads → higher knee), that confirms the cap is
   the per-column gather AND that the current ≤5-thread column parallelism is a
   contributing factor (cheap partial fix: see Option A.0 below).
3. **Selectivity sweep.** Re-run at ~1% pass (Q06-like) vs ~30% pass
   (current shipdate ~30%). If high-selectivity gathers more rows and plateaus
   harder, that's gather-bound (c); if low-selectivity (decompress-dominated)
   also plateaus, suspect (b) and stop.

**Kill-criterion for the whole ADR:** if step 1 shows the non-scaling frames
are predominantly Snappy decompress (b), close this ADR — the gap is the
decompress floor and no selection-vector work helps.

---

## 3. Options

All three keep the engine free of TPC-H-specific hardcoding (the win must come
from a generalized "filtered scan" pattern, per `[[feedback-no-tpch-hardcoding]]`).

### Option A — Faster / more-parallel take kernel (keep compacting)

Keep eager compaction; make the gather scale past 7.

- **A.0 (cheapest) — raise column-parallel thread count / add row-parallel
  gather.** Today the masked path parallelizes across ≤5 columns
  (`emat_arrow_reader.rs:1226`). If §2 step 2 shows column-count drives the
  knee, split each column's row range across threads so a 5-column scan can use
  all 14 cores. Blast radius: one function. Risk: re-merge cost; thread
  oversubscription vs the outer partition parallelism (the budget cap at
  `emat_arrow_reader.rs:1206` exists precisely to avoid `N_part × N_col`
  oversubscription — must be respected).
- **A.1 — SIMD / blocked / prefetch gather.** Replace the scalar
  `plain_sparse_decode_*` / `gather_dict_at_bitmap_into` loops with a blocked
  gather that software-prefetches the next block's source addresses, and (where
  the target supports it) NEON/AVX2 gather lanes. Removes the `Vec::push`
  vectorization barrier by writing into a pre-sized buffer (the
  `[[rev14-narrow-autovec]]` trick already proved pre-sized pointer-writes let
  LLVM auto-vectorize a narrow that `Vec::push` blocked).
- **Blast radius:** kernel-local, in `ematix-parquet` (`plain.rs`, `dict.rs`,
  `bitpack_neon.rs`) + the thin bridge. No DataFusion-operator changes. No new
  optimizer rule (avoids the codegen tax of `[[optimizer-codegen-sensitivity]]`).
- **Expected ceiling:** the gather is *part* of the ~53 ms delta (filtered
  116–120 ms vs unfiltered 66 ms = ~50–54 ms). A.0 alone could recover a chunk
  if column-parallelism is the knee. A.1 attacks the latency wall directly but a
  pure latency-bound gather has a hard floor — software prefetch typically
  recovers 20–40% of latency stalls, not all. **Partial** win expected; could be
  meaningful, won't fully close to DuckDB.
- **Effort:** A.0 ~1–2 days. A.1 ~1–2 weeks (kernel + microbench + the
  cross-architecture matrix that `bitpack_neon.rs` already maintains).
- **Risk:** **Low-to-medium.** Kernel-local, easy to A/B, no plan changes.
  Prior caution: `ematix-parquet` commits/publishes are gated — these kernels
  are modifiable locally but a published tag is **not** assumed available, so
  the change must be vendorable/local-buildable (see §5).

### Option B — Selection-vector-aware masked streaming reader (finish #516 right)

Scan emits batches that carry a **selection vector** and defers compaction;
compaction happens lazily, only when a downstream truly needs dense rows.

- **Mechanism:** finish #516 so `EmatInlineStreamingReader` /
  `EmatPageStreamingReader` accept a `BridgeFilter` (they don't today —
  `emat_page_stream.rs:706, 950`). Instead of gathering, they emit a batch of
  the **dense decoded page columns + a `BooleanArray`/index selection**.
- **The hard boundary (must be defined precisely):** DataFusion operators
  consume dense batches. There are exactly two honest ways to expose a
  selection without forking DataFusion:
  1. **Emit a dense batch + a separate `FilterExec` above the scan** — but
     that's *exactly* the dense+`FilterExec` shape we measured at 116–120 ms.
     It does not avoid the take; it relocates it. **No win.**
  2. **Carry the selection as a nullability/validity trick or a sidecar
     column** — non-standard; downstream `HashJoinExec`/`AggregateExec` will
     not honor it; correctness breaks. **Not viable** without operator changes.
- **Conclusion:** compaction becomes unavoidable **the instant the batch
  crosses into a stock DataFusion operator.** So Option B *by itself* (scan-only)
  collapses into either "still compacting" (no win, matches the measured
  neutral full-Q07 A/B) or "compact lazily, but the first consumer forces it
  anyway." Option B is only meaningful **bundled with at least one
  selection-aware consumer** — at which point it is the first phase of Option C.
- **Blast radius:** scan + reader; but the *useful* version pulls in operator
  changes (→ C).
- **Expected ceiling:** ~0 as scan-only (confirmed by the neutral full-Q07
  A/B). Non-zero only with a selection-aware consumer.
- **Effort:** medium for the reader plumbing; the value is gated on C.
- **Risk:** **High of wasted effort** — strong chance of rebuilding the
  measured-neutral path. **Do not pursue B in isolation.**

### Option C — Selection-vector execution model end-to-end (DuckDB/Velox-style)

Custom `ExecutionPlan` nodes — a selection-aware Filter, and at minimum a
selection-aware probe side for `HashJoinExec` and ingest for `AggregateExec` —
that consume and propagate `(dense columns, selection vector)` without
compacting between them. Compact only at a final materialization boundary.

- **Mechanism:** define an internal batch envelope `(RecordBatch, Option<Selection>)`
  and a set of operators that understand it. The scan emits dense pages +
  selection (Option B's reader); the selection-aware Filter ANDs predicates into
  the selection; the join probe and agg ingest read through the selection
  (gather happens once, at the point where a row genuinely must be touched —
  e.g. building a hash key — instead of materializing a full intermediate).
- **The honest question — is this realistic on top of DataFusion without
  forking it?** Partially. DataFusion lets you add custom `ExecutionPlan` nodes,
  so a *closed sub-pipeline* (selection-aware Scan→Filter→Join-probe→Agg) can be
  built as custom operators that interoperate with stock DataFusion at the
  edges (dense in, dense out). That is **not** a fork. **But** it only works for
  plan fragments where we control every operator in the chain; the moment a
  stock DataFusion operator sits in the middle, the selection collapses to a
  compaction there. DuckDB gets end-to-end selection because *every* operator is
  selection-aware; we would get it only on the fragments we re-implement. So
  this is "fork-scale effort on a per-operator basis," delivered incrementally,
  not a literal fork.
- **Blast radius:** **very large.** New operators, new batch envelope, planner
  rules to substitute the custom operators for the stock chain, and a
  correctness surface across joins/aggregations. High risk of the
  `[[optimizer-codegen-sensitivity]]` tax (every new rule has cost ~5–8% geomean
  historically) and of the `[[sigma-l3c-reverted]]` failure mode (a masked-path
  change that starves parallel downstream).
- **Expected ceiling:** the full ~50 ms delta in principle — this is the only
  option that can architecturally match DuckDB. In practice, capped by how much
  of the hot plan we can keep inside the selection-aware fragment.
- **Effort:** **months.** Multi-operator, multi-quarter.
- **Risk:** **Very high.** Correctness across 22 queries; regression surface
  huge; codegen tax; long time-to-first-measured-win.

---

## 4. Recommendation & phased plan

**Recommended: Option A, sequenced and gated. Treat B/C as explicitly deferred
until A's ceiling is measured.** Rationale: the engineer is measurement-first
and rejects unmeasured levers. A is kernel-local, cheap to A/B, carries no plan
or codegen-tax risk, and directly attacks the kernel §2 must confirm. B in
isolation is a near-certain rebuild of a measured-neutral path. C is the only
true architectural fix but is months of very-high-risk work that should not
start before A establishes how much head-room a "keep-compacting" approach
leaves on the table.

Every phase below is independently measurable with a kill-criterion, and every
phase must pass the **shared regression gate** before merge:

- **22-query SF=10 A/B regression gate.** Q06/Q12/Q14/Q19 route through the
  same masked path and are **current wins** (low-selectivity scans). They must
  not regress. Use the interleaved A/B harness (`scripts/bench/strict_ab.sh`)
  per `[[sigma-ai-1-strict-bench-landed]]`, not loose single-shot runs.
- **`tpch_validate` 22/22** correctness (row counts + sums vs DuckDB).
- **No TPC-H-specific hardcoding** — the change must be a generalized
  "filtered-scan gather" improvement, not a Q07/Q08 special case.
- **`ematix-parquet` is local-modifiable but publish-gated** — any kernel
  change must build against the vendored/local crate; do **not** assume a
  published tag.

### Phase 0 — Confirm the capped kernel (PREREQUISITE, §2)

Profile the dense+`FilterExec` path (PARTITIONS=7 and 14); attribute wall to
predicate-eval / decompress / take; run the column-count and selectivity
sweeps. ~1–2 h.

- **Kill-criterion:** if the non-scaling frames are predominantly Snappy
  decompress (b), **close this ADR** — decompress floor, no selection-vector
  win. If predominantly predicate-eval (a), pivot to a predicate-kernel ADR
  (out of scope here).
- **Proceed only if** the take/gather (c) is confirmed as the cap.

### Phase 1 — Option A.0: row-parallel / wider gather

If Phase 0 step 2 shows column-count drives the knee, split per-column gather
across row ranges so a 5-column scan saturates all cores. Respect the
parallelism budget (`emat_arrow_reader.rs:1206`) to avoid oversubscription.

- **Measure:** the §1.2 filtered-scan microbench at 4/7/14 + the 22q SF=10 gate.
- **Kill-criterion:** if the filtered scan does not improve the 7→14 plateau,
  or any of Q06/Q12/Q14/Q19 regresses past the 2σ noise bar, revert. (Recall
  `[[sigma-l3c-reverted]]`: a masked-path change that won a microbench but
  starved parallel downstream and gave back 16.8% geomean — kernel-bench wins do
  not always survive wall-time.)

### Phase 2 — Option A.1: SIMD / blocked / prefetch gather

Rewrite the scalar `plain_sparse_decode_*` / `gather_dict_at_bitmap_into` loops
as a blocked, prefetched, pre-sized-buffer (no-`Vec::push`) gather; add SIMD
lanes where the target allows.

- **Measure:** isolated gather microbench (random index gather, the only honest
  proxy) **and** the full filtered-scan wall **and** the 22q SF=10 gate.
- **Kill-criterion:** the `ematix-parquet` repo's existing discipline applies —
  if the microbench wins but real Q07 + the 22q gate don't, **do not ship**
  (this is the exact `[[hand-rolled-snappy-neg]]` / `[[sigma-nf3-beats-stock]]`
  lesson: microbench regime ≠ SQL regime). Add a `bench_*` regression guard if
  it ships.

### Phase 3 — DECISION POINT (re-open this ADR)

After Phases 1–2, measure the **residual** Q07/Q08 gap. Then decide:

- If A closed enough that the residual is within noise of DuckDB → **done**,
  mark this ADR Accepted-as-Option-A.
- If a meaningful gap remains AND Phase 0 confirmed it is unavoidable
  compaction → escalate to a **new** ADR scoping Option C as an incremental
  selection-aware-operator program (Scan→Filter→Join-probe first, since Q07's
  hot path is a broadcast join). Option B's reader work is folded into C's
  Phase 1, **never pursued standalone.** This escalation is a months-long,
  very-high-risk commitment and deserves its own decision record with the
  codegen-tax mitigation (PGO / out-of-optimizer rules per
  `[[optimizer-codegen-sensitivity]]`) designed up front.

---

## 5. Out of scope

- Predicate-eval (bitmap-build) kernel optimization — separate ADR if Phase 0
  attributes the cap there.
- Snappy decompress rate — known floor (`[[q06-sf10-polars-gap-wall]]`),
  needs a codec or writer-side change, not an engine change.
- Forking DataFusion to make *all* operators selection-aware — not on the table.
- Publishing changes to `ematix-parquet` (commits/publishes are gated; all
  kernel work here is local-buildable only).
- SF=100+ distributed behavior — this ADR is single-node SF=10.

## 6. Decision

**OPEN.** This is a draft for the human (a measurement-first perf engineer) to
decide. The recommendation is to run Phase 0 (cheap, ~1–2 h) before committing
to any build, then pursue Option A in two gated phases, and only escalate to an
Option-C ADR if a confirmed-unavoidable residual remains.

## 7. References

- `crates/ematix-flow-core/src/ematix_fast_parquet.rs`
  - `BridgeFilter` (`:68`), `build_bitmap` (`:460`), `estimate_pass_rate`
    (`:94`/`:713`), `with_predicted_pass_rate` (`:167`), call site applying it
    to runtime sideband predicates (`:2675–2676`).
  - `has_filter` gate forcing off inline/page-streaming (`:3191`, `:3223–3241`);
    auto-inline rule `partition_rows >= 2M AND row_groups>1` (`:3200`,
    `:3223–3226`); eager-reader `.with_filter` wire-up (`:3293`).
  - Batches leave via `RecordBatchStreamAdapter` (`:2698`).
- `crates/ematix-flow-core/src/emat_arrow_reader.rs`
  - `load_row_group_masked` (`:1056`), `load_row_group_masked_legacy`
    (`:1139`), parallel column gather + dense-fallback selectivity gate
    (`:1189`, `:1199–1281`), `load_row_group_dense` (`:1482`),
    `masked_decode_one_column` (`:1779`), `slice_batch` (`:1671`),
    parallelism budget cap (`:1206`), `EMAT_TIMING` batch-filter timer
    (`:1692`).
- `crates/ematix-flow-core/src/emat_page_stream.rs`
  - `EmatPageStreamingReader::new` (`:706`), `EmatInlineStreamingReader::new`
    (`:950`) — neither takes a filter (the #516 gap).
- `crates/ematix-flow-core/src/ematix_parquet_bridge.rs`
  - `masked_decode_i32/i64/f64` (`:795/:808/:821`), `masked_decode_byte_array`
    (`:1188`).
- `../ematix-parquet/crates/ematix-parquet-codec/src/`
  - `read.rs`: `read_column_i64_masked_into` (`:224`),
    `decode_chunk_row_masked_into` (`:2045`) — the masked decode loop.
  - `plain.rs`: `plain_sparse_decode_i64_into` (`:250`) — scalar PLAIN gather.
  - `dict.rs`: `gather_dict_at_bitmap_into` (`:635`) — dependent-load dict gather.
  - `bitpack_neon.rs`: `decode_predicate_bitmap_neon_bw12` (`:306`) — NEON
    predicate bitmap.
- Commit `af4f88d` — "perf(Σ.E5): auto-inline streaming reader for large
  multi-RG partitions" (the shipped Q18 fix; verified).
- Memory: `[[rev20-q07-q08-decode-bound]]`, `[[rev19-q08-buildside-swap-rejected]]`,
  `[[q06-sf10-polars-gap-wall]]`, `[[sigma-l3c-reverted]]`,
  `[[optimizer-codegen-sensitivity]]`, `[[rev14-narrow-autovec]]`,
  `[[hand-rolled-snappy-neg]]`, `[[sigma-ai-1-strict-bench-landed]]`,
  `[[feedback-no-tpch-hardcoding]]`.
