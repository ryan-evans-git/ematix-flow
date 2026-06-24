# Owning the writer — lowering the decode floor

*Scope draft, 2026-06-20. Successor to the "fastest theoretical" external-dependency
audit. The execution spine (push/morsel/fused-agg/CSE) is largely in-housed and pays
where boundary-materialization is the cost (Q01/Q06/Q08). The remaining SF=10/SF=100
gaps are **not** framework tax — they are the decode floor, cache-miss algorithms, and
plan quality. This doc scopes the **decode floor**: the one load-bearing thing on the
critical path we do **not** yet own — how the bytes were written.*

## Phase-0 RESULTS — GO (2026-06-20)

Ran the Phase-0 codec A/B (Snappy vs LZ4_RAW lineitem, codec-only swap on the
`lineitem.lz4.parquet` faithful transcode — uncompressed sizes identical to
Snappy, same encoding). ematix-only, SF=10, interleaved 4 rounds (cold round
discarded), `caffeinate -i`. Correctness gated first: ematix-LZ4 == DuckDB on
8/8 queries (Q01/Q03/Q06/Q07/Q12/Q14/Q15/Q19, FP-tolerant cell compare).

| Query | LZ4 vs Snappy (sized path) | note |
|---|---|---|
| Q06 | **−27.5%** | extprice/discount/shipdate decode-bound |
| Q14 | **−31.2%** | shipdate/extprice — **current SF=10 loss vs Polars** |
| Q15 | **−23.5%** | extprice/discount — **current Polars parity** |
| Q19 | **−18.3%** | extprice/quantity |
| Q07 | **−7.8%** | join keys — **current SF=10 loss vs DuckDB (~13%)** |
| Q03 | **−9.0%** | join keys (l_orderkey) |
| Q01 | +0.1% (tie) | agg-bound, not decode-bound → codec-inert |
| Q12 | **+5.2%** | incompressible dates (ratio 1.0) — see nuance below |

**Verdict: GO.** Changing *only* the lineitem codec moved the wall −8% to −31%
on 6/8 queries — which is itself the cleanest possible proof that Snappy
decompress was the dominant *movable* slice (nothing else changed). The
Phase-0 wall-decomposition (profiler) is therefore redundant — the A/B is a
stronger direct proof.

**Three findings sharper than the scope predicted:**

1. **Reader prerequisite found & fixed.** The production reader
   (`emat_arrow_reader::decompress_into`) supported only Uncompressed/Snappy/Zstd
   — `Lz4Raw` hard-errored ("not yet supported"). The v0.14 LZ4 work
   ([[ematix-parquet-lz4-decode-bug]]) had landed in the sibling's
   `emat_page_stream`, a path the production `EmatixFastParquetTableProvider` no
   longer uses. **Now wired** (import + `Lz4Raw` arm + a `decompress_into_sized`
   helper using the page-header's declared size on the two primitive-decode
   callsites). ~25 lines, local/uncommitted on `docs/readme-numbers-correction`.
   This is the natural first Phase-1 increment (and arguably a standalone bug fix
   — any user with LZ4 parquet hits the error today).

2. **The sized path matters.** First A/B used the size-less LZ4 decode (scans
   block tags to find the uncompressed length). That penalty made Q03/Q07/Q12
   look +5–7% *worse* — masking the win on high-entropy join-key columns
   (l_orderkey ratio 0.36, many tags). Threading the page-header
   `uncompressed_page_size` into the decode (sized path, no scan) flipped Q03
   −9% and Q07 −7.8% and roughly 4×'d Q06's win (−6% → −27.5%). Lesson: the
   codec win is real but **only realized with the sized decode** — the writer
   work must pair LZ4 with the sized reader path (done).

3. **Codec must be PER-COLUMN, not global.** Q12 stayed +5% worse: it's
   dominated by **incompressible date columns** (l_shipdate/commitdate/receiptdate,
   ratio 1.00). For those there is no decompress to save, and LZ4_RAW framing
   makes the bytes marginally *larger* (1.592MB vs Snappy 1.587MB) → slight net
   loss. **This changes the writer design:** pick the codec per column by
   measured compressibility (LZ4 for the compressible/decode-heavy columns;
   uncompressed or Snappy for ratio-1.0 columns), not one global codec. Q12 is
   the proof that global-LZ4 isn't strictly optimal.

**Flip potential (Phase-1 to confirm head-to-head):** Q14 (−31%) very likely
flips its Polars loss; Q15 (−24%) widens parity to a clear win; Q07 (−8%)
narrows but may need a second lever to fully flip its ~13% DuckDB gap. These
are ematix-side deltas — the fair ematix-LZ4-vs-DuckDB-LZ4 and
ematix-LZ4-vs-DuckDB-Snappy head-to-heads are the first Phase-1 measurement.

---

## MONEY-QUESTION RESULT — writer is a PRODUCT feature, NOT a campaign-flip lever (2026-06-20)

Ran ematix vs DuckDB (preset harness) and ematix vs Polars (triangulation,
Polars-only) on canonical-Snappy vs optimized-LZ4, SF=10, interleaved. **The
LZ4 codec lever lowers the decode floor for EVERY engine — so apples-to-apples
it flips NOTHING, and actually erodes ematix's decode-bound standings.**

vs **DuckDB** (both read the same file):

| Q | emat_S / duck_S | emat_L / duck_L | apples-to-apples |
|---|---|---|---|
| Q06 | 52 / 80 (EMAT) | 42 / 68 | EMAT 1.61× (gap grew) |
| Q14 | 92 / 134 (EMAT) | 66 / 109 | EMAT 1.64× (grew) |
| Q15 | 68 / 88 (EMAT) | 50 / 75 | EMAT 1.49× (grew) |
| Q07 | 149 / 148 (duck) | 134 / 133 | **duck 1.00× — no flip** |

DuckDB also decodes LZ4 faster than Snappy. ematix's advantage *grows* on the
decode-heavy wins (our SIMD LZ4 decoder beats DuckDB's), but Q07 — the actual
DuckDB loss — does NOT flip apples-to-apples (its gap isn't pure decode).

vs **Polars** (the Q14/Q15 competitor) — the killer:

| Q | ematix ΔLZ4 | Polars ΔLZ4 | now (Snappy) | apples-to-apples (LZ4) |
|---|---|---|---|---|
| Q06 | −19% | **−30%** | EMAT 1.08 | **pol 1.07 — flipped AGAINST us** |
| Q14 | −28% | **−34%** | pol 1.03 | pol 1.12 (lead widened) |
| Q15 | −26% | **−27%** | pol 1.08 | pol 1.10 (widened) |

**Polars benefits from LZ4 MORE than ematix** (overturns the Phase-1 inference
that its weak no-SIMD decoder would gain less). Root cause: ematix already had
the **most-optimized Snappy decode**, so the least headroom from a faster codec;
the slower-baseline engine gains more. Apples-to-apples, LZ4 *flips Q06 against
us* and *widens* Polars's Q14/Q15 lead.

**Conclusion — two truths:**
1. **As a TPC-H campaign lever (fair fight, identical inputs): NO-GO.** The codec
   lifts all boats and erodes our hard-won Snappy-decode edge. The SF=10 losses
   (Q05/Q07/Q18 vs DuckDB; Q14/Q15 vs Polars) need engine/plan levers, not
   storage. This *kills* the Phase-0/1 "writer is a multi-flip lever" claim.
2. **As a product feature: real and valuable.** ematix-on-LZ4 is 19–28% faster
   in absolute terms than ematix-on-Snappy. In the product (ematix controls
   `ManagedTable` storage; users query through ematix; competitors never read
   ematix's storage), that's a genuine "faster queries + optimal storage"
   capability. Ship it as a product feature, market it honestly as absolute
   speedup — NOT as a benchmark-flip.

Cross-harness caveat: ematix preset vs Polars triangulation absolutes carry
drift, but each engine's ΔLZ4 is a clean within-harness interleaved A/B, so
"Polars gains more" is robust. Bench logs `/tmp/ab_money`, `/tmp/ab_pol`.

---

## Phase-1 RESULTS — writer built; per-column ≈ global-LZ4; Q12 regression is NOT codec (2026-06-20)

Built `crates/ematix-flow-core/examples/optimize_table.rs` — reads any parquet,
picks a codec **per column** from the source's compressibility ratio (ratio <
0.90 → LZ4_RAW, else UNCOMPRESSED), and re-emits through the **arrow-rs writer**
(full schema fidelity — DATE/DECIMAL/STRING preserved; the sibling `ColumnData`
writer can't carry logical types, so it's the wrong tool until the native-format
phase), V1 data pages, streaming batch-by-batch (bounded memory). Per-column
codec API confirmed present in published `ematix-parquet-codec` 0.17 too, but
arrow-rs is the right Phase-1 writer for schema fidelity + fair-bench (standard
parquet any engine reads). Decisions on SF=10 lineitem matched Phase-0 exactly
(LZ4 for orderkey/partkey/suppkey/linenumber/extprice/comment; uncompressed for
the 10 ratio-1.0 cols). File +1.9%. Correctness: ematix(per-column) == DuckDB
8/8 (Q01/03/06/07/12/14/15/19).

**3-way SF=10 A/B (Snappy vs global-LZ4 vs per-column, ematix-only, interleaved, median r2-4):**

| Q | Snappy | global-LZ4 | per-column | best |
|---|---|---|---|---|
| Q06 | 52.1 | **−26.3%** | −25.5% | LZ4 |
| Q14 | 90.7 | **−31.2%** | −29.2% | LZ4 |
| Q15 | 68.4 | **−24.1%** | −23.8% | LZ4 |
| Q19 | 138.6 | −19.3% | **−21.0%** | LZ4 |
| Q03 | 140.4 | **−8.3%** | −5.6% | LZ4 |
| Q07 | 143.8 | **−7.3%** | −5.4% | LZ4 |
| Q01 | 263.9 | −5.0% | **−5.9%** | LZ4 |
| Q12 | 98.0 | +7.0% | +7.0% | **Snappy** |

**Two Phase-0 inferences overturned by this A/B (profile-don't-infer in action):**

1. **Per-column codec ≈ global-LZ4 — the per-column refinement does NOT pay at
   SF=10.** Every query is within noise between the two arms. Phase-0's "codec
   must be per-column" was an artifact of the **size-less** LZ4 penalty (which
   only hurt the global arm on high-entropy columns); with the **sized** decode
   path, LZ4 on incompressible columns ≈ uncompressed, so global-LZ4 is just as
   good. Per-column stays the right *general* design (a fat incompressible BLOB
   column would benefit, and it may pull ahead at **SF=100** where decompress
   CPU on ~4 GB of incompressible columns is non-free under IO pressure — untested),
   but for TPC-H the incompressible columns are too small to matter. **The win
   is simply "LZ4 not Snappy."**

2. **Q12's +7% is NOT the date codec.** Phase-0 inferred Q12 regressed because
   LZ4-on-incompressible-dates. Disproved twice here: (a) per-column keeps all
   three date columns **uncompressed** (less decode work, byte-identical size to
   source — optimized l_shipdate uncomp == source 90 757 554) yet Q12 is still
   +7%; (b) the Phase-0 **DuckDB**-written LZ4 file *also* showed Q12 +5%. So the
   regression is **common to both writers and survives uncompressed dates** → an
   LZ4-rewrite/page-layout/stats effect specific to Q12's multi-date-comparison
   shape (l_commitdate<l_receiptdate, l_shipdate<l_commitdate + l_shipmode IN),
   not codec. Lone regression vs 7 wins; needs a dedicated dig (Phase-1.5).

**Net:** the writer is built, correct, and delivers the decode-floor win on 7/8
decode-touching queries (−5% to −31%) through a real schema-preserving writer.
Simpler than scoped (global-LZ4 suffices at SF=10; per-column is insurance for
general data + SF=100). Files: `/tmp/sf10_opt` (per-column), `/tmp/sf10_glz4`
(global), bench logs `/tmp/ab_p1`. Tool uncommitted on docs branch.

---

## The lever in one line

We **cannot decompress Snappy faster** — that well is dry and proven dry:

- Hand-rolled Snappy decompressor: **REJECTED** (wins a random microbench, loses 12%
  on the real pipeline — `project_hand_rolled_snappy_neg`).
- `read_uvarint` and the narrow-decode auto-vec paths are at the **cycle floor**
  (`project_ematix_parquet_varint_optimal`, `project_rev14_narrow_autovec`).
- REV.20: on Q07/Q08 the scan is **74% / 58%** of total CPU, and that decode is
  **97% decompress / 3% materialize** — there is no materialize fat left to trim.
- extprice decompresses at **~1.73 GB/s** (Snappy); that rate is the wall.

But **if we write the bytes, we don't have to use Snappy** — and we also control sort
order, encoding, dictionary preservation, and page layout. The decode floor is a
*writer* property masquerading as a decoder limit. We own the decoder
(`../ematix-parquet`, v0.17.0, CI-published). We do **not** own a writer. That is the
gap.

## Why this is the broadest unowned lever

Blast radius against the **remaining SF=10 losses + margins** (vs DuckDB unless noted):

| Query | Today (SF=10) | Bottleneck | What the writer attacks |
|---|---|---|---|
| Q07 | **loss ~13%** | scan 74% CPU, 97% decompress | codec (Snappy→LZ4) directly cuts the 74% |
| Q14 | **loss 1–10%** (Polars) | "at the Snappy-decompress rate" | codec; sort-by-shipdate re-enables page pruning |
| Q15 | **parity** (Polars 1.06×) | residual = extprice Snappy-decompress | codec could flip parity → clear win |
| Q08 | win 151 vs 173 (bloom rescue) | decode 58% CPU | codec widens the margin |
| Q06 | win, ~40% under Polars | already stripped to 36ms | marginal further gain |

So the writer is plausibly **2 of the remaining SF=10 losses (Q07, Q14)** plus a
parity→win flip (Q15) and margin on Q08 — a major fraction of the SF=10 100%-win gap,
alongside Q05 (plan/optimizer) and Q18 (radix agg). At SF=100 the decode is at a
**proven floor** (`project_sf100_decode_floor_proven`); the memo there already names the
only lever: *"writer-side codec LZ4/uncompressed — real-user feature, NOT TPC-H."* This
doc is the build-out of that one line.

## What controlling the writer buys — five knobs

1. **Codec (the headline).** Snappy → **LZ4** (≈2–3× faster decompress, similar ratio)
   or **uncompressed** (zero decompress). Scale-dependent:
   - **SF=10 (in-cache):** LZ4 or uncompressed both win — CPU-bound, IO is free.
   - **SF=100 (IO/cache-bound, WS > RAM):** uncompressed **doubles bytes read** → can
     *hurt*; LZ4 (less CPU, ~same IO) is strictly better than Snappy; **Zstd** (better
     ratio = less IO, slower decode) may win where IO dominates. The writer should expose
     the codec / auto-pick by deployment scale.

2. **Sort order.** Sorting lineitem by the common filter/group key (l_shipdate for
   Q06/Q14/Q15; l_suppkey for the Q15 agg) buys three things at once:
   - **Page-index min/max pruning** — currently **DEAD** ("shipdate random within every
     page", `project_page_index_q14_dead_end`). Sorting *is* the fix.
   - **Encoding density** — RLE/delta on sorted runs → smaller files → less IO at SF=100,
     fewer decoded rows at SF=10.
   - **Smaller dictionaries** for clustered strings.

3. **Encoding.** Delta/RLE for sorted numeric columns; tuned bit-width to our SIMD
   unpackers (bw 1..=32, NEON+AVX2 — `project_sigma_e5_small_bw_simd_landed`).

4. **Dictionary preservation.** The dict-arrival blocker
   (`project_dict_arrival_blocker`): TPC-H strings materialize at decode, so
   `EnableDictGroupCountRule` (a **2.17×** kernel) is a no-op on real data. If **we**
   write dict-encoded **and** the reader preserves it (`read_column_byte_array_dict_preserved`
   exists upstream), the 2.17× kernel finally fires (Q12 −41% shown when dict survives).

5. **Page size / column-chunk layout.** Tune page size to the decoder's ideal (pruning
   granularity vs per-page overhead); co-locate columns read together.

## Wrapper vs. native format

**Recommend: parquet-writer wrapper first.** Still valid parquet (interoperable —
DuckDB/Polars can read the same files), just *optimal encode choices*: a
`flow optimize-table` / `rewrite` pass that takes any parquet and re-emits it
sorted + LZ4 + dict-preserved + tuned pages. Low risk, immediately measurable, no
interop break. Only consider a **native format** (max control, breaks interop, multi-month)
if the wrapper provably hits a parquet-imposed ceiling. Don't pay native-format cost
on spec.

## This is a product feature, not TPC-H hardcoding

ematix-flow is an ETL product with `ManagedTable` targets — **ematix writes those tables**.
A writer that lays out parquet-backed managed tables, the decode cache
(`project_sigma_oc2_provider_landed`), and SF=100 spill files in the engine's
fastest-to-read layout is a legitimate, generalized **storage feature** —
"ematix stores your tables in the layout its engine reads fastest." It happens to also
lower the benchmark floor. This sidesteps the no-TPC-H-hardcoding rule
(`feedback_no_tpch_hardcoding`) entirely: the lever is general storage-layout
optimization, not a per-query trick.

## Fair-benchmark methodology

Rewriting the *input data's* codec changes a benchmark input, so report **both**:
- **(a) canonical Snappy files** — apples-to-apples on given data (the current bench,
  unchanged).
- **(b) ematix-optimized files** — what the full stack delivers when it controls
  storage, **with DuckDB/Polars reading the same optimized files**. If our decoder is
  better-tuned to LZ4/sorted layout, we win by more; if not, the gain is shared and
  honest. Both panels ship.

## Phased plan

### Phase 0 — kill-gate (≈1 day, pure measurement, NO new code) — DO FIRST

Two cheap measurements that gate the whole effort:

1. **Q15/Q06/Q07 SF=10 wall decomposition.** Confirm decompress is the dominant
   *movable* slice. Decompose into `Snappy-decompress | Arrow-array-build |
   predicate-eval | agg-hash-writes | scheduler-idle`. (This is also the long-promised
   "Q15 boundaries-collapsed, decode-floored" confirmation — bank it with fresh numbers.)
   - **Kill condition:** if decompress is *not* a large movable fraction (e.g. the wall
     is scheduler-idle / parallel-efficiency, which work-steal already kill-gated at
     ~10% — `project_sf100_decode_floor_proven` / PV.M.5), the codec lever is capped →
     reconsider before building.
2. **LZ4-rewrite A/B.** Rewrite lineitem to **LZ4 + sorted-by-shipdate** with the
   existing arrow/parquet tooling (no writer code needed yet), point the current reader
   at it, run strict interleaved Q06/Q14/Q15/Q07 SF=10 vs the Snappy baseline.
   - **GO condition:** LZ4-rewrite alone moves the decode-bound queries materially → the
     writer thesis is proven cheaply and we know the ceiling before committing to the
     wrapper.

### Phase 1 — `optimize-table` writer wrapper (≈1–2 weeks)

In `../ematix-parquet` (we own it; CI-published): codec selection + sort + dict-encode +
page-size tuning, emitting valid parquet. Wire a `flow optimize-table` CLI verb +
ManagedTable storage hook. Ship the (a)/(b) benchmark panels.

### Phase 2 — reader dict-preservation tie-in (≈few days, gated on Phase 1)

With us writing dict-encoded, finish reader-level dict preservation so
`EnableDictGroupCountRule` (2.17×) fires (`project_dict_arrival_blocker`,
`project_emat_dict_preserved_upstream`). Q12 −41% when dict survives.

### Deferred — native format

Only if Phase 1 hits a parquet-imposed ceiling. Months of work; do not start on spec.

## Effort / risk summary

| Phase | Effort | Risk | Payoff |
|---|---|---|---|
| 0 kill-gate | ~1 day | none (measurement) | proves/kills the thesis cheaply |
| 1 wrapper | 1–2 wk | low (valid parquet, interop kept) | Q07/Q14 losses, Q15 flip, Q08 margin |
| 2 dict | few days | low | Q12 −41% (2.17× kernel unblocked) |
| native fmt | months | high (interop break) | deferred — only if (1) ceilings |

## Open questions for the user

- Does the product already write any parquet-backed ManagedTable storage we'd hook, or
  is the writer net-new infrastructure? (Affects Phase 1 wiring.)
- Codec default policy: ship LZ4 as the ematix-write default, or auto-pick by detected
  scale (SF=10 LZ4/uncompressed vs SF=100 LZ4/Zstd)?
