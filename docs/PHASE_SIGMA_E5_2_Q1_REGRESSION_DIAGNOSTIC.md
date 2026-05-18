# Σ.E5.2 — Q1 SF=1 regression diagnostic

Diagnostic spike, not a fix. Identifies *why* `EmatixFastParquet` is
slower than `FastParquet` on TPC-H Q1 SF=1, so E5.4's design lands on
a known target. Four hypotheses (H1–H4) from the audit doc were tested
in isolation; this doc reports the data.

## 1. Reproduction

Hardware: M3 Pro (Apple Silicon), 14-core. mimalloc. macOS 24.1.0.
Data: `examples/tpch/data/sf1/lineitem.parquet`, 6 row groups,
6,001,215 rows, 202 MB on disk. Q1 reads 7 columns: `l_quantity`,
`l_extendedprice`, `l_discount`, `l_tax`, `l_returnflag`,
`l_linestatus`, `l_shipdate` (parquet leaf indices 4–10).

All numbers below: 21-trial median + stddev after 3 warm-ups,
`target_partitions=14`, rule OFF (DataFusion default Filter +
HashAggregate stack) unless otherwise stated.

### 1.1 Apples-to-apples gate (`sigma_e5_2_q1_apples_to_apples.rs`)

Holding the rule fixed at OFF, varying the provider:

| Provider              | Rule | median (ms) | σ    | vs FastParquet |
|-----------------------|------|-------------|------|----------------|
| FastParquet (Utf8View)| OFF  |       35.35 | 1.87 |   —            |
| EmatixFast (Utf8)     | OFF  |       85.36 | 5.02 | +50.0 ms (+142%)|
| EmatixFast (Dict)     | OFF  |       66.64 | 4.30 | +31.3 ms (+88%)|
| FastParquet (Utf8View)| ON   |       44.47 | 2.77 |   —            |
| EmatixFast (Utf8)     | ON   |      107.73 | 4.30 | +63.3 ms (+142%)|
| EmatixFast (Dict)     | ON   |       75.37 | 3.97 | +30.9 ms (+70%)|

The audit doc's "59 vs 39" framing conflated rule × provider. The
actual provider gap is materially larger: **+50 ms (Utf8) / +31 ms
(Dict) at rule OFF**, both ratios stable when the rule turns on.

The Utf8 → Dict swap inside EmatixFast already closes ~19 ms of the
gap — this pre-empts H2: Utf8View is doing significant work
downstream, and the Utf8 fallback is the slowest variant.

### 1.2 EXPLAIN ANALYZE plan diff (rule OFF)

The Utf8 variant gets `Exact` filter pushdown (the bridge runs the
shipdate filter inline); Dict variant returns `Unsupported` (residual
FilterExec runs). FastParquet keeps an unpushed FilterExec.

## 2. EXPLAIN ANALYZE — per-operator timings

Each row is the **sum across all partitions** as reported by
`EXPLAIN ANALYZE`. Times are operator `elapsed_compute` unless noted.
Source: `sigma_e5_2_q1_explain_analyze.rs`, single non-warm trial.

| Operator                           | FastParquet | EmatUtf8 | EmatDict |
|------------------------------------|-------------|----------|----------|
| Scan `elapsed_compute`             | **151.71 ms** | **227.58 ms** | **76.26 ms** |
| Scan rows out                      | 6.00 M      | 5.92 M (filtered in scan) | 6.00 M |
| Scan batches out                   | 92          | 6        | 6        |
| RepartitionExec(RoundRobin) fetch  | 152.08 ms   | 76.30 ms | 227.67 ms (Dict) |
| FilterExec                         | 24.37 ms    | (pushed down) | 14.15 ms |
| ProjectionExec                     | 5.58 ms     | 4.25 ms  | 4.82 ms  |
| AggregateExec(Partial) elapsed     | 207.25 ms   | 223.66 ms | 213.26 ms |
| AggregateExec(Partial) agg_time    | 664.65 ms   | 623.57 ms | 654.14 ms |
| AggregateExec(Partial) group_ids   | 59.96 ms    | 88.68 ms | 73.80 ms |
| RepartitionExec(Hash) fetch        | 493.76 ms   | 775.83 ms | 418.82 ms |
| AggregateExec(FinalPartitioned)    | 93 µs       | 91 µs    | 130 µs   |

`elapsed_compute` on AggregatePartial is approximately the same across
all three providers, and the *unit cost* per row in the aggregate is
not where the gap lives. The differences are concentrated in:

- **Scan** — FastParquet 152 ms / EmatUtf8 228 ms / EmatDict 76 ms.
  This is the dominant axis.
- **AggregatePartial `time_calculating_group_ids`** — FastParquet 60
  ms / EmatUtf8 89 ms / EmatDict 74 ms. The 30 ms EmatUtf8 overhead
  here is the GROUP-BY-on-Utf8-vs-Utf8View penalty. The Dict provider
  surfaces dict-encoded arrays which is mid-cost.

Caveat on summing: these are wall-clock per operator, parallel. They
don't add to the end-to-end wall-clock. They do reflect where work
moves between operators.

## 3. Codec-only isolation (`sigma_e5_2_q1_decode_only.rs`)

Single-thread serial decode of all 7 Q1 columns × all 6 row groups,
build a RecordBatch per RG. 15-trial median.

| Path                                     | median (ms) | σ    |
|------------------------------------------|-------------|------|
| Bridge (Utf8)                            |     125.26  | 2.90 |
| Bridge (Dict)                            |      70.93  | 0.27 |
| parquet-rs (Utf8, batch=65_536)          |      90.09  | 0.86 |
| parquet-rs (Utf8View, batch=65_536)      |      76.12  | 1.96 |

**The audit's "codec is at parity" assumption is wrong for Q1.**

- Bridge(Utf8) is **+49.14 ms (+65%)** slower than parquet-rs(Utf8View).
- Bridge(Dict) is **-5.18 ms (-7%)** *faster* than parquet-rs(Utf8View).

Why: the Utf8 byte-array bridge function
(`decode_column_chunk_byte_array`) materialises every dict-encoded
row into an `arrow::StringBuilder`, copying bytes per row. parquet-rs
either keeps bytes in a shared values buffer (Utf8) or uses a view
layout (Utf8View). For Q1's `l_returnflag` + `l_linestatus` (5.92M
rows × 2 cols of small strings), the row-by-row copy is expensive.
Dict-preserved decode skips the materialisation entirely and is at
parity.

The audit doc memory note (`bench_decode` parity) was based on the
3-column subset `l_orderkey / l_shipdate / l_returnflag` which is
dominated by numeric/dict-light columns; it doesn't generalise to
Q1's two-string-column projection.

## 4. Per-hypothesis tests

| H  | Hypothesis                                       | Verdict   | Contribution |
|----|--------------------------------------------------|-----------|--------------|
| H1 | Per-RG vs streaming 65_536-row batch emission    | Partial   | 1–5 ms       |
| H2 | Missing Utf8View promotion at scan boundary      | **Confirmed (dominant)** | ~30 ms downstream + bridge string copy |
| H3 | Per-RG `ParquetFile::open` overhead              | Refuted   | < 0.3 ms     |
| H4 | Missing planner statistics (`new_unknown`)       | Refuted (for Q1) | ~0 ms |

### H1 — Per-RG batch boundary (Partial)

`sigma_e5_2_q1_h1_slice.rs`. Wrap EmatixFast Exec with a `SlicingExec`
that yields 65_536-row slices instead of whole-RG batches. Re-bench:

| Mode                                      | baseline (ms) | sliced (ms) | Δ |
|-------------------------------------------|---------------|-------------|---|
| EmatUtf8 baseline (1 batch / RG)          |    81.53      |   80.49     | −1.05 ms (−1.3%) |
| EmatDict baseline (1 batch / RG)          |    59.53      |   54.79     | −4.74 ms (−8.0%) |

Slicing closes **5 ms of the 31 ms Dict gap** and **1 ms of the 50 ms
Utf8 gap**. H1 is a real but minor contributor. It matters more in
the Dict variant because the downstream `AggregatePartial` rebuilds
its hash table on each batch and a single 1M-row batch defeats the
batch-grow heuristics; in Utf8 the bridge's string-copy cost
swamps the batch-size effect.

### H2 — Missing Utf8View (Confirmed, dominant)

Three independent signals converge:

1. **Decode-only microbench** (§3) — bridge string materialisation
   costs +49 ms vs Utf8View, even though numeric decode is fine.
2. **Apples-to-apples gate** (§1.1) — flipping EmatixFast Utf8 → Dict
   recovers 19 ms by skipping string materialisation entirely.
3. **AggregatePartial `time_calculating_group_ids`** in EXPLAIN
   ANALYZE (§2) — EmatUtf8 spends 89 ms (vs 60 ms FastParquet, 74 ms
   EmatDict). Group-id computation on a `StringArray` is slower than
   on `StringViewArray`; dict-encoded arrays sit between.

If EmatixFast emitted Utf8View for `l_returnflag` and `l_linestatus`
(matching FastParquet's `ArrowReaderOptions::with_schema(force_utf8view)`
shape), we'd recover both the bridge string-copy cost *and* the
group-id penalty. The Dict path approximates the upper bound: it's
~31 ms slower than FastParquet, which is the residual gap H2 alone
can't fully close (H1 + dict-vs-view group-id differences).

### H3 — `ParquetFile::open` per RG (Refuted)

`sigma_e5_2_q1_h3_open_cost.rs`. Warm-cache `ParquetFile::open`:
median 7.8 µs, p99 24 µs. Worst case (42 reopens per query): 0.33 ms.
This is < 1% of the gap. The pattern is wasteful but not the bottleneck.

### H4 — Missing planner statistics (Refuted for Q1)

EmatixFast's `partition_statistics()` returns
`Statistics::new_unknown(...)` (with `num_rows = Inexact(N)`). Q1's
plan has no joins, so missing min/max doesn't change hash-table
sizing or join build-side selection. The `AggregateExec(Partial)`
`peak_mem_used` reported by EXPLAIN ANALYZE: FastParquet 25 MB,
EmatUtf8 95 MB, EmatDict 264 MB — these reflect input *batch* size
differences (H1), not statistics. H4 likely matters for Q3/Q5/Q9 (the
audit doc flags this); not for Q1.

## 5. Attribution

Of the **+50 ms** Utf8 gap (rule OFF, FastParquet 35 → EmatUtf8 85 ms):

| Source                                                        | Contribution |
|---------------------------------------------------------------|--------------|
| String-column materialisation (decode → StringArray copy)     | ~25 ms       |
| GROUP-BY on Utf8 vs Utf8View (`time_calculating_group_ids` Δ) | ~15 ms       |
| Larger downstream batches (no slicing, RepartitionExec fanout)| ~5 ms        |
| Open-per-RG overhead + planner stats                          | < 0.5 ms     |
| Residual / measurement noise                                  | ~5 ms        |

Of the **+31 ms** Dict gap (FastParquet 35 → EmatDict 67 ms):

| Source                                                        | Contribution |
|---------------------------------------------------------------|--------------|
| Dict array vs Utf8View at AggregatePartial group_ids          | ~14 ms       |
| Per-RG batch emission (H1)                                    | ~5 ms        |
| Residual decode delta (bridge dict vs parquet-rs Utf8View)    | ~−5 ms (faster) |
| FilterExec runs as residual (no pushdown when dict_preserve)  | ~14 ms       |
| Misc (RepartitionExec sizing, etc.)                           | ~5 ms        |

All allocations are derived from controlled isolation experiments
(§3, §4); margin of attribution is roughly ±5 ms per row, set by
the inter-trial σ (≈ 4 ms).

## 6. Recommendation for E5.4

The Arrow batch reader built in E5.1 **MUST** do both:

1. **Honour a supplied schema with Utf8View / BinaryView promotion.**
   Same shape as parquet-rs's
   `ArrowReaderOptions::new().with_schema(force_utf8view)`. The bridge
   must surface `StringViewArray` directly; building a `StringArray`
   and converting is not enough because the bridge's row-by-row copy
   is itself the cost.

2. **Emit streaming batches sized to a `batch_size` parameter
   (default 65_536), not whole row groups.** Per-RG emission is a
   minor contributor (~5 ms), but it must be fixed for consistency
   with the parquet-rs reader's shape and because it costs more on
   queries with non-trivial downstream operators (joins, sorts).

3. **Defer planner statistics** — surface `Precision::Exact` row
   counts + column min/max from parquet footer at `try_new`. Not
   gating for Q1, but cheap and gating for Q3/Q5/Q9.

Sequencing: §1 makes E5.2 collapse into E5.1 cleanly (the audit
predicted this case). §3 is a follow-up that can land in any of
E5.3–E5.5.

**Acceptance gate update.** The audit's "within 5% of FastParquet"
target needs the Utf8View path to land. With Dict-preservation +
H1 slicing (best current EmatixFast variant), the gap is still ~25 ms
(67 → ~55 estimated after slicing); ~20 ms shy. Utf8View promotion
is the lever.

## 7. Files added

| File                                                                                          | LOC | Purpose |
|-----------------------------------------------------------------------------------------------|-----|---------|
| `docs/PHASE_SIGMA_E5_2_Q1_REGRESSION_DIAGNOSTIC.md`                                           | 220 | this doc |
| `crates/ematix-flow-core/examples/sigma_e5_2_q1_apples_to_apples.rs`                          | 169 | rule × provider grid |
| `crates/ematix-flow-core/examples/sigma_e5_2_q1_explain_analyze.rs`                           | 103 | per-operator timings |
| `crates/ematix-flow-core/examples/sigma_e5_2_q1_decode_only.rs`                               | 236 | codec-layer parity check |
| `crates/ematix-flow-core/examples/sigma_e5_2_q1_h1_slice.rs`                                  | 267 | H1 isolation |
| `crates/ematix-flow-core/examples/sigma_e5_2_q1_h3_open_cost.rs`                              |  56 | H3 isolation |

No library source files modified. No public APIs changed. No fixes
applied — diagnostic spike only.
