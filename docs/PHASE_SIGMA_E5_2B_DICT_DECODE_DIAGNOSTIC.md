# Σ.E5.2b — dict-aware decode-gap diagnostic

**Status:** diagnostic spike — findings only, no behaviour changes.
**Branch:** `diag/ematix-dict-decode-gap` (off `main` @ `1e73301`).
**Motivation:** before falling back to parquet-rs for the dict-aware
preset, prove where ematix-parquet's dict-preserved decode is losing
time vs parquet-rs, so the fix can land upstream and EmatixFastParquet
becomes the better choice everywhere (the strategic goal is parquet-rs
elimination, not provider toggling).

**Canonical test query (`Q_DICT_COUNT`):**

```sql
SELECT l_returnflag, COUNT(*) AS n
FROM lineitem
GROUP BY l_returnflag
ORDER BY l_returnflag
```

Plus the dict-string-heavy regression query Σ.E5.4.a flagged at +111%:
the full TPC-H Q1 (`l_returnflag, l_linestatus, sum(quantity), …`).

---

## 1. Reproduction

**Hardware.** Apple Silicon (Darwin 24.1.0), 14 logical cores per
`available_parallelism()`. mimalloc allocator. SF=1 TPC-H lineitem
parquet at `examples/tpch/data/sf1/lineitem.parquet`, 6 row groups,
6,001,215 rows. `l_returnflag` is column index 8 — a 1-character
BYTE_ARRAY with 3 distinct dict values per RG.

**Trials.** 11 timed trials with 3 warmups discarded (e2e), 21 trials
with 3 warmups (codec/stages diagnostics).

**Dict cardinality.** `FastParquet+dict` reports the following columns
promoted to `Dictionary(UInt32, …)` (all-RGs-dict-encoded by the
parquet writer): `l_returnflag, l_linestatus, l_shipinstruct,
l_shipmode, l_comment`.

### 1.1 End-to-end SQL wall-clock

`cargo run --release -p ematix-flow-core --example diag_dict_decode_e2e`

| Query           | FastParquet+dict | Emat (stream default) | Emat+with_dict_preservation |
|-----------------|-----------------:|----------------------:|----------------------------:|
| `Q_DICT_COUNT`  |   11.28 ± 0.79 ms |        10.44 ± 0.17 ms |              10.84 ± 0.49 ms |
| Q1 (full)       |   24.96 ± 1.17 ms |        30.12 ± 1.48 ms |              24.46 ± 1.16 ms |

| Query           | Δ (Emat-stream / Fast) | Δ (Emat-dict / Fast) |
|-----------------|-----------------------:|---------------------:|
| `Q_DICT_COUNT`  |                  -7.4% |                -3.8% |
| Q1 (full)       |                 +20.7% |                -2.0% |

**Surprise #1.** The originally framed "dict-aware decode gap" does
not exist on `Q_DICT_COUNT` — EmatixFastParquet is **faster** than
FastParquet+dict at the canonical low-cardinality GROUP BY shape,
in *both* its modes (streaming default and explicit dict-pres).

**Surprise #2.** The Σ.E5.4.a +111% Q1 regression is **not** a
dict-decode regression. Emat's `with_dict_preservation(true)` mode
hits Q1 parity (-2.0%) with FastParquet+dict. The +20.7% regression
visible on `Emat (stream default)` is the same provider, same codec,
*different operator-level shape* (StringView output instead of
DictionaryArray output) — i.e. the gap lives at the operator
boundary, not in the codec.

---

## 2. EXPLAIN ANALYZE per-operator timing

Side-by-side scan vs aggregate(Partial) timings, from
`diag_dict_decode_e2e`. SF=1, 14 target partitions. `elapsed_compute`
is per-operator CPU summed across all worker partitions.

### 2.1 `Q_DICT_COUNT` (scan `l_returnflag` only, projection=[8])

| Operator              | FastParquet+dict | Emat (stream default) | Emat+with_dict_pres |
|-----------------------|-----------------:|----------------------:|--------------------:|
| Scan (`*Exec`)        |          7.46 ms |              33.53 ms |             19.67 ms |
| AggregateExec(Partial)|         73.08 ms |              43.75 ms |             78.41 ms |
| ↳ time_calc_group_ids |         61.86 ms |              32.08 ms |             67.25 ms |
| AggregateExec(Final)  |       113.25 µs |              26.83 µs |             36.75 µs |
| Total wall (median)   |        11.28 ms  |              10.44 ms |             10.84 ms |

**Reading.** FastParquet's scan is 4.5× faster (7.46 vs 33.53 ms) but
its AggregateExec(Partial) is 67% slower (73.08 vs 43.75 ms) — Emat's
StringView output produces faster group-id calculation than
FastParquet's DictionaryArray output here. Net wall-clock favours
Emat.

### 2.2 Q1 (projection=[4, 5, 8, 9, 10] = 5 columns inc. 3 strings)

| Operator               | FastParquet+dict | Emat+with_dict_pres |
|------------------------|-----------------:|--------------------:|
| Scan (`*Exec`)         |         90.72 ms |             55.20 ms |
| FilterExec             |         16.50 ms |              8.29 ms |
| AggregateExec(Partial) |        157.51 ms |            134.91 ms |
| ↳ time_calc_group_ids  |        106.50 ms |             89.48 ms |
| AggregateExec(Final)   |          87.4 µs |            126.5 µs  |
| Total wall (median)    |         24.96 ms |             24.46 ms |

**Reading.** Emat+dict actually *wins* on the scan (55.2 vs 90.7 ms
elapsed_compute) AND on Aggregate(Partial). Q1 parity holds when the
provider emits DictionaryArray.

### 2.3 What's missing: Emat (streaming) Q1 (+20.7%)

The streaming-default emits `StringView` (per Σ.E5.1.d Utf8View
promotion) instead of `Dictionary(UInt32, Utf8)`. On Q1 specifically
the per-row group-id computation against StringView is more expensive
than against the dict (3 distinct returnflag × 2 distinct linestatus
= 6 group keys; gather-by-dict-code beats hash-of-StringView), so
the AggregateExec(Partial) regresses. This is the Σ.E5.4.a Q1 +111%
finding — the absolute number diverges from that bench because the
fixture and rule chain differ — and it is **fixed downstream of the
codec**, in the provider's choice of output type, not in
ematix-parquet itself.

---

## 3. Codec-only isolation

`cargo run --release -p ematix-flow-core --example diag_dict_decode_codec`

Strips off DataFusion, the providers, the aggregate, and the Arrow
assembly. Decodes `l_returnflag` (column 8) across all 6 row groups
into the raw dict-preserved column structure, for each engine:

* **ematix-parquet:** `read_column_byte_array_dict_preserved` directly.
* **parquet-rs:** `ParquetRecordBatchReaderBuilder` with
  `ArrowReaderOptions::with_schema(...)` forcing
  `Dictionary(UInt32, Utf8)` on `l_returnflag` + `ProjectionMask::leaves`
  of just that column — the same code path FastParquet+dict invokes.

| Engine        | Median  | σ      | p95     | ns/row |
|---------------|--------:|-------:|--------:|-------:|
| ematix-parquet|  7.077 ms | 0.089 | 7.200 ms |  1.18 |
| parquet-rs    |  6.418 ms | 0.197 | 6.636 ms |  1.07 |
| **Δ (e / p)** |   +10.3% |       |          | +0.11 ns |

**Reading.** At the codec layer the gap is +10% / +0.11 ns/row, NOT
the +65–111% the parity bench showed for Q1/Q12/Q19. The codec layer
is essentially at parity. **The audit's "codec at parity per
`bench_decode`" assumption holds.**

This also confirms surprise #2: the Σ.E5.4.a regressions on Q1/Q12/
Q19 are *not* attributable to ematix-parquet decode time on
dict-encoded string columns. They live in the provider/aggregate
shape mismatch that surfaces only when the codec output is
`StringView` instead of `DictionaryArray`.

---

## 4. Per-stage attribution inside ematix-parquet

`cargo run --release -p ematix-flow-core --example diag_dict_decode_stages`

Wraps each stage of `read_column_byte_array_dict_preserved_into`'s
inner loop in `Instant::now()` checkpoints (using only the public
codec API — no upstream patch). Sums across all 6 RGs of
`l_returnflag`.

| Stage         | Median   | σ      | Share  |
|---------------|---------:|-------:|-------:|
| footer        | 0.023 ms | 0.002  |  0.4% |
| read_range    | 0.077 ms | 0.014  |  1.4% |
| page_walk     | 0.011 ms | 0.001  |  0.2% |
| snappy        | 0.040 ms | 0.001  |  0.7% |
| dict_plain    | 0.000 ms | 0.000  |  0.0% |
| **rle_indices** | **5.014 ms** | **0.062** | **92.5%** |
| assembly      | 0.218 ms | 0.007  |  4.0% |
| sum_stages    | 5.383 ms |        | 99.3% |
| total         | 5.423 ms |        |  100% |

**Reading.** 92.5% of ematix-parquet's dict-preserved decode wall-
clock is in `ematix_parquet_codec::dict::decode_rle_dictionary_indices`
— the RLE/bit-packed indices unpacker. Snappy is 0.7%; dict-PLAIN
decode is rounding error; metadata is 0.4%; the offsets + indices
assembly is 4%.

The fact that the codec-isolation total (7.08 ms) is higher than the
stage-loop total (5.42 ms) on the same input is consistent with the
codec function having ~30% additional cost in invariant checks (the
`indices.iter().find(|&&i| (i as usize) >= dict_len)` validation
pass) and `Vec::reserve`/`extend` ergonomics on `dict_offsets`. The
inner loop in
`read_column_byte_array_dict_preserved_into` does that index-bounds
sweep before `indices.append(...)`; that's two passes over indices
when one would suffice if the validation were fused into
`decode_rle_dictionary_indices` itself or skipped (the codec already
bounds-checks the bit-width-implied range internally on NEON).

**Where ematix-parquet's RLE decoder lives:**
`ematix-parquet/crates/ematix-parquet-codec/src/dict.rs:53`,
function `decode_rle_dictionary_indices`. With `l_returnflag`'s
dict_len = 3 the bit-width is 2 (smallest 2-bit RLE/bit-packed
encoding), so this hits the const-generic per-bit-width unpacker
dispatch.

---

## 5. Root cause

**There is no "dict-aware decode-gap" between ematix-parquet and
parquet-rs at the codec layer.** Pure dict-preserved decode is +10%
slower in ematix-parquet, well within the codec-parity band and 6×
smaller than the bench-visible regressions. The Σ.E5.4.a Q1/Q12/Q19
regressions are **not** decode-bound — they are caused by
EmatixFastParquet's streaming path emitting `StringView` where
FastParquet+dict emits `Dictionary(UInt32, Utf8)`, and DataFusion's
`AggregateExec(Partial)` is faster on the Dictionary input for the
low-cardinality TPC-H string columns.

Evidence:
* Codec-only: +10% gap (§3).
* Q_DICT_COUNT e2e: Emat wins by 7% (StringView happens to favour
  the `time_calc_group_ids` cost on this query — surprise #1).
* Q1 e2e with `with_dict_preservation(true)`: Emat at parity
  (-2.0%) — confirms the codec is not the issue and the provider's
  DictionaryArray emission closes the gap.
* Q1 e2e on streaming default: Emat +20.7% — confirms the regression
  is downstream of decode.

The within-codec 92.5%-share hot spot is the RLE indices unpacker
itself. It is **not** the source of the Σ.E5.4.a regressions, but it
*is* the right place to reach for if upstream ever wants to close
even the +10% codec-layer gap as a follow-on improvement.

---

## 6. Recommendation

**The fix lives in `crates/ematix-flow-core/src/ematix_fast_parquet.rs`,
not in `../ematix-parquet`.**

Specifically: extend Σ.E5.1.d's automatic Utf8View promotion to a
**Dictionary promotion** for columns the parquet footer already
guarantees are all-RGs-dict-encoded (the same gating
`FastParquetTableProvider::promote_dict_encoded_to_dictionary` uses
at `fast_parquet.rs:103-162`). When the streaming reader path's
`try_new` time sees all-RGs-dict columns, advertise them as
`Dictionary(UInt32, Utf8)` and emit `DictionaryArray` from
`EmatArrowBatchReader` (the dict-preserved decode is already there —
it is even today the default path for Utf8 in
`emat_arrow_reader.rs:699`). The only change is to the *output type
the column is wrapped in*, not the decode.

This:
1. Closes the Σ.E5.4.a Q1/Q12/Q19 regressions by giving the
   aggregate the DictionaryArray shape it currently relies on for
   speed (confirmed by §2.2: `Emat+with_dict_preservation` is at Q1
   parity).
2. Keeps `EnableDictGroupCountRule` from being a no-op on real TPC-H
   strings (closes the `dict-arrival-blocker` memory note: dict
   arrives at the operator surface for free).
3. Costs nothing upstream — `read_column_byte_array_dict_preserved`
   already does the work; we'd just stop *destroying* the dict
   structure on the way out by wrapping into StringView.

**Effort estimate.** ~2–3 days end-to-end:

* 0.5 d: schema-promotion logic mirroring
  `promote_dict_encoded_to_dictionary` in
  `EmatixFastParquetTableProvider::try_new` (with an opt-in flag,
  default-off initially).
* 0.5 d: branch in
  `emat_arrow_reader::decode_byte_array_to_string_view` (rename or
  branch sibling) to emit `DictionaryArray` when the advertised type
  is Dictionary — the data is already in
  `DictPreservedColumn` shape.
* 0.5 d: per-RG `dict_offsets` consistency check (parquet writers
  can emit different dict orderings per row group; either rebuild a
  unified dict at provider time or emit one per-batch and let
  DataFusion's `concat_batches` handle merging — Σ.E3 already covers
  this case via `dict-arrival-blocker`).
* 0.5 d: tests + gate via the existing `tpch_q1_e2e_gate.rs` and
  `dict_group_count_bench.rs`.
* 0.5 d: re-run `sigma_e5_4a_22_parity_bench` and freeze findings.

**Out of scope for this fix (separate, optional follow-up):** the
+10% codec-layer gap. If we ever want to close that, the lever is
`decode_rle_dictionary_indices` (`../ematix-parquet/crates/ematix-
parquet-codec/src/dict.rs:53`) — but it would close only ~10% of a
2 ms savings per scan, so it's far below the operator-side fix in
priority.

---

## 7. Files added / modified

**New (this branch):**

* `docs/PHASE_SIGMA_E5_2B_DICT_DECODE_DIAGNOSTIC.md` — this doc.
* `crates/ematix-flow-core/examples/diag_dict_decode_e2e.rs` — e2e
  SQL wall-clock + EXPLAIN ANALYZE harness.
* `crates/ematix-flow-core/examples/diag_dict_decode_codec.rs` —
  codec-only isolation across both engines.
* `crates/ematix-flow-core/examples/diag_dict_decode_stages.rs` —
  per-stage attribution inside ematix-parquet's dict-preserved
  decode.

**Modified:**

* `crates/ematix-flow-core/Cargo.toml` — registers the three
  `[[example]]` entries above.

**Not modified (constraint):** no provider, no codec, no upstream
ematix-parquet source. This is observational only — the fix is a
separate PR informed by §6.

---

## 8. Reproduction commands

```sh
# Pre-req: SF=1 TPC-H parquet under examples/tpch/data/sf1/
# (override with TPCH_DATA_DIR=...)

cargo run --release -p ematix-flow-core --example diag_dict_decode_codec
cargo run --release -p ematix-flow-core --example diag_dict_decode_stages
cargo run --release -p ematix-flow-core --example diag_dict_decode_e2e
```

All three are deterministic to within noise on a quiescent host (~5%
σ on the e2e, <2% σ on the codec/stages diagnostics — fewer threads
contending for cores at the codec level).
