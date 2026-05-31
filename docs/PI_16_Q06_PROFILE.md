# Π.16 — Q06 SF=10 profile findings (2026-05-26)

Profile-led investigation of the 14-18 ms gap to Polars on Q06 SF=10
(ematix 77.40 ms ± 5.40 vs Polars 62.50 ms ± 5.54 vs DuckDB 70.88 ±
1.34 per the 20-trial canonical bench in `docs/SIGMA_AC_REBENCH.md`).

## Tooling

- `crates/ematix-flow-core/examples/q06_profile_loop.rs` — runs Q06 in
  a tight loop (default 200 reps × 5 warmups, ~18 sec total), suitable
  for `samply record`.
- `/tmp/analyze_samply.py` — raw frame counter (no symbol resolution).
- `/tmp/symbolicate.py` — combines profile + `.syms.json` sidecar to
  print hot functions with full Rust mangled names.

Reproduce:

```
CARGO_PROFILE_RELEASE_DEBUG=1 CARGO_PROFILE_RELEASE_STRIP=false \
  cargo build --release -p ematix-flow-core \
    --example q06_profile_loop --features triangulation

REPS=200 WARMUPS=5 TPCH_DATA_DIR=examples/tpch/data/sf10 \
  samply record -s --unstable-presymbolicate \
    -o /tmp/q06_profile_sym.json \
    ./target/release/examples/q06_profile_loop

python3 /tmp/symbolicate.py
```

## Findings

### Top 12 hot functions (self time, 200-rep profile)

| % | function |
|---|----------|
| 28.2 | `__psynch_cvwait` (tokio worker idle — not real work) |
| 18.7 | **`snap::decompress::Decoder::decompress`** |
| 11.5 | `ematix_parquet_codec::read::read_column_f64_masked_into` |
| 6.6  | `ematix_parquet_codec::read::read_column_i32_masked_into` |
| 5.6  | `madvise` |
| 4.2  | `ematix_parquet_codec::dict::unpack_8_indices` |
| 4.0  | `pread` |
| 2.9  | `__bzero` |
| 2.6  | `ematix_parquet_codec::plain::plain_sparse_decode_f64_into` |
| 1.9  | `_platform_memmove` |
| 1.0  | `ematix_parquet_format::compact::read_uvarint` |
| 1.0  | `ematix_parquet_codec::bitpack_neon::decode_predicate_bitmap_neon_bw12` |

### Top in-stack callers (total time)

- 47.6% `read_column_f64_masked_into` → wraps the f64 read path
- 19.9% `decompress_snappy_into` (parent of Snappy itself)
- 17.1% `data_page_view` (per-page decode dispatcher)
- 11.8% `read_column_i32_masked_into`
- 10.7% `read_chunk_raw` (raw bytes IO)

### Diagnosis

**Snappy itself accounts for 18.7% of profile self time.** This is the
largest hot spot. Per the Π.16 v1 probe (`SIGMA_AC_REBENCH.md`), Polars
uses literally the same `snap::raw::Decoder` we do — so the gap isn't
in *how* we call Snappy; it's in *how often* we call it relative to
Polars.

**Root cause — `l_discount` is decompressed TWICE.** Q06 SQL:

```
WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
  AND l_discount BETWEEN 0.05 AND 0.07
  AND l_quantity < 24
SELECT SUM(l_extendedprice * l_discount)
```

Current path:
1. **BridgeFilter::build_bitmap** decodes l_shipdate, l_discount, and
   l_quantity dense, evaluates each predicate, AND-combines into a
   bitmap.
2. **masked_decode_one_column** then re-reads `l_discount` and
   `l_extendedprice` using the bitmap (sparse decode).

`l_discount` is in BOTH the WHERE and the SELECT, so it goes through
the full per-page Snappy decompression twice. At SF=10's 58 row groups
× ~14 pages per column, that's ~812 extra Snappy decompressions per
query.

Conservative budget for the redundant l_discount decompression:
~25% of column Snappy cost × 18.7% Snappy self time = **~4.7% of
profile = ~4.3 ms of 91 ms**. Closes ~1/4 of the 14-18 ms gap to
Polars.

The remaining 10-13 ms gap likely lives in:
- `__bzero` (2.9% — over-zeroing buffers we then overwrite)
- `madvise`/`pread` (9.6% combined — could be reduced via mmap if
  Polars uses mmap and we use pread)
- `unpack_8_indices` (4.2% — bit-unpack hot loop micro-opt potential)

## Implementation plan (next session)

### Sub-task 1 — eliminate l_discount double-decompress (highest ROI)

**Estimated payoff:** ~4 ms / 18 ms gap (≈ 1/4 of gap closed).
**Estimated effort:** 4-6 hours.

Changes needed:

1. **ematix-flow-core** `BridgeFilter::build_bitmap`:
   - Replace return type `DfResult<(Vec<u8>, usize)>` with
     `DfResult<(Vec<u8>, usize, HashMap<usize, DecodedColumn>)>` where
     `DecodedColumn` is a typed enum holding the full dense decode of
     each predicate column.
   - For F64Range predicates, retain the decoded f64 vector instead of
     consuming it in-place.

2. **ematix-flow-core** `EmatArrowBatchReader::load_row_group_masked`:
   - Accept the optional decoded-column map.
   - When projecting a column that was already decoded for filtering,
     apply the bitmap to the cached column (cheap byte-stride extract)
     rather than re-reading via `masked_decode_one_column`.

3. **Correctness gate:** `tpch_validate` at SF=1+SF=10 with the new
   path. Q06 result must match DuckDB to machine epsilon (relative
   error < 1e-9 — Q06 sums ~123 billion).

4. **Bench gate:** standalone `q06_profile_loop` 200-rep median should
   drop measurably; full 22q SF=10 bench should not regress (any
   query that goes through BridgeFilter inherits the change).

### Sub-task 2 — `__bzero` audit (~2.9% self time)

Search for `Vec::resize(N, 0)` or `vec![0u8; N]` patterns on hot
buffers in `read_column_f64_masked_into` and `read_column_i32_masked_into`.
Each one is a `__bzero` of a buffer we then overwrite with decode
output. Replace with `Vec::with_capacity(N)` + manual `set_len()` after
fill (unsafe), OR use `MaybeUninit<f64>` slabs.

**Estimated payoff:** ~2 ms / 18 ms gap.
**Estimated effort:** 2-3 hours + careful audit (unsafe code).

### Sub-task 3 — investigate `madvise` source

5.6% self time in `madvise` is suspicious — that's a syscall on the
hot path. Trace which call site (`mmap()`-related?
`std::alloc::System` returning pages?). If we can amortize or
eliminate, could save another 1-3 ms.

### Sub-task 4 — micro-opt `unpack_8_indices` (4.2% self)

Already has NEON SIMD variants (`bitpack_neon::decode_*_neon_bw*`).
The 4.2% in `unpack_8_indices` suggests we hit the scalar fallback
for some bit-widths. Audit which bit-widths actually appear in Q06
columns and add NEON dispatch for the missing ones.

**Estimated payoff:** ~1 ms.
**Estimated effort:** 1-2 hours.

## Total expected wins if all subtasks land

Sub-task 1 + 2 + 3 = ~7-9 ms of the 14-18 ms gap. Closes most of the
distance. Sub-task 4 is gravy.

## Why we're not implementing now

- Multi-repo change (ematix-flow-core API + ematix-parquet kernels)
- Touches BridgeFilter — wired into many queries beyond Q06
- Regression risk on every BridgeFilter-using query at every SF
- Requires a fresh `tpch_validate` + 20-trial bench cycle
- The 14-18 ms is below the 5-trial bench noise floor in the
  triangulation harness; we'd need 20-trial gates for every iteration

Documented here so the next session can pick it up cold with the
profile findings already in hand.
