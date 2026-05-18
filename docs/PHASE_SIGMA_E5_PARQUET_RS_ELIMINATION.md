# Σ.E5 — parquet-rs Elimination Audit

**Status:** Draft (audit only, no migration yet)
**Date:** 2026-05-18
**Scope:** Remove the direct `parquet = { workspace = true }` dependency from
`crates/ematix-flow-core/`. Replace every direct `parquet::*` / `datafusion::
parquet::*` call site with the sibling `ematix-parquet-*` crates (v0.10.x).

**Out of scope:** DataFusion's transitive parquet-rs dependency. DataFusion
re-exports `parquet` through `datafusion::parquet`; even after E5 lands the
parquet crate will still be present in the dep graph via DataFusion. The
goal is to make ematix-flow-core's *direct* surface zero — so we can pin
versions independently, swap codecs without DF coordination, and so the
"hand-rolled" promise of ematix-parquet is reflected in our Cargo manifest.

**Pin versions today:**
- Workspace: `parquet = { version = "58", features = ["arrow", "async", "object_store"] }`
- ematix-parquet: `ematix-parquet-codec / -format / -io = "0.10.0"`
- ematix-parquet-async exists at v0.10 but is *not yet a dep* of flow-core.

---

## 1. Inventory: every parquet-rs API surface used in `ematix-flow-core`

Five direct consumer files. The hot-path column tags whether the API is in
a query-time decode loop (`fast_parquet`, `ematix_fast_parquet`,
`objectstore_backend.read_arrow_stream`) vs. setup-only (provider
construction) vs. test-only.

| File | parquet-rs API | Line | Purpose | Hot-path? |
|------|----------------|------|---------|-----------|
| `fast_parquet.rs` | `datafusion::parquet::arrow::ProjectionMask` (`::leaves`) | 51, 1072 | Project leaf columns at decode | Yes |
| `fast_parquet.rs` | `arrow_reader::ArrowReaderMetadata` (`::try_new`, `.schema()`) | 52–54, 494, 559–566, 612–623, 832, 1071 | Cached parquet metadata + supplied Arrow schema hint; cloned into every partition worker | Yes (shared via Arc) |
| `fast_parquet.rs` | `arrow_reader::ArrowReaderOptions` (`::new`, `.with_page_index_policy`, `.with_size_stats_policy`, `.with_encoding_stats_policy`, `.with_schema`) | 52–54, 525–528, 559, 612 | Footer-parse tuning + Utf8View / dict-preservation schema hint | Setup |
| `fast_parquet.rs` | `arrow_reader::ParquetRecordBatchReaderBuilder` (`::try_new_with_options`, `::new_with_metadata`, `.with_projection`, `.with_row_groups`, `.with_batch_size`, `.build`, `.schema`, `.metadata`, `.parquet_schema`) | 53, 529, 1070–1077 | The core Arrow batch reader builder used by every partition | **Yes — primary** |
| `fast_parquet.rs` | `schema::types::SchemaDescriptor` | 55, 482, 552, 796 | Leaf-index input for `ProjectionMask::leaves` | Setup |
| `fast_parquet.rs` | `file::metadata::ParquetMetaData` (`::num_row_groups`, `.row_group`, `.column`, `.encodings`, `.statistics`, `.file_metadata().num_rows()`) | 120, 145–148, 185, 433, 487, 547–554, 1797 | Row-group pruning, dict-encoding detection, file-level stats | Setup + plan time |
| `fast_parquet.rs` | `file::metadata::PageIndexPolicy`, `ParquetStatisticsPolicy` | 524 | Skip page-index + size/encoding stats at footer-parse to cut open cost | Setup |
| `fast_parquet.rs` | `basic::Encoding` (`PLAIN_DICTIONARY`, `RLE_DICTIONARY`) | 123, 148 | Detect dict-encoded RGs for Σ.E3b dict-preservation | Setup |
| `fast_parquet.rs` | `file::statistics::Statistics` (`Int32`, `Int64`, `Float`, `Double`, `.min_opt`, `.max_opt`, `.null_count_opt`) | 189, 222–262, 397–425, 1797–1800 | Row-group pruning + `partition_statistics()` | Plan time |
| `ematix_fast_parquet.rs` | `arrow_reader::ArrowReaderMetadata` (`::load`, `.schema`) | 47, 209–215 | Borrow parquet-rs Arrow-schema derivation from parquet schema | Setup |
| `ematix_fast_parquet.rs` | `arrow_reader::ArrowReaderOptions::new` | 47, 209 | Default options bundle | Setup |
| `ematix_fast_parquet.rs` | `file::reader::SerializedFileReader::new`, `FileReader::metadata`, `.file_metadata().num_rows`, `.num_row_groups` | 48, 235–247 | Footer parse → num_rows / num_row_groups | Setup |
| `ematix_fast_parquet.rs` | (write path, behind `#[cfg(test)]`) `basic::{Compression, Repetition, Type}`, `column::writer::ColumnWriter`, `file::properties::WriterProperties`, `file::writer::SerializedFileWriter`, `schema::types::Type`, `basic::ConvertedType`, `data_type::ByteArray`, `column::reader::ColumnReader` | 906–1242 | Test-fixture parquet writes (Date32-as-Int32 logical-Date, REQUIRED columns) and oracle reads | **Test only** |
| `ematix_parquet_bridge.rs` | (test-only) `file::reader::SerializedFileReader`, `FileReader`, `column::reader::ColumnReader`, `data_type::ByteArray` | 699–763 | Reference decoder for oracle tests against the bridge | **Test only** |
| `objectstore_backend.rs` | `arrow::AsyncArrowWriter::{try_new, write, close}` | 37, 332–349 | Stream-write Arrow batches → parquet over object_store::BufWriter | **Yes — write side** |
| `objectstore_backend.rs` | `arrow::async_reader::ParquetObjectReader::{new, with_file_size}` | 38, 285 | Wrap an ObjectStore + path into an async parquet reader source | **Yes — read side** |
| `objectstore_backend.rs` | `arrow::async_reader::ParquetRecordBatchStreamBuilder::{new, build}` | 39, 286–291 | Build the streaming Arrow batch reader | **Yes — read side** |
| `objectstore_backend.rs` | `file::properties::WriterProperties::{builder, set_compression, build}` | 40, 319–323 | Configure write-side compression | Setup |
| `objectstore_backend.rs` | `basic::{Compression, GzipLevel, ZstdLevel}` | 506–512 | Map our narrow `ParquetCompression` enum onto parquet-rs's wide one | Setup |
| `test_support.rs` | `arrow::ArrowWriter::{try_new, write, close}` | 36, 106–108 | Synthetic TPC-H mini-fixture parquet writes | **Test only** |
| `test_support.rs` | `basic::Compression::SNAPPY`, `file::properties::WriterProperties::builder().set_compression().set_max_row_group_row_count()` | 37–38, 102–105 | Force multi-row-group fixtures with Snappy | **Test only** |

**Surface summary:**
- 2 hot-path call sites: `fast_parquet.rs` (sync local-file Arrow batch
  reader, the workhorse `TableProvider` for TPC-H benches) and
  `objectstore_backend.rs` (async object-store read + write for the cloud
  Backend).
- 1 setup-only site: `ematix_fast_parquet.rs::try_new` uses parquet-rs to
  derive an Arrow schema from the parquet schema; the actual decode goes
  through `ematix_parquet_bridge` already.
- 3 test-only sites: synthetic fixture writes and bridge-oracle reads.
- 5 read-side APIs total (`ArrowReaderMetadata`, `ArrowReaderOptions`,
  `ParquetRecordBatchReaderBuilder`, `ParquetObjectReader`,
  `ParquetRecordBatchStreamBuilder`) and 3 write-side
  (`ArrowWriter` sync, `AsyncArrowWriter`, `WriterProperties` +
  `Compression`/`GzipLevel`/`ZstdLevel`).

---

## 2. Inventory: `ematix-parquet`'s current public API surface

Five sub-crates. Versions: all on `0.10.x` (the v1.0 cut criteria — every
parquet shape covered — are met per upstream README).

### 2.1 `ematix-parquet-format` (no deps beyond `std`)

**Exports:** `compact::{Cursor, ...}`, `compact_writer`, `metadata`,
`metadata_writer`, `types::{ThriftEnum, CompressionCodec, Encoding,
PageType, ...}`.

**Coverage:**
- All parquet thrift compact-protocol primitives, read + write.
- All metadata structs: `Statistics`, `KeyValue`, `DataPageHeader`,
  `DataPageHeaderV2`, `PageHeader`, `DictionaryPageHeader`,
  `DecimalType`, `IntType`, `TimeUnit`, `TimestampType`, `TimeType`,
  `VariantType`, `LogicalType`, `SortingColumn`, `RowGroup`,
  `SchemaElement`, `PageLocation`, `OffsetIndex`, `ColumnIndex`,
  `ColumnOrder`, `FileMetaData`, `PageEncodingStats`,
  `SizeStatistics`, `BloomFilterHeader/Algorithm/Hash/Compression`,
  `ColumnMetaData`, `ColumnChunk`, plus the encryption structs
  (`AesGcmV1`, `AesGcmCtrV1`, `EncryptionAlgorithm`,
  `EncryptionWithColumnKey`, `ColumnCryptoMetaData`,
  `FileCryptoMetaData`).
- Read fns for every struct above (`read_*`).

**Future work flagged:** none material; format is settled.

### 2.2 `ematix-parquet-io` (depends on `format`)

**Exports:** `ParquetFile`, `PageWalker`, `IoError`, `Result`.

**Coverage:**
- `ParquetFile::open(path) -> Result<Self>` — opens local file, parses
  footer, caches `FileMetaData`. Includes pread-based unlocked
  `read_range` so parallel workers don't serialise on the file handle
  (v0.10 addition).
- `PageWalker::new(&chunk_bytes)` — iterator over
  `(PageHeader, body_bytes)` pairs in a column chunk.

**Future work flagged:** none material; `ParquetFile` is the sync entry
point shape.

### 2.3 `ematix-parquet-codec` (depends on `format` + `io`)

**Exports:** large surface — see `read.rs` + `write.rs` summary below.

**High-level read façade (`read.rs`):**
- `read_column_i64 / i32 / f64 (file, rg, col) -> Vec<T>` + `_into(&mut Vec<T>)`
  reuse-buffer siblings.
- `read_column_byte_array(...) -> Vec<Vec<u8>>` + `_into`.
- `read_column_byte_array_offsets(...) -> (bytes, offsets)` — Arrow-style.
- `read_column_int96`, `read_column_flba` (FIXED_LEN_BYTE_ARRAY).
- **Late-mat (v0.3 / Π.10):** `read_column_{i32,i64,f64,byte_array,byte_array_offsets}_masked_into(file, rg, col, &mask, &mut out)`
  + `build_packed_mask(n, pred)`.
- **Dict-preserved (v0.7 — the Σ.E3b enabler):**
  `read_column_byte_array_dict_preserved` → `DictPreservedColumn`
  (bytes + offsets + index-vec), `..._u8` for bw ≤ 8.
- **Adaptive predicate dispatch (v0.8):**
  `read_column_{i32,i64,f64,byte_array}_predicate_adaptive` — probes
  first N pages, emits bitmap vs values per chunk.
- **Page-index range pruning:**
  `read_column_i64_with_range(file, rg, col, lo, hi)` + i32 sibling,
  plus `_into` shapes.
- **Streaming batched decode (v0.2):**
  `read_column_{i32,i64,f64,byte_array}_batches(file, rg, col, batch_size)`
  → iterator of `Vec<T>` / `(bytes, offsets)`.

**Mid-level decoders (each `pub`):**
- `plain::{decode_plain_i32, ..., decode_plain_byte_array, plain_sparse_decode_*_into}`
- `dict::{build_dict_predicate_mask, decode_rle_dictionary_into,
  decode_rle_dictionary_predicate_bitmap, gather_dict_at_bitmap_into}`
- `compression::{decompress_snappy_into, decompress_zstd_into,
  decompress_gzip_into, decompress_brotli_into, decompress_lz4_raw_into}`
- `delta` (DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY).
- `byte_stream_split`.
- `rle`, `levels`, `bitpack` (+ NEON / AVX2 specialisations).
- `bloom` (Split-Block + XXHash64 — decoder; writer also lands in v0.10).
- `page_index::{select_pages_overlapping_i32, select_pages_overlapping_i64}`.
- `parallel::*` (feature `parallel`): `read_columns_parallel(file, &targets, opts, decode_one)` + Linux NUMA submodule.
- `encrypted::*` (feature `encryption`): full AES-GCM PME read + write.

**Write façade (`write.rs`):**
- Single-column: `write_{i64,i32,f64,bool,byte_array}_column_to_path` +
  `_with_codec` + `_with_bloom_to_path` + `_dict_to_path` +
  `_dict_with_bloom_to_path` siblings (all five codecs).
- Multi-column: `write_table_to_path(path, &cols, codec)`,
  `write_table_with_row_group_size`, `write_table_to_path_v2` (V2 page
  headers), `write_table_with_blooms_to_path`,
  `write_table_with_dict_to_path`, `write_table_with_dict_and_blooms_to_path`,
  `write_table_with_options_to_path(path, &cols, WriteOptions{...})` —
  per-column codec / dict / bloom choice.
- `ColumnData<'_>` enum carrying borrowed columns (`I32`, `I64`, `F64`,
  `Bool`, `ByteArray`).
- `WriteOptions` — per-column codec / dict-encoding / bloom-filter knobs.
- `PageVersion` enum (V1/V2), `FooterMode` (plain / encrypted footer).
- `write_i32_column_to_path_encrypted` + `_encrypted_footer` variants.

**Future work flagged in module docs:**
- Per-column encoding choice on `write_table_*` (`WriteOptions` covers
  codec / dict / bloom; raw encoding choice still single-knob).
- The Π.10 – Π.15 roadmap (custom LLVM codegen etc.) — not relevant to
  Σ.E5 directly.

### 2.4 `ematix-parquet-async` (depends on `format` + `io` + `codec` + tokio + object_store)

**Exports:** `AsyncParquetFile`, `read_column_{i64,i32,f64}_async`,
`_async_into`, `_async_stream` (Stream of Vec<T>),
`read_column_byte_array_async`, `_async_into`,
`read_column_byte_array_offsets_async`, `_async_into`.

**Coverage:**
- `AsyncParquetFile::open(store, path) -> Result<Self>` — footer fetch
  via `Range: bytes=-8192` suffix-range (≤ 2 RTs cold).
- Per-column async reads issue one GET per chunk.
- `.metadata()` exposes the parsed `FileMetaData`.

**Re-exports `object_store` so consumers don't take a second dep.**

**Future work flagged:** the async crate mirrors *read* facades only.
There is no `AsyncArrowWriter` equivalent, and no
`Stream<Item = Result<RecordBatch>>` adapter. (Today it streams
`Vec<T>` — column-at-a-time, not row-at-a-time.)

### 2.5 `ematix-parquet-crypto` (depends on `aes-gcm`, only behind `codec/encryption`)

AES-GCM-128 / AES-GCM-CTR primitives + AAD construction. Used by
`ematix-parquet-codec::encrypted` for PME read + write. Not a direct
consumer concern for E5; flow-core doesn't touch crypto today.

---

## 3. Gap analysis

| parquet-rs feature we use | ematix-parquet equivalent | Gap | Effort to close |
|---|---|---|---|
| `ArrowReaderMetadata` (cached parquet metadata + supplied-schema hint, cloneable) | `ParquetFile` (caches footer) + `FileMetaData` from `format` | **Medium gap**: no "supplied schema" concept; no Arrow-schema synthesis from parquet schema; no clone-share via `Arc<ArrowReaderMetadata>`. flow-core builds `ArrowReaderMetadata` once and clones it into every partition worker — the Arc-clone semantics matter for hot-path partition spawn. | **Medium (~1 wk)**: build `flow-core` wrapper `EmatArrowMetadata { file: Arc<ParquetFile>, supplied_schema: Option<SchemaRef> }` plus Arrow-schema-from-parquet-schema synth. ematix-parquet doesn't need to add anything if flow-core owns the Arrow synthesis. |
| `ArrowReaderOptions::{with_page_index_policy, with_size_stats_policy, with_encoding_stats_policy}` (skip-at-footer-parse) | `ParquetFile::open` parses footer once; no "skip stats" knob | **Small gap**: ematix-parquet doesn't have a footer-parse policy enum. Its footer parse is already cheap (no Arrow-schema build, no page-index unless asked). Flow-core's three `Skip*` calls would become no-ops by default. | **Trivial (re-express as no-op)**: drop the calls; document that ematix-parquet's footer is already lean. Verify on bench. |
| `ArrowReaderOptions::with_schema(schema)` — supply a target Arrow schema (drives Utf8View / Dictionary promotion at decode time) | **No equivalent.** ematix-parquet's read façades return concrete `Vec<T>` or `DictPreservedColumn`; no Arrow-schema-driven type rewrite at decode. | **Large gap**: this is the linchpin. parquet-rs's `with_schema` is *the* mechanism we use today to (a) get Utf8View instead of Utf8 (Σ.E2 root cause fix) and (b) get DictionaryArray for dict-encoded BYTE_ARRAY (Σ.E3b substrate). The bridge has the *primitives* (`read_column_byte_array_dict_preserved`, view-aware Arrow build) but the *Arrow RecordBatch reader* doesn't exist yet. | **Large (~2-4 wk)**: build the `ParquetRecordBatchReaderBuilder` equivalent — see §4 phase E5.4. |
| `ParquetRecordBatchReaderBuilder` (`with_projection` via `ProjectionMask`, `with_batch_size`, `with_row_groups`, `with_row_filter`, `build()` → `Iterator<Item=Result<RecordBatch>>`) | **No high-level Arrow batch reader exists in ematix-parquet.** `ematix-parquet-codec` has column-at-a-time façades (`read_column_*`) plus a `*_batches` streaming iterator that emits `Vec<T>` per chunk. The bridge composes these into Arrow arrays but only for the EmatixFast path; there's no batched `RecordBatch` emitter that handles arbitrary schemas + projection + batch_size = 65_536. | **Large (~2-4 wk)**: this is the single highest-leverage gap. Needs design: see §4 phase E5.4. We probably want it inside `ematix-parquet-codec` (or a new `ematix-parquet-arrow` crate) rather than in flow-core, so it benefits non-flow consumers. **Open question for the user**: which repo owns the Arrow batch reader? Flow-core can prototype it locally; if it generalises, push it down. |
| `ProjectionMask::leaves(&parquet_schema, leaf_indices)` | No equivalent. flow-core constructs this from leaf indices. | **Small gap**: a tuple `(SchemaDescriptor-equivalent, Vec<usize>)`. ematix-parquet exposes `SchemaElement` and `FileMetaData.schema` but no `SchemaDescriptor` analog with leaf-traversal helper. | **Small (~1 day)**: add `leaves_in_order(&FileMetaData) -> Vec<LeafInfo>` to ematix-parquet, or replicate in flow-core's bridge. |
| `SchemaDescriptor` | No equivalent. flow-core holds an `Arc<SchemaDescriptor>` to feed `ProjectionMask::leaves` repeatedly. | **Small gap**: derivable from `FileMetaData.schema` (which we have). | **Small (~1 day)**: add a wrapper or replace usage with leaf-index list directly. |
| `ParquetMetaData::row_group(i).column(j).encodings()` (detect RLE_DICTIONARY in every RG → Σ.E3b dict promotion) | `FileMetaData.row_groups[i].columns[j].meta_data.encodings` (parsed thrift Vec) | **Trivial**: ematix-parquet already exposes this on `ColumnMetaData` per `format/src/metadata.rs:806`. | **Trivial**: rewrite the access path; semantics identical. |
| `ParquetMetaData::row_group(i).column(j).statistics()` (typed min/max/null_count for row-group pruning + `partition_statistics`) | `format::metadata::Statistics<'a>` on `ColumnMetaData`, plus `read_statistics` parser. | **Small gap**: ematix-parquet's `Statistics` is a thin thrift-decoded struct holding `min: Option<&[u8]>`, `max: Option<&[u8]>`, `null_count: Option<i64>` (raw bytes). It does NOT do the typed-decode-into-ScalarValue that parquet-rs's `Statistics::Int32(s).min_opt()` does. flow-core's `aggregate_column_statistics` + `row_group_min_max` rely on parquet-rs decoding the typed min/max already. | **Small (~2-3 days)**: add typed `min<T>` / `max<T>` accessors to ematix-parquet (or do the byte → typed decode in flow-core given the column's physical type — only 5 types: Int32, Int64, Float, Double, Bool). |
| `Encoding::{RLE_DICTIONARY, PLAIN_DICTIONARY}` constants | `ematix_parquet_format::types::Encoding` — same enum, identical variants | **Trivial**: name-level re-import. | **Trivial**: search/replace. |
| `PageIndexPolicy::Skip`, `ParquetStatisticsPolicy::SkipAll` | No equivalent. ematix-parquet's footer parse is already lean; these are no-ops. | **Trivial (drop)**. | **Trivial**. |
| `AsyncArrowWriter::{try_new, write, close}` over an `AsyncWrite` sink | **No equivalent.** ematix-parquet-async has *no write façade* — only async reads. | **Large gap**: this blocks `objectstore_backend.write_parquet_at_path` directly. ematix-parquet *has* sync writers (`write_table_to_path`, etc.) but they take `Write`, not `AsyncWrite`, and they write whole-table-at-once not stream-friendly. | **Large (~2-3 wk)**: needs (a) an `AsyncArrowWriter`-shape API in `ematix-parquet-async` that takes `AsyncWrite + Send + Unpin` and accepts `RecordBatch` writes, OR (b) a streaming row-group writer that we adapt in flow-core with `spawn_blocking` + `BufWriter`. Option (b) is the safer/smaller bite. **This is the second-highest-leverage gap.** |
| `ParquetObjectReader::{new, with_file_size}` | `AsyncParquetFile::open(store, path)` does the same job; `.metadata()` exposes the footer | **Trivial**: same shape, different name. | **Trivial**: substitution. |
| `ParquetRecordBatchStreamBuilder::{new, build}` → `Stream<Item = Result<RecordBatch>>` | **No equivalent.** ematix-parquet-async exposes `read_column_*_async_stream` which streams `Vec<T>` column-at-a-time, not row-at-a-time `RecordBatch`. | **Large gap, mirror of the sync one**: this is the async sibling of the missing batch reader. Needs the same Arrow-batch emitter, just over `AsyncParquetFile`. | **Medium-Large (~1-2 wk on top of the sync version)**: once the sync Arrow batch reader exists, the async one can be a thin wrapper that uses `AsyncParquetFile` + `spawn_blocking` for the per-RG decode. Or, more ambitious, a true async iterator. |
| `WriterProperties::{builder, set_compression, build}` | `write_table_to_path(path, &cols, codec: CompressionCodec)` takes the codec directly; `WriteOptions` carries per-column codec/dict/bloom. | **Small gap**: parquet-rs's `WriterProperties` is one knob (compression) in our usage. ematix-parquet's `WriteOptions` covers the same surface for the multi-column path. | **Small (~1 day)**: map `ParquetCompression` → `ematix_parquet_format::types::CompressionCodec`. Identical 4-way mapping (Uncompressed / Snappy / Gzip / Zstd). |
| `basic::{Compression, GzipLevel, ZstdLevel}` | `CompressionCodec` enum in ematix-parquet-format | **Trivial gap**: same set of codecs. `GzipLevel::default` / `ZstdLevel::default` baked into ematix-parquet's defaults. | **Trivial**: replace. |
| `ArrowWriter` (sync, used in test_support only) | `write_table_to_path(path, &cols, codec)` — sync API, takes `ColumnData` slices | **Small gap**: test_support builds `RecordBatch`es from Arrow arrays and writes them. To use `write_table_to_path` we'd need to project each Arrow array's backing buffer to a `&[T]` (for primitives) or `&[&[u8]]` (for byte_array). Mechanical but per-type. | **Small (~1 day)**: rewrite the 3 fixture-writer fns. Test code; low risk. |
| `SerializedFileReader` + `FileReader::metadata` (test_support + bridge tests) | `ParquetFile::open(path)` + `.metadata()` | **Trivial**: same shape. | **Trivial**: substitution. |
| `ColumnReader`, `ColumnWriter`, `ByteArray` data type, `ConvertedType::{UTF8, DATE}`, `Type` (PhysicalType), `Repetition` (test_support + bridge tests) | Lower-level: `decode_plain_*`, `PageWalker`, `write_byte_array_column_to_path`, etc. + `Encoding`, `LogicalType`. | **Small**: the test writes use parquet-rs's low-level column-writer to control logical type (e.g. DATE annotation on Int32). ematix-parquet's writer supports `LogicalType::Date` per `format/src/metadata.rs` but I haven't verified the per-column-write API exposes it. **Open question**: does ematix-parquet's write path expose logical-type annotation today? If not, write the test fixtures with parquet-rs and migrate as a final cleanup pass. | **Small-medium**: test-only; can lag. |

### Gaps not flagged above — checked + confirmed parity

- **Snappy / Zstd / Gzip / Brotli / LZ4_RAW decompression** —
  ematix-parquet-codec ships all five; we only use Snappy / Zstd in
  current write paths. Confirmed parity.
- **INT96, FIXED_LEN_BYTE_ARRAY, DELTA_*, BYTE_STREAM_SPLIT** — all
  decoded by ematix-parquet. flow-core uses none of these today on
  the hot path; parity is "we won't regress when we touch them."
- **NEON unpackers (bw=12, 14, 15, 16, 17, 18 + bw=4, 8)** —
  ematix-parquet has these and parquet-rs does not (Photon-style
  fused decode). **This is a feature we'll *gain*, not lose,
  during migration.** Surface in §5 as an opportunity.
- **Encryption (PME)** — ematix-parquet covers read + write of both
  plaintext-footer and encrypted-footer modes. flow-core doesn't use
  parquet-rs encryption today.
- **Parallel multi-RG decode (`parallel::read_columns_parallel`)** —
  ematix-parquet exposes this behind a feature; flow-core's current
  RG parallelism is at the DataFusion partition layer. Could collapse
  some of the `spawn_blocking` machinery in `fast_parquet.rs` onto
  this primitive but it's an *opportunity*, not a *requirement*.
- **Bloom-filter writer** — ematix-parquet v0.10 closed this. Not on
  the flow-core path today.

---

## 4. Sequenced migration plan

### Phase E5.0 — this audit

This document. Sets the inventory + gap surface so subsequent phases each
land a tight scope.

**Acceptance:** doc reviewed; user signs off on phase ordering. No code
changes.

---

### Phase E5.1 — Close capability gaps in ematix-parquet (or in flow-core, if local)

Per §3, the *required* new capabilities are:

1. **Arrow `RecordBatch` reader** (`ParquetRecordBatchReaderBuilder`
   equivalent) — sync, accepts:
   - `ParquetFile` + cached `FileMetaData`
   - leaf-index projection
   - row-group selection
   - batch size (default 65_536 to match `fast_parquet.rs::DEFAULT_BATCH_SIZE`)
   - optional supplied Arrow schema for type-hint promotion
     (Utf8View, Dictionary)
   - per-page popcount-skip for downstream late-mat compatibility
2. **Async `RecordBatch` reader** — the async sibling, over
   `AsyncParquetFile`. Either truly async (per-page GETs) or
   `spawn_blocking` over the sync version. The latter is fine for
   object_store consumers since `object_store` already does the per-
   chunk GET batching.
3. **Async `RecordBatch` writer** (`AsyncArrowWriter` equivalent) —
   over an `AsyncWrite` sink. Streams `RecordBatch` → row groups
   → flush. Supports `WriterProperties`-style compression.
4. **Typed statistics decode** — `Statistics::min_typed::<T>()` /
   `max_typed::<T>()` / `null_count()` on `ColumnMetaData` for the
   5 types we use (Int32, Int64, Float, Double, Bool). Can live in
   flow-core as a helper module without upstreaming.

**Files touched:** new module `crates/ematix-flow-core/src/ematix_parquet_arrow.rs`
(or split into `..._arrow_reader.rs` / `..._arrow_writer.rs`) **OR**
upstream new module in `ematix-parquet-codec` (preferred long-term).

**Sketch of new flow-side API** (regardless of which repo owns it):

```rust
// Sync reader
pub struct EmatArrowBatchReaderBuilder { file: Arc<ParquetFile>, ... }
impl EmatArrowBatchReaderBuilder {
    pub fn try_new(file: ParquetFile, opts: EmatArrowReaderOptions) -> Result<Self>;
    pub fn with_projection(self, leaves: Vec<usize>) -> Self;
    pub fn with_row_groups(self, rgs: Vec<usize>) -> Self;
    pub fn with_batch_size(self, n: usize) -> Self;
    pub fn schema(&self) -> &SchemaRef;
    pub fn parquet_schema(&self) -> &FileMetaData<'_>;
    pub fn build(self) -> Result<impl Iterator<Item = Result<RecordBatch>>>;
}

pub struct EmatArrowReaderOptions {
    pub supplied_schema: Option<SchemaRef>,  // drives Utf8View / Dictionary
    pub dict_preserve_columns: Vec<usize>,   // explicit opt-in per col
}

// Async reader (mirror)
pub struct EmatAsyncArrowBatchStreamBuilder { file: AsyncParquetFile, ... }
// ... .build() -> impl Stream<Item = Result<RecordBatch>>

// Async writer
pub struct EmatAsyncArrowWriter<W: AsyncWrite + Send + Unpin> { ... }
impl<W> EmatAsyncArrowWriter<W> {
    pub fn try_new(sink: W, schema: SchemaRef, opts: EmatWriteOptions) -> Result<Self>;
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()>;
    pub async fn close(self) -> Result<()>;
}
```

**Perf risk:** high. The sync reader is the hot path for every TPC-H
bench. Parity gate: bench all 22 queries SF=1 + SF=10 against the
parquet-rs path before any migration. **Budget: ≤ 5% regression
per-query, ≤ 2% on geomean.** Faster is fine.

**Effort:** large, ~3–4 weeks if both repos coordinate. The reader is
the longest pole.

**Acceptance criteria:**
- New APIs exist + have unit tests against synthetic + TPC-H mini
  fixture data.
- TPC-H 22-query parity bench (existing harness) shows ≤ 5% per-
  query regression vs `parquet=58`.
- `fast_parquet.rs` and `objectstore_backend.rs` can each be
  rewritten against the new API in a *follow-up* phase (not this
  one — keep the bites small).

---

### Phase E5.2 — Close the EmatixFastParquet Q1 SQL regression

**Observation (from PR queue / Q1 SQL gate):** `EmatixFastParquetTableProvider`
runs Q1 SF=1 at ~59 ms vs `FastParquetTableProvider`'s ~39 ms — a 51%
regression at the engine level (not at the codec level; codec-only is at
parity per `bench_decode` in ematix-parquet).

**Hypothesis space (none yet validated):**
1. Per-RG vs streaming emission: EmatixFastParquet emits **one
   RecordBatch per row group** (whole-chunk decode); FastParquet
   emits 65_536-row batches sized by parquet-rs's reader. Downstream
   AggregateExec sees longer pauses with EmatixFast → worse pipelining.
2. Missing Utf8View on Q1's `l_returnflag` / `l_linestatus`:
   EmatixFast emits `Utf8` (or `Dictionary<UInt32, Utf8>` if
   `with_dict_preservation(true)`), not `Utf8View`. Q1 GROUP BY
   kernel selection downstream may be picking the slower path.
3. Per-RG `spawn_blocking` overhead: the late-mat path opens the
   file once per RG-decode call (`ParquetFile::open` per
   `decode_column_chunk_*`). Cold-open cost is small but n×6 RGs ×
   n columns adds up.
4. Stats not exposed to planner: EmatixFast's `partition_statistics()`
   is `new_unknown` — no min/max → planner can't size the GROUP BY
   hash table or join build side.

**Approach:**
- Add EXPLAIN ANALYZE harness for Q1 against both providers; compare
  per-operator timings.
- Run the codec-layer bench (`bench_decode` in ematix-parquet) on
  the exact column set Q1 reads (l_returnflag, l_linestatus,
  l_quantity, l_extendedprice, l_discount, l_tax, l_shipdate); confirm
  decode time matches.
- Subtract decode from total → that's the engine-layer gap. Bisect on
  the four hypotheses above.

**This is gating.** If perf parity isn't reached, migrating
`fast_parquet.rs` will regress every TPC-H query. **Do not start E5.4
until E5.2 closes.** It is acceptable for E5.2 to ship a finding rather
than a fix — e.g. "Utf8View emission lands in E5.4 as part of supplied-
schema support and closes the Q1 gap." But the *reason* must be
identified.

**Sequencing caveat:** E5.2 may surface a finding that the gap is
*entirely* attributable to the missing supplied-schema + Utf8View
path — in which case E5.2 collapses into E5.1 (the Arrow reader gets
supplied-schema and the gap closes for free). The audit can't tell
without measurement. Plan a 1-week diagnostic spike first; re-sequence
if the bottleneck is upstream.

**Effort:** 1–2 weeks (diagnostic spike + potential fix).

**Acceptance:** EmatixFastParquet Q1 SF=1 within 5% of FastParquet.

---

### Phase E5.3 — Migrate `ematix_fast_parquet.rs` metadata + test_support

The easiest sites. EmatixFastParquet already uses
ematix-parquet-codec for decode; the only parquet-rs surface left is
metadata loading (`ArrowReaderMetadata::load`, `SerializedFileReader`).

**Sub-bite 1: `ematix_fast_parquet.rs::try_new`** — replace lines 209–
215 + 235–247 with `ParquetFile::open(&path)` + an Arrow-schema synth
helper (the new typed-from-parquet-schema function from E5.1, item 4).
~50 LOC change.

**Sub-bite 2: `test_support.rs::write_lineitem / write_part / ...`** —
replace `ArrowWriter` + `WriterProperties` with `write_table_to_path` +
`CompressionCodec::Snappy`. ~100 LOC across 8 fixture writers.

**Sub-bite 3: `ematix_parquet_bridge.rs` test-only oracle** — replace
`SerializedFileReader` + `ColumnReader` reads with `read_column_*`
from ematix-parquet-codec. ~30 LOC. **Caveat:** if the oracle's whole
point is to verify ematix-parquet against an *independent* reader,
keep parquet-rs in `[dev-dependencies]` only — moving the oracle to
self-decode is a correctness regression.

**Files touched:** `src/ematix_fast_parquet.rs`,
`src/test_support.rs`, `src/ematix_parquet_bridge.rs` (test-only
portions only — the production decode path is already ematix-parquet
native).

**Effort:** ~3–4 days.

**Acceptance:** all tests pass; parquet-rs no longer imported outside
`[dev-dependencies]` from these files.

---

### Phase E5.4 — Migrate `fast_parquet.rs`

The big one. ~750 LOC of parquet-rs use.

**Pre-requisite:** E5.1 + E5.2 both green.

**Approach: build it side-by-side, swap by feature flag, retire
parquet-rs surface last.**

1. New struct `EmatBatchFastParquetTableProvider` next to the existing
   `FastParquetTableProvider`. Same TableProvider trait impl. Internally
   uses the new ematix-parquet Arrow batch reader from E5.1.
2. Wire into the same examples + benches that exercise FastParquet
   today.
3. Run the 22-query parity bench. **Block on ≤ 5% per-query
   regression.**
4. Switch in-tree call sites (rules, examples) to the new provider,
   one at a time, gating each on its own bench.
5. Delete `FastParquetTableProvider` + its parquet-rs imports.
6. Verify `cargo tree -p ematix-flow-core -e=normal | grep parquet`
   no longer shows `parquet 58` as a normal dep through this file.

**Files touched:** `src/fast_parquet.rs` (eventually deleted, replaced
by a new module).

**Perf risk:** **highest in the migration.** Every TPC-H bench gates
on this.

**Effort:** ~2 weeks once E5.1 + E5.2 are done. Side-by-side execution
matters more than raw LOC.

**Acceptance:**
- 22-query parity bench within 5% per-query, geomean ≤ 2% loss
  (faster is fine).
- All TPC-H tests pass.
- Dict-preservation (Σ.E3b) still works against the new provider.
- Row-group pruning + `partition_statistics()` still drive the
  planner.

---

### Phase E5.5 — Migrate `objectstore_backend.rs`

Async path. **Pre-requisite:** E5.1 capability gaps 2 + 3 (async
batch reader + async batch writer) closed.

**Sub-bite 1: read path** — replace `ParquetObjectReader` +
`ParquetRecordBatchStreamBuilder` (lines 280–297) with the new async
batch stream over `AsyncParquetFile`. ~30 LOC delta.

**Sub-bite 2: write path** — replace `AsyncArrowWriter` (lines 307–
349) + `WriterProperties` (319–323) + `parquet_compression_to_codec`
(505–513) with the new `EmatAsyncArrowWriter`. ~50 LOC delta.

**Perf risk:** medium. The async path is bottlenecked by network /
disk I/O at the object_store layer, not by the parquet decoder, so
the codec swap is unlikely to dominate. But:
- **Cold-open RTs:** parquet-rs's `ParquetObjectReader` doesn't use
  the suffix-range trick by default; `AsyncParquetFile::open` does
  (≤ 2 RTs cold). **Net win likely.**
- **Per-column GETs:** both issue one GET per chunk; parity expected.
- **Write multipart:** both flush to object_store::BufWriter; parity
  expected (the wrapping is what matters).

**Files touched:** `src/objectstore_backend.rs`.

**Effort:** ~1 week.

**Acceptance:**
- Existing object-store backend tests pass against local-FS +
  in-memory stores.
- S3 integration test (if present) passes.
- Read + write throughput within 10% on a 1 GB synthetic fixture
  (local-FS); ≤ 0% on cold-open RT count for S3.

---

### Phase E5.6 — Remove the workspace dep

After E5.3 + E5.4 + E5.5 all merge:

1. Move `parquet = { workspace = true }` from
   `crates/ematix-flow-core/Cargo.toml`'s `[dependencies]` to
   `[dev-dependencies]` *only if* the bridge oracle test still needs
   it. Otherwise delete the line.
2. Re-run `cargo build` + `cargo test -p ematix-flow-core --all-features`.
3. Re-run the 22-query parity bench one final time.
4. Update `docs/PHASE_SIGMA_E_*` to mark E5 complete.

**Files touched:** `Cargo.toml`.

**Effort:** ~1 day (verification dominates).

**Acceptance:** `cargo tree -p ematix-flow-core -e=normal | grep ^parquet`
shows zero results. Workspace `Cargo.toml` retains the `parquet`
workspace dep (other crates / DataFusion transitive may keep using it).

---

## 5. Non-goals + open questions

### Non-goals (explicit)

- **DataFusion's transitive parquet-rs dep stays.** DF re-exports
  `parquet` via `datafusion::parquet::*` and uses it internally for
  `register_parquet`, the default `ListingTable` parquet reader, and
  `partition_statistics` glue. Removing that requires forking DF or
  swapping its `ParquetFormatFactory` — explicitly out of scope.
- **No new parquet codec features.** The migration is a port, not a
  capability expansion. NEON fused decode, dict-preserved Arrow,
  late-mat — these come along for the ride (already in ematix-parquet)
  but no new SIMD / encoding work in E5.
- **No write-side perf push.** Object-store write throughput parity
  is the bar, not beat-parquet-rs.

### Opportunities the migration unlocks (surface but defer to follow-ups)

- **NEON fused decode by default on TPC-H reads.** Today
  `fast_parquet.rs` uses parquet-rs which doesn't have NEON-fused
  unpack + predicate. The Σ.E2 / Q14 work shows the kernels exist and
  win. Post-E5.4 every TPC-H query gets them on aarch64. Track as a
  *separate* perf phase (Σ.E5-followup-perf).
- **Dict-aware reads end-to-end.** `EnableDictGroupCountRule` is a
  no-op today because parquet-rs materialises dicts at decode (per
  the `dict-arrival-blocker` memory note). After E5.4, the Arrow
  batch reader can preserve dicts at the projection boundary natively
  — Σ.E3b lights up on real TPC-H data without the per-call
  `with_dict_preservation(true)` opt-in.
- **Cold-open round-trip cut on S3.** `AsyncParquetFile::open` does
  the suffix-range trick; `ParquetObjectReader` doesn't by default.
  Measurable cold-start win for warehouse-style queries against S3.
- **Encryption optional.** ematix-parquet's PME is feature-gated
  off by default; we can default the workspace build to no-crypto,
  smaller binary. parquet-rs always pulls in encryption symbols on
  the `experimental` feature.

### Open questions

1. **Where does the new Arrow batch reader live?** Two options:
   - (a) **In `ematix-parquet-codec`** — a new `arrow` module behind
     a feature flag (`features = ["arrow"]`), pulls in
     `arrow-array` / `arrow-schema` as a feature-gated dep.
     Pro: benefits all consumers, not just flow-core. Symmetric with
     `ematix-parquet-async`.
     Con: introduces an Arrow-version coupling on ematix-parquet
     (it currently has Arrow only as a dev-dep). Versioning gets
     stickier.
   - (b) **In `ematix-flow-core` as a new module** —
     `src/emat_arrow_reader.rs`. Pro: zero downstream coordination;
     ematix-parquet stays Arrow-free. Con: every other consumer who
     wants Arrow output has to reimplement.
   - **Recommendation:** start with (b) (low risk, fast), promote to
     (a) once the API surface is settled. The user owns both repos so
     either is mechanically possible.
2. **Does the bridge oracle test stay on parquet-rs?** The bridge's
   purpose is to verify ematix-parquet's decode against an
   independent reference. If we replace the reference with
   ematix-parquet itself, the oracle becomes tautological. **Keep
   parquet-rs as a `[dev-dependencies]` for oracle tests** unless
   the user explicitly wants it gone too. (The E5 brief says "remove
   as a direct dep" — `[dev-dependencies]` is a separate axis.)
3. **What does the EmatixFastParquet Q1 regression turn out to
   be?** E5.2 is gated diagnostically — answer determines E5.4
   schedule. **Risk:** if the gap is intrinsic to per-RG-batch
   emission (vs parquet-rs's 65k-row streaming batches), we need a
   streaming batch emitter in ematix-parquet *before* E5.4, not as
   part of it. Could push E5.1 from 4 weeks to 5–6.
4. **Logical-type annotation on writes** — does
   `write_table_to_path` round-trip `Date32`-as-Int32-with-DATE
   correctly today? E5.3 sub-bite 2 depends on this. **Action:** a
   30-min spike against an existing TPC-H parquet fixture answers
   it; if no, flag a small ematix-parquet enhancement first.
5. **Should `ProjectionMask`-equivalent be a flow-core type or a
   shared ematix-parquet type?** Same dependent on Q1 above. The
   "leaf indices into FileMetaData" pattern is reusable enough that
   it belongs upstream, but the urgency is low.
6. **TPC-H non-goal sanity check:** the migration must NOT bake
   TPC-H assumptions into the Arrow batch reader. Specifically: the
   batch size (65_536) is *DataFusion's* default, not a TPC-H tuning
   knob — keep it configurable per builder, not const-baked. The
   "all-RGs dict criterion" for dict preservation (in
   `promote_dict_encoded_to_dictionary`) is also generic, not TPC-H-
   specific. Flagging both so a reviewer can rule them in/out.

---

## 6. Estimated wall-time + risk summary

**Total wall-time:** ~9–12 weeks of focused work, assuming a single
engineer split across ematix-flow and ematix-parquet.

| Phase | Weeks (low) | Weeks (high) |
|-------|-------------|--------------|
| E5.0 (this audit) | 0 | 0 |
| E5.1 (capability gaps) | 3 | 4 |
| E5.2 (Q1 regression diag) | 1 | 2 |
| E5.3 (metadata + test sites) | 0.5 | 1 |
| E5.4 (fast_parquet migration) | 2 | 3 |
| E5.5 (objectstore migration) | 1 | 1.5 |
| E5.6 (remove dep + verify) | 0.2 | 0.5 |
| **Total** | **~8** | **~12** |

E5.1 and E5.2 can partially overlap (the Q1 diagnostic informs the
Arrow batch reader's batch-emission shape, but the typed-stats /
async-writer work is independent). E5.5 can run in parallel with
E5.4 once E5.1 closes. Realistic critical path is ~9 weeks.

### Top 3 risks

1. **Hot-path perf regression on `fast_parquet.rs` migration (E5.4).**
   The 22 TPC-H queries are well-tuned against parquet-rs's reader
   today (Σ.E2 Utf8View work, batch size, row-group pruning, dict
   preservation). The new reader has to match each lever or geomean
   regresses. **Mitigation:** side-by-side providers, per-query
   parity gate before swap, keep the parquet-rs provider alive
   behind a feature flag for 1 release cycle so we can revert without
   breaking benches.
2. **Q1 regression root cause (E5.2) turns out to be the missing
   65_536-row streaming batch emission.** If true, E5.1 grows by
   ~1 week to add page-aware streaming inside the Arrow batch reader
   (each chunk emits multiple RecordBatches, not one). Schedules
   compress.  **Mitigation:** treat the streaming batch emitter as
   *in-scope for E5.1* from the start rather than adding it
   reactively — it's clearly the parquet-rs shape and we should
   match it.
3. **Async write path needs more design than expected (E5.1
   capability 3).** ematix-parquet has no async writer today. The
   sync `write_table_to_path` is whole-table-at-once. Building a
   streaming row-group writer that flushes to `AsyncWrite`
   incrementally is non-trivial (footer goes at the end, row-group
   metadata has to be tracked as we write, multipart commit needs
   careful close semantics). **Mitigation:** ship a flow-core-local
   `spawn_blocking` adapter first (wraps the sync writer + a pipe
   sink) — proves the API surface, gives objectstore_backend its
   replacement, defers the proper async writer to ematix-parquet
   v0.11.

### What could derail the plan

- **DataFusion 59 bump** — if DF goes to arrow 59 / parquet 59
  mid-migration, both crates need to bump in lockstep. ematix-parquet's
  Arrow dep (currently 58 for tests only) becomes load-bearing if we
  go option (a) in §5 open question 1; coordinate the arrow major.
- **A second consumer landing inside flow-core** — e.g. a Delta /
  Iceberg surface that takes a parquet-rs handle directly. Currently
  not the case; deltalake hides parquet behind its own surface.
  Audit the dep graph at start of each phase.
- **Π.10/Π.11 cycle in ematix-parquet pulling perf focus** — the
  upstream `docs/plans/CURRENT.md` lists work that may compete with
  flow's E5 needs. **Coordination check:** before starting E5.1,
  confirm the Arrow batch reader and async writer are *prioritised*
  over Π.10+ items.

---

**Audit complete. Next:** review with user, slot E5.0 sign-off, schedule
E5.1 + E5.2 in parallel as the first migration bites.
