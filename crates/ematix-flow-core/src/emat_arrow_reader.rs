//! Σ.E5.1 — Streaming Arrow `RecordBatch` reader over `ematix-parquet`.
//!
//! Reads parquet column chunks via the sibling-repo `ematix-parquet-io`
//! `PageWalker` + `ematix-parquet-codec` decoders, and emits Arrow
//! `RecordBatch`es of a caller-supplied schema. Replaces the whole-
//! row-group emission shape used by `ematix_parquet_bridge`'s typed
//! decoders for the Q1-shape workload — downstream operators see
//! 65 536-row batches (the FastParquet shape) instead of one mega-
//! batch per row group.
//!
//! ## API contract (locked by Σ.E5.2)
//!
//! The supplied Arrow schema carries the caller's column-type intent.
//! In particular:
//!   - `Utf8View`  → `StringViewArray`, materialised once per RG by
//!     pushing the parquet dictionary bytes as one backing block and
//!     building per-row views over it (no per-row `StringBuilder`).
//!   - `Dictionary(UInt32, Utf8)` → `DictionaryArray<UInt32Type>`
//!     preserving the parquet dict end-to-end.
//!   - `Int32` / `Date32` / `Int64` / `Float64` → primitive Arrays
//!     sliced from per-RG buffers.
//!   - `Utf8` (slow path) → `StringArray` materialised row-by-row.
//!
//! Anything else returns `DataFusionError::NotImplemented` from
//! `build()` — extend later when a workload needs it.
//!
//! ## Streaming semantics
//!
//! Per row group:
//!  1. Decode every projected column once (per-RG dict reuse).
//!  2. Slice the per-RG arrays into `batch_size`-row windows.
//!  3. Emit one `RecordBatch` per window.
//!  4. Cross RG boundaries between windows — never within. (Dict
//!     codes change at the RG boundary; mixing them in a single
//!     `DictionaryArray` would mis-index.)
//!
//! See `docs/PHASE_SIGMA_E5_PARQUET_RS_ELIMINATION.md` §E5.1.
//!
//! ## Scope (Σ.E5.1)
//!
//! Build + test + bench. No provider/exec wire-up — that's Σ.E5.1.b.
//! Async sibling + writer are Σ.E5.3/E5.4.

use std::sync::Arc;

use arrow_array::builder::{ArrayBuilder, StringBuilder, make_view};
use arrow_array::types::UInt32Type;
use arrow_array::{
    Array, ArrayRef, Date32Array, DictionaryArray, Float64Array, Int32Array, Int64Array,
    RecordBatch, StringArray, StringViewArray, UInt32Array,
};
use arrow_schema::{DataType, SchemaRef};
// Buffer types come in via datafusion's arrow re-export — keeps this
// module's direct dep list aligned with the rest of the crate.
use datafusion::arrow::buffer::{Buffer, NullBuffer, ScalarBuffer};
use datafusion::error::{DataFusionError, Result as DfResult};

use ematix_parquet_codec::compression::{decompress_snappy_into, decompress_zstd_into};
use ematix_parquet_codec::dict::decode_rle_dictionary_into;
use ematix_parquet_codec::plain::{
    decode_plain_byte_array, decode_plain_f64, decode_plain_i32, decode_plain_i64,
};
use ematix_parquet_codec::read::read_column_byte_array_dict_preserved;
use ematix_parquet_format::types::{CompressionCodec, Encoding, ParquetType};
use ematix_parquet_io::{PageWalker, ParquetFile};

/// Default batch size — matches `FastParquetTableProvider`'s
/// `DEFAULT_BATCH_SIZE` and DataFusion's pipelining sweet spot.
pub const DEFAULT_BATCH_SIZE: usize = 65_536;

// ============================================================
// Builder
// ============================================================

/// Builder for [`EmatArrowBatchReader`].
///
/// See module docs for semantics. `arrow_schema` is REQUIRED and drives
/// type promotion (Utf8View / Dictionary). `projection` carries leaf
/// indices in the parquet column order; the output `RecordBatch`
/// schema has the same column order, with one Arrow field per
/// projected leaf taken from `arrow_schema` (or all fields if no
/// projection is supplied).
pub struct EmatArrowBatchReaderBuilder {
    file: ParquetFile,
    arrow_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    row_groups: Option<Vec<usize>>,
    batch_size: usize,
}

impl EmatArrowBatchReaderBuilder {
    pub fn new(file: ParquetFile, arrow_schema: SchemaRef) -> Self {
        Self {
            file,
            arrow_schema,
            projection: None,
            row_groups: None,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    pub fn with_projection(mut self, leaf_indices: Vec<usize>) -> Self {
        self.projection = Some(leaf_indices);
        self
    }

    pub fn with_row_groups(mut self, rgs: Vec<usize>) -> Self {
        self.row_groups = Some(rgs);
        self
    }

    pub fn with_batch_size(mut self, n: usize) -> Self {
        self.batch_size = n.max(1);
        self
    }

    pub fn build(self) -> DfResult<EmatArrowBatchReader> {
        let md = self
            .file
            .metadata()
            .map_err(|e| ext(format!("metadata: {e}")))?;

        // Determine projection (defaults to all leaves in column order).
        let num_cols = if let Some(rg0) = md.row_groups.first() {
            rg0.columns.len()
        } else {
            0
        };
        let projection = self.projection.unwrap_or_else(|| (0..num_cols).collect());

        // Cross-check schema length with projection.
        if self.arrow_schema.fields().len() != projection.len() {
            return Err(ext(format!(
                "arrow_schema has {} fields but projection has {} columns; \
                 supply one Arrow field per projected leaf",
                self.arrow_schema.fields().len(),
                projection.len(),
            )));
        }

        // Bounds-check projection.
        for &c in &projection {
            if c >= num_cols {
                return Err(ext(format!(
                    "projection: leaf {c} out of range (num_cols = {num_cols})"
                )));
            }
        }

        // Resolve row group order.
        let total_rgs = md.row_groups.len();
        let row_groups = match self.row_groups {
            Some(rgs) => {
                for &rg in &rgs {
                    if rg >= total_rgs {
                        return Err(ext(format!(
                            "row_groups: rg {rg} out of range (total = {total_rgs})"
                        )));
                    }
                }
                rgs
            }
            None => (0..total_rgs).collect(),
        };

        // Pre-validate each projected column's Arrow target against
        // its parquet physical type — fail-fast at build() so callers
        // get a clean error before they start iterating.
        for (proj_idx, &leaf) in projection.iter().enumerate() {
            let phys = column_physical_type(&md, leaf)?;
            let target = self.arrow_schema.field(proj_idx).data_type();
            validate_type_pair(self.arrow_schema.field(proj_idx).name(), phys, target)?;
        }

        Ok(EmatArrowBatchReader {
            file: self.file,
            arrow_schema: self.arrow_schema,
            projection,
            row_groups,
            batch_size: self.batch_size,
            cur_rg_idx: 0,
            cur_rg_columns: None,
            cur_rg_row: 0,
            cur_rg_total: 0,
        })
    }
}

#[inline]
fn column_physical_type(
    md: &ematix_parquet_format::metadata::FileMetaData<'_>,
    leaf: usize,
) -> DfResult<ParquetType> {
    let rg0 = md
        .row_groups
        .first()
        .ok_or_else(|| ext("file has no row groups"))?;
    let cm = rg0.columns[leaf]
        .meta_data
        .as_ref()
        .ok_or_else(|| ext(format!("leaf {leaf}: missing meta_data")))?;
    Ok(cm.column_type)
}

fn validate_type_pair(name: &str, phys: ParquetType, target: &DataType) -> DfResult<()> {
    let ok = match target {
        DataType::Int32 | DataType::Date32 => phys == ParquetType::Int32,
        DataType::Int64 => phys == ParquetType::Int64,
        DataType::Float64 => phys == ParquetType::Double,
        DataType::Utf8 | DataType::Utf8View => phys == ParquetType::ByteArray,
        DataType::Dictionary(k, v) => {
            matches!(k.as_ref(), DataType::UInt32)
                && matches!(v.as_ref(), DataType::Utf8)
                && phys == ParquetType::ByteArray
        }
        _ => false,
    };
    if !ok {
        return Err(DataFusionError::NotImplemented(format!(
            "EmatArrowBatchReader: column `{name}`: parquet physical type {phys:?} not yet \
             supported with target Arrow type {target:?} \
             (supported: Int32/Date32←INT32, Int64←INT64, Float64←DOUBLE, \
              Utf8/Utf8View/Dictionary(UInt32,Utf8)←BYTE_ARRAY)"
        )));
    }
    Ok(())
}

// ============================================================
// Reader
// ============================================================

/// Per-RG decoded buffers for one projected column. Each variant is a
/// fully-materialised representation of the *entire* RG for that
/// column; the reader slices these into `batch_size`-row windows
/// without re-decoding.
enum DecodedColumn {
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    Float64(Vec<f64>),
    /// (views as ScalarBuffer<u128>, backing data block)
    StringView {
        views: Vec<u128>,
        data: Buffer,
    },
    /// Dict-preserved BYTE_ARRAY → DictionaryArray<UInt32, Utf8>
    /// values + indices. Indices are RG-local — never spans RGs.
    DictUtf8 {
        values: Arc<StringArray>,
        indices: Vec<u32>,
    },
    /// Slow path: materialised UTF-8 (per-row copy).
    Utf8(Arc<StringArray>),
}

impl DecodedColumn {
    fn len(&self) -> usize {
        match self {
            DecodedColumn::Int32(v) => v.len(),
            DecodedColumn::Int64(v) => v.len(),
            DecodedColumn::Float64(v) => v.len(),
            DecodedColumn::StringView { views, .. } => views.len(),
            DecodedColumn::DictUtf8 { indices, .. } => indices.len(),
            DecodedColumn::Utf8(s) => s.len(),
        }
    }
}

pub struct EmatArrowBatchReader {
    file: ParquetFile,
    arrow_schema: SchemaRef,
    /// Leaf indices into the parquet column order. `projection[i]`
    /// gives the parquet leaf for the i-th Arrow field.
    projection: Vec<usize>,
    /// Row groups to scan, in order.
    row_groups: Vec<usize>,
    batch_size: usize,

    // ---- iteration state ----
    /// Index into `row_groups`; `cur_rg_idx == row_groups.len()`
    /// signals end-of-stream.
    cur_rg_idx: usize,
    /// Per-projected-column decoded buffers for the current RG, or
    /// `None` before the first batch / after the RG is exhausted.
    cur_rg_columns: Option<Vec<DecodedColumn>>,
    /// Next row index within the current RG.
    cur_rg_row: usize,
    /// Total rows in the current RG.
    cur_rg_total: usize,
}

impl EmatArrowBatchReader {
    pub fn schema(&self) -> &SchemaRef {
        &self.arrow_schema
    }

    /// Decode every projected column of `rg` into `cur_rg_columns`.
    fn load_row_group(&mut self, rg: usize) -> DfResult<()> {
        let md = self
            .file
            .metadata()
            .map_err(|e| ext(format!("metadata: {e}")))?;
        let row_group = &md.row_groups[rg];
        self.cur_rg_total = row_group.num_rows as usize;

        let mut cols: Vec<DecodedColumn> = Vec::with_capacity(self.projection.len());
        for (proj_idx, &leaf) in self.projection.iter().enumerate() {
            let target = self.arrow_schema.field(proj_idx).data_type();
            let decoded = decode_one_column(&self.file, rg, leaf, target)?;
            cols.push(decoded);
        }

        // Sanity: every column has the same length as the RG.
        for (i, c) in cols.iter().enumerate() {
            if c.len() != self.cur_rg_total {
                return Err(ext(format!(
                    "RG {rg}: column {i} (leaf {}) decoded {} rows but RG declares {}",
                    self.projection[i],
                    c.len(),
                    self.cur_rg_total,
                )));
            }
        }

        self.cur_rg_columns = Some(cols);
        self.cur_rg_row = 0;
        Ok(())
    }

    /// Slice `[start, start+n)` from each per-RG buffer and assemble
    /// the `RecordBatch`. Caller guarantees the window stays inside
    /// the current RG.
    fn slice_batch(&self, start: usize, n: usize) -> DfResult<RecordBatch> {
        let cols = self
            .cur_rg_columns
            .as_ref()
            .expect("slice_batch called with no current RG");
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(cols.len());
        for (i, c) in cols.iter().enumerate() {
            let target = self.arrow_schema.field(i).data_type();
            arrays.push(slice_decoded(c, start, n, target));
        }
        RecordBatch::try_new(self.arrow_schema.clone(), arrays)
            .map_err(|e| ext(format!("RecordBatch::try_new: {e}")))
    }
}

impl Iterator for EmatArrowBatchReader {
    type Item = DfResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // If the current RG is exhausted (or unset), advance.
            let need_new_rg = self.cur_rg_columns.is_none() || self.cur_rg_row >= self.cur_rg_total;
            if need_new_rg {
                if self.cur_rg_idx >= self.row_groups.len() {
                    return None;
                }
                let rg = self.row_groups[self.cur_rg_idx];
                self.cur_rg_idx += 1;
                if let Err(e) = self.load_row_group(rg) {
                    return Some(Err(e));
                }
                if self.cur_rg_total == 0 {
                    // Empty RG — try the next one.
                    continue;
                }
            }

            let remaining = self.cur_rg_total - self.cur_rg_row;
            let n = remaining.min(self.batch_size);
            let start = self.cur_rg_row;
            self.cur_rg_row += n;
            return Some(self.slice_batch(start, n));
        }
    }
}

// ============================================================
// Per-column decode
// ============================================================

fn decode_one_column(
    file: &ParquetFile,
    rg: usize,
    leaf: usize,
    target: &DataType,
) -> DfResult<DecodedColumn> {
    match target {
        DataType::Int32 | DataType::Date32 => {
            let v = decode_dict_chunk_typed::<i32>(file, rg, leaf, |b| {
                decode_plain_i32(b).map_err(|e| ext(format!("plain i32: {e}")))
            })?;
            Ok(DecodedColumn::Int32(v))
        }
        DataType::Int64 => {
            let v = decode_dict_chunk_typed::<i64>(file, rg, leaf, |b| {
                decode_plain_i64(b).map_err(|e| ext(format!("plain i64: {e}")))
            })?;
            Ok(DecodedColumn::Int64(v))
        }
        DataType::Float64 => {
            let v = decode_dict_chunk_typed::<f64>(file, rg, leaf, |b| {
                decode_plain_f64(b).map_err(|e| ext(format!("plain f64: {e}")))
            })?;
            Ok(DecodedColumn::Float64(v))
        }
        DataType::Utf8View => decode_byte_array_to_string_view(file, rg, leaf),
        DataType::Dictionary(_, _) => decode_byte_array_dict_preserved(file, rg, leaf),
        DataType::Utf8 => decode_byte_array_to_utf8(file, rg, leaf),
        other => Err(DataFusionError::NotImplemented(format!(
            "EmatArrowBatchReader: target Arrow type {other:?} not yet supported"
        ))),
    }
}

/// PR-2-style generic dict-or-PLAIN decoder for fixed-size primitives.
/// Mirrors `ematix_parquet_bridge::decode_dict_chunk_generic`.
fn decode_dict_chunk_typed<T: Copy>(
    file: &ParquetFile,
    rg: usize,
    col: usize,
    decode_plain: impl Fn(&[u8]) -> DfResult<Vec<T>>,
) -> DfResult<Vec<T>> {
    let md = file.metadata().map_err(|e| ext(format!("metadata: {e}")))?;
    let cm = md.row_groups[rg].columns[col]
        .meta_data
        .as_ref()
        .ok_or_else(|| ext("column missing meta_data"))?;
    let total = cm.num_values as usize;
    let codec = cm.codec;
    let start = cm
        .dictionary_page_offset
        .filter(|&d| d < cm.data_page_offset)
        .unwrap_or(cm.data_page_offset) as u64;
    let length = cm.total_compressed_size as u64;
    let chunk = file
        .read_range(start, length)
        .map_err(|e| ext(format!("read_range: {e}")))?;

    let mut walker = PageWalker::new(&chunk);
    let mut scratch: Vec<u8> = Vec::with_capacity(128 * 1024);
    let mut out: Vec<T> = Vec::with_capacity(total);

    let (first_hdr, first_body) = walker
        .next_page()
        .map_err(|e| ext(format!("next_page (first): {e}")))?
        .ok_or_else(|| ext("empty chunk"))?;
    decompress_into(codec, first_body, &mut scratch)?;

    let dict: Vec<T> = if first_hdr.dictionary_page_header.is_some() {
        decode_plain(&scratch)?
    } else {
        let dph = first_hdr
            .data_page_header
            .as_ref()
            .ok_or_else(|| ext("first page neither dict nor v1 data"))?;
        let n = dph.num_values as usize;
        match dph.encoding {
            Encoding::Plain => {
                let mut vals = decode_plain(&scratch)?;
                vals.truncate(n);
                out.extend(vals);
            }
            other => {
                return Err(ext(format!(
                    "first page is data but encoding {other:?} (need dict context)"
                )));
            }
        }
        Vec::new()
    };

    while out.len() < total {
        let (hdr, body) = walker
            .next_page()
            .map_err(|e| ext(format!("next_page: {e}")))?
            .ok_or_else(|| ext("chunk ended before num_values consumed"))?;
        let dph = hdr
            .data_page_header
            .as_ref()
            .ok_or_else(|| ext("v2 pages not yet supported"))?;
        let n = dph.num_values as usize;
        decompress_into(codec, body, &mut scratch)?;
        match dph.encoding {
            Encoding::RleDictionary | Encoding::PlainDictionary => {
                decode_rle_dictionary_into(&scratch, &dict, n, &mut out)
                    .map_err(|e| ext(format!("rle_dict: {e}")))?;
            }
            Encoding::Plain => {
                let mut vals = decode_plain(&scratch)?;
                vals.truncate(n);
                out.extend(vals);
            }
            other => {
                return Err(ext(format!("unexpected data page encoding {other:?}")));
            }
        }
    }
    Ok(out)
}

/// BYTE_ARRAY → `StringViewArray` with the parquet dictionary bytes
/// pushed as one backing block. The per-row view (u128) points at an
/// offset inside that block; no per-row UTF-8 validation, no
/// `StringBuilder` round-trip. PLAIN-fallback pages append their
/// bytes to the same block and emit views over the new offsets.
fn decode_byte_array_to_string_view(
    file: &ParquetFile,
    rg: usize,
    col: usize,
) -> DfResult<DecodedColumn> {
    let md = file.metadata().map_err(|e| ext(format!("metadata: {e}")))?;
    let cm = md.row_groups[rg].columns[col]
        .meta_data
        .as_ref()
        .ok_or_else(|| ext("column missing meta_data"))?;
    let total = cm.num_values as usize;
    let codec = cm.codec;
    let start = cm
        .dictionary_page_offset
        .filter(|&d| d < cm.data_page_offset)
        .unwrap_or(cm.data_page_offset) as u64;
    let length = cm.total_compressed_size as u64;
    let chunk = file
        .read_range(start, length)
        .map_err(|e| ext(format!("read_range: {e}")))?;

    let mut walker = PageWalker::new(&chunk);
    let mut scratch: Vec<u8> = Vec::with_capacity(128 * 1024);

    // Backing data buffer. Pre-reserve to the chunk's uncompressed
    // size (slight over-allocation for dict columns since the bytes
    // are written once, indexed many times, but for PLAIN-only the
    // estimate is right).
    let mut data: Vec<u8> = Vec::with_capacity(cm.total_uncompressed_size as usize);
    let mut views: Vec<u128> = Vec::with_capacity(total);

    // Dict offsets within `data` for the column's dictionary, if any.
    let mut dict_offsets: Vec<u32> = Vec::new();
    let mut dict_lengths: Vec<u32> = Vec::new();

    let (first_hdr, first_body) = walker
        .next_page()
        .map_err(|e| ext(format!("next_page (first): {e}")))?
        .ok_or_else(|| ext("empty chunk"))?;
    decompress_into(codec, first_body, &mut scratch)?;

    if first_hdr.dictionary_page_header.is_some() {
        // Decode dict; append every entry to `data` and remember its
        // (offset, length).
        let entries = decode_plain_byte_array(&scratch)
            .map_err(|e| ext(format!("plain byte_array dict: {e}")))?;
        for s in &entries {
            let off = data.len() as u32;
            data.extend_from_slice(s);
            dict_offsets.push(off);
            dict_lengths.push(s.len() as u32);
        }
    } else {
        // First page IS a data page; handle it inline (PLAIN-only column).
        let dph = first_hdr
            .data_page_header
            .as_ref()
            .ok_or_else(|| ext("first page neither dict nor v1 data"))?;
        if !matches!(dph.encoding, Encoding::Plain) {
            return Err(ext(format!(
                "first page is data but encoding {:?} (need dict context)",
                dph.encoding
            )));
        }
        let n = dph.num_values as usize;
        let slices =
            decode_plain_byte_array(&scratch).map_err(|e| ext(format!("plain byte_array: {e}")))?;
        for s in slices.iter().take(n) {
            push_plain_view(&mut data, &mut views, s);
        }
    }

    while views.len() < total {
        let (hdr, body) = walker
            .next_page()
            .map_err(|e| ext(format!("next_page: {e}")))?
            .ok_or_else(|| ext("chunk ended before num_values"))?;
        let dph = hdr
            .data_page_header
            .as_ref()
            .ok_or_else(|| ext("v2 pages not yet supported"))?;
        let n = dph.num_values as usize;
        decompress_into(codec, body, &mut scratch)?;
        match dph.encoding {
            Encoding::RleDictionary | Encoding::PlainDictionary => {
                let idxs = ematix_parquet_codec::dict::decode_rle_dictionary_indices(&scratch, n)
                    .map_err(|e| ext(format!("rle_dict_indices byte_array: {e}")))?;
                let dict_len = dict_offsets.len();
                for &i in &idxs {
                    let i = i as usize;
                    if i >= dict_len {
                        return Err(ext(format!("dict idx {i} out of range {dict_len}")));
                    }
                    let off = dict_offsets[i];
                    let len = dict_lengths[i];
                    let bytes = &data[off as usize..(off + len) as usize];
                    views.push(make_view(bytes, 0, off));
                }
            }
            Encoding::Plain => {
                let slices = decode_plain_byte_array(&scratch)
                    .map_err(|e| ext(format!("plain byte_array: {e}")))?;
                for s in slices.iter().take(n) {
                    push_plain_view(&mut data, &mut views, s);
                }
            }
            other => {
                return Err(ext(format!(
                    "unexpected byte_array data page encoding {other:?}"
                )));
            }
        }
    }

    debug_assert_eq!(views.len(), total);

    // For string > 12 bytes the view encodes `block_id = 0` (we push
    // the whole data block as buffer 0). For string ≤ 12 bytes the
    // view is fully inline and the data buffer entry is unused; this
    // is fine — the data block still exists.
    let data_buffer = Buffer::from_vec(data);
    Ok(DecodedColumn::StringView {
        views,
        data: data_buffer,
    })
}

/// Append `bytes` to `data` and push a view that points at the new
/// offset. For ≤ 12-byte strings `make_view` returns a fully-inline
/// view (the offset is ignored), so we still append (cheap memcpy)
/// to keep the data block dense for downstream consumers that may
/// scan it linearly.
#[inline]
fn push_plain_view(data: &mut Vec<u8>, views: &mut Vec<u128>, bytes: &[u8]) {
    let off = data.len() as u32;
    data.extend_from_slice(bytes);
    views.push(make_view(bytes, 0, off));
}

/// BYTE_ARRAY → `DictionaryArray<UInt32, Utf8>` — same shape as
/// `ematix_parquet_bridge::decode_column_chunk_byte_array_dict_preserved`
/// but stashed in the `DecodedColumn::DictUtf8` variant so per-batch
/// slicing produces a `DictionaryArray` with the original `values`
/// shared across slices.
fn decode_byte_array_dict_preserved(
    file: &ParquetFile,
    rg: usize,
    col: usize,
) -> DfResult<DecodedColumn> {
    let raw = read_column_byte_array_dict_preserved(file, rg, col)
        .map_err(|e| ext(format!("dict_preserved (rg={rg}, col={col}): {e}")))?;

    let dict_len = raw.dict_offsets.len() - 1;
    let mut dict_strings: Vec<&str> = Vec::with_capacity(dict_len);
    for i in 0..dict_len {
        let s = raw.dict_offsets[i] as usize;
        let e = raw.dict_offsets[i + 1] as usize;
        let bytes = &raw.dict_bytes[s..e];
        let txt = std::str::from_utf8(bytes)
            .map_err(|err| ext(format!("dict entry {i} not valid UTF-8: {err}")))?;
        dict_strings.push(txt);
    }
    let values = Arc::new(StringArray::from(dict_strings));
    Ok(DecodedColumn::DictUtf8 {
        values,
        indices: raw.indices,
    })
}

/// Slow path: BYTE_ARRAY → `StringArray` row-by-row, using
/// `StringBuilder`. Kept for completeness; callers should prefer
/// `Utf8View` or `Dictionary(UInt32, Utf8)` on the hot path.
fn decode_byte_array_to_utf8(file: &ParquetFile, rg: usize, col: usize) -> DfResult<DecodedColumn> {
    let md = file.metadata().map_err(|e| ext(format!("metadata: {e}")))?;
    let cm = md.row_groups[rg].columns[col]
        .meta_data
        .as_ref()
        .ok_or_else(|| ext("column missing meta_data"))?;
    let total = cm.num_values as usize;
    let codec = cm.codec;
    let start = cm
        .dictionary_page_offset
        .filter(|&d| d < cm.data_page_offset)
        .unwrap_or(cm.data_page_offset) as u64;
    let length = cm.total_compressed_size as u64;
    let chunk = file
        .read_range(start, length)
        .map_err(|e| ext(format!("read_range: {e}")))?;

    let mut walker = PageWalker::new(&chunk);
    let mut scratch: Vec<u8> = Vec::with_capacity(128 * 1024);
    let mut builder = StringBuilder::with_capacity(total, cm.total_uncompressed_size as usize);

    let (first_hdr, first_body) = walker
        .next_page()
        .map_err(|e| ext(format!("next_page (first): {e}")))?
        .ok_or_else(|| ext("empty chunk"))?;
    decompress_into(codec, first_body, &mut scratch)?;

    let dict: Vec<Vec<u8>> = if first_hdr.dictionary_page_header.is_some() {
        let slices = decode_plain_byte_array(&scratch)
            .map_err(|e| ext(format!("plain byte_array dict: {e}")))?;
        slices.iter().map(|s| s.to_vec()).collect()
    } else {
        let dph = first_hdr
            .data_page_header
            .as_ref()
            .ok_or_else(|| ext("first page neither dict nor v1 data"))?;
        let n = dph.num_values as usize;
        let slices =
            decode_plain_byte_array(&scratch).map_err(|e| ext(format!("plain byte_array: {e}")))?;
        for s in slices.iter().take(n) {
            append_utf8(&mut builder, s)?;
        }
        Vec::new()
    };

    while builder.len() < total {
        let (hdr, body) = walker
            .next_page()
            .map_err(|e| ext(format!("next_page: {e}")))?
            .ok_or_else(|| ext("chunk ended before num_values"))?;
        let dph = hdr
            .data_page_header
            .as_ref()
            .ok_or_else(|| ext("v2 pages not yet supported"))?;
        let n = dph.num_values as usize;
        decompress_into(codec, body, &mut scratch)?;
        match dph.encoding {
            Encoding::RleDictionary | Encoding::PlainDictionary => {
                let idxs = ematix_parquet_codec::dict::decode_rle_dictionary_indices(&scratch, n)
                    .map_err(|e| ext(format!("rle_dict_indices: {e}")))?;
                for &i in &idxs {
                    let s = dict
                        .get(i as usize)
                        .ok_or_else(|| ext(format!("dict idx {i} out of range {}", dict.len())))?;
                    append_utf8(&mut builder, s)?;
                }
            }
            Encoding::Plain => {
                let slices = decode_plain_byte_array(&scratch)
                    .map_err(|e| ext(format!("plain byte_array: {e}")))?;
                for s in slices.iter().take(n) {
                    append_utf8(&mut builder, s)?;
                }
            }
            other => {
                return Err(ext(format!(
                    "unexpected byte_array data page encoding {other:?}"
                )));
            }
        }
    }
    debug_assert_eq!(builder.len(), total);
    Ok(DecodedColumn::Utf8(Arc::new(builder.finish())))
}

#[inline]
fn append_utf8(builder: &mut StringBuilder, bytes: &[u8]) -> DfResult<()> {
    let s = std::str::from_utf8(bytes).map_err(|e| ext(format!("byte_array not UTF-8: {e}")))?;
    builder.append_value(s);
    Ok(())
}

// ============================================================
// Batch slicing
// ============================================================

/// Build an `ArrayRef` covering `[start, start+n)` of the decoded
/// per-RG buffer. For primitives this is a single Buffer slice
/// (zero-copy). For `StringView` we share the backing data buffer
/// across all slices and emit a fresh views array per slice.
/// For `DictUtf8` we share the `values` and slice the indices.
fn slice_decoded(c: &DecodedColumn, start: usize, n: usize, target: &DataType) -> ArrayRef {
    match c {
        DecodedColumn::Int32(v) => {
            // Same underlying i32 buffer for both Int32 and Date32;
            // pick the Arrow type by target.
            let buf = Buffer::from_slice_ref(&v[start..start + n]);
            let scalar = ScalarBuffer::<i32>::new(buf, 0, n);
            match target {
                DataType::Date32 => Arc::new(Date32Array::new(scalar, None)),
                _ => Arc::new(Int32Array::new(scalar, None)),
            }
        }
        DecodedColumn::Int64(v) => {
            let buf = Buffer::from_slice_ref(&v[start..start + n]);
            let scalar = ScalarBuffer::<i64>::new(buf, 0, n);
            Arc::new(Int64Array::new(scalar, None))
        }
        DecodedColumn::Float64(v) => {
            let buf = Buffer::from_slice_ref(&v[start..start + n]);
            let scalar = ScalarBuffer::<f64>::new(buf, 0, n);
            Arc::new(Float64Array::new(scalar, None))
        }
        DecodedColumn::StringView { views, data } => {
            // Build a sliced views buffer; data buffer is shared.
            let view_slice: Vec<u128> = views[start..start + n].to_vec();
            let views_buf = ScalarBuffer::<u128>::from(view_slice);
            // SAFETY: we built every view ourselves with `make_view`
            // against `data` so the offsets are valid and the bytes
            // are valid UTF-8 (decode_plain_byte_array round-trips
            // the parquet bytes which are UTF-8 by Utf8 logical type).
            //
            // We use the safe `try_new` since the upfront cost of
            // validating views is negligible relative to decode time
            // and gives us a defensive check.
            let arr = StringViewArray::try_new(views_buf, vec![data.clone()], None::<NullBuffer>)
                .expect("StringViewArray::try_new on internally-built views");
            Arc::new(arr)
        }
        DecodedColumn::DictUtf8 { values, indices } => {
            let key_slice = indices[start..start + n].to_vec();
            let keys = UInt32Array::from(key_slice);
            let arr = DictionaryArray::<UInt32Type>::try_new(keys, values.clone() as ArrayRef)
                .expect("DictionaryArray::try_new on internally-built dict");
            Arc::new(arr)
        }
        DecodedColumn::Utf8(s) => {
            // Arrow array slice is O(1) — shares the offsets/values
            // buffers, just bumps the offset.
            Arc::new(StringArray::from(s.slice(start, n).to_data()))
        }
    }
}

// ============================================================
// Helpers
// ============================================================

#[inline]
fn ext<S: Into<String>>(msg: S) -> DataFusionError {
    DataFusionError::External(format!("emat_arrow_reader: {}", msg.into()).into())
}

/// Codec-aware decompress helper — same shape as the bridge's.
fn decompress_into(codec: CompressionCodec, body: &[u8], out: &mut Vec<u8>) -> DfResult<()> {
    match codec {
        CompressionCodec::Uncompressed => {
            out.clear();
            out.extend_from_slice(body);
            Ok(())
        }
        CompressionCodec::Snappy => {
            decompress_snappy_into(body, out).map_err(|e| ext(format!("snappy: {e}")))
        }
        CompressionCodec::Zstd => {
            decompress_zstd_into(body, out).map_err(|e| ext(format!("zstd: {e}")))
        }
        other => Err(ext(format!(
            "codec {other:?} not yet supported in emat_arrow_reader"
        ))),
    }
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_schema::{Field, Schema};
    use ematix_parquet_codec::write::ColumnData;
    use ematix_parquet_codec::write::{
        write_table_to_path, write_table_to_path_with_row_group_size, write_table_with_dict_to_path,
    };
    use ematix_parquet_format::types::CompressionCodec;

    fn tmp_parquet(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "emat_arrow_reader_test_{}_{}",
            std::process::id(),
            name
        ));
        let _ = std::fs::create_dir_all(&dir);
        dir.join(format!("{name}.parquet"))
    }

    fn write_three_primitives(path: &std::path::Path, n: usize) {
        let i32s: Vec<i32> = (0..n as i32).collect();
        let i64s: Vec<i64> = (0..n as i64).map(|x| x * 7).collect();
        let f64s: Vec<f64> = (0..n).map(|x| x as f64 * 0.5).collect();
        write_table_to_path(
            path,
            &[
                ("c_i32", ColumnData::I32(&i32s)),
                ("c_i64", ColumnData::I64(&i64s)),
                ("c_f64", ColumnData::F64(&f64s)),
            ],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
    }

    fn schema_three_primitives() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("c_i32", DataType::Int32, false),
            Field::new("c_i64", DataType::Int64, false),
            Field::new("c_f64", DataType::Float64, false),
        ]))
    }

    #[test]
    fn roundtrip_i32_i64_f64() {
        let path = tmp_parquet("roundtrip");
        let n = 1024;
        write_three_primitives(&path, n);

        let file = ParquetFile::open(&path).unwrap();
        let reader = EmatArrowBatchReaderBuilder::new(file, schema_three_primitives())
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, n);
        assert_eq!(batches[0].num_columns(), 3);
        // Spot-check first batch values.
        let i32_arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..i32_arr.len() {
            assert_eq!(i32_arr.value(i), i as i32);
        }
        let f64_arr = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(f64_arr.value(0), 0.0);
        assert_eq!(f64_arr.value(1), 0.5);
    }

    #[test]
    fn streaming_emits_65k_chunks() {
        let path = tmp_parquet("streaming");
        // > 65_536 rows in one RG. Write a single-column file (lighter).
        let n: usize = 200_000;
        let xs: Vec<i32> = (0..n as i32).collect();
        write_table_to_path(
            &path,
            &[("c", ColumnData::I32(&xs))],
            CompressionCodec::Uncompressed,
        )
        .unwrap();

        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("c", DataType::Int32, false)]));
        let file = ParquetFile::open(&path).unwrap();
        let reader = EmatArrowBatchReaderBuilder::new(file, schema)
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        assert!(
            batches.len() > 1,
            "expected > 1 batch, got {}",
            batches.len()
        );
        // All but the last should be exactly 65_536 rows.
        for b in &batches[..batches.len() - 1] {
            assert_eq!(b.num_rows(), DEFAULT_BATCH_SIZE);
        }
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), n);
    }

    #[test]
    fn utf8view_promotion() {
        let path = tmp_parquet("utf8view");
        // 3 distinct strings, repeated; dict-encoded.
        let raw = [b"alpha".as_slice(), b"beta".as_slice(), b"gamma".as_slice()];
        let mut rows: Vec<&[u8]> = Vec::with_capacity(300);
        for i in 0..300 {
            rows.push(raw[i % 3]);
        }
        write_table_with_dict_to_path(
            &path,
            &[("s", ColumnData::ByteArray(&rows))],
            CompressionCodec::Uncompressed,
            usize::MAX,
            &[true],
        )
        .unwrap();

        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Utf8View,
            false,
        )]));
        let file = ParquetFile::open(&path).unwrap();
        let reader = EmatArrowBatchReaderBuilder::new(file, schema)
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 300);

        // Concatenate values and compare row-wise.
        let mut got: Vec<String> = Vec::with_capacity(300);
        for b in &batches {
            let arr = b
                .column(0)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("expected StringViewArray for Utf8View target");
            for i in 0..arr.len() {
                got.push(arr.value(i).to_string());
            }
        }
        for (i, g) in got.iter().enumerate() {
            assert_eq!(g.as_bytes(), raw[i % 3], "mismatch at row {i}");
        }
    }

    #[test]
    fn dictionary_preservation() {
        let path = tmp_parquet("dict");
        let raw = [b"R".as_slice(), b"A".as_slice(), b"N".as_slice()];
        let mut rows: Vec<&[u8]> = Vec::with_capacity(900);
        for i in 0..900 {
            rows.push(raw[i % 3]);
        }
        write_table_with_dict_to_path(
            &path,
            &[("flag", ColumnData::ByteArray(&rows))],
            CompressionCodec::Uncompressed,
            usize::MAX,
            &[true],
        )
        .unwrap();

        let dict_ty = DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8));
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("flag", dict_ty, false)]));
        let file = ParquetFile::open(&path).unwrap();
        let reader = EmatArrowBatchReaderBuilder::new(file, schema)
            .build()
            .unwrap();

        let mut total = 0usize;
        let mut row_idx = 0usize;
        for b in reader {
            let b = b.unwrap();
            let dict_arr = b
                .column(0)
                .as_any()
                .downcast_ref::<DictionaryArray<UInt32Type>>()
                .expect("expected DictionaryArray<UInt32Type>");
            let values = dict_arr.values().as_string::<i32>();
            // Sanity: dict values should be small (3 distinct).
            assert!(values.len() <= 8);
            let keys = dict_arr.keys();
            for i in 0..keys.len() {
                let k = keys.value(i) as usize;
                let s = values.value(k);
                assert_eq!(s.as_bytes(), raw[row_idx % 3]);
                row_idx += 1;
            }
            total += b.num_rows();
        }
        assert_eq!(total, 900);
    }

    #[test]
    fn projection_subset() {
        // 5 columns, project 2 (indices 1 + 3).
        let path = tmp_parquet("projection");
        let n = 128usize;
        let a: Vec<i32> = (0..n as i32).collect();
        let b: Vec<i32> = (0..n as i32).map(|x| x + 1000).collect();
        let c: Vec<i64> = (0..n as i64).collect();
        let d: Vec<f64> = (0..n).map(|x| x as f64).collect();
        let e: Vec<i32> = (0..n as i32).map(|x| x - 50).collect();
        write_table_to_path(
            &path,
            &[
                ("a", ColumnData::I32(&a)),
                ("b", ColumnData::I32(&b)),
                ("c", ColumnData::I64(&c)),
                ("d", ColumnData::F64(&d)),
                ("e", ColumnData::I32(&e)),
            ],
            CompressionCodec::Uncompressed,
        )
        .unwrap();

        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Int32, false),
            Field::new("d", DataType::Float64, false),
        ]));
        let file = ParquetFile::open(&path).unwrap();
        let reader = EmatArrowBatchReaderBuilder::new(file, schema)
            .with_projection(vec![1, 3])
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = reader.map(|x| x.unwrap()).collect();
        assert_eq!(batches.len(), 1);
        let rb = &batches[0];
        assert_eq!(rb.num_columns(), 2);
        let b_arr = rb.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let d_arr = rb
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(b_arr.value(0), 1000);
        assert_eq!(d_arr.value(10), 10.0);
    }

    #[test]
    fn row_group_selection() {
        let path = tmp_parquet("rg_select");
        // 3 RGs of 100 rows each.
        let n_per_rg = 100usize;
        let n = n_per_rg * 3;
        let xs: Vec<i32> = (0..n as i32).collect();
        write_table_to_path_with_row_group_size(
            &path,
            &[("c", ColumnData::I32(&xs))],
            CompressionCodec::Uncompressed,
            n_per_rg,
        )
        .unwrap();

        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("c", DataType::Int32, false)]));
        let file = ParquetFile::open(&path).unwrap();
        let reader = EmatArrowBatchReaderBuilder::new(file, schema)
            .with_row_groups(vec![1])
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, n_per_rg);
        // RG 1 should hold values 100..200.
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(arr.value(0), 100);
        assert_eq!(arr.value(arr.len() - 1), 199);
    }

    #[test]
    fn crosses_row_group_boundary_safely() {
        let path = tmp_parquet("rg_boundary");
        // 2 RGs of 100_000 rows each. batch_size = 65_536 < RG size.
        let n_per_rg = 100_000usize;
        let n = n_per_rg * 2;
        let xs: Vec<i32> = (0..n as i32).collect();
        write_table_to_path_with_row_group_size(
            &path,
            &[("c", ColumnData::I32(&xs))],
            CompressionCodec::Uncompressed,
            n_per_rg,
        )
        .unwrap();

        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("c", DataType::Int32, false)]));
        let file = ParquetFile::open(&path).unwrap();
        let reader = EmatArrowBatchReaderBuilder::new(file, schema)
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, n);
        // Each batch must come from a single RG, i.e. its first and
        // last values must lie in the same `[k * n_per_rg, (k+1)*
        // n_per_rg)` window. With the row-id pattern xs[i] = i this
        // means `first / n_per_rg == last / n_per_rg`.
        for b in &batches {
            let arr = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            let first = arr.value(0) as usize;
            let last = arr.value(arr.len() - 1) as usize;
            assert_eq!(
                first / n_per_rg,
                last / n_per_rg,
                "batch crosses RG boundary: first={first} last={last}"
            );
        }
    }

    #[test]
    fn utf8view_zero_copy_or_efficient() {
        // For N rows of avg length L, the backing buffer size should
        // be bounded by ~dict_unique_bytes + a small fixed overhead.
        // We assert it's at most N * L (the worst case of *every* row
        // copied into the buffer). For a dict-encoded column with K
        // distinct values, it should be far less.
        let path = tmp_parquet("utf8view_efficient");
        let n = 50_000usize;
        // Use long strings (> 12 bytes) so views point at the backing
        // buffer rather than inlining. Without dict reuse, copying
        // every row would burn n * 20 bytes ≈ 1 MB; with dict reuse
        // it should be 3 * 20 + change.
        let raw = [
            b"alpha_quite_long_string".as_slice(),
            b"beta_quite_long_string".as_slice(),
            b"gamma_quite_long_string".as_slice(),
        ];
        let avg_len: usize = raw.iter().map(|s| s.len()).sum::<usize>() / raw.len();
        let mut rows: Vec<&[u8]> = Vec::with_capacity(n);
        for i in 0..n {
            rows.push(raw[i % raw.len()]);
        }
        write_table_with_dict_to_path(
            &path,
            &[("s", ColumnData::ByteArray(&rows))],
            CompressionCodec::Uncompressed,
            usize::MAX,
            &[true],
        )
        .unwrap();

        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Utf8View,
            false,
        )]));
        let file = ParquetFile::open(&path).unwrap();
        let reader = EmatArrowBatchReaderBuilder::new(file, schema)
            .build()
            .unwrap();
        let mut total_backing_bytes = 0usize;
        let mut total_rows = 0usize;
        for b in reader {
            let b = b.unwrap();
            total_rows += b.num_rows();
            let arr = b
                .column(0)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap();
            // data_buffers() returns the backing blocks; each slice
            // shares the same Arc<Buffer> so we count unique ones
            // by pointer identity.
            for buf in arr.data_buffers() {
                total_backing_bytes += buf.len();
            }
        }
        assert_eq!(total_rows, n);
        // We share one data block per RG across all slices of that
        // RG, but each slice clones the Buffer (`Arc` bump) into its
        // own array — so the sum here counts the block once per
        // *slice*, not once per *RG*. The relevant upper bound is
        // `(slice_count) * dict_unique_bytes`, comfortably <
        // `n * avg_len` for any K << n.
        let dict_unique_bytes: usize = raw.iter().map(|s| s.len()).sum();
        let upper_bound_per_slice = dict_unique_bytes + 64; // overhead slack
        let n_slices = n.div_ceil(DEFAULT_BATCH_SIZE);
        let upper_bound = upper_bound_per_slice * n_slices;
        assert!(
            total_backing_bytes <= upper_bound,
            "total backing bytes {total_backing_bytes} > upper bound {upper_bound} \
             (dict_unique={dict_unique_bytes}, slices={n_slices}, avg_len={avg_len}, n={n})"
        );
        // Crisper sanity: total backing bytes per row is « avg_len.
        assert!(
            total_backing_bytes < n * avg_len,
            "backing bytes {total_backing_bytes} reached row-by-row materialisation bound"
        );
    }
}
