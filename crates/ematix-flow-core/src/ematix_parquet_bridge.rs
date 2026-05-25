//! Bridge: ematix-parquet kernels → Arrow arrays.
//!
//! Decodes a parquet column chunk to an Arrow array using the
//! sibling-repo ematix-parquet kernels (Phase 5/6 + NEON bw=12/17 +
//! bitmap-driven sparse gather) instead of parquet-rs's
//! `ParquetRecordBatchReader`.
//!
//! Today's surface (Phase 1 of the integration):
//!   - `decode_column_chunk_i32` — INT32 / Date32 column, RLE_DICT
//!     + PLAIN data pages, optional dict page.
//!   - `decode_column_chunk_i64` — INT64, same shape.
//!   - `decode_column_chunk_f64` — DOUBLE, same shape.
//!
//! All functions return Arrow arrays (Int32Array, Int64Array,
//! Float64Array). Nullability is not yet supported — the column is
//! assumed REQUIRED (Q14's lineitem columns all qualify). Multi-RG
//! caller responsibility for now.
//!
//! The integration roadmap:
//!   - Phase 1 (this module): scalar-shape decoders, no levels, no
//!     filtering. Validates the bridge plumbs through correctly.
//!   - Phase 2: alternate `EmatixFastParquetExec` ExecutionPlan that
//!     uses these decoders behind a TableProvider. Behind a runtime
//!     opt-in so we can A/B against the parquet-rs path.
//!   - Phase 3: predicate pushdown via Phase 5's fused bitmap
//!     pattern at the exec layer (the biggest projected gain).
//!
//! Q14 lever: the late-mat path on `EmatixFastParquetTableProvider`
//! (Π.10 `read_column_*_masked_into`) is the canonical Q14
//! implementation and replaced the older bespoke fused exec.

use std::path::Path;
use std::sync::Arc;

use arrow_array::builder::ArrayBuilder;
use arrow_array::types::UInt32Type;
use arrow_array::{
    Array, DictionaryArray, Float64Array, Int32Array, Int64Array, StringArray, UInt32Array,
};
use datafusion::error::{DataFusionError, Result as DfResult};

use ematix_parquet_codec::compression::{
    decompress_lz4_raw_into_sized, decompress_snappy_into, decompress_zstd_into,
};
use ematix_parquet_codec::dict::{
    build_dict_predicate_mask, decode_rle_dictionary_into, decode_rle_dictionary_predicate_bitmap,
    gather_dict_at_bitmap_into,
};
use ematix_parquet_codec::plain::{
    decode_plain_byte_array, decode_plain_f64, decode_plain_i32, decode_plain_i64,
};
use ematix_parquet_codec::read::{
    read_column_byte_array_dict_preserved, read_column_byte_array_dict_preserved_into,
    read_column_byte_array_masked_into, read_column_f64_masked_into, read_column_i32_masked_into,
    read_column_i64_masked_into,
};
use ematix_parquet_format::types::{CompressionCodec, Encoding};
use ematix_parquet_io::{PageWalker, ParquetFile};

/// Decode an INT32 column chunk to a contiguous `Int32Array`.
///
/// Walks dict + data pages, dispatching on per-page encoding. The
/// dict (if present) is PLAIN-decoded once and reused across all
/// data pages. Decompression buffer is reused across pages (no per-
/// page allocation).
pub fn decode_column_chunk_i32(path: &Path, rg: usize, col: usize) -> DfResult<Arc<Int32Array>> {
    let buf = decode_dict_chunk_generic::<i32>(path, rg, col, |bytes| {
        decode_plain_i32(bytes).map_err(|e| ext(format!("plain i32: {e}")))
    })?;
    Ok(Arc::new(Int32Array::from(buf)))
}

/// Decode an INT64 column chunk to a contiguous `Int64Array`.
pub fn decode_column_chunk_i64(path: &Path, rg: usize, col: usize) -> DfResult<Arc<Int64Array>> {
    let buf = decode_dict_chunk_generic::<i64>(path, rg, col, |bytes| {
        decode_plain_i64(bytes).map_err(|e| ext(format!("plain i64: {e}")))
    })?;
    Ok(Arc::new(Int64Array::from(buf)))
}

/// Decode a DOUBLE column chunk to a contiguous `Float64Array`.
pub fn decode_column_chunk_f64(path: &Path, rg: usize, col: usize) -> DfResult<Arc<Float64Array>> {
    let buf = decode_dict_chunk_generic::<f64>(path, rg, col, |bytes| {
        decode_plain_f64(bytes).map_err(|e| ext(format!("plain f64: {e}")))
    })?;
    Ok(Arc::new(Float64Array::from(buf)))
}

/// Decode a BYTE_ARRAY column chunk (Utf8 logical type) to a
/// contiguous `StringArray`. Handles PLAIN and RLE_DICTIONARY data
/// pages. Owned-byte output — copies dictionary entries into the
/// values buffer for each row (no zero-copy yet; that'd need a
/// custom Arrow buffer reuse).
///
/// REQUIRED columns only — no nulls. Phase 4 v1.
pub fn decode_column_chunk_byte_array(
    path: &Path,
    rg: usize,
    col: usize,
) -> DfResult<Arc<StringArray>> {
    let file = ParquetFile::open(path).map_err(|e| ext(format!("ParquetFile::open: {e}")))?;
    let md = file.metadata().map_err(|e| ext(format!("metadata: {e}")))?;
    let cm = md.row_groups[rg].columns[col]
        .meta_data
        .as_ref()
        .ok_or_else(|| ext("column missing meta_data"))?;
    let total = cm.num_values as usize;
    let codec = cm.codec;

    let start = cm.dictionary_page_offset.unwrap_or(cm.data_page_offset) as u64;
    let length = cm.total_compressed_size as u64;
    let chunk = file
        .read_range(start, length)
        .map_err(|e| ext(format!("read_range: {e}")))?;

    let mut walker = PageWalker::new(&chunk);
    let mut scratch: Vec<u8> = Vec::with_capacity(128 * 1024);

    // Use Arrow's StringBuilder which handles offsets + values
    // buffers + UTF-8 validation. Pre-size by total values + a rough
    // bytes estimate.
    let mut builder = arrow_array::builder::StringBuilder::with_capacity(
        total,
        cm.total_uncompressed_size as usize,
    );

    // Peek first page: dict or directly a data page.
    let (first_hdr, first_body) = walker
        .next_page()
        .map_err(|e| ext(format!("next_page (first): {e}")))?
        .ok_or_else(|| ext("empty chunk"))?;
    decompress_into(
        codec,
        first_body,
        page_uncompressed_size(&first_hdr)?,
        &mut scratch,
    )?;

    // If first is a dict page, decode it to an owned Vec<Vec<u8>> so
    // each lookup copies into `values`. Borrowed slices won't survive
    // the scratch buffer reuse across pages.
    let dict: Vec<Vec<u8>> = if first_hdr.dictionary_page_header.is_some() {
        let borrowed = decode_plain_byte_array(&scratch)
            .map_err(|e| ext(format!("plain byte_array dict: {e}")))?;
        borrowed.iter().map(|s| s.to_vec()).collect()
    } else {
        let dph = first_hdr
            .data_page_header
            .as_ref()
            .ok_or_else(|| ext("first page neither dict nor v1 data"))?;
        let n = dph.num_values as usize;
        match dph.encoding {
            Encoding::Plain => {
                let borrowed = decode_plain_byte_array(&scratch)
                    .map_err(|e| ext(format!("plain byte_array: {e}")))?;
                for s in borrowed.iter().take(n) {
                    append_utf8(&mut builder, s)?;
                }
            }
            other => {
                return Err(ext(format!(
                    "first page is data but encoding {other:?} for byte_array (need dict)"
                )));
            }
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
            .ok_or_else(|| ext("v2 byte_array pages not yet supported"))?;
        let n = dph.num_values as usize;
        decompress_into(codec, body, page_uncompressed_size(&hdr)?, &mut scratch)?;
        match dph.encoding {
            Encoding::RleDictionary | Encoding::PlainDictionary => {
                let idxs = ematix_parquet_codec::dict::decode_rle_dictionary_indices(&scratch, n)
                    .map_err(|e| ext(format!("rle_dict_indices byte_array: {e}")))?;
                for &i in &idxs {
                    let s = dict
                        .get(i as usize)
                        .ok_or_else(|| ext(format!("dict idx {i} out of range {}", dict.len())))?;
                    append_utf8(&mut builder, s)?;
                }
            }
            Encoding::Plain => {
                let borrowed = decode_plain_byte_array(&scratch)
                    .map_err(|e| ext(format!("plain byte_array: {e}")))?;
                for s in borrowed.iter().take(n) {
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
    Ok(Arc::new(builder.finish()))
}

/// Decode a BYTE_ARRAY column chunk (Utf8 logical type) to a
/// `DictionaryArray<UInt32, Utf8>` — preserving the parquet
/// dictionary structure end-to-end instead of materialising every
/// row's value into a contiguous `StringArray`.
///
/// Σ.E3b substrate: lets downstream dict-aware operators
/// (`DictGroupCountExec`, `DictFilterExec`) stay on dict codes
/// rather than paying per-batch gather + hash. The values dict is
/// passed through as-is; the indices become Arrow keys of type
/// `UInt32`.
///
/// Errors if the chunk has no dictionary page or if any data page
/// fell back to PLAIN — same surface as the underlying
/// `read_column_byte_array_dict_preserved` API. Callers should
/// fall back to `decode_column_chunk_byte_array` in that case.
pub fn decode_column_chunk_byte_array_dict_preserved(
    path: &Path,
    rg: usize,
    col: usize,
) -> DfResult<Arc<DictionaryArray<UInt32Type>>> {
    let file = ParquetFile::open(path).map_err(|e| ext(format!("ParquetFile::open: {e}")))?;
    let raw = read_column_byte_array_dict_preserved(&file, rg, col).map_err(|e| {
        ext(format!(
            "read_column_byte_array_dict_preserved (rg={rg}, col={col}): {e}"
        ))
    })?;

    // Materialise dict entries as &str. The dict is small (lineitem
    // shipdate has ~2.5k distinct values; returnflag has 3) so the
    // per-entry slice + UTF-8 validate cost is negligible relative
    // to the savings of not gathering every row.
    let dict_len = raw.dict_offsets.len() - 1;
    let mut dict_strings: Vec<&str> = Vec::with_capacity(dict_len);
    for i in 0..dict_len {
        let s = raw.dict_offsets[i] as usize;
        let e = raw.dict_offsets[i + 1] as usize;
        let bytes = &raw.dict_bytes[s..e];
        let txt = std::str::from_utf8(bytes).map_err(|err| {
            ext(format!(
                "dict-preserved decode: dict entry {i} not valid UTF-8: {err}"
            ))
        })?;
        dict_strings.push(txt);
    }
    let values: Arc<dyn Array> = Arc::new(StringArray::from(dict_strings));
    let keys = UInt32Array::from(raw.indices);
    let dict_arr = DictionaryArray::<UInt32Type>::try_new(keys, values)
        .map_err(|e| ext(format!("DictionaryArray::try_new: {e}")))?;
    Ok(Arc::new(dict_arr))
}

#[inline]
fn append_utf8(builder: &mut arrow_array::builder::StringBuilder, bytes: &[u8]) -> DfResult<()> {
    // SAFETY check: parquet Utf8 logical type guarantees UTF-8 but
    // we validate defensively. For dict-encoded columns this runs
    // ~once per ~1000 rows (cached); for PLAIN pages it runs per row.
    let s = std::str::from_utf8(bytes).map_err(|e| ext(format!("byte_array not UTF-8: {e}")))?;
    builder.append_value(s);
    Ok(())
}

/// Generic decoder for a dict-encoded REQUIRED column with a fixed-
/// size primitive type. Handles the dict-page → data-pages pattern,
/// with PLAIN fallback when writers emit dict-overflow pages (common
/// for high-cardinality columns like l_partkey at SF=1).
fn decode_dict_chunk_generic<T: Copy>(
    path: &Path,
    rg: usize,
    col: usize,
    decode_plain: impl Fn(&[u8]) -> DfResult<Vec<T>>,
) -> DfResult<Vec<T>> {
    let file = ParquetFile::open(path).map_err(|e| ext(format!("ParquetFile::open: {e}")))?;
    let md = file.metadata().map_err(|e| ext(format!("metadata: {e}")))?;
    let cm = md.row_groups[rg].columns[col]
        .meta_data
        .as_ref()
        .ok_or_else(|| ext("column missing meta_data"))?;
    let total = cm.num_values as usize;
    let codec = cm.codec;

    let start = cm.dictionary_page_offset.unwrap_or(cm.data_page_offset) as u64;
    let length = cm.total_compressed_size as u64;
    let chunk = file
        .read_range(start, length)
        .map_err(|e| ext(format!("read_range: {e}")))?;

    let mut walker = PageWalker::new(&chunk);
    let mut scratch: Vec<u8> = Vec::with_capacity(128 * 1024);
    let mut out: Vec<T> = Vec::with_capacity(total);

    // Peek first page: dict page (RLE_DICT column) or data page
    // (PLAIN-only column).
    let (first_hdr, first_body) = walker
        .next_page()
        .map_err(|e| ext(format!("next_page (first): {e}")))?
        .ok_or_else(|| ext("empty chunk"))?;
    decompress_into(
        codec,
        first_body,
        page_uncompressed_size(&first_hdr)?,
        &mut scratch,
    )?;

    let dict: Vec<T> = if first_hdr.dictionary_page_header.is_some() {
        decode_plain(&scratch)?
    } else {
        // First page is itself a data page; decode + emit inline.
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
        decompress_into(codec, body, page_uncompressed_size(&hdr)?, &mut scratch)?;
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

#[inline]
fn ext<S: Into<String>>(msg: S) -> DataFusionError {
    DataFusionError::External(format!("ematix_parquet_bridge: {}", msg.into()).into())
}

/// Phase 3: build a row bitmap for an INT32/Date32 column chunk by
/// evaluating a closure-shaped predicate against the column's PLAIN-
/// decoded dict. The dict_mask is sized `1 << bit_width` so the NEON
/// kernel can safely use bounds-free gathers; v0.2.0's width-generic
/// `decode_rle_dictionary_predicate_bitmap` covers bw ∈ {12, 14, 15,
/// 16, 17, 18} on NEON and scalar fallback for other widths.
///
/// Returns the bitmap (1 bit per row, byte-stride) and the row count.
/// `predicate(dict_value) -> bool` is evaluated once per dict entry —
/// negligible cost (~thousands of ops for shipdate-sized dicts) vs
/// the per-row gain.
///
/// If the first data page is not dict-encoded (RLE_DICTIONARY /
/// PLAIN_DICTIONARY), the function returns `Err` so the caller can
/// fall back to a dense decode + filter.
pub fn filter_i32_column_to_bitmap(
    path: &std::path::Path,
    rg: usize,
    col: usize,
    predicate: impl Fn(i32) -> bool,
) -> DfResult<(Vec<u8>, usize)> {
    let file = ParquetFile::open(path).map_err(|e| ext(format!("ParquetFile::open: {e}")))?;
    let md = file.metadata().map_err(|e| ext(format!("metadata: {e}")))?;
    let cm = md.row_groups[rg].columns[col]
        .meta_data
        .as_ref()
        .ok_or_else(|| ext("column missing meta_data"))?;
    let total = cm.num_values as usize;
    let codec = cm.codec;
    let start = cm.dictionary_page_offset.unwrap_or(cm.data_page_offset) as u64;
    let length = cm.total_compressed_size as u64;
    let chunk = file
        .read_range(start, length)
        .map_err(|e| ext(format!("read_range: {e}")))?;

    let mut walker = PageWalker::new(&chunk);
    let mut scratch: Vec<u8> = Vec::with_capacity(128 * 1024);

    // First page must be the dict (Phase 3 v1 requires dict-encoded).
    let (first_hdr, first_body) = walker
        .next_page()
        .map_err(|e| ext(format!("next_page (dict): {e}")))?
        .ok_or_else(|| ext("empty chunk"))?;
    if first_hdr.dictionary_page_header.is_none() {
        return Err(ext(
            "filter_i32_column_to_bitmap requires dict-encoded chunk; \
             use dense decode + scan for PLAIN-only columns",
        ));
    }
    decompress_into(
        codec,
        first_body,
        page_uncompressed_size(&first_hdr)?,
        &mut scratch,
    )?;
    let dict = decode_plain_i32(&scratch).map_err(|e| ext(format!("plain i32 dict: {e}")))?;

    // Workstream 2A: dict-popcount-zero early-skip. If no value in
    // the dict passes the predicate, every dict-encoded data page in
    // this RG will produce zero bitmap matches. Emit an all-zero
    // bitmap and skip the per-page decompress + RLE walk entirely.
    // Dormant on uniformly-distributed TPC-H (every RG's date range
    // overlaps any query range); fires on partitioned/sorted data
    // where some RG's dict is fully outside the filter window.
    if !dict.iter().any(|&v| predicate(v)) {
        let mut bitmap = vec![0u8; total.div_ceil(8)];
        // Defensive: bitmap.len() may exceed total.div_ceil(8) by
        // padding when div_ceil rounds up — keep that as-is so
        // downstream popcount over [0, total) sees only zero bits.
        bitmap.truncate(total.div_ceil(8));
        return Ok((bitmap, total));
    }

    // Walk data pages. Sniff bit_width from the first data page's
    // body[0] so we can size the dict_mask correctly for v0.2.0's
    // width-generic fused-predicate kernel. The bit_width is stable
    // across pages within a chunk by construction (chosen at write
    // time from the dict size).
    let mut bitmap: Vec<u8> = Vec::with_capacity(total.div_ceil(8));
    let mut dict_mask: Vec<u8> = Vec::new();
    let mut emitted: usize = 0;
    while emitted < total {
        let (hdr, body) = walker
            .next_page()
            .map_err(|e| ext(format!("next_page: {e}")))?
            .ok_or_else(|| ext("chunk ended before num_values"))?;
        let dph = hdr
            .data_page_header
            .as_ref()
            .ok_or_else(|| ext("filter_i32: v2 data pages not yet supported in Phase 3"))?;
        let n = dph.num_values as usize;
        decompress_into(codec, body, page_uncompressed_size(&hdr)?, &mut scratch)?;
        match dph.encoding {
            Encoding::RleDictionary | Encoding::PlainDictionary => {
                if dict_mask.is_empty() {
                    if scratch.is_empty() {
                        return Err(ext("filter_i32: empty data page body"));
                    }
                    let bit_width = scratch[0];
                    dict_mask = build_dict_predicate_mask(&dict, bit_width, |&v| predicate(v))
                        .map_err(|e| ext(format!("build_dict_predicate_mask: {e}")))?;
                }
                decode_rle_dictionary_predicate_bitmap(&scratch, n, &dict_mask, &mut bitmap)
                    .map_err(|e| ext(format!("rle_dict_predicate_bitmap: {e}")))?;
            }
            other => {
                return Err(ext(format!(
                    "filter_i32: unexpected data page encoding {other:?} \
                     (Phase 3 v1 dict-only)"
                )));
            }
        }
        emitted += n;
    }
    Ok((bitmap, total))
}

/// Phase 6 wrapper for a typed dict-encoded column: bitmap-driven
/// sparse-gather of values at bitmap-true rows. Returns a Vec<T> of
/// length `popcount(bitmap)`. Caller wraps in an Arrow array.
fn gather_chunk_typed<T: Copy>(
    path: &std::path::Path,
    rg: usize,
    col: usize,
    bitmap: &[u8],
    decode_dict_plain: impl Fn(&[u8]) -> DfResult<Vec<T>>,
    decode_plain_page: impl Fn(&[u8]) -> DfResult<Vec<T>>,
) -> DfResult<Vec<T>> {
    let file = ParquetFile::open(path).map_err(|e| ext(format!("ParquetFile::open: {e}")))?;
    let md = file.metadata().map_err(|e| ext(format!("metadata: {e}")))?;
    let cm = md.row_groups[rg].columns[col]
        .meta_data
        .as_ref()
        .ok_or_else(|| ext("column missing meta_data"))?;
    let total = cm.num_values as usize;
    let codec = cm.codec;
    let start = cm.dictionary_page_offset.unwrap_or(cm.data_page_offset) as u64;
    let length = cm.total_compressed_size as u64;
    let chunk = file
        .read_range(start, length)
        .map_err(|e| ext(format!("read_range: {e}")))?;

    let mut walker = PageWalker::new(&chunk);
    let mut scratch: Vec<u8> = Vec::with_capacity(128 * 1024);

    let (first_hdr, first_body) = walker
        .next_page()
        .map_err(|e| ext(format!("next_page (first): {e}")))?
        .ok_or_else(|| ext("empty chunk"))?;
    decompress_into(
        codec,
        first_body,
        page_uncompressed_size(&first_hdr)?,
        &mut scratch,
    )?;

    let dict: Vec<T> = if first_hdr.dictionary_page_header.is_some() {
        decode_dict_plain(&scratch)?
    } else {
        Vec::new()
    };

    let mut out: Vec<T> = Vec::with_capacity(bitmap.iter().map(|b| b.count_ones() as usize).sum());
    let mut emitted: usize = 0;

    // If first page was a data page, decode + sparse-emit inline.
    if first_hdr.dictionary_page_header.is_none() {
        let dph = first_hdr
            .data_page_header
            .as_ref()
            .ok_or_else(|| ext("first page neither dict nor v1 data"))?;
        let n = dph.num_values as usize;
        if !matches!(dph.encoding, Encoding::Plain) {
            return Err(ext(format!(
                "non-dict first page must be PLAIN (got {:?})",
                dph.encoding
            )));
        }
        let vals = decode_plain_page(&scratch)?;
        for (i, &v) in vals.iter().enumerate().take(n) {
            if (bitmap[i / 8] >> (i % 8)) & 1 == 1 {
                out.push(v);
            }
        }
        emitted = n;
    }

    while emitted < total {
        let (hdr, body) = walker
            .next_page()
            .map_err(|e| ext(format!("next_page: {e}")))?
            .ok_or_else(|| ext("chunk ended before num_values"))?;
        let dph = hdr
            .data_page_header
            .as_ref()
            .ok_or_else(|| ext("v2 pages not yet supported in sparse gather"))?;
        let n = dph.num_values as usize;
        decompress_into(codec, body, page_uncompressed_size(&hdr)?, &mut scratch)?;
        match dph.encoding {
            Encoding::RleDictionary | Encoding::PlainDictionary => {
                gather_dict_at_bitmap_into(&scratch, n, bitmap, emitted, &dict, &mut out)
                    .map_err(|e| ext(format!("gather_dict_at_bitmap: {e}")))?;
            }
            Encoding::Plain => {
                let vals = decode_plain_page(&scratch)?;
                for (i, &v) in vals.iter().enumerate().take(n) {
                    let bp = emitted + i;
                    if (bitmap[bp / 8] >> (bp % 8)) & 1 == 1 {
                        out.push(v);
                    }
                }
            }
            other => {
                return Err(ext(format!("unexpected encoding {other:?}")));
            }
        }
        emitted += n;
    }
    Ok(out)
}

/// Phase 6 public wrappers (typed). Each one calls `gather_chunk_typed`
/// with the appropriate decoders for its primitive type.
pub fn sparse_gather_chunk_i32(
    path: &std::path::Path,
    rg: usize,
    col: usize,
    bitmap: &[u8],
) -> DfResult<Vec<i32>> {
    gather_chunk_typed::<i32>(
        path,
        rg,
        col,
        bitmap,
        |b| decode_plain_i32(b).map_err(|e| ext(format!("plain i32 dict: {e}"))),
        |b| decode_plain_i32(b).map_err(|e| ext(format!("plain i32 page: {e}"))),
    )
}
pub fn sparse_gather_chunk_i64(
    path: &std::path::Path,
    rg: usize,
    col: usize,
    bitmap: &[u8],
) -> DfResult<Vec<i64>> {
    gather_chunk_typed::<i64>(
        path,
        rg,
        col,
        bitmap,
        |b| decode_plain_i64(b).map_err(|e| ext(format!("plain i64 dict: {e}"))),
        |b| decode_plain_i64(b).map_err(|e| ext(format!("plain i64 page: {e}"))),
    )
}
pub fn sparse_gather_chunk_f64(
    path: &std::path::Path,
    rg: usize,
    col: usize,
    bitmap: &[u8],
) -> DfResult<Vec<f64>> {
    gather_chunk_typed::<f64>(
        path,
        rg,
        col,
        bitmap,
        |b| decode_plain_f64(b).map_err(|e| ext(format!("plain f64 dict: {e}"))),
        |b| decode_plain_f64(b).map_err(|e| ext(format!("plain f64 page: {e}"))),
    )
}

// Σ.E5a (Π.10 integration): late-materialisation entry points that
// defer the actual decode to ematix-parquet's `read_column_*_masked_into`
// public façade. The kernel decodes only at rows where `mask`'s bit is
// set; for high-selectivity filters (Q14's 30-day shipdate window ≈
// 1.4% of lineitem) this skips 99% of the per-row decode work that
// the sparse_gather path still incurs end-to-end.
//
// The new APIs take a `ParquetFile` (ematix-parquet-io's high-level
// handle); callers open it once per row-group and pass it down to
// each column read.

/// Façade-level masked-decode for INT32 / Date32 columns.
pub fn masked_decode_i32(
    file: &ParquetFile,
    rg: usize,
    col: usize,
    mask: &[u8],
) -> DfResult<Vec<i32>> {
    let mut out: Vec<i32> = Vec::new();
    read_column_i32_masked_into(file, rg, col, mask, &mut out)
        .map_err(|e| ext(format!("masked_decode_i32: {e}")))?;
    Ok(out)
}

/// Façade-level masked-decode for INT64 columns.
pub fn masked_decode_i64(
    file: &ParquetFile,
    rg: usize,
    col: usize,
    mask: &[u8],
) -> DfResult<Vec<i64>> {
    let mut out: Vec<i64> = Vec::new();
    read_column_i64_masked_into(file, rg, col, mask, &mut out)
        .map_err(|e| ext(format!("masked_decode_i64: {e}")))?;
    Ok(out)
}

/// Façade-level masked-decode for DOUBLE columns.
pub fn masked_decode_f64(
    file: &ParquetFile,
    rg: usize,
    col: usize,
    mask: &[u8],
) -> DfResult<Vec<f64>> {
    let mut out: Vec<f64> = Vec::new();
    read_column_f64_masked_into(file, rg, col, mask, &mut out)
        .map_err(|e| ext(format!("masked_decode_f64: {e}")))?;
    Ok(out)
}

/// Σ.E5 #512: build a row bitmap from a BYTE_ARRAY column. Uses the
/// dict-preserved fast path: decode dict once, evaluate predicate
/// per-dict-entry (O(|dict|)) into a dict_mask, then walk indices and
/// scatter into the bitmap. Returns `Err` if the column isn't fully
/// dict-encoded (PLAIN-only chunks fall back to dense-decode + filter
/// at the caller).
pub fn filter_byte_array_to_bitmap(
    path: &std::path::Path,
    rg: usize,
    col: usize,
    predicate: impl Fn(&[u8]) -> bool,
) -> DfResult<(Vec<u8>, usize)> {
    let file = ParquetFile::open(path).map_err(|e| ext(format!("ParquetFile::open: {e}")))?;
    let mut dict_bytes: Vec<u8> = Vec::new();
    let mut dict_offsets: Vec<u32> = Vec::new();
    let mut indices: Vec<u32> = Vec::new();
    read_column_byte_array_dict_preserved_into(
        &file,
        rg,
        col,
        &mut dict_bytes,
        &mut dict_offsets,
        &mut indices,
    )
    .map_err(|e| ext(format!("filter_byte_array dict-preserved: {e}")))?;

    let dict_len = dict_offsets.len().saturating_sub(1);
    let mut dict_mask: Vec<bool> = Vec::with_capacity(dict_len);
    let mut any_match = false;
    for i in 0..dict_len {
        let off = dict_offsets[i] as usize;
        let end = dict_offsets[i + 1] as usize;
        let m = predicate(&dict_bytes[off..end]);
        any_match |= m;
        dict_mask.push(m);
    }

    let total = indices.len();
    // Workstream 2A: if no dict entry passes, the index walk would
    // produce an all-zero bitmap. Skip it. NOTE: this only saves the
    // index walk; the byte_array path already decompresses upstream
    // via read_column_byte_array_dict_preserved_into. A full skip
    // would need a dict-only fast path.
    if !any_match {
        return Ok((vec![0u8; total.div_ceil(8)], total));
    }

    let mut bitmap = vec![0u8; total.div_ceil(8)];
    for (row, &idx) in indices.iter().enumerate() {
        if dict_mask[idx as usize] {
            bitmap[row >> 3] |= 1 << (row & 7);
        }
    }
    Ok((bitmap, total))
}

/// Σ.E6 — dict-fused FLOAT64 column → row bitmap, mirroring the
/// i32 path. Decode dict ONCE, eval predicate against ~50 dict
/// entries to build a `dict_mask`, then the width-generic NEON
/// kernel walks the RLE indices and writes the bitmap directly —
/// zero Vec<f64> materialization. Falls back to
/// [`filter_f64_column_to_bitmap_dense`] for PLAIN-only chunks.
pub fn filter_f64_column_to_bitmap(
    path: &std::path::Path,
    rg: usize,
    col: usize,
    predicate: impl Fn(f64) -> bool,
) -> DfResult<(Vec<u8>, usize)> {
    let file = ParquetFile::open(path).map_err(|e| ext(format!("ParquetFile::open: {e}")))?;
    let md = file.metadata().map_err(|e| ext(format!("metadata: {e}")))?;
    let cm = md.row_groups[rg].columns[col]
        .meta_data
        .as_ref()
        .ok_or_else(|| ext("column missing meta_data"))?;
    let total = cm.num_values as usize;
    let codec = cm.codec;
    let start = cm.dictionary_page_offset.unwrap_or(cm.data_page_offset) as u64;
    let length = cm.total_compressed_size as u64;
    let chunk = file
        .read_range(start, length)
        .map_err(|e| ext(format!("read_range: {e}")))?;

    let mut walker = PageWalker::new(&chunk);
    let mut scratch: Vec<u8> = Vec::with_capacity(128 * 1024);

    let (first_hdr, first_body) = walker
        .next_page()
        .map_err(|e| ext(format!("next_page (dict): {e}")))?
        .ok_or_else(|| ext("empty chunk"))?;
    if first_hdr.dictionary_page_header.is_none() {
        return Err(ext(
            "filter_f64_column_to_bitmap requires dict-encoded chunk; \
             use dense decode + scan for PLAIN-only columns",
        ));
    }
    decompress_into(
        codec,
        first_body,
        page_uncompressed_size(&first_hdr)?,
        &mut scratch,
    )?;
    let dict = decode_plain_f64(&scratch).map_err(|e| ext(format!("plain f64 dict: {e}")))?;

    // Workstream 2A: dict-popcount-zero early-skip. See i32 sibling
    // above for rationale.
    if !dict.iter().any(|&v| predicate(v)) {
        let bitmap = vec![0u8; total.div_ceil(8)];
        return Ok((bitmap, total));
    }

    let mut bitmap: Vec<u8> = Vec::with_capacity(total.div_ceil(8));
    let mut dict_mask: Vec<u8> = Vec::new();
    let mut emitted: usize = 0;
    while emitted < total {
        let (hdr, body) = walker
            .next_page()
            .map_err(|e| ext(format!("next_page: {e}")))?
            .ok_or_else(|| ext("chunk ended before num_values"))?;
        let dph = hdr
            .data_page_header
            .as_ref()
            .ok_or_else(|| ext("filter_f64: v2 data pages not yet supported"))?;
        let n = dph.num_values as usize;
        decompress_into(codec, body, page_uncompressed_size(&hdr)?, &mut scratch)?;
        match dph.encoding {
            Encoding::RleDictionary | Encoding::PlainDictionary => {
                if dict_mask.is_empty() {
                    if scratch.is_empty() {
                        return Err(ext("filter_f64: empty data page body"));
                    }
                    let bit_width = scratch[0];
                    dict_mask = build_dict_predicate_mask(&dict, bit_width, |&v| predicate(v))
                        .map_err(|e| ext(format!("build_dict_predicate_mask f64: {e}")))?;
                }
                decode_rle_dictionary_predicate_bitmap(&scratch, n, &dict_mask, &mut bitmap)
                    .map_err(|e| ext(format!("rle_dict_predicate_bitmap f64: {e}")))?;
            }
            other => {
                return Err(ext(format!(
                    "filter_f64: unexpected data page encoding {other:?} (dict-only)"
                )));
            }
        }
        emitted += n;
    }
    Ok((bitmap, total))
}

/// Σ.E5 #518: build a row bitmap from a FLOAT64 column. Uses dense
/// decode + per-row predicate eval. Cheap because the predicate is
/// usually a small range (Q06 `l_quantity < 24`, Q19 `l_quantity
/// BETWEEN 1 AND 30`) and the column is fixed-width 8-byte numeric.
pub fn filter_f64_column_to_bitmap_dense(
    path: &std::path::Path,
    rg: usize,
    col: usize,
    predicate: impl Fn(f64) -> bool,
) -> DfResult<(Vec<u8>, usize)> {
    let file = ParquetFile::open(path).map_err(|e| ext(format!("ParquetFile::open: {e}")))?;
    let md = file.metadata().map_err(|e| ext(format!("metadata: {e}")))?;
    let cm = md.row_groups[rg].columns[col]
        .meta_data
        .as_ref()
        .ok_or_else(|| ext("column missing meta_data"))?;
    let total = cm.num_values as usize;
    let all_ones = vec![0xFFu8; total.div_ceil(8)];
    let values = masked_decode_f64(&file, rg, col, &all_ones)?;
    let mut bitmap = vec![0u8; total.div_ceil(8)];
    for (row, &v) in values.iter().enumerate() {
        if predicate(v) {
            bitmap[row >> 3] |= 1 << (row & 7);
        }
    }
    Ok((bitmap, total))
}

/// Σ.E5: dense fallback for BYTE_ARRAY bitmap filter. Used when the
/// chunk isn't dict-encoded (PLAIN-only) — `o_comment`-style
/// high-cardinality columns. Decodes every row and applies the
/// predicate.
pub fn filter_byte_array_to_bitmap_dense(
    path: &std::path::Path,
    rg: usize,
    col: usize,
    predicate: impl Fn(&[u8]) -> bool,
) -> DfResult<(Vec<u8>, usize)> {
    let file = ParquetFile::open(path).map_err(|e| ext(format!("ParquetFile::open: {e}")))?;
    let md = file.metadata().map_err(|e| ext(format!("metadata: {e}")))?;
    let cm = md.row_groups[rg].columns[col]
        .meta_data
        .as_ref()
        .ok_or_else(|| ext("column missing meta_data"))?;
    let total = cm.num_values as usize;
    let all_ones = vec![0xFFu8; total.div_ceil(8)];
    let values = masked_decode_byte_array(&file, rg, col, &all_ones)?;
    if values.len() != total {
        return Err(ext(format!(
            "byte_array dense filter: decoded {} rows, expected {total}",
            values.len()
        )));
    }
    let mut bitmap = vec![0u8; total.div_ceil(8)];
    for (row, v) in values.iter().enumerate() {
        if predicate(v.as_slice()) {
            bitmap[row >> 3] |= 1 << (row & 7);
        }
    }
    Ok((bitmap, total))
}

/// Lever 3 (2026-05-24) — return `(min, max)` for an i64 column-chunk
/// from its parquet statistics, or `None` if stats are absent or the
/// values aren't 8-byte little-endian i64 (parquet's INT64 stats are
/// stored as 8 raw LE bytes per the spec).
pub fn rg_i64_min_max(
    path: &std::path::Path,
    rg: usize,
    col: usize,
) -> DfResult<Option<(i64, i64)>> {
    let file = ParquetFile::open(path).map_err(|e| ext(format!("ParquetFile::open: {e}")))?;
    let md = file.metadata().map_err(|e| ext(format!("metadata: {e}")))?;
    let cm = md.row_groups[rg].columns[col]
        .meta_data
        .as_ref()
        .ok_or_else(|| ext("column missing meta_data"))?;
    let Some(stats) = &cm.statistics else {
        return Ok(None);
    };
    // Parquet 2.0+ writers prefer min_value / max_value; older writers
    // wrote the deprecated `min` / `max`. Try the new fields first.
    let min_bytes = stats.min_value.or(stats.min);
    let max_bytes = stats.max_value.or(stats.max);
    match (min_bytes, max_bytes) {
        (Some(mn), Some(mx)) if mn.len() == 8 && mx.len() == 8 => {
            let mn = i64::from_le_bytes(mn.try_into().unwrap());
            let mx = i64::from_le_bytes(mx.try_into().unwrap());
            Ok(Some((mn, mx)))
        }
        _ => Ok(None),
    }
}

/// Lever 3 — return the number of values in an i64 column-chunk
/// without decoding it. Used as the bitmap size when an RG is
/// short-circuited by an `I64Range` predicate.
pub fn rg_num_values(path: &std::path::Path, rg: usize, col: usize) -> DfResult<usize> {
    let file = ParquetFile::open(path).map_err(|e| ext(format!("ParquetFile::open: {e}")))?;
    let md = file.metadata().map_err(|e| ext(format!("metadata: {e}")))?;
    let cm = md.row_groups[rg].columns[col]
        .meta_data
        .as_ref()
        .ok_or_else(|| ext("column missing meta_data"))?;
    Ok(cm.num_values as usize)
}

/// Σ.Q.L4′: build a row bitmap from an INT64 column. Used for bloom-
/// filter probe pushdown on i64 join keys (l_orderkey, l_partkey,
/// l_suppkey, o_orderkey, ...). PLAIN dense decode for now — TPC-H
/// i64 columns are bw≥20 so dict-encoding is rare; a dict-aware fast
/// path can be added later if a workload demands it.
pub fn filter_i64_column_to_bitmap_dense(
    path: &std::path::Path,
    rg: usize,
    col: usize,
    predicate: impl Fn(i64) -> bool,
) -> DfResult<(Vec<u8>, usize)> {
    let file = ParquetFile::open(path).map_err(|e| ext(format!("ParquetFile::open: {e}")))?;
    let md = file.metadata().map_err(|e| ext(format!("metadata: {e}")))?;
    let cm = md.row_groups[rg].columns[col]
        .meta_data
        .as_ref()
        .ok_or_else(|| ext("column missing meta_data"))?;
    let total = cm.num_values as usize;
    let all_ones = vec![0xFFu8; total.div_ceil(8)];
    let values = masked_decode_i64(&file, rg, col, &all_ones)?;
    let mut bitmap = vec![0u8; total.div_ceil(8)];
    for (row, &v) in values.iter().enumerate() {
        if predicate(v) {
            bitmap[row >> 3] |= 1 << (row & 7);
        }
    }
    Ok((bitmap, total))
}

/// Σ.E5 #512: build a row bitmap from an INT32 column via a simple
/// dense-decode path. Used as a fallback when the chunk isn't
/// dict-encoded (PLAIN-only). Mirrors `filter_i32_column_to_bitmap`
/// but without the dict-mask optimisation. Path-based to match the
/// existing public signature shape.
pub fn filter_i32_column_to_bitmap_dense(
    file: &ParquetFile,
    rg: usize,
    col: usize,
    predicate: impl Fn(i32) -> bool,
) -> DfResult<(Vec<u8>, usize)> {
    // Decode the whole column via the existing masked-decode kernel
    // with an all-ones mask. Then apply the predicate.
    let md = file.metadata().map_err(|e| ext(format!("metadata: {e}")))?;
    let cm = md.row_groups[rg].columns[col]
        .meta_data
        .as_ref()
        .ok_or_else(|| ext("column missing meta_data"))?;
    let total = cm.num_values as usize;
    let all_ones = vec![0xFFu8; total.div_ceil(8)];
    let values = masked_decode_i32(file, rg, col, &all_ones)?;
    let mut bitmap = vec![0u8; total.div_ceil(8)];
    for (row, &v) in values.iter().enumerate() {
        if predicate(v) {
            bitmap[row >> 3] |= 1 << (row & 7);
        }
    }
    Ok((bitmap, total))
}

/// Façade-level masked-decode for BYTE_ARRAY (Utf8 / Binary) columns.
/// Returns owned `Vec<u8>` per matched value; the caller materialises
/// these into a `StringArray` / `BinaryArray`.
pub fn masked_decode_byte_array(
    file: &ParquetFile,
    rg: usize,
    col: usize,
    mask: &[u8],
) -> DfResult<Vec<Vec<u8>>> {
    let mut out: Vec<Vec<u8>> = Vec::new();
    read_column_byte_array_masked_into(file, rg, col, mask, &mut out)
        .map_err(|e| ext(format!("masked_decode_byte_array: {e}")))?;
    Ok(out)
}

/// Codec-aware decompress helper. Dispatches on the column chunk's
/// declared codec. UNCOMPRESSED pages just copy bytes into `out`.
/// SNAPPY and ZSTD route to ematix-parquet's `_into` variants for
/// buffer reuse across pages. LZ4_RAW uses the size-known variant
/// (the format has no embedded length).
fn decompress_into(
    codec: CompressionCodec,
    body: &[u8],
    uncompressed_size: usize,
    out: &mut Vec<u8>,
) -> DfResult<()> {
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
        CompressionCodec::Lz4Raw => decompress_lz4_raw_into_sized(body, uncompressed_size, out)
            .map_err(|e| ext(format!("lz4_raw: {e}"))),
        other => Err(ext(format!(
            "codec {other:?} not yet wired into bridge; use FastParquetTableProvider"
        ))),
    }
}

/// Convert PageHeader.uncompressed_page_size to usize, defensively.
#[inline]
fn page_uncompressed_size(
    hdr: &ematix_parquet_format::metadata::PageHeader<'_>,
) -> DfResult<usize> {
    let n = hdr.uncompressed_page_size;
    if n < 0 {
        return Err(ext(format!(
            "page header uncompressed_page_size is negative ({n})"
        )));
    }
    Ok(n as usize)
}

#[cfg(test)]
mod tests {
    //! Oracle tests: each bridge function must produce values
    //! byte-for-byte identical to parquet-rs reading the same column.

    use super::*;
    use arrow_array::Array;
    use std::fs::File;
    use std::path::PathBuf;

    use datafusion::parquet::column::reader::ColumnReader;
    use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};

    fn lineitem_path() -> Option<PathBuf> {
        // 1. Developer override.
        if let Ok(s) = std::env::var("TPCH_DATA_DIR") {
            let p = PathBuf::from(s).join("lineitem.parquet");
            if p.exists() {
                return Some(p);
            }
        }
        // 2. Workspace SF=1 dataset, resolved against CARGO_MANIFEST_DIR
        //    so the path is CWD-independent.
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        if let Some(real) = manifest
            .parent()
            .and_then(|p| p.parent())
            .map(|p| p.join("examples/tpch/data/sf1/lineitem.parquet"))
        {
            if real.exists() {
                return Some(real);
            }
        }
        // 3. Fallback to the synthetic mini-fixture so the parquet-rs
        //    oracle tests (decode-equality between our column decoders
        //    and parquet-rs's typed column readers) can run in CI.
        let mini = PathBuf::from(crate::test_support::tpch_mini_dir()).join("lineitem.parquet");
        mini.exists().then_some(mini)
    }

    fn pr_read_i32(path: &PathBuf, rg: usize, col: usize) -> Vec<i32> {
        let r = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
        let total = r.metadata().row_group(rg).column(col).num_values() as usize;
        let rgr = r.get_row_group(rg).unwrap();
        let mut typed = match rgr.get_column_reader(col).unwrap() {
            ColumnReader::Int32ColumnReader(t) => t,
            _ => panic!("not Int32"),
        };
        let mut out: Vec<i32> = Vec::with_capacity(total);
        typed.read_records(total, None, None, &mut out).unwrap();
        out
    }

    fn pr_read_i64(path: &PathBuf, rg: usize, col: usize) -> Vec<i64> {
        let r = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
        let total = r.metadata().row_group(rg).column(col).num_values() as usize;
        let rgr = r.get_row_group(rg).unwrap();
        let mut typed = match rgr.get_column_reader(col).unwrap() {
            ColumnReader::Int64ColumnReader(t) => t,
            _ => panic!("not Int64"),
        };
        let mut out: Vec<i64> = Vec::with_capacity(total);
        typed.read_records(total, None, None, &mut out).unwrap();
        out
    }

    fn pr_read_byte_array(path: &PathBuf, rg: usize, col: usize) -> Vec<Vec<u8>> {
        let r = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
        let total = r.metadata().row_group(rg).column(col).num_values() as usize;
        let rgr = r.get_row_group(rg).unwrap();
        let mut typed = match rgr.get_column_reader(col).unwrap() {
            ColumnReader::ByteArrayColumnReader(t) => t,
            _ => panic!("not ByteArray"),
        };
        let mut out: Vec<datafusion::parquet::data_type::ByteArray> = Vec::with_capacity(total);
        typed.read_records(total, None, None, &mut out).unwrap();
        out.into_iter().map(|b| b.data().to_vec()).collect()
    }

    fn pr_read_f64(path: &PathBuf, rg: usize, col: usize) -> Vec<f64> {
        let r = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
        let total = r.metadata().row_group(rg).column(col).num_values() as usize;
        let rgr = r.get_row_group(rg).unwrap();
        let mut typed = match rgr.get_column_reader(col).unwrap() {
            ColumnReader::DoubleColumnReader(t) => t,
            _ => panic!("not Double"),
        };
        let mut out: Vec<f64> = Vec::with_capacity(total);
        typed.read_records(total, None, None, &mut out).unwrap();
        out
    }

    #[test]
    fn shipdate_rg0_matches_parquet_rs() {
        let Some(path) = lineitem_path() else {
            eprintln!("skipping: TPCH_DATA_DIR or examples/tpch/data/sf1 missing");
            return;
        };
        let ours = decode_column_chunk_i32(&path, 0, 10).unwrap();
        let theirs = pr_read_i32(&path, 0, 10);
        assert_eq!(ours.len(), theirs.len());
        let ours_vals: &[i32] = ours.values();
        assert_eq!(ours_vals, theirs.as_slice());
    }

    #[test]
    fn orderkey_rg0_matches_parquet_rs() {
        let Some(path) = lineitem_path() else {
            return;
        };
        let ours = decode_column_chunk_i64(&path, 0, 0).unwrap();
        let theirs = pr_read_i64(&path, 0, 0);
        assert_eq!(ours.len(), theirs.len());
        let ours_vals: &[i64] = ours.values();
        assert_eq!(ours_vals, theirs.as_slice());
    }

    #[test]
    fn extendedprice_rg0_matches_parquet_rs() {
        let Some(path) = lineitem_path() else {
            return;
        };
        let ours = decode_column_chunk_f64(&path, 0, 5).unwrap();
        let theirs = pr_read_f64(&path, 0, 5);
        assert_eq!(ours.len(), theirs.len());
        let ours_vals: &[f64] = ours.values();
        // f64 must match exactly (PLAIN decode is bit-for-bit copy).
        assert_eq!(ours_vals, theirs.as_slice());
    }

    #[test]
    fn returnflag_rg0_matches_parquet_rs() {
        // l_returnflag (col 8) is a 1-character BYTE_ARRAY with 3
        // distinct values ('R', 'A', 'N') — heavily dict-encoded.
        // Validates the RLE_DICTIONARY byte_array path.
        let Some(path) = lineitem_path() else {
            return;
        };
        let ours = decode_column_chunk_byte_array(&path, 0, 8).unwrap();
        let theirs = pr_read_byte_array(&path, 0, 8);
        assert_eq!(ours.len(), theirs.len());
        for (i, t) in theirs.iter().enumerate() {
            assert_eq!(
                ours.value(i).as_bytes(),
                t.as_slice(),
                "mismatch at row {i}"
            );
        }
    }

    #[test]
    fn comment_rg0_matches_parquet_rs() {
        // l_comment (col 15) is a longer, higher-cardinality
        // BYTE_ARRAY. Exercises both dict pages and (likely) PLAIN-
        // overflow pages depending on writer.
        let Some(path) = lineitem_path() else {
            return;
        };
        let ours = decode_column_chunk_byte_array(&path, 0, 15).unwrap();
        let theirs = pr_read_byte_array(&path, 0, 15);
        assert_eq!(ours.len(), theirs.len());
        for (i, t) in theirs.iter().enumerate() {
            assert_eq!(
                ours.value(i).as_bytes(),
                t.as_slice(),
                "mismatch at row {i}"
            );
        }
    }

    #[test]
    fn returnflag_dict_preserved_matches_materialised() {
        // Σ.E3b substrate: dict-preserved decode must produce a
        // DictionaryArray whose materialised row stream is byte-
        // identical to the StringArray returned by the eager path.
        let Some(path) = lineitem_path() else {
            return;
        };
        let materialised = decode_column_chunk_byte_array(&path, 0, 8).unwrap();
        let dict_arr = decode_column_chunk_byte_array_dict_preserved(&path, 0, 8).unwrap();
        assert_eq!(dict_arr.len(), materialised.len());

        // The values dictionary should be small (l_returnflag has
        // 3 distinct values) — the whole point of preserving it.
        let values = dict_arr
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("dict values must be StringArray");
        assert!(
            values.len() <= 8,
            "dict too large for l_returnflag: {}",
            values.len()
        );

        // Walk every row and confirm the indexed value matches the
        // materialised value.
        let keys = dict_arr.keys();
        for i in 0..materialised.len() {
            let k = keys.value(i) as usize;
            assert_eq!(
                values.value(k),
                materialised.value(i),
                "mismatch at row {i}"
            );
        }
    }

    #[test]
    fn comment_dict_preserved_or_errors() {
        // l_comment may have multiple data pages and may include
        // PLAIN-encoded fallback pages depending on the writer.
        // If it does, the dict-preserved path must error so the
        // caller knows to fall back; if it doesn't, the dict must
        // round-trip.
        let Some(path) = lineitem_path() else {
            return;
        };
        let materialised = decode_column_chunk_byte_array(&path, 0, 15).unwrap();
        match decode_column_chunk_byte_array_dict_preserved(&path, 0, 15) {
            Ok(dict_arr) => {
                assert_eq!(dict_arr.len(), materialised.len());
                let values = dict_arr
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("dict values must be StringArray");
                let keys = dict_arr.keys();
                for i in 0..materialised.len() {
                    let k = keys.value(i) as usize;
                    assert_eq!(
                        values.value(k),
                        materialised.value(i),
                        "mismatch at row {i}"
                    );
                }
            }
            Err(e) => {
                // Acceptable: PLAIN fallback page in chunk — caller
                // would fall back to the eager path.
                let msg = format!("{e}");
                assert!(
                    msg.contains("PLAIN") || msg.contains("dictionary"),
                    "unexpected error: {msg}"
                );
            }
        }
    }

    #[test]
    fn shipdate_all_row_groups_match() {
        // Walk every row group to confirm multi-RG handling.
        let Some(path) = lineitem_path() else {
            return;
        };
        let file = ParquetFile::open(&path).unwrap();
        let md = file.metadata().unwrap();
        for rg in 0..md.row_groups.len() {
            let ours = decode_column_chunk_i32(&path, rg, 10).unwrap();
            let theirs = pr_read_i32(&path, rg, 10);
            let ours_vals: &[i32] = ours.values();
            assert_eq!(ours_vals, theirs.as_slice(), "mismatch in RG {rg}");
        }
    }
}
