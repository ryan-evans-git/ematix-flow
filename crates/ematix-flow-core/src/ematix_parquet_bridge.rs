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
//! Q14 lever already shown (`tpch_q14_ematix_lever` example):
//!   14.60 ms manual end-to-end vs FusedQ14FullExec 15.06 ms.

use std::path::Path;
use std::sync::Arc;

use arrow_array::{Float64Array, Int32Array, Int64Array};
use datafusion::error::{DataFusionError, Result as DfResult};

use ematix_parquet_codec::compression::{decompress_snappy_into, decompress_zstd_into};
use ematix_parquet_codec::dict::decode_rle_dictionary_into;
use ematix_parquet_codec::plain::{decode_plain_f64, decode_plain_i32, decode_plain_i64};
use ematix_parquet_format::types::{CompressionCodec, Encoding};
use ematix_parquet_io::{PageWalker, ParquetFile};

/// Decode an INT32 column chunk to a contiguous `Int32Array`.
///
/// Walks dict + data pages, dispatching on per-page encoding. The
/// dict (if present) is PLAIN-decoded once and reused across all
/// data pages. Decompression buffer is reused across pages (no per-
/// page allocation).
pub fn decode_column_chunk_i32(
    path: &Path,
    rg: usize,
    col: usize,
) -> DfResult<Arc<Int32Array>> {
    let buf = decode_dict_chunk_generic::<i32>(
        path,
        rg,
        col,
        |bytes| {
            decode_plain_i32(bytes)
                .map_err(|e| ext(format!("plain i32: {e}")))
        },
    )?;
    Ok(Arc::new(Int32Array::from(buf)))
}

/// Decode an INT64 column chunk to a contiguous `Int64Array`.
pub fn decode_column_chunk_i64(
    path: &Path,
    rg: usize,
    col: usize,
) -> DfResult<Arc<Int64Array>> {
    let buf = decode_dict_chunk_generic::<i64>(
        path,
        rg,
        col,
        |bytes| {
            decode_plain_i64(bytes)
                .map_err(|e| ext(format!("plain i64: {e}")))
        },
    )?;
    Ok(Arc::new(Int64Array::from(buf)))
}

/// Decode a DOUBLE column chunk to a contiguous `Float64Array`.
pub fn decode_column_chunk_f64(
    path: &Path,
    rg: usize,
    col: usize,
) -> DfResult<Arc<Float64Array>> {
    let buf = decode_dict_chunk_generic::<f64>(
        path,
        rg,
        col,
        |bytes| {
            decode_plain_f64(bytes)
                .map_err(|e| ext(format!("plain f64: {e}")))
        },
    )?;
    Ok(Arc::new(Float64Array::from(buf)))
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
    let file = ParquetFile::open(path)
        .map_err(|e| ext(format!("ParquetFile::open: {e}")))?;
    let md = file
        .metadata()
        .map_err(|e| ext(format!("metadata: {e}")))?;
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
    decompress_into(codec, first_body, &mut scratch)?;

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

#[inline]
fn ext<S: Into<String>>(msg: S) -> DataFusionError {
    DataFusionError::External(format!("ematix_parquet_bridge: {}", msg.into()).into())
}

/// Codec-aware decompress helper. Dispatches on the column chunk's
/// declared codec. UNCOMPRESSED pages just copy bytes into `out`.
/// SNAPPY and ZSTD route to ematix-parquet's `_into` variants for
/// buffer reuse across pages. Other codecs error.
fn decompress_into(
    codec: CompressionCodec,
    body: &[u8],
    out: &mut Vec<u8>,
) -> DfResult<()> {
    match codec {
        CompressionCodec::Uncompressed => {
            out.clear();
            out.extend_from_slice(body);
            Ok(())
        }
        CompressionCodec::Snappy => decompress_snappy_into(body, out)
            .map_err(|e| ext(format!("snappy: {e}"))),
        CompressionCodec::Zstd => decompress_zstd_into(body, out)
            .map_err(|e| ext(format!("zstd: {e}"))),
        other => Err(ext(format!(
            "codec {other:?} not yet wired into bridge; use FastParquetTableProvider"
        ))),
    }
}

#[cfg(test)]
mod tests {
    //! Oracle tests: each bridge function must produce values
    //! byte-for-byte identical to parquet-rs reading the same column.

    use super::*;
    use std::fs::File;
    use std::path::PathBuf;

    use parquet::column::reader::ColumnReader;
    use parquet::file::reader::{FileReader, SerializedFileReader};

    fn lineitem_path() -> Option<PathBuf> {
        let p = match std::env::var("TPCH_DATA_DIR") {
            Ok(s) => PathBuf::from(s).join("lineitem.parquet"),
            Err(_) => PathBuf::from("examples/tpch/data/sf1/lineitem.parquet"),
        };
        if p.exists() {
            Some(p)
        } else {
            None
        }
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
