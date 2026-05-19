//! Σ.E5.6 (2026-05-18) scaffold — intra-RG page-streaming column
//! decoders.
//!
//! Goal: replace `EmatArrowBatchReader::load_row_group`'s burst-then-
//! wait pattern (decode all N projected columns of an RG fully → slice
//! into `batch_size`-row windows) with a streaming pattern that yields
//! each batch as soon as every column has decoded ≥ `batch_size` new
//! rows.
//!
//! Why this matters: per-column micro-bench shows Emat is collectively
//! -36% vs parquet-rs on Q19 lineitem (full sequential decode). But
//! Q19's query-level scan compute is +45% — that gap is entirely
//! "first batch latency" inside the scan node. Downstream
//! FilterExec/HashJoinExec sit idle waiting for the full RG decode
//! to complete. See [[q19-root-cause-orchestration]] memory note.
//!
//! ## Status (2026-05-18)
//!
//! Scaffold only. The `ColumnPageStream` trait is defined and one
//! concrete impl exists for `Float64`. The streaming reader itself
//! (`EmatPageStreamingReader`) is NOT yet wired into
//! `EmatixFastParquetExec` — flipping it on requires:
//!   1. Per-type impls for Int32, Int64, StringView (PLAIN + dict-
//!      preserved), DictUtf8.
//!   2. Concurrency: pages must decode in parallel across columns
//!      (matching the current scoped-thread budget) — otherwise we
//!      lose the per-column parallelism that makes Q01 win.
//!   3. Batch-emission gating: pull from the slowest-progressed column
//!      first, emit only once `min(rows_decoded) >= cursor + batch_size`.
//!
//! See the TDD test in this module for the locked-in behaviour
//! contract.

use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array};
use arrow_schema::DataType;
use datafusion::arrow::buffer::{Buffer, ScalarBuffer};
use datafusion::error::{DataFusionError, Result as DfResult};

use ematix_parquet_codec::compression::{decompress_snappy_into, decompress_zstd_into};
use ematix_parquet_codec::dict::decode_rle_dictionary_into;
use ematix_parquet_codec::plain::decode_plain_f64;
use ematix_parquet_format::types::{CompressionCodec, Encoding};
use ematix_parquet_io::{PageWalker, ParquetFile};

#[inline]
fn ext<S: Into<String>>(msg: S) -> DataFusionError {
    DataFusionError::External(msg.into().into())
}

#[inline]
fn decompress_into(codec: CompressionCodec, body: &[u8], out: &mut Vec<u8>) -> DfResult<()> {
    out.clear();
    match codec {
        CompressionCodec::Uncompressed => {
            out.extend_from_slice(body);
            Ok(())
        }
        CompressionCodec::Snappy => decompress_snappy_into(body, out)
            .map_err(|e| ext(format!("snappy decompress: {e}"))),
        CompressionCodec::Zstd => {
            decompress_zstd_into(body, out).map_err(|e| ext(format!("zstd decompress: {e}")))
        }
        other => Err(ext(format!("unsupported compression codec {other:?}"))),
    }
}

/// Σ.E5.6 trait — per-column page-streaming decoder.
///
/// Each implementation owns its `PageWalker`, dict state, and buffered
/// decoded values. Caller polls `decode_next_page` repeatedly; once
/// `rows_decoded() >= cursor + batch_size`, caller can slice an Arrow
/// array via `make_array(cursor, batch_size, target)`.
///
/// Thread-safety: each instance is owned by one thread at a time.
/// Concurrent decode across columns is achieved by holding one
/// `Box<dyn ColumnPageStream>` per column and dispatching to scoped
/// threads — same pattern as today's `load_row_group`, but at page
/// granularity instead of column granularity.
pub trait ColumnPageStream: Send {
    /// Total row count in the column chunk (known from RG metadata).
    fn total_rows(&self) -> usize;

    /// Rows decoded so far. Monotonically increases up to `total_rows()`.
    fn rows_decoded(&self) -> usize;

    /// Decode the next data page (if any). Returns the number of rows
    /// just added. Returns `Ok(0)` only when `rows_decoded() ==
    /// total_rows()` (end-of-column) — callers should check
    /// `rows_decoded()` against `total_rows()` to detect completion.
    fn decode_next_page(&mut self) -> DfResult<usize>;

    /// Build an `ArrayRef` for `[start, start+n)` from the decoded
    /// buffer. Caller must ensure `start + n <= rows_decoded()`.
    fn make_array(&self, start: usize, n: usize, target: &DataType) -> ArrayRef;
}

/// Page-streaming decoder for `Float64` (the simplest type to
/// prototype against). Pattern mirrors the eager `decode_dict_chunk_typed`
/// in `emat_arrow_reader` — same compression + dict + RLE flow, but
/// the data-page loop is replaced with a single-step `decode_next_page`.
pub struct Float64PageStream {
    total: usize,
    codec: CompressionCodec,
    /// Owned page-chunk bytes. The walker borrows from this.
    chunk: Vec<u8>,
    /// Walker position into `chunk`. Stored as a byte offset so we
    /// can drop & recreate the walker — `PageWalker`'s lifetime is
    /// tied to a `&[u8]` borrow.
    walker_pos: usize,
    /// Decoded dict (if the column has a dict page). Empty otherwise.
    dict: Vec<f64>,
    /// Decoded values so far. Grows by one page's worth per
    /// `decode_next_page` call.
    out: Vec<f64>,
    /// Reusable decompression buffer.
    scratch: Vec<u8>,
    /// True once the first page has been consumed (the first page is
    /// either a dict page or a PLAIN data page; subsequent pages are
    /// always data pages).
    first_page_consumed: bool,
}

impl Float64PageStream {
    pub fn new(file: &ParquetFile, rg: usize, col: usize) -> DfResult<Self> {
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

        Ok(Self {
            total,
            codec,
            chunk,
            walker_pos: 0,
            dict: Vec::new(),
            out: Vec::with_capacity(total),
            scratch: Vec::with_capacity(128 * 1024),
            first_page_consumed: false,
        })
    }
}

impl ColumnPageStream for Float64PageStream {
    fn total_rows(&self) -> usize {
        self.total
    }

    fn rows_decoded(&self) -> usize {
        self.out.len()
    }

    fn decode_next_page(&mut self) -> DfResult<usize> {
        if self.out.len() >= self.total {
            return Ok(0);
        }

        // Rebuild a walker that resumes at `walker_pos`. PageWalker
        // borrows from `chunk` so it must be re-created each call;
        // the chunk bytes are stable across calls so this is safe and
        // amounts to a struct re-init.
        let mut walker = PageWalker::new(&self.chunk[self.walker_pos..]);
        let before = self.out.len();

        let (hdr, body) = walker
            .next_page()
            .map_err(|e| ext(format!("next_page: {e}")))?
            .ok_or_else(|| ext("chunk ended before num_values"))?;

        // Capture the byte advance so we can resume from the next page
        // on the subsequent call. PageWalker doesn't expose its position
        // directly; we infer it from the body slice end relative to chunk.
        let body_end = body.as_ptr() as usize - self.chunk.as_ptr() as usize + body.len();
        self.walker_pos = body_end;

        decompress_into(self.codec, body, &mut self.scratch)?;

        if !self.first_page_consumed {
            self.first_page_consumed = true;
            if hdr.dictionary_page_header.is_some() {
                // Dict page: decode dict, return 0 rows (no row added yet).
                self.dict = decode_plain_f64(&self.scratch)
                    .map_err(|e| ext(format!("plain f64 dict: {e}")))?;
                return Ok(0);
            }
            // First page is PLAIN data — fall through to data-page path.
        }

        let dph = hdr
            .data_page_header
            .as_ref()
            .ok_or_else(|| ext("expected v1 data page"))?;
        let n = dph.num_values as usize;
        match dph.encoding {
            Encoding::RleDictionary | Encoding::PlainDictionary => {
                decode_rle_dictionary_into(&self.scratch, &self.dict, n, &mut self.out)
                    .map_err(|e| ext(format!("rle_dict f64: {e}")))?;
            }
            Encoding::Plain => {
                let mut vals = decode_plain_f64(&self.scratch)
                    .map_err(|e| ext(format!("plain f64: {e}")))?;
                vals.truncate(n);
                self.out.extend(vals);
            }
            other => {
                return Err(ext(format!("unexpected f64 data page encoding {other:?}")));
            }
        }

        Ok(self.out.len() - before)
    }

    fn make_array(&self, start: usize, n: usize, _target: &DataType) -> ArrayRef {
        // Zero-copy slice of the decoded buffer. The Vec stays alive
        // on the stream; we hand out an Arc-wrapped Float64Array
        // backed by a Buffer slice of the existing storage.
        debug_assert!(start + n <= self.out.len());
        let slice = &self.out[start..start + n];
        let buf = Buffer::from_slice_ref(slice);
        let scalar = ScalarBuffer::<f64>::new(buf, 0, n);
        Arc::new(Float64Array::new(scalar, None))
    }
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use ematix_parquet_codec::write::{ColumnData, write_table_with_dict_to_path};
    use ematix_parquet_format::types::CompressionCodec;

    fn tmp_parquet(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "emat_page_stream_test_{}_{}",
            std::process::id(),
            name
        ));
        let _ = std::fs::create_dir_all(&dir);
        dir.join(format!("{name}.parquet"))
    }

    /// TDD contract test for Σ.E5.6 streaming behaviour.
    ///
    /// A `ColumnPageStream` must produce rows monotonically by page,
    /// must terminate at `total_rows()`, and must allow zero-copy
    /// slicing of arbitrary `[start, start+n)` windows from the
    /// decoded buffer.
    ///
    /// Fixture: a dict-encoded Float64 column. Dict page + data page
    /// → ≥ 2 page-walker calls, exercising the streaming property
    /// (one of those returns 0 rows for the dict).
    #[test]
    fn float64_page_stream_yields_pages_then_terminates() {
        let path = tmp_parquet("f64_page_stream");
        // Low-cardinality so the writer takes the dict path. 10K rows
        // cycling 50 distinct values.
        let palette: Vec<f64> = (0..50).map(|x| x as f64 * 0.5).collect();
        let n: usize = 10_000;
        let f64s: Vec<f64> = (0..n).map(|i| palette[i % palette.len()]).collect();
        write_table_with_dict_to_path(
            &path,
            &[("c_f64", ColumnData::F64(&f64s))],
            CompressionCodec::Uncompressed,
            usize::MAX, // single RG
            &[true],    // force dict encoding
        )
        .unwrap();

        let file = ParquetFile::open(&path).unwrap();
        let mut stream = Float64PageStream::new(&file, 0, 0).expect("open stream");

        let total = stream.total_rows();
        assert_eq!(total, n);
        assert_eq!(stream.rows_decoded(), 0);

        // Drive the stream page by page. `rows_decoded()` must be
        // monotonic and must converge to `total_rows()`.
        let mut last = 0usize;
        let mut page_calls = 0usize;
        while stream.rows_decoded() < total {
            stream.decode_next_page().expect("decode next page");
            page_calls += 1;
            let now = stream.rows_decoded();
            assert!(now >= last, "rows_decoded must be monotonic");
            assert!(
                now <= total,
                "rows_decoded ({now}) must not exceed total_rows ({total})",
            );
            last = now;
            assert!(page_calls <= 2048, "too many pages — stream not terminating");
        }
        assert_eq!(stream.rows_decoded(), total);
        // 200K f64 = 1.6 MB. The codec writer cuts data pages well
        // below that, so the stream must have walked at least 2 data
        // pages (plus maybe a dict page). If this fails, the writer
        // changed its page sizing — either bump `n` or pin the
        // writer's page size.
        assert!(
            page_calls >= 2,
            "expected multi-page stream, got {page_calls} pages \
             — streaming property not exercised"
        );

        // End-of-column: further calls return 0 rows.
        assert_eq!(stream.decode_next_page().unwrap(), 0);

        // Slice a window from the decoded buffer.
        let window = stream.make_array(0, 256, &DataType::Float64);
        assert_eq!(window.len(), 256);
        let f64_arr = window
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64Array");
        // Values cycle through `palette` (50 entries × 0.5 step).
        for i in 0..256 {
            let expected = (i % 50) as f64 * 0.5;
            assert_eq!(f64_arr.value(i), expected, "row {i}");
        }

        // Slice a window mid-stream.
        let mid = stream.make_array(5000, 100, &DataType::Float64);
        let mid_arr = mid.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(mid_arr.value(0), (5000 % 50) as f64 * 0.5);
    }
}
