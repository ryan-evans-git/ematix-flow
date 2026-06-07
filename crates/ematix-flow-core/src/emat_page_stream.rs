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

use arrow_array::builder::make_view;
use arrow_array::{
    ArrayRef, Date32Array, Float64Array, Int32Array, Int64Array, StringViewArray, UInt64Array,
};
use arrow_schema::DataType;
use datafusion::arrow::buffer::{Buffer, NullBuffer, ScalarBuffer};
use datafusion::error::{DataFusionError, Result as DfResult};

use ematix_parquet_codec::compression::{
    decompress_lz4_raw_into_sized, decompress_snappy_into, decompress_zstd_into,
};
use ematix_parquet_codec::dict::decode_rle_dictionary_into;
use ematix_parquet_codec::plain::{
    decode_plain_byte_array, decode_plain_f64, decode_plain_i32, decode_plain_i64,
};
use ematix_parquet_format::types::{CompressionCodec, Encoding};
use ematix_parquet_io::{PageWalker, ParquetFile};

use crate::emat_arrow_reader::{CachedColumnChunk, CachedFileMetadata};

#[inline]
fn ext<S: Into<String>>(msg: S) -> DataFusionError {
    DataFusionError::External(msg.into().into())
}

#[inline]
fn decompress_into(
    codec: CompressionCodec,
    body: &[u8],
    uncompressed_size: usize,
    out: &mut Vec<u8>,
) -> DfResult<()> {
    out.clear();
    match codec {
        CompressionCodec::Uncompressed => {
            out.extend_from_slice(body);
            Ok(())
        }
        CompressionCodec::Snappy => {
            decompress_snappy_into(body, out).map_err(|e| ext(format!("snappy decompress: {e}")))
        }
        CompressionCodec::Zstd => {
            decompress_zstd_into(body, out).map_err(|e| ext(format!("zstd decompress: {e}")))
        }
        // Q06.c1 (2026-05-24): LZ4_RAW needs the page header's
        // uncompressed_page_size — the codec has no embedded length.
        CompressionCodec::Lz4Raw => decompress_lz4_raw_into_sized(body, uncompressed_size, out)
            .map_err(|e| ext(format!("lz4_raw decompress: {e}"))),
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

/// Compact trait describing how a primitive type is PLAIN-decoded
/// from parquet bytes and how a slice of decoded values is wrapped
/// into an Arrow array.
///
/// One impl per supported primitive: `i32` (also covers `Date32`),
/// `i64`, `f64`. The trait lets `PrimitivePageStream<T>` be one
/// type-parametric struct instead of three near-identical copies.
pub trait PrimitiveType: Copy + Send + 'static {
    /// Identifier for error messages.
    const NAME: &'static str;

    /// PLAIN-decode a page's worth of bytes into a `Vec<Self>`.
    fn decode_plain(bytes: &[u8]) -> DfResult<Vec<Self>>;

    /// Build an Arrow array from a contiguous slice. `target`
    /// distinguishes physically-identical types (e.g. `Int32` vs
    /// `Date32`).
    fn make_array(slice: &[Self], target: &DataType) -> ArrayRef;
}

impl PrimitiveType for i32 {
    const NAME: &'static str = "i32";
    fn decode_plain(bytes: &[u8]) -> DfResult<Vec<Self>> {
        decode_plain_i32(bytes).map_err(|e| ext(format!("plain i32: {e}")))
    }
    fn make_array(slice: &[Self], target: &DataType) -> ArrayRef {
        let buf = Buffer::from_slice_ref(slice);
        let scalar = ScalarBuffer::<i32>::new(buf, 0, slice.len());
        match target {
            DataType::Date32 => Arc::new(Date32Array::new(scalar, None)),
            _ => Arc::new(Int32Array::new(scalar, None)),
        }
    }
}

impl PrimitiveType for i64 {
    const NAME: &'static str = "i64";
    fn decode_plain(bytes: &[u8]) -> DfResult<Vec<Self>> {
        decode_plain_i64(bytes).map_err(|e| ext(format!("plain i64: {e}")))
    }
    fn make_array(slice: &[Self], target: &DataType) -> ArrayRef {
        let buf = Buffer::from_slice_ref(slice);
        match target {
            // KEYS.4.b — UInt64 is physically INT64; reinterpret the same
            // bytes as u64 (bit-for-bit, parallel to i32's Date32 branch).
            DataType::UInt64 => {
                let scalar = ScalarBuffer::<u64>::new(buf, 0, slice.len());
                Arc::new(UInt64Array::new(scalar, None))
            }
            _ => {
                let scalar = ScalarBuffer::<i64>::new(buf, 0, slice.len());
                Arc::new(Int64Array::new(scalar, None))
            }
        }
    }
}

impl PrimitiveType for f64 {
    const NAME: &'static str = "f64";
    fn decode_plain(bytes: &[u8]) -> DfResult<Vec<Self>> {
        decode_plain_f64(bytes).map_err(|e| ext(format!("plain f64: {e}")))
    }
    fn make_array(slice: &[Self], _target: &DataType) -> ArrayRef {
        let buf = Buffer::from_slice_ref(slice);
        let scalar = ScalarBuffer::<f64>::new(buf, 0, slice.len());
        Arc::new(Float64Array::new(scalar, None))
    }
}

/// Page-streaming decoder for fixed-size primitives. Pattern mirrors
/// the eager `decode_dict_chunk_typed` in `emat_arrow_reader` — same
/// compression + dict + RLE flow, but the data-page loop is replaced
/// with a single-step `decode_next_page`.
pub struct PrimitivePageStream<T: PrimitiveType> {
    total: usize,
    codec: CompressionCodec,
    /// Owned page-chunk bytes. The walker borrows from this.
    chunk: Vec<u8>,
    /// Walker position into `chunk` as a byte offset (re-created each
    /// call since `PageWalker`'s lifetime is tied to a `&[u8]` borrow).
    walker_pos: usize,
    /// Decoded dict (if the column has a dict page). Empty otherwise.
    dict: Vec<T>,
    /// Decoded values so far. Grows by one page's worth per call.
    out: Vec<T>,
    /// Reusable decompression buffer.
    scratch: Vec<u8>,
    /// True once the first page has been consumed (the first page is
    /// either a dict page or a PLAIN data page; subsequent pages are
    /// always data pages).
    first_page_consumed: bool,
}

impl<T: PrimitiveType> PrimitivePageStream<T> {
    pub(crate) fn new(file: &ParquetFile, cm: &CachedColumnChunk) -> DfResult<Self> {
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

impl<T: PrimitiveType> ColumnPageStream for PrimitivePageStream<T> {
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

        let mut walker = PageWalker::new(&self.chunk[self.walker_pos..]);
        let before = self.out.len();

        let (hdr, body) = walker
            .next_page()
            .map_err(|e| ext(format!("next_page: {e}")))?
            .ok_or_else(|| ext("chunk ended before num_values"))?;

        // Capture byte advance so we can resume on the next call.
        let body_end = body.as_ptr() as usize - self.chunk.as_ptr() as usize + body.len();
        self.walker_pos = body_end;

        let usize_hdr = hdr.uncompressed_page_size.max(0) as usize;
        decompress_into(self.codec, body, usize_hdr, &mut self.scratch)?;

        if !self.first_page_consumed {
            self.first_page_consumed = true;
            if hdr.dictionary_page_header.is_some() {
                self.dict = T::decode_plain(&self.scratch)
                    .map_err(|e| ext(format!("{} dict: {e}", T::NAME)))?;
                return Ok(0);
            }
            // First page is PLAIN data — fall through.
        }

        let dph = hdr
            .data_page_header
            .as_ref()
            .ok_or_else(|| ext("expected v1 data page"))?;
        let n = dph.num_values as usize;
        match dph.encoding {
            Encoding::RleDictionary | Encoding::PlainDictionary => {
                decode_rle_dictionary_into(&self.scratch, &self.dict, n, &mut self.out)
                    .map_err(|e| ext(format!("rle_dict {}: {e}", T::NAME)))?;
            }
            Encoding::Plain => {
                let mut vals = T::decode_plain(&self.scratch)
                    .map_err(|e| ext(format!("plain {}: {e}", T::NAME)))?;
                vals.truncate(n);
                self.out.extend(vals);
            }
            other => {
                return Err(ext(format!(
                    "unexpected {} data page encoding {other:?}",
                    T::NAME
                )));
            }
        }

        Ok(self.out.len() - before)
    }

    fn make_array(&self, start: usize, n: usize, target: &DataType) -> ArrayRef {
        debug_assert!(start + n <= self.out.len());
        T::make_array(&self.out[start..start + n], target)
    }
}

/// Convenience aliases matching the existing `DecodedColumn` variants.
pub type Int32PageStream = PrimitivePageStream<i32>;
pub type Int64PageStream = PrimitivePageStream<i64>;
pub type Float64PageStream = PrimitivePageStream<f64>;

// ============================================================
// StringView page stream
// ============================================================

/// Page-streaming decoder for `BYTE_ARRAY → StringViewArray`. Mirrors
/// `decode_byte_array_to_string_view_slow` in `emat_arrow_reader` but
/// yields page-by-page instead of decode-all-at-once.
///
/// Two encoding paths, both handled uniformly via `PageWalker`:
///   * Dict-encoded chunks: first page is the DictionaryPage (decoded
///     into `dict_offsets`/`dict_lengths` + stashed as
///     `data_buffers[0]`); subsequent data pages emit views that
///     reference `data_buffers[0]`.
///   * PLAIN data pages: each page's decompressed bytes become an
///     owned `Buffer` appended to `data_buffers`; views encode
///     `(block_id = data_buffers.len() - 1, offset_in_page)`.
///
/// The multi-buffer layout (Σ.E5 multi-buffer commit 9cdf890) makes
/// per-page zero-copy hand-off natural — no coalescing memcpy needed.
pub struct StringViewPageStream {
    total: usize,
    codec: CompressionCodec,
    chunk: Vec<u8>,
    walker_pos: usize,
    /// Per-dict-entry offsets/lengths into `data_buffers[0]` (only
    /// populated when the column chunk has a DictionaryPage).
    dict_offsets: Vec<u32>,
    dict_lengths: Vec<u32>,
    /// One u128 view per dict entry, built once on dict-page parse.
    /// Empty for PLAIN-only chunks. Mirrors the dict-preserved fast
    /// path in `build_string_view_from_dict_preserved` — per-row
    /// emission becomes `views.push(dict_views[idx])` instead of a
    /// per-row `make_view` call (which is `#[inline(never)]`).
    dict_views: Vec<u128>,
    /// Per-page backing buffers (Σ.E5 multi-buffer layout).
    data_buffers: Vec<Buffer>,
    /// Views (16-byte each). Grown by one page's worth per call.
    views: Vec<u128>,
    /// Reusable per-page scratch buffers — eliminates the per-page
    /// `Vec` alloc churn (~50 page allocs per lineitem column chunk
    /// dropped to 0 after the first call).
    idx_scratch: Vec<u8>,
    idx_buf: Vec<u32>,
    first_page_consumed: bool,
}

impl StringViewPageStream {
    pub(crate) fn new(file: &ParquetFile, cm: &CachedColumnChunk) -> DfResult<Self> {
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
            dict_offsets: Vec::new(),
            dict_lengths: Vec::new(),
            dict_views: Vec::new(),
            data_buffers: Vec::new(),
            views: Vec::with_capacity(total),
            idx_scratch: Vec::new(),
            idx_buf: Vec::new(),
            first_page_consumed: false,
        })
    }
}

impl ColumnPageStream for StringViewPageStream {
    fn total_rows(&self) -> usize {
        self.total
    }

    fn rows_decoded(&self) -> usize {
        self.views.len()
    }

    fn decode_next_page(&mut self) -> DfResult<usize> {
        if self.views.len() >= self.total {
            return Ok(0);
        }

        let mut walker = PageWalker::new(&self.chunk[self.walker_pos..]);
        let before = self.views.len();

        let (hdr, body) = walker
            .next_page()
            .map_err(|e| ext(format!("next_page: {e}")))?
            .ok_or_else(|| ext("chunk ended before num_values"))?;
        let body_end = body.as_ptr() as usize - self.chunk.as_ptr() as usize + body.len();
        self.walker_pos = body_end;

        // Handle the first page (dict or PLAIN data) specially.
        if !self.first_page_consumed {
            self.first_page_consumed = true;
            if hdr.dictionary_page_header.is_some() {
                // Decompress into an owned buffer; record per-entry
                // offsets/lengths; pre-compute one u128 view per dict
                // entry (so per-row emission is a tight `dict_views`
                // gather instead of a per-row `make_view` call —
                // mirrors `build_string_view_from_dict_preserved` in
                // the eager reader).
                let mut dict_scratch: Vec<u8> = Vec::with_capacity(body.len() * 2);
                let usize_hdr = hdr.uncompressed_page_size.max(0) as usize;
                decompress_into(self.codec, body, usize_hdr, &mut dict_scratch)?;
                let entries = decode_plain_byte_array(&dict_scratch)
                    .map_err(|e| ext(format!("plain byte_array dict: {e}")))?;
                let base = dict_scratch.as_ptr() as usize;
                let dict_block = 0u32; // dict pages always land in data_buffers[0]
                self.dict_views.reserve(entries.len());
                for s in &entries {
                    let off = (s.as_ptr() as usize - base) as u32;
                    self.dict_offsets.push(off);
                    self.dict_lengths.push(s.len() as u32);
                    self.dict_views.push(make_view(s, dict_block, off));
                }
                self.data_buffers.push(Buffer::from_vec(dict_scratch));
                return Ok(0);
            }
            // First page is PLAIN data: fall through to the PLAIN arm.
        }

        let dph = hdr
            .data_page_header
            .as_ref()
            .ok_or_else(|| ext("expected v1 data page"))?;
        let n = dph.num_values as usize;
        let usize_hdr = hdr.uncompressed_page_size.max(0) as usize;
        match dph.encoding {
            Encoding::RleDictionary | Encoding::PlainDictionary => {
                decompress_into(self.codec, body, usize_hdr, &mut self.idx_scratch)?;
                self.idx_buf.clear();
                ematix_parquet_codec::dict::decode_rle_dictionary_indices_into(
                    &self.idx_scratch,
                    n,
                    &mut self.idx_buf,
                )
                .map_err(|e| ext(format!("rle_dict_indices: {e}")))?;
                let dict_len = self.dict_views.len();
                if let Some(bad) = self.idx_buf.iter().find(|&&i| (i as usize) >= dict_len) {
                    return Err(ext(format!("dict idx {bad} out of range {dict_len}")));
                }
                self.views.reserve(self.idx_buf.len());
                // Tight gather — same shape as the eager dict-
                // preserved path's per-row hot loop.
                for &i in &self.idx_buf {
                    // SAFETY (logical): bounds validated above.
                    self.views.push(self.dict_views[i as usize]);
                }
            }
            Encoding::Plain => {
                let mut page_buf: Vec<u8> = Vec::with_capacity(body.len() * 2);
                decompress_into(self.codec, body, usize_hdr, &mut page_buf)?;
                let block_id = self.data_buffers.len() as u32;
                // Inline single-pass parse — same shape as
                // `plain_byte_array_to_views_in_place` in
                // `emat_arrow_reader`.
                let mut off = 0usize;
                let page_len = page_buf.len();
                for i in 0..n {
                    if off + 4 > page_len {
                        return Err(ext(format!(
                            "plain byte_array: truncated length prefix at {i}/{n}"
                        )));
                    }
                    let len = u32::from_le_bytes([
                        page_buf[off],
                        page_buf[off + 1],
                        page_buf[off + 2],
                        page_buf[off + 3],
                    ]) as usize;
                    off += 4;
                    if off + len > page_len {
                        return Err(ext(format!(
                            "plain byte_array: value {i}/{n} len {len} overruns page"
                        )));
                    }
                    let bytes = &page_buf[off..off + len];
                    self.views.push(make_view(bytes, block_id, off as u32));
                    off += len;
                }
                self.data_buffers.push(Buffer::from_vec(page_buf));
            }
            other => {
                return Err(ext(format!(
                    "unexpected byte_array data page encoding {other:?}"
                )));
            }
        }

        Ok(self.views.len() - before)
    }

    fn make_array(&self, start: usize, n: usize, _target: &DataType) -> ArrayRef {
        debug_assert!(start + n <= self.views.len());
        let buf = Buffer::from_slice_ref(&self.views[start..start + n]);
        let views_buf = ScalarBuffer::<u128>::new(buf, 0, n);
        // Share all backing data buffers (Arc bump). Each page's
        // buffer is referenced by potentially many slices; that's
        // fine — StringViewArray supports it.
        let arr =
            StringViewArray::try_new(views_buf, self.data_buffers.clone(), None::<NullBuffer>)
                .expect("StringViewArray::try_new on internally-built views");
        Arc::new(arr)
    }
}

// ============================================================
// Streaming reader (driver)
// ============================================================

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex, OnceLock, mpsc};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

// ---------- Shared decode-thread pool (Σ.E5.6) ----------
//
// Problem: spawning one OS thread per (partition × column) bursts the
// active-thread count to N_partitions × N_cols (~36-42 on Q19) on a
// 14-core box. The previous per-RG `std::thread::spawn` version
// regressed the 22-query geomean from 0.9363 → 1.0017 because of this
// oversubscription.
//
// Fix: a process-global pool of `available_parallelism()` worker
// threads. Every partition + every RG submits decode jobs into the
// same pool, so the global active-thread count tracks core count.
// Streams are wrapped in `Mutex<>` so a single pool worker holds the
// per-column lock during decode (no cross-thread contention on the
// stream — one worker per column per moment).
//
// Job submission overhead: ~1µs per send + lock acquisition; cheap
// relative to per-page decode work (~50µs-1ms).

type Job = Box<dyn FnOnce() + Send + 'static>;

struct DecodePool {
    sender: mpsc::Sender<Job>,
    // Worker threads run forever (until process exit). No JoinHandle
    // stored — drop wouldn't be sound since the pool is `static`.
}

impl DecodePool {
    fn new(n_workers: usize) -> Self {
        let (sender, receiver) = mpsc::channel::<Job>();
        let receiver = std::sync::Arc::new(Mutex::new(receiver));
        for i in 0..n_workers {
            let receiver = std::sync::Arc::clone(&receiver);
            std::thread::Builder::new()
                .name(format!("emat-decode-pool-{i}"))
                .spawn(move || {
                    loop {
                        let job = {
                            let guard = match receiver.lock() {
                                Ok(g) => g,
                                Err(_) => return, // poisoned — exit worker
                            };
                            match guard.recv() {
                                Ok(j) => j,
                                Err(_) => return, // sender dropped — exit
                            }
                        };
                        job();
                    }
                })
                .expect("spawn emat decode pool worker");
        }
        Self { sender }
    }

    fn global() -> &'static Self {
        static POOL: OnceLock<DecodePool> = OnceLock::new();
        POOL.get_or_init(|| {
            let n = std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(8);
            DecodePool::new(n)
        })
    }

    fn submit<F: FnOnce() + Send + 'static>(&self, job: F) {
        self.sender
            .send(Box::new(job))
            .expect("emat decode pool send");
    }
}

/// Per-RG decode state, shared between the pool's column-decoder
/// jobs and the emit thread.
///
/// One decoder job per column runs to completion; each job
/// monotonically grows its `rows_decoded[i]` atomic after each page.
/// The emit thread reads atomics + uses the condvar to sleep-wait
/// for progress; it locks each stream only briefly (during slicing).
///
/// This eliminates per-batch mpsc round-trips — for an RG with 16
/// batches and 6 columns, the previous design did 192 channel ops
/// (12 per batch). This design does 12 ops per RG (6 submits +
/// 6 implicit "done" signals via the rows_decoded atomic reaching
/// total).
struct RgDecodeState {
    streams: Vec<Mutex<Box<dyn ColumnPageStream>>>,
    /// Per-column rows decoded so far. Monotonic. `Acquire`/`Release`
    /// pair across decoder and emit thread.
    rows_decoded: Vec<AtomicUsize>,
    /// Per-column total row count (set at open time, immutable).
    total_rows: Vec<usize>,
    /// Per-column target Arrow type (for `make_array`).
    targets: Vec<DataType>,
    /// `(notify_mutex, cv)` — decoder threads `notify_one` after
    /// each page decode; emit thread `wait`s when no column meets
    /// the target.
    notify: Mutex<()>,
    cv: Condvar,
    /// Latched decoder error (set on first failure; subsequent
    /// decoders short-circuit; emit thread reports it).
    error: Mutex<Option<DataFusionError>>,
}

impl RgDecodeState {
    fn decoder_loop(self: std::sync::Arc<Self>, col_idx: usize) {
        loop {
            // Short-circuit if another column already failed.
            if self.error.lock().unwrap().is_some() {
                return;
            }

            let mut guard = match self.streams[col_idx].lock() {
                Ok(g) => g,
                Err(_) => return,
            };
            if guard.rows_decoded() >= guard.total_rows() {
                return;
            }
            let r = match guard.decode_next_page() {
                Ok(_) => guard.rows_decoded(),
                Err(e) => {
                    *self.error.lock().unwrap() = Some(e);
                    drop(guard);
                    // Final wake — emit thread polls error and surfaces it.
                    let _g = self.notify.lock().unwrap();
                    self.cv.notify_one();
                    return;
                }
            };
            drop(guard);
            // Publish progress to emit thread.
            self.rows_decoded[col_idx].store(r, Ordering::Release);
            let _g = self.notify.lock().unwrap();
            self.cv.notify_one();
        }
    }
}

/// Σ.E5.6 page-streaming reader (shared-pool + atomic-poll variant).
///
/// **Per-RG model:** when a new RG opens, one decode-to-completion
/// job per column is submitted to the global `DecodePool`. Each
/// decoder thread runs its `ColumnPageStream` page-by-page,
/// publishing `rows_decoded` via an atomic after each page.
///
/// **Per-batch emit:** the emit thread polls the per-column atomic
/// counters; when every column has `rows_decoded >= cursor +
/// batch_size` (or is exhausted at its `total_rows`), it briefly
/// locks each stream, calls `make_array(cursor, n, target)`, and
/// emits the batch. Between polls it sleeps on the per-RG Condvar
/// (woken by each decoder's `notify_one`).
///
/// **What's eliminated vs the per-batch variant:** the mpsc round-
/// trip per (column × batch). For an RG with 16 batches × 6 cols
/// that's 192 channel ops → 12 per RG. Decoders also run *ahead*
/// of the emit thread, so their pages are pre-positioned by the
/// time the next batch boundary arrives.
///
/// **Sync cost:** one atomic-store + one `notify_one` per page;
/// emit thread does one atomic-load per column per poll iteration.
/// Lock acquisitions only for the brief `make_array` window
/// (typically <10µs).
pub struct EmatPageStreamingReader {
    file: ParquetFile,
    arrow_schema: SchemaRef,
    projection: Vec<usize>,
    row_groups: Vec<usize>,
    batch_size: usize,
    /// Σ.E5.6: cached parquet metadata snapshot (decoded once at
    /// construction, reused across RGs/columns). Same pattern as the
    /// eager `EmatArrowBatchReader`.
    cached_md: std::sync::Arc<CachedFileMetadata>,

    cur_rg_idx: usize,
    cur_rg_state: Option<std::sync::Arc<RgDecodeState>>,
    cur_rg_total: usize,
    cur_rg_row: usize,
}

impl EmatPageStreamingReader {
    pub fn new(
        file: ParquetFile,
        arrow_schema: SchemaRef,
        projection: Vec<usize>,
        row_groups: Vec<usize>,
        batch_size: usize,
    ) -> DfResult<Self> {
        let cached_md = std::sync::Arc::new(CachedFileMetadata::from_file(&file)?);
        Ok(Self {
            file,
            arrow_schema,
            projection,
            row_groups,
            batch_size,
            cached_md,
            cur_rg_idx: 0,
            cur_rg_state: None,
            cur_rg_total: 0,
            cur_rg_row: 0,
        })
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.arrow_schema
    }

    fn open_row_group(&mut self, rg: usize) -> DfResult<()> {
        // Σ.E5.6: use cached metadata snapshot — no thrift re-parse.
        self.cur_rg_total = self.cached_md.row_groups[rg].num_rows as usize;

        let n_cols = self.projection.len();
        let mut streams: Vec<Mutex<Box<dyn ColumnPageStream>>> = Vec::with_capacity(n_cols);
        let mut total_rows: Vec<usize> = Vec::with_capacity(n_cols);
        let mut targets: Vec<DataType> = Vec::with_capacity(n_cols);
        for (i, &leaf) in self.projection.iter().enumerate() {
            let target = self.arrow_schema.field(i).data_type().clone();
            let cm = &self.cached_md.row_groups[rg].columns[leaf];
            let stream: Box<dyn ColumnPageStream> = match &target {
                DataType::Int32 | DataType::Date32 => {
                    Box::new(Int32PageStream::new(&self.file, cm)?)
                }
                // KEYS.4.b — UInt64 shares the i64 page-decode; make_array
                // reinterprets the buffer as UInt64Array.
                DataType::Int64 | DataType::UInt64 => {
                    Box::new(Int64PageStream::new(&self.file, cm)?)
                }
                DataType::Float64 => Box::new(Float64PageStream::new(&self.file, cm)?),
                DataType::Utf8View => Box::new(StringViewPageStream::new(&self.file, cm)?),
                other => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "EmatPageStreamingReader: target type {other:?} not yet supported"
                    )));
                }
            };
            total_rows.push(stream.total_rows());
            targets.push(target);
            streams.push(Mutex::new(stream));
        }

        let state = std::sync::Arc::new(RgDecodeState {
            streams,
            rows_decoded: (0..n_cols).map(|_| AtomicUsize::new(0)).collect(),
            total_rows,
            targets,
            notify: Mutex::new(()),
            cv: Condvar::new(),
            error: Mutex::new(None),
        });

        // Submit one decode-to-completion job per column.
        let pool = DecodePool::global();
        for col_idx in 0..n_cols {
            let state = std::sync::Arc::clone(&state);
            pool.submit(move || state.decoder_loop(col_idx));
        }

        self.cur_rg_state = Some(state);
        self.cur_rg_row = 0;
        Ok(())
    }

    /// Wait until every column has either reached `target_rows` or
    /// exhausted its column total. Returns `Err` if any decoder
    /// surfaced an error.
    ///
    /// Lost-wakeup safety: the standard condvar pattern requires the
    /// progress check + `wait` to happen WHILE holding the same mutex
    /// the notifier acquires. Otherwise a decoder can `notify_one`
    /// between the emit thread's atomic load and its `wait` call —
    /// the signal is lost and emit hangs forever. We hold `notify`
    /// for the entire loop body and only release it inside `wait`.
    fn wait_for_target(state: &RgDecodeState, target_rows: usize) -> DfResult<()> {
        let mut guard = state
            .notify
            .lock()
            .map_err(|e| ext(format!("notify lock poisoned: {e}")))?;
        loop {
            // Error check (under the notify lock — decoder takes the
            // same lock before storing into `error`+notifying, so we
            // observe a consistent view).
            if let Some(e) = state.error.lock().unwrap().take() {
                return Err(e);
            }

            let mut all_at_target = true;
            for i in 0..state.rows_decoded.len() {
                // Acquire-load pairs with the decoder's Release-store.
                let r = state.rows_decoded[i].load(Ordering::Acquire);
                if r < target_rows && r < state.total_rows[i] {
                    all_at_target = false;
                    break;
                }
            }
            if all_at_target {
                return Ok(());
            }

            // Release lock, sleep, re-acquire on notify. Any
            // decoder's notify_one that happens between the load
            // above and this wait still wakes us — because the
            // decoder must acquire `notify` (which we still hold) to
            // call notify_one.
            guard = state
                .cv
                .wait(guard)
                .map_err(|e| ext(format!("cv wait poisoned: {e}")))?;
        }
    }

    fn next_batch(&mut self) -> DfResult<Option<RecordBatch>> {
        loop {
            let need_new_rg = self.cur_rg_state.is_none() || self.cur_rg_row >= self.cur_rg_total;
            if need_new_rg {
                // Drop previous state. Decoders may still be running
                // briefly; the Arc keeps the state alive until they
                // finish their current page, then they exit (their
                // streams are at total_rows or they hit the error
                // latch).
                self.cur_rg_state = None;
                if self.cur_rg_idx >= self.row_groups.len() {
                    return Ok(None);
                }
                let rg = self.row_groups[self.cur_rg_idx];
                self.cur_rg_idx += 1;
                self.open_row_group(rg)?;
                if self.cur_rg_total == 0 {
                    continue;
                }
            }

            let remaining = self.cur_rg_total - self.cur_rg_row;
            let n = remaining.min(self.batch_size);
            let start = self.cur_rg_row;
            let target_rows = start + n;

            let state = self
                .cur_rg_state
                .as_ref()
                .expect("cur_rg_state set above")
                .clone();

            Self::wait_for_target(&state, target_rows)?;

            // All columns at target. Briefly lock each to slice.
            let n_cols = state.rows_decoded.len();
            let mut arrays: Vec<ArrayRef> = Vec::with_capacity(n_cols);
            for i in 0..n_cols {
                let guard = state.streams[i]
                    .lock()
                    .map_err(|e| ext(format!("stream lock poisoned: {e}")))?;
                arrays.push(guard.make_array(start, n, &state.targets[i]));
            }
            self.cur_rg_row += n;
            return Ok(Some(
                RecordBatch::try_new(self.arrow_schema.clone(), arrays)
                    .map_err(|e| ext(format!("RecordBatch::try_new: {e}")))?,
            ));
        }
    }
}

impl Iterator for EmatPageStreamingReader {
    type Item = DfResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_batch() {
            Ok(Some(b)) => Some(Ok(b)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

// ============================================================
// Inline streaming reader (single-threaded, parquet-rs shape)
// ============================================================
//
// Σ.E5 (2026-05-19): a SIMPLER alternative to `EmatPageStreamingReader`
// that does no per-column threading, no mutex/condvar, no decode pool.
//
// One next() call:
//   1. Ensure the current RG has decode state (build column streams on
//      first call / RG advance).
//   2. Determine `target = min(cursor + batch_size, rg_total)`.
//   3. For each column, call `decode_next_page` until rows_decoded >=
//      target (or the column is exhausted at total_rows).
//   4. Build a RecordBatch from `[cursor, target)`.
//   5. Advance cursor.
//
// **Why single-threaded?** For the regression queries (single-RG files
// like part / customer / supplier / partsupp), the partition has only
// one RG and one decoder. There is no column-parallel work to lose —
// the existing eager reader's scoped-thread fan-out already shrinks to
// a single thread per partition (parallelism_budget = 1 when n_cols
// dominates). The win is **first-batch latency**: parquet-rs ships the
// first 65k rows after ~1-2ms; eager Emat blocks for ~5-6ms.
//
// **Why not for lineitem?** Lineitem RGs are 1M rows × 7 cols ×
// ~1-2ms/page each. Doing it inline-sequential adds 7× single-threaded
// page decoding per batch — measurably slower than the eager 2-way
// scoped-thread fan-out. Caller picks based on per-RG row count.
//
// **Win path:** caller (EmatixFastParquetExec) auto-routes to this
// reader when its assigned partition holds a single small RG.

/// Σ.E5 inline streaming reader. Single-threaded; decodes pages
/// inline within each `next()` call. Compatible with
/// `EmatArrowBatchReader`'s output: 65k-row `RecordBatch`es matching
/// the projected schema.
pub struct EmatInlineStreamingReader {
    file: ParquetFile,
    arrow_schema: SchemaRef,
    projection: Vec<usize>,
    row_groups: Vec<usize>,
    batch_size: usize,
    cached_md: std::sync::Arc<CachedFileMetadata>,

    /// Index into `row_groups` of the RG we're currently emitting from.
    cur_rg_idx: usize,
    /// Total rows in the current RG (zero until first RG opens).
    cur_rg_total: usize,
    /// One column-stream per projected column. `None` between RGs.
    cur_rg_columns: Option<Vec<Box<dyn ColumnPageStream>>>,
    /// Next row to emit within the current RG.
    cursor: usize,
}

impl EmatInlineStreamingReader {
    pub fn new(
        file: ParquetFile,
        arrow_schema: SchemaRef,
        projection: Vec<usize>,
        row_groups: Vec<usize>,
        batch_size: usize,
    ) -> DfResult<Self> {
        let cached_md = std::sync::Arc::new(CachedFileMetadata::from_file(&file)?);
        Ok(Self {
            file,
            arrow_schema,
            projection,
            row_groups,
            batch_size: batch_size.max(1),
            cached_md,
            cur_rg_idx: 0,
            cur_rg_total: 0,
            cur_rg_columns: None,
            cursor: 0,
        })
    }

    fn open_row_group(&mut self, rg: usize) -> DfResult<()> {
        self.cur_rg_total = self.cached_md.row_groups[rg].num_rows as usize;
        let mut streams: Vec<Box<dyn ColumnPageStream>> = Vec::with_capacity(self.projection.len());
        for (i, &leaf) in self.projection.iter().enumerate() {
            let target = self.arrow_schema.field(i).data_type();
            let cm = &self.cached_md.row_groups[rg].columns[leaf];
            let stream: Box<dyn ColumnPageStream> = match target {
                DataType::Int32 | DataType::Date32 => {
                    Box::new(Int32PageStream::new(&self.file, cm)?)
                }
                // KEYS.4.b — UInt64 shares the i64 page-decode; make_array
                // reinterprets the buffer as UInt64Array.
                DataType::Int64 | DataType::UInt64 => {
                    Box::new(Int64PageStream::new(&self.file, cm)?)
                }
                DataType::Float64 => Box::new(Float64PageStream::new(&self.file, cm)?),
                DataType::Utf8View => Box::new(StringViewPageStream::new(&self.file, cm)?),
                other => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "EmatInlineStreamingReader: target type {other:?} not yet supported"
                    )));
                }
            };
            streams.push(stream);
        }
        self.cur_rg_columns = Some(streams);
        self.cursor = 0;
        Ok(())
    }

    fn next_batch(&mut self) -> DfResult<Option<RecordBatch>> {
        loop {
            // Advance to the next RG when the current one is done /
            // unset.
            let need_new_rg = self.cur_rg_columns.is_none() || self.cursor >= self.cur_rg_total;
            if need_new_rg {
                if self.cur_rg_idx >= self.row_groups.len() {
                    return Ok(None);
                }
                let rg = self.row_groups[self.cur_rg_idx];
                self.cur_rg_idx += 1;
                self.open_row_group(rg)?;
                if self.cur_rg_total == 0 {
                    // Empty RG — try next.
                    self.cur_rg_columns = None;
                    continue;
                }
            }

            // Decode pages until every column has reached `target`
            // rows (or its column total). Single-threaded: each column
            // is exhausted in turn before moving to the next.
            //
            // `decode_next_page` may legitimately return Ok(0) for
            // non-data pages (dict page on first call; IndexPage; etc.)
            // — we re-poll. To avoid an infinite loop on a stuck
            // stream we cap the consecutive zero-progress iterations.
            let target = (self.cursor + self.batch_size).min(self.cur_rg_total);
            let cols = self
                .cur_rg_columns
                .as_mut()
                .expect("cur_rg_columns must be Some past the new-RG branch");
            for col in cols.iter_mut() {
                let mut zero_streak = 0usize;
                while col.rows_decoded() < target && col.rows_decoded() < col.total_rows() {
                    let before = col.rows_decoded();
                    let added = col.decode_next_page()?;
                    debug_assert!(col.rows_decoded() == before + added);
                    if added == 0 {
                        zero_streak += 1;
                        if zero_streak > 4 {
                            return Err(ext(format!(
                                "inline reader: column stuck at {} / {} rows after {} consecutive zero-progress pages",
                                col.rows_decoded(),
                                col.total_rows(),
                                zero_streak,
                            )));
                        }
                    } else {
                        zero_streak = 0;
                    }
                }
            }

            // Slice the batch. `n` is bounded by what's actually
            // decoded (matches `target` when no column hit its end
            // first; otherwise the minimum across columns).
            let mut n = target - self.cursor;
            for col in cols.iter() {
                let avail = col.rows_decoded().saturating_sub(self.cursor);
                if avail < n {
                    n = avail;
                }
            }
            if n == 0 {
                // Shouldn't happen given the decode loop above — but
                // guard so we don't emit an empty batch.
                continue;
            }

            let mut arrays: Vec<ArrayRef> = Vec::with_capacity(cols.len());
            for (i, col) in cols.iter().enumerate() {
                let dt = self.arrow_schema.field(i).data_type();
                arrays.push(col.make_array(self.cursor, n, dt));
            }
            let batch = RecordBatch::try_new(self.arrow_schema.clone(), arrays)
                .map_err(|e| ext(format!("RecordBatch::try_new: {e}")))?;
            self.cursor += n;
            return Ok(Some(batch));
        }
    }
}

impl Iterator for EmatInlineStreamingReader {
    type Item = DfResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_batch() {
            Ok(Some(b)) => Some(Ok(b)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use arrow_schema::{Field, Schema};
    use ematix_parquet_codec::write::{
        ColumnData, write_table_to_path, write_table_with_dict_to_path,
    };
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
        let mut stream = {
            let _md = CachedFileMetadata::from_file(&file).unwrap();
            Float64PageStream::new(&file, &_md.row_groups[0].columns[0])
        }
        .expect("open stream");

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
            assert!(
                page_calls <= 2048,
                "too many pages — stream not terminating"
            );
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

    #[test]
    fn int32_page_stream_yields_pages_then_terminates() {
        let palette: Vec<i32> = (0..50).collect();
        let n: usize = 10_000;
        let i32s: Vec<i32> = (0..n).map(|i| palette[i % palette.len()]).collect();
        let path = tmp_parquet("i32_page_stream");
        write_table_with_dict_to_path(
            &path,
            &[("c_i32", ColumnData::I32(&i32s))],
            CompressionCodec::Uncompressed,
            usize::MAX,
            &[true],
        )
        .unwrap();
        let file = ParquetFile::open(&path).unwrap();
        let mut stream = {
            let _md = CachedFileMetadata::from_file(&file).unwrap();
            Int32PageStream::new(&file, &_md.row_groups[0].columns[0])
        }
        .unwrap();
        let mut page_calls = 0;
        while stream.rows_decoded() < n {
            stream.decode_next_page().unwrap();
            page_calls += 1;
            assert!(page_calls <= 2048);
        }
        assert_eq!(stream.rows_decoded(), n);
        assert!(page_calls >= 2);
        let arr = stream.make_array(0, 100, &DataType::Int32);
        let i32_arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
        for i in 0..100 {
            assert_eq!(i32_arr.value(i), (i % 50) as i32);
        }
        // Date32 dispatch.
        let date_arr = stream.make_array(0, 10, &DataType::Date32);
        assert_eq!(date_arr.data_type(), &DataType::Date32);
    }

    #[test]
    fn string_view_page_stream_dict_encoded() {
        // Dict-encoded byte_array column. Streams as: dict page →
        // index pages, with views referencing data_buffers[0].
        let palette: Vec<&[u8]> = vec![
            b"apple",
            b"banana",
            b"cherry",
            b"date",
            b"elderberry",
            b"fig",
            b"grape",
            b"honeydew",
        ];
        let n: usize = 10_000;
        let rows: Vec<&[u8]> = (0..n).map(|i| palette[i % palette.len()]).collect();
        let path = tmp_parquet("svps_dict");
        write_table_with_dict_to_path(
            &path,
            &[("s", ColumnData::ByteArray(&rows))],
            CompressionCodec::Uncompressed,
            usize::MAX,
            &[true],
        )
        .unwrap();
        let file = ParquetFile::open(&path).unwrap();
        let mut stream = {
            let _md = CachedFileMetadata::from_file(&file).unwrap();
            StringViewPageStream::new(&file, &_md.row_groups[0].columns[0])
        }
        .unwrap();
        let mut page_calls = 0;
        while stream.rows_decoded() < n {
            stream.decode_next_page().unwrap();
            page_calls += 1;
            assert!(page_calls <= 2048);
        }
        assert_eq!(stream.rows_decoded(), n);
        assert!(page_calls >= 2, "got {page_calls} pages");
        let arr = stream.make_array(0, 200, &DataType::Utf8View);
        let sv = arr.as_any().downcast_ref::<StringViewArray>().unwrap();
        for i in 0..200 {
            let s = std::str::from_utf8(sv.value(i).as_bytes()).unwrap();
            let expected = std::str::from_utf8(palette[i % palette.len()]).unwrap();
            assert_eq!(s, expected, "row {i}");
        }
    }

    #[test]
    fn string_view_page_stream_plain_encoded() {
        // PLAIN-encoded byte_array column (no dict). Each data page
        // gets its own backing buffer; views encode the page block id.
        // Force PLAIN by using high-entropy unique strings + dict
        // opt-out.
        let n: usize = 10_000;
        let strings: Vec<String> = (0..n)
            .map(|i| format!("unique_value_with_padding_for_size_{i:010}"))
            .collect();
        let rows: Vec<&[u8]> = strings.iter().map(|s| s.as_bytes()).collect();
        let path = tmp_parquet("svps_plain");
        write_table_with_dict_to_path(
            &path,
            &[("s", ColumnData::ByteArray(&rows))],
            CompressionCodec::Uncompressed,
            usize::MAX,
            &[false], // dict OFF — force PLAIN
        )
        .unwrap();
        let file = ParquetFile::open(&path).unwrap();
        let mut stream = {
            let _md = CachedFileMetadata::from_file(&file).unwrap();
            StringViewPageStream::new(&file, &_md.row_groups[0].columns[0])
        }
        .unwrap();
        while stream.rows_decoded() < n {
            stream.decode_next_page().unwrap();
        }
        assert_eq!(stream.rows_decoded(), n);
        let arr = stream.make_array(0, 100, &DataType::Utf8View);
        let sv = arr.as_any().downcast_ref::<StringViewArray>().unwrap();
        for (i, expected) in strings.iter().enumerate().take(100) {
            let s = std::str::from_utf8(sv.value(i).as_bytes()).unwrap();
            assert_eq!(s, expected, "row {i}");
        }
    }

    /// End-to-end reader test: 3-column mixed-type RG decoded
    /// through the streaming reader. Verifies that batches arrive
    /// correctly, all 3 column types interleave, and the row counts
    /// sum to the file total.
    #[test]
    fn streaming_reader_round_trips_mixed_types() {
        let path = tmp_parquet("streaming_reader_mixed");
        let n: usize = 50_000;
        let i32s: Vec<i32> = (0..n as i32).collect();
        let f64s: Vec<f64> = (0..n).map(|x| x as f64 * 0.5).collect();
        let palette: Vec<&[u8]> = vec![b"red", b"green", b"blue", b"yellow"];
        let strs: Vec<&[u8]> = (0..n).map(|i| palette[i % palette.len()]).collect();
        write_table_to_path(
            &path,
            &[
                ("c_i32", ColumnData::I32(&i32s)),
                ("c_f64", ColumnData::F64(&f64s)),
                ("c_str", ColumnData::ByteArray(&strs)),
            ],
            CompressionCodec::Uncompressed,
        )
        .unwrap();

        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("c_i32", DataType::Int32, false),
            Field::new("c_f64", DataType::Float64, false),
            Field::new("c_str", DataType::Utf8View, false),
        ]));

        let file = ParquetFile::open(&path).unwrap();
        let reader =
            EmatPageStreamingReader::new(file, schema, vec![0, 1, 2], vec![0], 8192).unwrap();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, n);
        // Multiple batches (batch_size=8192 vs 50K rows → ≥ 6 batches).
        assert!(batches.len() >= 6, "got {} batches", batches.len());

        // Spot-check the first batch.
        let first = &batches[0];
        assert_eq!(first.num_columns(), 3);
        let i = first
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let f = first
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let s = first
            .column(2)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        assert_eq!(i.value(0), 0);
        assert_eq!(f.value(0), 0.0);
        assert_eq!(s.value(0), "red");
        assert_eq!(i.value(3), 3);
        assert_eq!(
            std::str::from_utf8(s.value(3).as_bytes()).unwrap(),
            std::str::from_utf8(palette[3]).unwrap()
        );
    }

    #[test]
    fn int64_page_stream_yields_pages_then_terminates() {
        let palette: Vec<i64> = (0..50).map(|x| x * 1_000_000_000_000i64).collect();
        let n: usize = 10_000;
        let i64s: Vec<i64> = (0..n).map(|i| palette[i % palette.len()]).collect();
        let path = tmp_parquet("i64_page_stream");
        write_table_with_dict_to_path(
            &path,
            &[("c_i64", ColumnData::I64(&i64s))],
            CompressionCodec::Uncompressed,
            usize::MAX,
            &[true],
        )
        .unwrap();
        let file = ParquetFile::open(&path).unwrap();
        let mut stream = {
            let _md = CachedFileMetadata::from_file(&file).unwrap();
            Int64PageStream::new(&file, &_md.row_groups[0].columns[0])
        }
        .unwrap();
        let mut page_calls = 0;
        while stream.rows_decoded() < n {
            stream.decode_next_page().unwrap();
            page_calls += 1;
            assert!(page_calls <= 2048);
        }
        assert_eq!(stream.rows_decoded(), n);
        assert!(page_calls >= 2);
        let arr = stream.make_array(0, 100, &DataType::Int64);
        let i64_arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..100 {
            assert_eq!(i64_arr.value(i), (i % 50) as i64 * 1_000_000_000_000i64);
        }
    }
}
