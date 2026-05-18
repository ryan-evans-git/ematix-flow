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
    /// Σ.E5.1.c: per-RG column-decode parallelism budget. `None` means
    /// "default to `available_parallelism()`" — the original behaviour.
    /// `Some(n)` caps the per-RG scoped-thread count at `n` (still
    /// further capped by `n_projected_columns`). Callers that know how
    /// much outer parallelism is already in play (e.g. a per-partition
    /// `ExecutionPlan`) pass `total_threads / outer_partitions` so the
    /// total concurrent thread count tracks the core count instead of
    /// the product.
    parallelism_budget: Option<usize>,
}

impl EmatArrowBatchReaderBuilder {
    pub fn new(file: ParquetFile, arrow_schema: SchemaRef) -> Self {
        Self {
            file,
            arrow_schema,
            projection: None,
            row_groups: None,
            batch_size: DEFAULT_BATCH_SIZE,
            parallelism_budget: None,
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

    /// Σ.E5.1.c: cap the per-RG column-decode thread count at `n`.
    ///
    /// Default behaviour (no call / `None`) is "saturate
    /// `available_parallelism()`" — every column gets a thread until
    /// core count is hit. Callers that spawn N concurrent readers (one
    /// per outer DataFusion partition) should pass `total_threads / N`
    /// to keep the global thread count anchored to the core count
    /// rather than the product, which oversubscribes the scheduler and
    /// inflates wall-clock variance on the streaming path.
    ///
    /// Setting `n = 1` forces sequential per-RG column decode and is
    /// the confirmation knob for "is per-column parallelism the
    /// dominant cost when outer parallelism already saturates cores?".
    pub fn with_parallelism_budget(mut self, n: usize) -> Self {
        self.parallelism_budget = Some(n.max(1));
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
            parallelism_budget: self.parallelism_budget,
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
///
/// Σ.E5.1.c: primitive variants hold `Buffer` instead of `Vec` so that
/// per-batch slicing is zero-copy (`Buffer::slice_with_length` = Arc
/// bump + offset/length change). Previously `Buffer::from_slice_ref`
/// inside `slice_batch` did a fresh memcpy of every batch — for Q1
/// (3 numeric cols × 65K rows × 8 bytes ≈ 1.5 MB per batch × ~20
/// batches/partition × 6 partitions ≈ 200 MB of copying per query).
enum DecodedColumn {
    /// 4-byte primitives (i32/Date32). `n_rows` is the logical row
    /// count; `data.len() == n_rows * 4`.
    Int32 { data: Buffer, n_rows: usize },
    /// 8-byte primitives (i64). `data.len() == n_rows * 8`.
    Int64 { data: Buffer, n_rows: usize },
    /// 8-byte primitives (f64). `data.len() == n_rows * 8`.
    Float64 { data: Buffer, n_rows: usize },
    /// (views as `Buffer` of `u128`, backing data blocks). The views
    /// buffer is sliced zero-copy per batch (Σ.E5.1.c); the backing
    /// data buffers are shared across every batch in the RG via Arc.
    ///
    /// Σ.E5 (2026-05-18): widened from a single `data: Buffer` to
    /// `data_buffers: Vec<Buffer>` — one buffer per decompressed
    /// page, so we can take ownership of each page's decompressed
    /// scratch directly instead of memcpy-per-row from scratch into
    /// a single accumulator. Skips ~45 MB of memory traffic per RG
    /// on the Q13-shape PLAIN byte_array decode. Views encode the
    /// `block_id` (= index into `data_buffers`) of their source page.
    StringView {
        views: Buffer,
        n_rows: usize,
        data_buffers: Vec<Buffer>,
    },
    /// Dict-preserved BYTE_ARRAY → DictionaryArray<UInt32, Utf8>
    /// values + indices. Indices are RG-local — never spans RGs.
    /// `indices` is a `Buffer` so per-batch key slicing is zero-copy
    /// (Σ.E5.1.c).
    DictUtf8 {
        values: Arc<StringArray>,
        indices: Buffer,
        n_rows: usize,
    },
    /// Slow path: materialised UTF-8 (per-row copy).
    Utf8(Arc<StringArray>),
}

impl DecodedColumn {
    fn len(&self) -> usize {
        match self {
            DecodedColumn::Int32 { n_rows, .. } => *n_rows,
            DecodedColumn::Int64 { n_rows, .. } => *n_rows,
            DecodedColumn::Float64 { n_rows, .. } => *n_rows,
            DecodedColumn::StringView { n_rows, .. } => *n_rows,
            DecodedColumn::DictUtf8 { n_rows, .. } => *n_rows,
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
    /// Σ.E5.1.c: caller-supplied cap on per-RG column-decode threads.
    /// See [`EmatArrowBatchReaderBuilder::with_parallelism_budget`].
    parallelism_budget: Option<usize>,

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
    ///
    /// Parallel-decode shape (Σ.E5.1 follow-up):
    ///
    /// Each projected column's decode is independent — distinct
    /// `PageWalker` over a disjoint byte range from a shared
    /// `ParquetFile` whose `read_range` is lock-free (`pread(2)` on
    /// Unix, positional `ReadFile` on Windows). `ParquetFile` is `Sync`
    /// — borrowing it across `std::thread::scope` works without
    /// re-opening per thread. The `Arc<Schema>` is also `Sync` so each
    /// thread can index its own `target` `DataType`.
    ///
    /// Threadpool sizing: we spawn `min(num_projected_columns,
    /// available_parallelism())` threads. For the Q1 shape (7 columns
    /// on a 14-core machine) every column gets its own thread; for a
    /// 200-column wide table the spawn count is bounded by core count
    /// and threads pull from a shared work queue.
    ///
    /// Memory note: doing N columns concurrently raises per-RG peak
    /// memory by ~Nx vs the sequential path. At SF=1 a numeric RG
    /// column is ~8 MB and 7 concurrent columns peak at ~50-100 MB —
    /// well inside budget. At SF=10+ this is worth revisiting (chunked
    /// or page-streaming decode is the next lever).
    fn load_row_group(&mut self, rg: usize) -> DfResult<()> {
        let md = self
            .file
            .metadata()
            .map_err(|e| ext(format!("metadata: {e}")))?;
        let row_group = &md.row_groups[rg];
        self.cur_rg_total = row_group.num_rows as usize;
        // Drop `md` so `&self.file` is the only outstanding borrow
        // before we hand it to scoped threads.
        drop(md);

        let projection = &self.projection;
        let schema = &self.arrow_schema;
        let file = &self.file;
        let n_cols = projection.len();

        // Cap on spawned threads. Default: never exceed available
        // cores, even for wide projections (Q1 = 7 cols on a 14-core
        // box → 7 threads). When the caller supplied a parallelism
        // budget, honour it instead — this is how the
        // `EmatixFastParquetExec` partition wrapper avoids
        // oversubscribing the scheduler with `N_partitions × N_cols`
        // scoped threads on top of its own outer parallelism.
        let cap = self.parallelism_budget.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(1)
        });
        let max_threads = cap.max(1).min(n_cols.max(1));

        // Sequential fast path: skip scoped-thread overhead when
        // there's nothing to parallelise (single-column projection or
        // single-core machine).
        let cols: Vec<DecodedColumn> = if max_threads <= 1 || n_cols <= 1 {
            let mut out = Vec::with_capacity(n_cols);
            for (proj_idx, &leaf) in projection.iter().enumerate() {
                let target = schema.field(proj_idx).data_type();
                out.push(decode_one_column(file, rg, leaf, target)?);
            }
            out
        } else {
            // Pre-allocate result slots so we can scatter into them
            // by index without a final sort step.
            let mut slots: Vec<Option<DfResult<DecodedColumn>>> =
                (0..n_cols).map(|_| None).collect();

            // Shared work queue — atomic counter handing out the next
            // column index. Caps thread spawn count at `max_threads`
            // while still letting each thread chew through multiple
            // columns when n_cols > cores.
            use std::sync::atomic::{AtomicUsize, Ordering};
            let next = AtomicUsize::new(0);

            std::thread::scope(|s| {
                // Each thread writes into a disjoint subset of `slots`
                // (column index handed out by `next.fetch_add`), so we
                // collect the per-thread results and merge them after
                // join. No interior mutability across threads.
                let mut handles = Vec::with_capacity(max_threads);
                for _ in 0..max_threads {
                    let next = &next;
                    handles.push(s.spawn(move || -> Vec<(usize, DfResult<DecodedColumn>)> {
                        let mut local: Vec<(usize, DfResult<DecodedColumn>)> = Vec::new();
                        loop {
                            let i = next.fetch_add(1, Ordering::Relaxed);
                            if i >= n_cols {
                                break;
                            }
                            let leaf = projection[i];
                            let target = schema.field(i).data_type();
                            local.push((i, decode_one_column(file, rg, leaf, target)));
                        }
                        local
                    }));
                }
                for h in handles {
                    // Propagate panics — a column-decode panic is a
                    // bug, not a recoverable error.
                    let partial = h.join().expect("emat_arrow_reader decode thread panicked");
                    for (i, r) in partial {
                        slots[i] = Some(r);
                    }
                }
            });

            // Fail-fast on the first column error (in projection order
            // — gives deterministic error messages).
            let mut out = Vec::with_capacity(n_cols);
            for (i, slot) in slots.into_iter().enumerate() {
                let r = slot.ok_or_else(|| ext(format!("column {i} decode slot never filled")))?;
                out.push(r?);
            }
            out
        };

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
            let n_rows = v.len();
            Ok(DecodedColumn::Int32 {
                data: Buffer::from_vec(v),
                n_rows,
            })
        }
        DataType::Int64 => {
            let v = decode_dict_chunk_typed::<i64>(file, rg, leaf, |b| {
                decode_plain_i64(b).map_err(|e| ext(format!("plain i64: {e}")))
            })?;
            let n_rows = v.len();
            Ok(DecodedColumn::Int64 {
                data: Buffer::from_vec(v),
                n_rows,
            })
        }
        DataType::Float64 => {
            let v = decode_dict_chunk_typed::<f64>(file, rg, leaf, |b| {
                decode_plain_f64(b).map_err(|e| ext(format!("plain f64: {e}")))
            })?;
            let n_rows = v.len();
            Ok(DecodedColumn::Float64 {
                data: Buffer::from_vec(v),
                n_rows,
            })
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

/// BYTE_ARRAY → `StringViewArray`.
///
/// Σ.E5.1.d: the overwhelmingly common case is a dict-encoded chunk
/// (parquet's default writer behaviour for low-cardinality byte
/// arrays — `l_returnflag` / `l_linestatus` on TPC-H, every enum-ish
/// column in practice). For that case we hand off to
/// `decode_dict_byte_array_to_string_view` which:
///   * decodes the dict + indices once via
///     `read_column_byte_array_dict_preserved` (zero per-row UTF-8
///     validation; reuses the codec's tuned dict-preservation path),
///   * builds a `Vec<u128>` of *per-dict-entry* views — `make_view`
///     (which is `#[inline(never)]` — a real call) runs `dict_len`
///     times instead of `n_rows` times, and
///   * fills the `n_rows` view buffer with a tight gather
///     (`views[r] = dict_views[indices[r] as usize]`) — branch-free,
///     no `extend_from_slice`, no per-row inline/buffer split.
///
/// We use `dict_bytes` straight from the dict-preserved column as the
/// backing data block (no copy). For inline (≤12B) views the data
/// block is unused but it's still attached for >12B-prefix arrays
/// that might land here in future workloads.
///
/// Σ.E5 (2026-05-18) bench-only entry point. Exposes the per-RG
/// StringView decode for direct micro-benchmarks against parquet-rs;
/// returns just `(row_count, total_bytes_decoded)` so we don't have
/// to widen `DecodedColumn`'s visibility.
pub fn decode_byte_array_to_string_view_for_bench(
    file: &ParquetFile,
    rg: usize,
    col: usize,
) -> DfResult<(usize, usize)> {
    let dc = decode_byte_array_to_string_view(file, rg, col)?;
    let rows = dc.len();
    // Total decoded byte size = views buffer (16 bytes/row) + sum of
    // backing data buffers (one per page in the page-streaming layout).
    let bytes = match &dc {
        DecodedColumn::StringView {
            views,
            data_buffers,
            ..
        } => views.len() + data_buffers.iter().map(|b| b.len()).sum::<usize>(),
        _ => 0,
    };
    Ok((rows, bytes))
}

/// Σ.E5 (2026-05-18) bench-only entry point: decode an arbitrary
/// column (numeric or string) for one RG, return `(rows, bytes)`. Used
/// by `sigma_e5_column_decode_diff` to time decode by Arrow type
/// without going through DataFusion / mpsc orchestration.
///
/// `target` must be one of the Arrow types `decode_one_column`
/// recognises (Int32/Date32, Int64, Float64, Utf8View, Dictionary,
/// Utf8). Bench callers pick the type by inspecting parquet-rs's
/// promoted Arrow schema.
pub fn decode_one_column_for_bench(
    file: &ParquetFile,
    rg: usize,
    leaf: usize,
    target: &DataType,
) -> DfResult<(usize, usize)> {
    let dc = decode_one_column(file, rg, leaf, target)?;
    let rows = dc.len();
    let bytes = match &dc {
        DecodedColumn::Int32 { data, .. } => data.len(),
        DecodedColumn::Int64 { data, .. } => data.len(),
        DecodedColumn::Float64 { data, .. } => data.len(),
        DecodedColumn::StringView {
            views,
            data_buffers,
            ..
        } => views.len() + data_buffers.iter().map(|b| b.len()).sum::<usize>(),
        DecodedColumn::DictUtf8 {
            values, indices, ..
        } => indices.len() + values.get_array_memory_size(),
        DecodedColumn::Utf8(a) => a.get_array_memory_size(),
    };
    Ok((rows, bytes))
}

/// For the rare PLAIN-only (no dict) case — extremely unusual in
/// real parquet — we fall back to the previous row-by-row path so we
/// still produce a correct result.
fn decode_byte_array_to_string_view(
    file: &ParquetFile,
    rg: usize,
    col: usize,
) -> DfResult<DecodedColumn> {
    // Fast path: try the dict-preserved reader first. It fails only
    // when the column has no DictionaryPage or has a PLAIN-fallback
    // data page (writer wrote some pages dict, some PLAIN).
    match read_column_byte_array_dict_preserved(file, rg, col) {
        Ok(raw) => Ok(build_string_view_from_dict_preserved(raw)),
        Err(_) => decode_byte_array_to_string_view_slow(file, rg, col),
    }
}

/// Σ.E5.1.d hot path — collapses to a tight gather:
///   1. Build `dict_views: Vec<u128>` once (size = `dict_len`,
///      typically 3–100 for low-card columns).
///   2. For each `idx` in `indices`, `views.push(dict_views[idx])`.
///      No `make_view` call; no inline-vs-buffered branch in the loop.
///   3. Reuse `dict_bytes` as the backing block — zero copy.
fn build_string_view_from_dict_preserved(
    raw: ematix_parquet_codec::read::DictPreservedColumn,
) -> DecodedColumn {
    let dict_len = raw.dict_offsets.len() - 1;
    let n_rows = raw.indices.len();

    // (1) Build one view per dict entry. `make_view` is
    // `#[inline(never)]` so we want this to run dict_len times, not
    // n_rows times.
    let mut dict_views: Vec<u128> = Vec::with_capacity(dict_len);
    for i in 0..dict_len {
        let start = raw.dict_offsets[i] as usize;
        let end = raw.dict_offsets[i + 1] as usize;
        let bytes = &raw.dict_bytes[start..end];
        // block_id = 0 (we attach `dict_bytes` as buffer 0); offset
        // = start (`make_view` ignores it for ≤12B inline strings,
        // uses it for the long path). `make_view` already
        // jump-tables by length to avoid `ptr::copy_nonoverlapping`
        // for inline strings — see godbolt link in arrow-array.
        dict_views.push(make_view(bytes, 0, raw.dict_offsets[i]));
    }

    // (2) Tight gather. Bounds were validated by the dict-preserved
    // reader (it rejects any idx >= dict_len up front), so unchecked
    // indexing is sound — but the bounds check usually elides under
    // the optimiser here anyway. Keep it safe; the read is the cost.
    let mut views: Vec<u128> = Vec::with_capacity(n_rows);
    for &idx in &raw.indices {
        // SAFETY (logical, not unsafe): the dict-preserved reader
        // validates every idx < dict_len before returning.
        views.push(dict_views[idx as usize]);
    }
    debug_assert_eq!(views.len(), n_rows);

    // (3) `dict_bytes` is exactly the backing block our views point
    // at (for >12B strings; ≤12B strings are inlined in the view).
    // Dict-preserved path: single backing buffer (block_id=0) since
    // all values come from the one DictionaryPage.
    DecodedColumn::StringView {
        views: Buffer::from_vec(views),
        n_rows,
        data_buffers: vec![Buffer::from_vec(raw.dict_bytes)],
    }
}

/// Slow path: PLAIN-only or mixed-encoding BYTE_ARRAY → StringView,
/// row-by-row. Pre-Σ.E5.1.d behaviour; kept for correctness when the
/// fast dict-preserved reader can't claim the chunk.
fn decode_byte_array_to_string_view_slow(
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
    let mut views: Vec<u128> = Vec::with_capacity(total);

    // Σ.E5 (2026-05-18): page-streaming layout. Each data page's
    // decompressed bytes become an owned `Vec<u8>` that we hand off
    // as a distinct `Buffer` (`data_buffers[block_id]`). Views into
    // that page use `block_id = data_buffers.len() - 1` so no per-row
    // memcpy into a coalesced backing buffer is needed.
    //
    // The dictionary page (if any) is `data_buffers[0]`; dict-encoded
    // data pages emit views referencing block 0. PLAIN data pages emit
    // views referencing their own (newly-pushed) block.
    let mut data_buffers: Vec<Buffer> = Vec::new();

    // Dict offsets/lengths within `data_buffers[0]`, if a dict page is
    // present.
    let mut dict_offsets: Vec<u32> = Vec::new();
    let mut dict_lengths: Vec<u32> = Vec::new();

    let (first_hdr, first_body) = walker
        .next_page()
        .map_err(|e| ext(format!("next_page (first): {e}")))?
        .ok_or_else(|| ext("empty chunk"))?;

    if first_hdr.dictionary_page_header.is_some() {
        // Decompress the dict page into a fresh owned buffer and
        // record per-entry (offset, length) within it. The decoded
        // bytes are *exactly* what we want as the backing block.
        let mut dict_scratch: Vec<u8> = Vec::with_capacity(first_body.len() * 2);
        decompress_into(codec, first_body, &mut dict_scratch)?;
        let entries = decode_plain_byte_array(&dict_scratch)
            .map_err(|e| ext(format!("plain byte_array dict: {e}")))?;
        // Compute offsets directly into `dict_scratch` by pointer math:
        // every slice from `decode_plain_byte_array` is a view into
        // `dict_scratch`, so its offset = ptr - dict_scratch.as_ptr().
        let base = dict_scratch.as_ptr() as usize;
        for s in &entries {
            let off = (s.as_ptr() as usize - base) as u32;
            dict_offsets.push(off);
            dict_lengths.push(s.len() as u32);
        }
        data_buffers.push(Buffer::from_vec(dict_scratch));
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
        let mut page_buf: Vec<u8> = Vec::with_capacity(first_body.len() * 2);
        decompress_into(codec, first_body, &mut page_buf)?;
        let block_id = data_buffers.len() as u32;
        plain_byte_array_to_views_in_place(&page_buf, &mut views, n, block_id)?;
        data_buffers.push(Buffer::from_vec(page_buf));
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
        match dph.encoding {
            Encoding::RleDictionary | Encoding::PlainDictionary => {
                let mut idx_scratch: Vec<u8> = Vec::with_capacity(body.len() * 2);
                decompress_into(codec, body, &mut idx_scratch)?;
                let idxs = ematix_parquet_codec::dict::decode_rle_dictionary_indices(
                    &idx_scratch,
                    n,
                )
                .map_err(|e| ext(format!("rle_dict_indices byte_array: {e}")))?;
                let dict_len = dict_offsets.len();
                // Dict pages always reside in `data_buffers[0]`.
                let dict_block = 0u32;
                // SAFETY: data_buffers[0] is the dict page; established
                // above. Slicing is sound since dict_offsets/lengths
                // were computed against its full contents.
                let dict_bytes: &[u8] = data_buffers[0].as_slice();
                for &i in &idxs {
                    let i = i as usize;
                    if i >= dict_len {
                        return Err(ext(format!("dict idx {i} out of range {dict_len}")));
                    }
                    let off = dict_offsets[i];
                    let len = dict_lengths[i];
                    let bytes = &dict_bytes[off as usize..(off + len) as usize];
                    views.push(make_view(bytes, dict_block, off));
                }
            }
            Encoding::Plain => {
                let mut page_buf: Vec<u8> = Vec::with_capacity(body.len() * 2);
                decompress_into(codec, body, &mut page_buf)?;
                let block_id = data_buffers.len() as u32;
                plain_byte_array_to_views_in_place(&page_buf, &mut views, n, block_id)?;
                data_buffers.push(Buffer::from_vec(page_buf));
            }
            other => {
                return Err(ext(format!(
                    "unexpected byte_array data page encoding {other:?}"
                )));
            }
        }
    }

    debug_assert_eq!(views.len(), total);

    let n_rows = views.len();
    let views_buffer = Buffer::from_vec(views);
    Ok(DecodedColumn::StringView {
        views: views_buffer,
        n_rows,
        data_buffers,
    })
}

/// Σ.E5 (2026-05-18): PLAIN-encoded BYTE_ARRAY → views buffer in a
/// single pass, pointing views at the decompressed page bytes
/// directly. No coalescing copy.
///
/// `page_buf` is the decompressed page (which the caller will hand
/// off as `data_buffers[block_id]`). Each emitted view encodes
/// `(block_id, offset_in_page)` — Arrow's StringViewArray dereferences
/// it against `data_buffers[block_id]` at access time.
///
/// On Q13's `o_comment` (1.5M rows × ~30 B/value PLAIN) this
/// eliminates ~45 MB of `extend_from_slice` memcpy that the old
/// coalescing path did.
///
/// Format: parquet BYTE_ARRAY PLAIN is `[u32 len][bytes]` repeated.
#[inline]
fn plain_byte_array_to_views_in_place(
    page_buf: &[u8],
    views: &mut Vec<u128>,
    n: usize,
    block_id: u32,
) -> DfResult<()> {
    let mut off = 0usize;
    let page_len = page_buf.len();
    for i in 0..n {
        if off + 4 > page_len {
            return Err(ext(format!(
                "plain byte_array: truncated length prefix at value {i}/{n}, offset {off}/{page_len}"
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
                "plain byte_array: value {i}/{n} length {len} overruns page at offset {off}"
            )));
        }
        let bytes = &page_buf[off..off + len];
        views.push(make_view(bytes, block_id, off as u32));
        off += len;
    }
    Ok(())
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
    let n_rows = raw.indices.len();
    let indices_buf = Buffer::from_vec(raw.indices);
    Ok(DecodedColumn::DictUtf8 {
        values,
        indices: indices_buf,
        n_rows,
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
        DecodedColumn::Int32 { data, .. } => {
            // Σ.E5.1.c: zero-copy slice via `Buffer::slice_with_length`
            // — Arc bump + offset/length update, no memcpy. The
            // previous `Buffer::from_slice_ref(&v[start..start+n])`
            // copied every batch's worth of i32 (4 bytes × 65K rows ×
            // many batches) for nothing.
            let sliced = data.slice_with_length(start * 4, n * 4);
            let scalar = ScalarBuffer::<i32>::new(sliced, 0, n);
            match target {
                DataType::Date32 => Arc::new(Date32Array::new(scalar, None)),
                _ => Arc::new(Int32Array::new(scalar, None)),
            }
        }
        DecodedColumn::Int64 { data, .. } => {
            let sliced = data.slice_with_length(start * 8, n * 8);
            let scalar = ScalarBuffer::<i64>::new(sliced, 0, n);
            Arc::new(Int64Array::new(scalar, None))
        }
        DecodedColumn::Float64 { data, .. } => {
            let sliced = data.slice_with_length(start * 8, n * 8);
            let scalar = ScalarBuffer::<f64>::new(sliced, 0, n);
            Arc::new(Float64Array::new(scalar, None))
        }
        DecodedColumn::StringView {
            views,
            data_buffers,
            ..
        } => {
            // Σ.E5.1.c: zero-copy slice for the views buffer too. Each
            // view is 16 bytes — at 65K rows × ~20 batches × 2 string
            // cols on Q1 that's ~40 MB of u128 copying eliminated.
            // Backing data buffers are shared via Arc bump (one clone
            // per page; cheap — typically 1–6 pages per RG).
            let sliced_views = views.slice_with_length(start * 16, n * 16);
            let views_buf = ScalarBuffer::<u128>::new(sliced_views, 0, n);
            // SAFETY: we built every view ourselves with `make_view`
            // against the corresponding `data_buffers[block_id]` so
            // the (block_id, offset) coordinates are valid and the
            // bytes are valid UTF-8 (parquet Utf8 logical type).
            let arr = StringViewArray::try_new(views_buf, data_buffers.clone(), None::<NullBuffer>)
                .expect("StringViewArray::try_new on internally-built views");
            Arc::new(arr)
        }
        DecodedColumn::DictUtf8 {
            values, indices, ..
        } => {
            // Σ.E5.1.c: zero-copy slice on dict keys.
            let sliced = indices.slice_with_length(start * 4, n * 4);
            let scalar = ScalarBuffer::<u32>::new(sliced, 0, n);
            let keys = UInt32Array::new(scalar, None);
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

    /// Σ.E5.1.d: dict-encoded BYTE_ARRAY → StringView must match an
    /// independently-computed expected `StringViewArray` value-for-
    /// value. Exercises the new gather-from-dict-views fast path
    /// (Q1's `l_returnflag` / `l_linestatus` shape — short strings
    /// inlined in the view).
    #[test]
    fn utf8view_dict_encoded_decode_matches_plain() {
        let path = tmp_parquet("utf8view_dict");
        // Mix of inline (≤12B) and long (>12B) entries to exercise
        // both `make_view` branches inside the dict-views build loop.
        // Q1's returnflag/linestatus are 1-byte; we add longer
        // strings too so the data buffer path is also tested.
        let raw: [&[u8]; 4] = [
            b"R",
            b"AB",
            b"long_string_more_than_twelve_bytes_yo",
            b"another_long_one",
        ];
        let n = 5_000usize;
        let rows: Vec<&[u8]> = (0..n).map(|i| raw[i % raw.len()]).collect();
        // Repeat values heavily → writer emits RLE_DICTIONARY data
        // pages, exercising the fast path.
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

        // Expected StringViewArray from the row sequence the writer
        // saw. Built via the standard arrow-array path so we know
        // it's correct regardless of how we built ours.
        let expected_strs: Vec<&str> = rows
            .iter()
            .map(|s| std::str::from_utf8(s).unwrap())
            .collect();
        let expected = StringViewArray::from(expected_strs);

        // Concatenate every batch row-by-row and compare against
        // `expected`. We can't use array-level equality because we
        // emit one StringViewArray per batch (one per RG slice) and
        // `expected` is one array — but the row sequence is locked.
        let mut got_idx = 0usize;
        for b in &batches {
            let arr = b
                .column(0)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap();
            for i in 0..arr.len() {
                assert_eq!(
                    arr.value(i),
                    expected.value(got_idx),
                    "row {got_idx}: got {:?}, expected {:?}",
                    arr.value(i),
                    expected.value(got_idx),
                );
                got_idx += 1;
            }
        }
        assert_eq!(got_idx, n);
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

    /// Σ.E5.1 follow-up: parallel per-column decode must produce
    /// identical output to its own previous run (no race in the
    /// scoped-thread implementation) and identical output across
    /// batch-size variants.
    ///
    /// Fixture mixes every supported `DecodedColumn` variant —
    /// Int32, Int64, Float64, Utf8View, Dictionary(UInt32, Utf8) —
    /// so each decode path runs concurrently with the others.
    #[test]
    fn parallel_decode_equivalence() {
        let path = tmp_parquet("parallel_eq");
        let n = 12_000usize;
        let c_i32: Vec<i32> = (0..n as i32).collect();
        let c_i64: Vec<i64> = (0..n as i64).map(|x| x * 11).collect();
        let c_f64: Vec<f64> = (0..n).map(|x| x as f64 * 0.25).collect();
        let s_pool = [
            b"alpha_long_string".as_slice(),
            b"beta_long_string".as_slice(),
            b"gamma_long_string".as_slice(),
            b"delta_long_string".as_slice(),
        ];
        let s_view: Vec<&[u8]> = (0..n).map(|i| s_pool[i % s_pool.len()]).collect();
        let d_pool = [b"R".as_slice(), b"A".as_slice(), b"N".as_slice()];
        let s_dict: Vec<&[u8]> = (0..n).map(|i| d_pool[i % d_pool.len()]).collect();

        write_table_with_dict_to_path(
            &path,
            &[
                ("c_i32", ColumnData::I32(&c_i32)),
                ("c_i64", ColumnData::I64(&c_i64)),
                ("c_f64", ColumnData::F64(&c_f64)),
                ("s_view", ColumnData::ByteArray(&s_view)),
                ("s_dict", ColumnData::ByteArray(&s_dict)),
            ],
            CompressionCodec::Uncompressed,
            usize::MAX,
            // Force dict encoding on both byte_array columns so the
            // DictUtf8 + StringView decode paths both run.
            &[false, false, false, true, true],
        )
        .unwrap();

        let dict_ty = DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8));
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("c_i32", DataType::Int32, false),
            Field::new("c_i64", DataType::Int64, false),
            Field::new("c_f64", DataType::Float64, false),
            Field::new("s_view", DataType::Utf8View, false),
            Field::new("s_dict", dict_ty, false),
        ]));

        // Snapshot the rows of every column into plain Vecs for a
        // batch-size-independent equality check.
        type Snapshot = (Vec<i32>, Vec<i64>, Vec<f64>, Vec<String>, Vec<String>);
        fn collect_rows(batches: &[RecordBatch]) -> Snapshot {
            let mut a = Vec::new();
            let mut b = Vec::new();
            let mut c = Vec::new();
            let mut d = Vec::new();
            let mut e = Vec::new();
            for rb in batches {
                let i32_arr = rb.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
                let i64_arr = rb.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                let f64_arr = rb
                    .column(2)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                let sv = rb
                    .column(3)
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .unwrap();
                let dict = rb
                    .column(4)
                    .as_any()
                    .downcast_ref::<DictionaryArray<UInt32Type>>()
                    .unwrap();
                let dict_vals = dict.values().as_string::<i32>();
                let keys = dict.keys();
                for i in 0..rb.num_rows() {
                    a.push(i32_arr.value(i));
                    b.push(i64_arr.value(i));
                    c.push(f64_arr.value(i));
                    d.push(sv.value(i).to_string());
                    e.push(dict_vals.value(keys.value(i) as usize).to_string());
                }
            }
            (a, b, c, d, e)
        }

        fn read_with_batch_size(
            path: &std::path::Path,
            schema: SchemaRef,
            batch_size: usize,
        ) -> Vec<RecordBatch> {
            let file = ParquetFile::open(path).unwrap();
            let reader = EmatArrowBatchReaderBuilder::new(file, schema)
                .with_batch_size(batch_size)
                .build()
                .unwrap();
            reader.map(|b| b.unwrap()).collect()
        }

        // Read twice with the default 65_536 batch size to catch any
        // race in the scoped-thread path.
        let r1 = read_with_batch_size(&path, schema.clone(), DEFAULT_BATCH_SIZE);
        let r2 = read_with_batch_size(&path, schema.clone(), DEFAULT_BATCH_SIZE);
        assert_eq!(collect_rows(&r1), collect_rows(&r2));

        // And cross-batch-size: the row sequence must be identical
        // regardless of slicing.
        let r_small = read_with_batch_size(&path, schema.clone(), 1024);
        let r_large = read_with_batch_size(&path, schema.clone(), 65_536);
        let g = collect_rows(&r1);
        assert_eq!(g, collect_rows(&r_small));
        assert_eq!(g, collect_rows(&r_large));

        // Spot-check the actual values too — guards against a subtler
        // bug where every read produces the same wrong answer.
        assert_eq!(g.0.len(), n);
        assert_eq!(g.0[0], 0);
        assert_eq!(g.0[n - 1], (n - 1) as i32);
        assert_eq!(g.1[7], 7 * 11);
        assert_eq!(g.2[4], 1.0);
        assert_eq!(g.3[0].as_bytes(), s_pool[0]);
        assert_eq!(g.3[5].as_bytes(), s_pool[5 % s_pool.len()]);
        assert_eq!(g.4[0].as_bytes(), d_pool[0]);
        assert_eq!(g.4[8].as_bytes(), d_pool[8 % d_pool.len()]);
    }

    /// Σ.E5.1.c: with `parallelism_budget = 1` (sequential per-RG
    /// column decode), every variant must produce byte-identical
    /// output to the default `available_parallelism()`-saturating
    /// path. Same fixture as `parallel_decode_equivalence`.
    #[test]
    fn parallelism_budget_one_matches_default() {
        let path = tmp_parquet("budget_one_eq");
        let n = 8_000usize;
        let c_i32: Vec<i32> = (0..n as i32).collect();
        let c_i64: Vec<i64> = (0..n as i64).map(|x| x * 13).collect();
        let c_f64: Vec<f64> = (0..n).map(|x| x as f64 * 0.125).collect();
        let s_pool = [
            b"alpha_long_string".as_slice(),
            b"beta_long_string".as_slice(),
            b"gamma_long_string".as_slice(),
        ];
        let s_view: Vec<&[u8]> = (0..n).map(|i| s_pool[i % s_pool.len()]).collect();

        write_table_with_dict_to_path(
            &path,
            &[
                ("c_i32", ColumnData::I32(&c_i32)),
                ("c_i64", ColumnData::I64(&c_i64)),
                ("c_f64", ColumnData::F64(&c_f64)),
                ("s_view", ColumnData::ByteArray(&s_view)),
            ],
            CompressionCodec::Uncompressed,
            usize::MAX,
            &[false, false, false, true],
        )
        .unwrap();

        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("c_i32", DataType::Int32, false),
            Field::new("c_i64", DataType::Int64, false),
            Field::new("c_f64", DataType::Float64, false),
            Field::new("s_view", DataType::Utf8View, false),
        ]));

        let read = |budget: Option<usize>| -> Vec<RecordBatch> {
            let file = ParquetFile::open(&path).unwrap();
            let mut b = EmatArrowBatchReaderBuilder::new(file, schema.clone());
            if let Some(n) = budget {
                b = b.with_parallelism_budget(n);
            }
            b.build().unwrap().map(|x| x.unwrap()).collect()
        };

        fn snap(batches: &[RecordBatch]) -> (Vec<i32>, Vec<i64>, Vec<f64>, Vec<String>) {
            let mut a = Vec::new();
            let mut b = Vec::new();
            let mut c = Vec::new();
            let mut d = Vec::new();
            for rb in batches {
                let i32_arr = rb.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
                let i64_arr = rb.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                let f64_arr = rb
                    .column(2)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                let sv = rb
                    .column(3)
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .unwrap();
                for i in 0..rb.num_rows() {
                    a.push(i32_arr.value(i));
                    b.push(i64_arr.value(i));
                    c.push(f64_arr.value(i));
                    d.push(sv.value(i).to_string());
                }
            }
            (a, b, c, d)
        }

        let default = snap(&read(None));
        let budget_1 = snap(&read(Some(1)));
        let budget_2 = snap(&read(Some(2)));
        assert_eq!(default, budget_1, "budget=1 must match default output");
        assert_eq!(default, budget_2, "budget=2 must match default output");
    }
}
