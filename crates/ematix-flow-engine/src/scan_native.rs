//! Native scan via ematix-parquet (DF-free, Arrow-free): decode
//! row-group columns straight into engine [`Vector`]s.
//!
//! This is the engine's *real* decode path — it replaces the P0
//! stock-parquet scan (`src/scan.rs`, kept only as a cross-check
//! reference). It calls the ematix-parquet codec's masked-decode
//! primitives directly (the same ones `ematix-flow-core` wraps in its
//! bridge), so the engine keeps no DataFusion dependency. Columns are
//! addressed by leaf index (schema order); name→index resolution is a
//! planner concern (P3).

use std::path::Path;

use ematix_parquet_codec::read::{
    read_column_f64_masked_into, read_column_i32_masked_into, read_column_i64_masked_into,
};
use ematix_parquet_io::ParquetFile;

use crate::chunk::DataChunk;
use crate::vector::{LogicalType, Vector};

/// How to decode a requested column (physical type → engine storage).
#[derive(Clone, Copy, Debug)]
pub enum NativeColKind {
    /// INT32 physical → i32 storage, carrying `logical` (e.g. `Date32`).
    I32(LogicalType),
    /// INT64 physical → i64 storage (join keys / FKs).
    I64,
    /// DOUBLE physical → f64 storage.
    F64,
}

/// All-ones row mask for a full (unfiltered) scan of `nrows` rows.
fn all_ones_mask(nrows: usize) -> Vec<u8> {
    let nb = nrows.div_ceil(8);
    let mut m = vec![0xFFu8; nb];
    let rem = nrows % 8;
    if rem != 0 {
        m[nb - 1] = (1u8 << rem) - 1;
    }
    m
}

/// Decode `columns` (by leaf index) from a single row group `rg`
/// (`nrows` rows) into one [`DataChunk`], column order matching
/// `columns`. Full scan (all-ones mask). `file` is shared read-only
/// across decode threads (ematix-parquet's lock-free pread model), so
/// this is the per-morsel unit the parallel driver calls.
pub fn decode_row_group(
    file: &ParquetFile,
    rg: usize,
    nrows: usize,
    columns: &[(usize, NativeColKind)],
) -> Result<DataChunk, String> {
    let mask = all_ones_mask(nrows);
    let mut cols = Vec::with_capacity(columns.len());
    for &(leaf, kind) in columns {
        let v = match kind {
            NativeColKind::I32(logical) => {
                let mut out: Vec<i32> = Vec::with_capacity(nrows);
                read_column_i32_masked_into(file, rg, leaf, &mask, &mut out)
                    .map_err(|e| format!("decode i32 col {leaf} rg {rg}: {e}"))?;
                Vector::i32(out, logical)
            }
            NativeColKind::I64 => {
                let mut out: Vec<i64> = Vec::with_capacity(nrows);
                read_column_i64_masked_into(file, rg, leaf, &mask, &mut out)
                    .map_err(|e| format!("decode i64 col {leaf} rg {rg}: {e}"))?;
                Vector::i64(out)
            }
            NativeColKind::F64 => {
                let mut out: Vec<f64> = Vec::with_capacity(nrows);
                read_column_f64_masked_into(file, rg, leaf, &mask, &mut out)
                    .map_err(|e| format!("decode f64 col {leaf} rg {rg}: {e}"))?;
                Vector::f64(out)
            }
        };
        cols.push(v);
    }
    Ok(DataChunk::new(cols))
}

/// Decode `columns` from every row group, yielding one [`DataChunk`] per
/// row group (sequential). The parallel path is
/// [`crate::exec::run_scan_pipeline`].
pub fn scan_row_groups(
    path: &Path,
    columns: &[(usize, NativeColKind)],
) -> Result<Vec<DataChunk>, String> {
    let file = ParquetFile::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    let md = file.metadata().map_err(|e| format!("metadata: {e}"))?;
    let n_rg = md.row_groups.len();
    let mut chunks = Vec::with_capacity(n_rg);
    for rg in 0..n_rg {
        let nrows = md.row_groups[rg].num_rows as usize;
        chunks.push(decode_row_group(&file, rg, nrows, columns)?);
    }
    Ok(chunks)
}
