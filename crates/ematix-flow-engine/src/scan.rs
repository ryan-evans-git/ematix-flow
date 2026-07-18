//! P0 scan: decode named columns from a Parquet file into native
//! [`Vector`]s via the `parquet` crate's LOW-LEVEL column reader.
//!
//! This is a byte-level file reader (typed physical values), NOT the
//! Arrow reader — no `arrow::Array` crosses into the engine. It is the
//! P0 decode *source* only; P1 swaps in `ematix-parquet`'s native-vector
//! decode (the fast codec we own). Kept deliberately small and obvious:
//! the risk P0 de-risks is the push spine, not decode (already proven).

use std::fs::File;
use std::path::Path;

use parquet::column::reader::ColumnReader;
use parquet::file::reader::{FileReader, SerializedFileReader};

use crate::chunk::DataChunk;
use crate::vector::{LogicalType, Vector};

/// How to decode a requested column.
#[derive(Clone, Copy, Debug)]
pub enum ColKind {
    /// INT32 physical → i32 storage, carrying the given logical type.
    I32(LogicalType),
    /// DOUBLE physical → f64 storage.
    F64,
}

/// Decode `columns` (by leaf name) from every row group, yielding one
/// [`DataChunk`] per row group. Column order within each chunk matches
/// the order of `columns`.
pub fn scan_columns(path: &Path, columns: &[(&str, ColKind)]) -> Result<Vec<DataChunk>, String> {
    let file = File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    let reader = SerializedFileReader::new(file).map_err(|e| format!("parquet open: {e}"))?;
    let meta = reader.metadata();
    let descr = meta.file_metadata().schema_descr();

    // Resolve each requested name to its leaf-column index.
    let mut leaf_of = Vec::with_capacity(columns.len());
    for (name, _) in columns {
        let idx = (0..descr.num_columns())
            .find(|&i| descr.column(i).name() == *name)
            .ok_or_else(|| format!("column {name} not found in {}", path.display()))?;
        leaf_of.push(idx);
    }

    let n_rg = meta.num_row_groups();
    let mut chunks = Vec::with_capacity(n_rg);
    for rg in 0..n_rg {
        let rg_reader = reader
            .get_row_group(rg)
            .map_err(|e| format!("row group {rg}: {e}"))?;
        let rows = rg_reader.metadata().num_rows() as usize;

        let mut cols = Vec::with_capacity(columns.len());
        for (ci, &(name, kind)) in columns.iter().enumerate() {
            let cr = rg_reader
                .get_column_reader(leaf_of[ci])
                .map_err(|e| format!("col reader {name}: {e}"))?;
            let vector = match kind {
                ColKind::I32(logical) => {
                    let mut typed = match cr {
                        ColumnReader::Int32ColumnReader(r) => r,
                        _ => return Err(format!("expected INT32 for {name}")),
                    };
                    let mut vals: Vec<i32> = Vec::with_capacity(rows);
                    while vals.len() < rows {
                        let (records, _, _) = typed
                            .read_records(rows - vals.len(), None, None, &mut vals)
                            .map_err(|e| format!("read i32 {name}: {e}"))?;
                        if records == 0 {
                            break;
                        }
                    }
                    Vector::i32(vals, logical)
                }
                ColKind::F64 => {
                    let mut typed = match cr {
                        ColumnReader::DoubleColumnReader(r) => r,
                        _ => return Err(format!("expected DOUBLE for {name}")),
                    };
                    let mut vals: Vec<f64> = Vec::with_capacity(rows);
                    while vals.len() < rows {
                        let (records, _, _) = typed
                            .read_records(rows - vals.len(), None, None, &mut vals)
                            .map_err(|e| format!("read f64 {name}: {e}"))?;
                        if records == 0 {
                            break;
                        }
                    }
                    Vector::f64(vals)
                }
            };
            cols.push(vector);
        }
        chunks.push(DataChunk::new(cols));
    }
    Ok(chunks)
}
