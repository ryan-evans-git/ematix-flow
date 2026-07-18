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
use parquet::data_type::ByteArray;
use parquet::file::reader::{FileReader, SerializedFileReader};

use crate::chunk::DataChunk;
use crate::vector::{LogicalType, Vector};

/// How to decode a requested column.
#[derive(Clone, Copy, Debug)]
pub enum ColKind {
    /// INT32 physical → i32 storage, carrying the given logical type.
    I32(LogicalType),
    /// INT64 physical → i64 storage (keys / FKs on dimension tables).
    I64,
    /// DOUBLE physical → f64 storage.
    F64,
    /// BYTE_ARRAY physical → Utf8 storage (dimension-table string predicates).
    Utf8,
}

/// A stock-parquet scan handle exposing **per-row-group** decode — what the
/// executor's morsel-parallel driver needs for string-bearing scans (each
/// worker opens its own handle; the footer parse is cheap).
pub struct StockScan {
    reader: SerializedFileReader<File>,
    columns: Vec<(String, ColKind)>,
    leaf_of: Vec<usize>,
}

impl StockScan {
    /// Open `path` and resolve `columns` (by leaf name) to leaf indices.
    pub fn open(path: &Path, columns: &[(&str, ColKind)]) -> Result<Self, String> {
        let file = File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
        let reader = SerializedFileReader::new(file).map_err(|e| format!("parquet open: {e}"))?;
        let descr = reader.metadata().file_metadata().schema_descr();
        let mut leaf_of = Vec::with_capacity(columns.len());
        for (name, _) in columns {
            let idx = (0..descr.num_columns())
                .find(|&i| descr.column(i).name() == *name)
                .ok_or_else(|| format!("column {name} not found in {}", path.display()))?;
            leaf_of.push(idx);
        }
        Ok(StockScan {
            reader,
            columns: columns.iter().map(|&(n, k)| (n.to_string(), k)).collect(),
            leaf_of,
        })
    }

    pub fn n_row_groups(&self) -> usize {
        self.reader.metadata().num_row_groups()
    }

    /// Decode one row group into a [`DataChunk`] (column order = the open
    /// order).
    pub fn decode_rg(&self, rg: usize) -> Result<DataChunk, String> {
        decode_stock_rg(&self.reader, &self.columns, &self.leaf_of, rg)
    }
}

/// Decode `columns` (by leaf name) from every row group, yielding one
/// [`DataChunk`] per row group. Column order within each chunk matches
/// the order of `columns`.
pub fn scan_columns(path: &Path, columns: &[(&str, ColKind)]) -> Result<Vec<DataChunk>, String> {
    let scan = StockScan::open(path, columns)?;
    (0..scan.n_row_groups())
        .map(|rg| scan.decode_rg(rg))
        .collect()
}

fn decode_stock_rg(
    reader: &SerializedFileReader<File>,
    columns: &[(String, ColKind)],
    leaf_of: &[usize],
    rg: usize,
) -> Result<DataChunk, String> {
    {
        let rg_reader = reader
            .get_row_group(rg)
            .map_err(|e| format!("row group {rg}: {e}"))?;
        let rows = rg_reader.metadata().num_rows() as usize;

        let mut cols = Vec::with_capacity(columns.len());
        for (ci, (name, kind)) in columns.iter().enumerate() {
            let kind = *kind;
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
                ColKind::I64 => {
                    let mut typed = match cr {
                        ColumnReader::Int64ColumnReader(r) => r,
                        _ => return Err(format!("expected INT64 for {name}")),
                    };
                    let mut vals: Vec<i64> = Vec::with_capacity(rows);
                    while vals.len() < rows {
                        let (records, _, _) = typed
                            .read_records(rows - vals.len(), None, None, &mut vals)
                            .map_err(|e| format!("read i64 {name}: {e}"))?;
                        if records == 0 {
                            break;
                        }
                    }
                    Vector::i64(vals)
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
                ColKind::Utf8 => {
                    let mut typed = match cr {
                        ColumnReader::ByteArrayColumnReader(r) => r,
                        _ => return Err(format!("expected BYTE_ARRAY for {name}")),
                    };
                    let mut vals: Vec<ByteArray> = Vec::with_capacity(rows);
                    while vals.len() < rows {
                        let (records, _, _) = typed
                            .read_records(rows - vals.len(), None, None, &mut vals)
                            .map_err(|e| format!("read utf8 {name}: {e}"))?;
                        if records == 0 {
                            break;
                        }
                    }
                    // Pack the byte arrays into one buffer + offsets.
                    let mut offsets: Vec<u32> = Vec::with_capacity(vals.len() + 1);
                    let mut data: Vec<u8> = Vec::new();
                    offsets.push(0);
                    for ba in &vals {
                        data.extend_from_slice(ba.data());
                        offsets.push(data.len() as u32);
                    }
                    Vector::utf8(offsets, data)
                }
            };
            cols.push(vector);
        }
        Ok(DataChunk::new(cols))
    }
}
