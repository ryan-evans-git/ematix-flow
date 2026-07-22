//! P0 scan: decode named columns from a Parquet file into native
//! [`Vector`]s via the `parquet` crate's LOW-LEVEL column reader.
//!
//! This is a byte-level file reader (typed physical values), NOT the
//! Arrow reader — no `arrow::Array` crosses into the engine. It is the
//! P0 decode *source* only; P1 swaps in `ematix-parquet`'s native-vector
//! decode (the fast codec we own) for the required-numeric fast path.
//! This reader keeps the general cases: strings, `optional` columns
//! (definition levels → validity), and INT-backed decimals (scaled into
//! f64 on decode).

use std::fs::File;
use std::path::Path;

use parquet::column::reader::{ColumnReader, get_typed_column_reader};
use parquet::data_type::{ByteArrayType, DataType, DoubleType, Int32Type, Int64Type};
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
    /// INT-backed parquet `DECIMAL(p, s)` → f64 storage (÷ `10^s`); the
    /// INT32-vs-INT64 backing width is resolved from the file footer at
    /// open time.
    Dec(u8),
}

/// A stock-parquet scan handle exposing **per-row-group** decode — what the
/// executor's morsel-parallel driver needs for string-bearing scans (each
/// worker opens its own handle; the footer parse is cheap).
pub struct StockScan {
    reader: SerializedFileReader<File>,
    columns: Vec<(String, ColKind)>,
    leaf_of: Vec<usize>,
    /// Per requested column: the leaf's max definition level (0 =
    /// required; >0 = decode reads def levels into validity).
    max_def: Vec<i16>,
    /// Per requested column: the leaf's physical type is INT64 (only
    /// consulted for `ColKind::Dec` width resolution).
    phys64: Vec<bool>,
}

impl StockScan {
    /// Open `path` and resolve `columns` (by leaf name) to leaf indices.
    pub fn open(path: &Path, columns: &[(&str, ColKind)]) -> Result<Self, String> {
        let file = File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
        let reader = SerializedFileReader::new(file).map_err(|e| format!("parquet open: {e}"))?;
        let descr = reader.metadata().file_metadata().schema_descr();
        let mut leaf_of = Vec::with_capacity(columns.len());
        let mut max_def = Vec::with_capacity(columns.len());
        let mut phys64 = Vec::with_capacity(columns.len());
        for (name, kind) in columns {
            let idx = (0..descr.num_columns())
                .find(|&i| descr.column(i).name() == *name)
                .ok_or_else(|| format!("column {name} not found in {}", path.display()))?;
            leaf_of.push(idx);
            max_def.push(descr.column(idx).max_def_level());
            let is64 = descr.column(idx).physical_type() == parquet::basic::Type::INT64;
            if matches!(kind, ColKind::Dec(_))
                && !is64
                && descr.column(idx).physical_type() != parquet::basic::Type::INT32
            {
                return Err(format!(
                    "column {name}: decimal decode needs INT32/INT64 backing, got {}",
                    descr.column(idx).physical_type()
                ));
            }
            phys64.push(is64);
        }
        Ok(StockScan {
            reader,
            columns: columns.iter().map(|&(n, k)| (n.to_string(), k)).collect(),
            leaf_of,
            max_def,
            phys64,
        })
    }

    pub fn n_row_groups(&self) -> usize {
        self.reader.metadata().num_row_groups()
    }

    /// Decode one row group into a [`DataChunk`] (column order = the open
    /// order).
    pub fn decode_rg(&self, rg: usize) -> Result<DataChunk, String> {
        let rg_reader = self
            .reader
            .get_row_group(rg)
            .map_err(|e| format!("row group {rg}: {e}"))?;
        let rows = rg_reader.metadata().num_rows() as usize;

        let mut cols = Vec::with_capacity(self.columns.len());
        for (ci, (name, kind)) in self.columns.iter().enumerate() {
            let cr = rg_reader
                .get_column_reader(self.leaf_of[ci])
                .map_err(|e| format!("col reader {name}: {e}"))?;
            let max_def = self.max_def[ci];
            let vector = match *kind {
                ColKind::I32(logical) => {
                    let (vals, valid) = read_leaf::<Int32Type>(cr, rows, max_def, name)?;
                    Vector::i32(vals, logical).with_validity(valid)
                }
                ColKind::I64 => {
                    let (vals, valid) = read_leaf::<Int64Type>(cr, rows, max_def, name)?;
                    Vector::i64(vals).with_validity(valid)
                }
                ColKind::F64 => {
                    let (vals, valid) = read_leaf::<DoubleType>(cr, rows, max_def, name)?;
                    Vector::f64(vals).with_validity(valid)
                }
                ColKind::Dec(scale) => {
                    let div = 10f64.powi(scale as i32);
                    if self.phys64[ci] {
                        let (vals, valid) = read_leaf::<Int64Type>(cr, rows, max_def, name)?;
                        Vector::f64(vals.iter().map(|&v| v as f64 / div).collect())
                            .with_validity(valid)
                    } else {
                        let (vals, valid) = read_leaf::<Int32Type>(cr, rows, max_def, name)?;
                        Vector::f64(vals.iter().map(|&v| v as f64 / div).collect())
                            .with_validity(valid)
                    }
                }
                ColKind::Utf8 => {
                    let (vals, valid) = read_leaf::<ByteArrayType>(cr, rows, max_def, name)?;
                    // Pack the byte arrays into one buffer + offsets. NULL
                    // rows are empty with validity false — and must not be
                    // touched: a defaulted `ByteArray` has no backing
                    // buffer and `.data()` panics.
                    let mut offsets: Vec<u32> = Vec::with_capacity(vals.len() + 1);
                    let mut data: Vec<u8> = Vec::new();
                    offsets.push(0);
                    for (i, ba) in vals.iter().enumerate() {
                        if valid.as_ref().is_none_or(|v| v[i]) {
                            data.extend_from_slice(ba.data());
                        }
                        offsets.push(data.len() as u32);
                    }
                    Vector::utf8(offsets, data).with_validity(valid)
                }
            };
            cols.push(vector);
        }
        Ok(DataChunk::new(cols))
    }
}

/// Decoded leaf values + validity (`None` = all valid).
type LeafVals<T> = (Vec<T>, Option<Vec<bool>>);

/// Read one leaf of one row group. Required columns (`max_def == 0`)
/// decode dense; `optional` columns read definition levels and expand the
/// dense non-null values to full length with a validity mask (`None` when
/// the row group happens to contain no NULLs — the branch-free fast path).
fn read_leaf<D: DataType>(
    cr: ColumnReader,
    rows: usize,
    max_def: i16,
    name: &str,
) -> Result<LeafVals<D::T>, String>
where
    D::T: Clone + Default,
{
    let mut typed = get_typed_column_reader::<D>(cr);
    if max_def == 0 {
        let mut vals: Vec<D::T> = Vec::with_capacity(rows);
        while vals.len() < rows {
            let (records, _, _) = typed
                .read_records(rows - vals.len(), None, None, &mut vals)
                .map_err(|e| format!("read {name}: {e}"))?;
            if records == 0 {
                break;
            }
        }
        return Ok((vals, None));
    }
    let mut vals: Vec<D::T> = Vec::with_capacity(rows);
    let mut defs: Vec<i16> = Vec::with_capacity(rows);
    let mut records_read = 0usize;
    while records_read < rows {
        let (records, _, _) = typed
            .read_records(rows - records_read, Some(&mut defs), None, &mut vals)
            .map_err(|e| format!("read {name}: {e}"))?;
        if records == 0 {
            break;
        }
        records_read += records;
    }
    let mut out: Vec<D::T> = Vec::with_capacity(defs.len());
    let mut valid: Vec<bool> = Vec::with_capacity(defs.len());
    let mut vi = 0usize;
    let mut any_null = false;
    for &d in &defs {
        if d == max_def {
            out.push(vals[vi].clone());
            vi += 1;
            valid.push(true);
        } else {
            out.push(D::T::default());
            valid.push(false);
            any_null = true;
        }
    }
    Ok((out, any_null.then_some(valid)))
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
