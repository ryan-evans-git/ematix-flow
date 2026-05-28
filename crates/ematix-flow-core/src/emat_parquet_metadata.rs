//! Σ.Q06.SF10.5.a (2026-05-28) — ematix-parquet-based metadata helpers
//! that replace specific parquet-rs paths in
//! [`crate::ematix_fast_parquet::EmatixFastParquetTableProvider::try_new`].
//!
//! ## Why this exists
//!
//! Profile of Q06 SF=10 (samply + macOS `sample`) showed that ~92% of
//! main-thread CPU during the bench's per-trial provider construction
//! went into the parquet-rs `SerializedPageReader::get_next_page` →
//! `decode_page` → `SnappyCodec::decompress` path. That path is
//! invoked by the Σ.AH.2 Story 1'.2 dict-page distinct-count walk:
//! for every column that has a dictionary page in every row group,
//! we read each dict page to extract `num_values`.
//!
//! parquet-rs's `get_next_page` returns a fully-decoded `Page` —
//! which means it Snappy-decompresses the dict page payload even
//! though we only need the header. For SF=10 lineitem that's up to
//! 928 Snappy decompresses (16 columns × 58 row groups) per provider
//! construction, ~19 ms of main-thread work at the bench's 200-trial
//! loop.
//!
//! The fix here uses ematix-parquet's `read_page_header(&mut Cursor)`
//! which parses only the uncompressed Thrift page header — no
//! decompress, no payload read.
//!
//! ## Scope cut
//!
//! This module currently exposes only the dict-distinct walk path
//! (decompress-free). The other parquet-rs walks in `try_new`
//! (Arrow schema mapping, column statistics, encoding stats,
//! no-nulls) are larger refactors and stay on parquet-rs for now
//! pending follow-up stories Σ.Q06.SF10.5.{c,d,e,f}.
//!
//! See `[[project_q06_sf10_polars_gap_wall]]` and
//! `[[ematix_parquet_repo]]` memory entries for context.

use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, ScalarValue};
use ematix_parquet_format::compact::Cursor;
use ematix_parquet_format::metadata::{read_page_header, ColumnChunk, FileMetaData, SchemaElement};
use ematix_parquet_format::types::{ConvertedType, Encoding, ParquetType};
use ematix_parquet_io::ParquetFile;

/// All the metadata-derived fields `EmatixFastParquetTableProvider::try_new`
/// needs, computed in one pass from the parsed thrift footer via
/// ematix-parquet — replacing the parquet-rs `ArrowReaderMetadata::load`
/// + `SerializedFileReader` path entirely (Σ.Q06.SF10.5.c). The schema
/// is the RAW Arrow schema (Utf8 for byte-array string columns); the
/// caller applies the Σ.E5 Utf8→Utf8View promotion + type validation.
pub struct EmatProviderMetadata {
    pub schema: SchemaRef,
    pub num_rows: usize,
    pub num_row_groups: usize,
    pub rg_num_rows: Vec<usize>,
    pub column_stats: Vec<ColumnStatistics>,
    pub column_is_dict_encoded: Vec<bool>,
    pub column_has_no_nulls: Vec<bool>,
}

/// Map a leaf `SchemaElement` to an Arrow `DataType`. Covers the
/// primitive types `EmatixFastParquetTableProvider` supports
/// (Int32/Int64/Float64/Date32/Utf8) plus a few neighbours; unsupported
/// physical types fall through to a placeholder the provider's own
/// validation rejects (matching the pre-5.c behaviour where parquet-rs
/// produced a type the validator then refused).
fn arrow_type_for(se: &SchemaElement<'_>) -> DataType {
    let is_date = matches!(se.converted_type, Some(ConvertedType::Date))
        || matches!(
            se.logical_type,
            Some(ematix_parquet_format::metadata::LogicalType::Date)
        );
    let is_string = matches!(se.converted_type, Some(ConvertedType::Utf8))
        || matches!(
            se.logical_type,
            Some(ematix_parquet_format::metadata::LogicalType::String)
        );
    match se.column_type {
        Some(ParquetType::Boolean) => DataType::Boolean,
        Some(ParquetType::Int32) if is_date => DataType::Date32,
        Some(ParquetType::Int32) => DataType::Int32,
        Some(ParquetType::Int64) => DataType::Int64,
        Some(ParquetType::Float) => DataType::Float32,
        Some(ParquetType::Double) => DataType::Float64,
        Some(ParquetType::ByteArray) if is_string => DataType::Utf8,
        Some(ParquetType::ByteArray) => DataType::Binary,
        // Int96 / FixedLenByteArray / None: not bridge-supported; emit a
        // placeholder the provider validation will reject.
        _ => DataType::Binary,
    }
}

/// Build the RAW Arrow schema from `FileMetaData.schema`. Handles the
/// flat (no nested groups) shape that the bridge targets: schema[0] is
/// the root group, schema[1..] are leaf columns in order. Returns the
/// fields in column-chunk order so they align with `row_groups[].columns[]`.
fn arrow_schema_from_metadata(meta: &FileMetaData<'_>) -> Result<SchemaRef, EmatMetadataError> {
    if meta.schema.is_empty() {
        return Err(EmatMetadataError::Metadata("empty schema".into()));
    }
    let mut fields: Vec<Field> = Vec::with_capacity(meta.schema.len().saturating_sub(1));
    // Skip schema[0] (root). Each subsequent element with a physical
    // column_type is a leaf. A group node (num_children set, no
    // column_type) would mean a nested schema — the bridge doesn't
    // support those, so bail so the caller can fall back.
    for se in meta.schema.iter().skip(1) {
        if se.column_type.is_none() {
            return Err(EmatMetadataError::Metadata(
                "nested/group schema not supported by ematix metadata mapper".into(),
            ));
        }
        let name = std::str::from_utf8(se.name)
            .map_err(|_| EmatMetadataError::Metadata("non-utf8 column name".into()))?
            .to_string();
        let nullable = !matches!(
            se.repetition_type,
            Some(ematix_parquet_format::types::FieldRepetitionType::Required)
        );
        fields.push(Field::new(name, arrow_type_for(se), nullable));
    }
    Ok(Arc::new(Schema::new(fields)))
}

/// Parse a min/max stat byte slice into a typed `ScalarValue` for the
/// numeric Arrow types the provider exposes. Returns `None` for types
/// we don't summarise (strings, binary) — matching
/// `fast_parquet::aggregate_column_statistics`, which only carries
/// numeric min/max.
fn scalar_from_le_bytes(ty: &DataType, b: &[u8]) -> Option<ScalarValue> {
    match ty {
        DataType::Int32 => b
            .get(..4)
            .map(|s| ScalarValue::Int32(Some(i32::from_le_bytes(s.try_into().unwrap())))),
        DataType::Date32 => b
            .get(..4)
            .map(|s| ScalarValue::Date32(Some(i32::from_le_bytes(s.try_into().unwrap())))),
        DataType::Int64 => b
            .get(..8)
            .map(|s| ScalarValue::Int64(Some(i64::from_le_bytes(s.try_into().unwrap())))),
        DataType::Float64 => b
            .get(..8)
            .map(|s| ScalarValue::Float64(Some(f64::from_le_bytes(s.try_into().unwrap())))),
        DataType::Float32 => b
            .get(..4)
            .map(|s| ScalarValue::Float32(Some(f32::from_le_bytes(s.try_into().unwrap())))),
        _ => None,
    }
}

/// Σ.Q06.SF10.5.c — load every metadata-derived field
/// `EmatixFastParquetTableProvider::try_new` needs in one pass, via
/// ematix-parquet, replacing parquet-rs's `ArrowReaderMetadata::load`
/// + `SerializedFileReader`. Returns the RAW schema (Utf8, not
/// Utf8View-promoted) so the caller can apply its Σ.E5 promotion +
/// type validation unchanged.
///
/// Stats semantics mirror `fast_parquet::aggregate_column_statistics`:
/// per leaf column, fold row-group stats into one `ColumnStatistics`
/// — null_count summed (Exact iff every RG had it), min = min across
/// RGs, max = max across RGs, numeric types only. distinct_count left
/// Absent (the optional dict-page walk enriches it separately).
pub fn load_provider_metadata<P: AsRef<Path>>(
    path: P,
) -> Result<EmatProviderMetadata, EmatMetadataError> {
    let pf = ParquetFile::open(path.as_ref()).map_err(|e| EmatMetadataError::Io(e.to_string()))?;
    let meta = pf
        .metadata()
        .map_err(|e| EmatMetadataError::Metadata(e.to_string()))?;

    let schema = arrow_schema_from_metadata(&meta)?;
    let num_cols = schema.fields().len();
    let num_rows = meta.num_rows.max(0) as usize;
    let num_row_groups = meta.row_groups.len();
    let rg_num_rows: Vec<usize> = meta
        .row_groups
        .iter()
        .map(|rg| rg.num_rows.max(0) as usize)
        .collect();

    // Per-column aggregation across row groups.
    let mut null_count: Vec<Option<i64>> = vec![Some(0); num_cols];
    let mut min_sv: Vec<Option<ScalarValue>> = vec![None; num_cols];
    let mut max_sv: Vec<Option<ScalarValue>> = vec![None; num_cols];
    let mut no_nulls: Vec<bool> = vec![true; num_cols];
    // is_dict: every data page in every RG must be dict-encoded.
    let mut all_dict: Vec<bool> = vec![true; num_cols];

    for rg in &meta.row_groups {
        let cols = rg.columns.len().min(num_cols);
        for col_idx in 0..num_cols {
            let arrow_ty = schema.field(col_idx).data_type().clone();
            if col_idx >= cols {
                null_count[col_idx] = None;
                no_nulls[col_idx] = false;
                all_dict[col_idx] = false;
                continue;
            }
            let Some(cm) = rg.columns[col_idx].meta_data.as_ref() else {
                null_count[col_idx] = None;
                no_nulls[col_idx] = false;
                all_dict[col_idx] = false;
                continue;
            };

            // --- null_count + no_nulls + min/max from Statistics ---
            match cm.statistics.as_ref() {
                Some(stats) => {
                    match (stats.null_count, null_count[col_idx]) {
                        (Some(nc), Some(curr)) => {
                            null_count[col_idx] = Some(curr.saturating_add(nc.max(0)))
                        }
                        _ => null_count[col_idx] = None,
                    }
                    if stats.null_count != Some(0) {
                        no_nulls[col_idx] = false;
                    }
                    // Prefer the current `*_value` fields, fall back to
                    // the deprecated `min`/`max`.
                    let min_b = stats.min_value.or(stats.min);
                    let max_b = stats.max_value.or(stats.max);
                    if let Some(b) = min_b {
                        if let Some(v) = scalar_from_le_bytes(&arrow_ty, b) {
                            match &min_sv[col_idx] {
                                Some(curr) if !(v < *curr) => {}
                                _ => min_sv[col_idx] = Some(v),
                            }
                        }
                    }
                    if let Some(b) = max_b {
                        if let Some(v) = scalar_from_le_bytes(&arrow_ty, b) {
                            match &max_sv[col_idx] {
                                Some(curr) if !(v > *curr) => {}
                                _ => max_sv[col_idx] = Some(v),
                            }
                        }
                    }
                }
                None => {
                    null_count[col_idx] = None;
                    no_nulls[col_idx] = false;
                }
            }

            // --- is_dict_encoded: needs a dict page + all data pages dict ---
            if all_dict[col_idx] {
                if cm.dictionary_page_offset.is_none() {
                    all_dict[col_idx] = false;
                } else {
                    match cm.encoding_stats.as_ref() {
                        Some(es) => {
                            let ok = es.iter().all(|s| {
                                !matches!(
                                    s.page_type,
                                    ematix_parquet_format::types::PageType::DataPage
                                        | ematix_parquet_format::types::PageType::DataPageV2
                                ) || matches!(
                                    s.encoding,
                                    Encoding::RleDictionary | Encoding::PlainDictionary
                                )
                            });
                            if !ok {
                                all_dict[col_idx] = false;
                            }
                        }
                        None => all_dict[col_idx] = false,
                    }
                }
            }
        }
    }

    let column_stats: Vec<ColumnStatistics> = (0..num_cols)
        .map(|i| ColumnStatistics {
            null_count: match null_count[i] {
                Some(n) => Precision::Exact(n.max(0) as usize),
                None => Precision::Absent,
            },
            max_value: max_sv[i].clone().map(Precision::Exact).unwrap_or(Precision::Absent),
            min_value: min_sv[i].clone().map(Precision::Exact).unwrap_or(Precision::Absent),
            sum_value: Precision::Absent,
            distinct_count: Precision::Absent,
            byte_size: Precision::Absent,
        })
        .collect();

    Ok(EmatProviderMetadata {
        schema,
        num_rows,
        num_row_groups,
        rg_num_rows,
        column_stats,
        column_is_dict_encoded: all_dict,
        column_has_no_nulls: no_nulls,
    })
}

/// For each leaf column in `meta.row_groups[].columns[]`, return the
/// MAX of `DictionaryPageHeader.num_values` across all row groups
/// where the column has a dictionary page. Returns `None` for
/// columns that don't have a dict page in every row group (so the
/// caller leaves the distinct_count as Absent).
///
/// **Replaces** the parquet-rs `SerializedPageReader::get_next_page()`
/// → `SnappyCodec::decompress` path inside try_new. Reads only the
/// uncompressed Thrift page header at each `dictionary_page_offset`.
///
/// The page header is variable-length but the Thrift compact protocol
/// keeps PageHeader well under 256 bytes for the cases we care about.
/// We read a 256-byte window and ignore any leftover after the
/// header. The page payload that follows is not touched at all.
///
/// `num_leaf_cols` must equal `arrow_schema.fields().len()` so the
/// returned Vec aligns with the caller's column indexing.
pub fn dict_distinct_max_per_column<P: AsRef<Path>>(
    path: P,
    num_leaf_cols: usize,
) -> Result<Vec<Option<usize>>, EmatMetadataError> {
    const PAGE_HEADER_WINDOW: u64 = 256;

    let pf = ParquetFile::open(path.as_ref()).map_err(|e| EmatMetadataError::Io(e.to_string()))?;
    let meta = pf
        .metadata()
        .map_err(|e| EmatMetadataError::Metadata(e.to_string()))?;

    let mut max_per_col: Vec<Option<usize>> = vec![None; num_leaf_cols];
    // A column is "always dict" iff every RG has dictionary_page_offset.is_some().
    // We track `seen_in_every_rg[col_idx]`: starts true, set false if any RG
    // lacks the dict.
    let mut seen_in_every_rg: Vec<bool> = vec![true; num_leaf_cols];

    let num_rgs = meta.row_groups.len();
    if num_rgs == 0 {
        return Ok(max_per_col);
    }

    // Reusable buffer for page header reads.
    let mut buf: Vec<u8> = Vec::with_capacity(PAGE_HEADER_WINDOW as usize);

    for rg in &meta.row_groups {
        let n_cols = rg.columns.len().min(num_leaf_cols);
        for col_idx in 0..n_cols {
            if !seen_in_every_rg[col_idx] {
                continue;
            }
            let Some(dict_offset) = dict_offset_for(&rg.columns[col_idx]) else {
                seen_in_every_rg[col_idx] = false;
                max_per_col[col_idx] = None;
                continue;
            };
            buf.clear();
            pf.read_range_into(&mut buf, dict_offset, PAGE_HEADER_WINDOW)
                .map_err(|e| EmatMetadataError::Io(e.to_string()))?;
            let mut cur = Cursor::new(&buf);
            let hdr = read_page_header(&mut cur)
                .map_err(|e| EmatMetadataError::Metadata(e.to_string()))?;
            if let Some(dph) = hdr.dictionary_page_header {
                let n = dph.num_values.max(0) as usize;
                let prev = max_per_col[col_idx].unwrap_or(0);
                max_per_col[col_idx] = Some(prev.max(n));
            } else {
                // Header at that offset wasn't a dict page — invalidate.
                seen_in_every_rg[col_idx] = false;
                max_per_col[col_idx] = None;
            }
        }
        // Columns past this RG's count never have a dict either.
        if num_leaf_cols > n_cols {
            for col_idx in n_cols..num_leaf_cols {
                seen_in_every_rg[col_idx] = false;
                max_per_col[col_idx] = None;
            }
        }
    }

    // Drop columns that weren't seen in every RG — keep only those with
    // a consistent dict-page-per-RG signal. Matches the upstream
    // Σ.AH.2 Story 1'.2 semantics.
    for col_idx in 0..num_leaf_cols {
        if !seen_in_every_rg[col_idx] {
            max_per_col[col_idx] = None;
        }
    }

    Ok(max_per_col)
}

/// Σ.Q06.SF10.5.a — read just `FileMetaData` once and pass it
/// (alongside the original `ParquetFile`) so the caller can reuse the
/// same parsed footer for multiple walks. This avoids re-parsing on
/// successive calls when we extend this module to cover more of
/// `try_new`'s work.
pub struct EmatParquetMetadata {
    pf: ParquetFile,
}

impl EmatParquetMetadata {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, EmatMetadataError> {
        let pf =
            ParquetFile::open(path.as_ref()).map_err(|e| EmatMetadataError::Io(e.to_string()))?;
        Ok(Self { pf })
    }

    /// Borrow the file's parsed `FileMetaData`.
    pub fn file_metadata(&self) -> Result<FileMetaData<'_>, EmatMetadataError> {
        self.pf
            .metadata()
            .map_err(|e| EmatMetadataError::Metadata(e.to_string()))
    }

    /// Convenience: same as the free function `dict_distinct_max_per_column`
    /// but reuses the already-open file.
    pub fn dict_distinct_max_per_column(
        &self,
        num_leaf_cols: usize,
    ) -> Result<Vec<Option<usize>>, EmatMetadataError> {
        const PAGE_HEADER_WINDOW: u64 = 256;
        let meta = self.file_metadata()?;
        let mut max_per_col: Vec<Option<usize>> = vec![None; num_leaf_cols];
        let mut seen_in_every_rg: Vec<bool> = vec![true; num_leaf_cols];
        if meta.row_groups.is_empty() {
            return Ok(max_per_col);
        }
        let mut buf: Vec<u8> = Vec::with_capacity(PAGE_HEADER_WINDOW as usize);

        for rg in &meta.row_groups {
            let n_cols = rg.columns.len().min(num_leaf_cols);
            for col_idx in 0..n_cols {
                if !seen_in_every_rg[col_idx] {
                    continue;
                }
                let Some(dict_offset) = dict_offset_for(&rg.columns[col_idx]) else {
                    seen_in_every_rg[col_idx] = false;
                    max_per_col[col_idx] = None;
                    continue;
                };
                buf.clear();
                self.pf
                    .read_range_into(&mut buf, dict_offset, PAGE_HEADER_WINDOW)
                    .map_err(|e| EmatMetadataError::Io(e.to_string()))?;
                let mut cur = Cursor::new(&buf);
                let hdr = read_page_header(&mut cur)
                    .map_err(|e| EmatMetadataError::Metadata(e.to_string()))?;
                if let Some(dph) = hdr.dictionary_page_header {
                    let n = dph.num_values.max(0) as usize;
                    let prev = max_per_col[col_idx].unwrap_or(0);
                    max_per_col[col_idx] = Some(prev.max(n));
                } else {
                    seen_in_every_rg[col_idx] = false;
                    max_per_col[col_idx] = None;
                }
            }
            if num_leaf_cols > n_cols {
                for col_idx in n_cols..num_leaf_cols {
                    seen_in_every_rg[col_idx] = false;
                    max_per_col[col_idx] = None;
                }
            }
        }

        for col_idx in 0..num_leaf_cols {
            if !seen_in_every_rg[col_idx] {
                max_per_col[col_idx] = None;
            }
        }
        Ok(max_per_col)
    }

    /// Borrow the underlying `ParquetFile` for ad-hoc reads.
    pub fn parquet_file(&self) -> &ParquetFile {
        &self.pf
    }
}

/// Inspect a ColumnChunk's metadata for a dictionary page offset.
/// The encoding-stats walk done elsewhere in try_new still uses
/// parquet-rs's `page_encoding_stats()`; here we just need the
/// offset so we can read the dict page's header.
fn dict_offset_for(col: &ColumnChunk<'_>) -> Option<u64> {
    let meta = col.meta_data.as_ref()?;
    let off = meta.dictionary_page_offset?;
    if off <= 0 {
        return None;
    }
    Some(off as u64)
}

/// Helper to expose the number of row groups quickly without
/// importing FileMetaData at the caller.
pub fn num_row_groups<P: AsRef<Path>>(path: P) -> Result<usize, EmatMetadataError> {
    let m = EmatParquetMetadata::open(path)?;
    let fm = m.file_metadata()?;
    Ok(fm.row_groups.len())
}

/// Same as above for `num_rows`.
pub fn num_rows<P: AsRef<Path>>(path: P) -> Result<i64, EmatMetadataError> {
    let m = EmatParquetMetadata::open(path)?;
    let fm = m.file_metadata()?;
    Ok(fm.num_rows)
}

#[derive(Debug, thiserror::Error)]
pub enum EmatMetadataError {
    #[error("ematix-parquet io: {0}")]
    Io(String),
    #[error("ematix-parquet metadata: {0}")]
    Metadata(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Smoke test: missing file path returns an Io error without
    /// panicking. Full TPC-H lineitem coverage is exercised by the
    /// bench A/B (Q06 SF=10 strict A/B + 22q SF=10 regression check).
    #[test]
    fn missing_file_returns_io_error() {
        let res = dict_distinct_max_per_column("/tmp/__nonexistent_q06_path__.parquet", 4);
        match res {
            Err(EmatMetadataError::Io(_)) => {}
            other => panic!("expected Io error, got {other:?}"),
        }
    }

    // Σ.Q06.SF10.5.a integration semantics (PageHeader window size,
    // dict-page detection across all RGs of a column, max aggregation)
    // are exercised against TPC-H lineitem via the Q06 / 22q strict A/B
    // bench gate. A future Σ.Q06.SF10.5.b will add a synthetic
    // fixture using `ematix_parquet_codec::write`.
}
