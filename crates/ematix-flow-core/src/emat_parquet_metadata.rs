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

use ematix_parquet_format::compact::Cursor;
use ematix_parquet_format::metadata::{read_page_header, ColumnChunk, FileMetaData};
use ematix_parquet_io::ParquetFile;

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
