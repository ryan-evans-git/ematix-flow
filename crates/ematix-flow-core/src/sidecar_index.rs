//! Read-side sidecar index lookups — Phase 1 of `feat/sidecar-indexes`.
//!
//! A sidecar (`<source>.parquet.idx`, produced by ematix-parquet's
//! [`IndexBuilder`](ematix_parquet_codec::index::IndexBuilder)) lets a selective
//! equality/range predicate skip-decompress whole row groups and masked-decode
//! only the matching rows — Postgres-style indexing over a Parquet file that was
//! never sorted on that column, without rewriting the file.
//!
//! This module is the flow-side entry point for the *read* path. It follows the
//! opt-in principle from `ematix-parquet/docs/ematix-flow-integration.md`: a
//! sidecar only ever adds capability. Every function here returns `Ok(None)`
//! (never an error) when there is no current sidecar covering the query, so the
//! caller keeps its existing full-scan path unchanged. A stale sidecar (the
//! source Parquet was rewritten) is treated as "no sidecar" and reported via
//! `Ok(None)` as well — it is recoverable, not fatal.
//!
//! Scope of this first cut: sorted-`INT64` equality lookup returning one target
//! column. Range, other physical types, multi-column projection, and the
//! `EmatixFastParquetTableProvider::scan` wiring build on this primitive in
//! subsequent commits (see `docs/SIDECAR_INDEXES_PLAN.md`).

use std::path::{Path, PathBuf};

use datafusion::error::{DataFusionError, Result as DfResult};

/// Conventional sidecar location for a source Parquet file:
/// `data.parquet` → `data.parquet.idx`.
///
/// `Path::with_extension` replaces the final component's extension, so for a
/// stem of `data` and current extension `parquet` this yields
/// `data.parquet.idx` — the path convention the builder writes to by default.
pub fn sidecar_path(source: &Path) -> PathBuf {
    source.with_extension("parquet.idx")
}

/// A sidecar open failed because the source was rewritten after the sidecar was
/// built. Recoverable — the caller should fall back to a full scan and, ideally,
/// schedule an offline rebuild. The codec surfaces this as
/// `InvalidInput("sidecar fingerprint mismatch: source file changed ...")`; we
/// match on the stable `"fingerprint mismatch"` substring of the error's debug
/// form so we do not depend on the codec's error enum being publicly
/// destructurable.
fn is_stale(err: &impl std::fmt::Debug) -> bool {
    format!("{err:?}").contains("fingerprint mismatch")
}

/// Answer `WHERE <indexed_col> = key` from a sorted-`INT64` sidecar index,
/// returning the matching values of `target_column` (a source-Parquet column
/// ordinal).
///
/// Returns:
/// - `Ok(Some(values))` when a current sidecar carries an index named
///   `index_name` — the masked-decoded `target_column` values for matching rows;
/// - `Ok(None)` when there is no sidecar, the sidecar is stale, or it has no
///   index by that name — the caller full-scans;
/// - `Err(_)` only on an unexpected I/O or codec failure.
pub fn indexed_i64_eq(
    source_path: &Path,
    index_name: &str,
    key: i64,
    target_column: usize,
) -> DfResult<Option<Vec<i64>>> {
    let idx_path = sidecar_path(source_path);
    if !idx_path.exists() {
        return Ok(None);
    }
    let source = ematix_parquet_io::ParquetFile::open(source_path).map_err(|e| {
        DataFusionError::Execution(format!("sidecar: open source {source_path:?}: {e:?}"))
    })?;
    let idx = match ematix_parquet_codec::index::ParquetIndex::open(&idx_path, &source) {
        Ok(idx) => idx,
        Err(e) if is_stale(&e) => return Ok(None),
        Err(e) => {
            return Err(DataFusionError::Execution(format!(
                "sidecar: open index {idx_path:?}: {e:?}"
            )));
        }
    };
    // The sidecar exists but does not carry the index we need — not covered.
    let covered = idx
        .manifest()
        .indexes
        .iter()
        .any(|entry| entry.name == index_name);
    if !covered {
        return Ok(None);
    }
    let values = idx
        .read_column_i64_where_eq(&source, index_name, key, target_column)
        .map_err(|e| {
            DataFusionError::Execution(format!("sidecar: eq lookup {index_name}: {e:?}"))
        })?;
    Ok(Some(values))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ematix_parquet_codec::index::IndexBuilder;
    use ematix_parquet_codec::write::{ColumnData, write_table_to_path};
    use ematix_parquet_format::types::CompressionCodec;

    /// Write a two-column `(key: i64, val: i64)` Parquet file and return its
    /// path inside `dir`.
    fn write_kv_fixture(dir: &Path, keys: &[i64], vals: &[i64]) -> PathBuf {
        let path = dir.join("data.parquet");
        write_table_to_path(
            &path,
            &[
                ("key", ColumnData::I64(keys)),
                ("val", ColumnData::I64(vals)),
            ],
            CompressionCodec::Uncompressed,
        )
        .expect("write fixture parquet");
        path
    }

    /// Full-scan oracle: values of `val` where `key == k`, order-independent.
    fn full_scan_eq(keys: &[i64], vals: &[i64], k: i64) -> Vec<i64> {
        let mut out: Vec<i64> = keys
            .iter()
            .zip(vals)
            .filter(|(key, _)| **key == k)
            .map(|(_, v)| *v)
            .collect();
        out.sort_unstable();
        out
    }

    #[test]
    fn eq_lookup_matches_full_scan() {
        let dir = tempfile::tempdir().unwrap();
        // 100 keys, each appearing a few times, values distinct.
        let keys: Vec<i64> = (0..600).map(|i| i % 100).collect();
        let vals: Vec<i64> = (0..600).collect();
        let src = write_kv_fixture(dir.path(), &keys, &vals);

        let source = ematix_parquet_io::ParquetFile::open(&src).unwrap();
        IndexBuilder::new(&source)
            .write_sorted_i64(sidecar_path(&src), "idx_key", 0)
            .expect("build sidecar");

        for k in [0_i64, 42, 99] {
            let mut got = indexed_i64_eq(&src, "idx_key", k, 1)
                .unwrap()
                .expect("sidecar present and covers idx_key");
            got.sort_unstable();
            assert_eq!(got, full_scan_eq(&keys, &vals, k), "key={k}");
        }
    }

    #[test]
    fn no_sidecar_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let src = write_kv_fixture(dir.path(), &[1, 2, 3], &[10, 20, 30]);
        // No sidecar built.
        assert!(indexed_i64_eq(&src, "idx_key", 2, 1).unwrap().is_none());
    }

    #[test]
    fn unknown_index_name_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let src = write_kv_fixture(dir.path(), &[1, 2, 3], &[10, 20, 30]);
        let source = ematix_parquet_io::ParquetFile::open(&src).unwrap();
        IndexBuilder::new(&source)
            .write_sorted_i64(sidecar_path(&src), "idx_key", 0)
            .unwrap();
        // Sidecar exists but has no "idx_other" — fall back, not an error.
        assert!(indexed_i64_eq(&src, "idx_other", 2, 1).unwrap().is_none());
    }

    #[test]
    fn stale_sidecar_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let src = write_kv_fixture(dir.path(), &[1, 2, 3], &[10, 20, 30]);
        let source = ematix_parquet_io::ParquetFile::open(&src).unwrap();
        IndexBuilder::new(&source)
            .write_sorted_i64(sidecar_path(&src), "idx_key", 0)
            .unwrap();
        // Rewrite the source with different data — the sidecar's footer
        // fingerprint no longer matches, so open must report staleness and we
        // recover to a full scan (None), never an error.
        write_kv_fixture(dir.path(), &[7, 8, 9, 10], &[70, 80, 90, 100]);
        assert!(indexed_i64_eq(&src, "idx_key", 2, 1).unwrap().is_none());
    }
}
