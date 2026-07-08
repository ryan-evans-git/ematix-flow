//! Write-side sidecar index creation — Phase 2 of `feat/sidecar-indexes`.
//!
//! Backfill a `<source>.parquet.idx` sidecar onto an existing Parquet file
//! **without rewriting the source** — the "index without rewriting the file"
//! story. Wraps `ematix_parquet_codec::index::IndexBuilder`, resolving a column
//! by name or ordinal and dispatching to the right physical-type writer. The
//! `flow index build` CLI is a thin shell over [`build_sorted_sidecar`].
//!
//! Scope of this first cut: a **sorted** index (eq + range) on an `INT32`,
//! `INT64`, or `BYTE_ARRAY` column. Bloom / composite / inverted writers and
//! multi-index sidecars follow (see `docs/SIDECAR_INDEXES_PLAN.md`). One index
//! per sidecar file today, matching the codec's current builder.

use std::path::{Path, PathBuf};

use datafusion::error::{DataFusionError, Result as DfResult};
use ematix_parquet_codec::index::IndexBuilder;
use ematix_parquet_format::types::ParquetType;
use ematix_parquet_io::ParquetFile;

use crate::sidecar_index::sidecar_path;

/// Build a **sorted** sidecar index named `index_name` on `column` of the
/// Parquet file at `source_path`, writing the sidecar to `out_path` (or the
/// conventional `<source>.parquet.idx` when `None`). Returns the path written.
///
/// `column` is either a leaf-column **name** (e.g. `"l_orderkey"`) or a numeric
/// **ordinal** (e.g. `"0"`). The physical type is read from the source schema
/// and dispatched:
/// - `INT64`  → [`IndexBuilder::write_sorted_i64`]
/// - `INT32`  → [`IndexBuilder::write_sorted_i32`]
/// - `BYTE_ARRAY` → [`IndexBuilder::write_sorted_byte_array`]
///
/// Other physical types return [`DataFusionError::NotImplemented`].
pub fn build_sorted_sidecar(
    source_path: &Path,
    index_name: &str,
    column: &str,
    out_path: Option<PathBuf>,
) -> DfResult<PathBuf> {
    let source = ParquetFile::open(source_path).map_err(|e| {
        DataFusionError::Execution(format!("index build: open {source_path:?}: {e:?}"))
    })?;
    let (ordinal, ptype) = resolve_leaf(&source, column)?;
    let out = out_path.unwrap_or_else(|| sidecar_path(source_path));

    let builder = IndexBuilder::new(&source);
    let written = match ptype {
        ParquetType::Int64 => builder.write_sorted_i64(&out, index_name, ordinal),
        ParquetType::Int32 => builder.write_sorted_i32(&out, index_name, ordinal),
        ParquetType::ByteArray => builder.write_sorted_byte_array(&out, index_name, ordinal),
        other => {
            return Err(DataFusionError::NotImplemented(format!(
                "sorted sidecar on physical type {other:?} (column {column:?}); \
                 supported: INT32, INT64, BYTE_ARRAY"
            )));
        }
    };
    written.map_err(|e| {
        DataFusionError::Execution(format!("index build: write index {index_name}: {e:?}"))
    })?;
    Ok(out)
}

/// Resolve a column reference (numeric ordinal or leaf name) to a
/// `(leaf_ordinal, physical_type)` pair against the source's flat schema.
///
/// Matches [`IndexBuilder`]'s flat-schema convention: leaf ordinal `n` lives at
/// `schema[n + 1]` (`schema[0]` is the root group node).
fn resolve_leaf(source: &ParquetFile, column: &str) -> DfResult<(usize, ParquetType)> {
    let md = source
        .metadata()
        .map_err(|e| DataFusionError::Execution(format!("index build: read metadata: {e:?}")))?;

    // Numeric → treat as an ordinal directly.
    if let Ok(ordinal) = column.parse::<usize>() {
        let ptype = md
            .schema
            .get(ordinal + 1)
            .and_then(|s| s.column_type)
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "index build: column ordinal {ordinal} is out of range or not a leaf"
                ))
            })?;
        return Ok((ordinal, ptype));
    }

    // Name → scan leaves (schema[1..]); ordinal is position among them.
    for (schema_idx, el) in md.schema.iter().enumerate().skip(1) {
        if let Some(ptype) = el.column_type {
            if el.name == column.as_bytes() {
                return Ok((schema_idx - 1, ptype));
            }
        }
    }

    let leaf_names: Vec<&str> = md
        .schema
        .iter()
        .skip(1)
        .filter_map(|s| std::str::from_utf8(s.name).ok())
        .collect();
    Err(DataFusionError::Execution(format!(
        "index build: no leaf column named {column:?}; available leaves: {leaf_names:?}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sidecar_index::indexed_i64_eq;
    use ematix_parquet_codec::write::{ColumnData, write_table_to_path};
    use ematix_parquet_format::types::CompressionCodec;

    /// Write a two-column `(key: i64, val: i64)` Parquet fixture.
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

    /// Building by column NAME produces a sidecar the read side can use, and the
    /// indexed result equals the full scan — the write→read oracle.
    #[test]
    fn build_by_name_then_indexed_read_matches_full_scan() {
        let dir = tempfile::tempdir().unwrap();
        let keys: Vec<i64> = (0..600).map(|i| i % 100).collect();
        let vals: Vec<i64> = (0..600).collect();
        let src = write_kv_fixture(dir.path(), &keys, &vals);

        let out = build_sorted_sidecar(&src, "idx_key", "key", None).unwrap();
        assert_eq!(out, sidecar_path(&src), "defaults to <source>.parquet.idx");

        for k in [0_i64, 42, 99] {
            let mut got = indexed_i64_eq(&src, "idx_key", k, 1).unwrap().unwrap();
            got.sort_unstable();
            let mut want: Vec<i64> = keys
                .iter()
                .zip(&vals)
                .filter(|(kk, _)| **kk == k)
                .map(|(_, v)| *v)
                .collect();
            want.sort_unstable();
            assert_eq!(got, want, "key={k}");
        }
    }

    /// Building by numeric ORDINAL resolves the same leaf as the name.
    #[test]
    fn build_by_ordinal_resolves_same_leaf() {
        let dir = tempfile::tempdir().unwrap();
        let src = write_kv_fixture(dir.path(), &[5, 5, 9], &[10, 20, 30]);
        build_sorted_sidecar(&src, "idx_key", "0", None).unwrap();
        let got = indexed_i64_eq(&src, "idx_key", 5, 1).unwrap().unwrap();
        assert_eq!(got.len(), 2, "key 5 appears twice");
    }

    /// An unknown column name is a clear error, not a panic.
    #[test]
    fn unknown_column_name_errors() {
        let dir = tempfile::tempdir().unwrap();
        let src = write_kv_fixture(dir.path(), &[1], &[2]);
        let err = build_sorted_sidecar(&src, "idx_key", "nope", None).unwrap_err();
        assert!(
            format!("{err}").contains("no leaf column named"),
            "unexpected error: {err}"
        );
    }

    /// An explicit out-path is honoured.
    #[test]
    fn honours_explicit_out_path() {
        let dir = tempfile::tempdir().unwrap();
        let src = write_kv_fixture(dir.path(), &[1, 2], &[3, 4]);
        let custom = dir.path().join("custom.idx");
        let out = build_sorted_sidecar(&src, "idx_key", "key", Some(custom.clone())).unwrap();
        assert_eq!(out, custom);
        assert!(custom.exists(), "sidecar written at the explicit path");
    }
}
