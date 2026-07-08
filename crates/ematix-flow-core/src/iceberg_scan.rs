//! Iceberg manifest-level file pruning for the mesh coordinator — Phase I.3 of
//! `feat/sidecar-indexes`.
//!
//! The distributed scan's file-enumeration step becomes a *manifest prune*: the
//! coordinator walks the current snapshot's data files, drops the ones whose
//! stamped `(min_key, max_key)` summary provably cannot match the predicate
//! (`O(1)` per file, no I/O), and fans out only the survivors. Combined with the
//! per-file sidecar (`crate::sidecar_index`), this is the SF1000 pruning stack:
//!
//! ```text
//! manifest prune  → drop files by summary     [O(1)/file, no I/O]
//! sidecar prune   → skip row groups per file  [crate::sidecar_index]
//! fan-out         → mesh tasks over survivors  [strictly less work]
//! ```
//!
//! Gated behind `--features iceberg` (off by default) so the default build and
//! the PyPI wheel never pull iceberg-rust.
//!
//! ## Conservative-prune correctness
//!
//! [`prune_data_files_eq`] / [`prune_data_files_range`] are *conservative*: a
//! file survives whenever it *could* match, and a file with **no** ematix
//! extension is kept unconditionally. So among the survivors there are two
//! kinds, and the split matters:
//!
//! - the file's sidecar covers the queried index → the worker does an index
//!   lookup + masked decode ([`ScanTarget::Indexed`]);
//! - the file has no sidecar for this index → the worker **must full-scan** it
//!   ([`ScanTarget::FullScan`]). Dropping it (as [`pair_with_extensions`] would)
//!   silently loses matching rows, because the prune is conservative, not exact.
//!
//! [`pair_with_extensions`]: ematix_iceberg::iceberg_rs::pair_with_extensions

use datafusion::error::{DataFusionError, Result as DfResult};
use ematix_iceberg::iceberg_rs::{
    extract_extension, prune_data_files_eq, prune_data_files_range, resolve_sidecar_uri,
};
use ematix_parquet_codec::index::Key;
use iceberg::spec::DataFile;

/// One data file the coordinator will fan out, tagged with how the worker
/// should read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanTarget {
    /// Survived the manifest prune *and* the file's sidecar covers the queried
    /// index. The worker opens the sidecar at `sidecar_uri` and does an index
    /// lookup + masked decode against `data_uri`.
    Indexed {
        data_uri: String,
        sidecar_uri: String,
    },
    /// Survived the prune but has no sidecar covering this index. The worker
    /// **must** full-scan `data_uri`; the manifest prune is conservative, so
    /// this file may still hold matching rows.
    FullScan { data_uri: String },
}

impl ScanTarget {
    /// The source Parquet URI, regardless of read strategy.
    pub fn data_uri(&self) -> &str {
        match self {
            ScanTarget::Indexed { data_uri, .. } | ScanTarget::FullScan { data_uri } => data_uri,
        }
    }
}

/// The coordinator's fan-out plan for one indexed predicate over an Iceberg
/// table's current-snapshot data files: the survivors, each tagged
/// index-lookup vs full-scan. Files pruned away by the manifest summary do not
/// appear.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IcebergScanPlan {
    pub targets: Vec<ScanTarget>,
}

impl IcebergScanPlan {
    /// Total files to fan out (indexed + full-scan).
    pub fn len(&self) -> usize {
        self.targets.len()
    }

    /// No file survived the prune — the mesh has nothing to scan.
    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }

    /// Files that will be read through their sidecar index.
    pub fn indexed_count(&self) -> usize {
        self.targets
            .iter()
            .filter(|t| matches!(t, ScanTarget::Indexed { .. }))
            .count()
    }

    /// Survivors with no covering sidecar — full-scanned.
    pub fn full_scan_count(&self) -> usize {
        self.targets
            .iter()
            .filter(|t| matches!(t, ScanTarget::FullScan { .. }))
            .count()
    }
}

/// Tag each conservatively-pruned survivor as index-lookup or full-scan,
/// resolving the sidecar URI for the indexed ones.
fn split_survivors(survivors: &[&DataFile], index_name: &str) -> DfResult<IcebergScanPlan> {
    let mut targets = Vec::with_capacity(survivors.len());
    for df in survivors {
        let data_uri = df.file_path().to_string();
        // A survivor is index-readable only if it carries an extension *and*
        // that extension has a summary for the queried index. Otherwise it must
        // be full-scanned — never dropped.
        let ext = extract_extension(df)
            .map_err(|e| DataFusionError::Execution(format!("iceberg: decode extension: {e:?}")))?;
        let indexed = match ext {
            Some(ext) if ext.summary(index_name).is_some() => Some(ext),
            _ => None,
        };
        match indexed {
            Some(ext) => {
                let sidecar_uri = resolve_sidecar_uri(df.file_path(), &ext.sidecar_relative_path);
                targets.push(ScanTarget::Indexed {
                    data_uri,
                    sidecar_uri,
                });
            }
            None => targets.push(ScanTarget::FullScan { data_uri }),
        }
    }
    Ok(IcebergScanPlan { targets })
}

/// Plan a fan-out for `WHERE <indexed_col> = key` over `files` (typically the
/// output of `ematix_iceberg::iceberg_rs::collect_data_files`).
pub fn plan_scan_eq(
    files: &[DataFile],
    index_name: &str,
    key: &Key<'_>,
) -> DfResult<IcebergScanPlan> {
    let survivors = prune_data_files_eq(files, index_name, key).map_err(|e| {
        DataFusionError::Execution(format!("iceberg: eq prune {index_name}: {e:?}"))
    })?;
    split_survivors(&survivors, index_name)
}

/// Plan a fan-out for `WHERE <indexed_col> BETWEEN low AND high` (either bound
/// open) over `files`.
pub fn plan_scan_range(
    files: &[DataFile],
    index_name: &str,
    low: Option<&Key<'_>>,
    high: Option<&Key<'_>>,
) -> DfResult<IcebergScanPlan> {
    let survivors = prune_data_files_range(files, index_name, low, high).map_err(|e| {
        DataFusionError::Execution(format!("iceberg: range prune {index_name}: {e:?}"))
    })?;
    split_survivors(&survivors, index_name)
}

/// [`plan_scan_eq`] for an `INT64` key — the shape the distributed
/// coordinator lowers from (`IcebergPredicate` is i64-only on the v1 wire, so
/// keeping the `Key` type out of the caller keeps ematix-parquet-codec out of
/// the distributed crate's API surface).
pub fn plan_scan_eq_i64(
    files: &[DataFile],
    index_name: &str,
    key: i64,
) -> DfResult<IcebergScanPlan> {
    plan_scan_eq(files, index_name, &Key::I64(key))
}

/// [`plan_scan_range`] for `INT64` bounds (either open). See
/// [`plan_scan_eq_i64`] for why this exists.
pub fn plan_scan_range_i64(
    files: &[DataFile],
    index_name: &str,
    low: Option<i64>,
    high: Option<i64>,
) -> DfResult<IcebergScanPlan> {
    plan_scan_range(
        files,
        index_name,
        low.map(Key::I64).as_ref(),
        high.map(Key::I64).as_ref(),
    )
}

/// Plan a fan-out with **no prunable predicate**: every file survives, and
/// every file is a [`ScanTarget::FullScan`] — with nothing to look up, a
/// covering sidecar buys nothing, so none is resolved.
pub fn plan_scan_all(files: &[DataFile]) -> IcebergScanPlan {
    IcebergScanPlan {
        targets: files
            .iter()
            .map(|df| ScanTarget::FullScan {
                data_uri: df.file_path().to_string(),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ematix_iceberg::iceberg_rs::encode_key_metadata;
    use ematix_iceberg::{EmatixDataFileExtension, IndexSummary, SummaryKey};
    use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};

    const IDX: &str = "idx_key";

    /// Build a standalone `DataFile` (no catalog) at `uri`, optionally carrying
    /// an ematix extension.
    fn data_file(uri: &str, ext: Option<&EmatixDataFileExtension>) -> DataFile {
        let mut b = DataFileBuilder::default();
        b.content(DataContentType::Data)
            .file_path(uri.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition_spec_id(0)
            .partition(Struct::empty());
        if let Some(e) = ext {
            b.key_metadata(Some(encode_key_metadata(e)));
        }
        b.build().expect("data file builder")
    }

    /// An extension whose `idx_key` summary covers the closed range `[min,max]`
    /// and points at the conventional sidecar name.
    fn ext_covering(min: i64, max: i64) -> EmatixDataFileExtension {
        EmatixDataFileExtension {
            sidecar_relative_path: "data.parquet.idx".into(),
            summaries: vec![
                IndexSummary::new(IDX).with_range(SummaryKey::I64(min), SummaryKey::I64(max)),
            ],
        }
    }

    #[test]
    fn eq_prune_keeps_only_in_range_files_as_indexed() {
        // Three files with disjoint key ranges; key=150 falls only in file B.
        let files = vec![
            data_file("s3://t/data/a.parquet", Some(&ext_covering(0, 99))),
            data_file("s3://t/data/b.parquet", Some(&ext_covering(100, 199))),
            data_file("s3://t/data/c.parquet", Some(&ext_covering(200, 299))),
        ];
        let plan = plan_scan_eq(&files, IDX, &Key::I64(150)).unwrap();

        assert_eq!(plan.len(), 1, "only file B could contain key 150");
        assert_eq!(plan.indexed_count(), 1);
        assert_eq!(plan.full_scan_count(), 0);
        match &plan.targets[0] {
            ScanTarget::Indexed {
                data_uri,
                sidecar_uri,
            } => {
                assert_eq!(data_uri, "s3://t/data/b.parquet");
                // resolve_sidecar_uri anchors the relative name to the data dir.
                assert_eq!(sidecar_uri, "s3://t/data/data.parquet.idx");
            }
            other => panic!("expected Indexed, got {other:?}"),
        }
    }

    #[test]
    fn survivor_without_sidecar_is_full_scan_not_dropped() {
        // File B could match but carries NO extension → must be full-scanned,
        // never silently dropped (that would lose rows: the prune is
        // conservative, not exact).
        let files = vec![
            data_file("s3://t/data/a.parquet", Some(&ext_covering(0, 99))),
            data_file("s3://t/data/b.parquet", None),
        ];
        // Open-ended range [100, ∞): file A (0..99) is provably out, file B has
        // no summary so it is conservatively kept.
        let plan = plan_scan_range(&files, IDX, Some(&Key::I64(100)), None).unwrap();

        assert_eq!(plan.len(), 1);
        assert_eq!(plan.indexed_count(), 0);
        assert_eq!(plan.full_scan_count(), 1);
        assert_eq!(
            plan.targets[0],
            ScanTarget::FullScan {
                data_uri: "s3://t/data/b.parquet".into(),
            }
        );
    }

    #[test]
    fn extension_present_but_wrong_index_falls_back_to_full_scan() {
        // The sidecar covers "idx_other", not the queried "idx_key" — we cannot
        // index-lookup this predicate on it, so it must full-scan.
        let ext = EmatixDataFileExtension {
            sidecar_relative_path: "data.parquet.idx".into(),
            summaries: vec![
                IndexSummary::new("idx_other").with_range(SummaryKey::I64(0), SummaryKey::I64(500)),
            ],
        };
        let files = vec![data_file("s3://t/data/a.parquet", Some(&ext))];
        let plan = plan_scan_eq(&files, IDX, &Key::I64(150)).unwrap();

        assert_eq!(plan.full_scan_count(), 1);
        assert_eq!(plan.indexed_count(), 0);
    }

    /// Σ.SC I.3: the i64 convenience wrappers (what the distributed
    /// coordinator lowers from — its wire predicate is i64-only) agree with
    /// the `Key`-typed planners.
    #[test]
    fn i64_wrappers_match_key_planners() {
        let files = vec![
            data_file("s3://t/data/a.parquet", Some(&ext_covering(0, 99))),
            data_file("s3://t/data/b.parquet", Some(&ext_covering(100, 199))),
            data_file("s3://t/data/c.parquet", Some(&ext_covering(200, 299))),
        ];
        assert_eq!(
            plan_scan_eq_i64(&files, IDX, 150).unwrap(),
            plan_scan_eq(&files, IDX, &Key::I64(150)).unwrap()
        );
        assert_eq!(
            plan_scan_range_i64(&files, IDX, Some(100), None).unwrap(),
            plan_scan_range(&files, IDX, Some(&Key::I64(100)), None).unwrap()
        );
    }

    /// Σ.SC I.3: with no prunable predicate every file survives and must be
    /// full-scanned — there is no index lookup to run, so nothing is tagged
    /// `Indexed` even when a covering sidecar exists.
    #[test]
    fn plan_scan_all_keeps_every_file_as_full_scan() {
        let files = vec![
            data_file("s3://t/data/a.parquet", Some(&ext_covering(0, 99))),
            data_file("s3://t/data/b.parquet", None),
            data_file("s3://t/data/c.parquet", Some(&ext_covering(200, 299))),
        ];
        let plan = plan_scan_all(&files);
        assert_eq!(plan.len(), 3);
        assert_eq!(plan.full_scan_count(), 3);
        assert_eq!(plan.indexed_count(), 0);
        assert_eq!(plan.targets[1].data_uri(), "s3://t/data/b.parquet");
    }

    #[test]
    fn eq_prune_to_empty_when_no_file_can_match() {
        let files = vec![
            data_file("s3://t/data/a.parquet", Some(&ext_covering(0, 99))),
            data_file("s3://t/data/b.parquet", Some(&ext_covering(100, 199))),
        ];
        let plan = plan_scan_eq(&files, IDX, &Key::I64(10_000)).unwrap();
        assert!(plan.is_empty(), "no file's range covers 10000");
    }
}
