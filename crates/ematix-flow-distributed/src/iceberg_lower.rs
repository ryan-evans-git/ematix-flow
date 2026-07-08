//! Σ.SC I.3 — coordinator lowering: manifest-pruned Iceberg scan → WorkUnits.
//!
//! The coordinator runs the conservative `(min,max)` summary prune **once**
//! (reusing `ematix_flow_core::iceberg_scan`'s planners over the snapshot's
//! `DataFile`s) and lowers each surviving file into its own
//! [`WorkUnit`] carrying [`Input::IcebergScan`] — the iceberg-free wire form.
//! The mesh therefore starts with a strictly smaller task set than a
//! prefix-listing fan-out: pruned files never become tasks at all.
//!
//! One WorkUnit per surviving file (not per chunk, yet): the per-file sidecar
//! already sub-divides a file into row groups on the worker, and TPC-H parted
//! layouts keep files near-uniform, so file granularity is the fan-out unit
//! until a skew campaign says otherwise.
//!
//! Only this module (coordinator-side) needs `--features iceberg`; the wire
//! types it emits live un-gated in [`crate::work_unit`] so workers never link
//! iceberg-rust.

use datafusion::error::Result as DfResult;
use ematix_flow_core::iceberg_scan::{
    IcebergScanPlan, ScanTarget, plan_scan_all, plan_scan_eq_i64, plan_scan_range_i64,
};
use iceberg::spec::DataFile;

use crate::work_unit::{IcebergPredicate, IcebergScanTarget, Input, Output, Query, WorkUnit};

/// Everything about the fan-out that is NOT derived from the prune: which
/// table the files belong to, which index was pruned on, what each worker
/// executes, and where ids/outputs are rooted.
#[derive(Debug, Clone)]
pub struct IcebergLoweringSpec {
    /// Table name the worker registers the surviving files under.
    pub table: String,
    /// Index the prune ran on; workers replay it as the per-file sidecar
    /// lookup on `Indexed` targets.
    pub index_name: String,
    /// Query every unit executes (cloned per unit).
    pub query: Query,
    /// Each unit `i` writes `{output_uri_prefix}/{unit_id}.arrow`.
    pub output_uri_prefix: String,
    /// Unit ids are `{unit_id_prefix}-{i:04}` in target order — stable for a
    /// given plan, so coordinator retries re-emit identical units (the wire
    /// schema's byte-stability guarantee does the rest).
    pub unit_id_prefix: String,
}

/// Lower an already-computed scan plan. `predicate` is echoed onto every unit
/// (`None` = pure enumeration fan-out). Exposed so a caller that already holds
/// an [`IcebergScanPlan`] (e.g. from a custom prune) can reuse the lowering.
pub fn lower_plan(
    plan: &IcebergScanPlan,
    predicate: Option<IcebergPredicate>,
    spec: &IcebergLoweringSpec,
) -> Vec<WorkUnit> {
    let prefix = spec.output_uri_prefix.trim_end_matches('/');
    plan.targets
        .iter()
        .enumerate()
        .map(|(i, target)| {
            let id = format!("{}-{i:04}", spec.unit_id_prefix);
            let wire_target = match target {
                ScanTarget::Indexed {
                    data_uri,
                    sidecar_uri,
                } => IcebergScanTarget::Indexed {
                    data_uri: data_uri.clone(),
                    sidecar_uri: sidecar_uri.clone(),
                },
                ScanTarget::FullScan { data_uri } => IcebergScanTarget::FullScan {
                    data_uri: data_uri.clone(),
                },
            };
            WorkUnit {
                schema: WorkUnit::default_schema(),
                id: id.clone(),
                query: spec.query.clone(),
                input: Input::IcebergScan {
                    table: spec.table.clone(),
                    index_name: spec.index_name.clone(),
                    predicate: predicate.clone(),
                    targets: vec![wire_target],
                },
                output: Output::ArrowIpc {
                    uri: format!("{prefix}/{id}.arrow"),
                },
                execution: Default::default(),
            }
        })
        .collect()
}

/// Prune `files` for `WHERE <index> = key` ONCE, then lower the survivors —
/// one WorkUnit per file, `Indexed` where a covering sidecar exists.
pub fn lower_scan_eq(
    files: &[DataFile],
    key: i64,
    spec: &IcebergLoweringSpec,
) -> DfResult<Vec<WorkUnit>> {
    let plan = plan_scan_eq_i64(files, &spec.index_name, key)?;
    Ok(lower_plan(&plan, Some(IcebergPredicate::Eq { key }), spec))
}

/// Prune `files` for `WHERE <index> BETWEEN low AND high` (either bound open)
/// ONCE, then lower the survivors.
pub fn lower_scan_range(
    files: &[DataFile],
    low: Option<i64>,
    high: Option<i64>,
    spec: &IcebergLoweringSpec,
) -> DfResult<Vec<WorkUnit>> {
    let plan = plan_scan_range_i64(files, &spec.index_name, low, high)?;
    Ok(lower_plan(
        &plan,
        Some(IcebergPredicate::Range { low, high }),
        spec,
    ))
}

/// No prunable predicate: every file fans out as a full scan.
pub fn lower_scan_all(files: &[DataFile], spec: &IcebergLoweringSpec) -> Vec<WorkUnit> {
    lower_plan(&plan_scan_all(files), None, spec)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ematix_iceberg::iceberg_rs::encode_key_metadata;
    use ematix_iceberg::{EmatixDataFileExtension, IndexSummary, SummaryKey};
    use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};

    const IDX: &str = "idx_id";

    /// Standalone `DataFile` (no catalog) at `uri`, optionally stamped with an
    /// ematix extension. Mirrors the `iceberg_scan` test fixture.
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

    /// Extension whose `idx_id` summary covers `[min,max]` and points at the
    /// conventional sidecar name for `stem`.
    fn ext_covering(stem: &str, min: i64, max: i64) -> EmatixDataFileExtension {
        EmatixDataFileExtension {
            sidecar_relative_path: format!("{stem}.parquet.idx"),
            summaries: vec![
                IndexSummary::new(IDX).with_range(SummaryKey::I64(min), SummaryKey::I64(max)),
            ],
        }
    }

    /// Three non-overlapping files (0-99 / 100-199 / 200-299).
    fn three_files() -> Vec<DataFile> {
        vec![
            data_file("s3://t/data/a.parquet", Some(&ext_covering("a", 0, 99))),
            data_file("s3://t/data/b.parquet", Some(&ext_covering("b", 100, 199))),
            data_file("s3://t/data/c.parquet", Some(&ext_covering("c", 200, 299))),
        ]
    }

    fn spec() -> IcebergLoweringSpec {
        IcebergLoweringSpec {
            table: "t".into(),
            index_name: IDX.into(),
            query: Query::Tpch { id: "Q06".into() },
            // Trailing slash on purpose: lowering must not emit `//`.
            output_uri_prefix: "s3://results/run-1/".into(),
            unit_id_prefix: "wu-ice".into(),
        }
    }

    /// THE lowering contract: prune runs once, and only survivors become
    /// units. key=150 lives only in file b → exactly ONE unit, carrying b's
    /// path + resolved sidecar URI and the echoed predicate.
    #[test]
    fn eq_prune_to_one_file_lowers_to_one_unit() {
        let units = lower_scan_eq(&three_files(), 150, &spec()).unwrap();
        assert_eq!(units.len(), 1, "only file b can contain key 150");

        let wu = &units[0];
        wu.validate_schema().expect("schema pin set");
        assert_eq!(wu.id, "wu-ice-0000");
        assert_eq!(
            wu.output,
            Output::ArrowIpc {
                uri: "s3://results/run-1/wu-ice-0000.arrow".into(),
            }
        );
        match &wu.input {
            Input::IcebergScan {
                table,
                index_name,
                predicate,
                targets,
            } => {
                assert_eq!(table, "t");
                assert_eq!(index_name, IDX);
                assert_eq!(*predicate, Some(IcebergPredicate::Eq { key: 150 }));
                assert_eq!(
                    *targets,
                    vec![IcebergScanTarget::Indexed {
                        data_uri: "s3://t/data/b.parquet".into(),
                        sidecar_uri: "s3://t/data/b.parquet.idx".into(),
                    }]
                );
            }
            other => panic!("expected IcebergScan, got {other:?}"),
        }
    }

    /// No predicate → no prune: all three files fan out, one unit each, all
    /// full-scan (nothing to index-look-up), with stable sequential ids and
    /// distinct output URIs.
    #[test]
    fn no_predicate_lowers_every_file() {
        let units = lower_scan_all(&three_files(), &spec());
        assert_eq!(units.len(), 3);
        for (i, (wu, path)) in units
            .iter()
            .zip(["a.parquet", "b.parquet", "c.parquet"])
            .enumerate()
        {
            assert_eq!(wu.id, format!("wu-ice-{i:04}"));
            assert_eq!(
                wu.output,
                Output::ArrowIpc {
                    uri: format!("s3://results/run-1/wu-ice-{i:04}.arrow"),
                }
            );
            match &wu.input {
                Input::IcebergScan {
                    predicate, targets, ..
                } => {
                    assert_eq!(*predicate, None);
                    assert_eq!(
                        *targets,
                        vec![IcebergScanTarget::FullScan {
                            data_uri: format!("s3://t/data/{path}"),
                        }]
                    );
                }
                other => panic!("expected IcebergScan, got {other:?}"),
            }
        }
    }

    /// A range survivor with NO covering sidecar lowers as a full-scan target
    /// — conservative prune means dropping it would lose rows.
    #[test]
    fn range_survivor_without_sidecar_lowers_to_full_scan() {
        let files = vec![
            data_file("s3://t/data/a.parquet", Some(&ext_covering("a", 0, 99))),
            data_file("s3://t/data/b.parquet", None),
        ];
        // [100, ∞): a is provably out; b has no summary → conservatively kept.
        let units = lower_scan_range(&files, Some(100), None, &spec()).unwrap();
        assert_eq!(units.len(), 1);
        match &units[0].input {
            Input::IcebergScan {
                predicate, targets, ..
            } => {
                assert_eq!(
                    *predicate,
                    Some(IcebergPredicate::Range {
                        low: Some(100),
                        high: None,
                    })
                );
                assert_eq!(
                    *targets,
                    vec![IcebergScanTarget::FullScan {
                        data_uri: "s3://t/data/b.parquet".into(),
                    }]
                );
            }
            other => panic!("expected IcebergScan, got {other:?}"),
        }
    }

    /// Every lowered unit round-trips the wire byte-stably — the property the
    /// coordinator's retry/dedupe layer leans on.
    #[test]
    fn lowered_units_round_trip_byte_stable() {
        for wu in lower_scan_eq(&three_files(), 150, &spec())
            .unwrap()
            .iter()
            .chain(lower_scan_all(&three_files(), &spec()).iter())
        {
            let s1 = serde_json::to_string(wu).unwrap();
            let wu2: WorkUnit = serde_json::from_str(&s1).unwrap();
            assert_eq!(*wu, wu2);
            assert_eq!(s1, serde_json::to_string(&wu2).unwrap());
        }
    }
}
