//! Σ.SC I.4 — write-side Iceberg manifest stamping.
//!
//! When flow writes (or backfills onto) an Iceberg table, each data file's
//! manifest entry gets the ematix extension stamped into `key_metadata`: the
//! per-index `(min_key, max_key)` summary plus the relative sidecar path. The
//! coordinator's manifest prune (`crate::iceberg_scan`) reads these back —
//! this module is the producer half of that contract.
//!
//! The stamped summary is derived **self-contained from the parquet footer
//! statistics** (`crate::emat_parquet_metadata`): no data pages are read and
//! no sidecar is opened, so stamping N files is N footer reads. The sidecar
//! itself (built by `crate::sidecar_build`) is only *pointed at*, never
//! validated here — a stale/absent sidecar degrades to a worker-side full
//! scan, which the read path already treats as recoverable.
//!
//! Gated behind `--features iceberg` (off by default) like its read-side
//! sibling, so the default build and the PyPI wheel never pull iceberg-rust.

use std::path::Path;

use datafusion::common::ScalarValue;
use datafusion::common::stats::Precision;
use datafusion::error::{DataFusionError, Result as DfResult};
use ematix_iceberg::iceberg_rs::attach_extension;
use ematix_iceberg::{EmatixDataFileExtension, IndexSummary, SummaryKey};
use iceberg::spec::{DataContentType, DataFile, DataFileBuilder, DataFileFormat, Struct};

use crate::emat_parquet_metadata::load_provider_metadata;

/// Derive the per-file [`IndexSummary`] for `index_name` on `column` from the
/// parquet footer statistics of `source_path`. `column` is a leaf-column
/// **name** (e.g. `"l_orderkey"`) or numeric **ordinal** (e.g. `"0"`) —
/// mirroring `crate::sidecar_build`'s convention.
///
/// Types map footer → summary as `Int64 → I64`, `Int32`/`Date32` → `I32`,
/// `Utf8` → `Bytes`. A column whose footer carries no min/max (e.g. an
/// unsupported physical type) yields a **bound-less** summary — the manifest
/// prune then conservatively keeps the file on every query, which is correct,
/// just unhelpful.
pub fn summary_from_footer(
    source_path: &Path,
    index_name: &str,
    column: &str,
) -> DfResult<IndexSummary> {
    let md = load_provider_metadata(source_path).map_err(|e| {
        DataFusionError::Execution(format!("iceberg stamp: read footer {source_path:?}: {e:?}"))
    })?;

    // Resolve name-or-ordinal against the footer-derived arrow schema.
    let ordinal = match column.parse::<usize>() {
        Ok(i) if i < md.schema.fields().len() => i,
        Ok(i) => {
            return Err(DataFusionError::Execution(format!(
                "iceberg stamp: column ordinal {i} out of range ({} leaves)",
                md.schema.fields().len()
            )));
        }
        Err(_) => md.schema.index_of(column).map_err(|_| {
            let names: Vec<&str> = md
                .schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect();
            DataFusionError::Execution(format!(
                "iceberg stamp: no leaf column named {column:?}; available leaves: {names:?}"
            ))
        })?,
    };

    let stats = &md.column_stats[ordinal];
    let summary = IndexSummary::new(index_name);
    match (summary_key(&stats.min_value), summary_key(&stats.max_value)) {
        (Some(min), Some(max)) => Ok(summary.with_range(min, max)),
        // No usable bounds — stamp the bound-less summary (conservative keep).
        _ => Ok(summary),
    }
}

/// Footer `ScalarValue` bound → [`SummaryKey`], for the types the sidecar
/// layer indexes. `None` for absent/inexact bounds or unindexable types.
fn summary_key(bound: &Precision<ScalarValue>) -> Option<SummaryKey> {
    // Footer-derived bounds are Exact; anything weaker must not be stamped as
    // a pruning bound (a too-tight inexact bound would cause false-negative
    // prunes — silently dropped rows).
    let Precision::Exact(sv) = bound else {
        return None;
    };
    match sv {
        ScalarValue::Int64(Some(v)) => Some(SummaryKey::I64(*v)),
        ScalarValue::Int32(Some(v)) => Some(SummaryKey::I32(*v)),
        ScalarValue::Date32(Some(v)) => Some(SummaryKey::I32(*v)),
        ScalarValue::Utf8(Some(s)) => Some(SummaryKey::Bytes(s.clone().into_bytes())),
        _ => None,
    }
}

/// The full extension for one data file: footer-derived summary + the
/// relative sidecar path (defaults to the conventional
/// `<file-name>.idx` next to the data file, i.e. `data.parquet.idx`).
pub fn extension_from_footer(
    source_path: &Path,
    index_name: &str,
    column: &str,
    sidecar_relative: Option<String>,
) -> DfResult<EmatixDataFileExtension> {
    let relative = match sidecar_relative {
        Some(r) => r,
        None => {
            let name = source_path
                .file_name()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "iceberg stamp: {source_path:?} has no file name"
                    ))
                })?
                .to_string_lossy();
            format!("{name}.idx")
        }
    };
    Ok(EmatixDataFileExtension {
        sidecar_relative_path: relative,
        summaries: vec![summary_from_footer(source_path, index_name, column)?],
    })
}

/// Backfill entry point (the `flow index build`-then-stamp flow): read the
/// local parquet at `source_path`, derive the summary, and produce a
/// manifest-ready [`DataFile`] recorded at `data_uri` (which may differ from
/// the local staging path, e.g. `s3://…` after upload) with `key_metadata`
/// stamped. Unpartitioned (`Struct::empty()`); partitioned stamping composes
/// [`extension_from_footer`] + [`attach_extension`] into the caller's own
/// `DataFileBuilder` flow instead.
pub fn stamped_data_file(
    source_path: &Path,
    data_uri: &str,
    index_name: &str,
    column: &str,
    partition_spec_id: i32,
) -> DfResult<DataFile> {
    let ext = extension_from_footer(source_path, index_name, column, None)?;
    let md = load_provider_metadata(source_path).map_err(|e| {
        DataFusionError::Execution(format!("iceberg stamp: read footer {source_path:?}: {e:?}"))
    })?;
    let file_size = std::fs::metadata(source_path)
        .map_err(|e| {
            DataFusionError::Execution(format!("iceberg stamp: stat {source_path:?}: {e}"))
        })?
        .len();

    let mut builder = DataFileBuilder::default();
    builder
        .content(DataContentType::Data)
        .file_path(data_uri.to_string())
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(file_size)
        .record_count(md.num_rows as u64)
        .partition_spec_id(partition_spec_id)
        .partition(Struct::empty());
    attach_extension(builder, &ext).build().map_err(|e| {
        DataFusionError::Execution(format!(
            "iceberg stamp: build data file {data_uri:?}: {e:?}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg_scan::{ScanTarget, plan_scan_eq_i64};
    use crate::sidecar_build::build_sorted_sidecar;
    use ematix_iceberg::iceberg_rs::extract_extension;
    use ematix_parquet_codec::write::{ColumnData, write_table_to_path};
    use ematix_parquet_format::types::CompressionCodec;
    use iceberg::io::FileIOBuilder;
    use iceberg::spec::{
        Manifest, ManifestWriterBuilder, NestedField, PartitionSpec, PrimitiveType, Schema, Type,
    };
    use std::path::PathBuf;
    use std::sync::Arc;

    const IDX: &str = "idx_id";

    /// Write an `(id: i64, val: i64)` parquet at `dir/name`. (`id`, not `key`
    /// — names ending in `key` hit the scale-gated EMAT_DOWNCAST_KEYS
    /// narrowing and make fixtures env-race-flaky.)
    fn write_part(dir: &Path, name: &str, ids: &[i64], vals: &[i64]) -> PathBuf {
        let path = dir.join(name);
        write_table_to_path(
            &path,
            &[("id", ColumnData::I64(ids)), ("val", ColumnData::I64(vals))],
            CompressionCodec::Uncompressed,
        )
        .expect("write fixture parquet");
        path
    }

    /// The iceberg table schema matching the fixture parts.
    fn table_schema() -> Arc<Schema> {
        Arc::new(
            Schema::builder()
                .with_schema_id(0)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::required(2, "val", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .expect("schema"),
        )
    }

    /// The footer-derived summary carries the file's EXACT min/max — the
    /// bounds the manifest prune's correctness rests on.
    #[test]
    fn summary_from_footer_derives_exact_min_max() {
        let dir = tempfile::tempdir().unwrap();
        let ids: Vec<i64> = (100..200).collect();
        let vals: Vec<i64> = ids.iter().map(|k| k * 2).collect();
        let p = write_part(dir.path(), "b.parquet", &ids, &vals);

        // By name and by ordinal, same result.
        for col in ["id", "0"] {
            let s = summary_from_footer(&p, IDX, col).unwrap();
            assert_eq!(s.name, IDX);
            assert_eq!(s.min_key, Some(SummaryKey::I64(100)), "col={col}");
            assert_eq!(s.max_key, Some(SummaryKey::I64(199)), "col={col}");
        }
        // Unknown column is a clear error, not a bound-less summary — the
        // caller asked to index something that doesn't exist.
        let err = summary_from_footer(&p, IDX, "nope").unwrap_err();
        assert!(format!("{err}").contains("no leaf column named"), "{err}");
    }

    /// THE I.4 round-trip: stamp two files (with sidecars) + one unstamped
    /// file into a real v2 manifest, read the manifest back, and assert every
    /// stamped entry carries the summary + sidecar path while the unstamped
    /// one reads back as `None` — and the read-path prune treats it as a
    /// full-scan survivor, never an error.
    #[tokio::test]
    async fn stamped_manifest_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let a = write_part(
            dir.path(),
            "a.parquet",
            &(0..100).collect::<Vec<_>>(),
            &(0..100).collect::<Vec<_>>(),
        );
        let b = write_part(
            dir.path(),
            "b.parquet",
            &(100..200).collect::<Vec<_>>(),
            &(100..200).collect::<Vec<_>>(),
        );
        let c = write_part(
            dir.path(),
            "c.parquet",
            &(200..300).collect::<Vec<_>>(),
            &(200..300).collect::<Vec<_>>(),
        );
        // Phase 2 sidecars for the stamped files (the stamp only points at
        // them; building first mirrors the real backfill order).
        build_sorted_sidecar(&a, IDX, "id", None).unwrap();
        build_sorted_sidecar(&b, IDX, "id", None).unwrap();

        // Stamped a + b; c goes in plain (e.g. written by a non-ematix engine).
        let df_a = stamped_data_file(&a, &format!("file://{}", a.display()), IDX, "id", 0).unwrap();
        let df_b = stamped_data_file(&b, &format!("file://{}", b.display()), IDX, "id", 0).unwrap();
        let mut plain = DataFileBuilder::default();
        plain
            .content(DataContentType::Data)
            .file_path(format!("file://{}", c.display()))
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(std::fs::metadata(&c).unwrap().len())
            .record_count(100)
            .partition_spec_id(0)
            .partition(Struct::empty());
        let df_c = plain.build().unwrap();

        // Manual manifest construction — the buildable-today write path.
        let manifest_path = dir.path().join("m1.avro");
        let io = FileIOBuilder::new_fs_io().build().unwrap();
        let output = io.new_output(manifest_path.to_string_lossy()).unwrap();
        let mut writer = ManifestWriterBuilder::new(
            output,
            Some(1),
            None,
            table_schema(),
            PartitionSpec::unpartition_spec(),
        )
        .build_v2_data();
        for df in [df_a, df_b, df_c] {
            writer.add_file(df, 1).unwrap();
        }
        writer.write_manifest_file().await.unwrap();

        // ---- Read the manifest back ----
        let bytes = std::fs::read(&manifest_path).unwrap();
        let manifest = Manifest::parse_avro(&bytes).unwrap();
        let files: Vec<DataFile> = manifest
            .entries()
            .iter()
            .map(|e| e.data_file().clone())
            .collect();
        assert_eq!(files.len(), 3);

        // Stamped entries: summary + sidecar path survive the avro round-trip.
        for (df, name, (min, max)) in [
            (&files[0], "a.parquet", (0, 99)),
            (&files[1], "b.parquet", (100, 199)),
        ] {
            let ext = extract_extension(df)
                .expect("well-formed extension")
                .unwrap_or_else(|| panic!("{name} must be stamped"));
            assert_eq!(ext.sidecar_relative_path, format!("{name}.idx"));
            let s = ext.summary(IDX).expect("summary for idx_id");
            assert_eq!(s.min_key, Some(SummaryKey::I64(min)), "{name}");
            assert_eq!(s.max_key, Some(SummaryKey::I64(max)), "{name}");
        }
        // Unstamped entry: None, not an error.
        assert!(extract_extension(&files[2]).unwrap().is_none());

        // Read-path handoff: pruning eq=150 keeps b (indexed, with its
        // sidecar URI resolved next to the data file) and conservatively
        // keeps the unstamped c as a FULL SCAN — never dropped, never an
        // error. a is provably out.
        let plan = plan_scan_eq_i64(&files, IDX, 150).unwrap();
        assert_eq!(plan.len(), 2);
        assert_eq!(plan.indexed_count(), 1);
        assert_eq!(plan.full_scan_count(), 1);
        match &plan.targets[0] {
            ScanTarget::Indexed { sidecar_uri, .. } => {
                assert!(
                    sidecar_uri.ends_with("b.parquet.idx"),
                    "sidecar next to data file: {sidecar_uri}"
                );
            }
            other => panic!("expected b Indexed, got {other:?}"),
        }
        assert_eq!(
            plan.targets[1].data_uri(),
            &format!("file://{}", c.display())
        );
    }
}
