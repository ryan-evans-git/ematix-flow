//! Single-node **multi-file** table provider — read a directory of Parquet
//! parts (`<table>/<table>-NNNN.parquet`) as one table through the
//! ematix-parquet codec, with **no distribution** and **no arrow-rs reader**.
//!
//! ## The gap this closes
//!
//! The shipped [`EmatixFastParquetTableProvider`] reads exactly **one file** per
//! instance (`try_new(path)` → `ParquetFile::open(path)`). So a single node
//! pointed at a partitioned dataset — the ubiquitous `table/part-*.parquet`
//! layout — could previously only read it by:
//!   - arrow-rs `register_parquet` (a `ListingTable`), which **decodes with
//!     arrow-rs, not the ematix codec** (the benchmark "confound"); or
//!   - distributing, where arrow-rs enumerates files and each peer decodes its
//!     one shard via ematix.
//!
//! There was no way to read a multi-file dataset **single-node through
//! ematix-parquet**. This provider is that missing primitive: it wraps one
//! [`EmatixFastParquetTableProvider`] per part and [`UnionExec`]s their scans.
//! Each part is decoded by the ematix codec, and the parts run as independent
//! partitions across cores — the scan parallelism a single huge file is starved
//! of (e.g. the SF100 Q09 penalty on one 22 GB `lineitem`).
//!
//! It is also the local building block the distributed Iceberg plan lowers onto
//! (`crate::iceberg_scan`): "these N surviving files are one table."

use std::any::Any;
use std::path::Path;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Statistics;
use datafusion::common::stats::{ColumnStatistics, Precision};
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::union::UnionExec;

use crate::ematix_fast_parquet::EmatixFastParquetTableProvider;

/// A table backed by many Parquet part-files, each read via the ematix-parquet
/// fast reader. Single-node; the parts scan in parallel via [`UnionExec`].
///
/// `Debug` is required by DataFusion's `TableProvider` supertrait.
#[derive(Debug)]
pub struct EmatixFastParquetMultiTableProvider {
    /// One single-file provider per part. All share `schema`.
    parts: Vec<EmatixFastParquetTableProvider>,
    /// Shared (Utf8View-promoted) schema — validated identical across parts.
    schema: SchemaRef,
    /// Exact total row count (Σ of the parts' file-metadata row counts).
    num_rows: usize,
}

impl EmatixFastParquetMultiTableProvider {
    /// Build from an explicit, ordered list of part-file paths. Every part must
    /// share the first part's schema (fields); a mismatch is an error. Empty
    /// input is an error.
    pub fn try_new_files(paths: Vec<String>) -> DfResult<Self> {
        if paths.is_empty() {
            return Err(DataFusionError::Plan(
                "multi-file provider: no part files given".into(),
            ));
        }
        let mut parts = Vec::with_capacity(paths.len());
        for p in paths {
            parts.push(EmatixFastParquetTableProvider::try_new(p)?);
        }
        let schema = parts[0].schema();
        for (i, part) in parts.iter().enumerate().skip(1) {
            if part.schema().fields() != schema.fields() {
                return Err(DataFusionError::Plan(format!(
                    "multi-file provider: part {i} schema differs from part 0 — all parts \
                     must share a schema"
                )));
            }
        }
        let num_rows = parts.iter().map(exact_rows).sum();
        Ok(Self {
            parts,
            schema,
            num_rows,
        })
    }

    /// Build from a directory: every `*.parquet` directly under `dir`, in
    /// lexicographic order (deterministic, matching the `part-NNNN` convention).
    /// A directory with no parts is an error.
    pub fn try_new_dir(dir: impl AsRef<Path>) -> DfResult<Self> {
        let dir = dir.as_ref();
        let mut paths: Vec<String> = std::fs::read_dir(dir)
            .map_err(|e| {
                DataFusionError::Plan(format!("multi-file provider: read_dir {dir:?}: {e}"))
            })?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().map(|x| x == "parquet").unwrap_or(false))
            .map(|p| p.to_string_lossy().into_owned())
            .collect();
        paths.sort();
        if paths.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "multi-file provider: no .parquet parts under {dir:?}"
            )));
        }
        Self::try_new_files(paths)
    }

    /// Number of part-files this table spans.
    pub fn num_parts(&self) -> usize {
        self.parts.len()
    }
}

/// Exact file-metadata row count of a single-file provider (via its public
/// `statistics()`), defaulting to 0 if absent.
fn exact_rows(p: &EmatixFastParquetTableProvider) -> usize {
    match p.statistics().map(|s| s.num_rows) {
        Some(Precision::Exact(n)) | Some(Precision::Inexact(n)) => n,
        _ => 0,
    }
}

#[async_trait]
impl TableProvider for EmatixFastParquetMultiTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// All parts share a schema, so filter-pushdown support is identical across
    /// them — delegate to part 0. The same `filters` are pushed into every
    /// part's scan below, so the decision is honoured uniformly.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        self.parts[0].supports_filters_pushdown(filters)
    }

    /// Exact total row count (so the planner sizes this table correctly).
    /// Per-column min/max are left unknown for now — a conservative, correct
    /// input; merging the parts' typed stats is a follow-up.
    fn statistics(&self) -> Option<Statistics> {
        let cols = self.schema.fields().len();
        Some(Statistics {
            num_rows: Precision::Exact(self.num_rows),
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics::new_unknown(); cols],
        })
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Fan the SAME projection + filters into each part's scan (reusing all of
        // the single-file provider's machinery: RG assignment, BridgeFilter
        // pushdown, late-mat), then union the per-part execs. Each child yields
        // its own partitions, so the union runs the parts in parallel.
        let mut children: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(self.parts.len());
        for part in &self.parts {
            children.push(part.scan(state, projection, filters, limit).await?);
        }
        // A single part needs no union wrapper.
        if children.len() == 1 {
            return Ok(children.pop().expect("len checked"));
        }
        Ok(Arc::new(UnionExec::new(children)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::ExecutionPlanProperties;
    use datafusion::prelude::SessionContext;
    use ematix_parquet_codec::write::{ColumnData, write_table_to_path};
    use ematix_parquet_format::types::CompressionCodec;

    /// Write a `(key: i64, val: i64)` parquet at `path`.
    fn write_part(path: &Path, keys: &[i64], vals: &[i64]) {
        write_table_to_path(
            path,
            &[
                ("key", ColumnData::I64(keys)),
                ("val", ColumnData::I64(vals)),
            ],
            CompressionCodec::Uncompressed,
        )
        .expect("write parquet");
    }

    /// Build 3 parts (disjoint key ranges) + one combined file with the same
    /// rows; return (part_paths, combined_path).
    fn fixture(dir: &Path) -> (Vec<String>, String) {
        let mut all_k = Vec::new();
        let mut all_v = Vec::new();
        let mut parts = Vec::new();
        for (i, base) in [0i64, 100, 200].iter().enumerate() {
            let keys: Vec<i64> = (*base..*base + 100).collect();
            let vals: Vec<i64> = keys.iter().map(|k| k * 2).collect();
            let p = dir.join(format!("part-{i:04}.parquet"));
            write_part(&p, &keys, &vals);
            parts.push(p.to_string_lossy().into_owned());
            all_k.extend(keys);
            all_v.extend(vals);
        }
        let combined = dir.join("combined.parquet");
        write_part(&combined, &all_k, &all_v);
        (parts, combined.to_string_lossy().into_owned())
    }

    async fn run(ctx: &SessionContext, sql: &str) -> String {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        datafusion::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string()
    }

    /// THE oracle: querying N parts through the multi-file provider returns
    /// exactly what querying one combined file returns — with projection,
    /// a pushed predicate, and an aggregate.
    #[tokio::test]
    async fn multi_matches_single_combined_file() {
        let dir = tempfile::tempdir().unwrap();
        let (parts, combined) = fixture(dir.path());

        let ctx = SessionContext::new();
        ctx.register_table(
            "multi",
            Arc::new(EmatixFastParquetMultiTableProvider::try_new_files(parts).unwrap()),
        )
        .unwrap();
        ctx.register_table(
            "single",
            Arc::new(EmatixFastParquetTableProvider::try_new(combined).unwrap()),
        )
        .unwrap();

        // Filter (pushdown-eligible) + projection + aggregate spanning parts.
        for tmpl in [
            "SELECT count(*) c, sum(val) s FROM {} WHERE key >= 50 AND key < 250",
            "SELECT key, val FROM {} WHERE key IN (0, 150, 299) ORDER BY key",
            "SELECT count(*) FROM {}",
        ] {
            let m = run(&ctx, &tmpl.replace("{}", "multi")).await;
            let s = run(&ctx, &tmpl.replace("{}", "single")).await;
            assert_eq!(m, s, "multi != single for: {tmpl}");
        }
    }

    /// `try_new_dir` discovers all parts, and the union exposes one partition
    /// per part — i.e. real single-node scan parallelism.
    #[tokio::test]
    async fn dir_discovers_parts_and_parallelizes() {
        let dir = tempfile::tempdir().unwrap();
        let (_parts, _combined) = fixture(dir.path());
        // fixture also wrote combined.parquet into dir — that's a 4th "part".
        // Use a clean subdir with only parts to keep the count exact.
        let pdir = dir.path().join("parts");
        std::fs::create_dir(&pdir).unwrap();
        for (i, base) in [0i64, 100, 200].iter().enumerate() {
            let keys: Vec<i64> = (*base..*base + 100).collect();
            let vals: Vec<i64> = keys.iter().map(|k| k * 3).collect();
            write_part(&pdir.join(format!("part-{i:04}.parquet")), &keys, &vals);
        }
        let prov = EmatixFastParquetMultiTableProvider::try_new_dir(&pdir).unwrap();
        assert_eq!(prov.num_parts(), 3);

        let ctx = SessionContext::new();
        let plan = prov.scan(&ctx.state(), None, &[], None).await.unwrap();
        assert_eq!(
            plan.output_partitioning().partition_count(),
            3,
            "union should expose one partition per part (parallel scan)"
        );
    }

    #[tokio::test]
    async fn schema_mismatch_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.parquet");
        write_part(&a, &[1, 2, 3], &[1, 2, 3]);
        // Different schema: a single i64 column named differently / one column.
        let b = dir.path().join("b.parquet");
        write_table_to_path(
            &b,
            &[("only", ColumnData::I64(&[9, 9]))],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
        let err = EmatixFastParquetMultiTableProvider::try_new_files(vec![
            a.to_string_lossy().into_owned(),
            b.to_string_lossy().into_owned(),
        ])
        .unwrap_err();
        assert!(format!("{err}").contains("schema differs"), "got: {err}");
    }

    #[tokio::test]
    async fn empty_inputs_error() {
        assert!(EmatixFastParquetMultiTableProvider::try_new_files(vec![]).is_err());
        let dir = tempfile::tempdir().unwrap();
        assert!(EmatixFastParquetMultiTableProvider::try_new_dir(dir.path()).is_err());
    }

    /// A single part behaves like the plain single-file provider (no union).
    #[tokio::test]
    async fn single_part_passthrough() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("only.parquet");
        write_part(&p, &[1, 2, 3, 4], &[10, 20, 30, 40]);
        let prov = EmatixFastParquetMultiTableProvider::try_new_files(vec![
            p.to_string_lossy().into_owned(),
        ])
        .unwrap();
        assert_eq!(prov.num_parts(), 1);
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(prov)).unwrap();
        let out = run(&ctx, "SELECT sum(val) FROM t WHERE key >= 2").await;
        assert!(
            out.contains("90"),
            "sum(val where key>=2)=20+30+40=90; got {out}"
        );
    }
}
