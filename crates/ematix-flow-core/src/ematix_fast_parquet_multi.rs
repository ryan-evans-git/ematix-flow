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
use datafusion::common::stats::Precision;
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

    /// Exact total row count + MERGED per-column stats across parts: `min` =
    /// min of mins, `max` = max of maxes, `null_count`/`sum` = sums,
    /// `distinct_count` = unknown (can't be merged across parts). This gives the
    /// join planner the same selectivity signal the single-file provider gives;
    /// without it, a partitioned fact table looks stats-less and the planner
    /// picks a memory-blowing join order that OOMs at SF100 (observed on Q07).
    fn statistics(&self) -> Option<Statistics> {
        let mut cols = self.parts[0].statistics()?.column_statistics;
        for part in &self.parts[1..] {
            if let Some(s) = part.statistics() {
                for (a, b) in cols.iter_mut().zip(s.column_statistics.iter()) {
                    a.null_count = a.null_count.add(&b.null_count);
                    a.min_value = a.min_value.min(&b.min_value);
                    a.max_value = a.max_value.max(&b.max_value);
                    a.sum_value = a.sum_value.add(&b.sum_value);
                    a.distinct_count = Precision::Absent;
                }
            }
        }
        Some(Statistics {
            num_rows: Precision::Exact(self.num_rows),
            total_byte_size: Precision::Absent,
            column_statistics: cols,
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
        //
        // Σ.MW.1: split the session's partition budget across parts instead of
        // letting every part size itself to the full `target_partitions` — the
        // union's width is what the operators above poll CONCURRENTLY, and
        // parts × target_partitions multiplied the query's decode working set
        // by the part count (13-part SF100 lineitem: ~182 streams, Q01 ~9 GB
        // peak, kernel OOM on 32 GB boxes within a few executions). Floor
        // division keeps Σ(part partitions) ≤ target_partitions once every
        // part has its guaranteed one; a table with more parts than the budget
        // degrades to one partition per part (width = part count).
        let target = state.config_options().execution.target_partitions;
        let per_part = (target / self.parts.len()).max(1);
        let mut children: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(self.parts.len());
        for part in &self.parts {
            children.push(
                part.scan_with_partition_budget(state, projection, filters, limit, Some(per_part))
                    .await?,
            );
        }
        // A single part needs no union wrapper.
        if children.len() == 1 {
            return Ok(children.pop().expect("len checked"));
        }
        // `try_new` returns `Arc<dyn ExecutionPlan>` already.
        UnionExec::try_new(children)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::ScalarValue;
    use datafusion::physical_plan::ExecutionPlanProperties;
    use datafusion::prelude::SessionContext;
    use ematix_parquet_codec::write::{ColumnData, write_table_to_path};
    use ematix_parquet_format::types::CompressionCodec;

    /// Write a `(id: i64, val: i64)` parquet at `path`.
    fn write_part(path: &Path, keys: &[i64], vals: &[i64]) {
        write_table_to_path(
            path,
            &[
                ("id", ColumnData::I64(keys)),
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
            "SELECT count(*) c, sum(val) s FROM {} WHERE id >= 50 AND id < 250",
            "SELECT id, val FROM {} WHERE id IN (0, 150, 299) ORDER BY id",
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
        // ≥ 3, not == 3: each part contributes AT LEAST one partition (the
        // parallel fan-out this test pins). The exact per-part count depends
        // on process-global `EMAT_*` env that parallel tests legitimately
        // mutate (observed flake: a concurrent env write let a child scan
        // expose extra partitions), so an exact bound races.
        let pc = plan.output_partitioning().partition_count();
        assert!(
            pc >= 3,
            "union should expose at least one partition per part (parallel scan), got {pc}"
        );
    }

    /// Merged statistics fold the row count and SPAN the parts' ranges — the
    /// signal the join planner needs (a stats-less partitioned fact table
    /// mis-plans and OOMs at SF100).
    #[tokio::test]
    async fn merged_stats_fold_rowcount_and_span_ranges() {
        let dir = tempfile::tempdir().unwrap();
        let (parts, _combined) = fixture(dir.path()); // 3 parts, key 0..=299
        let prov = EmatixFastParquetMultiTableProvider::try_new_files(parts).unwrap();
        let s = prov.statistics().unwrap();
        assert_eq!(s.num_rows, Precision::Exact(300), "row count = Σ parts");
        assert_eq!(s.column_statistics.len(), 2, "id, val");
        // When the parts carry exact footer min/max, the merge must span all of
        // them: key ranges 0..99, 100..199, 200..299 -> [0, 299].
        if let (Precision::Exact(min), Precision::Exact(max)) = (
            &s.column_statistics[0].min_value,
            &s.column_statistics[0].max_value,
        ) {
            assert_eq!(min, &ScalarValue::Int64(Some(0)), "min spans all parts");
            assert_eq!(max, &ScalarValue::Int64(Some(299)), "max spans all parts");
        }
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

    /// Write a `(id: i64, val: i64)` parquet with `num_rgs` row groups of
    /// `rows_per_rg` rows each — the shape that exposes the union-width bug
    /// (single-RG parts can only contribute one partition regardless).
    fn write_part_multi_rg(path: &Path, base: i64, num_rgs: usize, rows_per_rg: usize) {
        use datafusion::parquet::basic::{Repetition, Type as PhysicalType};
        use datafusion::parquet::column::writer::ColumnWriter;
        use datafusion::parquet::file::properties::WriterProperties;
        use datafusion::parquet::file::writer::SerializedFileWriter;
        use datafusion::parquet::schema::types::Type as PType;
        let f = |n: &str| {
            Arc::new(
                PType::primitive_type_builder(n, PhysicalType::INT64)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap(),
            )
        };
        let schema = Arc::new(
            PType::group_type_builder("schema")
                .with_fields(vec![f("id"), f("val")])
                .build()
                .unwrap(),
        );
        let file = std::fs::File::create(path).unwrap();
        let mut writer =
            SerializedFileWriter::new(file, schema, Arc::new(WriterProperties::builder().build()))
                .unwrap();
        for rg_i in 0..num_rgs {
            let start = base + (rg_i * rows_per_rg) as i64;
            let ids: Vec<i64> = (start..start + rows_per_rg as i64).collect();
            let vals: Vec<i64> = ids.iter().map(|k| k * 2).collect();
            let mut rg = writer.next_row_group().unwrap();
            for col_vals in [&ids, &vals] {
                let mut col = rg.next_column().unwrap().unwrap();
                if let ColumnWriter::Int64ColumnWriter(t) = col.untyped() {
                    t.write_batch(col_vals, None, None).unwrap();
                }
                col.close().unwrap();
            }
            rg.close().unwrap();
        }
        writer.close().unwrap();
    }

    /// Σ.MW.1 (parted-SF100 fast-path OOM): the union must not MULTIPLY scan
    /// width. Before the per-part budget split, every part independently took
    /// `min(num_rgs, target_partitions)` partitions — 13 lineitem parts on a
    /// 14-core box exposed ~182 concurrently-polled decode streams (~13× the
    /// intended width; Q01 peaked ~9 GB instead of ~2 GB and the 32 GB bench
    /// boxes kernel-OOM'd within a few executions). With the split,
    /// Σ(part partitions) stays within the session budget.
    #[tokio::test]
    async fn union_width_bounded_by_target_partitions() {
        let dir = tempfile::tempdir().unwrap();
        let pdir = dir.path().join("parts");
        std::fs::create_dir(&pdir).unwrap();
        for i in 0..3i64 {
            write_part_multi_rg(&pdir.join(format!("part-{i:04}.parquet")), i * 400, 4, 100);
        }
        let prov = EmatixFastParquetMultiTableProvider::try_new_dir(&pdir).unwrap();
        let config = datafusion::prelude::SessionConfig::new().with_target_partitions(8);
        let ctx = SessionContext::new_with_config(config);
        let plan = prov.scan(&ctx.state(), None, &[], None).await.unwrap();
        let pc = plan.output_partitioning().partition_count();
        assert!(
            pc <= 8,
            "union width must respect target_partitions=8 (3 parts × 4 RGs \
             used to expose 3 × min(4, 8) = 12), got {pc}"
        );
        assert!(pc >= 3, "each part still contributes a partition, got {pc}");
        // Correctness under the budget: full scan returns every row.
        ctx.register_table("t", Arc::new(prov)).unwrap();
        let out = run(&ctx, "SELECT count(*) c FROM t").await;
        assert!(out.contains("1200"), "3 parts × 400 rows; got {out}");
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
        let out = run(&ctx, "SELECT sum(val) FROM t WHERE id >= 2").await;
        assert!(
            out.contains("90"),
            "sum(val where id>=2)=20+30+40=90; got {out}"
        );
    }
}
