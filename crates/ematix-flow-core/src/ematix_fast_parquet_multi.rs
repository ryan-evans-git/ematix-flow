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
//! [`EmatixFastParquetTableProvider`] per part and unions their scans via
//! [`EmatixInterleaveUnionExec`] (width-pinned — see Σ.MW.2 on that type).
//! Each part is decoded by the ematix codec, and the parts run as independent
//! partitions across cores — the scan parallelism a single huge file is starved
//! of (e.g. the SF100 Q09 penalty on one 22 GB `lineitem`).
//!
//! It is also the local building block the distributed Iceberg plan lowers onto
//! (`crate::iceberg_scan`): "these N surviving files are one table."

use std::any::Any;
use std::fmt;
use std::path::Path;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Statistics;
use datafusion::common::stats::Precision;
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};

use crate::ematix_fast_parquet::EmatixFastParquetTableProvider;

/// Union-semantics N-ary node whose children are PINNED at their own
/// partition counts (Σ.MW.2).
///
/// The stock [`UnionExec`](datafusion::physical_plan::union::UnionExec)
/// reports `benefits_from_input_partitioning() == true` for every
/// child, so `EnforceDistribution` wraps EACH part's scan in its own
/// `RepartitionExec(RoundRobinBatch(target))` — 8 parts × target 14 =
/// 112 concurrent decode streams on SF100 lineitem, re-introducing
/// above the provider the exact width amplification Σ.MW.1 removed
/// inside it (observed: parted single-node Q07 8.9×, Q18 6.1× slower
/// than flat; setting per-part budgets can't help because the rule
/// expands every union child to `target` regardless of the union's
/// total). This node is the same concatenation — children stay fully
/// visible to every downcast-driven rule (runtime blooms, clustered
/// agg, Σ.JS grounding) — but answers `false`, so the planner takes
/// the union's own width as final and parallelism is provided by the
/// provider's ceil-division part sizing instead.
#[derive(Debug)]
pub struct EmatixInterleaveUnionExec {
    children: Vec<Arc<dyn ExecutionPlan>>,
    /// Prefix map: output partition `k` executes
    /// `children[map[k].0].execute(map[k].1)`.
    map: Vec<(usize, usize)>,
    /// RANGE.AGG Stage-2 claim (see `try_new_claiming_hash`).
    hash_claim: Option<Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>>,
    props: Arc<PlanProperties>,
}

impl EmatixInterleaveUnionExec {
    /// All children must share the first child's schema. Empty input
    /// is an error.
    pub fn try_new(children: Vec<Arc<dyn ExecutionPlan>>) -> DfResult<Self> {
        Self::try_new_inner(children, None)
    }

    /// Like [`Self::try_new`], but the node CLAIMS
    /// `Partitioning::Hash(exprs, total)` — the RANGE.AGG Stage-2
    /// trick, lifted to parted scans. The caller must have proven that
    /// every distinct key lands in exactly one output partition
    /// (strict-gap chunking within each part AND strict key gaps
    /// between parts); the claim then lets `EnforceDistribution`
    /// accept this node as a SinglePartitioned agg's input without
    /// re-inserting a full-input hash shuffle. Callers must cap the
    /// claim above the agg with `PartitionClaimResetExec`, exactly as
    /// the single-file path does.
    pub fn try_new_claiming_hash(
        children: Vec<Arc<dyn ExecutionPlan>>,
        hash_exprs: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    ) -> DfResult<Self> {
        Self::try_new_inner(children, Some(hash_exprs))
    }

    fn try_new_inner(
        children: Vec<Arc<dyn ExecutionPlan>>,
        hash_claim: Option<Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>>,
    ) -> DfResult<Self> {
        let Some(first) = children.first() else {
            return Err(DataFusionError::Plan(
                "EmatixInterleaveUnionExec: no children".into(),
            ));
        };
        let schema = first.schema();
        let mut map = Vec::new();
        for (ci, c) in children.iter().enumerate() {
            if c.schema().fields() != schema.fields() {
                return Err(DataFusionError::Plan(format!(
                    "EmatixInterleaveUnionExec: child {ci} schema mismatch"
                )));
            }
            for p in 0..c.output_partitioning().partition_count() {
                map.push((ci, p));
            }
        }
        let partitioning = match &hash_claim {
            Some(exprs) => Partitioning::Hash(exprs.clone(), map.len().max(1)),
            None => Partitioning::UnknownPartitioning(map.len().max(1)),
        };
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            children,
            map,
            hash_claim,
            props,
        })
    }
}

impl DisplayAs for EmatixInterleaveUnionExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "EmatixInterleaveUnionExec: parts={}, partitions={} (width-pinned)",
            self.children.len(),
            self.map.len()
        )
    }
}

impl ExecutionPlan for EmatixInterleaveUnionExec {
    fn name(&self) -> &str {
        "EmatixInterleaveUnionExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.children.iter().collect()
    }
    /// The whole point of this node: no child "benefits" from an
    /// inserted round-robin — the provider already sized the parts.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false; self.children.len()]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::try_new_inner(
            children,
            self.hash_claim.clone(),
        )?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let Some(&(ci, cp)) = self.map.get(partition) else {
            return Err(DataFusionError::Internal(format!(
                "EmatixInterleaveUnionExec: partition {partition} out of range ({})",
                self.map.len()
            )));
        };
        self.children[ci].execute(cp, context)
    }
    /// Merged statistics across children (rows/nulls summed, min/max
    /// widened) — same semantics as the provider's `statistics()`, so
    /// planner rules that consult the plan see what the table-level
    /// stats promised.
    fn partition_statistics(&self, partition: Option<usize>) -> DfResult<Statistics> {
        match partition {
            Some(k) => {
                let Some(&(ci, cp)) = self.map.get(k) else {
                    return Err(DataFusionError::Internal(format!(
                        "EmatixInterleaveUnionExec: partition {k} out of range"
                    )));
                };
                self.children[ci].partition_statistics(Some(cp))
            }
            None => {
                let mut merged = self.children[0].partition_statistics(None)?;
                for c in &self.children[1..] {
                    let s = c.partition_statistics(None)?;
                    merged.num_rows = merged.num_rows.add(&s.num_rows);
                    merged.total_byte_size = merged.total_byte_size.add(&s.total_byte_size);
                    for (a, b) in merged
                        .column_statistics
                        .iter_mut()
                        .zip(s.column_statistics.iter())
                    {
                        a.null_count = a.null_count.add(&b.null_count);
                        a.min_value = a.min_value.min(&b.min_value);
                        a.max_value = a.max_value.max(&b.max_value);
                        a.sum_value = a.sum_value.add(&b.sum_value);
                        a.distinct_count = Precision::Absent;
                    }
                }
                Ok(merged)
            }
        }
    }
}

/// A table backed by many Parquet part-files, each read via the ematix-parquet
/// fast reader. Single-node; the parts scan in parallel via
/// [`EmatixInterleaveUnionExec`].
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
    /// Build from an explicit, ordered list of part-file paths. Empty input is
    /// an error. Every part must share the first part's schema (fields); a
    /// genuine mismatch is an error.
    ///
    /// Σ.PK.1 (parted key-domain straddle): KEYS.2 narrows an INT64 join/group
    /// key to Int32 per file when THAT file's footer stats prove every value
    /// fits i32. For a range-partitioned dataset (`<table>-NNNN` split by key
    /// range), a key whose *global* domain crosses `i32::MAX` — e.g.
    /// `l_orderkey` at SF ≳ 350 — fits i32 in the low parts but not the high
    /// ones, so the parts derive DIFFERENT schemas (Int32 vs Int64) and cannot
    /// union. Narrowing safety must therefore be judged over the whole table,
    /// not per file: when the per-part decisions disagree, every part is rebuilt
    /// with narrowing OFF so they share the raw Int64 key schema. When every
    /// part agrees (SF100 and below), narrowing is kept — the common path is
    /// untouched.
    pub fn try_new_files(paths: Vec<String>) -> DfResult<Self> {
        // Resolve the KEYS.2 downcast decision ONCE, for the whole table.
        // Observe every part first so `scale_class` AUTO classifies the dataset
        // by its full membership (mirrors per-file `try_new`), then read the
        // gate — so all parts make the SAME decision rather than each its own.
        for p in &paths {
            crate::scale_class::observe_file(p);
        }
        Self::try_new_files_opt(paths, crate::ematix_fast_parquet::key_downcast_enabled())
    }

    /// Σ.Q15.LS — [`Self::try_new_files`] with the KEYS.2 downcast pinned
    /// OFF: the multi-file mirror of
    /// [`EmatixFastParquetTableProvider::try_new_no_downcast`]. The mesh
    /// gate's local-commit leaf swap must reproduce the STOCK parquet
    /// schema exactly (parents were planned against it), and stock never
    /// narrows Int64 keys.
    pub fn try_new_files_no_downcast(paths: Vec<String>) -> DfResult<Self> {
        Self::try_new_files_opt(paths, false)
    }

    /// [`Self::try_new_files`] with an explicit KEYS.2 downcast flag, so tests
    /// can drive the parted-straddle reconciliation deterministically without
    /// mutating the process-global `EMAT_DOWNCAST_KEYS` env (which races
    /// parallel tests). Production `try_new_files` passes the scale-gated
    /// `key_downcast_enabled()`.
    fn try_new_files_opt(paths: Vec<String>, downcast_keys: bool) -> DfResult<Self> {
        if paths.is_empty() {
            return Err(DataFusionError::Plan(
                "multi-file provider: no part files given".into(),
            ));
        }
        let build = |downcast: bool| -> DfResult<Vec<EmatixFastParquetTableProvider>> {
            paths
                .iter()
                .map(|p| EmatixFastParquetTableProvider::try_new_opt(p.clone(), downcast))
                .collect()
        };
        let mut parts = build(downcast_keys)?;
        // If per-part narrowing produced divergent schemas, the key domain
        // straddles i32 across parts (Σ.PK.1). Rebuild with narrowing off so
        // every part keeps its raw Int64 keys — a schema all parts share.
        let base = parts[0].schema();
        let consistent = parts
            .iter()
            .skip(1)
            .all(|part| part.schema().fields() == base.fields());
        if !consistent && downcast_keys {
            parts = build(false)?;
        }
        // Post-reconciliation, any remaining mismatch is a genuine schema
        // difference (not a narrowing artefact) and is an error.
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

    /// Σ.Q15.LS — synchronous core of `scan` (the multi-file mirror of
    /// [`EmatixFastParquetTableProvider::scan_exec`]): fan the SAME
    /// projection + filters into each part's sync scan, split the
    /// partition budget across parts (Σ.MW.1, ceil per Σ.MW.2), and
    /// union via the width-pinned [`EmatixInterleaveUnionExec`].
    /// Factored out so the mesh gate's local-commit leaf swap — a sync
    /// physical-rule context — can build the parted exec directly;
    /// `TableProvider::scan` below delegates here, so the two paths
    /// cannot drift.
    pub fn scan_exec(
        &self,
        target_partitions: usize,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let per_part = target_partitions.div_ceil(self.parts.len()).max(1);
        let mut children: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(self.parts.len());
        for part in &self.parts {
            children.push(part.scan_exec(
                target_partitions,
                projection,
                filters,
                Some(per_part),
            )?);
        }
        // A single part needs no union wrapper.
        if children.len() == 1 {
            return Ok(children.pop().expect("len checked"));
        }
        Ok(Arc::new(EmatixInterleaveUnionExec::try_new(children)?))
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

    /// Fans the SAME projection + filters into each part's scan (reusing all
    /// of the single-file provider's machinery: RG assignment, BridgeFilter
    /// pushdown, late-mat), then unions the per-part execs. Each child yields
    /// its own partitions, so the union runs the parts in parallel.
    ///
    /// Σ.MW.1: split the session's partition budget across parts instead of
    /// letting every part size itself to the full `target_partitions` — the
    /// union's width is what the operators above poll CONCURRENTLY, and
    /// parts × target_partitions multiplied the query's decode working set
    /// by the part count (13-part SF100 lineitem: ~182 streams, Q01 ~9 GB
    /// peak, kernel OOM on 32 GB boxes within a few executions).
    ///
    /// Σ.MW.2: CEIL division, so Σ(part partitions) lands in
    /// [target, target + parts) — never below target. Below-target
    /// width made `EnforceDistribution` "helpfully" wrap every part
    /// in RoundRobinBatch(target) (parts × target streams, the
    /// Σ.MW.1 bug re-introduced one level up); the width-pinned
    /// union refuses that, so the union's own width must carry the
    /// query's parallelism. A table with more parts than the budget
    /// still degrades to one partition per part.
    ///
    /// (All of the above lives in [`Self::scan_exec`]; `limit` is unused —
    /// the per-part scans ignore it, exactly as before the Σ.Q15.LS sync
    /// factoring.)
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        self.scan_exec(
            state.config_options().execution.target_partitions,
            projection,
            filters,
        )
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

    /// Like `write_part`, but the key column is named `xkey` so
    /// `is_downcast_key_name` (KEYS.2) treats it as a downcast candidate — the
    /// exact trigger for the Σ.PK.1 parted straddle. (Naming a fixture column
    /// `*key` is normally avoided precisely because it flips this narrowing on;
    /// here that IS the behaviour under test.)
    fn write_keyed_part(path: &Path, keys: &[i64], vals: &[i64]) {
        write_table_to_path(
            path,
            &[
                ("xkey", ColumnData::I64(keys)),
                ("val", ColumnData::I64(vals)),
            ],
            CompressionCodec::Uncompressed,
        )
        .expect("write parquet");
    }

    /// Σ.PK.1: range-partitioned parts whose INT64 key straddles `i32::MAX`
    /// (fits i32 in the low part, exceeds it in the high part) must still union.
    /// Per-part KEYS.2 narrowing would give part 0 an Int32 `xkey` and part 1 an
    /// Int64 `xkey` — divergent schemas that previously errored "part 1 schema
    /// differs from part 0". The provider must reconcile to one raw-Int64 schema
    /// and answer correctly (a value above `i32::MAX` must survive intact).
    #[tokio::test]
    async fn parted_key_straddling_i32_reconciles_and_unions() {
        let dir = tempfile::tempdir().unwrap();
        let lo: Vec<i64> = (1..=4).collect();
        let big = i32::MAX as i64 + 1; // 2_147_483_648 — first value past i32
        let hi: Vec<i64> = (big..big + 4).collect();
        let vlo: Vec<i64> = lo.iter().map(|k| k * 2).collect();
        let vhi: Vec<i64> = hi.iter().map(|k| k * 2).collect();
        let p0 = dir.path().join("t-0000.parquet");
        let p1 = dir.path().join("t-0001.parquet");
        write_keyed_part(&p0, &lo, &vlo);
        write_keyed_part(&p1, &hi, &vhi);
        let paths = vec![
            p0.to_string_lossy().into_owned(),
            p1.to_string_lossy().into_owned(),
        ];

        // Force KEYS.2 downcast ON, as SF1000 AUTO would. Before the fix this
        // panics at `try_new_files_opt` (part 1's Int64 xkey != part 0's Int32).
        let prov = EmatixFastParquetMultiTableProvider::try_new_files_opt(paths, true)
            .expect("straddling parts must reconcile to one schema, not error");

        // The reconciled key keeps its raw Int64 — never a lossy Int32 that
        // would truncate part 1's values above i32::MAX.
        let f = prov.schema().field_with_name("xkey").unwrap().clone();
        assert_eq!(
            f.data_type(),
            &arrow_schema::DataType::Int64,
            "straddling key must stay Int64, not narrow to a lossy Int32"
        );

        // And the union answers across both parts: 8 rows, and the max key
        // (above i32::MAX) round-trips without truncation.
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(prov)).unwrap();
        let out = run(&ctx, "SELECT count(*) c, max(xkey) m FROM t").await;
        assert!(
            out.contains(&(big + 3).to_string()),
            "max key above i32::MAX must survive intact, got:\n{out}"
        );
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
        // Σ.MW.2 ceil sizing: width ∈ [target, target + parts) — at
        // least target (below-target used to invite per-part
        // RoundRobin expansion), and never the parts × target blowup
        // (3 parts × 4 RGs used to expose 3 × min(4, 8) = 12).
        assert!(
            pc < 8 + 3,
            "union width must stay under target+parts, got {pc}"
        );
        assert!(
            pc >= 8,
            "union width must reach target_partitions=8, got {pc}"
        );
        // Correctness under the budget: full scan returns every row.
        ctx.register_table("t", Arc::new(prov)).unwrap();
        let out = run(&ctx, "SELECT count(*) c FROM t").await;
        assert!(out.contains("1200"), "3 parts × 400 rows; got {out}");
    }

    /// Σ.MW.2 regression oracle: a GROUP BY forces a hash exchange
    /// above the parted scan — exactly where `EnforceDistribution`
    /// used to wrap EVERY part in `RoundRobinBatch(target)` (8 parts
    /// × 14 = 112 decode streams on SF100 Q07). The width-pinned
    /// union must survive physical optimization with no RoundRobin
    /// under it and produce correct results.
    #[tokio::test]
    async fn interleave_union_defeats_roundrobin_expansion() {
        use datafusion::physical_plan::displayable;
        let dir = tempfile::tempdir().unwrap();
        let pdir = dir.path().join("parts");
        std::fs::create_dir(&pdir).unwrap();
        for i in 0..3i64 {
            write_part_multi_rg(&pdir.join(format!("part-{i:04}.parquet")), i * 400, 4, 100);
        }
        let prov = EmatixFastParquetMultiTableProvider::try_new_dir(&pdir).unwrap();
        let config = datafusion::prelude::SessionConfig::new().with_target_partitions(8);
        let ctx = SessionContext::new_with_config(config);
        ctx.register_table("t", Arc::new(prov)).unwrap();
        let df = ctx
            .sql("SELECT id % 7 AS g, count(*) AS c, sum(val) AS s FROM t GROUP BY id % 7")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let text = format!("{}", displayable(plan.as_ref()).indent(true));
        assert!(
            text.contains("EmatixInterleaveUnionExec"),
            "parted scan must plan through the width-pinned union:\n{text}"
        );
        assert!(
            !text.contains("RoundRobinBatch"),
            "no per-part RoundRobin expansion may survive (Σ.MW.2):\n{text}"
        );
        // Execution parity: the grouped totals cover every row once.
        let out = run(
            &ctx,
            "SELECT count(*) FROM (SELECT id % 7 g, count(*) c FROM t GROUP BY id % 7)",
        )
        .await;
        assert!(out.contains("7"), "7 groups expected; got {out}");
        let out = run(
            &ctx,
            "SELECT sum(c) FROM (SELECT id % 7 g, count(*) c FROM t GROUP BY id % 7)",
        )
        .await;
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
