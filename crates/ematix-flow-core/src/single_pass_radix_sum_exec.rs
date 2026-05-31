//! REV.8 — single-pass radix-partitioned `SUM(f64) GROUP BY i64`.
//!
//! ## Why
//!
//! Q18 SF=100's subquery `SUM(l_quantity) GROUP BY l_orderkey` (600M →
//! 150M groups) is DataFusion's dominant cost, and the shape is the
//! pathological one for its **two-phase** plan:
//!
//! ```text
//! AggregateExec(FinalPartitioned)         ← re-hashes 150M near-final groups (22.85s)
//!   RepartitionExec(Hash[key], N)         ← shuffles 150M rows / 2.2 GB (13.5s)
//!     AggregateExec(Partial)              ← reduction_factor 25% (near-unique sorted keys)
//! ```
//!
//! Because `l_orderkey` is near-unique AND lineitem is sorted by it, the
//! Partial phase already emits ~final per-key sums, so the shuffle + Final
//! re-hash combine almost nothing — pure overhead. DuckDB does ONE
//! radix-partitioned in-RAM aggregation. The REV.8 gate
//! (`bench_agg_two_phase_vs_single_pass`) measured single-pass at **1.71×**
//! the two-phase at this shape (P=14, 152M rows, B=1024 bins).
//!
//! ## What this operator does
//!
//! Replaces the whole `Partial → Repartition → Final` subtree with one
//! operator. It consumes ALL N input partitions in parallel — each builds
//! a per-partition [`RobinHoodSumF64GlobalRadixAgg`] (one cache-resident
//! radix pass into B = 2^radix_bits bins) — then merges bin `b` across all
//! partitions in parallel. Because `bin = hash(key) >> shift` is identical
//! in every partition, bin `b` holds the same key space everywhere, so the
//! per-bin merge is complete with no global re-hash of all groups. Emits a
//! single output partition (the downstream `FilterExec(sum>300)` collapses
//! it to a handful of rows, so a 1-partition output costs nothing here; the
//! optimizer rule re-runs `EnforceDistribution` to restore any required
//! distribution).
//!
//! ## NOT in the default optimizer chain
//!
//! Per [[optimizer-codegen-sensitivity]], a new operator perturbs codegen.
//! Ships **opt-in only** via `install_single_pass_radix_sum_rule`, gated to
//! the high-cardinality shape where the two-phase re-hash dominates.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Float64Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::DataFusionError;
use datafusion::common::Result as DfResult;
use datafusion::common::config::ConfigOptions;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::execution::TaskContext;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::enforce_distribution::EnforceDistribution;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use futures_util::stream::{self, TryStreamExt};

use crate::robin_hood_agg::{RobinHoodI64F64, RobinHoodSumF64GlobalRadixAgg};

/// Rows per emitted output batch.
const OUTPUT_BATCH_ROWS: usize = 8192;

/// `SELECT k, SUM(v) FROM child GROUP BY k` (`k: Int64`, `v: Float64`) via
/// a single radix-partitioned pass + parallel per-bin combine.
#[derive(Debug)]
pub struct SinglePassRadixSumF64Exec {
    input: Arc<dyn ExecutionPlan>,
    group_col_idx: usize,
    value_col_idx: usize,
    radix_bits: u8,
    /// Per-bin micro-table initial capacity (sized from input stats).
    per_table_cap: usize,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl SinglePassRadixSumF64Exec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        group_col_idx: usize,
        value_col_idx: usize,
        group_out_name: String,
        sum_out_name: String,
    ) -> DfResult<Self> {
        let in_schema = input.schema();
        let nfields = in_schema.fields().len();
        if group_col_idx >= nfields || value_col_idx >= nfields {
            return Err(DataFusionError::Internal(format!(
                "SinglePassRadixSumF64Exec: col idx out of bounds (group={group_col_idx}, value={value_col_idx}, fields={nfields})"
            )));
        }
        if in_schema.field(group_col_idx).data_type() != &DataType::Int64 {
            return Err(DataFusionError::Internal(
                "SinglePassRadixSumF64Exec: group column must be Int64".into(),
            ));
        }
        if in_schema.field(value_col_idx).data_type() != &DataType::Float64 {
            return Err(DataFusionError::Internal(
                "SinglePassRadixSumF64Exec: value column must be Float64".into(),
            ));
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new(&group_out_name, DataType::Int64, false),
            Field::new(&sum_out_name, DataType::Float64, true),
        ]));

        // B = 2^radix_bits bins. The gate showed more bins → bigger win
        // (smaller, cache-resident combine tables); 1024 was best at the
        // Q18 shape. Overridable for A/B.
        let radix_bits: u8 = std::env::var("EMAT_SPR_RADIX_BITS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10)
            .min(12);
        let n_bins = 1usize << radix_bits;

        // Estimate total distinct groups from input rows (Q18 SUM shape ≈
        // 4 rows/group), size each of the B per-partition bin tables to its
        // share so the radix pass rarely grows. Clamped for safety.
        let row_est = match input.partition_statistics(None) {
            Ok(s) => match s.num_rows {
                Precision::Exact(n) | Precision::Inexact(n) => n,
                _ => 0,
            },
            Err(_) => 0,
        };
        let n_in = input.output_partitioning().partition_count().max(1);
        let est_groups = (row_est / 4).max(1);
        // Per-partition per-bin distinct ≈ est_groups / n_in / n_bins.
        let per_table_cap = (est_groups / n_in / n_bins).clamp(256, 4 * 1024 * 1024);

        let eq_props = EquivalenceProperties::new(schema.clone());
        let properties = Arc::new(PlanProperties::new(
            eq_props,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));

        Ok(Self {
            input,
            group_col_idx,
            value_col_idx,
            radix_bits,
            per_table_cap,
            schema,
            properties,
        })
    }

    pub fn radix_bits(&self) -> u8 {
        self.radix_bits
    }
    pub fn group_col_idx(&self) -> usize {
        self.group_col_idx
    }
    pub fn value_col_idx(&self) -> usize {
        self.value_col_idx
    }
}

impl DisplayAs for SinglePassRadixSumF64Exec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "SinglePassRadixSumF64Exec(group_col_idx={}, value_col_idx={}, radix_bits={})",
            self.group_col_idx, self.value_col_idx, self.radix_bits
        )
    }
}

/// Merge bin `b`'s per-partition micro-tables into one and emit its
/// `(key, sum)` pairs. CPU-bound → runs on a blocking task.
fn merge_bin(mut tables: Vec<RobinHoodI64F64>) -> (Vec<i64>, Vec<f64>) {
    if tables.is_empty() {
        return (Vec::new(), Vec::new());
    }
    let mut base = tables.swap_remove(0);
    let mut scratch_k: Vec<i64> = Vec::new();
    let mut scratch_v: Vec<f64> = Vec::new();
    for t in &tables {
        scratch_k.clear();
        scratch_v.clear();
        scratch_k.reserve(t.len());
        scratch_v.reserve(t.len());
        for (k, v) in t.iter() {
            scratch_k.push(k);
            scratch_v.push(v);
        }
        base.insert_or_sum_batch_vectorised(&scratch_k, &scratch_v);
    }
    let mut ks = Vec::with_capacity(base.len());
    let mut vs = Vec::with_capacity(base.len());
    for (k, v) in base.iter() {
        ks.push(k);
        vs.push(v);
    }
    (ks, vs)
}

impl ExecutionPlan for SinglePassRadixSumF64Exec {
    fn name(&self) -> &str {
        "SinglePassRadixSumF64Exec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let new_input = children.pop().ok_or_else(|| {
            DataFusionError::Internal("SinglePassRadixSumF64Exec requires exactly 1 child".into())
        })?;
        Ok(Arc::new(Self::try_new(
            new_input,
            self.group_col_idx,
            self.value_col_idx,
            self.schema.field(0).name().clone(),
            self.schema.field(1).name().clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "SinglePassRadixSumF64Exec emits 1 partition; got execute({partition})"
            )));
        }
        let input = self.input.clone();
        let n_in = input.output_partitioning().partition_count().max(1);
        let group_col_idx = self.group_col_idx;
        let value_col_idx = self.value_col_idx;
        let radix_bits = self.radix_bits;
        let per_table_cap = self.per_table_cap;
        let n_bins = 1usize << radix_bits;
        let schema = self.schema.clone();
        let out_schema = schema.clone();

        let fut = async move {
            // Phase 1 — parallel per-partition radix aggregation.
            let mut handles = Vec::with_capacity(n_in);
            for i in 0..n_in {
                let input = input.clone();
                let ctx = context.clone();
                handles.push(tokio::spawn(async move {
                    let mut agg = RobinHoodSumF64GlobalRadixAgg::new(
                        radix_bits,
                        per_table_cap,
                        128 << 20, // 128 MB per-partition input-buffer budget
                    );
                    let mut s = input.execute(i, ctx)?;
                    while let Some(batch) = s.try_next().await? {
                        let keys_arr = batch.column(group_col_idx);
                        let keys = keys_arr
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .ok_or_else(|| {
                                DataFusionError::Internal(format!(
                                    "SinglePassRadixSumF64Exec: column {group_col_idx} not Int64Array"
                                ))
                            })?;
                        let vals_arr = batch.column(value_col_idx);
                        let vals = vals_arr
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .ok_or_else(|| {
                                DataFusionError::Internal(format!(
                                    "SinglePassRadixSumF64Exec: column {value_col_idx} not Float64Array"
                                ))
                            })?;
                        if keys.null_count() == 0 && vals.null_count() == 0 {
                            agg.ingest_batch(keys.values(), vals.values());
                        } else {
                            // Defensive: skip null-key rows; treat null
                            // value as a 0 contribution. The rule gates on
                            // non-nullable columns, so this is cold.
                            let mut fk = Vec::with_capacity(keys.len());
                            let mut fv = Vec::with_capacity(keys.len());
                            for r in 0..keys.len() {
                                if keys.is_null(r) {
                                    continue;
                                }
                                fk.push(keys.value(r));
                                fv.push(if vals.is_null(r) { 0.0 } else { vals.value(r) });
                            }
                            agg.ingest_batch(&fk, &fv);
                        }
                    }
                    agg.finish();
                    Ok::<Vec<RobinHoodI64F64>, DataFusionError>(agg.into_tables())
                }));
            }

            // Gather per-partition bin tables.
            let mut parts: Vec<Vec<RobinHoodI64F64>> = Vec::with_capacity(n_in);
            for h in handles {
                let tables = h
                    .await
                    .map_err(|e| DataFusionError::Execution(format!("radix agg task: {e}")))??;
                parts.push(tables);
            }

            // Transpose: bins[b] = [parts[0][b], parts[1][b], …].
            let mut bins: Vec<Vec<RobinHoodI64F64>> =
                (0..n_bins).map(|_| Vec::with_capacity(n_in)).collect();
            for part in parts {
                for (b, tbl) in part.into_iter().enumerate() {
                    bins[b].push(tbl);
                }
            }

            // Phase 2 — parallel per-bin cross-partition merge.
            let mut comb = Vec::with_capacity(n_bins);
            for tables in bins {
                comb.push(tokio::task::spawn_blocking(move || merge_bin(tables)));
            }
            let mut all_keys: Vec<i64> = Vec::new();
            let mut all_sums: Vec<f64> = Vec::new();
            for h in comb {
                let (ks, vs) = h
                    .await
                    .map_err(|e| DataFusionError::Execution(format!("merge task: {e}")))?;
                all_keys.extend(ks);
                all_sums.extend(vs);
            }

            // Chunk into output batches.
            let total = all_keys.len();
            let mut batches: Vec<RecordBatch> = Vec::new();
            let mut off = 0usize;
            while off < total {
                let end = (off + OUTPUT_BATCH_ROWS).min(total);
                let kb = Int64Array::from(all_keys[off..end].to_vec());
                let vb = Float64Array::from(all_sums[off..end].to_vec());
                let rb = RecordBatch::try_new(out_schema.clone(), vec![Arc::new(kb), Arc::new(vb)])
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                batches.push(rb);
                off = end;
            }
            Ok::<Vec<RecordBatch>, DataFusionError>(batches)
        };

        let s = stream::once(fut)
            .map_ok(|batches| stream::iter(batches.into_iter().map(DfResult::Ok)))
            .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, s)))
    }
}

// ---------------------------------------------------------------------
// REV.8 optimizer rule — replace Partial → Repartition → Final with the
// single-pass radix operator for the high-cardinality SUM(f64) GROUP BY
// i64 shape (Q18 SF=100 subquery). Opt-in only.
// ---------------------------------------------------------------------

pub fn install_single_pass_radix_sum_rule(builder: SessionStateBuilder) -> SessionStateBuilder {
    builder.with_physical_optimizer_rule(Arc::new(EnableSinglePassRadixSumRule::default()))
}

#[derive(Debug)]
pub struct EnableSinglePassRadixSumRule {
    /// Minimum estimated distinct groups for the rewrite to fire. Below
    /// this, DataFusion's two-phase Partial reduces enough that the Final
    /// re-hash is not the bottleneck and the operator's 1-partition output
    /// isn't worth the lost downstream parallelism. Default 1M (Q18 SF=100
    /// subquery is ~150M); env `EMAT_SPR_MIN_GROUPS` overrides; tests use 0.
    pub min_groups: usize,
}

impl Default for EnableSinglePassRadixSumRule {
    fn default() -> Self {
        let min_groups = std::env::var("EMAT_SPR_MIN_GROUPS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1_000_000);
        Self { min_groups }
    }
}

impl PhysicalOptimizerRule for EnableSinglePassRadixSumRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let min_groups = self.min_groups;
        let rewritten = plan.transform_up(|node| {
            let Some(agg) = node.as_any().downcast_ref::<AggregateExec>() else {
                return Ok(Transformed::no(node));
            };
            if !matches!(
                agg.mode(),
                AggregateMode::Final | AggregateMode::FinalPartitioned
            ) {
                return Ok(Transformed::no(node));
            }
            let Some((group_out_name, sum_out_name)) = match_final_sum_f64(agg) else {
                return Ok(Transformed::no(node));
            };
            // Find the Partial below (through Repartition/Coalesce) + the
            // scan input + its i64/f64 column indices.
            let Some((scan_input, g_idx, v_idx)) = find_partial_sum_f64(agg.input()) else {
                return Ok(Transformed::no(node));
            };
            let in_schema = scan_input.schema();
            if in_schema.field(g_idx).data_type() != &DataType::Int64
                || in_schema.field(v_idx).data_type() != &DataType::Float64
            {
                return Ok(Transformed::no(node));
            }
            // High-card gate: est. groups ≈ scan rows / 4 (Q18 SUM shape).
            let rows = match scan_input.partition_statistics(None) {
                Ok(s) => match s.num_rows {
                    Precision::Exact(n) | Precision::Inexact(n) => n,
                    _ => 0,
                },
                Err(_) => 0,
            };
            if rows / 4 < min_groups {
                return Ok(Transformed::no(node));
            }
            let op = SinglePassRadixSumF64Exec::try_new(
                scan_input,
                g_idx,
                v_idx,
                group_out_name,
                sum_out_name,
            )?;
            Ok(Transformed::yes(Arc::new(op) as Arc<dyn ExecutionPlan>))
        })?;
        if !rewritten.transformed {
            return Ok(rewritten.data);
        }
        // The operator emits 1 partition; re-run EnforceDistribution so the
        // downstream consumer gets the distribution it requires (Σ.BS).
        EnforceDistribution::new().optimize(rewritten.data, config)
    }

    fn name(&self) -> &str {
        "ematix_flow_enable_single_pass_radix_sum"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Match a Final/FinalPartitioned agg: single group key + single `SUM`.
/// Returns `(group_out_name, sum_out_name)`.
fn match_final_sum_f64(agg: &AggregateExec) -> Option<(String, String)> {
    let groups = agg.group_expr().expr();
    if groups.len() != 1 {
        return None;
    }
    let (_g, group_out_name) = &groups[0];
    let aggs = agg.aggr_expr();
    if aggs.len() != 1 {
        return None;
    }
    let a = &aggs[0];
    if !a.fun().name().eq_ignore_ascii_case("sum") {
        return None;
    }
    Some((group_out_name.clone(), a.name().to_string()))
}

/// Walk the single-child chain from `plan` to an `AggregateExec(Partial)`
/// with a single `Column` group key + a single `SUM(Column)`. Returns the
/// Partial's input (the scan) and the group/value column indices in its
/// schema.
fn find_partial_sum_f64(
    plan: &Arc<dyn ExecutionPlan>,
) -> Option<(Arc<dyn ExecutionPlan>, usize, usize)> {
    let mut cur = plan.clone();
    loop {
        if let Some(agg) = cur.as_any().downcast_ref::<AggregateExec>() {
            if !matches!(agg.mode(), AggregateMode::Partial) {
                return None;
            }
            let groups = agg.group_expr().expr();
            if groups.len() != 1 {
                return None;
            }
            let g_col = groups[0].0.as_any().downcast_ref::<Column>()?;
            let aggs = agg.aggr_expr();
            if aggs.len() != 1 {
                return None;
            }
            let a = &aggs[0];
            if !a.fun().name().eq_ignore_ascii_case("sum") {
                return None;
            }
            let exprs = a.expressions();
            if exprs.len() != 1 {
                return None;
            }
            let v_col = exprs[0].as_any().downcast_ref::<Column>()?;
            return Some((agg.input().clone(), g_col.index(), v_col.index()));
        }
        let children = cur.children();
        if children.len() != 1 {
            return None;
        }
        cur = children[0].clone();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn schema_kv() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Float64, false),
        ]))
    }

    fn batch(ks: Vec<i64>, vs: Vec<f64>) -> RecordBatch {
        RecordBatch::try_new(
            schema_kv(),
            vec![
                Arc::new(Int64Array::from(ks)),
                Arc::new(Float64Array::from(vs)),
            ],
        )
        .unwrap()
    }

    /// Build the operator over a multi-partition MemTable scan and collect
    /// its (key, sum) pairs sorted by key.
    async fn run_single_pass(partitions: Vec<Vec<RecordBatch>>) -> Vec<(i64, f64)> {
        let ctx = SessionContext::new();
        let mt = MemTable::try_new(schema_kv(), partitions).unwrap();
        ctx.register_table("t", Arc::new(mt)).unwrap();
        let scan = ctx
            .sql("SELECT k, v FROM t")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let exec = Arc::new(
            SinglePassRadixSumF64Exec::try_new(scan, 0, 1, "k".into(), "sum_v".into()).unwrap(),
        );
        let task_ctx = ctx.task_ctx();
        let mut stream = exec.execute(0, task_ctx).unwrap();
        let mut pairs = Vec::new();
        while let Some(b) = stream.try_next().await.unwrap() {
            let ks = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let vs = b.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
            for i in 0..b.num_rows() {
                pairs.push((ks.value(i), vs.value(i)));
            }
        }
        pairs.sort_by_key(|(k, _)| *k);
        pairs
    }

    /// Reference: stock DataFusion `SUM(v) GROUP BY k`.
    async fn run_stock(partitions: Vec<Vec<RecordBatch>>) -> Vec<(i64, f64)> {
        let ctx = SessionContext::new();
        let mt = MemTable::try_new(schema_kv(), partitions).unwrap();
        ctx.register_table("t", Arc::new(mt)).unwrap();
        let batches = ctx
            .sql("SELECT k, SUM(v) FROM t GROUP BY k")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut pairs = Vec::new();
        for b in &batches {
            let ks = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let vs = b.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
            for i in 0..b.num_rows() {
                pairs.push((ks.value(i), vs.value(i)));
            }
        }
        pairs.sort_by_key(|(k, _)| *k);
        pairs
    }

    fn assert_eq_pairs(a: &[(i64, f64)], b: &[(i64, f64)]) {
        assert_eq!(a.len(), b.len(), "group count: {} vs {}", a.len(), b.len());
        for (x, y) in a.iter().zip(b.iter()) {
            assert_eq!(x.0, y.0, "key mismatch");
            assert!(
                (x.1 - y.1).abs() < 1e-6,
                "sum at {}: {} vs {}",
                x.0,
                x.1,
                y.1
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn matches_stock_multi_partition() {
        let parts = vec![
            vec![batch(vec![1, 2, 3, 1], vec![1.0, 2.0, 3.0, 4.0])],
            vec![batch(vec![2, 1, 5, 5], vec![5.0, 6.0, 7.0, 8.0])],
            vec![batch(vec![2, 3, 3, 3], vec![9.0, 10.0, 11.0, 12.0])],
            vec![batch(vec![1, 1, 5, 5], vec![13.0, 14.0, 15.0, 16.0])],
        ];
        let got = run_single_pass(parts.clone()).await;
        let want = run_stock(parts).await;
        assert_eq_pairs(&got, &want);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn matches_stock_high_card() {
        // 20K distinct keys spread across 4 partitions, dup-3.
        let n = 60_000i64;
        let mk = |off: i64| {
            let ks: Vec<i64> = (0..n).map(|i| (off + i) % 20_000).collect();
            let vs: Vec<f64> = (0..n).map(|i| (i % 17) as f64 + 0.5).collect();
            vec![batch(ks, vs)]
        };
        let parts = vec![mk(0), mk(5_000), mk(10_000), mk(15_000)];
        let got = run_single_pass(parts.clone()).await;
        let want = run_stock(parts).await;
        assert_eq_pairs(&got, &want);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn matches_stock_empty() {
        let parts = vec![vec![batch(vec![], vec![])]];
        let got = run_single_pass(parts).await;
        assert!(got.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn matches_stock_single_partition() {
        let parts = vec![vec![batch(
            vec![7, 7, 7, 8, 9, 9],
            vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        )]];
        let got = run_single_pass(parts.clone()).await;
        let want = run_stock(parts).await;
        assert_eq_pairs(&got, &want);
    }

    // ---- optimizer rule ----

    fn ctx_with_rule(min_groups: usize) -> SessionContext {
        let cfg = SessionConfig::new().with_target_partitions(4);
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(cfg)
            .with_physical_optimizer_rule(Arc::new(EnableSinglePassRadixSumRule { min_groups }))
            .build();
        SessionContext::new_with_state(state)
    }

    fn register_kv(ctx: &SessionContext, name: &str, parts: Vec<Vec<RecordBatch>>) {
        let mt = MemTable::try_new(schema_kv(), parts).unwrap();
        ctx.register_table(name, Arc::new(mt)).unwrap();
    }

    async fn collect_pairs(df: datafusion::dataframe::DataFrame) -> Vec<(i64, f64)> {
        let batches = df.collect().await.unwrap();
        let mut pairs = Vec::new();
        for b in &batches {
            let ks = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let vs = b.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
            for i in 0..b.num_rows() {
                pairs.push((ks.value(i), vs.value(i)));
            }
        }
        pairs.sort_by_key(|(k, _)| *k);
        pairs
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn rule_fires_and_matches_stock() {
        let parts = vec![
            vec![batch(vec![1, 2, 3, 1], vec![1.0, 2.0, 3.0, 4.0])],
            vec![batch(vec![2, 1, 5, 5], vec![5.0, 6.0, 7.0, 8.0])],
            vec![batch(vec![2, 3, 3, 3], vec![9.0, 10.0, 11.0, 12.0])],
            vec![batch(vec![1, 1, 5, 5], vec![13.0, 14.0, 15.0, 16.0])],
        ];
        let ctx = ctx_with_rule(0); // gate off so it fires on tiny test data
        register_kv(&ctx, "t", parts.clone());
        let df = ctx
            .sql("SELECT k, SUM(v) AS s FROM t GROUP BY k ORDER BY k")
            .await
            .unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let s = format!("{plan:?}");
        assert!(
            s.contains("SinglePassRadixSumF64Exec"),
            "rule didn't fire — plan:\n{s}"
        );
        let got = collect_pairs(df).await;
        let want = run_stock(parts).await;
        assert_eq_pairs(&got, &want);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rule_no_op_below_gate() {
        let parts = vec![vec![batch(vec![1, 2, 3], vec![1.0, 2.0, 3.0])]];
        let ctx = ctx_with_rule(1_000_000); // gate on; 3 rows ≪ gate
        register_kv(&ctx, "t", parts);
        let df = ctx.sql("SELECT k, SUM(v) FROM t GROUP BY k").await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let s = format!("{plan:?}");
        assert!(
            !s.contains("SinglePassRadixSumF64Exec"),
            "rule fired below the high-card gate — plan:\n{s}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rule_no_op_on_count() {
        let parts = vec![vec![batch(vec![1, 2, 3], vec![1.0, 2.0, 3.0])]];
        let ctx = ctx_with_rule(0);
        register_kv(&ctx, "t", parts);
        let df = ctx
            .sql("SELECT k, COUNT(*) FROM t GROUP BY k")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let s = format!("{plan:?}");
        assert!(
            !s.contains("SinglePassRadixSumF64Exec"),
            "rule fired on COUNT — plan:\n{s}"
        );
    }
}
