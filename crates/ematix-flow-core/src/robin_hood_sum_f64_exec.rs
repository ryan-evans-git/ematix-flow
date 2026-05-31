//! Σ.Q.L1b slice 2 — `SUM(f64) GROUP BY i64` operator + planner rule.
//!
//! Sister to [`crate::robin_hood_agg::RobinHoodAggregateExec`] (which
//! does `COUNT(*) GROUP BY i64`). Targets the Q18 SF=10 shape:
//! `SUM(l_quantity::Float64) GROUP BY l_orderkey::Int64` at 15M
//! cardinality, where the Σ.Q.0 profile pinned DataFusion's
//! FinalPartitioned `sum(f64)` aggregate as the dominant cost
//! (~364s elapsed_compute of ~700ms wall time).
//!
//! ## Plan shape detected
//!
//! ```text
//! AggregateExec(mode=FinalPartitioned,
//!               gby=[Column(col, idx)],         // Int64
//!               aggr=[sum(other_col)])          // Float64
//!   RepartitionExec(Hash([col]))                // optional
//!     AggregateExec(mode=Partial,
//!                   gby=[Column(col, idx)],
//!                   aggr=[sum(other_col)])
//!       <child>
//! ```
//!
//! ## NOT in the default optimizer chain
//!
//! Per [[optimizer-codegen-sensitivity]], adding any new
//! PhysicalOptimizerRule costs ~7% geomean. Ships **opt-in only** via
//! [`install_robin_hood_sum_f64_rule`].

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Float64Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::DataFusionError;
use datafusion::common::Result as DfResult;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use futures_util::stream::{self, TryStreamExt};

use crate::robin_hood_agg::RobinHoodSumF64Agg;

/// Σ.Q.L1b — execution mode mirroring `RobinHoodAggregateExec`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RobinHoodSumF64Mode {
    /// Per-input-partition `SUM(f64) GROUP BY i64`. Output column 1
    /// is `partial_sum: Float64`.
    Partial,
    /// Consumes partial sums from upstream `Partial` (after
    /// `RepartitionExec(Hash)`) and sums them per group key. Output
    /// column 1 is the final `sum: Float64`.
    FinalPartitioned,
}

/// Σ.Q.L1b — `SELECT k, SUM(v) FROM child GROUP BY k` operator where
/// `k: Int64` and `v: Float64`.
#[derive(Debug)]
pub struct RobinHoodSumF64Exec {
    input: Arc<dyn ExecutionPlan>,
    group_col_idx: usize,
    /// For Partial: index of the Float64 value column to sum.
    /// For FinalPartitioned: index of the Float64 partial_sum column
    /// produced by an upstream Partial.
    value_col_idx: usize,
    mode: RobinHoodSumF64Mode,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    /// Σ.Q.L1b retry — pre-computed init_cap derived from the input's
    /// `Statistics::num_rows`. Avoids the default-cap grow chain that
    /// cost +15.9% on Q18 SF=10 in slice 3. Sized at planner time so
    /// the hot path doesn't pay any stats-lookup cost.
    init_cap: usize,
}

impl RobinHoodSumF64Exec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        group_col_idx: usize,
        value_col_idx: usize,
        mode: RobinHoodSumF64Mode,
        group_out_name: String,
        sum_out_name: String,
    ) -> DfResult<Self> {
        let input_schema = input.schema();
        if group_col_idx >= input_schema.fields().len() {
            return Err(DataFusionError::Internal(format!(
                "RobinHoodSumF64Exec: group_col_idx={group_col_idx} out of bounds"
            )));
        }
        if value_col_idx >= input_schema.fields().len() {
            return Err(DataFusionError::Internal(format!(
                "RobinHoodSumF64Exec: value_col_idx={value_col_idx} out of bounds"
            )));
        }
        let gb_type = input_schema.field(group_col_idx).data_type();
        if gb_type != &DataType::Int64 {
            return Err(DataFusionError::Internal(format!(
                "RobinHoodSumF64Exec: group column must be Int64, got {gb_type:?}"
            )));
        }
        let v_type = input_schema.field(value_col_idx).data_type();
        if v_type != &DataType::Float64 {
            return Err(DataFusionError::Internal(format!(
                "RobinHoodSumF64Exec: value column must be Float64, got {v_type:?}"
            )));
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new(&group_out_name, DataType::Int64, false),
            Field::new(&sum_out_name, DataType::Float64, true),
        ]));
        let eq_props = EquivalenceProperties::new(schema.clone());
        let n_parts = input.output_partitioning().partition_count();
        let properties = Arc::new(PlanProperties::new(
            eq_props,
            Partitioning::UnknownPartitioning(n_parts),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        // Σ.Q.L1b retry — derive init_cap from upstream Statistics.
        // For Partial we see raw input rows; the # of groups is bounded
        // by the row count and typically smaller (4× ratio is a good
        // heuristic for Q18-shape SUM aggs). For FinalPartitioned each
        // input row already represents one group, so init_cap ≈
        // row count. Distributed evenly across `n_parts`.
        let row_est = match input.partition_statistics(None) {
            Ok(stats) => match stats.num_rows {
                Precision::Exact(n) | Precision::Inexact(n) => n,
                _ => 0,
            },
            Err(_) => 0,
        };
        let n_parts_safe = n_parts.max(1);
        let per_partition_rows = row_est / n_parts_safe;
        let raw_cap = match mode {
            RobinHoodSumF64Mode::Partial => per_partition_rows / 4,
            RobinHoodSumF64Mode::FinalPartitioned => per_partition_rows,
        };
        // Bound below by the previous default and above by a cap that
        // guards against bad stats blowing memory. 32M buckets × 24 B =
        // ~768 MB per partition worst case.
        // Σ.AΩ Phase 2.6 (E.3): floor overridable via `EMAT_RH_INITIAL_CAP`.
        let min_init_cap: usize = std::env::var("EMAT_RH_INITIAL_CAP")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(65_536);
        const MAX_INIT_CAP: usize = 32 * 1024 * 1024;
        let init_cap = raw_cap.clamp(min_init_cap, MAX_INIT_CAP);
        Ok(Self {
            input,
            group_col_idx,
            value_col_idx,
            mode,
            schema,
            properties,
            init_cap,
        })
    }

    /// Σ.Q.L1b retry — the auto-sized initial table capacity, derived
    /// from the input's `Statistics::num_rows` at planning time. The
    /// `EMAT_RH_SUM_F64_INIT_CAP` env var still overrides this at
    /// `execute()` time for ad-hoc benchmarking.
    pub fn init_cap(&self) -> usize {
        self.init_cap
    }

    pub fn mode(&self) -> RobinHoodSumF64Mode {
        self.mode
    }
    pub fn group_col_idx(&self) -> usize {
        self.group_col_idx
    }
    pub fn value_col_idx(&self) -> usize {
        self.value_col_idx
    }
}

impl DisplayAs for RobinHoodSumF64Exec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mode_str = match self.mode {
            RobinHoodSumF64Mode::Partial => "Partial",
            RobinHoodSumF64Mode::FinalPartitioned => "FinalPartitioned",
        };
        write!(
            f,
            "RobinHoodSumF64Exec(mode={mode_str}, group_col_idx={}, value_col_idx={})",
            self.group_col_idx, self.value_col_idx
        )
    }
}

impl ExecutionPlan for RobinHoodSumF64Exec {
    fn name(&self) -> &str {
        "RobinHoodSumF64Exec"
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
            DataFusionError::Internal("RobinHoodSumF64Exec requires exactly 1 child".into())
        })?;
        let group_out_name = self.schema.field(0).name().clone();
        let sum_out_name = self.schema.field(1).name().clone();
        Ok(Arc::new(Self::try_new(
            new_input,
            self.group_col_idx,
            self.value_col_idx,
            self.mode,
            group_out_name,
            sum_out_name,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let input = self.input.clone();
        let group_col_idx = self.group_col_idx;
        let value_col_idx = self.value_col_idx;
        let schema = self.schema.clone();
        let schema_for_stream = schema.clone();
        // Σ.Q.L1b retry — planner-derived init_cap (no env lookup in
        // the hot path). Env override kept for ad-hoc bench A/B.
        let planner_cap = self.init_cap;
        // Σ.Q.L1b retry — vectorised vs scalar A/B toggle. Default ON
        // since the microbench shows +13% at 15M card pre-grown and
        // +65% at default-cap; set `EMAT_RH_SUM_F64_VEC=0` to compare.
        let use_vec = std::env::var("EMAT_RH_SUM_F64_VEC")
            .ok()
            .map(|s| s != "0")
            .unwrap_or(true);

        let fut = async move {
            let init_cap: usize = std::env::var("EMAT_RH_SUM_F64_INIT_CAP")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(planner_cap);
            let mut agg = RobinHoodSumF64Agg::with_capacity(init_cap);
            let mut s = input.execute(partition, context)?;
            let mut first_batch_seen = false;
            loop {
                let Some(batch) = s.try_next().await? else {
                    break;
                };
                let keys_arr = batch.column(group_col_idx);
                let keys = keys_arr
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "RobinHoodSumF64Exec: column {group_col_idx} not Int64Array"
                        ))
                    })?;
                let vals_arr = batch.column(value_col_idx);
                let vals = vals_arr
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "RobinHoodSumF64Exec: column {value_col_idx} not Float64Array"
                        ))
                    })?;
                if use_vec {
                    agg.ingest_batch_vectorised(keys, vals);
                } else {
                    agg.ingest_batch(keys, vals);
                }
                if !first_batch_seen {
                    first_batch_seen = true;
                    let len = agg.table().len();
                    let cap = agg.table().capacity();
                    if len * 2 > cap {
                        agg.table_mut().reserve_to_capacity_pow2_of(len * 8);
                    }
                }
            }
            let n = agg.table().len();
            let mut keys: Vec<i64> = Vec::with_capacity(n);
            let mut sums: Vec<f64> = Vec::with_capacity(n);
            for (k, v) in agg.table().iter() {
                keys.push(k);
                sums.push(v);
            }
            let out = RecordBatch::try_new(
                schema_for_stream.clone(),
                vec![
                    Arc::new(Int64Array::from(keys)),
                    Arc::new(Float64Array::from(sums)),
                ],
            )
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            DfResult::Ok(out)
        };

        let s = stream::once(fut);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, s)))
    }
}

// ---------------------------------------------------------------------
// Σ.Q.L1b planner rule.
// ---------------------------------------------------------------------

pub fn install_robin_hood_sum_f64_rule(builder: SessionStateBuilder) -> SessionStateBuilder {
    builder.with_physical_optimizer_rule(Arc::new(EnableRobinHoodSumF64Rule::default()))
}

/// REV.10 (2026-05-30): cardinality-gated. The single-table RobinHood agg
/// WINS at low/mid cardinality (1-5% over stock at 10K-2M groups,
/// [[sigma-nf3-beats-stock]]) but THRASHES catastrophically once groups
/// vastly exceed the largest pre-sized table (`MAX_INIT_CAP` = 32M):
/// Q18 SF=100's 150M-group `SUM(l_quantity) GROUP BY l_orderkey` subquery
/// ran **12× SLOWER** than DataFusion's stock vectorised two-phase
/// (7963 ms vs 646 ms). The rule now refuses the rewrite when estimated
/// groups exceed `max_groups`, leaving very-high-card aggs to stock.
#[derive(Debug)]
pub struct EnableRobinHoodSumF64Rule {
    pub max_groups: usize,
    /// REV.18d lower bound: below this est_groups the row-at-a-time kernel
    /// loses to stock (gate-calibration sweep), so the rule no-ops and leaves
    /// the agg to DataFusion. See [`DEFAULT_RH_SUM_F64_MIN_GROUPS`].
    pub min_groups: usize,
}

/// REV.18 (2026-05-31): tightened from 32M → 256K. The 32M default only
/// lowered the gate enough to fix Q18 SF=100 (150M groups); it still let the
/// kernel fire on Q18 SF=1/10 (est 1.5M/15M groups), where it is ~12× SLOWER
/// than DataFusion's stock vectorised `AggregateExec` (SF=1: 27 vs 2.2 ms;
/// SF=10: 207 vs 17 ms — measured). Root cause is NOT a bug: the kernel is a
/// correct Robin Hood table (splitmix64 hash, 70%-load, auto-sized) but a
/// ROW-AT-A-TIME open-addressing agg structurally can't match a vectorised
/// columnar group-by at scale (the Σ.N.f lesson — microbench wins are vs
/// hashbrown, not vs DataFusion's operator). The operator crossover sweep put
/// any win regime at best in a narrow ~100–250K band; above that it always
/// loses. 256K leaves the only TPC-H trigger (Q18, ≥1.5M groups) to stock,
/// recovering −91% on Q18 SF=10 with no other query affected (Q18 is the sole
/// `SUM(f64) GROUP BY i64` shape). Candidate for full opt-in demotion later;
/// 256K is the conservative tightening. Env `EMAT_RH_SUM_F64_MAX_GROUPS`.
pub const DEFAULT_RH_SUM_F64_MAX_GROUPS: usize = 256 * 1024;

/// REV.18d (2026-05-31) lower bound, gate-calibration measured. At ~4
/// rows/group (so est_groups ≈ actual groups, which is what the `rows/4` gate
/// assumes), the RobinHoodSumF64 operator has NO win band in the firing
/// regime — it loses 1.0–2.0× below ~128K est_groups and only reaches ~tie
/// above. Its earlier "win" needed ~30 rows/group / millions of rows, which
/// the `est = rows/4` gate can't reach (it only fires at ≤~1.05M rows). So the
/// lower bound is harm-reduction: refuse the rewrite on small SUM aggs where
/// the kernel loses. Env `EMAT_RH_SUM_F64_MIN_GROUPS`.
pub const DEFAULT_RH_SUM_F64_MIN_GROUPS: usize = 128 * 1024;

impl Default for EnableRobinHoodSumF64Rule {
    fn default() -> Self {
        let max_groups = std::env::var("EMAT_RH_SUM_F64_MAX_GROUPS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_RH_SUM_F64_MAX_GROUPS);
        let min_groups = std::env::var("EMAT_RH_SUM_F64_MIN_GROUPS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_RH_SUM_F64_MIN_GROUPS);
        Self {
            max_groups,
            min_groups,
        }
    }
}

impl PhysicalOptimizerRule for EnableRobinHoodSumF64Rule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let max_groups = self.max_groups;
        let min_groups = self.min_groups;
        let result = plan.transform_up(|node| {
            if let Some(agg) = node.as_any().downcast_ref::<AggregateExec>() {
                if matches!(agg.mode(), AggregateMode::Partial) {
                    if let Some(m) = match_partial(agg) {
                        let input = agg.input().clone();
                        let in_schema = input.schema();
                        if in_schema.field(m.group_col_idx).data_type() == &DataType::Int64
                            && in_schema.field(m.value_col_idx).data_type() == &DataType::Float64
                        {
                            // REV.10 cardinality gate. Estimate groups from
                            // input rows (Partial sees raw rows; ~4 rows/group
                            // for the Q18-shape SUM agg, mirroring the
                            // init_cap heuristic). If groups would blow past
                            // the largest pre-sized table, the single-table
                            // RobinHood agg thrashes — leave it to stock.
                            let est_groups = match input.partition_statistics(None) {
                                Ok(s) => match s.num_rows {
                                    Precision::Exact(n) | Precision::Inexact(n) => n / 4,
                                    _ => 0,
                                },
                                Err(_) => 0,
                            };
                            if est_groups >= min_groups && est_groups <= max_groups {
                                let new = RobinHoodSumF64Exec::try_new(
                                    input,
                                    m.group_col_idx,
                                    m.value_col_idx,
                                    RobinHoodSumF64Mode::Partial,
                                    m.group_out_name,
                                    m.sum_out_name,
                                )?;
                                return Ok(Transformed::yes(
                                    Arc::new(new) as Arc<dyn ExecutionPlan>
                                ));
                            }
                        }
                    }
                }
                if matches!(
                    agg.mode(),
                    AggregateMode::Final | AggregateMode::FinalPartitioned
                ) {
                    if let Some(m) = match_final(agg) {
                        if find_robin_hood_sum_f64_partial(agg.input()).is_some() {
                            let new = RobinHoodSumF64Exec::try_new(
                                agg.input().clone(),
                                0,
                                1,
                                RobinHoodSumF64Mode::FinalPartitioned,
                                m.group_out_name,
                                m.sum_out_name,
                            )?;
                            return Ok(Transformed::yes(Arc::new(new) as Arc<dyn ExecutionPlan>));
                        }
                    }
                }
            }
            Ok(Transformed::no(node))
        })?;
        Ok(result.data)
    }

    fn name(&self) -> &str {
        "ematix_flow_enable_robin_hood_sum_f64"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

struct Matched {
    group_col_idx: usize,
    value_col_idx: usize,
    group_out_name: String,
    sum_out_name: String,
}

fn match_partial(agg: &AggregateExec) -> Option<Matched> {
    let groups = agg.group_expr().expr();
    if groups.len() != 1 {
        return None;
    }
    let (group_expr, group_out_name) = &groups[0];
    let col = group_expr.as_any().downcast_ref::<Column>()?;
    let group_col_idx = col.index();

    let aggs = agg.aggr_expr();
    if aggs.len() != 1 {
        return None;
    }
    let a = &aggs[0];
    if !a.fun().name().eq_ignore_ascii_case("sum") {
        return None;
    }
    // The SUM's input must be a single column reference.
    let exprs = a.expressions();
    if exprs.len() != 1 {
        return None;
    }
    let value_col = exprs[0].as_any().downcast_ref::<Column>()?;
    let value_col_idx = value_col.index();
    let sum_out_name = a.name().to_string();
    Some(Matched {
        group_col_idx,
        value_col_idx,
        group_out_name: group_out_name.clone(),
        sum_out_name,
    })
}

fn match_final(agg: &AggregateExec) -> Option<Matched> {
    // Same shape constraints as Partial. The col indices change at
    // FinalPartitioned (input is the Partial's output schema), so we
    // only use group_out_name + sum_out_name from this matcher; the
    // indices are hardcoded to 0, 1 in the rule body.
    let groups = agg.group_expr().expr();
    if groups.len() != 1 {
        return None;
    }
    let (group_expr, group_out_name) = &groups[0];
    let _col = group_expr.as_any().downcast_ref::<Column>()?;

    let aggs = agg.aggr_expr();
    if aggs.len() != 1 {
        return None;
    }
    let a = &aggs[0];
    if !a.fun().name().eq_ignore_ascii_case("sum") {
        return None;
    }
    let sum_out_name = a.name().to_string();
    Some(Matched {
        group_col_idx: 0,
        value_col_idx: 1,
        group_out_name: group_out_name.clone(),
        sum_out_name,
    })
}

fn find_robin_hood_sum_f64_partial(
    plan: &Arc<dyn ExecutionPlan>,
) -> Option<Arc<dyn ExecutionPlan>> {
    let mut cur = plan.clone();
    loop {
        if let Some(rh) = cur.as_any().downcast_ref::<RobinHoodSumF64Exec>() {
            if rh.mode() == RobinHoodSumF64Mode::Partial {
                return Some(cur);
            }
            return None;
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
    use arrow_array::{Float64Array, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn make_ctx_with_rule() -> SessionContext {
        let cfg = SessionConfig::new().with_target_partitions(4);
        // min_groups: 0 so the REV.18d lower bound doesn't suppress firing on
        // the tiny test tables — these tests exercise the kernel mechanics +
        // correctness; the lower bound itself is covered by a dedicated test.
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(cfg)
            .with_physical_optimizer_rule(Arc::new(EnableRobinHoodSumF64Rule {
                min_groups: 0,
                max_groups: usize::MAX,
            }))
            .build();
        SessionContext::new_with_state(state)
    }

    fn register_q18_shape_table(ctx: &SessionContext, name: &str) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Float64, false),
        ]));
        let schema_clone = schema.clone();
        let make_batch = move |ks: Vec<i64>, vs: Vec<f64>| {
            RecordBatch::try_new(
                schema_clone.clone(),
                vec![
                    Arc::new(Int64Array::from(ks)),
                    Arc::new(Float64Array::from(vs)),
                ],
            )
            .unwrap()
        };
        let partitions = vec![
            vec![make_batch(vec![1, 2, 3, 1], vec![1.0, 2.0, 3.0, 4.0])],
            vec![make_batch(vec![2, 1, 5, 5], vec![5.0, 6.0, 7.0, 8.0])],
            vec![make_batch(vec![2, 3, 3, 3], vec![9.0, 10.0, 11.0, 12.0])],
            vec![make_batch(vec![1, 1, 5, 5], vec![13.0, 14.0, 15.0, 16.0])],
        ];
        let mt = MemTable::try_new(schema, partitions).unwrap();
        ctx.register_table(name, Arc::new(mt)).unwrap();
    }

    #[tokio::test]
    async fn rule_fires_and_matches_stock_output() {
        let ctx = make_ctx_with_rule();
        register_q18_shape_table(&ctx, "t");
        let df = ctx
            .sql("SELECT k, SUM(v) FROM t GROUP BY k ORDER BY k")
            .await
            .unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let s = format!("{plan:?}");
        assert!(
            s.contains("RobinHoodSumF64Exec"),
            "plan didn't contain RobinHoodSumF64Exec — rule didn't fire. Got:\n{s}"
        );

        // Collect rh output.
        let rh_batches = df.collect().await.unwrap();
        let mut rh_pairs: Vec<(i64, f64)> = Vec::new();
        for b in &rh_batches {
            let ks = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let vs = b.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
            for i in 0..b.num_rows() {
                rh_pairs.push((ks.value(i), vs.value(i)));
            }
        }
        rh_pairs.sort_by_key(|(k, _)| *k);

        // Same query through stock DataFusion.
        let stock_ctx = SessionContext::new();
        register_q18_shape_table(&stock_ctx, "t");
        let df2 = stock_ctx
            .sql("SELECT k, SUM(v) FROM t GROUP BY k ORDER BY k")
            .await
            .unwrap();
        let stock_batches = df2.collect().await.unwrap();
        let mut stock_pairs: Vec<(i64, f64)> = Vec::new();
        for b in &stock_batches {
            let ks = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let vs = b.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
            for i in 0..b.num_rows() {
                stock_pairs.push((ks.value(i), vs.value(i)));
            }
        }
        stock_pairs.sort_by_key(|(k, _)| *k);

        assert_eq!(rh_pairs.len(), stock_pairs.len());
        for (a, b) in rh_pairs.iter().zip(stock_pairs.iter()) {
            assert_eq!(a.0, b.0);
            assert!(
                (a.1 - b.1).abs() < 1e-9,
                "sum mismatch for key {}: rh={} stock={}",
                a.0,
                a.1,
                b.1
            );
        }
    }

    #[tokio::test]
    async fn rule_no_op_on_count_agg() {
        let ctx = make_ctx_with_rule();
        register_q18_shape_table(&ctx, "t");
        let df = ctx
            .sql("SELECT k, COUNT(*) FROM t GROUP BY k")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let s = format!("{plan:?}");
        assert!(
            !s.contains("RobinHoodSumF64Exec"),
            "rule fired on COUNT — that's wrong. Got:\n{s}"
        );
    }

    #[tokio::test]
    async fn rule_no_op_on_multi_agg() {
        let ctx = make_ctx_with_rule();
        register_q18_shape_table(&ctx, "t");
        let df = ctx
            .sql("SELECT k, SUM(v), COUNT(*) FROM t GROUP BY k")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let s = format!("{plan:?}");
        assert!(
            !s.contains("RobinHoodSumF64Exec"),
            "rule fired on multi-agg — that's wrong. Got:\n{s}"
        );
    }

    #[tokio::test]
    async fn rule_gated_off_at_high_cardinality() {
        // REV.10: with max_groups=0, any non-empty input is "high card",
        // so the rule must REFUSE the rewrite (stock vectorised agg handles
        // very high cardinality far better — Q18 SF=100's 150M-group
        // subquery was 12× slower under RobinHood). Mirrors the REV.18
        // default 256K gate that leaves Q18's 1.5M+-group agg to stock.
        let cfg = SessionConfig::new().with_target_partitions(4);
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(cfg)
            .with_physical_optimizer_rule(Arc::new(EnableRobinHoodSumF64Rule {
                min_groups: 0,
                max_groups: 0,
            }))
            .build();
        let ctx = SessionContext::new_with_state(state);
        register_q18_shape_table(&ctx, "t");
        let df = ctx.sql("SELECT k, SUM(v) FROM t GROUP BY k").await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let s = format!("{plan:?}");
        assert!(
            !s.contains("RobinHoodSumF64Exec"),
            "rule fired despite max_groups=0 high-card gate — Got:\n{s}"
        );
    }

    #[tokio::test]
    async fn rule_fires_under_generous_gate() {
        // With a generous gate (16-row test table ≪ cap), the rule fires.
        let cfg = SessionConfig::new().with_target_partitions(4);
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(cfg)
            .with_physical_optimizer_rule(Arc::new(EnableRobinHoodSumF64Rule {
                min_groups: 0,
                max_groups: 1_000_000,
            }))
            .build();
        let ctx = SessionContext::new_with_state(state);
        register_q18_shape_table(&ctx, "t");
        let df = ctx.sql("SELECT k, SUM(v) FROM t GROUP BY k").await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let s = format!("{plan:?}");
        assert!(
            s.contains("RobinHoodSumF64Exec"),
            "rule didn't fire under a generous gate — Got:\n{s}"
        );
    }

    #[tokio::test]
    async fn rule_gated_off_below_min_groups() {
        // REV.18d lower bound: below min_groups est_groups the kernel loses to
        // stock, so the rule must REFUSE the rewrite. The 16-row Q18-shape
        // table has est_groups = 4 ≪ 1000, so with min_groups=1000 the plan
        // must keep DataFusion's stock AggregateExec.
        let cfg = SessionConfig::new().with_target_partitions(4);
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(cfg)
            .with_physical_optimizer_rule(Arc::new(EnableRobinHoodSumF64Rule {
                min_groups: 1_000,
                max_groups: usize::MAX,
            }))
            .build();
        let ctx = SessionContext::new_with_state(state);
        register_q18_shape_table(&ctx, "t");
        let df = ctx.sql("SELECT k, SUM(v) FROM t GROUP BY k").await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let s = format!("{plan:?}");
        assert!(
            !s.contains("RobinHoodSumF64Exec"),
            "rule fired below the min_groups lower bound — Got:\n{s}"
        );
    }

    #[test]
    fn rev18d_default_min_gate_is_set() {
        // REV.18d: production must get a non-zero lower bound (harm-reduction),
        // and it must sit below the upper bound so a fire band exists.
        assert!(DEFAULT_RH_SUM_F64_MIN_GROUPS > 0);
        assert!(DEFAULT_RH_SUM_F64_MIN_GROUPS < DEFAULT_RH_SUM_F64_MAX_GROUPS);
    }

    #[test]
    fn rev18_default_gate_excludes_q18_scale() {
        // REV.18: the default gate must leave Q18-scale aggs (est ≥ ~1.5M
        // groups already at SF=1) to stock — the row-at-a-time kernel is ~12×
        // slower than DataFusion's vectorised AggregateExec at that
        // cardinality (measured: Q18 SF=1 27 vs 2.2 ms, SF=10 207 vs 17 ms).
        // Guards against a silent revert to the old 32M default.
        assert!(
            DEFAULT_RH_SUM_F64_MAX_GROUPS < 1_500_000,
            "default RH-sum gate {DEFAULT_RH_SUM_F64_MAX_GROUPS} must exclude \
             Q18's ~1.5M-group SF=1 agg"
        );
    }
}
