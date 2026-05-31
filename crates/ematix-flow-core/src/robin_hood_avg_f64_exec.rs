//! Σ.R.2 — `AVG(f64) GROUP BY i64` operator + planner rule.
//!
//! Sister to [`crate::robin_hood_sum_f64_exec::RobinHoodSumF64Exec`].
//! Targets the Q17 SF=10 hot kernel: `AVG(l_quantity) GROUP BY
//! l_partkey` at ~2M cardinality, where the Σ.Q closeout profile
//! identified `GroupValuesPrimitive::intern` (DataFusion's i64 GROUP
//! BY hash) at 21.6% self time.
//!
//! ## Plan shape detected
//!
//! ```text
//! AggregateExec(mode=FinalPartitioned,
//!               gby=[Column(col, idx)],         // Int64
//!               aggr=[avg(other_col)])          // Float64
//!   RepartitionExec(Hash([col]))                // optional
//!     AggregateExec(mode=Partial,
//!                   gby=[Column(col, idx)],
//!                   aggr=[avg(other_col)])
//!       <child>
//! ```
//!
//! ## Two-stage state shape
//!
//! - **Partial output**: `(group_key i64, partial_sum f64, partial_count u64)`
//! - **FinalPartitioned input**: same schema (preserved across
//!   RepartitionExec Hash).
//! - **FinalPartitioned output**: `(group_key i64, avg f64)`.
//!
//! ## NOT in the default optimizer chain
//!
//! Per [[optimizer-codegen-sensitivity]], adding any new
//! `PhysicalOptimizerRule` costs ~7% geomean. Ships **opt-in only**
//! via [`install_robin_hood_avg_f64_rule`].

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Float64Array, Int64Array, RecordBatch, UInt64Array};
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
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use futures_util::stream::{self, TryStreamExt};

use crate::robin_hood_agg::RobinHoodAvgF64Agg;

/// Σ.R.2 — execution mode mirroring `RobinHoodSumF64Exec`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RobinHoodAvgF64Mode {
    /// Per-input-partition `AVG(f64) GROUP BY i64`. Output columns:
    /// `(group_key i64, partial_sum f64, partial_count u64)`.
    Partial,
    /// Consumes Partial output (after `RepartitionExec(Hash)`) and
    /// merges per group key. Output columns: `(group_key i64, avg f64)`.
    FinalPartitioned,
}

/// Σ.R.2 — `SELECT k, AVG(v) FROM child GROUP BY k` operator where
/// `k: Int64` and `v: Float64`.
#[derive(Debug)]
pub struct RobinHoodAvgF64Exec {
    input: Arc<dyn ExecutionPlan>,
    /// For Partial: index of the Int64 group key column in `input.schema()`.
    /// For FinalPartitioned: same — column 0 of upstream Partial output.
    group_col_idx: usize,
    /// For Partial: index of the Float64 value column to average.
    /// For FinalPartitioned: index of the Float64 partial_sum column
    /// (column 1 of upstream Partial output). The partial_count column
    /// is always at `value_col_idx + 1` (column 2).
    value_col_idx: usize,
    mode: RobinHoodAvgF64Mode,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    /// Σ.R.2 — pre-computed init_cap derived from the input's
    /// `Statistics::num_rows`. Sized at planner time so the hot path
    /// doesn't pay any stats-lookup cost.
    init_cap: usize,
}

impl RobinHoodAvgF64Exec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        group_col_idx: usize,
        value_col_idx: usize,
        mode: RobinHoodAvgF64Mode,
        group_out_name: String,
        avg_out_name: String,
    ) -> DfResult<Self> {
        let input_schema = input.schema();
        if group_col_idx >= input_schema.fields().len() {
            return Err(DataFusionError::Internal(format!(
                "RobinHoodAvgF64Exec: group_col_idx={group_col_idx} out of bounds"
            )));
        }
        if value_col_idx >= input_schema.fields().len() {
            return Err(DataFusionError::Internal(format!(
                "RobinHoodAvgF64Exec: value_col_idx={value_col_idx} out of bounds"
            )));
        }
        let gb_type = input_schema.field(group_col_idx).data_type();
        if gb_type != &DataType::Int64 {
            return Err(DataFusionError::Internal(format!(
                "RobinHoodAvgF64Exec: group column must be Int64, got {gb_type:?}"
            )));
        }
        let v_type = input_schema.field(value_col_idx).data_type();
        if v_type != &DataType::Float64 {
            return Err(DataFusionError::Internal(format!(
                "RobinHoodAvgF64Exec: value column must be Float64, got {v_type:?}"
            )));
        }
        // FinalPartitioned reads (sum: Float64 at value_col_idx, count:
        // UInt64 at value_col_idx + 1). Validate that adjacent column.
        if mode == RobinHoodAvgF64Mode::FinalPartitioned {
            let count_idx = value_col_idx + 1;
            if count_idx >= input_schema.fields().len() {
                return Err(DataFusionError::Internal(format!(
                    "RobinHoodAvgF64Exec: FinalPartitioned needs count column at idx={count_idx}, schema has {} fields",
                    input_schema.fields().len()
                )));
            }
            let c_type = input_schema.field(count_idx).data_type();
            if c_type != &DataType::UInt64 {
                return Err(DataFusionError::Internal(format!(
                    "RobinHoodAvgF64Exec: count column must be UInt64, got {c_type:?}"
                )));
            }
        }

        let schema = match mode {
            RobinHoodAvgF64Mode::Partial => Arc::new(Schema::new(vec![
                Field::new(&group_out_name, DataType::Int64, false),
                // State columns: name them to match DataFusion's
                // convention (`sum`, `count`) so downstream plan
                // walking and EXPLAIN look familiar.
                Field::new("partial_sum", DataType::Float64, true),
                Field::new("partial_count", DataType::UInt64, false),
            ])),
            RobinHoodAvgF64Mode::FinalPartitioned => Arc::new(Schema::new(vec![
                Field::new(&group_out_name, DataType::Int64, false),
                Field::new(&avg_out_name, DataType::Float64, true),
            ])),
        };
        let eq_props = EquivalenceProperties::new(schema.clone());
        let n_parts = input.output_partitioning().partition_count();
        let properties = Arc::new(PlanProperties::new(
            eq_props,
            Partitioning::UnknownPartitioning(n_parts),
            EmissionType::Final,
            Boundedness::Bounded,
        ));

        // Σ.R.2 — derive init_cap from upstream Statistics. Same shape
        // as RobinHoodSumF64Exec: Partial sees raw rows (# groups is
        // bounded by row count, typically ¼); FinalPartitioned sees
        // one row per group per upstream partition.
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
            RobinHoodAvgF64Mode::Partial => per_partition_rows / 4,
            RobinHoodAvgF64Mode::FinalPartitioned => per_partition_rows,
        };
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

    pub fn init_cap(&self) -> usize {
        self.init_cap
    }

    pub fn mode(&self) -> RobinHoodAvgF64Mode {
        self.mode
    }
    pub fn group_col_idx(&self) -> usize {
        self.group_col_idx
    }
    pub fn value_col_idx(&self) -> usize {
        self.value_col_idx
    }
}

impl DisplayAs for RobinHoodAvgF64Exec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mode_str = match self.mode {
            RobinHoodAvgF64Mode::Partial => "Partial",
            RobinHoodAvgF64Mode::FinalPartitioned => "FinalPartitioned",
        };
        write!(
            f,
            "RobinHoodAvgF64Exec(mode={mode_str}, group_col_idx={}, value_col_idx={})",
            self.group_col_idx, self.value_col_idx
        )
    }
}

impl ExecutionPlan for RobinHoodAvgF64Exec {
    fn name(&self) -> &str {
        "RobinHoodAvgF64Exec"
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
            DataFusionError::Internal("RobinHoodAvgF64Exec requires exactly 1 child".into())
        })?;
        let group_out_name = self.schema.field(0).name().clone();
        // For Partial the "avg_out_name" is unused (output is state);
        // for FinalPartitioned it's column 1 of our schema.
        let avg_out_name = match self.mode {
            RobinHoodAvgF64Mode::Partial => "avg".to_string(),
            RobinHoodAvgF64Mode::FinalPartitioned => self.schema.field(1).name().clone(),
        };
        Ok(Arc::new(Self::try_new(
            new_input,
            self.group_col_idx,
            self.value_col_idx,
            self.mode,
            group_out_name,
            avg_out_name,
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
        let mode = self.mode;
        let schema = self.schema.clone();
        let schema_for_stream = schema.clone();
        let planner_cap = self.init_cap;
        // Σ.R.2 — vectorised vs scalar A/B toggle for the Partial stage.
        // FinalPartitioned uses `ingest_partials` which is always
        // null-aware row-by-row (counts are upstream-aggregated; the
        // per-row scalar cost is amortised because FinalPartitioned
        // sees at most `# groups / # partitions` rows).
        let use_vec = std::env::var("EMAT_RH_AVG_F64_VEC")
            .ok()
            .map(|s| s != "0")
            .unwrap_or(true);

        let fut = async move {
            let init_cap: usize = std::env::var("EMAT_RH_AVG_F64_INIT_CAP")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(planner_cap);
            let mut agg = RobinHoodAvgF64Agg::with_capacity(init_cap);
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
                            "RobinHoodAvgF64Exec: column {group_col_idx} not Int64Array"
                        ))
                    })?;
                match mode {
                    RobinHoodAvgF64Mode::Partial => {
                        let vals_arr = batch.column(value_col_idx);
                        let vals = vals_arr
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .ok_or_else(|| {
                                DataFusionError::Internal(format!(
                                    "RobinHoodAvgF64Exec: column {value_col_idx} not Float64Array"
                                ))
                            })?;
                        if use_vec {
                            agg.ingest_batch_vectorised(keys, vals);
                        } else {
                            agg.ingest_batch(keys, vals);
                        }
                    }
                    RobinHoodAvgF64Mode::FinalPartitioned => {
                        let sums_arr = batch.column(value_col_idx);
                        let sums = sums_arr
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .ok_or_else(|| {
                                DataFusionError::Internal(format!(
                                    "RobinHoodAvgF64Exec: column {value_col_idx} not Float64Array"
                                ))
                            })?;
                        let counts_arr = batch.column(value_col_idx + 1);
                        let counts = counts_arr
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .ok_or_else(|| {
                                DataFusionError::Internal(format!(
                                    "RobinHoodAvgF64Exec: column {} not UInt64Array",
                                    value_col_idx + 1
                                ))
                            })?;
                        agg.ingest_partials(keys, sums, counts);
                    }
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
            let out = match mode {
                RobinHoodAvgF64Mode::Partial => {
                    let mut keys: Vec<i64> = Vec::with_capacity(n);
                    let mut sums: Vec<f64> = Vec::with_capacity(n);
                    let mut counts: Vec<u64> = Vec::with_capacity(n);
                    for (k, s, c) in agg.table().iter_sum_count() {
                        keys.push(k);
                        sums.push(s);
                        counts.push(c);
                    }
                    RecordBatch::try_new(
                        schema_for_stream.clone(),
                        vec![
                            Arc::new(Int64Array::from(keys)),
                            Arc::new(Float64Array::from(sums)),
                            Arc::new(UInt64Array::from(counts)),
                        ],
                    )
                }
                RobinHoodAvgF64Mode::FinalPartitioned => {
                    let mut keys: Vec<i64> = Vec::with_capacity(n);
                    let mut avgs: Vec<f64> = Vec::with_capacity(n);
                    for (k, avg) in agg.table().iter_avg() {
                        keys.push(k);
                        avgs.push(avg);
                    }
                    RecordBatch::try_new(
                        schema_for_stream.clone(),
                        vec![
                            Arc::new(Int64Array::from(keys)),
                            Arc::new(Float64Array::from(avgs)),
                        ],
                    )
                }
            }
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            DfResult::Ok(out)
        };

        let s = stream::once(fut);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, s)))
    }
}

// ---------------------------------------------------------------------
// Σ.R.2 planner rule.
// ---------------------------------------------------------------------

pub fn install_robin_hood_avg_f64_rule(builder: SessionStateBuilder) -> SessionStateBuilder {
    builder.with_physical_optimizer_rule(Arc::new(EnableRobinHoodAvgF64Rule::default()))
}

/// Σ.R.2 rule.
///
/// REV.11 (2026-05-30): cardinality-gated, mirroring REV.10 on the sum
/// sibling ([`crate::robin_hood_sum_f64_exec::EnableRobinHoodSumF64Rule`]).
/// The single-table RobinHood AVG agg wins at low/mid cardinality but
/// THRASHES once groups vastly exceed the largest pre-sized table
/// (`MAX_INIT_CAP` = 32M) — the failure mode that made Q18 SF=100's
/// 150M-group SUM subquery 12× slower under RobinHood. Opt-in (not in the
/// default chain), so the bug never bit production, but the gate makes it
/// safe for anyone who installs it. Refuses the rewrite when estimated
/// groups exceed `max_groups`.
#[derive(Debug)]
pub struct EnableRobinHoodAvgF64Rule {
    pub max_groups: usize,
}

/// REV.18 (2026-05-31): tightened 32M → 256K, matching the RobinHoodSumF64
/// finding. This AVG rule is OPT-IN (not in preset.rs / bench; Σ.R.2 rejected
/// it — Q17 SF=10 +40-55%), so the old 32M default never bit production — but
/// it's the SAME row-at-a-time open-addressing kernel, which loses to
/// DataFusion's vectorised columnar agg above ~256K groups (see
/// DEFAULT_RH_SUM_F64_MAX_GROUPS). Tightened defensively against re-enable.
pub const DEFAULT_RH_AVG_F64_MAX_GROUPS: usize = 256 * 1024;

impl Default for EnableRobinHoodAvgF64Rule {
    fn default() -> Self {
        let max_groups = std::env::var("EMAT_RH_AVG_F64_MAX_GROUPS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_RH_AVG_F64_MAX_GROUPS);
        Self { max_groups }
    }
}

impl PhysicalOptimizerRule for EnableRobinHoodAvgF64Rule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let max_groups = self.max_groups;
        let result = plan.transform_up(|node| {
            if let Some(agg) = node.as_any().downcast_ref::<AggregateExec>() {
                if matches!(agg.mode(), AggregateMode::Partial) {
                    if let Some(m) = match_partial(agg) {
                        let input = agg.input().clone();
                        let in_schema = input.schema();
                        if in_schema.field(m.group_col_idx).data_type() == &DataType::Int64
                            && in_schema.field(m.value_col_idx).data_type() == &DataType::Float64
                        {
                            // REV.11 cardinality gate (mirrors REV.10 on the
                            // sum sibling). Estimate groups from input rows
                            // (~4 rows/group, matching init_cap). If groups
                            // would blow past the largest pre-sized table, the
                            // single-table RobinHood agg thrashes — leave it to
                            // DataFusion's stock vectorised agg.
                            let est_groups = match input.partition_statistics(None) {
                                Ok(s) => match s.num_rows {
                                    Precision::Exact(n) | Precision::Inexact(n) => n / 4,
                                    _ => 0,
                                },
                                Err(_) => 0,
                            };
                            if est_groups <= max_groups {
                                let new = RobinHoodAvgF64Exec::try_new(
                                    input,
                                    m.group_col_idx,
                                    m.value_col_idx,
                                    RobinHoodAvgF64Mode::Partial,
                                    m.group_out_name,
                                    m.avg_out_name,
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
                        if find_robin_hood_avg_f64_partial(agg.input()).is_some() {
                            // Upstream Partial sits below an optional
                            // RepartitionExec(Hash). The Partial's
                            // output schema is (key, sum, count), so
                            // we read group=col 0, value(sum)=col 1.
                            let new = RobinHoodAvgF64Exec::try_new(
                                agg.input().clone(),
                                0,
                                1,
                                RobinHoodAvgF64Mode::FinalPartitioned,
                                m.group_out_name,
                                m.avg_out_name,
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
        "ematix_flow_enable_robin_hood_avg_f64"
    }

    fn schema_check(&self) -> bool {
        // Our FinalPartitioned changes the agg's output column name
        // from `avg(v)` → caller-supplied name (matches the matcher's
        // `avg_out_name`), so the schema check still passes — we
        // preserve the LogicalPlan-level field name.
        true
    }
}

struct Matched {
    group_col_idx: usize,
    value_col_idx: usize,
    group_out_name: String,
    avg_out_name: String,
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
    if !a.fun().name().eq_ignore_ascii_case("avg") {
        return None;
    }
    let exprs = a.expressions();
    if exprs.len() != 1 {
        return None;
    }
    let value_col = exprs[0].as_any().downcast_ref::<Column>()?;
    let value_col_idx = value_col.index();
    let avg_out_name = a.name().to_string();
    Some(Matched {
        group_col_idx,
        value_col_idx,
        group_out_name: group_out_name.clone(),
        avg_out_name,
    })
}

fn match_final(agg: &AggregateExec) -> Option<Matched> {
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
    if !a.fun().name().eq_ignore_ascii_case("avg") {
        return None;
    }
    let avg_out_name = a.name().to_string();
    Some(Matched {
        // Indices are positional in the Partial's output schema — they
        // get used as 0, 1 in the rule body for FinalPartitioned.
        group_col_idx: 0,
        value_col_idx: 1,
        group_out_name: group_out_name.clone(),
        avg_out_name,
    })
}

fn find_robin_hood_avg_f64_partial(
    plan: &Arc<dyn ExecutionPlan>,
) -> Option<Arc<dyn ExecutionPlan>> {
    let mut cur = plan.clone();
    loop {
        if let Some(rh) = cur.as_any().downcast_ref::<RobinHoodAvgF64Exec>() {
            if rh.mode() == RobinHoodAvgF64Mode::Partial {
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
        let state = install_robin_hood_avg_f64_rule(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(cfg),
        )
        .build();
        SessionContext::new_with_state(state)
    }

    fn register_q17_shape_table(ctx: &SessionContext, name: &str) {
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
        register_q17_shape_table(&ctx, "t");
        let df = ctx
            .sql("SELECT k, AVG(v) FROM t GROUP BY k ORDER BY k")
            .await
            .unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let s = format!("{plan:?}");
        assert!(
            s.contains("RobinHoodAvgF64Exec"),
            "plan didn't contain RobinHoodAvgF64Exec — rule didn't fire. Got:\n{s}"
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
        register_q17_shape_table(&stock_ctx, "t");
        let df2 = stock_ctx
            .sql("SELECT k, AVG(v) FROM t GROUP BY k ORDER BY k")
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
                "avg mismatch for key {}: rh={} stock={}",
                a.0,
                a.1,
                b.1
            );
        }
    }

    #[tokio::test]
    async fn rule_no_op_on_sum_agg() {
        let ctx = make_ctx_with_rule();
        register_q17_shape_table(&ctx, "t");
        let df = ctx.sql("SELECT k, SUM(v) FROM t GROUP BY k").await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let s = format!("{plan:?}");
        assert!(
            !s.contains("RobinHoodAvgF64Exec"),
            "rule fired on SUM — that's wrong. Got:\n{s}"
        );
    }

    #[tokio::test]
    async fn rule_no_op_on_multi_agg() {
        let ctx = make_ctx_with_rule();
        register_q17_shape_table(&ctx, "t");
        let df = ctx
            .sql("SELECT k, AVG(v), COUNT(*) FROM t GROUP BY k")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let s = format!("{plan:?}");
        assert!(
            !s.contains("RobinHoodAvgF64Exec"),
            "rule fired on multi-agg — that's wrong. Got:\n{s}"
        );
    }

    #[tokio::test]
    async fn rule_no_op_on_non_int64_groupby() {
        let cfg = SessionConfig::new().with_target_partitions(4);
        let state = install_robin_hood_avg_f64_rule(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(cfg),
        )
        .build();
        let ctx = SessionContext::new_with_state(state);
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("v", DataType::Float64, false),
        ]));
        use arrow_array::StringArray;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a"])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
            ],
        )
        .unwrap();
        let mt = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
        ctx.register_table("t2", Arc::new(mt)).unwrap();
        let df = ctx
            .sql("SELECT k, AVG(v) FROM t2 GROUP BY k")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let s = format!("{plan:?}");
        assert!(
            !s.contains("RobinHoodAvgF64Exec"),
            "rule fired on Utf8 group key — that's wrong. Got:\n{s}"
        );
    }

    #[tokio::test]
    async fn high_cardinality_matches_stock() {
        // 2K keys × 30 rows/key across 4 partitions — Q17-ish shape.
        let cfg = SessionConfig::new().with_target_partitions(4);
        let state = install_robin_hood_avg_f64_rule(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(cfg),
        )
        .build();
        let ctx = SessionContext::new_with_state(state);

        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Float64, false),
        ]));
        let n_groups = 2_000i64;
        let rows_per_group = 30usize;
        let mut all_k: Vec<i64> = Vec::new();
        let mut all_v: Vec<f64> = Vec::new();
        for k in 0..n_groups {
            for r in 0..rows_per_group {
                all_k.push(k);
                all_v.push((r as f64 + 1.0) * 7.0);
            }
        }
        // Shard into 4 partitions to exercise the merge path.
        let chunk = all_k.len() / 4;
        let mut partitions: Vec<Vec<RecordBatch>> = Vec::with_capacity(4);
        for p in 0..4 {
            let lo = p * chunk;
            let hi = if p == 3 { all_k.len() } else { lo + chunk };
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(all_k[lo..hi].to_vec())),
                    Arc::new(Float64Array::from(all_v[lo..hi].to_vec())),
                ],
            )
            .unwrap();
            partitions.push(vec![batch]);
        }
        let mt = MemTable::try_new(schema.clone(), partitions).unwrap();
        ctx.register_table("t3", Arc::new(mt)).unwrap();

        let df = ctx
            .sql("SELECT k, AVG(v) FROM t3 GROUP BY k ORDER BY k")
            .await
            .unwrap();
        let rh_batches = df.collect().await.unwrap();
        // Per group: avg = sum_{r=1..30} 7r / 30 = 7 * 31/2 = 108.5
        let mut found = 0;
        for b in &rh_batches {
            let ks = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let vs = b.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
            for i in 0..b.num_rows() {
                assert!(
                    (vs.value(i) - 108.5).abs() < 1e-9,
                    "k={} got {}",
                    ks.value(i),
                    vs.value(i)
                );
                found += 1;
            }
        }
        assert_eq!(found, n_groups as usize);
    }

    #[tokio::test]
    async fn rule_gated_off_at_high_cardinality() {
        // REV.11: with max_groups=0, any non-empty input counts as "high
        // card", so the rule must REFUSE — DataFusion's stock vectorised
        // agg handles very high cardinality far better (Q18 SF=100's
        // 150M-group sibling agg was 12× slower under RobinHood).
        let cfg = SessionConfig::new().with_target_partitions(4);
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(cfg)
            .with_physical_optimizer_rule(Arc::new(EnableRobinHoodAvgF64Rule { max_groups: 0 }))
            .build();
        let ctx = SessionContext::new_with_state(state);
        register_q17_shape_table(&ctx, "t");
        let df = ctx.sql("SELECT k, AVG(v) FROM t GROUP BY k").await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let s = format!("{plan:?}");
        assert!(
            !s.contains("RobinHoodAvgF64Exec"),
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
            .with_physical_optimizer_rule(Arc::new(EnableRobinHoodAvgF64Rule {
                max_groups: 1_000_000,
            }))
            .build();
        let ctx = SessionContext::new_with_state(state);
        register_q17_shape_table(&ctx, "t");
        let df = ctx.sql("SELECT k, AVG(v) FROM t GROUP BY k").await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let s = format!("{plan:?}");
        assert!(
            s.contains("RobinHoodAvgF64Exec"),
            "rule didn't fire under a generous gate — Got:\n{s}"
        );
    }
}
