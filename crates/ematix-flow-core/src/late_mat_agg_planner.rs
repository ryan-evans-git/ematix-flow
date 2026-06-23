//! prod-C — the late-materialization `ExtensionPlanner`.
//!
//! Expands a [`crate::late_mat_agg::LateMatAggNode`] into the proven physical
//! subtree (the `q10_lategather_e2e` spike, generalized off the node):
//!
//! ```text
//!   LateGatherExec(reattach group cols from the shared build, by rowid)
//!     AggregateExec(SinglePartitioned, gby=[__lm_rowid], <rebuilt aggrs>)
//!       RepartitionExec(Hash[__lm_rowid])
//!         EmatixHashJoinExec(build_key=anchor PK, probe_key=fact FK;
//!                            emit [BuildRowId, probe cols…])
//!           <build: anchor ⋈ folded dims, projected to the group cols>
//!           <probe: the fact chain, projected to [fact FK, agg-arg cols…]>
//! ```
//!
//! The wide group columns are carried ONLY as a `u32` build-rowid through the
//! join + aggregate, then gathered at the (far smaller) aggregate outputs from
//! the SAME resident build batches (shared via the join's
//! `Arc<OnceCell<Arc<EmatHashJoiner>>>`) — no re-scan. Soundness is established
//! by the recognizer (the build is 1:1 with the anchor, so grouping the rowid is
//! identical to grouping the full wide key).
//!
//! Registered ONLY on the gated path (`EMAT_LATE_MAT_AGG=1`) in
//! [`crate::flow_query_planner`], so the default planner is byte-identical when
//! the flag is off. Mirrors `FusedProbePlanner`.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, DFSchema, DataFusionError, Result};
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_expr::{Partitioning, PhysicalExpr, create_physical_expr};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::expressions::Column as PhysColumn;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};

use crate::emat_hash_join::JoinColumn;
use crate::emat_hash_join_exec::EmatixHashJoinExec;
use crate::late_gather_exec::{LateGatherColumn, LateGatherExec};
use crate::late_mat_agg::LateMatAggNode;

/// The compact build-row index column the join emits in place of the wide cols.
const ROWID: &str = "__lm_rowid";

/// Plans a [`LateMatAggNode`] into the late-materialization physical subtree.
#[derive(Debug, Default)]
pub struct LateMatAggPlanner;

#[async_trait]
impl ExtensionPlanner for LateMatAggPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<LateMatAggNode>() else {
            return Ok(None); // not ours — let another planner try
        };
        if physical_inputs.len() != 2 {
            return Err(DataFusionError::Internal(format!(
                "LateMatAggregate: expected 2 physical inputs, got {}",
                physical_inputs.len()
            )));
        }
        let build = physical_inputs[0].clone();
        let probe = physical_inputs[1].clone();
        let probe_schema = probe.schema();

        // --- EmatixHashJoinExec: build_key = anchor PK; emit rowid + all probe cols ---
        // Output = [BuildRowId, Probe(0), …, Probe(n-1)] so the emat schema is
        // [__lm_rowid, <probe cols…>] and the aggregate args resolve against it.
        let build_key = node.build_key_pos;
        let probe_key = 0usize; // the probe projection's column 0 is the fact FK.
        let mut out_cols: Vec<JoinColumn> = Vec::with_capacity(probe_schema.fields().len() + 1);
        out_cols.push(JoinColumn::BuildRowId);
        let mut emat_fields: Vec<Field> = Vec::with_capacity(probe_schema.fields().len() + 1);
        emat_fields.push(Field::new(ROWID, DataType::UInt32, false));
        for (i, f) in probe_schema.fields().iter().enumerate() {
            out_cols.push(JoinColumn::Probe(i));
            emat_fields.push(Field::new(f.name(), f.data_type().clone(), f.is_nullable()));
        }
        let emat_schema: SchemaRef = Arc::new(Schema::new(emat_fields));
        let join = Arc::new(EmatixHashJoinExec::new(
            build,
            probe,
            build_key,
            probe_key,
            out_cols,
            emat_schema.clone(),
        ));
        let build_once = join.build_once();
        let join_dyn: Arc<dyn ExecutionPlan> = join;

        // --- RepartitionExec(Hash[rowid]) so the aggregate runs SinglePartitioned ---
        let nparts = join_dyn.output_partitioning().partition_count().max(1);
        let rowid_e: Arc<dyn PhysicalExpr> = Arc::new(PhysColumn::new(ROWID, 0));
        let repart: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
            join_dyn,
            Partitioning::Hash(vec![rowid_e.clone()], nparts),
        )?);

        // --- AggregateExec(SinglePartitioned, gby=[rowid], <rebuilt aggrs>) ---
        // Rebuild each logical aggregate function physically over the join output:
        // its argument exprs are evaluated against the emat schema (probe cols,
        // unqualified). The rowid-keyed group-id is a cheap i64 vs the wide key.
        let emat_df = DFSchema::try_from(emat_schema.as_ref().clone())?;
        let exec_props = session_state.execution_props();
        let mut aggrs = Vec::with_capacity(node.aggr_expr.len());
        for (k, ae) in node.aggr_expr.iter().enumerate() {
            let inner = unwrap_alias(ae);
            let Expr::AggregateFunction(af) = inner else {
                return Err(DataFusionError::Internal(format!(
                    "LateMatAggregate: expected an aggregate function, got {inner:?}"
                )));
            };
            // Output column name = the original aggregate output field (so the
            // node's schema names match what the wrappers above expect).
            let alias = node.schema.field(node.n_group + k).name().clone();
            let mut phys_args = Vec::with_capacity(af.params.args.len());
            for a in &af.params.args {
                let bare = strip_qualifiers(a.clone())?;
                phys_args.push(create_physical_expr(&bare, &emat_df, exec_props)?);
            }
            let built = AggregateExprBuilder::new(af.func.clone(), phys_args)
                .schema(emat_schema.clone())
                .alias(alias)
                .build()
                .map(Arc::new)?;
            aggrs.push(built);
        }
        let n_aggrs = aggrs.len();
        let group_by = PhysicalGroupBy::new_single(vec![(rowid_e, ROWID.to_string())]);
        let agg: Arc<dyn ExecutionPlan> = Arc::new(AggregateExec::try_new(
            AggregateMode::SinglePartitioned,
            group_by,
            aggrs,
            vec![None; n_aggrs],
            repart.clone(),
            repart.schema(),
        )?);
        // agg output schema = [__lm_rowid @0, <agg results @1..>].

        // --- LateGatherExec: reattach the group cols from the shared build ---
        // Output schema = the node's (group cols ++ agg cols). group col i ←
        // Build(i) (build projection is in group-by order); agg result k ←
        // Input(1 + k) (rowid occupies agg output index 0).
        let final_schema: SchemaRef = Arc::new(node.schema.as_arrow().clone());
        let mut output: Vec<LateGatherColumn> = Vec::with_capacity(final_schema.fields().len());
        for i in 0..node.n_group {
            output.push(LateGatherColumn::Build(i));
        }
        for k in 0..node.aggr_expr.len() {
            output.push(LateGatherColumn::Input(1 + k));
        }
        let late: Arc<dyn ExecutionPlan> =
            Arc::new(LateGatherExec::new(agg, build_once, 0, output, final_schema));
        Ok(Some(late))
    }
}

/// Peel `Expr::Alias` wrappers off an aggregate output expression.
fn unwrap_alias(e: &Expr) -> &Expr {
    match e {
        Expr::Alias(a) => unwrap_alias(&a.expr),
        other => other,
    }
}

/// Strip relation qualifiers from every `Column` in `e` so it resolves against
/// the (unqualified) emat output schema, whose field names are the probe
/// projection's flat column names.
fn strip_qualifiers(e: Expr) -> Result<Expr> {
    Ok(e.transform(|x| match x {
        Expr::Column(c) => Ok(Transformed::yes(Expr::Column(Column::new_unqualified(c.name)))),
        other => Ok(Transformed::no(other)),
    })?
    .data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ematix_fast_parquet::EmatixFastParquetTableProvider;
    use crate::late_mat_agg::reconstruct;
    use datafusion::arrow::array::{Array, Float64Array};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::collect;
    use datafusion::physical_planner::DefaultPhysicalPlanner;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use std::path::{Path, PathBuf};

    fn sf1_dir() -> Option<PathBuf> {
        if let Ok(env) = std::env::var("TPCH_DATA_DIR") {
            let p = PathBuf::from(env);
            if p.join("customer.parquet").exists() {
                return Some(p);
            }
        }
        let m = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let p = m.parent()?.parent()?.join("examples/tpch/data/sf1");
        p.join("customer.parquet").exists().then_some(p)
    }

    fn prov(dir: &Path, t: &str, pk: Option<usize>) -> EmatixFastParquetTableProvider {
        let p = dir.join(format!("{t}.parquet"));
        let mut prov = EmatixFastParquetTableProvider::try_new(p.to_string_lossy()).unwrap();
        if let Some(i) = pk {
            prov = prov.with_primary_key(vec![i]);
        }
        prov
    }

    async fn q10_ctx(dir: &Path) -> SessionContext {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
        ctx.register_table("customer", Arc::new(prov(dir, "customer", Some(0))))
            .unwrap();
        ctx.register_table("orders", Arc::new(prov(dir, "orders", Some(0))))
            .unwrap();
        ctx.register_table("lineitem", Arc::new(prov(dir, "lineitem", None)))
            .unwrap();
        ctx.register_table("nation", Arc::new(prov(dir, "nation", Some(0))))
            .unwrap();
        ctx
    }

    fn checksum(batches: &[RecordBatch]) -> (usize, f64) {
        let mut rows = 0usize;
        let mut sum = 0.0f64;
        for b in batches {
            rows += b.num_rows();
            if let Ok(idx) = b.schema().index_of("revenue") {
                if let Some(a) = b.column(idx).as_any().downcast_ref::<Float64Array>() {
                    for i in 0..a.len() {
                        if a.is_valid(i) {
                            sum += a.value(i);
                        }
                    }
                }
            }
        }
        (rows, sum)
    }

    /// END-TO-END (prod-C): the late-mat plan must produce row-for-row identical
    /// results (count + revenue sum, order-independent) to the stock Q10 plan at
    /// SF=1 — the correctness gate before any wiring / perf measurement.
    #[tokio::test]
    async fn late_mat_q10_matches_stock_sf1() {
        let Some(dir) = sf1_dir() else {
            eprintln!("skip: no SF=1 data");
            return;
        };
        let ctx = q10_ctx(&dir).await;
        let sql = std::fs::read_to_string("examples/tpch/queries/q10.sql")
            .or_else(|_| std::fs::read_to_string(dir.join("../../queries/q10.sql")))
            .unwrap();
        let sql = sql.trim().trim_end_matches(';');
        let logical = ctx.sql(sql).await.unwrap().into_optimized_plan().unwrap();

        // Stock arm.
        let stock_phys = ctx.state().create_physical_plan(&logical).await.unwrap();
        let stock_out = collect(stock_phys, ctx.task_ctx()).await.unwrap();

        // Late-mat arm: reconstruct + plan with the extension planner.
        let rewritten = reconstruct(&logical).expect("Q10 reconstructs");
        let planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(LateMatAggPlanner)]);
        let late_phys = planner
            .create_physical_plan(&rewritten, &ctx.state())
            .await
            .expect("late-mat physical plan");
        let late_out = collect(late_phys, ctx.task_ctx()).await.unwrap();

        let (sr, ss) = checksum(&stock_out);
        let (lr, ls) = checksum(&late_out);
        assert_eq!(sr, lr, "row count: stock {sr} vs late {lr}");
        assert!(
            (ss - ls).abs() < ss.abs() * 1e-9 + 1e-6,
            "revenue sum: stock {ss:.4} vs late {ls:.4}"
        );
        assert!(sr > 0, "sanity: Q10 SF1 returns rows");
    }
}
