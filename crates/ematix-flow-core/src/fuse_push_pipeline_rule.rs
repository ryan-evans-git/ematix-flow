//! PV.3 — the push-pipeline fusion recognizer (de-risk increment).
//!
//! Detects a **membership-reduction join** — a `CollectLeft` Inner (or
//! LeftSemi) `HashJoinExec` on a single i64 equi-key, no residual filter,
//! whose output is entirely PROBE (fact) columns, and whose probe (right)
//! subtree bottoms out at exactly one sideband-free `EmatixFastParquetExec`
//! — and replaces it with [`EmatPushPipelineExec`], which scans the fact
//! once and drops non-matching rows in a fused push pass instead of
//! materialising + `take`-gathering the join output.
//!
//! This is the **machinery de-risk** (ADR §6 / PV.3, the user's Option 1):
//! it proves the recognizer → build-child → splice → gate path on the
//! simplest real clean shape (a `part`-membership reduction of `lineitem`).
//! The Q08 snowflake-collapse (string carry, payload dims, `extract(year)`)
//! is the **PV.3b** follow-on; this recognizer deliberately **bails** on it
//! (the emit would include a build column → §`try_fuse` rejects it).
//!
//! ## Why value-equivalent, hence safe under any parent
//!
//! The fused node reproduces the join's EXACT output schema, rows, and
//! values: a CollectLeft Inner join on a UNIQUE build key (a dimension PK)
//! emits each surviving probe row exactly once with only probe columns —
//! identical to a membership filter. So the swap is a pure operator
//! substitution; the aggregate (or whatever consumes the join) is
//! unaffected. The `require_unique` guard (enforced at build time inside the
//! operator) makes the uniqueness precondition a hard runtime check, not an
//! assumption (architect C-R4 / multiplicity).
//!
//! ## Install + gate
//!
//! Invoked from [`crate::flow_query_planner::FlowQueryPlanner`] as a
//! POST-`create_physical_plan` transform — NOT a `PhysicalOptimizerRule`, to
//! avoid the 5–8% codegen tax (`flow_query_planner.rs` rationale). Gated
//! `EMAT_PUSH_PIPELINE=1`, **default OFF**. The transform runs after
//! `EnforceDistribution`/`ForceCollectLeft`/L9, so `partition_mode()` and any
//! `runtime_sideband()` are final when the gates read them.

use std::sync::Arc;

use arrow_schema::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{JoinType, Result as DfResult};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};

use crate::emat_push_pipeline_exec::{BuildBinding, BuildSource, EmatPushPipelineExec, EmitCol};
use crate::ematix_fast_parquet::EmatixFastParquetExec;

/// `true` iff `EMAT_PUSH_PIPELINE` is set to `1`/`true` (default OFF).
pub fn enabled() -> bool {
    std::env::var("EMAT_PUSH_PIPELINE")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn widens_to_i64(dt: &DataType) -> bool {
    matches!(dt, DataType::Int64 | DataType::Int32)
}

/// Only Int64/Float64 fact columns can be passed through in this increment.
fn passthrough_supported(dt: &DataType) -> bool {
    matches!(dt, DataType::Int64 | DataType::Float64)
}

/// The probe (right) subtree must be a linear chain of pipelined single-child
/// wrappers ending in EXACTLY ONE `EmatixFastParquetExec` that carries no L9
/// runtime sideband (architect R8/R9). A 2-child node (a nested join) or a
/// non-EMAT leaf breaks the chain → bail.
fn probe_spine_single_emat_no_sideband(node: &Arc<dyn ExecutionPlan>) -> bool {
    if let Some(emat) = node.as_any().downcast_ref::<EmatixFastParquetExec>() {
        return emat.runtime_sideband().is_none();
    }
    // Descend only through known pipelined single-child wrappers.
    const SPINE_OK: &[&str] = &[
        "RepartitionExec",
        "CoalesceBatchesExec",
        "CoalescePartitionsExec",
        "FilterExec",
        "ProjectionExec",
    ];
    if !SPINE_OK.contains(&node.name()) {
        return false;
    }
    let ch = node.children();
    if ch.len() != 1 {
        return false;
    }
    probe_spine_single_emat_no_sideband(ch[0])
}

/// Return the fused replacement if `hj` is a fusable membership-reduction
/// join, else `None` (caller keeps the stock join).
fn try_fuse(hj: &HashJoinExec) -> Option<EmatPushPipelineExec> {
    // --- join-shape gates (architect R6/R7) ---
    let jt = *hj.join_type();
    // Inner: value-equivalent only if the build is unique (guarded below).
    // LeftSemi: a pure reduction, never multiplies, no uniqueness needed.
    let require_unique = match jt {
        JoinType::Inner => true,
        JoinType::LeftSemi => false,
        _ => return None,
    };
    if *hj.partition_mode() != PartitionMode::CollectLeft {
        return None; // build-size guard: only broadcastable builds
    }
    if hj.filter().is_some() {
        return None; // R6: a residual non-equi filter can't be a bitset probe
    }
    if hj.on().len() != 1 {
        return None; // single equi-key only
    }
    let (lexpr, rexpr) = &hj.on()[0];
    let lcol = lexpr.as_any().downcast_ref::<Column>()?;
    let rcol = rexpr.as_any().downcast_ref::<Column>()?;

    let left = hj.left();
    let right = hj.right();
    let lt = left.schema().field(lcol.index()).data_type().clone();
    let rt = right.schema().field(rcol.index()).data_type().clone();
    if !widens_to_i64(&lt) || !widens_to_i64(&rt) {
        return None; // R6: i64-widenable keys only (what the kernel supports)
    }

    // --- probe-spine gate (R8/R9): one sideband-free EMAT fact scan ---
    if !probe_spine_single_emat_no_sideband(right) {
        return None;
    }

    // --- output mapping (R1/R2): every output column must be a PROBE column
    // of a supported passthrough type. A build column in the output means the
    // join carries a dimension attribute up (Q08/Q14 shape) → bail to PV.3b.
    let left_len = left.schema().fields().len();
    let join_schema = hj.join_schema();
    let out_schema = hj.schema();
    let mut emit: Vec<EmitCol> = Vec::with_capacity(out_schema.fields().len());
    for out_f in out_schema.fields() {
        // Resolve the output field to a unique column of the combined
        // (build ++ probe) join schema, by name (uniqueness-bail = R2).
        let mut hit: Option<usize> = None;
        let mut count = 0usize;
        for (j, jf) in join_schema.fields().iter().enumerate() {
            if jf.name() == out_f.name() {
                hit = Some(j);
                count += 1;
            }
        }
        if count != 1 {
            return None; // ambiguous (self-join) or missing → bail
        }
        let j = hit.unwrap();
        if j < left_len {
            return None; // a BUILD column in the output → not pure membership
        }
        if !passthrough_supported(out_f.data_type()) {
            return None; // R1: unsupported passthrough type (e.g. Utf8)
        }
        emit.push(EmitCol::ProbeColumn {
            col: j - left_len, // probe-local index
        });
    }
    if emit.is_empty() {
        return None; // nothing to emit (defensive)
    }

    let build = BuildBinding {
        source: BuildSource::Plan {
            plan: left.clone(),
            key_col: lcol.index(),
            payload_col: None, // membership only in this increment
        },
        probe_fk_col: rcol.index(),
        require_unique,
    };

    Some(EmatPushPipelineExec::new(
        right.clone(),
        vec![build],
        emit,
        out_schema,
    ))
}

/// Walk the physical plan bottom-up, swapping every fusable membership
/// reduction for [`EmatPushPipelineExec`]. No-op unless `EMAT_PUSH_PIPELINE=1`.
pub fn fuse_push_pipelines(plan: Arc<dyn ExecutionPlan>) -> DfResult<Arc<dyn ExecutionPlan>> {
    if !enabled() {
        return Ok(plan);
    }
    fuse_push_pipelines_always(plan)
}

/// The transform without the env gate — for tests/harnesses that drive the
/// recognizer deterministically (no racy global-env toggling).
pub fn fuse_push_pipelines_always(
    plan: Arc<dyn ExecutionPlan>,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let trace = std::env::var_os("EMAT_PUSH_PIPELINE_TRACE").is_some();
    let out = plan.transform_up(|node| {
        if let Some(hj) = node.as_any().downcast_ref::<HashJoinExec>() {
            if let Some(fused) = try_fuse(hj) {
                if trace {
                    eprintln!(
                        "[PV.3] fused {:?} CollectLeft membership join → EmatPushPipelineExec",
                        hj.join_type()
                    );
                }
                return Ok(Transformed::yes(Arc::new(fused) as Arc<dyn ExecutionPlan>));
            } else if trace {
                eprintln!("[PV.3] skip HashJoinExec (shape gate not met)");
            }
        }
        Ok(Transformed::no(node))
    })?;
    Ok(out.data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Float64Array};
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::physical_plan::{collect, displayable};
    use datafusion::prelude::{SessionConfig, SessionContext};
    use std::path::PathBuf;

    fn sf1_dir() -> Option<PathBuf> {
        if let Ok(env) = std::env::var("TPCH_DATA_DIR") {
            let p = PathBuf::from(env);
            if p.join("lineitem.parquet").exists() {
                return Some(p);
            }
        }
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let p = manifest.parent()?.parent()?.join("examples/tpch/data/sf1");
        p.join("lineitem.parquet").exists().then_some(p)
    }

    fn prod_ctx(dir: &std::path::Path) -> SessionContext {
        let builder = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(8))
            .with_default_features();
        let ctx =
            SessionContext::new_with_state(crate::preset::with_optimizer_rules(builder).build());
        for t in [
            "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
        ] {
            let path = dir.join(format!("{t}.parquet"));
            if t == "lineitem" || t == "orders" {
                ctx.register_table(
                    t,
                    Arc::new(
                        crate::ematix_fast_parquet::EmatixFastParquetTableProvider::try_new(
                            path.to_string_lossy(),
                        )
                        .unwrap(),
                    ),
                )
                .unwrap();
            } else {
                ctx.register_table(
                    t,
                    Arc::new(
                        crate::fast_parquet::FastParquetTableProvider::try_new(
                            path.to_string_lossy(),
                        )
                        .unwrap(),
                    ),
                )
                .unwrap();
            }
        }
        ctx
    }

    fn sum_f64(batches: &[arrow_array::RecordBatch]) -> f64 {
        let mut s = 0.0;
        for b in batches {
            if let Some(a) = b.column(0).as_any().downcast_ref::<Float64Array>() {
                for i in 0..a.len() {
                    if a.is_valid(i) {
                        s += a.value(i);
                    }
                }
            }
        }
        s
    }

    /// THE real-splice gate (the test PV.2 lacked). Plan the clean de-risk
    /// query through the production planner, splice the fused node under the
    /// REAL stock plan, and require: (1) the fused node actually appears, and
    /// (2) the result sum is bit-for-bit identical to the stock plan.
    #[tokio::test(flavor = "multi_thread")]
    async fn derisk_membership_reduction_splices_and_matches_stock() {
        let Some(dir) = sf1_dir() else {
            eprintln!("skip: no SF=1 TPC-H data");
            return;
        };
        let ctx = prod_ctx(&dir);
        let sql = "SELECT sum(l_extendedprice * (1 - l_discount)) AS revenue \
                   FROM lineitem, part \
                   WHERE l_partkey = p_partkey AND p_brand = 'Brand#23'";

        // Stock plan (recognizer not applied) → reference sum.
        let stock_plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let stock = collect(stock_plan, ctx.task_ctx()).await.unwrap();
        let stock_sum = sum_f64(&stock);

        // Apply the recognizer to a FRESH physical plan (re-executing the same
        // RepartitionExec instances would panic — they're single-use).
        let fresh = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let fused_plan = fuse_push_pipelines_always(fresh).unwrap();
        let rendered = format!("{}", displayable(fused_plan.as_ref()).indent(true));
        assert!(
            rendered.contains("EmatPushPipelineExec"),
            "recognizer must fire on the clean membership shape; plan was:\n{rendered}"
        );

        let fused = collect(fused_plan, ctx.task_ctx()).await.unwrap();
        let fused_sum = sum_f64(&fused);

        // RELATIVE tolerance: the membership filter emits survivors in a
        // different row/partition order than hash-join + `take`, so f64 SUM
        // non-associativity gives last-ULP differences on a ~1e10 value. The
        // result is value-equivalent to ~14 significant digits (NOT a bug —
        // the same FP-order effect DedupeAggregateForFloatDeterminism handles).
        assert!(
            (stock_sum - fused_sum).abs() <= 1e-9 * stock_sum.abs().max(1.0),
            "fused sum {fused_sum} must match stock {stock_sum}"
        );
        assert!(stock_sum > 0.0, "sanity: non-empty result");
    }
}
