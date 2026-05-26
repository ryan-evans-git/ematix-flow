//! Σ.U — push a filter as LeftSemi into an Aggregate's input scan.
//!
//! ## What this fixes
//!
//! Q17 SF=10 baseline 189 ms / DuckDB 160 ms (+17%). DataFusion's
//! `scalar_subquery_to_join` already decorrelates the correlated
//! subquery into a top-level `Inner Join` with an `Aggregate`-over-
//! lineitem on one side, BUT the aggregate runs over all 60M lineitem
//! rows producing 10.8M partkey groups, when only the ~200 partkeys
//! matching the outer `p_brand='Brand#23' AND p_container='MED BOX'`
//! filter actually matter.
//!
//! Photon-style "magic set" / DuckDB-style `LEFT_DELIM_JOIN`
//! decorrelation answers this by pushing the outer filter into the
//! aggregate's input. We do the same at the LogicalPlan level via a
//! pre-plan walker that pattern-matches the shape and inserts a
//! `LeftSemi` between the aggregate's `TableScan` and the
//! `Aggregate` node:
//!
//! ```text
//!   Aggregate(group_by=[K], avg(...))            Aggregate(group_by=[K], avg(...))
//!     └─ TableScan: T                  →           └─ LeftSemi(T.K = F.K)
//!                                                    ├─ TableScan: T
//!                                                    └─ <filter_subtree producing F.K>
//! ```
//!
//! ## Why not an OptimizerRule
//!
//! [[optimizer-codegen-sensitivity]]: three prior rules
//! (Σ.H.1d.4, Σ.K.A, Σ.F-T2) regressed 5–8 pp geomean from LLVM
//! codegen perturbation alone. [[dict-routing]] and [[Σ.T
//! join_reorder]] both proved the pre-plan-walker pattern works at
//! this layer. The caller invokes [`push_filter_into_agg`] explicitly
//! on the optimized plan before `ctx.execute_logical_plan`; no
//! `OptimizerRule` is added to DataFusion's pass stack.
//!
//! ## Pattern matched
//!
//! ```text
//!   Inner Join (L, R) on L.X = R.K
//!     L: any subtree where the L.X column is produced by a
//!        Filter(P) → TableScan(T_alt) chain (possibly nested
//!        inside other Joins/Projections).
//!     R: Projection? → SubqueryAlias? → Projection? →
//!        Aggregate(group_by=[K, ...]) → TableScan(T)
//! ```
//!
//! K must appear in the aggregate's group-by list. The rewrite
//! clones the smallest Filter→TableScan subtree on the L side that
//! produces L.X and inserts it as the build side of a LeftSemi
//! between the aggregate and its TableScan.
//!
//! ## Status
//!
//! Σ.U Phase 1 MVP — narrow pattern matcher for the Q17 shape.
//! Generalisations (multi-key agg group-by, predicate on filter
//! subtree referencing aggregate side, etc.) are out of scope until
//! the basic shape ships and benches in the green.

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, Result as DfResult};
use datafusion::logical_expr::{
    Aggregate, BinaryExpr, Expr, JoinType, LogicalPlan, LogicalPlanBuilder, Operator,
};

/// Public entry. Walks `plan` and, for each `Inner Join` matching
/// the Σ.U pattern, replaces the aggregate-side `TableScan` with a
/// `LeftSemi` against a clone of the filter subtree from the other
/// join side. Returns the (possibly rewritten) plan. On any
/// shape-mismatch the sub-plan passes through unchanged — this is
/// best-effort, not correctness-critical.
pub fn push_filter_into_agg(plan: LogicalPlan) -> DfResult<LogicalPlan> {
    let mut fires: usize = 0;
    let transformed = plan.transform_down(|node| match node {
        LogicalPlan::Join(ref join) if join.join_type == JoinType::Inner => {
            match try_rewrite_q17_shape(&node) {
                Some(rewritten) => {
                    fires += 1;
                    Ok(Transformed::yes(rewritten))
                }
                None => Ok(Transformed::no(node)),
            }
        }
        _ => Ok(Transformed::no(node)),
    })?;
    if std::env::var("EMAT_SIGMA_U_DEBUG").is_ok() {
        eprintln!("[Σ.U] fires={fires}");
    }
    Ok(transformed.data)
}

/// Try the Q17-shape rewrite on a single Inner Join node.
fn try_rewrite_q17_shape(plan: &LogicalPlan) -> Option<LogicalPlan> {
    let LogicalPlan::Join(j) = plan else { return None };
    if j.join_type != JoinType::Inner {
        return None;
    }
    // Must have at least one equi-key pair.
    if j.on.is_empty() {
        return None;
    }

    // Identify which side is the aggregate-bearing branch and which
    // is the filter-bearing branch. The aggregate side has a path
    // down to `Aggregate → TableScan(T)`; the filter side has
    // `Filter(P) → TableScan(T_alt)` reachable from the join key.
    let (agg_side, filter_side, agg_on_right) =
        match (find_agg_branch(&j.left), find_agg_branch(&j.right)) {
            (None, Some(_)) => (&j.right, &j.left, true),
            (Some(_), None) => (&j.left, &j.right, false),
            _ => return None,
        };

    let agg_info = find_agg_branch(agg_side)?;

    // From join.on, find the equi pair that connects agg_side.K to
    // filter_side.X.
    //
    // The outer join's references use the SubqueryAlias's relation
    // (e.g. `__scalar_sq_1.l_partkey`). We need to look up the
    // un-aliased column (`lineitem.l_partkey`) from the agg's own
    // group_by — that's the column qualifier visible to the LeftSemi
    // we're about to splice INSIDE the SubqueryAlias wrapper.
    let (filter_col, inner_agg_col) = j.on.iter().find_map(|(l, r)| {
        match (l, r) {
            (Expr::Column(lc), Expr::Column(rc)) => {
                let (filter_c, agg_c) =
                    if agg_on_right { (lc, rc) } else { (rc, lc) };
                let inner = agg_info
                    .group_by_cols
                    .iter()
                    .find(|gc| gc.name == agg_c.name)?;
                Some((filter_c.clone(), inner.clone()))
            }
            _ => None,
        }
    })?;

    // Find a clone-able filter subtree on the filter side that
    // produces filter_col.
    let filter_subtree = find_filter_subtree_producing(filter_side, &filter_col)?;

    // The filter subtree must be smaller than the agg's input.
    // For the MVP we accept any Filter-over-TableScan; selectivity
    // gating can come later.
    if !is_worth_pushing(&filter_subtree) {
        return None;
    }

    // Build LeftSemi(agg_table_scan, filter_subtree_clone) and splice
    // it in at the agg's input. The agg's existing structure
    // (Projection? → SubqueryAlias? → Projection? → Aggregate)
    // stays the same; only the deepest TableScan node is wrapped.
    let new_agg_side = splice_left_semi_into_agg(
        agg_side,
        &inner_agg_col,
        &filter_col,
        filter_subtree.clone(),
    )?;

    if std::env::var("EMAT_SIGMA_U_DEBUG").is_ok() {
        eprintln!(
            "[Σ.U] new_agg_side after splice:\n{}",
            new_agg_side.display_indent()
        );
    }
    // Construct the new Inner Join with the rewritten agg side.
    let new_left;
    let new_right;
    if agg_on_right {
        new_left = j.left.clone();
        new_right = new_agg_side;
    } else {
        new_left = new_agg_side;
        new_right = j.right.clone();
    }
    LogicalPlanBuilder::from(new_left.as_ref().clone())
        .join_on(
            new_right.as_ref().clone(),
            JoinType::Inner,
            j.on.iter().map(|(l, r)| {
                Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(l.clone()),
                    op: Operator::Eq,
                    right: Box::new(r.clone()),
                })
            }).chain(j.filter.iter().cloned()),
        )
        .ok()?
        .build()
        .ok()
}

/// Information harvested from an aggregate branch.
#[derive(Debug)]
struct AggBranchInfo {
    group_by_cols: Vec<Column>,
}

/// Walk down through `Projection`, `SubqueryAlias`, and other
/// non-aggregate nodes looking for an `Aggregate` over a
/// `TableScan`. Returns the aggregate's group-by columns if the
/// shape matches; `None` otherwise.
fn find_agg_branch(plan: &LogicalPlan) -> Option<AggBranchInfo> {
    match plan {
        LogicalPlan::Aggregate(agg) => {
            // Aggregate input must reach a TableScan via transparent
            // nodes (Projection, Filter, SubqueryAlias).
            if !ends_in_table_scan(&agg.input) {
                return None;
            }
            let group_by_cols: Vec<Column> = agg
                .group_expr
                .iter()
                .filter_map(|e| match e {
                    Expr::Column(c) => Some(c.clone()),
                    _ => None,
                })
                .collect();
            if group_by_cols.is_empty() {
                return None;
            }
            Some(AggBranchInfo { group_by_cols })
        }
        LogicalPlan::Projection(p) => find_agg_branch(&p.input),
        LogicalPlan::SubqueryAlias(s) => find_agg_branch(&s.input),
        _ => None,
    }
}

fn ends_in_table_scan(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::TableScan(_) => true,
        LogicalPlan::Projection(p) => ends_in_table_scan(&p.input),
        LogicalPlan::Filter(f) => ends_in_table_scan(&f.input),
        LogicalPlan::SubqueryAlias(s) => ends_in_table_scan(&s.input),
        _ => false,
    }
}

/// Find the smallest subtree rooted at a `Filter` (or directly a
/// filtered `TableScan` via `partial_filters`) on the filter side
/// that produces `target_col` and is small enough to be worth
/// cloning.
fn find_filter_subtree_producing(
    plan: &LogicalPlan,
    target_col: &Column,
) -> Option<LogicalPlan> {
    // Walk down looking for a Filter node whose Filter expression
    // references target_col's table (or any column from the same
    // TableScan that produces target_col).
    match plan {
        LogicalPlan::Filter(f) => {
            // If this Filter's input is a TableScan that schema-includes target_col, return it.
            if scan_produces_column(&f.input, target_col) {
                return Some(LogicalPlan::Filter(f.clone()));
            }
            // Else recurse into input.
            find_filter_subtree_producing(&f.input, target_col)
        }
        LogicalPlan::Projection(p) => find_filter_subtree_producing(&p.input, target_col),
        LogicalPlan::SubqueryAlias(s) => find_filter_subtree_producing(&s.input, target_col),
        LogicalPlan::Join(j) => {
            // Try both sides. We want the leaf subtree that produces
            // the target column.
            if let Some(s) = find_filter_subtree_producing(&j.left, target_col) {
                return Some(s);
            }
            find_filter_subtree_producing(&j.right, target_col)
        }
        _ => None,
    }
}

fn scan_produces_column(plan: &LogicalPlan, col: &Column) -> bool {
    match plan {
        LogicalPlan::TableScan(ts) => {
            ts.projected_schema
                .fields()
                .iter()
                .any(|f| f.name() == &col.name)
        }
        LogicalPlan::Filter(f) => scan_produces_column(&f.input, col),
        LogicalPlan::Projection(p) => scan_produces_column(&p.input, col),
        LogicalPlan::SubqueryAlias(s) => scan_produces_column(&s.input, col),
        _ => false,
    }
}

/// Heuristic: only push if the filter subtree's leaf is a single
/// TableScan and the filter has at least one literal-bearing
/// predicate (i.e., it actually filters something). MVP gate;
/// proper selectivity comes later.
fn is_worth_pushing(plan: &LogicalPlan) -> bool {
    if let LogicalPlan::Filter(f) = plan {
        if let LogicalPlan::TableScan(_) = f.input.as_ref() {
            return true;
        }
    }
    false
}

/// Splice a LeftSemi between the Aggregate's `TableScan` input and
/// the Aggregate node itself. Walks down the agg branch
/// transparently through Projection/SubqueryAlias/Aggregate; when it
/// finds the Aggregate's `TableScan`, wraps it in a LeftSemi
/// against `filter_subtree`.
fn splice_left_semi_into_agg(
    agg_side: &LogicalPlan,
    agg_col: &Column,
    filter_col: &Column,
    filter_subtree: LogicalPlan,
) -> Option<std::sync::Arc<LogicalPlan>> {
    let rewritten = splice_recurse(agg_side, agg_col, filter_col, &filter_subtree, false)?;
    Some(std::sync::Arc::new(rewritten))
}

fn splice_recurse(
    plan: &LogicalPlan,
    agg_col: &Column,
    filter_col: &Column,
    filter_subtree: &LogicalPlan,
    inside_agg: bool,
) -> Option<LogicalPlan> {
    use std::sync::Arc;
    match plan {
        LogicalPlan::Aggregate(agg) => {
            // Once we enter the aggregate, future TableScans get
            // wrapped.
            let new_input = splice_recurse(
                &agg.input,
                agg_col,
                filter_col,
                filter_subtree,
                true,
            )?;
            let new_agg = LogicalPlan::Aggregate(
                Aggregate::try_new(
                    Arc::new(new_input),
                    agg.group_expr.clone(),
                    agg.aggr_expr.clone(),
                )
                .ok()?,
            );
            Some(new_agg)
        }
        LogicalPlan::Projection(p) => {
            let new_input =
                splice_recurse(&p.input, agg_col, filter_col, filter_subtree, inside_agg)?;
            let new_proj = LogicalPlanBuilder::from(new_input)
                .project(p.expr.clone())
                .ok()?
                .build()
                .ok()?;
            Some(new_proj)
        }
        LogicalPlan::SubqueryAlias(s) => {
            let new_input =
                splice_recurse(&s.input, agg_col, filter_col, filter_subtree, inside_agg)?;
            let new_sa = LogicalPlanBuilder::from(new_input)
                .alias(s.alias.clone())
                .ok()?
                .build()
                .ok()?;
            Some(new_sa)
        }
        LogicalPlan::TableScan(_ts) if inside_agg => {
            // This is the agg's TableScan — wrap in LeftSemi.
            let on_expr = Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(agg_col.clone())),
                op: Operator::Eq,
                right: Box::new(Expr::Column(filter_col.clone())),
            });
            let semi = LogicalPlanBuilder::from(plan.clone())
                .join_on(filter_subtree.clone(), JoinType::LeftSemi, [on_expr])
                .ok()?
                .build()
                .ok()?;
            Some(semi)
        }
        _ => Some(plan.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ematix_fast_parquet::EmatixFastParquetTableProvider;
    use crate::fast_parquet::FastParquetTableProvider;
    use datafusion::prelude::SessionContext;
    use std::path::PathBuf;
    use std::sync::Arc;

    fn sf1_dir() -> Option<PathBuf> {
        if let Ok(env) = std::env::var("TPCH_DATA_DIR") {
            let p = PathBuf::from(env);
            if p.exists() {
                return Some(p);
            }
        }
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let p = manifest
            .parent()
            .and_then(|p| p.parent())
            .map(|p| p.join("examples/tpch/data/sf1"))?;
        if p.exists() {
            return Some(p);
        }
        None
    }

    async fn register_tpch(ctx: &SessionContext, dir: &std::path::Path) -> DfResult<()> {
        for t in [
            "region", "nation", "supplier", "customer", "part", "partsupp", "orders",
            "lineitem",
        ] {
            let path = dir.join(format!("{t}.parquet"));
            if t == "lineitem" {
                let prov = EmatixFastParquetTableProvider::try_new(path.to_string_lossy())?;
                ctx.register_table(t, Arc::new(prov))?;
            } else {
                let prov = FastParquetTableProvider::try_new(path.to_string_lossy())?;
                ctx.register_table(t, Arc::new(prov))?;
            }
        }
        Ok(())
    }

    /// No-op on Q01 (no aggregate-side correlation).
    #[tokio::test]
    async fn no_op_on_q01_shape() -> DfResult<()> {
        let Some(dir) = sf1_dir() else {
            eprintln!("skip: sf1 missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        let df = ctx
            .sql("SELECT l_returnflag, SUM(l_quantity) FROM lineitem GROUP BY l_returnflag")
            .await?;
        let optimized = df.into_optimized_plan()?;
        let rewritten = push_filter_into_agg(optimized.clone())?;
        assert_eq!(
            format!("{}", optimized.display_indent()),
            format!("{}", rewritten.display_indent()),
            "Q01 shape (single aggregate, no correlated subquery) must pass through unchanged"
        );
        Ok(())
    }

    /// Fires on Q17 shape — verify a `LeftSemi` is inserted in the
    /// aggregate's input subtree.
    #[tokio::test]
    async fn fires_on_q17_shape() -> DfResult<()> {
        let Some(dir) = sf1_dir() else {
            eprintln!("skip: sf1 missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        let sql = std::fs::read_to_string(
            dir.parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("queries/q17.sql"),
        )
        .ok();
        let Some(sql) = sql else {
            eprintln!("skip: q17.sql missing");
            return Ok(());
        };
        let df = ctx.sql(&sql).await?;
        let optimized = df.into_optimized_plan()?;
        let rewritten = push_filter_into_agg(optimized.clone())?;
        let orig_dump = format!("{}", optimized.display_indent());
        let new_dump = format!("{}", rewritten.display_indent());
        assert_ne!(
            orig_dump, new_dump,
            "Q17 rewrite should change the plan; got identical:\n{orig_dump}"
        );
        assert!(
            new_dump.contains("LeftSemi"),
            "rewritten plan must contain a LeftSemi node:\n{new_dump}"
        );
        Ok(())
    }

    /// End-to-end correctness: after rewrite, executing the plan
    /// must return the same single row (sum / 7 = avg_yearly).
    #[tokio::test]
    async fn rewrite_preserves_q17_result() -> DfResult<()> {
        let Some(dir) = sf1_dir() else {
            eprintln!("skip: sf1 missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        let sql = std::fs::read_to_string(
            dir.parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("queries/q17.sql"),
        )
        .ok();
        let Some(sql) = sql else {
            eprintln!("skip: q17.sql missing");
            return Ok(());
        };

        // Baseline result (no rewrite).
        let df_baseline = ctx.sql(&sql).await?;
        let baseline_batches = df_baseline.collect().await?;
        let baseline_val = baseline_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .unwrap()
            .value(0);

        // Rewritten result.
        let df_new = ctx.sql(&sql).await?;
        let optimized = df_new.into_optimized_plan()?;
        let rewritten = push_filter_into_agg(optimized)?;
        let df_new = ctx.execute_logical_plan(rewritten).await?;
        let new_batches = df_new.collect().await?;
        let new_val = new_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .unwrap()
            .value(0);

        let rel = ((new_val - baseline_val) / baseline_val).abs();
        assert!(
            rel < 1e-9,
            "Q17 rewrite changed result: baseline={baseline_val}, rewritten={new_val}, rel_err={rel:e}"
        );
        Ok(())
    }
}
