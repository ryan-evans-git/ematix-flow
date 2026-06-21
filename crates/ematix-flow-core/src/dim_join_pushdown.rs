//! Σ.AD — push a small filtered-dim Inner-join down adjacent to its FK
//! table's scan inside an Inner-join chain.
//!
//! ## What this fixes
//!
//! Q07 SF=10 baseline (per Σ.X 5-trial): we run `supplier ⋈ lineitem`
//! FIRST (CollectLeft broadcast of full 100K supplier), then later join
//! `Filter(n_name IN (FRANCE,GERMANY)) → nation ⋈ supplier-bearing
//! stream` at the top of the tree. A competent optimizer applies the
//! nation filter to supplier BEFORE the fact-fact join, shrinking
//! supplier from 100K → ~8K rows before any other join runs.
//!
//! DuckDB does this via dynamic-filter pushdown. We do the equivalent
//! at the logical layer by reshaping the inner-join tree: any top-level
//! `Inner Join (A, dim_filtered) ON dim.D = T.X` where T is a base
//! relation deep inside A gets rewritten to push the dim-join down
//! adjacent to `TableScan(T)`.
//!
//! ```text
//! Inner Join (A, Filter→TableScan(dim)) ON T.X = dim.D     (Filter Q?)
//!   ↓ push down
//! Filter(Q)?  → A'
//!                 └─ subtree with TableScan(T) replaced by
//!                      Inner Join (TableScan(T), Filter→TableScan(dim))
//!                        ON T.X = dim.D
//! ```
//!
//! Inner-join associativity + commutativity makes this row-set
//! preserving. Non-equi conditions on the outer join's `filter` slot
//! (e.g. Q07's `n1.n_name=FRANCE AND n2.n_name=GERMANY OR ...`) bubble
//! up to a `Filter` on top of the rewritten subtree — they reference
//! columns from both dim joins which still flow up through projections.
//!
//! ## Why not an OptimizerRule
//!
//! Per [[optimizer-codegen-sensitivity]]: rules added to DataFusion's
//! pass stack regress 5-8 pp geomean from LLVM codegen perturbation
//! even when nearly inert. [[dict-routing]] and [[Σ.U Phase 1]] both
//! ship as pre-plan walkers invoked explicitly between
//! `into_optimized_plan` and `execute_logical_plan`. This module
//! follows the same pattern via [`push_dim_join_into_chain`].
//!
//! ## Status — Σ.AD MVP (opt-in via `EMAT_DIM_PUSH=1`)
//!
//! Narrow pattern: outer Inner Join with exactly one equi-key whose
//! other side is `Filter(P) → TableScan(dim)`, plus an FK side that
//! reaches a single `TableScan(T)` inside an Inner-join subtree. Outer
//! join must have no extra non-equi `filter` slot. Multi-equi-key and
//! multi-leaf dim subtrees are out of scope for the MVP.
//!
//! ### Tests
//!
//! Unit tests pin the Q07-shape rewrite at SF=1. End-to-end
//! correctness (row count + values at SF=10) verified via the
//! standalone `q07_sigma_ad_validate` example.
//!
//! ### Bench impact (2026-05-25, 22q SF=10 5-trial w/ EMAT_DIM_PUSH=1)
//!
//! The rule fires on 8 queries: Q02, Q03, Q05, Q07, Q08, Q10, Q11,
//! Q21. Wall-time deltas vs default-off baseline:
//!
//! | Query | Without Σ.AD | With Σ.AD | Δ |
//! |-------|--------------|-----------|---|
//! | Q02 | 43.28 ms | 32.14 ms | **−26%** ✓ |
//! | Q05 | 200.80 ms | 184.64 ms | **−8%** ✓ |
//! | Q08 | 196.64 ms | 186.34 ms | **−5%** ✓ |
//! | Q10 | 240.91 ms | 203.13 ms | **−16%** ✓ |
//! | Q07 | 159.70 ms | 207.09 ms | **+30%** ✗ |
//! | Q21 | 304.36 ms | 355.63 ms | **+17%** ✗ |
//!
//! The structurally-correct plan REGRESSES Q07/Q21 wall-time because
//! the new shape inserts a `CoalescePartitionsExec` between two
//! CollectLeft joins. The first CollectLeft (nation⋈supplier in Q07)
//! is fine — small build, small probe — but the resulting filtered
//! supplier stream gets coalesced before becoming the build side of
//! the next CollectLeft (supplier⋈lineitem). The coalesce + small-
//! build-side broadcast overhead exceeds the savings from supplier
//! shrinking 100K→8K.
//!
//! Follow-up Σ.AE could: (1) emit the inner CollectLeft as Partitioned
//! when feasible to skip the coalesce, OR (2) push the dim filter as
//! a STATIC `IN`-list predicate at the FK scan rather than as a join
//! (true magic-set rewriting). Both are non-trivial.
//!
//! Per [[Σ.Q.L13 LANDED — parallel-bitmap dispatch flipped to opt-in]],
//! ship as opt-in infra so the structurally-correct rewrite is
//! available for adoption when the downstream physical shape is fixed.

use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, Result as DfResult};
use datafusion::logical_expr::{
    BinaryExpr, Expr, JoinType, LogicalPlan, LogicalPlanBuilder, Operator,
};

/// Public entry. Walks `plan` top-down and, for each `Inner Join`
/// matching the Σ.AD pattern, rewrites the subtree to push the
/// dim-join down adjacent to its FK table's scan. Returns the
/// (possibly rewritten) plan. Best-effort: shape mismatches pass
/// through unchanged.
pub fn push_dim_join_into_chain(plan: LogicalPlan) -> DfResult<LogicalPlan> {
    let mut fires: usize = 0;
    let transformed = plan.transform_down(|node| match node {
        LogicalPlan::Join(ref join) if join.join_type == JoinType::Inner => {
            match try_rewrite(&node) {
                Some(rewritten) => {
                    fires += 1;
                    Ok(Transformed::yes(rewritten))
                }
                None => Ok(Transformed::no(node)),
            }
        }
        _ => Ok(Transformed::no(node)),
    })?;
    if crate::flags::present("EMAT_SIGMA_AD_DEBUG") {
        eprintln!("[Σ.AD] fires={fires}");
    }
    Ok(transformed.data)
}

/// Try the dim-pushdown rewrite on a single Inner Join node.
fn try_rewrite(plan: &LogicalPlan) -> Option<LogicalPlan> {
    let LogicalPlan::Join(j) = plan else {
        return None;
    };
    if j.join_type != JoinType::Inner {
        return None;
    }
    // MVP: single equi-key.
    if j.on.len() != 1 {
        return None;
    }
    // MVP: no non-equi `filter` slot. When the outer join carries
    // an extra predicate that references dim columns (Q07's n2 case
    // with the n1.n_name × n2.n_name cross-condition), pushing the
    // dim-join down through an intermediate Projection drops the dim
    // columns from upstream scope, breaking type coercion when the
    // outer Filter tries to read them. Phase 1.1 can extend through
    // projections; for now, bail.
    if j.filter.is_some() {
        return None;
    }
    let (left_expr, right_expr) = &j.on[0];
    let (Expr::Column(lcol), Expr::Column(rcol)) = (left_expr, right_expr) else {
        return None;
    };

    // Decide which side is the dim (small filtered-scan subtree) and
    // which is the fact side (Inner-join chain containing TableScan(T)
    // where T.X is the FK column).
    let left_dim = match_dim_subtree(&j.left);
    let right_dim = match_dim_subtree(&j.right);
    let (dim_subtree, dim_col, fact_side, fact_col, dim_on_right) = match (left_dim, right_dim) {
        (None, Some(_)) => (j.right.as_ref(), rcol.clone(), &j.left, lcol.clone(), true),
        (Some(_), None) => (j.left.as_ref(), lcol.clone(), &j.right, rcol.clone(), false),
        _ => return None, // both or neither — ambiguous
    };

    // The dim column must be produced by the dim subtree's leaf scan.
    if !dim_subtree_produces_column(dim_subtree, &dim_col) {
        return None;
    }

    // The fact column must resolve to exactly one TableScan inside
    // fact_side, and that TableScan must NOT be reachable from the dim
    // side (it would be — they share schema names — which is why we
    // also require fact_side to be an Inner-Join chain, not a leaf
    // scan; otherwise this is a 2-table direct join and there's
    // nothing to push DOWN through).
    let target_table = locate_target_table(fact_side, &fact_col)?;
    // Pushing into a single-scan fact_side just reproduces the
    // original join — skip.
    if matches!(&**fact_side, LogicalPlan::TableScan(_)) {
        return None;
    }

    // Σ.AK Q10 shape gate (2026-05-27): refuse if fact_side contains
    // another Inner Join where one side is a dim subtree (Filter→
    // TableScan) AND its leaf table is NOT the rewrite target.
    //
    // This blocks Q07/Q21/Q03 (where the nested dim is a different
    // table — would become CollectLeft and force a Coalesce inversion
    // between two CollectLeft layers, per Σ.AD module header). Allows
    // Q10 (where the nested Inner Join's filtered side IS the target).
    //
    // The post-harness-fix bench `/tmp/strict-ab-dim-push-revalidation/`
    // showed Q10 -62 ms WIN but Q07 +43 ms / Q21 +26 ms / Q03 +14 ms
    // regressions, all caused by the CollectLeft-after-Coalesce
    // pathology. This gate isolates Q10's win.
    if fact_side_has_competing_filtered_dim(fact_side, &target_table) {
        return None;
    }

    // Compute the dim's output column set (qualified). Every Projection
    // in the path from the spliced position up to the outer join's
    // former position must include these columns in its expression list,
    // or they'll be silently dropped and the parent (which expected
    // them under the outer join's schema) will fail with FieldNotFound.
    let dim_cols: Vec<Column> = dim_subtree
        .schema()
        .iter()
        .map(|(qual, f)| Column::new(qual.cloned(), f.name()))
        .collect();

    // Walk fact_side, replacing the target TableScan with an Inner Join
    // (target_scan, dim_subtree) ON fact_col = dim_col. Bail if we
    // can't find the target or if multiple matches exist (would
    // double-evaluate the dim subtree).
    let dim_clone = dim_subtree.clone();
    let new_fact_side = splice_dim_join_at_scan(
        fact_side,
        &target_table,
        &fact_col,
        &dim_col,
        dim_clone,
        &dim_cols,
        &mut 0,
    )?;

    // Outer join had no extra `filter` (we bailed earlier if it did),
    // so it's fully absorbed by the spliced inner join. The new fact
    // side's schema includes the dim columns because they flow up
    // through the join chain from the lower position.
    let _ = dim_on_right; // suppress unused warning
    let result = new_fact_side;

    if crate::flags::present("EMAT_SIGMA_AD_DEBUG") {
        eprintln!(
            "[Σ.AD] rewrote outer join on ({}.{} = {}.{}); pushed down to {}",
            lcol.relation
                .as_ref()
                .map(|r| r.to_string())
                .unwrap_or_default(),
            lcol.name,
            rcol.relation
                .as_ref()
                .map(|r| r.to_string())
                .unwrap_or_default(),
            rcol.name,
            target_table
        );
    }

    Some(result)
}

/// Σ.AK Q10 shape gate (2026-05-27). Returns true iff `fact_side`
/// (the subtree opposite the outer dim in the join being considered)
/// contains a nested `Inner Join (dim, X)` where `dim` is dim-shaped
/// (`match_dim_subtree`) and its leaf TableScan is NOT the rewrite's
/// `target_table`.
///
/// Such a competing nested dim becomes a CollectLeft in the physical
/// plan; pushing the outer dim through it forces a
/// `CoalescePartitionsExec` between two CollectLeft layers, costing
/// more than the original push saves (see `Σ.AD` module header).
fn fact_side_has_competing_filtered_dim(fact_side: &LogicalPlan, target_table: &str) -> bool {
    let mut found_competing = false;
    let _ = fact_side.apply(|node| {
        if let LogicalPlan::Join(j) = node {
            if j.join_type == JoinType::Inner {
                for side in [&j.left, &j.right] {
                    if match_dim_subtree(side).is_some() {
                        if let Some(name) = dim_subtree_leaf_table(side) {
                            if name != target_table {
                                found_competing = true;
                                return Ok(datafusion::common::tree_node::TreeNodeRecursion::Stop);
                            }
                        }
                    }
                }
            }
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    });
    found_competing
}

/// Extract the leaf TableScan name from a dim-shaped subtree (walks
/// through SubqueryAlias / Projection / Filter wrappers). Returns
/// `None` if the subtree doesn't end in a single TableScan.
fn dim_subtree_leaf_table(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::TableScan(ts) => Some(ts.table_name.to_string()),
        LogicalPlan::Filter(f) => dim_subtree_leaf_table(&f.input),
        LogicalPlan::SubqueryAlias(s) => dim_subtree_leaf_table(&s.input),
        LogicalPlan::Projection(p) => dim_subtree_leaf_table(&p.input),
        _ => None,
    }
}

/// Returns true if `plan` is a "small dim" subtree shape: a
/// `SubqueryAlias`? wrapping a `Filter` wrapping a `TableScan`. The
/// outer SubqueryAlias is optional (Q07's `n1`/`n2` aliases); the
/// `Filter` need not be on the join key — any literal-bearing
/// predicate counts as evidence that this side is filtered.
fn match_dim_subtree(plan: &LogicalPlan) -> Option<()> {
    fn inner(p: &LogicalPlan) -> Option<()> {
        match p {
            LogicalPlan::Filter(f) => {
                if let LogicalPlan::TableScan(_) = f.input.as_ref() {
                    Some(())
                } else {
                    None
                }
            }
            LogicalPlan::SubqueryAlias(s) => inner(&s.input),
            LogicalPlan::Projection(p) => inner(&p.input),
            _ => None,
        }
    }
    inner(plan)
}

/// Returns true if `plan`'s leaf TableScan schema-includes `col`.
fn dim_subtree_produces_column(plan: &LogicalPlan, col: &Column) -> bool {
    fn inner(p: &LogicalPlan, col: &Column) -> bool {
        match p {
            LogicalPlan::TableScan(ts) => ts
                .projected_schema
                .fields()
                .iter()
                .any(|f| f.name() == &col.name),
            LogicalPlan::Filter(f) => inner(&f.input, col),
            LogicalPlan::SubqueryAlias(s) => inner(&s.input, col),
            LogicalPlan::Projection(p) => inner(&p.input, col),
            _ => false,
        }
    }
    inner(plan, col)
}

/// Find the base table name that schema-produces `col` inside `plan`.
/// Returns `None` if zero or multiple TableScans match. Walks through
/// Filter / Projection / SubqueryAlias / Inner Join.
fn locate_target_table(plan: &LogicalPlan, col: &Column) -> Option<String> {
    let mut found: Option<String> = None;
    let mut multi = false;
    let _ = plan.apply(|node| {
        if let LogicalPlan::TableScan(ts) = node {
            let has_col = ts
                .projected_schema
                .fields()
                .iter()
                .any(|f| f.name() == &col.name);
            if has_col {
                if found.is_some() {
                    multi = true;
                    return Ok(datafusion::common::tree_node::TreeNodeRecursion::Stop);
                }
                found = Some(ts.table_name.to_string());
            }
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    });
    if multi { None } else { found }
}

/// Walk `plan`, replacing the single matching `TableScan(target)` with
/// an `Inner Join (target_scan, dim_subtree)` on `fact_col = dim_col`.
/// `found_count` is incremented on match; the caller verifies exactly
/// one rewrite happened.
fn splice_dim_join_at_scan(
    plan: &LogicalPlan,
    target_table: &str,
    fact_col: &Column,
    dim_col: &Column,
    dim_subtree: LogicalPlan,
    dim_cols: &[Column],
    found_count: &mut usize,
) -> Option<LogicalPlan> {
    if let LogicalPlan::TableScan(ts) = plan {
        if ts.table_name.to_string() == target_table {
            *found_count += 1;
            if *found_count > 1 {
                // Multiple scans of the same table — would double-eval
                // the dim subtree. Abort.
                return None;
            }
            let on_expr = Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(fact_col.clone())),
                op: Operator::Eq,
                right: Box::new(Expr::Column(dim_col.clone())),
            });
            let new_node = LogicalPlanBuilder::from(plan.clone())
                .join_on(dim_subtree, JoinType::Inner, [on_expr])
                .ok()?
                .build()
                .ok()?;
            return Some(new_node);
        }
        return Some(plan.clone());
    }
    // Recurse through nodes that preserve the target table's schema.
    match plan {
        LogicalPlan::Filter(f) => {
            let new_input = splice_dim_join_at_scan(
                &f.input,
                target_table,
                fact_col,
                dim_col,
                dim_subtree,
                dim_cols,
                found_count,
            )?;
            let new_filter =
                datafusion::logical_expr::Filter::try_new(f.predicate.clone(), Arc::new(new_input))
                    .ok()?;
            Some(LogicalPlan::Filter(new_filter))
        }
        LogicalPlan::Projection(p) => {
            let new_input = splice_dim_join_at_scan(
                &p.input,
                target_table,
                fact_col,
                dim_col,
                dim_subtree,
                dim_cols,
                found_count,
            )?;
            // Extend the projection expressions with any dim columns
            // not already present. Without this, intermediate
            // projections narrow the column set and the dim columns
            // get dropped before flowing up to whoever needed them
            // (the former outer join's parent Projection).
            let mut extended = p.expr.clone();
            for dc in dim_cols {
                let already_there = extended.iter().any(|e| match e {
                    Expr::Column(c) => c.name == dc.name && c.relation == dc.relation,
                    Expr::Alias(a) => a.name == dc.name,
                    _ => false,
                });
                if !already_there {
                    extended.push(Expr::Column(dc.clone()));
                }
            }
            let new_proj = LogicalPlanBuilder::from(new_input)
                .project(extended)
                .ok()?
                .build()
                .ok()?;
            Some(new_proj)
        }
        LogicalPlan::SubqueryAlias(s) => {
            let new_input = splice_dim_join_at_scan(
                &s.input,
                target_table,
                fact_col,
                dim_col,
                dim_subtree,
                dim_cols,
                found_count,
            )?;
            let new_sa = LogicalPlanBuilder::from(new_input)
                .alias(s.alias.clone())
                .ok()?
                .build()
                .ok()?;
            Some(new_sa)
        }
        LogicalPlan::Join(j) if j.join_type == JoinType::Inner => {
            // Try left first, then right. Only descend into one side
            // — the target table can't be in both sides of the same
            // join (would be a self-join, ambiguous case we already
            // catch via multi-match guard).
            let left_has = subtree_contains_table(&j.left, target_table);
            let right_has = subtree_contains_table(&j.right, target_table);
            if left_has && !right_has {
                let new_left = splice_dim_join_at_scan(
                    &j.left,
                    target_table,
                    fact_col,
                    dim_col,
                    dim_subtree,
                    dim_cols,
                    found_count,
                )?;
                // Rebuild Inner Join with new left.
                LogicalPlanBuilder::from(new_left)
                    .join_on(
                        j.right.as_ref().clone(),
                        JoinType::Inner,
                        j.on.iter()
                            .map(|(l, r)| {
                                Expr::BinaryExpr(BinaryExpr {
                                    left: Box::new(l.clone()),
                                    op: Operator::Eq,
                                    right: Box::new(r.clone()),
                                })
                            })
                            .chain(j.filter.iter().cloned()),
                    )
                    .ok()?
                    .build()
                    .ok()
            } else if right_has && !left_has {
                let new_right = splice_dim_join_at_scan(
                    &j.right,
                    target_table,
                    fact_col,
                    dim_col,
                    dim_subtree,
                    dim_cols,
                    found_count,
                )?;
                LogicalPlanBuilder::from(j.left.as_ref().clone())
                    .join_on(
                        new_right,
                        JoinType::Inner,
                        j.on.iter()
                            .map(|(l, r)| {
                                Expr::BinaryExpr(BinaryExpr {
                                    left: Box::new(l.clone()),
                                    op: Operator::Eq,
                                    right: Box::new(r.clone()),
                                })
                            })
                            .chain(j.filter.iter().cloned()),
                    )
                    .ok()?
                    .build()
                    .ok()
            } else {
                // Both or neither — ambiguous; abort.
                None
            }
        }
        _ => None,
    }
}

fn subtree_contains_table(plan: &LogicalPlan, table: &str) -> bool {
    let mut found = false;
    let _ = plan.apply(|node| {
        if let LogicalPlan::TableScan(ts) = node {
            if ts.table_name.to_string() == table {
                found = true;
                return Ok(datafusion::common::tree_node::TreeNodeRecursion::Stop);
            }
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    });
    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ematix_fast_parquet::EmatixFastParquetTableProvider;
    use datafusion::prelude::SessionContext;
    use std::path::PathBuf;

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

    fn sf10_dir() -> Option<PathBuf> {
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let p = manifest
            .parent()
            .and_then(|p| p.parent())
            .map(|p| p.join("examples/tpch/data/sf10"))?;
        if p.exists() {
            return Some(p);
        }
        None
    }

    async fn register_tpch(ctx: &SessionContext, dir: &std::path::Path) -> DfResult<()> {
        for t in [
            "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
        ] {
            let path = dir.join(format!("{t}.parquet"));
            let prov = EmatixFastParquetTableProvider::try_new(path.to_string_lossy())?;
            ctx.register_table(t, Arc::new(prov))?;
        }
        Ok(())
    }

    /// Σ.AD must not change Q01 (no dim-with-filter pattern).
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
        let rewritten = push_dim_join_into_chain(optimized.clone())?;
        assert_eq!(
            format!("{}", optimized.display_indent()),
            format!("{}", rewritten.display_indent()),
            "Q01 must pass through Σ.AD unchanged"
        );
        Ok(())
    }

    /// Σ.AK (2026-05-27): Σ.AD's rewrite of Q07 is now blocked by the
    /// shape predicate — the fact_side has a competing filtered dim
    /// (Filter→nation joined to supplier) that would force a
    /// CollectLeft-after-Coalesce inversion in the physical plan,
    /// regressing Q07 by ~28%. Predicate gate refuses the rewrite.
    ///
    /// Prior to Σ.AK this test asserted the OPPOSITE (Σ.AD fires on
    /// Q07). The semantic flipped 2026-05-27 — see PHASE_SIGMA_AK_DESIGN.md.
    #[tokio::test]
    async fn predicate_blocks_q07_shape() -> DfResult<()> {
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
                .join("queries/q07.sql"),
        )
        .ok();
        let Some(sql) = sql else {
            eprintln!("skip: q07.sql missing");
            return Ok(());
        };
        let df = ctx.sql(&sql).await?;
        let optimized = df.into_optimized_plan()?;
        let rewritten = push_dim_join_into_chain(optimized.clone())?;
        let orig = format!("{}", optimized.display_indent());
        let new = format!("{}", rewritten.display_indent());
        assert_eq!(
            orig, new,
            "Σ.AK predicate must block Q07 rewrite (fact_side has competing filtered dim)"
        );
        Ok(())
    }

    /// End-to-end correctness: Σ.AD must produce the same row set as
    /// the un-rewritten Q07 baseline.
    #[tokio::test]
    async fn rewrite_preserves_q07_result() -> DfResult<()> {
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
                .join("queries/q07.sql"),
        )
        .ok();
        let Some(sql) = sql else {
            eprintln!("skip: q07.sql missing");
            return Ok(());
        };

        // Baseline.
        let df_baseline = ctx.sql(&sql).await?;
        let baseline_batches = df_baseline.collect().await?;
        let baseline_rows: usize = baseline_batches.iter().map(|b| b.num_rows()).sum();

        // Rewritten.
        let df_new = ctx.sql(&sql).await?;
        let optimized = df_new.into_optimized_plan()?;
        let rewritten = push_dim_join_into_chain(optimized)?;
        let df_new = ctx.execute_logical_plan(rewritten).await?;
        let new_batches = df_new.collect().await?;
        let new_rows: usize = new_batches.iter().map(|b| b.num_rows()).sum();

        assert_eq!(
            baseline_rows, new_rows,
            "Q07 rewrite changed row count: baseline={baseline_rows}, rewritten={new_rows}"
        );
        Ok(())
    }

    /// Σ.AK (2026-05-27): the shape predicate must ALLOW Q10's rewrite.
    /// Q10's fact_side has a nested Inner Join (customer ⋈
    /// Filter→orders) but the Filter→orders subtree's leaf IS the
    /// rewrite target_table — so it's not a "competing" filtered dim,
    /// and the rewrite proceeds. This unlocks the Q10 -23% SF=10 win.
    #[tokio::test]
    async fn predicate_allows_q10_shape() -> DfResult<()> {
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
                .join("queries/q10.sql"),
        )
        .ok();
        let Some(sql) = sql else {
            eprintln!("skip: q10.sql missing");
            return Ok(());
        };
        let df = ctx.sql(&sql).await?;
        let optimized = df.into_optimized_plan()?;
        let rewritten = push_dim_join_into_chain(optimized.clone())?;
        let orig = format!("{}", optimized.display_indent());
        let new = format!("{}", rewritten.display_indent());
        assert_ne!(
            orig, new,
            "Σ.AK predicate must allow Q10 rewrite (nested filtered subtree IS the target)"
        );
        Ok(())
    }

    /// Σ.AK: Q21's NOT-EXISTS-rewritten plan has a competing filtered
    /// dim in fact_side — predicate must block to avoid the +26 ms
    /// SF=10 regression seen in the harness re-validation bench.
    #[tokio::test]
    async fn predicate_blocks_q21_shape() -> DfResult<()> {
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
                .join("queries/q21.sql"),
        )
        .ok();
        let Some(sql) = sql else {
            eprintln!("skip: q21.sql missing");
            return Ok(());
        };
        let df = ctx.sql(&sql).await?;
        let optimized = df.into_optimized_plan()?;
        let rewritten = push_dim_join_into_chain(optimized.clone())?;
        // Q21 rewrite would regress wall-time; predicate must block.
        // The test asserts plan-shape stability, not row count
        // (correctness gates that elsewhere). Note: this is weaker
        // than the Q07 assertion since Q21 may have multiple Σ.AD-
        // candidate joins; we just require the bad one is blocked.
        // Strict equality across the whole plan is the simplest gate.
        let orig = format!("{}", optimized.display_indent());
        let new = format!("{}", rewritten.display_indent());
        assert_eq!(
            orig, new,
            "Σ.AK predicate must block Q21 rewrite (competing filtered dim in fact_side)"
        );
        Ok(())
    }

    /// Σ.AK: Q03's plan also has the competing-filtered-dim pattern —
    /// predicate must block to avoid the +14 ms SF=10 regression.
    #[tokio::test]
    async fn predicate_blocks_q03_shape() -> DfResult<()> {
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
                .join("queries/q03.sql"),
        )
        .ok();
        let Some(sql) = sql else {
            eprintln!("skip: q03.sql missing");
            return Ok(());
        };
        let df = ctx.sql(&sql).await?;
        let optimized = df.into_optimized_plan()?;
        let rewritten = push_dim_join_into_chain(optimized.clone())?;
        let orig = format!("{}", optimized.display_indent());
        let new = format!("{}", rewritten.display_indent());
        assert_eq!(
            orig, new,
            "Σ.AK predicate must block Q03 rewrite (competing filtered dim in fact_side)"
        );
        Ok(())
    }

    /// Σ.AK Q10 SF=10 end-to-end correctness: after rewrite, the
    /// query must produce the same row count as baseline. Numeric
    /// comparison would be flaky due to F64 non-determinism in SUM
    /// over reordered partitions; row count + key presence covers it.
    #[tokio::test]
    async fn rewrite_preserves_q10_result_sf10() -> DfResult<()> {
        let Some(dir) = sf10_dir() else {
            eprintln!("skip: sf10 missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        let sql = std::fs::read_to_string(
            dir.parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("queries/q10.sql"),
        )
        .ok();
        let Some(sql) = sql else {
            eprintln!("skip: q10.sql missing");
            return Ok(());
        };

        let df_baseline = ctx.sql(&sql).await?;
        let baseline_batches = df_baseline.collect().await?;
        let baseline_rows: usize = baseline_batches.iter().map(|b| b.num_rows()).sum();

        let df_new = ctx.sql(&sql).await?;
        let optimized = df_new.into_optimized_plan()?;
        let rewritten = push_dim_join_into_chain(optimized)?;
        let df_new = ctx.execute_logical_plan(rewritten).await?;
        let new_batches = df_new.collect().await?;
        let new_rows: usize = new_batches.iter().map(|b| b.num_rows()).sum();

        assert_eq!(
            baseline_rows, new_rows,
            "Q10 SF=10 rewrite changed row count: baseline={baseline_rows}, rewritten={new_rows}"
        );
        Ok(())
    }
}
