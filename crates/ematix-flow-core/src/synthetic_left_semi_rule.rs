//! Σ.Q.M — synthesise `LeftSemi` joins above `Inner` joins where one
//! side is a filtered dim and the other is a fact table.
//!
//! ## Why
//!
//! DuckDB closes TPC-H Q05 / Q07 / Q08 SF=10 by propagating dim-side
//! filters into fact-side scans as dynamic filters. Static analogue:
//! detect `Inner Join(dim_with_Filter ⋈ fact)` and wrap the fact side
//! with a redundant `LeftSemi`. Σ.Q.L10 (`PushDownLeftSemiRule`) then
//! pushes that semi down to wrap the fact `TableScan` directly,
//! filtering the fact rows BEFORE the outer Inner join runs.
//!
//! ```text
//! Before Σ.Q.M:
//!   Inner Join(dim_filt, fact, on=[(d_k, f_k)])
//!
//! After Σ.Q.M (the dim subtree is referenced twice):
//!   Inner Join(dim_filt, LeftSemi(fact, dim_filt, [(f_k, d_k)]),
//!              on=[(d_k, f_k)])
//!
//! After Σ.Q.L10 pushes the LeftSemi to the fact TableScan:
//!   Inner Join(dim_filt,
//!              <projection chain>(LeftSemi(fact_scan, dim_filt,
//!                                          [(f_k, d_k)])),
//!              on=[(d_k, f_k)])
//! ```
//!
//! ## Correctness
//!
//! `LeftSemi(L, R, K)` outputs every row of `L` for which `∃ r ∈ R:
//! K(l) = K(r)`. An `Inner Join(L, R, K)` already drops every L row
//! that doesn't satisfy that predicate (and emits one (l, r) pair per
//! matching r, but the *set of L rows allowed through* is identical to
//! the LeftSemi's output). Therefore adding a `LeftSemi(L, R, K)` above
//! the L input of `Inner Join(L, R, K)` is a no-op on the rows the
//! Inner emits.
//!
//! ## Cost gate
//!
//! Two gates, both must pass:
//!
//! 1. **Gate A — dim has a Filter.** The dim-side subtree must contain
//!    a `LogicalPlan::Filter` between its root and a TableScan. This
//!    is the static analogue of "the dim has been narrowed somehow."
//!    Without this gate we'd fire on `supplier ⋈ lineitem` (supplier
//!    unfiltered) and produce a net-negative bloom-on-FK shape (the
//!    same failure mode Σ.Q.L9 hit and Σ.Q.L13/L15 worked around).
//!
//! 2. **Gate B — fact base table is fact-class.** The fact side's
//!    base table must report `Statistics.num_rows() >= 1_000_000`. This
//!    ensures we're not adding a LeftSemi where the saving doesn't
//!    justify the duplicate dim-subtree evaluation. At TPC-H SF=10
//!    this gates IN lineitem (60M), orders (15M); gates OUT supplier
//!    (100K), customer (1.5M just barely), part (2M).
//!
//! Phase 1 scope restricts the dim subtree to a shallow shape
//! (`Filter → TableScan` or `Filter → Projection → TableScan`).
//!
//! ## Slice 2 (rejected 2026-05-23)
//!
//! Extending `dim_subtree_has_filter` to recurse through one level of
//! Inner Join (to cover Q07's `nation_filtered ⋈ supplier` cascade)
//! was attempted at SF=10. Result: 22q geomean degraded 0.80 → 1.02
//! (catastrophic regression). Q07 went from 159 ms → 187 ms (target
//! ≤145), Q08 from 202 ms → 272 ms (target ≤190). Root causes:
//! double-evaluation of joined dim subtree (DataFusion's CSE doesn't
//! share Join outputs), over-firing on shapes that don't benefit, and
//! the codegen-sensitivity tax compounding on top. See `[[sigma-qm-slice2-rejected]]`
//! in memory. Slice 1 (shallow Filter→Scan) stays as opt-in infra.
//!
//! ## Opt-in
//!
//! Per [[optimizer-codegen-sensitivity]] — installed via
//! `install_synthetic_left_semi_rule` only when `EMAT_SYNTHETIC_LEFT_SEMI=1`
//! is set (or via direct `with_optimizer_rule` for tests).

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::common::Result as DfResult;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::expr::Expr;
use datafusion::logical_expr::{Join, JoinType, LogicalPlan};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};

/// Σ.Q.M Phase 1: fact-class threshold. Tables smaller than this don't
/// qualify as the "fact" side of a synthesisable dim⋈fact pattern.
/// At TPC-H SF=10 this includes lineitem (60M) and orders (15M); it
/// excludes customer (1.5M just barely — see Slice 4), supplier (100K),
/// part (2M just over but we conservatively want only the two big fact
/// tables in Phase 1).
const FACT_ROW_THRESHOLD: usize = 5_000_000;

/// Install the Σ.Q.M synthetic LeftSemi rule into a SessionStateBuilder.
/// Mirrors `install_push_down_left_semi_rule`.
pub fn install_synthetic_left_semi_rule(
    builder: SessionStateBuilder,
) -> SessionStateBuilder {
    builder.with_optimizer_rule(Arc::new(SyntheticLeftSemiRule))
}

#[derive(Debug, Default)]
pub struct SyntheticLeftSemiRule;

impl OptimizerRule for SyntheticLeftSemiRule {
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        "ematix_flow_synthetic_left_semi"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DfResult<Transformed<LogicalPlan>> {
        plan.transform_up(|node| try_emit_semi(node))
    }
}

fn try_emit_semi(node: LogicalPlan) -> DfResult<Transformed<LogicalPlan>> {
    // Match Inner Join with equi-keys and no post-condition.
    let LogicalPlan::Join(Join {
        left,
        right,
        on,
        filter,
        join_type,
        join_constraint,
        schema,
        null_equality,
        null_aware,
    }) = node
    else {
        return Ok(Transformed::no(node));
    };

    let restore = |left: Arc<LogicalPlan>,
                   right: Arc<LogicalPlan>,
                   on: Vec<(Expr, Expr)>,
                   filter: Option<Expr>|
     -> LogicalPlan {
        LogicalPlan::Join(Join {
            left,
            right,
            on,
            filter,
            join_type,
            join_constraint,
            schema: schema.clone(),
            null_equality,
            null_aware,
        })
    };

    if !matches!(join_type, JoinType::Inner) {
        return Ok(Transformed::no(restore(left, right, on, filter)));
    }
    if filter.is_some() {
        return Ok(Transformed::no(restore(left, right, on, filter)));
    }
    if on.is_empty() {
        return Ok(Transformed::no(restore(left, right, on, filter)));
    }

    // Determine which side is dim (smaller base table with a Filter)
    // and which is fact (larger base table, fact-class size).
    let left_base_rows = base_table_rows(left.as_ref());
    let right_base_rows = base_table_rows(right.as_ref());
    let (Some(l_rows), Some(r_rows)) = (left_base_rows, right_base_rows) else {
        // Can't compare without stats on both sides.
        return Ok(Transformed::no(restore(left, right, on, filter)));
    };

    let dim_is_left = l_rows <= r_rows;
    let (dim, fact, dim_rows, fact_rows) = if dim_is_left {
        (&left, &right, l_rows, r_rows)
    } else {
        (&right, &left, r_rows, l_rows)
    };

    // Gate B: fact side base table is fact-class.
    if fact_rows < FACT_ROW_THRESHOLD {
        return Ok(Transformed::no(restore(left, right, on, filter)));
    }
    // Don't fire when dim and fact are the same size — likely a
    // self-join or fact⋈fact (Phase 4 lifts this).
    if dim_rows == fact_rows {
        return Ok(Transformed::no(restore(left, right, on, filter)));
    }

    // Gate A: dim subtree has a Filter on its path to its TableScan.
    if !dim_subtree_has_filter(dim.as_ref()) {
        return Ok(Transformed::no(restore(left, right, on, filter)));
    }

    // Build the semi `on`: needs (fact_key_expr, dim_key_expr).
    // Original `on` is (left_key, right_key). If dim is left, swap.
    let semi_on: Vec<(Expr, Expr)> = if dim_is_left {
        on.iter().map(|(l, r)| (r.clone(), l.clone())).collect()
    } else {
        on.clone()
    };

    // Idempotency: skip if the fact subtree already contains a
    // LeftSemi whose `on` structurally matches `semi_on`. This catches
    // both shallow (we just emitted) and deep (L10 has pushed it down)
    // cases.
    if fact_subtree_has_matching_left_semi(fact.as_ref(), &semi_on) {
        return Ok(Transformed::no(restore(left, right, on, filter)));
    }

    // Synthesise the LeftSemi. Schema = fact's schema (LeftSemi
    // preserves left's schema).
    let semi_schema = fact.schema().clone();
    let new_semi = Join {
        left: Arc::clone(fact),
        right: Arc::clone(dim),
        on: semi_on,
        filter: None,
        join_type: JoinType::LeftSemi,
        join_constraint,
        schema: semi_schema,
        null_equality,
        null_aware,
    };

    // Rebuild outer Inner with fact side replaced by the wrapped semi.
    let semi_arc = Arc::new(LogicalPlan::Join(new_semi));
    let (new_left, new_right) = if dim_is_left {
        (left, semi_arc)
    } else {
        (semi_arc, right)
    };

    let new_inner = LogicalPlan::Join(Join {
        left: new_left,
        right: new_right,
        on,
        filter,
        join_type,
        join_constraint,
        schema,
        null_equality,
        null_aware,
    });

    Ok(Transformed::yes(new_inner))
}

/// Find the largest base-table row count in `subtree`. Returns the
/// max `num_rows` from any TableScan's statistics, or None if no
/// TableScan reports stats. `TableSource` on a LogicalPlan TableScan
/// is normally a `DefaultTableSource` wrapping the physical
/// `TableProvider`; we downcast to read `provider.statistics()`.
fn base_table_rows(plan: &LogicalPlan) -> Option<usize> {
    use datafusion::catalog::default_table_source::DefaultTableSource;
    let mut max_rows: Option<usize> = None;
    let _ = plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            let provider_stats = scan
                .source
                .as_any()
                .downcast_ref::<DefaultTableSource>()
                .and_then(|dts| dts.table_provider.statistics());
            if let Some(stats) = provider_stats {
                let n = match stats.num_rows {
                    Precision::Exact(n) | Precision::Inexact(n) => Some(n),
                    Precision::Absent => None,
                };
                if let Some(n) = n {
                    max_rows = Some(max_rows.map_or(n, |cur| cur.max(n)));
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    max_rows
}

/// Σ.Q.M Phase 1 detection: walk `dim_subtree` looking for a `Filter`
/// node. Restricted to shallow shapes — descends through Projection /
/// SubqueryAlias / Filter / TableScan but NOT through Joins.
///
/// (Slice 2 — recursing through one level of Inner Join — was tried
/// and rejected, see module docstring. The match arm for Join stays
/// `false` to preserve the validated Slice 1 behavior.)
fn dim_subtree_has_filter(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Filter(_) => true,
        LogicalPlan::Projection(p) => dim_subtree_has_filter(p.input.as_ref()),
        LogicalPlan::SubqueryAlias(a) => dim_subtree_has_filter(a.input.as_ref()),
        LogicalPlan::TableScan(_) => false,
        LogicalPlan::Join(_) => false,
        _ => false,
    }
}

/// Walk `fact_subtree` looking for a `LeftSemi` whose `on` matches
/// `target_on` (set equality, ignoring order). Catches both the case
/// where Σ.Q.M has emitted on a prior pass and L10 hasn't pushed yet
/// (LeftSemi at fact_subtree's root) and the case where L10 has pushed
/// (LeftSemi wrapping the fact TableScan deep inside).
fn fact_subtree_has_matching_left_semi(
    plan: &LogicalPlan,
    target_on: &[(Expr, Expr)],
) -> bool {
    let target_set: HashSet<(String, String)> = target_on
        .iter()
        .map(|(l, r)| (expr_signature(l), expr_signature(r)))
        .collect();
    let mut found = false;
    let _ = plan.apply(|node| {
        if let LogicalPlan::Join(j) = node {
            if matches!(j.join_type, JoinType::LeftSemi) {
                let node_set: HashSet<(String, String)> = j
                    .on
                    .iter()
                    .map(|(l, r)| (expr_signature(l), expr_signature(r)))
                    .collect();
                if node_set == target_set {
                    found = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

/// Lossy signature of an Expr suitable for set-equality of equi-keys.
/// Column references are stringified by qualified name; everything
/// else falls back to debug. TPC-H equi-keys are always column refs
/// so this is precise for our use.
fn expr_signature(e: &Expr) -> String {
    if let Expr::Column(c) = e {
        match &c.relation {
            Some(rel) => format!("{}.{}", rel, c.name),
            None => c.name.clone(),
        }
    } else {
        format!("{e:?}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fast_parquet::FastParquetTableProvider;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use ematix_parquet_codec::write::{ColumnData, write_table_to_path};
    use ematix_parquet_format::types::CompressionCodec;

    fn tmp_parquet(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "synthetic_left_semi_{}_{}",
            std::process::id(),
            name
        ));
        let _ = std::fs::create_dir_all(&dir);
        dir.join(format!("{name}.parquet"))
    }

    /// Write a "customer-shaped" small table: 1000 c_custkey + c_mktsegment.
    /// (Used as the "dim" — gets a Filter on c_mktsegment.)
    fn write_customer(path: &std::path::Path) {
        // c_custkey 0..1000; c_mktsegment alternates two values.
        let c_custkey: Vec<i64> = (0..1000i64).collect();
        let c_mktsegment: Vec<i64> = (0..1000i64).map(|i| i % 2).collect();
        write_table_to_path(
            path,
            &[
                ("c_custkey", ColumnData::I64(&c_custkey)),
                ("c_mktsegment", ColumnData::I64(&c_mktsegment)),
            ],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
    }

    /// Write a "fact-shaped" table large enough to trip Gate B's
    /// FACT_ROW_THRESHOLD (5M rows).
    fn write_fact(path: &std::path::Path, n_rows: usize) {
        let o_orderkey: Vec<i64> = (0..n_rows as i64).collect();
        let o_custkey: Vec<i64> = (0..n_rows as i64).map(|i| i % 1000).collect();
        write_table_to_path(
            path,
            &[
                ("o_orderkey", ColumnData::I64(&o_orderkey)),
                ("o_custkey", ColumnData::I64(&o_custkey)),
            ],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
    }

    /// Write a small fact-shape table (below FACT_ROW_THRESHOLD). Used
    /// for the gate-B-fails test.
    fn write_small_fact(path: &std::path::Path) {
        write_fact(path, 1_000); // 1K rows ≪ FACT_ROW_THRESHOLD
    }

    /// Count LeftSemi joins whose left input is a TableScan of `table`
    /// (structural signature of "LeftSemi pushed to wrap the fact
    /// scan" — what we want to see after Σ.Q.M + Σ.Q.L10 run together)
    /// OR whose left input is itself the fact subtree (Σ.Q.M's
    /// pre-L10 output).
    fn count_left_semi_referencing(plan: &LogicalPlan, table: &str) -> usize {
        let mut count = 0;
        let _ = plan.apply(|node| {
            if let LogicalPlan::Join(j) = node {
                if matches!(j.join_type, JoinType::LeftSemi)
                    && subtree_references_table(j.left.as_ref(), table)
                {
                    count += 1;
                }
            }
            Ok(TreeNodeRecursion::Continue)
        });
        count
    }

    fn subtree_references_table(plan: &LogicalPlan, table: &str) -> bool {
        let mut found = false;
        let _ = plan.apply(|node| {
            if let LogicalPlan::TableScan(scan) = node {
                if scan.table_name.table() == table {
                    found = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
            Ok(TreeNodeRecursion::Continue)
        });
        found
    }

    fn make_ctx_with_rule_only() -> SessionContext {
        // Just the synthetic-semi rule, no L10. Lets us verify Σ.Q.M's
        // direct output (LeftSemi sits above Inner, NOT pushed to scan).
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(SessionConfig::new())
            .with_optimizer_rule(Arc::new(SyntheticLeftSemiRule))
            .build();
        SessionContext::new_with_state(state)
    }

    fn make_ctx_with_rule_and_l10() -> SessionContext {
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(SessionConfig::new())
            .with_optimizer_rule(Arc::new(SyntheticLeftSemiRule))
            .with_optimizer_rule(Arc::new(
                crate::push_down_left_semi_rule::PushDownLeftSemiRule,
            ))
            .build();
        SessionContext::new_with_state(state)
    }

    #[tokio::test]
    async fn fires_on_q03_shape() {
        // Q03 shape: customer with mktsegment filter ⋈ large orders.
        let cust = tmp_parquet("q03_customer");
        let ord = tmp_parquet("q03_orders");
        write_customer(&cust);
        write_fact(&ord, 5_500_000); // > FACT_ROW_THRESHOLD

        let ctx = make_ctx_with_rule_only();
        ctx.register_table(
            "customer",
            Arc::new(FastParquetTableProvider::try_new(cust.to_string_lossy()).unwrap()),
        )
        .unwrap();
        ctx.register_table(
            "orders",
            Arc::new(FastParquetTableProvider::try_new(ord.to_string_lossy()).unwrap()),
        )
        .unwrap();

        let df = ctx
            .sql(
                "SELECT o_orderkey FROM customer, orders
                 WHERE c_custkey = o_custkey AND c_mktsegment = 1",
            )
            .await
            .unwrap();
        let logical_plan = df.into_optimized_plan().unwrap();
        let n = count_left_semi_referencing(&logical_plan, "orders");
        assert!(
            n >= 1,
            "expected at least one LeftSemi referencing orders, got {n}.\nPlan:\n{logical_plan:?}"
        );
    }

    #[tokio::test]
    async fn does_not_fire_no_filter() {
        // Same shape as Q03 but no filter on customer.
        let cust = tmp_parquet("nofilter_customer");
        let ord = tmp_parquet("nofilter_orders");
        write_customer(&cust);
        write_fact(&ord, 5_500_000);

        let ctx = make_ctx_with_rule_only();
        ctx.register_table(
            "customer",
            Arc::new(FastParquetTableProvider::try_new(cust.to_string_lossy()).unwrap()),
        )
        .unwrap();
        ctx.register_table(
            "orders",
            Arc::new(FastParquetTableProvider::try_new(ord.to_string_lossy()).unwrap()),
        )
        .unwrap();

        let df = ctx
            .sql("SELECT o_orderkey FROM customer, orders WHERE c_custkey = o_custkey")
            .await
            .unwrap();
        let logical_plan = df.into_optimized_plan().unwrap();
        let n = count_left_semi_referencing(&logical_plan, "orders");
        assert_eq!(
            n, 0,
            "expected no LeftSemi (gate A fails - no filter), got {n}.\nPlan:\n{logical_plan:?}"
        );
    }

    #[tokio::test]
    async fn does_not_fire_when_fact_below_threshold() {
        // Gate B fails: fact side is below FACT_ROW_THRESHOLD.
        let cust = tmp_parquet("smallfact_customer");
        let ord = tmp_parquet("smallfact_orders");
        write_customer(&cust);
        write_small_fact(&ord); // 1K rows

        let ctx = make_ctx_with_rule_only();
        ctx.register_table(
            "customer",
            Arc::new(FastParquetTableProvider::try_new(cust.to_string_lossy()).unwrap()),
        )
        .unwrap();
        ctx.register_table(
            "orders",
            Arc::new(FastParquetTableProvider::try_new(ord.to_string_lossy()).unwrap()),
        )
        .unwrap();

        let df = ctx
            .sql(
                "SELECT o_orderkey FROM customer, orders
                 WHERE c_custkey = o_custkey AND c_mktsegment = 1",
            )
            .await
            .unwrap();
        let logical_plan = df.into_optimized_plan().unwrap();
        let n = count_left_semi_referencing(&logical_plan, "orders");
        assert_eq!(
            n, 0,
            "expected no LeftSemi (gate B fails - small fact), got {n}.\nPlan:\n{logical_plan:?}"
        );
    }

    #[tokio::test]
    async fn idempotent_on_repeated_application() {
        // After one Σ.Q.M pass + L10 push, a second Σ.Q.M pass should
        // see the existing LeftSemi in the fact subtree and NOT emit
        // another one. We verify by checking the LeftSemi count stays
        // at exactly 1 (not 2).
        let cust = tmp_parquet("idempotent_customer");
        let ord = tmp_parquet("idempotent_orders");
        write_customer(&cust);
        write_fact(&ord, 5_500_000);

        // Run with rule installed twice — DataFusion's optimizer loops
        // until fixed point. If Σ.Q.M weren't idempotent the second
        // iteration would add another LeftSemi.
        let ctx = make_ctx_with_rule_and_l10();
        ctx.register_table(
            "customer",
            Arc::new(FastParquetTableProvider::try_new(cust.to_string_lossy()).unwrap()),
        )
        .unwrap();
        ctx.register_table(
            "orders",
            Arc::new(FastParquetTableProvider::try_new(ord.to_string_lossy()).unwrap()),
        )
        .unwrap();

        let df = ctx
            .sql(
                "SELECT o_orderkey FROM customer, orders
                 WHERE c_custkey = o_custkey AND c_mktsegment = 1",
            )
            .await
            .unwrap();
        let logical_plan = df.into_optimized_plan().unwrap();
        let n = count_left_semi_referencing(&logical_plan, "orders");
        assert_eq!(
            n, 1,
            "expected exactly one LeftSemi (idempotent), got {n}.\nPlan:\n{logical_plan:?}"
        );
    }

    #[tokio::test]
    async fn composes_with_l10_to_wrap_fact_scan() {
        // Σ.Q.M emits the synthetic LeftSemi; Σ.Q.L10 then pushes it
        // down to wrap the fact TableScan. Verify the post-optimization
        // plan has a LeftSemi whose left input is the orders TableScan
        // (or a Projection over it).
        let cust = tmp_parquet("compose_customer");
        let ord = tmp_parquet("compose_orders");
        write_customer(&cust);
        write_fact(&ord, 5_500_000);

        let ctx = make_ctx_with_rule_and_l10();
        ctx.register_table(
            "customer",
            Arc::new(FastParquetTableProvider::try_new(cust.to_string_lossy()).unwrap()),
        )
        .unwrap();
        ctx.register_table(
            "orders",
            Arc::new(FastParquetTableProvider::try_new(ord.to_string_lossy()).unwrap()),
        )
        .unwrap();

        let df = ctx
            .sql(
                "SELECT o_orderkey FROM customer, orders
                 WHERE c_custkey = o_custkey AND c_mktsegment = 1",
            )
            .await
            .unwrap();
        let logical_plan = df.into_optimized_plan().unwrap();
        let n = count_left_semi_referencing(&logical_plan, "orders");
        assert!(
            n >= 1,
            "expected at least one LeftSemi referencing orders after Σ.Q.M+L10, got {n}.\nPlan:\n{logical_plan:?}"
        );
    }

    #[test]
    fn dim_subtree_has_filter_shallow() {
        // Direct constructed plans: smoke-test the gate-A walker.
        use datafusion::common::DFSchema;
        use datafusion::logical_expr::{Filter, LogicalPlan};
        use datafusion::scalar::ScalarValue;
        let schema = Arc::new(
            DFSchema::from_unqualified_fields(
                vec![datafusion::arrow::datatypes::Field::new(
                    "x",
                    datafusion::arrow::datatypes::DataType::Int64,
                    false,
                )]
                .into(),
                Default::default(),
            )
            .unwrap(),
        );
        let scan_dummy = LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
            produce_one_row: false,
            schema: schema.clone(),
        });
        assert!(!dim_subtree_has_filter(&scan_dummy));
        let filter_node = LogicalPlan::Filter(
            Filter::try_new(Expr::Literal(ScalarValue::Boolean(Some(true)), None),
                Arc::new(scan_dummy.clone())).unwrap(),
        );
        assert!(dim_subtree_has_filter(&filter_node));
    }


    #[test]
    fn expr_signature_column() {
        use datafusion::common::Column;
        let c = Expr::Column(Column::new(Some("customer"), "c_custkey"));
        assert_eq!(expr_signature(&c), "customer.c_custkey");
    }
}
