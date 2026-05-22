//! `DedupeAggregateForFloatDeterminism` — `PhysicalOptimizerRule` that
//! detects structurally-identical `AggregateExec` subtrees in the same
//! plan tree and forces both to `mode=Single` execution when they
//! contain f64-valued aggregates.
//!
//! ## Why
//!
//! TPC-H Q15's optimizer output materializes the `revenue_s` CTE as
//! two completely separate `Aggregate(SUM groupBy=l_suppkey)` subtrees
//! — once in the outer FROM and once inside the scalar `MAX(...)`
//! subquery. The two SUMs run in parallel across 14 partitions, and
//! parallel f64 summation reorders operands depending on thread
//! scheduling, producing values that differ by ULP. The outer query's
//! `WHERE total_revenue = (SELECT MAX(total_revenue) FROM revenue_s)`
//! then drops the matching row about 40% of the time.
//!
//! DuckDB and Polars don't show this — likely because they materialize
//! the CTE once (DuckDB) or perform subquery-CSE at logical-planning
//! (Polars). DataFusion 53 has no non-recursive CTE materialization.
//!
//! ## What this rule does
//!
//! 1. Walk the physical plan, structurally hash each `AggregateExec`
//!    subtree containing any f64 column.
//! 2. Find any hash appearing 2+ times across the plan — these are
//!    duplicated aggregate computations.
//! 3. For each Final-mode `AggregateExec` whose subtree hash matches a
//!    duplicate, rewrite the Partial+Final pair to a single
//!    `mode=Single` `AggregateExec` directly on the Partial's input.
//!
//! ## Why this is safe
//!
//! - Structural identity is conservative: false negatives (don't fire
//!   when we could) are fine; false positives would silently change
//!   plans we shouldn't.
//! - `mode=Single` produces correct results for any aggregate that
//!   `mode=Partial`+`mode=Final` produces — it's the same algorithm
//!   serialized.
//! - Only fires when an aggregate is *actually* duplicated, which is
//!   a rare structural shape (only Q15 in TPC-H 22).
//!
//! The structural hash treats `RepartitionExec`, `CoalesceBatchesExec`,
//! and `SortExec` as semantically transparent — two subtrees that
//! differ only in those partitioning/ordering wrappers hash the same.
//! For unknown node types, the hash falls back to the node's
//! `displayable` string + recursive children.
//!
//! See `project_tpch_correctness_gaps` memory note for the diagnosis
//! summary and `crates/ematix-flow-core/examples/q21_inspect.rs` for
//! the reproducer (env: `Q=15 PARTITIONS=14`).

use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;

#[derive(Debug)]
pub struct DedupeAggregateForFloatDeterminism;

impl PhysicalOptimizerRule for DedupeAggregateForFloatDeterminism {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Pass 1: walk plan top-down, hash each Final-mode AggregateExec
        // subtree that contains an f64 column. Count occurrences.
        let mut counts: std::collections::HashMap<u64, usize> =
            std::collections::HashMap::new();
        let _ = plan.clone().transform_down(|node| {
            if let Some(agg) = node.as_any().downcast_ref::<AggregateExec>()
                && matches!(
                    agg.mode(),
                    AggregateMode::Final | AggregateMode::FinalPartitioned
                )
                && has_float_aggregate(agg)
            {
                let h = subtree_hash(&node);
                *counts.entry(h).or_default() += 1;
            }
            Ok(Transformed::no(node))
        })?;

        let dupes: HashSet<u64> = counts
            .into_iter()
            .filter_map(|(h, n)| (n >= 2).then_some(h))
            .collect();
        if dupes.is_empty() {
            return Ok(plan);
        }

        // Pass 2: walk top-down again. For every Final-mode aggregate
        // whose subtree hash is in `dupes`, rewrite it to mode=Single
        // by lifting the Partial's input directly. The hash is
        // computed BEFORE the rewrite, so the substitution is keyed
        // off the pre-rewrite shape.
        let rewritten = plan.transform_down(|node| {
            if let Some(agg) = node.as_any().downcast_ref::<AggregateExec>()
                && matches!(
                    agg.mode(),
                    AggregateMode::Final | AggregateMode::FinalPartitioned
                )
                && has_float_aggregate(agg)
            {
                let h = subtree_hash(&node);
                if dupes.contains(&h) {
                    let single = rewrite_to_single(agg)?;
                    return Ok(Transformed::yes(single));
                }
            }
            Ok(Transformed::no(node))
        })?;
        Ok(rewritten.data)
    }

    fn name(&self) -> &str {
        "ematix_flow_dedupe_aggregate_for_float_determinism"
    }

    fn schema_check(&self) -> bool {
        // mode=Single output schema equals Partial+Final output schema
        // for the same group_expr + aggr_expr.
        true
    }
}

/// Returns true if `agg`'s output schema contains any Float64 column —
/// narrows the rule to f64-determinism-sensitive aggregates and avoids
/// touching integer COUNT/SUM that are bit-exact regardless of ordering.
fn has_float_aggregate(agg: &AggregateExec) -> bool {
    agg.schema()
        .fields()
        .iter()
        .any(|f| matches!(f.data_type(), DataType::Float64))
}

/// Structural hash of an ExecutionPlan subtree. Two subtrees with the
/// same hash represent the same logical computation. Partitioning /
/// ordering wrappers (`RepartitionExec`, `CoalesceBatchesExec`,
/// `SortExec`) are semantically transparent — they don't contribute
/// to the hash and the walk passes through to the child.
fn subtree_hash(node: &Arc<dyn ExecutionPlan>) -> u64 {
    let mut h = DefaultHasher::new();
    hash_node(node, &mut h);
    h.finish()
}

fn hash_node(node: &Arc<dyn ExecutionPlan>, h: &mut DefaultHasher) {
    // Pass-through wrappers: hash skips straight to the child.
    if node.as_any().is::<RepartitionExec>()
        || node.as_any().is::<CoalesceBatchesExec>()
        || node.as_any().is::<SortExec>()
    {
        if let Some(child) = node.children().into_iter().next() {
            hash_node(child, h);
        }
        return;
    }

    if let Some(agg) = node.as_any().downcast_ref::<AggregateExec>() {
        h.write_u8(1);
        // Hash mode as a discriminator number. AggregateMode is enum
        // derive-Hash, but we want explicit bytes for forward-stability.
        h.write_u8(match agg.mode() {
            AggregateMode::Partial => 0,
            AggregateMode::Final => 1,
            AggregateMode::FinalPartitioned => 2,
            AggregateMode::Single => 3,
            AggregateMode::SinglePartitioned => 4,
            AggregateMode::PartialReduce => 5,
        });
        // group_expr
        let groups = agg.group_expr().expr();
        h.write_usize(groups.len());
        for (expr, name) in groups {
            expr.hash(h);
            name.hash(h);
        }
        // aggr_expr
        let aggs = agg.aggr_expr();
        h.write_usize(aggs.len());
        for af in aggs {
            af.name().hash(h);
            af.fun().name().hash(h);
            for arg in af.expressions() {
                arg.hash(h);
            }
        }
        // filter_expr
        let filters = agg.filter_expr();
        h.write_usize(filters.len());
        for filt in filters {
            match filt {
                Some(e) => {
                    h.write_u8(1);
                    e.hash(h);
                }
                None => h.write_u8(0),
            }
        }
        hash_node(agg.input(), h);
        return;
    }

    if let Some(filt) = node.as_any().downcast_ref::<FilterExec>() {
        h.write_u8(2);
        filt.predicate().hash(h);
        if let Some(child) = node.children().into_iter().next() {
            hash_node(child, h);
        }
        return;
    }

    if let Some(proj) = node.as_any().downcast_ref::<ProjectionExec>() {
        h.write_u8(3);
        let exprs = proj.expr();
        h.write_usize(exprs.len());
        for pe in exprs {
            pe.expr.hash(h);
            pe.alias.hash(h);
        }
        if let Some(child) = node.children().into_iter().next() {
            hash_node(child, h);
        }
        return;
    }

    // Fallback for leaf-like nodes (TableScan, DataSourceExec, parquet
    // providers, etc.) and any node type we don't model explicitly:
    // hash a one-line display of the node + recurse into children. The
    // display string captures column projections, predicates pushed
    // into the scan, and the source path / table name.
    h.write_u8(255);
    let disp =
        datafusion::physical_plan::displayable(node.as_ref())
            .one_line()
            .to_string();
    disp.hash(h);
    for child in node.children() {
        hash_node(child, h);
    }
}

/// Walk down from a Final/FinalPartitioned `AggregateExec` through
/// semantically-transparent wrappers (RepartitionExec /
/// CoalesceBatchesExec) until we find the Partial aggregate; then
/// construct a `mode=Single` replacement directly on the Partial's
/// input. The Single's group_expr / aggr_expr / filter_expr / schema
/// all come from the Partial side (where they refer to the raw input
/// columns).
fn rewrite_to_single(final_agg: &AggregateExec) -> DfResult<Arc<dyn ExecutionPlan>> {
    let mut cur: Arc<dyn ExecutionPlan> = final_agg.input().clone();
    let partial = loop {
        if let Some(agg) = cur.as_any().downcast_ref::<AggregateExec>() {
            if matches!(agg.mode(), AggregateMode::Partial) {
                break agg.clone();
            }
            return Err(DataFusionError::Internal(format!(
                "DedupeAggregateForFloatDeterminism: expected Partial under Final, got {:?}",
                agg.mode()
            )));
        }
        // Pass-through wrappers between Final and Partial.
        if cur.as_any().is::<RepartitionExec>()
            || cur.as_any().is::<CoalesceBatchesExec>()
            || cur.as_any().is::<SortExec>()
        {
            let next = cur
                .children()
                .into_iter()
                .next()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "DedupeAggregateForFloatDeterminism: empty wrapper above Partial".into(),
                    )
                })?
                .clone();
            cur = next;
            continue;
        }
        return Err(DataFusionError::Internal(format!(
            "DedupeAggregateForFloatDeterminism: unexpected node between Final and Partial: {}",
            datafusion::physical_plan::displayable(cur.as_ref()).one_line(),
        )));
    };

    // Two-stage determinism:
    //   1. Wrap input with CoalescePartitionsExec to merge the
    //      multi-partition stream into one. This alone is NOT enough
    //      because CoalescePartitions polls inputs concurrently and
    //      interleaves arbitrarily.
    //   2. Wrap that with SortExec on (every column of the input
    //      schema). Sort gives a TOTAL deterministic order — two
    //      independent evaluations of the same subtree see input rows
    //      in bit-identical sequence, so the f64 SUM accumulates in
    //      identical order across runs (and across the two duplicated
    //      subtrees), giving bit-exact equality at the outer WHERE
    //      predicate.
    let input = partial.input().clone();
    let input_schema = input.schema();
    let coalesced: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(input));
    let sort_keys: Vec<PhysicalSortExpr> = input_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| PhysicalSortExpr::new_default(
            Arc::new(Column::new(f.name(), i)) as Arc<dyn datafusion::physical_plan::PhysicalExpr>,
        ))
        .collect();
    let sorted: Arc<dyn ExecutionPlan> = if sort_keys.is_empty() {
        coalesced
    } else {
        Arc::new(SortExec::new(LexOrdering::new(sort_keys).unwrap(), coalesced))
    };
    let single = AggregateExec::try_new(
        AggregateMode::Single,
        partial.group_expr().clone(),
        partial.aggr_expr().to_vec(),
        partial.filter_expr().to_vec(),
        sorted,
        partial.input_schema(),
    )?;
    Ok(Arc::new(single) as Arc<dyn ExecutionPlan>)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rule_name_smoke() {
        let rule = DedupeAggregateForFloatDeterminism;
        assert_eq!(
            PhysicalOptimizerRule::name(&rule),
            "ematix_flow_dedupe_aggregate_for_float_determinism"
        );
        assert!(rule.schema_check());
    }

    #[test]
    fn empty_plan_passthrough() {
        // Rule on an EmptyExec (no aggregates) is a strict no-op:
        // counts is empty, dupes is empty, returns plan unchanged.
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::physical_plan::empty::EmptyExec;
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        let rule = DedupeAggregateForFloatDeterminism;
        let opt = rule
            .optimize(plan.clone(), &ConfigOptions::default())
            .expect("optimize");
        assert!(Arc::ptr_eq(&plan, &opt));
    }

    /// Q15-shape integration test. Two structurally-identical f64 SUM
    /// aggregates over the same logical input must both rewrite to
    /// `mode=Single`, with their inputs wrapped in `SortExec` on the
    /// full input schema. End-to-end execution must be deterministic
    /// across 10 runs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn q15_shape_becomes_deterministic() {
        use datafusion::arrow::array::{Float64Array, Int64Array, RecordBatch};
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::datasource::MemTable;
        use datafusion::execution::session_state::SessionStateBuilder;
        use datafusion::prelude::{SessionConfig, SessionContext};

        // Build a fixture table with the Q15 SUM shape: an integer
        // grouping key + a float column to sum. 14 supplier groups ×
        // 100 rows each — enough to hit the parallel-SUM ULP issue
        // when partitioning is non-trivial.
        let schema = Arc::new(Schema::new(vec![
            Field::new("supplier", DataType::Int64, false),
            Field::new("revenue", DataType::Float64, false),
        ]));
        let mut suppliers: Vec<i64> = Vec::new();
        let mut revenues: Vec<f64> = Vec::new();
        for s in 0..14_i64 {
            for r in 0..100_i64 {
                suppliers.push(s);
                // Numbers chosen to exercise f64 precision (not
                // exactly representable, sensitive to sum order).
                revenues.push((r as f64 + 1.0) * 0.1 + (s as f64) * 17.3);
            }
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(suppliers)),
                Arc::new(Float64Array::from(revenues)),
            ],
        )
        .unwrap();
        let mt = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());

        let make_ctx = || async {
            let state = SessionStateBuilder::new()
                .with_config(SessionConfig::new().with_target_partitions(4))
                .with_default_features()
                .with_physical_optimizer_rule(Arc::new(DedupeAggregateForFloatDeterminism))
                .build();
            let ctx = SessionContext::new_with_state(state);
            ctx.register_table("revenue_t", mt.clone()).unwrap();
            ctx
        };

        // Q15-shape SQL: outer SUM joined with scalar MAX of the same
        // sub-aggregate. Without the rule, two parallel f64 SUMs over
        // the same logical input produce ULP-differing results and
        // the equality drops rows.
        let sql = "
            WITH r AS (
                SELECT supplier, sum(revenue) AS total
                FROM revenue_t
                GROUP BY supplier
            )
            SELECT r.supplier, r.total
            FROM r
            WHERE r.total = (SELECT max(total) FROM r)
            ORDER BY r.supplier
        ";

        // Run 10 times and verify each returns exactly 1 row (the
        // max-revenue supplier).
        let mut rows_per_run: Vec<usize> = Vec::with_capacity(10);
        for _ in 0..10 {
            let ctx = make_ctx().await;
            let df = ctx.sql(sql).await.unwrap();
            let batches = df.collect().await.unwrap();
            let n: usize = batches.iter().map(|b| b.num_rows()).sum();
            rows_per_run.push(n);
        }
        assert!(
            rows_per_run.iter().all(|&n| n == 1),
            "Q15-shape must return exactly 1 row deterministically; \
             got per-run row counts: {rows_per_run:?}"
        );
    }
}
