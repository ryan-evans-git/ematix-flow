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
//!    subtree (group_expr + aggr_expr + recursive input hash on
//!    column-projections and TableScan source).
//! 2. Find any hash appearing 2+ times across the plan — these are
//!    duplicated aggregate computations.
//! 3. For each such pair where the aggregate output schema contains
//!    any `DataType::Float64` column, rewrite both Partial+Final
//!    aggregate pairs to `mode=Single` (no Repartition).
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
//! ## Status
//!
//! **Scaffolded, not wired in.** The structural-hash helper and the
//! rewrite both need careful implementation:
//!
//! - `physical_plan_hash` must handle `PhysicalExpr` equality
//!   correctly (visit `Column` index, `Literal`, `BinaryExpr` op +
//!   children, function names, etc.). Aggregate function expressions
//!   need their own hashing.
//! - The rewrite needs to atomically replace a Partial+Final pair
//!   *and* the Partial+Final inside the scalar subquery's `Aggregate`.
//!   The DataFusion planner emits these as separate `AggregateExec`
//!   nodes that we need to find via structural walking.
//! - Test coverage: Q15 fires + Q12/Q14 don't + idempotent.
//!
//! See `project_tpch_correctness_gaps` memory note for the diagnosis
//! summary and `crates/ematix-flow-core/examples/q21_inspect.rs` for
//! the reproducer (env: `Q=15 PARTITIONS=14`).

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::Result as DfResult;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::AggregateExec;

/// Public rule entry point. Not yet wired into any SessionState; see
/// `examples/q21_inspect.rs` for the standalone Q15 reproducer that
/// this rule is intended to fix.
#[derive(Debug)]
pub struct DedupeAggregateForFloatDeterminism;

impl PhysicalOptimizerRule for DedupeAggregateForFloatDeterminism {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Step 1: walk plan, build a {hash -> count} map over all
        // AggregateExec subtrees.
        let mut counts: HashMap<u64, usize> = HashMap::new();
        plan.clone().transform_down(|node| {
            if let Some(agg) = node.as_any().downcast_ref::<AggregateExec>() {
                if has_float_aggregate(agg) {
                    let h = subtree_hash(&node);
                    *counts.entry(h).or_default() += 1;
                }
            }
            Ok(Transformed::no(node))
        })?;

        // Step 2: collect hashes that appear 2+ times — those are the
        // duplicated computations we need to force-single.
        let dupes: Vec<u64> = counts
            .into_iter()
            .filter_map(|(h, n)| (n >= 2).then_some(h))
            .collect();
        if dupes.is_empty() {
            return Ok(plan);
        }

        // Step 3: rewrite. For each AggregateExec whose subtree hash
        // matches a duplicated entry, replace the Partial+Final pair
        // with a single `mode=Single` AggregateExec.
        //
        // TODO: implement. The mechanical replacement of a Final-on-
        // Partial pair with a Single requires walking down from the
        // Final to find its Partial, lifting the Partial's input,
        // then constructing a new AggregateExec with the Final's
        // group_expr/aggr_expr but `mode=Single` and the Partial's
        // input as direct child. Care needed for Repartition /
        // CoalesceBatches wrappers between Partial and Final.
        let _ = dupes;
        Ok(plan)
    }

    fn name(&self) -> &str {
        "ematix_flow_dedupe_aggregate_for_float_determinism"
    }

    fn schema_check(&self) -> bool {
        // mode=Single output schema equals Partial+Final output schema
        // for the same group_expr + aggr_expr, so true.
        true
    }
}

/// Returns true if `agg`'s output schema contains any Float64 column
/// — narrows the rule to f64-determinism-sensitive aggregates and
/// avoids touching integer COUNT/SUM that are bit-exact regardless of
/// ordering.
fn has_float_aggregate(agg: &AggregateExec) -> bool {
    agg.schema()
        .fields()
        .iter()
        .any(|f| matches!(f.data_type(), DataType::Float64))
}

/// Structural hash of an ExecutionPlan subtree. Two subtrees with the
/// same hash represent the same logical computation:
///
///   - Same node type at each position
///   - Same key per-node attributes (group_expr/aggr_expr for
///     AggregateExec; predicate for FilterExec; column projections;
///     TableScan source path)
///   - Recursive over children
///
/// TODO: implement. Skeleton returns a unique-per-node hash so
/// nothing matches in practice — keeps the rule a no-op until the
/// hash is real.
fn subtree_hash(node: &Arc<dyn ExecutionPlan>) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;
    let mut h = DefaultHasher::new();
    // Placeholder: hash the Arc pointer so each node gets a unique
    // hash. Replace with a real structural hash that walks
    //   - downcast match on node type (AggregateExec / FilterExec /
    //     ProjectionExec / TableScan-backed Scan / RepartitionExec /
    //     CoalesceBatchesExec / ...)
    //   - per-type attribute hashing (e.g., AggregateExec must hash
    //     group_expr columns by index, aggr_expr by function name +
    //     arg expressions)
    //   - children recursively
    h.write_usize(Arc::as_ptr(node) as *const () as usize);
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn has_float_aggregate_smoke() {
        // Placeholder smoke test — real tests will need a fixture
        // SessionContext to materialize an AggregateExec plan.
        // Verifying the import surface compiles is the bar here.
        let _rule = DedupeAggregateForFloatDeterminism;
        assert_eq!(
            PhysicalOptimizerRule::name(&DedupeAggregateForFloatDeterminism),
            "ematix_flow_dedupe_aggregate_for_float_determinism"
        );
    }

    // TODO: integration test that registers the rule, plans Q15
    // against the lineitem fixture, and asserts:
    //   1. Two AggregateExec subtrees hash equal (sanity check on
    //      subtree_hash).
    //   2. After the rule runs, both subtrees use AggregateMode::Single.
    //   3. Q15 result is deterministic across 10+ runs.
    //
    // TODO: negative test on Q12-shape plan — verify the rule does
    // NOT fire (no duplicate aggregates in Q12).
    //
    // TODO: idempotency test — running the rule twice on the same plan
    // produces the same result the second time.
}
