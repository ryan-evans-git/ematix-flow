//! `DedupeAggregateForFloatDeterminism` — `PhysicalOptimizerRule` that
//! detects structurally-identical f64-aggregate subtrees in the plan
//! and rewrites both locations to share a single cached computation
//! via [`SharedSubtreeExec`](crate::shared_subtree_exec::SharedSubtreeExec).
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
//! DuckDB and Polars don't show this — DuckDB materializes the CTE
//! once; Polars performs subquery-CSE at logical-planning. DataFusion
//! 53 has no non-recursive CTE materialization. This rule supplies the
//! missing materialization at the physical layer.
//!
//! ## What this rule does
//!
//! 1. Walk the physical plan, structurally hash each `AggregateExec`
//!    subtree containing any f64 column.
//! 2. Find any hash appearing 2+ times — these are duplicated
//!    aggregate computations.
//! 3. For each Final/FinalPartitioned `AggregateExec` whose subtree
//!    hash matches a duplicate, wrap it in a `SharedSubtreeExec` whose
//!    cache is keyed on the structural hash. Both duplicate locations
//!    resolve to the same `Arc<CachedBatches>` through the
//!    [`SharedSubtreeRegistry`](crate::shared_subtree_exec::SharedSubtreeRegistry).
//!    First execution populates; the second replays. Result: ONE
//!    aggregate computation, served bit-identical to both consumers.
//!
//! ## Why this is safe
//!
//! - Structural identity is conservative: false negatives (don't fire
//!   when we could) are fine; false positives would silently change
//!   plans we shouldn't.
//! - Cache contents come from running the original Final aggregate
//!   subtree exactly as the planner produced it — no semantic
//!   rewriting, no `mode=Single` substitution. Whatever DataFusion
//!   computed once is what both consumers see.
//! - Only fires when an aggregate is *actually* duplicated, which is
//!   a rare structural shape (only Q15 in TPC-H 22).
//!
//! ## Session-scoping
//!
//! The registry is shared across all queries on the same
//! `SessionContext`. Two queries with the same f64-aggregate subtree
//! hit the same cache entry — the second query replays the first
//! query's batches without re-executing.
//!
//! Construct the rule via [`DedupeAggregateForFloatDeterminism::with_registry`]
//! so callers control registry lifetime. The `default()` impl creates
//! a fresh per-rule registry (still session-scoped — lives for the
//! life of the `SessionState`).
//!
//! ## Structural hash
//!
//! Treats `RepartitionExec`, `CoalesceBatchesExec`, and `SortExec` as
//! semantically transparent — two subtrees that differ only in those
//! partitioning/ordering wrappers hash the same. Unknown node types
//! fall back to `displayable` + recursive children, which catches
//! `TableScan` source path and pushed-down predicates.
//!
//! See `project_tpch_correctness_gaps` for diagnosis, the
//! `shared_subtree_exec` module for the cache primitive, and
//! `crates/ematix-flow-core/examples/q21_inspect.rs` for the
//! reproducer (env: `Q=15 PARTITIONS=14`).

use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::Result as DfResult;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::enforce_distribution::EnforceDistribution;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
// CoalesceBatchesExec is deprecated in favor of arrow-rs BatchCoalescer
// but DataFusion's planner still emits it in plan trees. We treat it as
// a transparent wrapper in our structural-hash walk.
#[allow(deprecated)]
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;

use crate::shared_subtree_exec::{SharedSubtreeExec, SharedSubtreeRegistry};

/// `PhysicalOptimizerRule` that wraps duplicated f64-aggregate subtrees
/// in `SharedSubtreeExec` so both consumers share one cached computation.
///
/// Construct via [`with_registry`] when you want cross-query cache
/// sharing within a session (recommended). `default()` is a convenience
/// that allocates a fresh per-rule registry.
#[derive(Debug)]
pub struct DedupeAggregateForFloatDeterminism {
    registry: Arc<SharedSubtreeRegistry>,
}

impl Default for DedupeAggregateForFloatDeterminism {
    fn default() -> Self {
        Self::with_registry(Arc::new(SharedSubtreeRegistry::new()))
    }
}

impl DedupeAggregateForFloatDeterminism {
    pub fn with_registry(registry: Arc<SharedSubtreeRegistry>) -> Self {
        Self { registry }
    }

    pub fn registry(&self) -> &Arc<SharedSubtreeRegistry> {
        &self.registry
    }
}

impl PhysicalOptimizerRule for DedupeAggregateForFloatDeterminism {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Pass 1: walk plan top-down, hash each Final-mode AggregateExec
        // subtree that contains an f64 column. Count occurrences. The
        // walk is cheap — 11-trial bench (2026-05-22) shows zero
        // measurable cost on Q22 vs the rule not being installed at
        // all. Earlier "Q22 +7%" measurements were 3/7-trial noise.
        let mut counts: std::collections::HashMap<u64, usize> = std::collections::HashMap::new();
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

        // Pass 2: walk top-down. For every Final-mode aggregate whose
        // subtree hash is in `dupes`, wrap it in a SharedSubtreeExec
        // keyed on that hash. All duplicates with the same hash resolve
        // to the SAME Arc<CachedBatches> via the registry, so first
        // execute() populates and the rest replay — one computation
        // total, bit-identical reads on both sides.
        //
        // The hash is computed BEFORE wrapping. SharedSubtreeExec is a
        // leaf to subsequent plan walks (children() = []), so the walk
        // stops once we replace and doesn't descend into the wrapped
        // subtree.
        let registry = self.registry.clone();
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
                    let cached = registry.get_or_create(h, node.schema());
                    let wrapped: Arc<dyn ExecutionPlan> =
                        Arc::new(SharedSubtreeExec::new(node.clone(), cached));
                    return Ok(Transformed::yes(wrapped));
                }
            }
            Ok(Transformed::no(node))
        })?;

        if !rewritten.transformed {
            return Ok(rewritten.data);
        }

        // Σ.BS — repair partitioning after the wrap.
        //
        // This rule runs LAST in the physical pipeline (custom rules are
        // appended after `JoinSelection` / `EnforceDistribution` /
        // `SanityCheckPlan`). When a duplicated f64 aggregate feeds a
        // `HashJoinExec(PartitionMode::Partitioned)` — which happens once
        // the build side is large enough, i.e. at SF≥100 — the optimizer
        // has already committed both join inputs to N hash-partitions.
        // The agg's own `Hash[key]` output satisfied the join's
        // requirement directly, so there is NO `RepartitionExec` above it
        // to act as a buffer. Wrapping the agg in `SharedSubtreeExec`
        // (which reports `UnknownPartitioning(1)`) then collapses that
        // side from N → 1 partitions, and the join's execute()-time
        // invariant `left_partitions == right_partitions` fails with
        // "Invalid HashJoinExec, partition count mismatch N!=1". At
        // SF=1/10 the build side is small, `JoinSelection` picks
        // `CollectLeft` (left must be 1 partition — which the wrapper
        // satisfies), and the bug stays hidden.
        //
        // Re-running `EnforceDistribution` on the wrapped plan re-derives
        // each parent's required input distribution from scratch and
        // inserts the `RepartitionExec` (Hash on the join key) above the
        // `SharedSubtreeExec` for the join consumer, while leaving the
        // scalar-MAX consumer coalesced to 1 partition — restoring a
        // valid, correct plan. The re-hash of the collapsed single stream
        // by the join key reproduces the hash distribution the sibling
        // side already uses, so results are unchanged.
        //
        // Gated on `rewritten.transformed`, so this only runs on plans
        // that actually contain a duplicated f64 aggregate (Q15 in TPC-H);
        // the other 21 queries returned early via the `dupes.is_empty()`
        // check above and never reach here.
        EnforceDistribution::new().optimize(rewritten.data, config)
    }

    fn name(&self) -> &str {
        "ematix_flow_dedupe_aggregate_for_float_determinism"
    }

    fn schema_check(&self) -> bool {
        // SharedSubtreeExec.schema() == input.schema(); wrapping is a
        // pure pass-through at the schema level.
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

#[allow(deprecated)] // CoalesceBatchesExec
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
    let disp = datafusion::physical_plan::displayable(node.as_ref())
        .one_line()
        .to_string();
    disp.hash(h);
    for child in node.children() {
        hash_node(child, h);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rule_name_smoke() {
        let rule = DedupeAggregateForFloatDeterminism::default();
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
        let rule = DedupeAggregateForFloatDeterminism::default();
        let opt = rule
            .optimize(plan.clone(), &ConfigOptions::default())
            .expect("optimize");
        assert!(Arc::ptr_eq(&plan, &opt));
    }

    /// Q15-shape integration test. Two structurally-identical f64 SUM
    /// aggregates over the same logical input must both wrap in
    /// `SharedSubtreeExec`, sharing one cached computation. End-to-end
    /// execution must be deterministic across 10 runs (one query each
    /// — separate SessionContexts → separate caches; the determinism
    /// comes from the WITHIN-query cache, not the cross-query cache).
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
                .with_physical_optimizer_rule(Arc::new(
                    DedupeAggregateForFloatDeterminism::default(),
                ))
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

    /// Σ.BS — Q15 SF=100 partition-mismatch regression guard.
    ///
    /// At SF=1/10 the `supplier ⋈ revenue0` join is small enough that
    /// `JoinSelection` picks `PartitionMode::CollectLeft` (which only
    /// requires the build side to be 1 partition — satisfied by
    /// `SharedSubtreeExec`'s `UnknownPartitioning(1)`). At SF=100 the
    /// build side exceeds the single-partition threshold, so
    /// `JoinSelection` picks `PartitionMode::Partitioned`, and
    /// `EnforceDistribution` repartitions both join inputs to N
    /// hash-partitions. The agg's `Hash[l_suppkey]` output already
    /// satisfies the join's `Hash[supplier_no]` requirement, so NO
    /// `RepartitionExec` is inserted above it. This rule then wraps that
    /// agg in `SharedSubtreeExec`, collapsing its side from N → 1
    /// partitions with nothing above to restore the count. The
    /// `HashJoinExec(Partitioned)` then fails its execute()-time
    /// assertion: "Invalid HashJoinExec, partition count mismatch N!=1".
    ///
    /// We reproduce the SF=100 plan shape cheaply at SF=1 data size by
    /// zeroing the single-partition threshold (forcing `Partitioned`).
    /// Before the fix this errors; after it returns the single
    /// max-revenue supplier.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn q15_partitioned_join_survives_shared_subtree_collapse() {
        use datafusion::arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::datasource::MemTable;
        use datafusion::execution::session_state::SessionStateBuilder;
        use datafusion::physical_plan::{collect, displayable};
        use datafusion::prelude::{SessionConfig, SessionContext};

        // supplier(s_suppkey, s_name) — 64 suppliers.
        let n_suppliers = 64_i64;
        let supplier_schema = Arc::new(Schema::new(vec![
            Field::new("s_suppkey", DataType::Int64, false),
            Field::new("s_name", DataType::Utf8, false),
        ]));
        let supplier_batch = RecordBatch::try_new(
            supplier_schema.clone(),
            vec![
                Arc::new(Int64Array::from((0..n_suppliers).collect::<Vec<_>>())),
                Arc::new(StringArray::from(
                    (0..n_suppliers)
                        .map(|i| format!("Supplier#{i:03}"))
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let supplier_mt =
            Arc::new(MemTable::try_new(supplier_schema, vec![vec![supplier_batch]]).unwrap());

        // lineitem(l_suppkey, l_extendedprice, l_discount) — 32 rows/supplier.
        let lineitem_schema = Arc::new(Schema::new(vec![
            Field::new("l_suppkey", DataType::Int64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
        ]));
        let (mut l_supp, mut l_price, mut l_disc) = (Vec::new(), Vec::new(), Vec::new());
        for s in 0..n_suppliers {
            for r in 0..32_i64 {
                l_supp.push(s);
                l_price.push((r as f64 + 1.0) * 1000.0 + (s as f64) * 7.0);
                l_disc.push(((r % 10) as f64) * 0.01);
            }
        }
        let lineitem_batch = RecordBatch::try_new(
            lineitem_schema.clone(),
            vec![
                Arc::new(Int64Array::from(l_supp)),
                Arc::new(Float64Array::from(l_price)),
                Arc::new(Float64Array::from(l_disc)),
            ],
        )
        .unwrap();
        let lineitem_mt =
            Arc::new(MemTable::try_new(lineitem_schema, vec![vec![lineitem_batch]]).unwrap());

        // Force PartitionMode::Partitioned even on tiny inputs — this is
        // what SF=100 does naturally (build side exceeds the single-
        // partition threshold).
        let mut config = SessionConfig::new().with_target_partitions(4);
        config
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold = 0;
        config
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold_rows = 0;

        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(DedupeAggregateForFloatDeterminism::default()))
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table("supplier", supplier_mt).unwrap();
        ctx.register_table("lineitem", lineitem_mt).unwrap();

        // Real TPC-H Q15 shape: revenue0 CTE referenced twice (joined to
        // supplier + inside the scalar MAX subquery) → duplicated f64 agg.
        let sql = "
            WITH revenue0 AS (
                SELECT l_suppkey AS supplier_no,
                       sum(l_extendedprice * (1 - l_discount)) AS total_revenue
                FROM lineitem
                GROUP BY l_suppkey
            )
            SELECT s.s_suppkey, s.s_name, r.total_revenue
            FROM supplier s, revenue0 r
            WHERE s.s_suppkey = r.supplier_no
              AND r.total_revenue = (SELECT max(total_revenue) FROM revenue0)
            ORDER BY s.s_suppkey
        ";

        let plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let plan_str = displayable(plan.as_ref()).indent(true).to_string();
        // Guard against vacuous pass: we must actually be exercising a
        // Partitioned hash join AND the dedupe wrap, else the bug can't
        // manifest.
        assert!(
            plan_str.contains("mode=Partitioned"),
            "test must exercise PartitionMode::Partitioned; plan:\n{plan_str}"
        );
        assert!(
            plan_str.contains("SharedSubtreeExec"),
            "dedupe rule must have wrapped the duplicated revenue0 agg; plan:\n{plan_str}"
        );

        let batches = collect(plan, ctx.task_ctx())
            .await
            .expect("Q15 Partitioned plan must execute without a partition-count mismatch");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            rows, 1,
            "Q15 returns exactly one (max-revenue) supplier; got {rows} rows"
        );
    }

    /// Cross-query cache hit. Two consecutive Q15-shape queries on the
    /// SAME `SessionContext` (so SAME `SharedSubtreeRegistry`) should:
    ///   1. First query populates: registry grows to ≥1 entry, and the
    ///      cached entry is marked `is_populated()`.
    ///   2. Second query reuses the cache: registry size doesn't grow.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cross_query_cache_hit() {
        use datafusion::arrow::array::{Float64Array, Int64Array, RecordBatch};
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::datasource::MemTable;
        use datafusion::execution::session_state::SessionStateBuilder;
        use datafusion::prelude::{SessionConfig, SessionContext};

        let schema = Arc::new(Schema::new(vec![
            Field::new("supplier", DataType::Int64, false),
            Field::new("revenue", DataType::Float64, false),
        ]));
        let mut suppliers: Vec<i64> = Vec::new();
        let mut revenues: Vec<f64> = Vec::new();
        for s in 0..14_i64 {
            for r in 0..100_i64 {
                suppliers.push(s);
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

        // Hold the registry locally so we can inspect cache state
        // between queries. Same instance is installed on the
        // SessionState, so cross-query sharing is exercised.
        let registry = Arc::new(SharedSubtreeRegistry::new());
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(4))
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(
                DedupeAggregateForFloatDeterminism::with_registry(registry.clone()),
            ))
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table("revenue_t", mt).unwrap();

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

        assert_eq!(registry.len(), 0, "registry starts empty");

        // First query — populates the cache.
        let df = ctx.sql(sql).await.unwrap();
        let batches1 = df.collect().await.unwrap();
        let rows1: usize = batches1.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows1, 1);
        let entries_after_first = registry.len();
        assert!(
            entries_after_first >= 1,
            "expected ≥1 cache entry after first query, got {entries_after_first}",
        );

        // Second query — same shape on same context. Cache hit. The
        // structural hash is deterministic, so no new entries are added.
        let df = ctx.sql(sql).await.unwrap();
        let batches2 = df.collect().await.unwrap();
        let rows2: usize = batches2.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows2, 1);
        assert_eq!(
            registry.len(),
            entries_after_first,
            "second query should reuse the existing cache entries, not add new ones",
        );
    }
}
