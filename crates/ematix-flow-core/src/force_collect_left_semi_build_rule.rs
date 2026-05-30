//! `ForceCollectLeftForSemiBoundedBuildRule` — REV.3 (2026-05-30).
//!
//! Fixes the Q18-class "Partitioned dimension⋈fact join hash-
//! repartitions the whole fact table behind a tiny semi-bounded build"
//! anti-pattern by forcing the join to `PartitionMode::CollectLeft`
//! (broadcast the small build once, stream the probe with NO hash
//! exchange) — the DuckDB-equivalent execution shape.
//!
//! ## Why
//!
//! TPC-H Q18: an Inner `HashJoinExec` joins a dimension subtree
//! (customer⋈orders⋈semi → ~624 rows at SF=10) to the lineitem fact
//! scan on `o_orderkey = l_orderkey`. Because the build subtree's
//! cardinality is `Precision::Absent` (an `AggregateExec` / semi-join
//! output — see `swap_semi_join_build_rule` docs), `JoinSelection`
//! cannot prove the build small and defaults to
//! `PartitionMode::Partitioned`, hash-repartitioning ALL 60M (SF=10) /
//! 600M (SF=100) lineitem rows to feed a 624-row build. DuckDB streams
//! the probe through a shared hash table with no exchange. That wasted
//! `O(|lineitem|)` shuffle is invisible at SF=1 (fits cache) and
//! dominant at SF=100 — the Q18 2.53× loss vs DuckDB.
//!
//! ## What this rule does
//!
//! Runs as a post-`JoinSelection` / `EnforceDistribution` physical
//! pass. For each Inner `HashJoinExec` in `Partitioned` mode whose
//! build (LEFT) subtree contains a semi/anti join — the structural
//! signal that the build is membership-bounded, hence small (detected
//! by [`build_subtree_has_semi_filter`]) — it rebuilds the join in
//! `CollectLeft` mode via `hj.builder().with_partition_mode(...)`.
//!
//! The children are untouched by the transform, so the rewritten plan
//! momentarily violates CollectLeft's "left == 1 partition" invariant.
//! We then re-run `EnforceDistribution`, which sees CollectLeft's
//! `required_input_distribution = [SinglePartition, Unspecified]`,
//! coalesces the build to a single partition, and drops the now-
//! redundant Hash repartition on the probe. This is exactly the Σ.BS
//! "repair partitioning after a structural rewrite" pattern (re-run
//! `EnforceDistribution`, gated on `transformed`).
//!
//! ## Why this is safe
//!
//! - A semi/anti-bounded build is membership-filtered, so it is
//!   genuinely small — broadcasting it is cheap. If a semi were
//!   pathologically non-selective, the worst case is a perf
//!   regression, never incorrectness: CollectLeft is semantically
//!   identical to Partitioned (same join output).
//! - The `build_subtree_has_semi_filter` gate already detects all four
//!   semi/anti variants, including the `RightSemi` that
//!   `SwapSemiJoinBuildSideRule` produces for Q18.
//!
//! ## Opt-in
//!
//! Default OFF (per the optimizer-codegen-sensitivity tax — adding a
//! pass perturbs the geomean). Callers route it in via
//! [`install_force_collect_left_semi_build_rule`] when enabled.

use std::sync::Arc;

use datafusion::common::JoinType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::Result;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::enforce_distribution::EnforceDistribution;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};

use crate::runtime_bloom_sideband_rule::build_subtree_has_semi_filter;

/// See module docs.
#[derive(Debug, Default)]
pub struct ForceCollectLeftForSemiBoundedBuildRule;

/// Install the REV.3 CollectLeft rule onto a `SessionStateBuilder`.
pub fn install_force_collect_left_semi_build_rule(
    builder: SessionStateBuilder,
) -> SessionStateBuilder {
    builder.with_physical_optimizer_rule(Arc::new(ForceCollectLeftForSemiBoundedBuildRule))
}

impl PhysicalOptimizerRule for ForceCollectLeftForSemiBoundedBuildRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let trace = std::env::var_os("EMAT_COLLECT_LEFT_TRACE").is_some();
        let rewritten = plan.transform_up(|node| {
            let Some(hj) = node.as_any().downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };
            // Only Inner joins in Partitioned mode are candidates. (Semi/
            // anti joins are handled by SwapSemiJoinBuildSideRule; other
            // modes are already CollectLeft or Auto.)
            if !matches!(hj.join_type(), JoinType::Inner) {
                return Ok(Transformed::no(node));
            }
            if !matches!(hj.partition_mode(), PartitionMode::Partitioned) {
                return Ok(Transformed::no(node));
            }
            // Determine which side is semi/anti-bounded — the structural
            // signal that it is genuinely small (DataFusion's stats report
            // `Absent` for it, which is why JoinSelection picked
            // Partitioned). We want that side to be the BUILD and to be
            // broadcast (CollectLeft), so the large side streams with no
            // hash exchange.
            let left_semi = build_subtree_has_semi_filter(hj.left());
            let right_semi = build_subtree_has_semi_filter(hj.right());
            let new_join: Arc<dyn ExecutionPlan> = if left_semi && !right_semi {
                // Semi-bounded side is already the build (left) → broadcast
                // it. (Q18 top join: customer⋈orders⋈semi ⋈ lineitem.)
                if trace {
                    eprintln!(
                        "[collect_left] CollectLeft (left build semi-bounded); on={:?}",
                        hj.on()
                    );
                }
                Arc::new(
                    hj.builder()
                        .with_partition_mode(PartitionMode::CollectLeft)
                        .build()?,
                )
            } else if right_semi && !left_semi {
                // Semi-bounded side is the PROBE (right) → swap it onto the
                // build, then broadcast. Fixes the Q18 customer⋈orders
                // inversion: build the ~624-order semi-filtered side, not
                // the 15M-row customer side. `swap_inputs` flips
                // children/on-keys/filter and (for Inner) reorders the
                // output to preserve the schema, so the parent join's
                // column indices stay valid.
                if trace {
                    eprintln!(
                        "[collect_left] swap+CollectLeft (right probe semi-bounded); on={:?}",
                        hj.on()
                    );
                }
                hj.swap_inputs(PartitionMode::CollectLeft)?
            } else {
                // Neither side, or both sides, semi-bounded → leave alone.
                return Ok(Transformed::no(node));
            };
            Ok(Transformed::yes(new_join))
        })?;

        if !rewritten.transformed {
            return Ok(rewritten.data);
        }

        // Repair partitioning: EnforceDistribution coalesces each
        // CollectLeft build to a single partition (its
        // `required_input_distribution` demands SinglePartition) and
        // drops the now-unnecessary Hash repartition on the probe side
        // (Unspecified distribution). Mirrors the Σ.BS dedupe-rule fix.
        EnforceDistribution::new().optimize(rewritten.data, config)
    }

    fn name(&self) -> &str {
        "ematix_flow_force_collect_left_semi_bounded_build"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::NullEquality;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_plan::expressions::Column;

    fn mem_table() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
        )
        .unwrap();
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    fn on() -> Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> {
        vec![(
            Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
        )]
    }

    fn hashjoin(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        jt: JoinType,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                on(),
                None,
                &jt,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        )
    }

    /// REV.3 — an Inner Partitioned `HashJoinExec` whose build (LEFT)
    /// subtree is semi-bounded (Q18 shape: a RightSemi sits in the
    /// build) must be rewritten to `CollectLeft`.
    #[test]
    fn inner_partitioned_over_semi_bounded_build_flips_to_collect_left() {
        // Build (LEFT) = a RightSemi join → semi-bounded.
        let semi = hashjoin(mem_table(), mem_table(), JoinType::RightSemi);
        // Top Inner join: build = semi subtree, probe = a fact-like scan.
        let top = hashjoin(semi, mem_table(), JoinType::Inner);

        // Precondition: the top join is Partitioned.
        let before = format!("{top:?}");
        assert!(
            before.contains("Partitioned"),
            "precondition: top join should start Partitioned:\n{before}"
        );

        let rule = ForceCollectLeftForSemiBoundedBuildRule;
        let out = rule.optimize(top, &ConfigOptions::default()).unwrap();
        let after = format!("{out:?}");
        assert!(
            after.contains("CollectLeft"),
            "top Inner join over a semi-bounded build must flip to CollectLeft:\n{after}"
        );
    }

    /// Negative: an Inner Partitioned join with NO semi/anti in its
    /// build must be left as Partitioned — we must not broadcast an
    /// unbounded build.
    #[test]
    fn inner_partitioned_without_semi_build_is_untouched() {
        let top = hashjoin(mem_table(), mem_table(), JoinType::Inner);
        let rule = ForceCollectLeftForSemiBoundedBuildRule;
        let out = rule.optimize(top, &ConfigOptions::default()).unwrap();
        let after = format!("{out:?}");
        assert!(
            !after.contains("CollectLeft"),
            "an Inner join with no semi-bounded build must stay Partitioned:\n{after}"
        );
    }

    /// REV.3 option 3 — when the semi-bounded side is the PROBE (right),
    /// the rule must SWAP it onto the build and broadcast (CollectLeft).
    /// This is the Q18 customer⋈orders inversion fix: build the ~624-order
    /// semi-filtered side, not the 15M-row customer side.
    #[test]
    fn inner_partitioned_with_semi_bounded_probe_swaps_and_collect_lefts() {
        // Probe (RIGHT) = a RightSemi join → semi-bounded; build (LEFT) =
        // a plain scan (customer-like, NOT semi-bounded).
        let semi = hashjoin(mem_table(), mem_table(), JoinType::RightSemi);
        let top = hashjoin(mem_table(), semi, JoinType::Inner);

        let rule = ForceCollectLeftForSemiBoundedBuildRule;
        let out = rule.optimize(top, &ConfigOptions::default()).unwrap();
        let after = format!("{out:?}");
        assert!(
            after.contains("CollectLeft"),
            "an Inner join whose PROBE is semi-bounded must swap onto the \
             build and CollectLeft:\n{after}"
        );
    }
}
