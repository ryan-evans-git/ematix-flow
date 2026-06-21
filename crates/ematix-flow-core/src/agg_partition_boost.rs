//! Σ.AN.1 — per-operator partition routing for high-cardinality aggregates.
//!
//! ## What this fixes
//!
//! Q18 SF=10's `AggregateExec(FinalPartitioned) GROUP BY l_orderkey`
//! produces ~15M groups distributed across 14 input partitions
//! (default = cores). Each partition's hash table is ~1.07M × 120 B
//! ≈ 125 MB — way past M3 Pro's L3 cache (12 MB shared). The
//! result: cache-miss-dominated probes, ~50 ms wall-time tax.
//!
//! Σ.AN.0 measurement showed `PARTITIONS=112` (8× cores) closes
//! ~50 ms of the Q18 gap by reducing per-partition hash table to
//! ~16 MB. But applying that globally regressed small-output queries
//! +4.4% on 22q (Q02 +37%, Q16 +22%, etc.) from per-partition
//! coordination overhead.
//!
//! ## Mechanism
//!
//! A `PhysicalOptimizerRule` that walks the plan top-down. For each
//! `AggregateExec(mode=FinalPartitioned)` whose input is a
//! `RepartitionExec(Hash([...]), N)`, the rule:
//!
//! 1. Estimates the agg's output cardinality (group count).
//! 2. Computes optimal partitions via:
//!    `clamp(ceil(card / 50_000), session_cores, 8 * session_cores)`
//! 3. If `optimal != current`, rewrites:
//!    - The input `RepartitionExec` to use the new partition count.
//!    - Inserts a NEW `RepartitionExec` AFTER the agg to restore the
//!      session partition count for downstream operators.
//!
//! Formula constants derived for M3 Pro (L3=12 MB):
//! - `TARGET_GROUPS_PER_PARTITION = 50_000` ≈ 6 MB per partition table
//! - `MAX_PARTITIONS_MULTIPLIER = 8` from the Q18 partition sweep
//!   inflection point (P=112 was optimal; P=224 regressed due to
//!   coordination overhead).
//!
//! Different hardware (Xeon L3=30 MB) would want different
//! constants — bake here for now; deferred autotune
//! (`[[autotune-program-deferred]]`) is the longer-term answer.
//!
//! ## Cardinality estimate
//!
//! Three-source fallback chain:
//! 1. Try the agg's input `partition_statistics(None).num_rows` Exact.
//!    For high-cardinality cases like Q18 (15M groups in 60M rows),
//!    this returns 60M which is an upper bound — over-shards
//!    slightly but doesn't break the formula's clamp ceiling.
//! 2. Fall back to Inexact num_rows.
//! 3. Fall back to a conservative `1_000_000` default so the rule
//!    fires when stats are missing but the agg might still be large.
//!
//! Σ.AH.2 work populates `distinct_count` from parquet dict pages
//! for some columns — we don't currently surface that through the
//! AggregateExec input chain (would need a separate walker). For
//! Phase 1, num_rows upper bound is sufficient.
//!
//! ## Default
//!
//! Opt-in via `EMAT_AGG_PARTITION_BOOST=1`. Following the
//! Σ.AK/Σ.AL/Σ.AM.1 pattern: ship as banked infra; flip default ON
//! only after strict A/B confirms no codegen tax
//! (`[[optimizer-codegen-sensitivity]]`).

use std::sync::Arc;

use datafusion::common::DataFusionError;
use datafusion::common::config::ConfigOptions;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning};

use crate::robin_hood_sum_f64_exec::{RobinHoodSumF64Exec, RobinHoodSumF64Mode};

type DfResult<T> = Result<T, DataFusionError>;

/// Target group count per output partition. Derived from M3 Pro
/// L3=12 MB / per-group overhead ≈ 120 B → ~50K groups × 120 B = 6 MB
/// per partition table, fits L3 with shared-cache headroom.
const TARGET_GROUPS_PER_PARTITION: usize = 50_000;

/// Cap multiplier on partition count. Q18 SF=10 sweep showed P=112
/// (8× cores) optimal; P=224 (16×) regressed due to coordination
/// overhead (per-partition setup + repartition send/recv).
const MAX_PARTITIONS_MULTIPLIER: usize = 8;

/// Conservative fallback group estimate when partition_statistics
/// returns Absent. Chosen to trigger the boost on plausible
/// medium-cardinality aggregates without firing on tiny ones.
const FALLBACK_GROUP_ESTIMATE: usize = 1_000_000;

/// Σ.AN.1 — physical optimizer rule that boosts the partition count
/// of `RepartitionExec(Hash) → AggregateExec(FinalPartitioned)`
/// pipelines whose agg output cardinality exceeds the per-partition
/// fit budget.
///
/// Opt-in via `EMAT_AGG_PARTITION_BOOST=1` env var; defaults to off
/// per the Σ.AK/Σ.AL/Σ.AM.1 banked-infra pattern.
#[derive(Default, Debug)]
pub struct AggPartitionBoostRule;

impl PhysicalOptimizerRule for AggPartitionBoostRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let enabled = crate::flags::opt_in("EMAT_AGG_PARTITION_BOOST");
        if !enabled {
            return Ok(plan);
        }
        let trace = crate::flags::present("EMAT_AGG_PARTITION_BOOST_TRACE");

        plan.transform_up(|node| {
            // Match the agg layer. We recognize both DataFusion's
            // AggregateExec(FinalPartitioned) and our own
            // RobinHoodSumF64Exec(FinalPartitioned) (Σ.NF.3 — kicks
            // in when EMAT_RH_SUM_F64=1, which is the default).
            let is_final_partitioned_agg = if let Some(agg) =
                node.as_any().downcast_ref::<AggregateExec>()
            {
                matches!(agg.mode(), AggregateMode::FinalPartitioned)
                    && !agg.group_expr().is_empty()
            } else if let Some(rh) = node.as_any().downcast_ref::<RobinHoodSumF64Exec>() {
                matches!(rh.mode(), RobinHoodSumF64Mode::FinalPartitioned)
            } else {
                false
            };
            if !is_final_partitioned_agg {
                return Ok(Transformed::no(node));
            }
            // Agg's input must be a RepartitionExec(Hash). We don't
            // attempt to rewrite if the planner used some other
            // partitioning strategy.
            let agg_children = node.children();
            if agg_children.len() != 1 {
                return Ok(Transformed::no(node));
            }
            let agg_input = agg_children[0].clone();
            let Some(rep) = agg_input.as_any().downcast_ref::<RepartitionExec>() else {
                if trace {
                    eprintln!(
                        "[Σ.AN.1] skip — agg input is not RepartitionExec ({})",
                        agg_input.name()
                    );
                }
                return Ok(Transformed::no(node));
            };
            let Partitioning::Hash(hash_exprs, current_n) = rep.partitioning().clone() else {
                if trace {
                    eprintln!("[Σ.AN.1] skip — repartition is not Hash");
                }
                return Ok(Transformed::no(node));
            };

            // Estimate cardinality from the repartition's input (= agg's
            // input upstream). Use num_rows as upper bound on distinct
            // groups.
            let estimated_groups = estimate_group_cardinality(rep.input());
            let session_cores = current_n.max(1);
            let optimal_n = compute_optimal_partitions(estimated_groups, session_cores);

            if optimal_n == current_n {
                if trace {
                    eprintln!(
                        "[Σ.AN.1] no-op — estimated_groups={estimated_groups}, current={current_n}, optimal={optimal_n}"
                    );
                }
                return Ok(Transformed::no(node));
            }

            if trace {
                eprintln!(
                    "[Σ.AN.1] BOOST — estimated_groups={estimated_groups}, {current_n} → {optimal_n} partitions"
                );
            }

            // Rewrite: new RepartitionExec with boosted partition count
            // using the same hash expressions.
            let boosted_partitioning =
                Partitioning::Hash(hash_exprs.clone(), optimal_n);
            let boosted_rep = Arc::new(RepartitionExec::try_new(
                rep.input().clone(),
                boosted_partitioning,
            )?);

            // Rebuild the agg with the new input.
            let new_agg = Arc::clone(&node)
                .with_new_children(vec![boosted_rep as Arc<dyn ExecutionPlan>])?;

            // Insert a restore Repartition AFTER the boosted agg to
            // bring partition count back to session default for
            // downstream operators. CRITICAL: must use the SAME hash
            // expressions, otherwise downstream HashJoinExec(Partitioned)
            // and similar consumers see a partition layout that doesn't
            // match their join key and silently produce wrong rows.
            let restore_partitioning = Partitioning::Hash(hash_exprs, current_n);
            let restore_rep = Arc::new(RepartitionExec::try_new(
                new_agg,
                restore_partitioning,
            )?);

            Ok(Transformed::yes(restore_rep))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "AggPartitionBoostRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Estimate the group cardinality upper bound.
///
/// Walks downward through Partial aggregates / Repartitions / Filters
/// to find a leaf scan whose num_rows can serve as an upper bound on
/// the group count. The Partial-agg layer doesn't expose meaningful
/// statistics (it returns Absent), so a naive `input.partition_statistics()`
/// on it produces no signal. We walk past those bookkeeping layers
/// to reach the data-source row count.
///
/// Bounded recursion depth (10 levels) defends against malformed plans
/// or accidental cycles.
fn estimate_group_cardinality(input: &Arc<dyn ExecutionPlan>) -> usize {
    if let Some(n) = walk_for_row_count(input.as_ref(), 10) {
        return n.max(1);
    }
    FALLBACK_GROUP_ESTIMATE
}

fn walk_for_row_count(plan: &dyn ExecutionPlan, depth: usize) -> Option<usize> {
    if depth == 0 {
        return None;
    }
    // Try the current node's stats first.
    if let Ok(stats) = plan.partition_statistics(None) {
        match stats.num_rows {
            Precision::Exact(n) | Precision::Inexact(n) => return Some(n),
            Precision::Absent => {}
        }
    }
    // Recurse — pick the max child estimate (most conservative upper bound).
    let mut best: Option<usize> = None;
    for child in plan.children() {
        if let Some(n) = walk_for_row_count(child.as_ref(), depth - 1) {
            best = Some(best.map(|prev| prev.max(n)).unwrap_or(n));
        }
    }
    best
}

/// Computes optimal partition count via the Σ.AN.1 formula:
///
/// ```text
/// clamp(
///     ceil(expected_groups / TARGET_GROUPS_PER_PARTITION),
///     session_cores,
///     session_cores * MAX_PARTITIONS_MULTIPLIER,
/// )
/// ```
fn compute_optimal_partitions(expected_groups: usize, session_cores: usize) -> usize {
    if session_cores == 0 {
        return 1;
    }
    let raw = expected_groups.div_ceil(TARGET_GROUPS_PER_PARTITION);
    let ceiling = session_cores.saturating_mul(MAX_PARTITIONS_MULTIPLIER);
    raw.clamp(session_cores, ceiling)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formula_below_target_returns_session_cores() {
        // 1K groups, 14 cores → cap at session_cores
        assert_eq!(compute_optimal_partitions(1_000, 14), 14);
    }

    #[test]
    fn formula_at_target_returns_session_cores() {
        // 50K * 14 = 700K — exactly fills 14 partitions at target
        assert_eq!(compute_optimal_partitions(700_000, 14), 14);
    }

    #[test]
    fn formula_just_above_target_boosts() {
        // 1M groups / 50K = 20 partitions, which is between 14 and 112
        assert_eq!(compute_optimal_partitions(1_000_000, 14), 20);
    }

    #[test]
    fn formula_q18_shape_caps_at_ceiling() {
        // Q18: 15M groups / 50K = 300, but ceiling = 14 × 8 = 112
        assert_eq!(compute_optimal_partitions(15_000_000, 14), 112);
    }

    #[test]
    fn formula_zero_cores_returns_one() {
        // Defensive against degenerate config
        assert_eq!(compute_optimal_partitions(1_000_000, 0), 1);
    }

    #[test]
    fn formula_q13_shape_modest_boost() {
        // Q13: ~1.5M groups / 50K = 30 partitions (above cores=14, under 112)
        assert_eq!(compute_optimal_partitions(1_500_000, 14), 30);
    }

    #[test]
    fn formula_huge_input_clamps_to_ceiling() {
        // Pathological: 1B groups → would want 20K partitions; clamps to 112
        assert_eq!(compute_optimal_partitions(1_000_000_000, 14), 112);
    }

    #[test]
    fn formula_alt_cores() {
        // Larger machine: 56 cores, 15M groups → ceiling 448
        // raw = 300, clamped to (56, 448) → 300
        assert_eq!(compute_optimal_partitions(15_000_000, 56), 300);
    }
}
