//! Σ.SP Phase 1b — `GraceJoinDemotionRule`: rewrite a `HashJoinExec`
//! whose build side is HONESTLY estimated to exceed the memory budget
//! into a [`GraceHashJoinExec`](crate::grace_hash_join::GraceHashJoinExec).
//!
//! The stock hash join cannot spill (DF 53); an oversized build either
//! rides the page-cache margin to a kernel OOM or fails at the
//! `ElasticFloorPool`. This rule is the *plan-time* alternative to the
//! upstream reactive-spill proposal (apache/datafusion#17267, unlanded):
//! the Σ.JS bottom-up estimator prices the build side before execution,
//! and only a KNOWN estimate may demote — an unknown side makes no
//! decision, exactly Σ.JS.1's honesty rule, so healthy TPC-H plans are
//! untouched and bench == release holds.
//!
//! Gate: `EMAT_GRACE_JOIN` tri-state, **default ON** since Σ.MG
//! (2026-07-11): the rule self-gates on an HONEST oversize estimate
//! (est build > budget), so on healthy plans it demotes nothing —
//! the earlier full-22q refutation (grace 108.7 s vs off 106.1 s,
//! Q09 +3.7 s) was measured on pre-Σ.JS.3 main, whose parted Q09
//! carried 150M-row builds that tripped demotion; post-Σ.JS.3 those
//! builds are the small filtered sides and SF=100 demotes zero
//! joins. The flip buys SF=1000 completion (oversized builds spill
//! instead of kernel-OOMing a 128 GB box). `EMAT_GRACE_JOIN=0` opts
//! out. Budget: `EMAT_GRACE_BUILD_BYTES=<n>` explicit, else AUTO =
//! ½ × sensed `MemAvailable` at plan time (the Σ.AI.6c sensor;
//! platforms without it never demote).
//!
//! Scope (Phase 2): Inner / LeftSemi / LeftAnti joins (the Q21 shape),
//! residual filter forwarded verbatim; still no embedded projection.
//! The build side under CollectLeft is the LEFT input for all three,
//! so the demotion estimate stays left-side.

use std::sync::Arc;

use datafusion::common::JoinType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::enforce_distribution::EnforceDistribution;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::HashJoinExec;

use crate::grace_hash_join::GraceHashJoinExec;
use crate::join_side_rule::estimate_rows;

/// Per-row byte estimate for a schema: fixed-width primitives priced
/// exactly, variable-width (strings/binary/views/nested) at a
/// deliberately conservative 24 B. Over-pricing biases TOWARD demotion
/// only for genuinely huge builds — the budget check also has the 2×
/// headroom of the pair sizing.
fn estimated_row_bytes(schema: &datafusion::arrow::datatypes::Schema) -> f64 {
    schema
        .fields()
        .iter()
        .map(|f| f.data_type().primitive_width().unwrap_or(24) as f64)
        .sum::<f64>()
        .max(8.0)
}

/// Spill fan-out for an estimated build: enough pairs that one pair's
/// build ≈ half the budget, clamped to [4, 256], rounded up to a power
/// of two (hash-modulo balance).
fn spill_partitions(est_build_bytes: f64, budget_bytes: f64) -> usize {
    let per_pair = (budget_bytes / 2.0).max(1.0);
    let k = (est_build_bytes / per_pair).ceil() as usize;
    k.clamp(4, 256).next_power_of_two().min(256)
}

/// See module docs.
#[derive(Debug)]
pub struct GraceJoinDemotionRule {
    /// Snapshot of `EMAT_GRACE_JOIN` (tri-state, default ON; `=0`
    /// opts out).
    pub enabled: bool,
    /// Explicit budget (`EMAT_GRACE_BUILD_BYTES`); `None` = AUTO from
    /// the MemAvailable sensor at optimize time.
    pub budget_bytes: Option<u64>,
}

impl Default for GraceJoinDemotionRule {
    fn default() -> Self {
        Self {
            enabled: crate::flags::tri_state("EMAT_GRACE_JOIN").unwrap_or(true),
            budget_bytes: std::env::var("EMAT_GRACE_BUILD_BYTES")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|b| *b > 0),
        }
    }
}

impl GraceJoinDemotionRule {
    /// Budget for this optimize pass: explicit env, else ½ of sensed
    /// MemAvailable, else `None` (never demote — no honest budget).
    fn resolved_budget(&self) -> Option<f64> {
        if let Some(b) = self.budget_bytes {
            return Some(b as f64);
        }
        crate::mem_pool::sensed_available_bytes().map(|a| a as f64 / 2.0)
    }
}

impl PhysicalOptimizerRule for GraceJoinDemotionRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !self.enabled {
            return Ok(plan);
        }
        let Some(budget) = self.resolved_budget() else {
            return Ok(plan);
        };
        let trace = crate::flags::present("EMAT_GRACE_JOIN_TRACE");
        let rewritten = plan.transform_up(|node| {
            let Some(hj) = node.as_any().downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };
            // Phase 2 shape: Inner/LeftSemi/LeftAnti (residual filter
            // forwards verbatim), no embedded projection — what
            // GraceHashJoinExec reproduces exactly.
            if !matches!(
                hj.join_type(),
                JoinType::Inner | JoinType::LeftSemi | JoinType::LeftAnti
            ) || hj.projection.is_some()
            {
                return Ok(Transformed::no(node));
            }
            // Honest estimate or no decision (Σ.JS.1's rule).
            let Some(build_rows) = estimate_rows(hj.left()) else {
                return Ok(Transformed::no(node));
            };
            let est_bytes = build_rows * estimated_row_bytes(hj.left().schema().as_ref());
            if est_bytes <= budget {
                return Ok(Transformed::no(node));
            }
            let k = spill_partitions(est_bytes, budget);
            if trace {
                eprintln!(
                    "[grace_join] demote: join_type={:?} est_build_bytes={est_bytes:.0} \
                     budget={budget:.0} k={k} on={:?}",
                    hj.join_type(),
                    hj.on()
                );
            }
            Ok(Transformed::yes(Arc::new(GraceHashJoinExec::try_new(
                Arc::clone(hj.left()),
                Arc::clone(hj.right()),
                hj.on().to_vec(),
                *hj.join_type(),
                hj.filter().cloned(),
                k,
            )?)))
        })?;
        if !rewritten.transformed {
            return Ok(rewritten.data);
        }
        // The demoted node exposes ONE output partition where the hash
        // join exposed many — re-derive the exchanges above it (the
        // Σ.BS/Σ.JS repair pattern). Ordering-sensitive roots are safe:
        // grace demotion only ever REDUCES partitioning, and
        // EnforceSorting's work is preserved because we bracket with
        // OutputRequirements like the stock pipeline does.
        use datafusion::physical_optimizer::enforce_sorting::EnforceSorting;
        use datafusion::physical_optimizer::output_requirements::OutputRequirements;
        let repaired = OutputRequirements::new_add_mode().optimize(rewritten.data, config)?;
        let repaired = EnforceDistribution::new().optimize(repaired, config)?;
        let repaired = EnforceSorting::new().optimize(repaired, config)?;
        OutputRequirements::new_remove_mode().optimize(repaired, config)
    }

    fn name(&self) -> &str {
        "ematix_flow_grace_join_demotion"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::NullEquality;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::ExecutionPlanProperties;
    use datafusion::physical_plan::common::collect;
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::joins::PartitionMode;
    use datafusion::physical_plan::joins::utils::JoinOn;
    use datafusion::prelude::SessionContext;

    // Repo trap (de-flake 23078a3a): no `*key` column names.

    fn mem_side(name: &str, n: usize, modulo: usize) -> (SchemaRef, Arc<dyn ExecutionPlan>) {
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(
                (0..n).map(|i| (i % modulo) as i64),
            ))],
        )
        .unwrap();
        let exec =
            MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&schema), None).unwrap();
        (schema, exec)
    }

    fn inner_join(l: Arc<dyn ExecutionPlan>, r: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        let on: JoinOn = vec![(
            Arc::new(Column::new("ident", 0)) as _,
            Arc::new(Column::new("ref_a", 0)) as _,
        )];
        Arc::new(
            HashJoinExec::try_new(
                l,
                r,
                on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        )
    }

    fn rule(budget: u64) -> GraceJoinDemotionRule {
        GraceJoinDemotionRule {
            enabled: true,
            budget_bytes: Some(budget),
        }
    }

    async fn rows_sorted(plan: &Arc<dyn ExecutionPlan>, ctx: &SessionContext) -> Vec<String> {
        let mut all = Vec::new();
        for p in 0..plan.output_partitioning().partition_count() {
            all.extend(
                collect(plan.execute(p, ctx.task_ctx()).unwrap())
                    .await
                    .unwrap(),
            );
        }
        let mut rows: Vec<String> = pretty_format_batches(&all)
            .unwrap()
            .to_string()
            .lines()
            .map(str::to_string)
            .collect();
        rows.sort();
        rows
    }

    /// Oversized honest build → demoted to GraceHashJoinExec, and the
    /// demoted plan returns IDENTICAL results.
    #[tokio::test(flavor = "multi_thread")]
    async fn demotes_oversized_build_with_result_parity() {
        let ctx = SessionContext::new();
        let (_, l) = mem_side("ident", 10_000, 500); // est 10k rows × 8B = 80 kB
        let (_, r) = mem_side("ref_a", 2_000, 500);
        let plan = inner_join(l, r);
        let expect = rows_sorted(&plan, &ctx).await;

        let out = rule(1_000) // budget 1 kB << 80 kB build
            .optimize(Arc::clone(&plan), &ConfigOptions::default())
            .unwrap();
        let text = format!("{}", displayable(out.as_ref()).indent(true));
        assert!(
            text.contains("GraceHashJoinExec"),
            "oversized build must demote:\n{text}"
        );
        let got = rows_sorted(&out, &ctx).await;
        assert_eq!(got, expect, "demoted plan must return identical rows");
    }

    /// Build inside the budget → untouched plan.
    #[tokio::test(flavor = "multi_thread")]
    async fn no_demotion_within_budget() {
        let (_, l) = mem_side("ident", 100, 50);
        let (_, r) = mem_side("ref_a", 100, 50);
        let plan = inner_join(l, r);
        let out = rule(1 << 20) // 1 MiB budget >> 800 B build
            .optimize(Arc::clone(&plan), &ConfigOptions::default())
            .unwrap();
        assert!(
            !format!("{}", displayable(out.as_ref()).indent(true)).contains("GraceHashJoinExec"),
            "in-budget build must not demote"
        );
    }

    /// The default snapshots the tri-state: unset → ON, `=0` → OFF
    /// (the Σ.MG flip; the oversize estimate is the real gate).
    #[test]
    fn default_snapshots_tri_state_on() {
        let _env = crate::flags::EMAT_ENV_TEST_LOCK.blocking_lock();
        unsafe { std::env::remove_var("EMAT_GRACE_JOIN") };
        assert!(
            GraceJoinDemotionRule::default().enabled,
            "unset => grace demotion armed (default ON)"
        );
        unsafe { std::env::set_var("EMAT_GRACE_JOIN", "0") };
        assert!(
            !GraceJoinDemotionRule::default().enabled,
            "=0 => opt-out honored"
        );
        unsafe { std::env::remove_var("EMAT_GRACE_JOIN") };
    }

    /// Disabled rule (`EMAT_GRACE_JOIN=0`) never rewrites.
    #[tokio::test(flavor = "multi_thread")]
    async fn disabled_rule_is_inert() {
        let (_, l) = mem_side("ident", 10_000, 500);
        let (_, r) = mem_side("ref_a", 2_000, 500);
        let plan = inner_join(l, r);
        let off = GraceJoinDemotionRule {
            enabled: false,
            budget_bytes: Some(1),
        };
        let out = off
            .optimize(Arc::clone(&plan), &ConfigOptions::default())
            .unwrap();
        assert_eq!(
            format!("{}", displayable(plan.as_ref()).indent(true)),
            format!("{}", displayable(out.as_ref()).indent(true)),
            "default-OFF rule must leave the plan untouched"
        );
    }

    /// No budget resolvable (no env, no sensor — the macOS dev-box
    /// shape) → never demote, however big the build.
    #[tokio::test(flavor = "multi_thread")]
    async fn no_budget_means_no_demotion() {
        let (_, l) = mem_side("ident", 10_000, 500);
        let (_, r) = mem_side("ref_a", 2_000, 500);
        let plan = inner_join(l, r);
        let auto = GraceJoinDemotionRule {
            enabled: true,
            budget_bytes: None,
        };
        let out = auto
            .optimize(Arc::clone(&plan), &ConfigOptions::default())
            .unwrap();
        let text = format!("{}", displayable(out.as_ref()).indent(true));
        // On Linux CI the sensor resolves (huge MemAvailable ≫ 80 kB
        // build → still no demotion); on macOS it is None → inert.
        // Either way: no rewrite.
        assert!(!text.contains("GraceHashJoinExec"));
    }

    /// Fan-out sizing: one pair's build ≈ half the budget, power of
    /// two, clamped to [4, 256].
    #[test]
    fn spill_partition_sizing() {
        assert_eq!(spill_partitions(100.0, 1_000_000.0), 4, "floor clamp");
        assert_eq!(spill_partitions(1_000_000.0, 100_000.0), 32, "20 → pow2 32");
        assert_eq!(spill_partitions(1e12, 8.0), 256, "ceiling clamp");
    }

    // ---- Phase 2: semi / anti demotion ----

    fn typed_join(
        l: Arc<dyn ExecutionPlan>,
        r: Arc<dyn ExecutionPlan>,
        join_type: JoinType,
    ) -> Arc<dyn ExecutionPlan> {
        let on: JoinOn = vec![(
            Arc::new(Column::new("ident", 0)) as _,
            Arc::new(Column::new("ref_a", 0)) as _,
        )];
        Arc::new(
            HashJoinExec::try_new(
                l,
                r,
                on,
                None,
                &join_type,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        )
    }

    /// Oversized LeftSemi and LeftAnti builds demote and keep parity —
    /// the Q21 shape this arc exists for.
    #[tokio::test(flavor = "multi_thread")]
    async fn demotes_semi_and_anti_with_result_parity() {
        let ctx = SessionContext::new();
        for join_type in [JoinType::LeftSemi, JoinType::LeftAnti] {
            let (_, l) = mem_side("ident", 10_000, 500);
            let (_, r) = mem_side("ref_a", 2_000, 300); // matches 0..300 only
            let plan = typed_join(l, r, join_type);
            let expect = rows_sorted(&plan, &ctx).await;
            assert!(expect.len() > 4, "{join_type:?} oracle non-trivial");

            let out = rule(1_000)
                .optimize(Arc::clone(&plan), &ConfigOptions::default())
                .unwrap();
            let text = format!("{}", displayable(out.as_ref()).indent(true));
            assert!(
                text.contains("GraceHashJoinExec"),
                "oversized {join_type:?} build must demote:\n{text}"
            );
            let got = rows_sorted(&out, &ctx).await;
            assert_eq!(got, expect, "demoted {join_type:?} plan must match");
        }
    }

    /// Join types outside Inner/LeftSemi/LeftAnti are never demoted,
    /// however oversized the build.
    #[tokio::test(flavor = "multi_thread")]
    async fn unsupported_join_types_not_demoted() {
        let (_, l) = mem_side("ident", 10_000, 500);
        let (_, r) = mem_side("ref_a", 2_000, 500);
        let plan = typed_join(l, r, JoinType::Right);
        let out = rule(1_000)
            .optimize(Arc::clone(&plan), &ConfigOptions::default())
            .unwrap();
        assert!(
            !format!("{}", displayable(out.as_ref()).indent(true)).contains("GraceHashJoinExec"),
            "Right join must not demote"
        );
    }
}
