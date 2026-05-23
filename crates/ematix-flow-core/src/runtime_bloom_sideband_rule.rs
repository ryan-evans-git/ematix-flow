//! Σ.Q.L9 slice 3 — PhysicalOptimizerRule that wires up the runtime
//! sideband between a HashJoinExec's build side and the probe-side
//! EmatixFastParquetExec.
//!
//! For each `HashJoinExec` with an i64 equi-key whose probe side
//! reaches an `EmatixFastParquetExec`:
//!
//! 1. Allocate a fresh `BridgeFilterSideband`.
//! 2. Wrap the join's **build child** (left) with a
//!    `BuildSideBloomEmitterExec` that observes the build-side key
//!    column. Replace the join via `with_new_children` so DataFusion
//!    sees the wrapper.
//! 3. Walk the **probe child** (right), find the
//!    `EmatixFastParquetExec` carrying the probe column, replace it
//!    with `scan.with_runtime_sideband(sideband.clone())`.
//!
//! Empty mismatch (no probe-side EmatixFastParquetExec, non-Int64
//! key, swap-unsupported join type) is a no-op for that join. The
//! rule never errors out — it's purely speculative.
//!
//! ## Opt-in only
//!
//! Per [[optimizer-codegen-sensitivity]] this rule is NOT installed
//! by default. Callers route it in via [`install_runtime_bloom_sideband_rule`]
//! when they want the sideband mechanism active for their session.

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::Result as DfResult;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::joins::HashJoinExec;

use crate::bridge_filter_sideband::BridgeFilterSideband;
use crate::build_side_bloom_emitter_exec::BuildSideBloomEmitterExec;
use crate::ematix_fast_parquet::EmatixFastParquetExec;

/// Install the L9 runtime-sideband rule.
pub fn install_runtime_bloom_sideband_rule(
    builder: SessionStateBuilder,
) -> SessionStateBuilder {
    builder.with_physical_optimizer_rule(Arc::new(EnableRuntimeBloomSidebandRule::default()))
}

/// Σ.Q.L9 — selectivity gate config. Only fires the rule when the
/// build-side row estimate × `min_probe_to_build_ratio` is less than
/// the probe-side leaf scan's row count. Default ratio = 64 (probe
/// must be at least 64× the build). Set ratio = 0 to disable the gate
/// (always fire) — used by tests over miniature parquet files.
#[derive(Debug, Clone, Copy)]
pub struct EnableRuntimeBloomSidebandRule {
    pub min_probe_to_build_ratio: usize,
}

impl Default for EnableRuntimeBloomSidebandRule {
    fn default() -> Self {
        // 64× is a permissive floor — accommodates semi-join shapes
        // (build/probe ≈ 1/1000+) while gating out fact⋈fact joins
        // (build/probe ≈ 1/4) where the bloom can't be selective.
        // Honor `EMAT_RT_BLOOM_SELECTIVITY=N` override at construction
        // for ad-hoc bench tuning.
        let ratio = std::env::var("EMAT_RT_BLOOM_SELECTIVITY")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(64);
        Self {
            min_probe_to_build_ratio: ratio,
        }
    }
}

impl PhysicalOptimizerRule for EnableRuntimeBloomSidebandRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            let Some(hj) = node.as_any().downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };
            // Only Inner / LeftSemi / RightSemi shapes — others would
            // change semantics if we dropped probe rows.
            use datafusion::common::JoinType;
            if !matches!(
                hj.join_type(),
                JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi
            ) {
                return Ok(Transformed::no(node));
            }

            // Σ.Q.L9: build = LEFT, probe = RIGHT in DataFusion's
            // HashJoinExec (per the SwapSemiJoinBuildSideRule docs).
            let build = hj.left().clone();
            let probe = hj.right().clone();

            // Find an i64 equi-key whose probe side reaches an
            // EmatixFastParquetExec carrying that column. If none
            // found, this join contributes no sideband — leave it
            // alone.
            let mut matched: Option<(usize, usize, Arc<EmatixFastParquetExec>)> = None;
            for (left_expr, right_expr) in hj.on().iter() {
                let Some(lcol) = left_expr.as_any().downcast_ref::<Column>() else {
                    continue;
                };
                let Some(rcol) = right_expr.as_any().downcast_ref::<Column>() else {
                    continue;
                };
                if build.schema().field(lcol.index()).data_type() != &DataType::Int64 {
                    continue;
                }
                if probe.schema().field(rcol.index()).data_type() != &DataType::Int64 {
                    continue;
                }
                let col_name = rcol.name().to_string();
                if let Some((scan, scan_col_idx)) =
                    find_probe_scan_for_column(&probe, &col_name)
                {
                    matched = Some((lcol.index(), scan_col_idx, scan));
                    break;
                }
            }

            let Some((build_key_idx, probe_scan_col_idx, scan_arc)) = matched else {
                return Ok(Transformed::no(node));
            };

            // Σ.Q.L9 selectivity gate (2026-05-23, late-evening
            // investigation):
            //
            // When we made the deferred-peek fix to the consumer side
            // (EmatixFastParquetExec::execute), suddenly blooms that
            // had ALWAYS been published-but-invisible were now actually
            // being applied at the scan. That exposed a latent problem:
            // L9 was firing on EVERY HashJoinExec, regardless of
            // whether the bloom could plausibly drop probe rows.
            //
            // For Q18 SF=10, two of the three L9 firings emit blooms
            // with ~100% pass rate against the probe side:
            //   * customer (1.5M) ⋈ orders (15M) on c_custkey/o_custkey
            //     — orders.o_custkey ∈ customer.c_custkey by FK
            //     constraint, so the bloom membership check pays full
            //     cost and drops 0 rows.
            //   * orders⋈customer (15M) ⋈ lineitem (60M) on o_orderkey/
            //     l_orderkey — same: lineitem.l_orderkey ∈ orders.
            //
            // Net effect of those two firings: ~360s of CPU spent
            // computing `bloom.might_contain_i64(v)` on lineitem rows
            // that all pass anyway, regressing Q18 wall-time from 530ms
            // → 606ms.
            //
            // The third firing — RightSemi(624 keys) ⋈ lineitem — is
            // where the bloom genuinely helps (0.004% pass rate).
            //
            // Gate: skip the wrap unless the build is meaningfully
            // smaller than the probe-side scan. Threshold 1/64 is
            // permissive (allows blooms where the build is up to ~1.5%
            // of the probe). For TPC-H this gates out the
            // dimension⋈fact and fact⋈fact joins (build ≈ probe / 4)
            // while keeping semi-join shapes (build ≈ probe / 1000+).
            let build_rows = estimate_build_rows(build.as_ref());
            let probe_rows = estimate_probe_scan_rows(&scan_arc);
            if self.min_probe_to_build_ratio > 0 {
                if let (Some(b), Some(p)) = (build_rows, probe_rows) {
                    if b.saturating_mul(self.min_probe_to_build_ratio) >= p {
                        return Ok(Transformed::no(node));
                    }
                }
            }

            // Σ.Q.L9: allocate the sideband, wrap the build child,
            // rewrite the probe subtree to thread the sideband into
            // the matching EmatixFastParquetExec.
            let sideband = BridgeFilterSideband::new();

            // Estimate expected build-side keys from the build's
            // partition statistics. Falls back to a generous default
            // (50K) when stats are unavailable.
            let expected_keys = build_rows.unwrap_or(50_000);

            let wrapped_build: Arc<dyn ExecutionPlan> = Arc::new(BuildSideBloomEmitterExec::try_new(
                build,
                build_key_idx,
                probe_scan_col_idx,
                sideband.clone(),
                expected_keys,
            )?);

            let new_probe = rewrite_probe_subtree(&probe, scan_arc.as_ref(), &sideband)?;

            // Build a new HashJoinExec with the two new children. The
            // node itself is an Arc<dyn ExecutionPlan> pointing at this
            // HashJoinExec; call the trait method on it directly.
            let new_join =
                Arc::clone(&node).with_new_children(vec![wrapped_build, new_probe])?;
            Ok(Transformed::yes(new_join))
        })
        .data()
    }

    fn name(&self) -> &str {
        "ematix_flow_enable_runtime_bloom_sideband"
    }

    fn schema_check(&self) -> bool {
        // The wrapper's output schema matches its input. The probe
        // scan's schema is unchanged (with_runtime_sideband just
        // adds a side-channel field).
        true
    }
}

/// Descend `plan` looking for an `EmatixFastParquetExec` whose
/// schema contains a column named `col_name`. Returns the exec +
/// the column index in its schema.
///
/// Walks through both single-child wrappers (FilterExec,
/// ProjectionExec, CoalescePartitionsExec, CoalesceBatchesExec,
/// RepartitionExec, BuildSideBloomEmitterExec, etc.) and
/// multi-child plans (HashJoinExec, UnionExec). For multi-child
/// nodes the descent tries each child and returns the first hit —
/// "first child carrying this column wins."
///
/// **Assumption**: column names are unique across the underlying
/// tables in the probe subtree. For TPC-H this is the case
/// (`l_*`, `o_*`, `c_*`, `s_*`, `n_*`, `r_*`, `p_*`, `ps_*`
/// prefixes ensure uniqueness). Workloads with duplicate column
/// names across joined tables would need column-index threading
/// through join projections — substantially more code; not done
/// here.
fn find_probe_scan_for_column(
    plan: &Arc<dyn ExecutionPlan>,
    col_name: &str,
) -> Option<(Arc<EmatixFastParquetExec>, usize)> {
    if let Some(scan) = plan.as_any().downcast_ref::<EmatixFastParquetExec>() {
        let sch = scan.schema();
        if let Some((idx, _)) = sch
            .fields()
            .iter()
            .enumerate()
            .find(|(_, f)| f.name() == col_name)
        {
            // Return an Arc<EmatixFastParquetExec> via the no-op
            // `with_added_predicates` clone path. The Arc is used
            // only as a type-plumbing handle — `rewrite_probe_subtree`
            // locates the actual scan in the live plan by path() match
            // and rewrites THAT instance, not this clone.
            let fresh = scan.with_added_predicates(Vec::new()).ok()?;
            return Some((fresh, idx));
        }
        return None;
    }
    // Descend into every child. For 1-child wrappers this is a
    // straight chain; for multi-child plans (HashJoinExec, Union)
    // we try each child in turn — first match wins (see assumption
    // in the doc comment).
    for child in plan.children() {
        if let Some(found) = find_probe_scan_for_column(child, col_name) {
            return Some(found);
        }
    }
    None
}

fn rewrite_probe_subtree(
    plan: &Arc<dyn ExecutionPlan>,
    target_scan: &EmatixFastParquetExec,
    sideband: &BridgeFilterSideband,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    // Bottom-up rewrite: if we find an EmatixFastParquetExec whose
    // path matches the target's, replace it with the sideband-
    // attached version. Match by `path()` since multiple distinct
    // EmatixFastParquetExec instances over the same parquet would
    // be equally valid attachment points.
    plan.clone()
        .transform_up(|node| {
            if let Some(scan) = node.as_any().downcast_ref::<EmatixFastParquetExec>() {
                if scan.path() == target_scan.path() {
                    let new = scan.with_runtime_sideband(sideband.clone());
                    return Ok(Transformed::yes(new as Arc<dyn ExecutionPlan>));
                }
            }
            Ok(Transformed::no(node))
        })
        .data()
}

/// Σ.Q.L9 selectivity gate — best-effort row-count estimate for the
/// probe-side leaf scan. The scan exposes `num_rows` directly via its
/// own field, so this is a cheap accessor. Returns the total row
/// count across partitions.
fn estimate_probe_scan_rows(scan: &EmatixFastParquetExec) -> Option<usize> {
    // EmatixFastParquetExec doesn't expose num_rows publicly, but
    // partition_statistics(None) returns total row count.
    use datafusion::common::stats::Precision;
    let stats = scan.partition_statistics(None).ok()?;
    match stats.num_rows {
        Precision::Exact(n) | Precision::Inexact(n) => Some(n.max(1)),
        _ => None,
    }
}

/// Best-effort row-count estimate for the build subtree. Uses
/// `partition_statistics` when available, sums across partitions.
fn estimate_build_rows(plan: &dyn ExecutionPlan) -> Option<usize> {
    let n = plan.output_partitioning().partition_count();
    let mut total: usize = 0;
    let mut have_any = false;
    for p in 0..n {
        let stats = plan.partition_statistics(Some(p)).ok()?;
        if let datafusion::common::stats::Precision::Exact(rows)
        | datafusion::common::stats::Precision::Inexact(rows) = stats.num_rows
        {
            total = total.saturating_add(rows);
            have_any = true;
        }
    }
    if have_any { Some(total.max(64)) } else { None }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ematix_fast_parquet::EmatixFastParquetTableProvider;
    use crate::fast_parquet::FastParquetTableProvider;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use ematix_parquet_codec::write::{ColumnData, write_table_to_path};
    use ematix_parquet_format::types::CompressionCodec;

    fn tmp_parquet(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "l9_rule_test_{}_{}",
            std::process::id(),
            name
        ));
        let _ = std::fs::create_dir_all(&dir);
        dir.join(format!("{name}.parquet"))
    }

    fn write_lineitem(path: &std::path::Path) {
        // 1000 lineitem rows: l_orderkey ∈ [0,100), l_suppkey ∈ [0,50).
        let l_orderkey: Vec<i64> = (0..1000i64).map(|i| i % 100).collect();
        let l_suppkey: Vec<i64> = (0..1000i64).map(|i| i % 50).collect();
        write_table_to_path(
            path,
            &[
                ("l_orderkey", ColumnData::I64(&l_orderkey)),
                ("l_suppkey", ColumnData::I64(&l_suppkey)),
            ],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
    }

    fn write_supplier(path: &std::path::Path) {
        // 25 supplier rows; s_suppkey ∈ [0,25).
        let s_suppkey: Vec<i64> = (0..25i64).collect();
        write_table_to_path(
            path,
            &[("s_suppkey", ColumnData::I64(&s_suppkey))],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
    }

    /// Σ.Q.L9 extension — small filter set that joins onto lineitem
    /// through `l_suppkey`. Mirrors Q18's "624-key filter set →
    /// orders scan" shape at miniature scale.
    fn write_filter_keys(path: &std::path::Path) {
        // 3 rows; key_suppkey ∈ {0, 5, 10}.
        let key_suppkey: Vec<i64> = vec![0, 5, 10];
        write_table_to_path(
            path,
            &[("key_suppkey", ColumnData::I64(&key_suppkey))],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
    }

    #[tokio::test]
    async fn rule_threads_sideband_through_hashjoin() {
        let li = tmp_parquet("lineitem");
        let sp = tmp_parquet("supplier");
        write_lineitem(&li);
        write_supplier(&sp);

        let cfg = SessionConfig::new().with_target_partitions(4);
        // Disable the selectivity gate (build × ratio < probe) so the
        // rule fires on this miniature test data; default gate is
        // intended for SF=10 TPC-H shapes.
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(cfg)
            .with_physical_optimizer_rule(Arc::new(EnableRuntimeBloomSidebandRule {
                min_probe_to_build_ratio: 0,
            }))
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table(
            "lineitem",
            Arc::new(EmatixFastParquetTableProvider::try_new(li.to_string_lossy())
                .unwrap()),
        )
        .unwrap();
        ctx.register_table(
            "supplier",
            Arc::new(FastParquetTableProvider::try_new(sp.to_string_lossy()).unwrap()),
        )
        .unwrap();

        let df = ctx
            .sql("SELECT l_orderkey FROM supplier JOIN lineitem ON s_suppkey = l_suppkey")
            .await
            .unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let s = format!("{plan:?}");
        // The rule should have injected the wrapper somewhere.
        assert!(
            s.contains("BuildSideBloomEmitterExec"),
            "expected the L9 wrapper in the plan:\n{s}"
        );
        // Output correctness — running the query should produce the
        // expected number of rows. supplier has 25 rows × lineitem
        // matches ~20 rows/key = ~500 rows.
        let batches = df.collect().await.unwrap();
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        // Each of the 25 supplier rows joins with lineitem rows where
        // l_suppkey == s_suppkey. l_suppkey distribution: 1000 rows
        // mod 50 → 20 rows per l_suppkey value. Only s_suppkey in
        // [0,25) participate → 25 × 20 = 500.
        assert_eq!(row_count, 500);
    }

    /// Σ.Q.L9 extension — verify the rule fires on a join whose
    /// probe side is itself a HashJoinExec. Mirrors Q18's RightSemi
    /// shape where the outer join's bloom needs to descend through
    /// an inner join to reach an Emat scan. Before the descent fix
    /// `find_probe_scan_for_column` bailed at 2-child plans, so the
    /// outer join contributed nothing.
    #[tokio::test]
    async fn rule_descends_through_inner_join_to_emat_scan() {
        let li = tmp_parquet("lineitem_outer");
        let sp = tmp_parquet("supplier_outer");
        let fk = tmp_parquet("filter_keys");
        write_lineitem(&li);
        write_supplier(&sp);
        write_filter_keys(&fk);

        let cfg = SessionConfig::new().with_target_partitions(4);
        // Disable the selectivity gate (build × ratio < probe) so the
        // rule fires on this miniature test data; default gate is
        // intended for SF=10 TPC-H shapes.
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(cfg)
            .with_physical_optimizer_rule(Arc::new(EnableRuntimeBloomSidebandRule {
                min_probe_to_build_ratio: 0,
            }))
            .build();
        let ctx = SessionContext::new_with_state(state);
        // Register all three as Emat so L9 can attach a sideband to
        // any of them.
        ctx.register_table(
            "lineitem",
            Arc::new(EmatixFastParquetTableProvider::try_new(li.to_string_lossy()).unwrap()),
        )
        .unwrap();
        ctx.register_table(
            "supplier",
            Arc::new(EmatixFastParquetTableProvider::try_new(sp.to_string_lossy()).unwrap()),
        )
        .unwrap();
        ctx.register_table(
            "filter_keys",
            Arc::new(EmatixFastParquetTableProvider::try_new(fk.to_string_lossy()).unwrap()),
        )
        .unwrap();

        // Subquery-forced shape: outer JOIN whose probe side is an
        // inner (supplier ⋈ lineitem). Outer key references s_suppkey,
        // which lives in the inner join's LEFT input (supplier).
        let sql = "SELECT l_orderkey \
                   FROM filter_keys \
                   JOIN (SELECT s_suppkey, l_orderkey \
                         FROM supplier JOIN lineitem ON s_suppkey = l_suppkey) sub \
                   ON key_suppkey = sub.s_suppkey";
        let df = ctx.sql(sql).await.unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let plan_str = format!("{plan:?}");

        // With the descent fix, L9 should fire on BOTH joins —
        // once on the inner (supplier⋈lineitem) and once on the
        // outer (filter_keys⋈sub) — so two BuildSideBloomEmitterExec
        // wrappers should appear in the plan.
        let n_emitters = plan_str.matches("BuildSideBloomEmitterExec").count();
        assert!(
            n_emitters >= 2,
            "expected ≥2 BuildSideBloomEmitterExec wrappers (one per join) — \
             only found {n_emitters}. The outer join's bloom didn't descend \
             through the inner HashJoinExec to reach an Emat scan. Plan:\n{plan_str}"
        );

        // Output correctness — query must produce the same rows as
        // without the rule. filter_keys ∈ {0,5,10}; for each k,
        // lineitem rows where l_suppkey == k and the join supplier
        // s_suppkey == k. l_suppkey mod 50; rows with l_suppkey == 0
        // = 20 rows (i ∈ {0, 50, 100, ..., 950}), same for 5 and 10.
        // 3 filter keys × 20 lineitem matches = 60.
        let batches = df.collect().await.unwrap();
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(row_count, 60, "wrong row count with L9 descent active");
    }
}
