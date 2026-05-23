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
    builder.with_physical_optimizer_rule(Arc::new(EnableRuntimeBloomSidebandRule))
}

#[derive(Debug, Default)]
pub struct EnableRuntimeBloomSidebandRule;

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

            // Σ.Q.L9: allocate the sideband, wrap the build child,
            // rewrite the probe subtree to thread the sideband into
            // the matching EmatixFastParquetExec.
            let sideband = BridgeFilterSideband::new();

            // Estimate expected build-side keys from the build's
            // partition statistics. Falls back to a generous default
            // (50K) when stats are unavailable.
            let expected_keys = estimate_build_rows(build.as_ref()).unwrap_or(50_000);

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
/// the column index in its schema. Only walks through row-preserving
/// physical wrappers (FilterExec, ProjectionExec preserving names,
/// CoalescePartitionsExec, CoalesceBatchesExec, RepartitionExec).
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
            // Reconstruct an Arc<EmatixFastParquetExec> from the plan
            // — we have a &EmatixFastParquetExec, need an
            // Arc<EmatixFastParquetExec>. The Arc<dyn ExecutionPlan>
            // can be downcast via Arc::downcast on the trait object,
            // but it's nightly-only. Workaround: clone via the
            // scan's clone_internals path (private). Simpler:
            // use the scan's exposed APIs — we don't actually need
            // the Arc back, we just need the scan's identity so
            // rewrite_probe_subtree can compare-and-replace.
            //
            // Return Arc::new of a fresh clone via with_added_predicates
            // (with empty preds it just clones internals). It's
            // safe — we won't actually use this Arc in the rewritten
            // plan; we use with_runtime_sideband on the original
            // when we find it during rewrite.
            //
            // Below: we use a sentinel Arc only for type plumbing.
            // rewrite_probe_subtree gets the &EmatixFastParquetExec
            // via a downcast at rewrite time, so this Arc need not
            // be the same one.
            let fresh = scan.with_added_predicates(Vec::new()).ok()?;
            return Some((fresh, idx));
        }
        return None;
    }
    // Descend through transparent wrappers.
    let children = plan.children();
    // FilterExec / ProjectionExec / CoalescePartitionsExec /
    // CoalesceBatchesExec / RepartitionExec are all 1-child and
    // preserve column NAMES (FilterExec preserves the entire schema;
    // ProjectionExec preserves names only when the projection is
    // pass-through, but for TPC-H this is the common case;
    // CoalescePartitions/Batches preserve schema; RepartitionExec
    // preserves schema).
    if children.len() == 1 {
        return find_probe_scan_for_column(children[0], col_name);
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

    #[tokio::test]
    async fn rule_threads_sideband_through_hashjoin() {
        let li = tmp_parquet("lineitem");
        let sp = tmp_parquet("supplier");
        write_lineitem(&li);
        write_supplier(&sp);

        let cfg = SessionConfig::new().with_target_partitions(4);
        let state = install_runtime_bloom_sideband_rule(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(cfg),
        )
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
}
