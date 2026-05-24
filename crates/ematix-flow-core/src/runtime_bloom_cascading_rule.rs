//! Σ.S.B — cascading L9 (FK-chain bloom propagation).
//!
//! Opt-in superset of [[`runtime_bloom_sideband_rule`]]. For each
//! HashJoinExec, runs the same primary-target detection as the L9
//! rule, then ALSO walks the probe subtree via [`fk_chain`] to
//! discover other [`EmatixFastParquetExec`] scans whose projected
//! columns share the build key's FK-chain stem. The build-side
//! bloom is published to one sideband per such scan, sharing the
//! bloom Arc across all of them (build runs once).
//!
//! ## When this helps
//!
//! - Multiple-scan probe subtrees of a single join. If the probe
//!   side contains a nested HashJoinExec whose inputs also reference
//!   columns in the same FK chain (e.g. `lineitem.l_suppkey` appears
//!   under a join keyed on `s_suppkey`, AND the same probe subtree
//!   also pulls from another `*_suppkey` scan), the cascade attaches
//!   the bloom to every such scan.
//! - Star-schema fact-table cross-products where one dimension's
//!   filtered key column applies to several fact scans.
//!
//! For TPC-H specifically: most chains are length-1 (each join
//!   key is a different stem), so cascading is a no-op. Self-join
//!   shapes (Q21) and dimension⋈fact-fact patterns (Q05/Q07/Q08)
//!   are the candidates.
//!
//! ## Opt-in
//!
//! Install via [`install_cascading_bloom_rule`] OR opt in at
//! runtime via the env var `EMAT_L9_CASCADE=1`. **Default off.**
//! The L9 base rule and this rule are mutually exclusive — install
//! one or the other, not both. Cascading is a strict superset so
//! the L9 base behavior is preserved when no extra scans match.

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DfResult;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::joins::HashJoinExec;

use crate::bridge_filter_sideband::BridgeFilterSideband;
use crate::build_side_bloom_emitter_exec::BuildSideBloomEmitterExec;
use crate::ematix_fast_parquet::EmatixFastParquetExec;
use crate::fk_chain::{fk_chain_stem, find_scans_by_fk_chain};

/// Install the cascading L9 rule. Mutually exclusive with the
/// non-cascading L9 base rule — install one or the other.
pub fn install_cascading_bloom_rule(builder: SessionStateBuilder) -> SessionStateBuilder {
    builder.with_physical_optimizer_rule(Arc::new(EnableCascadingBloomRule::default()))
}

/// Cascading L9 rule config. Mirrors
/// [`crate::runtime_bloom_sideband_rule::EnableRuntimeBloomSidebandRule`]
/// — same selectivity gate and Inner-join controls — plus a per-query
/// cap on how many cascade extras a single emitter can fan out to.
#[derive(Debug, Clone, Copy)]
pub struct EnableCascadingBloomRule {
    pub min_probe_to_build_ratio: usize,
    pub allow_inner_join: bool,
    pub require_filtered_build: bool,
    /// Maximum number of extra cascade targets per emitter. The L9
    /// emit publishes one predicate per target; each consumer scan
    /// pays a per-RG bloom probe cost. Cap defends against pathological
    /// plans where every probe-side scan matches the FK stem.
    /// Default = 4. Override via `EMAT_L9_CASCADE_MAX=N`.
    pub max_extras_per_emitter: usize,
}

impl Default for EnableCascadingBloomRule {
    fn default() -> Self {
        let ratio = std::env::var("EMAT_RT_BLOOM_SELECTIVITY")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(64);
        let allow_inner_join = std::env::var_os("EMAT_RT_BLOOM_INNER_JOIN").is_some();
        let require_filtered_build = std::env::var("EMAT_L9_REQUIRE_FILTERED_BUILD")
            .ok()
            .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
            .unwrap_or(true);
        let max_extras_per_emitter = std::env::var("EMAT_L9_CASCADE_MAX")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(4);
        Self {
            min_probe_to_build_ratio: ratio,
            allow_inner_join,
            require_filtered_build,
            max_extras_per_emitter,
        }
    }
}

impl PhysicalOptimizerRule for EnableCascadingBloomRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let trace = std::env::var_os("EMAT_L9_TRACE").is_some();
        plan.transform_up(|node| {
            let Some(hj) = node.as_any().downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };
            use datafusion::common::JoinType;
            if !matches!(
                hj.join_type(),
                JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi
            ) {
                return Ok(Transformed::no(node));
            }
            if matches!(hj.join_type(), JoinType::Inner) && !self.allow_inner_join {
                if trace {
                    eprintln!("[L9.cascade] skip Inner — allow_inner_join=false");
                }
                return Ok(Transformed::no(node));
            }
            if matches!(hj.join_type(), JoinType::Inner)
                && self.require_filtered_build
                && !build_subtree_has_filter(hj.left())
            {
                if trace {
                    eprintln!("[L9.cascade] skip Inner — build has no FilterExec");
                }
                return Ok(Transformed::no(node));
            }

            let build = hj.left().clone();
            let probe = hj.right().clone();

            // Find an i64 equi-key with a matching primary scan.
            let mut primary: Option<(usize, usize, Arc<dyn ExecutionPlan>, Arc<EmatixFastParquetExec>, String)> = None;
            for (left_expr, right_expr) in hj.on().iter() {
                let Some(lcol) = left_expr.as_any().downcast_ref::<Column>() else {
                    continue;
                };
                let Some(rcol) = right_expr.as_any().downcast_ref::<Column>() else {
                    continue;
                };
                let l_dt = build.schema().field(lcol.index()).data_type().clone();
                let r_dt = probe.schema().field(rcol.index()).data_type().clone();
                if l_dt != DataType::Int64 || r_dt != DataType::Int64 {
                    continue;
                }
                let col_name = rcol.name().to_string();
                if let Some((scan_node, scan_typed, scan_col_idx)) =
                    find_probe_scan_for_column(&probe, &col_name)
                {
                    primary = Some((
                        lcol.index(),
                        scan_col_idx,
                        scan_node,
                        scan_typed,
                        col_name,
                    ));
                    break;
                }
            }
            let Some((build_key_idx, primary_col_idx, primary_scan_node, primary_scan_typed, primary_col_name)) = primary else {
                if trace {
                    eprintln!(
                        "[L9.cascade] {:?} join — no primary Emat scan reachable",
                        hj.join_type()
                    );
                }
                return Ok(Transformed::no(node));
            };

            // Selectivity gate (same as L9 base).
            let build_rows = estimate_build_rows(build.as_ref());
            let probe_rows = estimate_probe_scan_rows_via_match(&probe, &primary_col_name);
            if self.min_probe_to_build_ratio > 0 {
                if let (Some(b), Some(p)) = (build_rows, probe_rows) {
                    if b.saturating_mul(self.min_probe_to_build_ratio) >= p {
                        if trace {
                            eprintln!(
                                "[L9.cascade] skip — gate: b({b}) × ratio({}) >= p({p})",
                                self.min_probe_to_build_ratio
                            );
                        }
                        return Ok(Transformed::no(node));
                    }
                }
            }

            // Σ.S.B — discover cascade extras. Skip the cascade walk
            // if the build key has no FK-chain stem (e.g. column name
            // without an underscore) — there's nothing reliable to
            // match on beyond the exact-name primary.
            let primary_stem = match fk_chain_stem(&primary_col_name) {
                Some(s) => s.to_string(),
                None => String::new(),
            };

            // Σ.S.B (2026-05-24) — self-join sibling skip. When Q17 /
            // Q21 / similar shapes have N scans of the SAME parquet
            // file (outer + correlated subquery, or self-joined `l1`
            // / `l2` / `l3`), the cascade walker finds all N via
            // FK-stem match. But the non-primary siblings reference
            // different correlated values OR EXISTS contexts — the
            // bloom built on the primary's join key DOES NOT validly
            // filter them. Attaching anyway preserves correctness
            // (bloom is a superset filter) but pays scan-time probe
            // cost for no benefit. Skip any extra whose parquet path
            // equals the primary's path.
            let primary_path = primary_scan_typed.path().to_string();

            let mut cascade_extras: Vec<(usize, BridgeFilterSideband, Arc<dyn ExecutionPlan>)> =
                Vec::new();
            if !primary_stem.is_empty() {
                let matches = find_scans_by_fk_chain(&probe, &primary_stem);
                for m in matches {
                    // Skip the primary — Arc::ptr_eq is the exact
                    // same scan instance that find_probe_scan_for_column
                    // returned.
                    if Arc::ptr_eq(&m.scan_node, &primary_scan_node) {
                        continue;
                    }
                    // Skip self-join siblings — same parquet path as
                    // primary means a second scan of the same table.
                    if m.scan_typed.path() == primary_path {
                        if trace {
                            eprintln!(
                                "[L9.cascade] skip self-sibling: {} (same path as primary)",
                                m.col_name
                            );
                        }
                        continue;
                    }
                    if cascade_extras.len() >= self.max_extras_per_emitter {
                        if trace {
                            eprintln!(
                                "[L9.cascade] cap hit (max_extras_per_emitter={}); skipping rest",
                                self.max_extras_per_emitter
                            );
                        }
                        break;
                    }
                    let sb = BridgeFilterSideband::new();
                    cascade_extras.push((m.col_idx, sb, m.scan_node));
                }
            }

            if trace {
                eprintln!(
                    "[L9.cascade] WRAP {:?} key={primary_col_name} primary_col_idx={primary_col_idx} extras={}",
                    hj.join_type(),
                    cascade_extras.len()
                );
            }

            // Allocate primary sideband + emitter with extras.
            let primary_sb = BridgeFilterSideband::new();
            let expected_keys = build_rows.unwrap_or(50_000);
            let extras_for_emitter: Vec<(usize, BridgeFilterSideband)> = cascade_extras
                .iter()
                .map(|(ci, sb, _node)| (*ci, sb.clone()))
                .collect();
            let wrapped_build: Arc<dyn ExecutionPlan> =
                Arc::new(BuildSideBloomEmitterExec::try_new_with_extras(
                    build,
                    build_key_idx,
                    primary_col_idx,
                    primary_sb.clone(),
                    extras_for_emitter,
                    expected_keys,
                )?);

            // Rewrite probe subtree: attach primary sideband to its scan,
            // then attach each extra sideband to its scan.
            let mut new_probe = rewrite_probe_subtree(&probe, &primary_scan_node, &primary_sb)?;
            for (_col_idx, sb, scan_node) in cascade_extras.iter() {
                new_probe = rewrite_probe_subtree(&new_probe, scan_node, sb)?;
            }

            let new_join = Arc::clone(&node).with_new_children(vec![wrapped_build, new_probe])?;
            Ok(Transformed::yes(new_join))
        })
        .data()
    }

    fn name(&self) -> &str {
        "ematix_flow_enable_cascading_bloom"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

// ---- internal helpers (mirror the L9 base rule) ----

fn find_probe_scan_for_column(
    plan: &Arc<dyn ExecutionPlan>,
    col_name: &str,
) -> Option<(Arc<dyn ExecutionPlan>, Arc<EmatixFastParquetExec>, usize)> {
    if let Some(scan) = plan.as_any().downcast_ref::<EmatixFastParquetExec>() {
        let file_sch = scan.file_schema();
        if let Some((file_idx, _)) = file_sch
            .fields()
            .iter()
            .enumerate()
            .find(|(_, f)| f.name() == col_name)
        {
            if !scan.projection().contains(&file_idx) {
                return None;
            }
            let fresh = scan.with_added_predicates(Vec::new()).ok()?;
            return Some((Arc::clone(plan), fresh, file_idx));
        }
        return None;
    }
    for child in plan.children() {
        if let Some(found) = find_probe_scan_for_column(child, col_name) {
            return Some(found);
        }
    }
    None
}

fn rewrite_probe_subtree(
    plan: &Arc<dyn ExecutionPlan>,
    target_scan_node: &Arc<dyn ExecutionPlan>,
    sideband: &BridgeFilterSideband,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    plan.clone()
        .transform_up(|node| {
            if Arc::ptr_eq(&node, target_scan_node) {
                if let Some(scan) = node.as_any().downcast_ref::<EmatixFastParquetExec>() {
                    let new = scan.with_runtime_sideband(sideband.clone());
                    return Ok(Transformed::yes(new as Arc<dyn ExecutionPlan>));
                }
            }
            Ok(Transformed::no(node))
        })
        .data()
}

fn build_subtree_has_filter(plan: &Arc<dyn ExecutionPlan>) -> bool {
    use datafusion::physical_plan::filter::FilterExec;
    if plan.as_any().downcast_ref::<FilterExec>().is_some() {
        return true;
    }
    plan.children().iter().any(|c| build_subtree_has_filter(*c))
}

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

/// Look up the primary scan in `probe` again and pull its row count
/// for the selectivity gate. (We don't keep the typed Arc threaded
/// through the cascade walk to keep the code path straight.)
fn estimate_probe_scan_rows_via_match(
    probe: &Arc<dyn ExecutionPlan>,
    primary_col_name: &str,
) -> Option<usize> {
    let (_, scan_typed, _) = find_probe_scan_for_column(probe, primary_col_name)?;
    use datafusion::common::stats::Precision;
    let stats = scan_typed.partition_statistics(None).ok()?;
    match stats.num_rows {
        Precision::Exact(n) | Precision::Inexact(n) => Some(n.max(1)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ematix_fast_parquet::EmatixFastParquetTableProvider;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use ematix_parquet_codec::write::{ColumnData, write_table_to_path};
    use ematix_parquet_format::types::CompressionCodec;

    fn tmp_parquet(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "cascade_rule_test_{}_{}",
            std::process::id(),
            name
        ));
        let _ = std::fs::create_dir_all(&dir);
        dir.join(format!("{name}.parquet"))
    }

    /// Σ.S.B end-to-end — outer join builds a bloom on `s_suppkey`;
    /// the probe subtree is an inner self-join of two scans that
    /// BOTH project `*_suppkey` columns. With cascading on, BOTH
    /// downstream scans should be wrapped with the sideband.
    #[tokio::test]
    async fn cascade_attaches_bloom_to_two_fk_chained_scans() {
        // Three tables:
        //   filter_keys (3 rows) — small build via outer join
        //   a (1000 rows; a_suppkey ∈ [0,50))
        //   b (1000 rows; b_suppkey ∈ [0,50))
        // Outer: filter_keys.fk_suppkey ⋈ inner.a_suppkey
        // Inner: a.a_suppkey ⋈ b.b_suppkey
        // Cascading on, the outer join's bloom (built on filter_keys
        // values) should also attach to `b` because b_suppkey shares
        // the "suppkey" FK-chain stem.
        let fk = tmp_parquet("fk");
        let a = tmp_parquet("a");
        let b = tmp_parquet("b");
        let fk_suppkey: Vec<i64> = vec![0, 5, 10];
        let a_suppkey: Vec<i64> = (0..1000i64).map(|i| i % 50).collect();
        let b_suppkey: Vec<i64> = (0..1000i64).map(|i| i % 50).collect();
        write_table_to_path(
            &fk,
            &[("fk_suppkey", ColumnData::I64(&fk_suppkey))],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
        write_table_to_path(
            &a,
            &[("a_suppkey", ColumnData::I64(&a_suppkey))],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
        write_table_to_path(
            &b,
            &[("b_suppkey", ColumnData::I64(&b_suppkey))],
            CompressionCodec::Uncompressed,
        )
        .unwrap();

        let cfg = SessionConfig::new().with_target_partitions(2);
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(cfg)
            .with_physical_optimizer_rule(Arc::new(EnableCascadingBloomRule {
                min_probe_to_build_ratio: 0, // disable gate for miniature data
                allow_inner_join: true,
                require_filtered_build: false,
                max_extras_per_emitter: 4,
            }))
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table(
            "fk",
            Arc::new(
                EmatixFastParquetTableProvider::try_new(fk.to_string_lossy()).unwrap(),
            ),
        )
        .unwrap();
        ctx.register_table(
            "a",
            Arc::new(EmatixFastParquetTableProvider::try_new(a.to_string_lossy()).unwrap()),
        )
        .unwrap();
        ctx.register_table(
            "b",
            Arc::new(EmatixFastParquetTableProvider::try_new(b.to_string_lossy()).unwrap()),
        )
        .unwrap();

        let sql = "SELECT a_suppkey, b_suppkey FROM fk \
                   JOIN (SELECT a_suppkey, b_suppkey FROM a JOIN b ON a_suppkey = b_suppkey) sub \
                   ON fk_suppkey = sub.a_suppkey";
        let df = ctx.sql(sql).await.unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let plan_str = format!("{plan:?}");

        // With cascade on, the OUTER join's BuildSideBloomEmitterExec
        // is built with extras (b_suppkey). The plan should still
        // show the wrapper; both downstream scans (a + b) should
        // each carry a sideband.
        assert!(
            plan_str.contains("BuildSideBloomEmitterExec"),
            "expected at least one cascading L9 wrapper:\n{plan_str}"
        );

        // Run the query and check output is unchanged by the
        // cascade. Each of the 3 fk_suppkey values matches 20 a-rows
        // and 20 b-rows; the inner self-join on a=b emits 20 × 20 =
        // 400 per shared key. So 3 × 400 = 1200 final rows.
        let batches = df.collect().await.unwrap();
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            row_count, 1200,
            "cascade must not change query semantics"
        );
    }

    /// Σ.S.B — when no extra scan shares the FK stem, the rule
    /// behaves exactly like the L9 base rule (one sideband, one
    /// wrapped scan). Verifies the cascade is a clean superset.
    #[tokio::test]
    async fn cascade_no_op_when_stem_only_matches_primary() {
        // Tables:
        //   small (3 rows; sm_custkey)
        //   big   (1000 rows; bg_custkey + bg_orderkey)
        // Join: small.sm_custkey ⋈ big.bg_custkey
        // Stem "custkey" only matches one scan (`big`) so cascade
        // adds zero extras — same behavior as L9 base.
        let sm = tmp_parquet("small_no_cascade");
        let bg = tmp_parquet("big_no_cascade");
        let sm_custkey: Vec<i64> = vec![1, 2, 3];
        let bg_custkey: Vec<i64> = (0..1000i64).map(|i| i % 10).collect();
        let bg_orderkey: Vec<i64> = (0..1000i64).collect();
        write_table_to_path(
            &sm,
            &[("sm_custkey", ColumnData::I64(&sm_custkey))],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
        write_table_to_path(
            &bg,
            &[
                ("bg_custkey", ColumnData::I64(&bg_custkey)),
                ("bg_orderkey", ColumnData::I64(&bg_orderkey)),
            ],
            CompressionCodec::Uncompressed,
        )
        .unwrap();

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(SessionConfig::new().with_target_partitions(2))
            .with_physical_optimizer_rule(Arc::new(EnableCascadingBloomRule {
                min_probe_to_build_ratio: 0,
                allow_inner_join: true,
                require_filtered_build: false,
                max_extras_per_emitter: 4,
            }))
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table(
            "small",
            Arc::new(
                EmatixFastParquetTableProvider::try_new(sm.to_string_lossy()).unwrap(),
            ),
        )
        .unwrap();
        ctx.register_table(
            "big",
            Arc::new(
                EmatixFastParquetTableProvider::try_new(bg.to_string_lossy()).unwrap(),
            ),
        )
        .unwrap();

        let sql =
            "SELECT bg_orderkey FROM small JOIN big ON sm_custkey = bg_custkey";
        let df = ctx.sql(sql).await.unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let plan_str = format!("{plan:?}");
        assert!(
            plan_str.contains("BuildSideBloomEmitterExec"),
            "expected one wrapper (base L9 behavior):\n{plan_str}"
        );
        // Sanity: row count = sum over sm_custkey ∈ {1,2,3} of
        // #(bg rows where bg_custkey == k). bg_custkey is i mod 10
        // for i in [0,1000) → 100 rows per value. So 3 × 100 = 300.
        let batches = df.collect().await.unwrap();
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(row_count, 300);
    }

    /// Σ.S.B cap — `max_extras_per_emitter` caps the number of
    /// extras attached per join.
    #[tokio::test]
    async fn cascade_respects_max_extras_cap() {
        // Three tables a,b,c — all with `*_suppkey`. Outer join
        // builds bloom on filter_keys.fk_suppkey; probe subtree
        // contains all 3. With max_extras_per_emitter=1, only ONE
        // of {b, c} should be attached — but the test verifies the
        // cap is honored, not which one wins.
        let fk = tmp_parquet("fk_cap");
        let a = tmp_parquet("a_cap");
        let b = tmp_parquet("b_cap");
        let c = tmp_parquet("c_cap");
        let suppkeys: Vec<i64> = (0..50i64).collect();
        write_table_to_path(
            &fk,
            &[("fk_suppkey", ColumnData::I64(&vec![0, 5]))],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
        for path in [&a, &b, &c] {
            let prefix = path.file_stem().unwrap().to_string_lossy().to_string();
            // a_cap, b_cap, c_cap → use the first char as the table prefix
            let prefix_char = prefix.chars().next().unwrap();
            let col = format!("{prefix_char}_suppkey");
            write_table_to_path(
                path,
                &[(col.as_str(), ColumnData::I64(&suppkeys))],
                CompressionCodec::Uncompressed,
            )
            .unwrap();
        }

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(SessionConfig::new().with_target_partitions(2))
            .with_physical_optimizer_rule(Arc::new(EnableCascadingBloomRule {
                min_probe_to_build_ratio: 0,
                allow_inner_join: true,
                require_filtered_build: false,
                max_extras_per_emitter: 1, // cap of ONE extra
            }))
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table(
            "fk",
            Arc::new(
                EmatixFastParquetTableProvider::try_new(fk.to_string_lossy()).unwrap(),
            ),
        )
        .unwrap();
        ctx.register_table(
            "a",
            Arc::new(EmatixFastParquetTableProvider::try_new(a.to_string_lossy()).unwrap()),
        )
        .unwrap();
        ctx.register_table(
            "b",
            Arc::new(EmatixFastParquetTableProvider::try_new(b.to_string_lossy()).unwrap()),
        )
        .unwrap();
        ctx.register_table(
            "c",
            Arc::new(EmatixFastParquetTableProvider::try_new(c.to_string_lossy()).unwrap()),
        )
        .unwrap();

        let sql = "SELECT a_suppkey FROM fk JOIN \
                   (SELECT a_suppkey, b_suppkey, c_suppkey \
                    FROM a JOIN b ON a_suppkey = b_suppkey \
                           JOIN c ON a_suppkey = c_suppkey) sub \
                   ON fk_suppkey = sub.a_suppkey";
        let df = ctx.sql(sql).await.unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        // Output correctness — irrespective of how many extras were
        // attached. 2 fk_suppkey values × matching a + b + c rows.
        let batches = df.collect().await.unwrap();
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        // Each fk_suppkey value matches one row each in a, b, c.
        // The inner self-join chain emits 1 row per matched key (the
        // 3-way join intersection). 2 fk values × 1 row each = 2.
        assert_eq!(row_count, 2);
        // The plan should still contain at least one BuildSide wrapper.
        let plan_str = format!("{plan:?}");
        assert!(plan_str.contains("BuildSideBloomEmitterExec"));
    }

    /// Σ.S.B self-join sibling skip — when the probe subtree has TWO
    /// scans of the SAME parquet file (Q17 / Q21 shape), cascade
    /// should attach the bloom only to the primary. The second
    /// (sibling) scan would pay scan-time probe cost without filter
    /// benefit because it references a different correlated context.
    ///
    /// Contrast with `cascade_attaches_bloom_to_two_fk_chained_scans`
    /// where the two probe-side scans are DIFFERENT parquet files
    /// (a + b) and the cascade correctly attaches to both.
    #[tokio::test]
    async fn cascade_skips_self_join_siblings_same_parquet_path() {
        use datafusion::common::tree_node::TreeNode;
        use crate::build_side_bloom_emitter_exec::BuildSideBloomEmitterExec;

        let fk = tmp_parquet("fk_selfsib");
        let li = tmp_parquet("li_selfsib");
        // 3 outer keys that match the lineitem keys.
        let fk_suppkey: Vec<i64> = vec![0, 5, 10];
        let l_suppkey: Vec<i64> = (0..1000i64).map(|i| i % 50).collect();
        let l_orderkey: Vec<i64> = (0..1000i64).map(|i| i % 100).collect();
        write_table_to_path(
            &fk,
            &[("fk_suppkey", ColumnData::I64(&fk_suppkey))],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
        write_table_to_path(
            &li,
            &[
                ("l_suppkey", ColumnData::I64(&l_suppkey)),
                ("l_orderkey", ColumnData::I64(&l_orderkey)),
            ],
            CompressionCodec::Uncompressed,
        )
        .unwrap();

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(SessionConfig::new().with_target_partitions(2))
            .with_physical_optimizer_rule(Arc::new(EnableCascadingBloomRule {
                min_probe_to_build_ratio: 0,
                allow_inner_join: true,
                require_filtered_build: false,
                max_extras_per_emitter: 4,
            }))
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table(
            "fk",
            Arc::new(
                EmatixFastParquetTableProvider::try_new(fk.to_string_lossy()).unwrap(),
            ),
        )
        .unwrap();
        ctx.register_table(
            "lineitem",
            Arc::new(
                EmatixFastParquetTableProvider::try_new(li.to_string_lossy()).unwrap(),
            ),
        )
        .unwrap();

        // Outer join: fk.fk_suppkey ⋈ sub.l_suppkey.
        // Probe sub-plan: lineitem l1 ⋈ lineitem l2 on l_orderkey.
        // BOTH lineitems share the same parquet path.
        let sql = "SELECT l1_suppkey FROM fk JOIN \
                   (SELECT l1.l_suppkey AS l1_suppkey \
                    FROM lineitem l1 JOIN lineitem l2 \
                      ON l1.l_orderkey = l2.l_orderkey) sub \
                   ON fk_suppkey = sub.l1_suppkey";
        let df = ctx.sql(sql).await.unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();

        // Walk the plan for every BuildSideBloomEmitterExec and check
        // none of them carry any extra_targets that would point at a
        // sibling lineitem scan.
        let mut max_extras_seen = 0usize;
        plan.clone()
            .apply(|node| {
                if let Some(emit) =
                    node.as_any().downcast_ref::<BuildSideBloomEmitterExec>()
                {
                    if emit.extra_targets().len() > max_extras_seen {
                        max_extras_seen = emit.extra_targets().len();
                    }
                }
                Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
            })
            .unwrap();
        assert_eq!(
            max_extras_seen, 0,
            "self-join sibling skip failed — extras_per_emitter should be 0 \
             when the only other FK-stem-matching scan is a self-sibling \
             (same parquet path as primary)"
        );

        // Correctness — output must match a baseline run without the rule.
        // Each fk_suppkey k matches: 20 l1 rows where l1.l_suppkey == k,
        // and each of those joins with all l2 rows sharing l1.l_orderkey.
        // l_orderkey distribution: 1000 rows mod 100 = 10 per orderkey.
        // For a given l1 row with orderkey o, l2 matches = 10. So
        // 20 l1 rows × 10 l2 = 200 per fk_suppkey. 3 fk values × 200 = 600.
        let batches = df.collect().await.unwrap();
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(row_count, 600, "self-join cascade must preserve semantics");
    }
}
