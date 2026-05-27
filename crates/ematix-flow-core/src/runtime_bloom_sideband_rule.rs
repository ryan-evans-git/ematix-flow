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

/// Install the L9 runtime-sideband rule.
pub fn install_runtime_bloom_sideband_rule(builder: SessionStateBuilder) -> SessionStateBuilder {
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
    /// Σ.Q.L9 / L14 (2026-05-23): Inner-join bloom pushdown was
    /// producing silently-wrong results because the col_idx threaded
    /// from `find_probe_scan_for_column` was a projected-schema index,
    /// but `filter_i64_column_to_bitmap_dense` interprets it as a
    /// file-schema (leaf) index. Q21 row count was wrong (1798 vs
    /// 4009); Q07 sums were 94% wrong (verified against DuckDB). The
    /// col_idx bug is fixed, but Inner-join L9 remains default-off:
    /// the L4'-style "bloom on FK is net-negative" pattern means that
    /// for joins where the build is unfiltered (e.g. full supplier in
    /// Q07's s_suppkey⋈l_suppkey), the bloom passes ~100% by FK
    /// constraint and the membership-test cost exceeds zero savings
    /// (Q07 281→379 ms, Q05 192→244 ms, Q08 197→269 ms). When the
    /// build IS pre-filtered (e.g. post-nation supplier), Inner-L9
    /// becomes a win — opt in via `EMAT_RT_BLOOM_INNER_JOIN=1`.
    pub allow_inner_join: bool,
    /// L9.SelectiveBuild (2026-05-24, Q05 investigation): when true,
    /// gate Inner-join firings on whether the build subtree contains
    /// a `FilterExec`. The bloom-on-FK net-negative pattern
    /// (customer⋈orders, lineitem⋈order_self) only hurts because the
    /// FK side is *unfiltered* — referential integrity guarantees
    /// ~100% bloom pass rate, so the membership probe is pure
    /// overhead. When the build IS filtered (Q17 filtered_part →
    /// lineitem, Q05 orders-post-date-filter → lineitem) the bloom
    /// genuinely drops probe rows. This flag flips the rule from
    /// "always fire Inner joins with allow_inner_join" to "fire Inner
    /// joins ONLY when the build subtree has at least one FilterExec
    /// on the way down". LeftSemi/RightSemi shapes are unaffected —
    /// they're intrinsically selective. Disable via
    /// `EMAT_L9_REQUIRE_FILTERED_BUILD=0` for benchmarking.
    pub require_filtered_build: bool,
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
        // Σ.Q.L14 (2026-05-23): default-OFF for Inner joins even with
        // the col_idx bug fixed. The L4'-style "bloom-on-FK is
        // net-negative" pattern holds whenever the join's build is
        // unfiltered. Opt-in via `EMAT_RT_BLOOM_INNER_JOIN=1` when
        // the build IS pre-filtered.
        let allow_inner_join = std::env::var_os("EMAT_RT_BLOOM_INNER_JOIN").is_some();
        // L9.SelectiveBuild defaults to true. Override via
        // `EMAT_L9_REQUIRE_FILTERED_BUILD=0` for A/B benching.
        let require_filtered_build = std::env::var("EMAT_L9_REQUIRE_FILTERED_BUILD")
            .ok()
            .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
            .unwrap_or(true);
        Self {
            min_probe_to_build_ratio: ratio,
            allow_inner_join,
            require_filtered_build,
        }
    }
}

impl PhysicalOptimizerRule for EnableRuntimeBloomSidebandRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Σ.Q.L9 trace (Q05 investigation, 2026-05-24): when
        // EMAT_L9_TRACE=1 is set, every HashJoinExec visit logs a one-
        // line reason for fire / skip. Helps explain missing wraps
        // (column-type mismatch, no probe scan reachable, gate
        // rejection). No-op when the env var is unset.
        let trace = std::env::var_os("EMAT_L9_TRACE").is_some();
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
            // Σ.Q.L9 / L14 (2026-05-23): Inner-join firings stay
            // available behind `allow_inner_join`. The Σ.Q.L14 fix
            // (file-schema vs projected-schema col_idx mismatch) closes
            // the silent-wrong-sums bug that originally motivated the
            // default-off. Tests opt in via `allow_inner_join: true`.
            if matches!(hj.join_type(), JoinType::Inner) && !self.allow_inner_join {
                if trace {
                    eprintln!("[L9.trace] skip Inner — allow_inner_join=false");
                }
                return Ok(Transformed::no(node));
            }
            // L9.SelectiveBuild (2026-05-24): for Inner joins, fire
            // only if the build subtree contains a FilterExec. The
            // bloom-on-FK net-negative pattern (Q05 c⋈o on unfiltered
            // customer, Q07/Q18 unfiltered-supplier joins) is exactly
            // the "no filter in build" shape — referential integrity
            // makes the bloom pass-through ~100%. LeftSemi/RightSemi
            // are intrinsically selective so the gate doesn't apply
            // to them.
            if matches!(hj.join_type(), JoinType::Inner)
                && self.require_filtered_build
                && !build_subtree_has_filter(hj.left())
            {
                if trace {
                    eprintln!(
                        "[L9.trace] skip Inner — build subtree has no FilterExec (require_filtered_build=true)"
                    );
                }
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
            #[allow(clippy::type_complexity)]
            let mut matched: Option<(
                usize,
                usize,
                Arc<dyn ExecutionPlan>,
                Arc<EmatixFastParquetExec>,
            )> = None;
            for (left_expr, right_expr) in hj.on().iter() {
                let Some(lcol) = left_expr.as_any().downcast_ref::<Column>() else {
                    if trace {
                        eprintln!("[L9.trace] equi-key left not a Column expr");
                    }
                    continue;
                };
                let Some(rcol) = right_expr.as_any().downcast_ref::<Column>() else {
                    if trace {
                        eprintln!("[L9.trace] equi-key right not a Column expr");
                    }
                    continue;
                };
                let l_dt = build.schema().field(lcol.index()).data_type().clone();
                let r_dt = probe.schema().field(rcol.index()).data_type().clone();
                if l_dt != DataType::Int64 {
                    if trace {
                        eprintln!(
                            "[L9.trace] skip — build key {} not Int64 (is {l_dt:?})",
                            lcol.name()
                        );
                    }
                    continue;
                }
                if r_dt != DataType::Int64 {
                    if trace {
                        eprintln!(
                            "[L9.trace] skip — probe key {} not Int64 (is {r_dt:?})",
                            rcol.name()
                        );
                    }
                    continue;
                }
                let col_name = rcol.name().to_string();
                if let Some((scan_node, scan_typed, scan_col_idx)) =
                    find_probe_scan_for_column(&probe, &col_name)
                {
                    if trace {
                        eprintln!(
                            "[L9.trace] matched key {col_name} → probe scan @ col_idx={scan_col_idx}"
                        );
                    }
                    matched = Some((lcol.index(), scan_col_idx, scan_node, scan_typed));
                    break;
                } else if trace {
                    eprintln!(
                        "[L9.trace] probe key {col_name} found no EmatixFastParquetExec on probe side"
                    );
                }
            }

            let Some((build_key_idx, probe_scan_col_idx, scan_node, scan_arc)) = matched
            else {
                if trace {
                    eprintln!(
                        "[L9.trace] {:?} join @ on={:?} — NO MATCH (no Emat scan reachable on probe)",
                        hj.join_type(),
                        hj.on()
                    );
                }
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
            if trace {
                eprintln!(
                    "[L9.trace] {:?} join — build_rows={build_rows:?} probe_rows={probe_rows:?} ratio_gate={}",
                    hj.join_type(),
                    self.min_probe_to_build_ratio
                );
            }
            if self.min_probe_to_build_ratio > 0 {
                if let (Some(b), Some(p)) = (build_rows, probe_rows) {
                    if b.saturating_mul(self.min_probe_to_build_ratio) >= p {
                        if trace {
                            eprintln!(
                                "[L9.trace] skip — gate rejects: b({b}) × ratio({}) >= p({p})",
                                self.min_probe_to_build_ratio
                            );
                        }
                        return Ok(Transformed::no(node));
                    }
                }
            }
            if trace {
                eprintln!(
                    "[L9.trace] WRAP {:?} join — expected_keys={}",
                    hj.join_type(),
                    build_rows.unwrap_or(50_000)
                );
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

            let new_probe = rewrite_probe_subtree(&probe, &scan_node, &sideband)?;

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
) -> Option<(Arc<dyn ExecutionPlan>, Arc<EmatixFastParquetExec>, usize)> {
    if let Some(scan) = plan.as_any().downcast_ref::<EmatixFastParquetExec>() {
        // Σ.Q.L14 (2026-05-23 Q07 sum fix): look up the column name in
        // the FILE schema and return the FILE-schema index, not the
        // projected-schema index. `ColumnPredicate::I64InBloom`'s
        // col_idx is consumed by `filter_i64_column_to_bitmap_dense`
        // which reads `md.row_groups[rg].columns[col]` — that needs
        // the leaf column position in the parquet file, not the
        // position in the projection. The earlier projected-idx code
        // caused the supplier→lineitem bloom on Q07 to apply against
        // l_partkey (file col 1) instead of l_suppkey (file col 2)
        // when the projection was [0,2,5,6,10] — Q07's sums were 94%
        // wrong (verified against DuckDB) silently for months because
        // the bench only checks row counts.
        let file_sch = scan.file_schema();
        if let Some((file_idx, _)) = file_sch
            .fields()
            .iter()
            .enumerate()
            .find(|(_, f)| f.name() == col_name)
        {
            // The column must also be in the projection — otherwise
            // the scan never decodes it and the bloom can't apply.
            if !scan.projection().contains(&file_idx) {
                return None;
            }
            // Σ.Q.L9 (2026-05-23 Q21 fix): return the ORIGINAL Arc node
            // alongside a typed handle. Q21 references lineitem three
            // times (l1, l2, l3) — all the same parquet path. The
            // earlier path-equality rewrite incorrectly attached the
            // bloom sideband to all three, dropping non-Saudi suppliers
            // from the EXISTS subquery (l2) whose semantics require
            // `l2.l_suppkey <> l1.l_suppkey`. Returning the original
            // Arc lets `rewrite_probe_subtree` use Arc::ptr_eq to
            // attach the sideband to ONLY this specific scan instance.
            let fresh = scan.with_added_predicates(Vec::new()).ok()?;
            return Some((Arc::clone(plan), fresh, file_idx));
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
    target_scan_node: &Arc<dyn ExecutionPlan>,
    sideband: &BridgeFilterSideband,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    // Σ.Q.L9 (2026-05-23 Q21 fix): match by Arc identity, not by path.
    // Multiple EmatixFastParquetExec instances over the same parquet
    // are valid in the same plan (TPC-H Q21 has 3 lineitem scans for
    // the l1 / l2 / l3 references) and the sideband must attach only
    // to the specific scan that find_probe_scan_for_column identified
    // as the probe side of THIS HashJoinExec.
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

/// L9.SelectiveBuild (2026-05-24) — does the build subtree contain
/// a `FilterExec`? Used as a heuristic for "the build side is
/// selective", which is necessary for an L9 bloom to pay off on an
/// Inner join.
///
/// Walks down 1-child wrappers (RepartitionExec, ProjectionExec,
/// CoalesceBatchesExec, BuildSideBloomEmitterExec, etc.) and into
/// every child of multi-child plans (HashJoinExec, UnionExec).
/// Returns true on the first `FilterExec` encountered.
///
/// For TPC-H this correctly returns:
/// - false for Q05's customer⋈orders join's build (customer is a
///   raw TableScan), or Q07's supplier⋈lineitem with build=supplier
///   alone
/// - true for Q05's orders_filtered⋈lineitem build (the c⋈o
///   intermediate contains the orders date FilterExec)
/// - true for Q17's filtered_part⋈lineitem build (filtered_part has
///   a FilterExec on p_brand + p_container)
fn build_subtree_has_filter(plan: &Arc<dyn ExecutionPlan>) -> bool {
    use datafusion::physical_plan::filter::FilterExec;
    if plan.as_any().downcast_ref::<FilterExec>().is_some() {
        return true;
    }
    plan.children().iter().any(|c| build_subtree_has_filter(c))
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
///
/// Σ.AH.2 Story 1'.3 (2026-05-26): when `EMAT_L9_TIGHT_CARDINALITY=1`,
/// first try the emat-stats-aware path that uses
/// `EmatixFastParquetExec::column_stats().distinct_count` (populated
/// by Story 1'.2 from parquet dict pages) to compute filter
/// selectivity directly, bypassing DataFusion's `FilterExec.statistics()`
/// which doesn't use distinct_count for string-Eq predicates and
/// falls back to a 0.2 default.
fn estimate_build_rows(plan: &dyn ExecutionPlan) -> Option<usize> {
    if std::env::var_os("EMAT_L9_TIGHT_CARDINALITY").is_some() {
        if let Some(tight) = estimate_build_rows_via_emat_stats(plan) {
            return Some(tight);
        }
    }
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

/// Σ.AH.2 Story 1'.3 — emat-stats-aware build-rows estimate. Returns
/// `Some(rows)` only when the build subtree contains a `FilterExec`
/// sitting (transitively) above exactly one `EmatixFastParquetExec`.
/// Computes selectivity from the predicate directly using emat
/// column_stats (which carry accurate `distinct_count` from dict
/// pages — Story 1'.2). Returns `None` for any other shape so the
/// caller falls back to the standard partition_statistics path.
fn estimate_build_rows_via_emat_stats(plan: &dyn ExecutionPlan) -> Option<usize> {
    let predicate = find_filter_predicate(plan)?;
    let (raw, column_stats) = find_emat_scan_stats(plan)?;
    let sel = estimate_filter_selectivity_via_emat_stats(&predicate, &column_stats);
    Some(((raw as f64 * sel).round() as usize).max(1))
}

/// Walk a plan subtree returning the first `FilterExec`'s predicate.
fn find_filter_predicate(
    plan: &dyn ExecutionPlan,
) -> Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>> {
    use datafusion::physical_plan::filter::FilterExec;
    if let Some(f) = plan.as_any().downcast_ref::<FilterExec>() {
        return Some(f.predicate().clone());
    }
    for c in plan.children() {
        if let Some(p) = find_filter_predicate(c.as_ref()) {
            return Some(p);
        }
    }
    None
}

/// Walk a plan subtree returning the first `EmatixFastParquetExec`'s
/// `(raw_row_count, column_stats)` pair.
fn find_emat_scan_stats(
    plan: &dyn ExecutionPlan,
) -> Option<(
    usize,
    Vec<datafusion::common::stats::ColumnStatistics>,
)> {
    if let Some(s) = plan.as_any().downcast_ref::<EmatixFastParquetExec>() {
        return Some((s.num_rows(), s.column_stats().to_vec()));
    }
    for c in plan.children() {
        if let Some(p) = find_emat_scan_stats(c.as_ref()) {
            return Some(p);
        }
    }
    None
}

/// Σ.AH.2 Story 1'.3 — predicate selectivity using emat column_stats.
/// Mirrors the structure of DataFusion's selectivity calc but uses
/// our populated `distinct_count` for string-Eq predicates instead
/// of falling back to the 0.2 default.
///
/// Handled shapes:
/// - `col = literal` → `1 / distinct_count(col)` when available
/// - `expr AND expr` → product of children's selectivity
/// - `expr OR expr`  → conservative max of children's selectivity
/// - anything else   → 0.2 (same conservative default as DataFusion)
fn estimate_filter_selectivity_via_emat_stats(
    predicate: &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    column_stats: &[datafusion::common::stats::ColumnStatistics],
) -> f64 {
    use datafusion::common::stats::Precision;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Literal};
    if let Some(bin) = predicate.as_any().downcast_ref::<BinaryExpr>() {
        match bin.op() {
            Operator::And => {
                let l = estimate_filter_selectivity_via_emat_stats(bin.left(), column_stats);
                let r = estimate_filter_selectivity_via_emat_stats(bin.right(), column_stats);
                return (l * r).clamp(0.0, 1.0);
            }
            Operator::Or => {
                let l = estimate_filter_selectivity_via_emat_stats(bin.left(), column_stats);
                let r = estimate_filter_selectivity_via_emat_stats(bin.right(), column_stats);
                return l.max(r).clamp(0.0, 1.0);
            }
            Operator::Eq => {
                if let Some(col) = bin.left().as_any().downcast_ref::<Column>() {
                    if bin.right().as_any().downcast_ref::<Literal>().is_some() {
                        if let Some(cs) = column_stats.get(col.index()) {
                            let n = match cs.distinct_count {
                                Precision::Exact(n) | Precision::Inexact(n) => n,
                                _ => 0,
                            };
                            if n > 0 {
                                return 1.0 / n as f64;
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
    0.2
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
        let dir =
            std::env::temp_dir().join(format!("l9_rule_test_{}_{}", std::process::id(), name));
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
                allow_inner_join: true,
                // Tests use miniature in-memory parquet with no
                // FilterExec in the build path — they're verifying
                // the wrap mechanism, not the selectivity heuristic.
                require_filtered_build: false,
            }))
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table(
            "lineitem",
            Arc::new(EmatixFastParquetTableProvider::try_new(li.to_string_lossy()).unwrap()),
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
                allow_inner_join: true,
                // Tests use miniature in-memory parquet with no
                // FilterExec in the build path — they're verifying
                // the wrap mechanism, not the selectivity heuristic.
                require_filtered_build: false,
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
