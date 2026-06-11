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
use crate::build_side_bloom_emitter_exec::{
    BuildSideBloomEmitterExec, is_string_key, widens_to_i64,
};
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
    /// Σ.AH.3 Story 2a (2026-05-27): absolute per-partition build-size
    /// ceiling. Refuses bloom emit when the estimated per-partition
    /// build key count exceeds this threshold, regardless of the
    /// build/probe ratio. Motivated by the AH.3 reorder spike showing
    /// L9 over-fires on non-selective FK columns (Q07 post-reorder
    /// fires emits at 3571 / 3571 / 50000 expected_keys_per_partition,
    /// adding ~+45 ms wall) and by the AH.2 closure pattern where
    /// baseline Q08 fires L9 with build=400k → +55 ms regression that
    /// fused-probe only partially mitigates.
    ///
    /// The existing `min_probe_to_build_ratio` gate only rejects
    /// fact⋈fact joins (build ≈ probe / 4); it permits a 938K build
    /// on a 60M probe. At ~1.4 ns/probe × 60M rows, even a 50K bloom
    /// adds ~84 ms / partition of probe cost.
    ///
    /// Default 25_000 per partition. Catches the AH.3 reorder bad
    /// fires + AH.2 baseline Q08 default-mode 400k fire while keeping
    /// the Q08-tight-cardinality 13k fire (mitigated by fused-probe).
    /// 0 disables the gate. Override via `EMAT_L9_MAX_EXPECTED_KEYS=N`.
    pub max_expected_keys_per_partition: usize,
    /// L9.WIDTH (2026-06-10) — probe-payoff guard for the OPT-IN
    /// tight-cardinality mode: wrap only when the probe-side scan projects
    /// at least this many columns. **Default 0 = disabled.** Bisecting a
    /// Q07 +125% / Q21 +45% regression showed the pre-existing default
    /// fires (Q07's two nation-IN blooms on 2-col supplier/customer
    /// probes, Σ.Y) are large WINNERS — projected width does NOT separate
    /// winning from losing wraps in general. The gate exists to pair with
    /// `EMAT_L9_TIGHT_CARDINALITY=1`, whose NDV-tightened estimates admit
    /// Inner-join wraps of unpredictable payoff (Q08 −30ms WIN at 5 cols;
    /// Q17 +25ms LOSS at 2-3 cols; Q02 +11ms LOSS at 5 cols — width
    /// blocks Q17's losers only). Override via
    /// `EMAT_L9_MIN_PROBE_PROJ_COLS=N`.
    pub min_probe_proj_cols: usize,
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
        // Σ.AH.3 Story 2a: per-partition absolute ceiling on build size.
        // OPT-IN — default 0 (disabled). The Story 2a bench measured no
        // baseline wall-time improvement (existing `require_filtered_build`
        // + `min_probe_to_build_ratio` already screen out the FK-bloom
        // pattern; my gate only sees Inner-join emits with pre-filtered
        // builds, which are net-positive per Q07 reorder measurement).
        // Lever remains opt-in via `EMAT_L9_MAX_EXPECTED_KEYS=N` for
        // hypothetical future shape regressions where the existing gates
        // are insufficient.
        let max_expected_keys_per_partition = std::env::var("EMAT_L9_MAX_EXPECTED_KEYS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        // L9.WIDTH — see the field docs; env override for tuning.
        let min_probe_proj_cols = std::env::var("EMAT_L9_MIN_PROBE_PROJ_COLS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        Self {
            min_probe_to_build_ratio: ratio,
            allow_inner_join,
            require_filtered_build,
            max_expected_keys_per_partition,
            min_probe_proj_cols,
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
            // Σ.AM.1 (2026-05-27, banked as opt-in): allow Inner L9
            // fire when the build subtree contains a LeftSemi/LeftAnti.
            //
            // Q18 SF=10 bench showed this delivers no incremental win:
            // L9 already fires on the inner customer⋈orders join and
            // emits a bloom that pre-filters lineitem to 4.37K rows
            // before reaching the top Inner HJ. Σ.AM.1's additional
            // bloom on the outer HJ is redundant (lineitem only attaches
            // one sideband) and adds ~15ms cumulative build_time per
            // partition. Net Q18 wall: +1.50 ms (noise).
            //
            // The mechanism is correct but the value is zero on the
            // current shape — kept as opt-in infra for future shapes
            // where the inner-HJ bloom path doesn't cover the outer.
            // Default OFF; set `EMAT_L9_INNER_WITH_SEMI=1` to enable.
            let am1_enabled = std::env::var("EMAT_L9_INNER_WITH_SEMI")
                .ok()
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false);
            let build_has_semi = am1_enabled && build_subtree_has_semi_filter(hj.left());
            if matches!(hj.join_type(), JoinType::Inner)
                && !self.allow_inner_join
                && !build_has_semi
            {
                if trace {
                    eprintln!(
                        "[L9.trace] skip Inner — allow_inner_join=false, no LeftSemi/LeftAnti in build"
                    );
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
                // KEYS.1: accept any join key that losslessly widens to i64
                // (Int8/16/32/64, UInt8/16/32, Date32/64), not just Int64.
                // The bloom is an i64-domain structure; the build emitter
                // widens on insert and the probe reads the native i64 parquet
                // column, so an Int32 key (e.g. a downcast-narrowed `*key`)
                // is byte-for-byte equivalent. Before this the rule silently
                // skipped narrowed keys → lost the probe-side bloom (Q21 +48%).
                let l_dt = build.schema().field(lcol.index()).data_type().clone();
                let r_dt = probe.schema().field(rcol.index()).data_type().clone();
                // KEYS.1 + KEYS.5: accept either an i64-domain key pair
                // (Int*/UInt*/Date* — rides the i64 bloom/set) OR a UTF-8
                // string key pair (Utf8/Utf8View/LargeUtf8 — rides the
                // StringInBloom/StringInSet sideband). Both sides must share
                // the domain; an equi-join can't cross int↔string, and a
                // mixed pair is left alone (no sideband). The build emitter
                // auto-dispatches on the build key's type and the probe scan
                // applies whichever ColumnPredicate the sideband publishes.
                let i64_pair = widens_to_i64(&l_dt) && widens_to_i64(&r_dt);
                let string_pair = is_string_key(&l_dt) && is_string_key(&r_dt);
                if !i64_pair && !string_pair {
                    if trace {
                        eprintln!(
                            "[L9.trace] skip — key pair {}/{} is neither i64-domain nor string ({l_dt:?}/{r_dt:?})",
                            lcol.name(),
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
            // Σ.AM.1: when the build contains a LeftSemi/LeftAnti, the
            // estimator (which doesn't see through HashJoinExec) gives
            // the unfiltered LEFT cardinality (e.g. 1.5M customer for
            // Q18's build) rather than the actual semi-filtered output
            // (~624 rows). Cap the estimate at 10K when this shape is
            // detected — generous enough for any selective subquery,
            // tight enough to pass the 64× ratio gate against a 60M
            // probe (10K × 64 = 640K < 60M).
            let build_rows = if build_has_semi {
                Some(
                    estimate_build_rows(build.as_ref())
                        .unwrap_or(10_000)
                        .min(10_000),
                )
            } else {
                estimate_build_rows(build.as_ref())
            };
            // L9.ADAPT — also compute the DEFAULT (stats-only) estimate so
            // wraps the default estimator would have rejected get marked
            // tight-admitted and carry the scoped guards (width floor +
            // publish-wait budget). Wraps both estimators admit keep
            // today's exact behavior.
            let tight_mode = tight_cardinality_enabled();
            let default_build_rows = if !tight_mode {
                build_rows
            } else if build_has_semi {
                Some(
                    estimate_build_rows_mode(build.as_ref(), false)
                        .unwrap_or(10_000)
                        .min(10_000),
                )
            } else {
                estimate_build_rows_mode(build.as_ref(), false)
            };
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
            // L9.WIDTH (2026-06-10): probe-payoff gate. The scan-level
            // predicate's economics: it adds a per-row set/bloom probe +
            // batch compaction over the FULL scan (60M rows on TPC-H facts)
            // and saves (a) the join's per-row probe and (b) the payload
            // columns flowing downstream. Measured per-operator: Q08's wide
            // join (5-col probe payload) cost 13ns/row → wrap nets −30ms;
            // Q17's narrow joins (2-col) cost 2ns/row → two wraps ADDED
            // 1.5s CPU (+15-50ms wall). The payoff tracks the probe scan's
            // projected width — wide scans both feed expensive joins and
            // amortize the probe against more saved column flow. Gate:
            // wrap only when the probe scan projects ≥ `min_probe_proj_cols`
            // columns (struct field, env-defaulted to 4 in `Default`;
            // calibrated on the Q08-win / Q17-regress boundary). NOTE: it is
            // a partial guard — Q02's regressing wrap passes it (wide
            // partsupp scan, cheap join), which is part of why the
            // NDV-tight estimate stays opt-in.
            // L9.ADAPT — tight-admitted wraps (default estimator would have
            // rejected; payoff unproven) get the width floor; everything
            // else keeps the configured (default 0 = disabled) value.
            let tight_only = tight_mode
                && admitted_only_by_tight(
                    build_rows,
                    default_build_rows,
                    probe_rows,
                    self.min_probe_to_build_ratio,
                );
            let min_proj_cols = effective_min_proj_cols(tight_only, self.min_probe_proj_cols);
            if scan_arc.projection().len() < min_proj_cols {
                if trace {
                    eprintln!(
                        "[L9.trace] skip — probe scan projects {} cols < min {} (tight_only={tight_only}; narrow-payload wrap loses: scan-probe cost exceeds the cheap join it relieves)",
                        scan_arc.projection().len(),
                        min_proj_cols
                    );
                }
                return Ok(Transformed::no(node));
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
            let sideband = if tight_only {
                BridgeFilterSideband::new().mark_tight_admitted()
            } else {
                BridgeFilterSideband::new()
            };

            // Estimate expected build-side keys from the build's
            // partition statistics. Falls back to a generous default
            // (50K) when stats are unavailable.
            let expected_keys = build_rows.unwrap_or(50_000);

            // Σ.AH.3 Story 2a — absolute per-partition build-size gate.
            // Reject when the per-partition bloom would be too large to
            // pay back its probe cost. Mirrors BuildSideBloomEmitterExec's
            // own per-partition computation: `(total / n_part).max(64)`.
            if self.max_expected_keys_per_partition > 0 {
                let n_part = build.output_partitioning().partition_count().max(1);
                let per_part = (expected_keys / n_part).max(64);
                if per_part > self.max_expected_keys_per_partition {
                    if trace {
                        eprintln!(
                            "[L9.trace] skip — expected_keys_per_partition={per_part} > ceiling={} (total={expected_keys}, n_part={n_part})",
                            self.max_expected_keys_per_partition
                        );
                    }
                    return Ok(Transformed::no(node));
                }
            }

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

/// Σ.AM.1 (2026-05-27), extended REV.2 (2026-05-30) — returns true
/// iff the build subtree contains a `HashJoinExec` whose join_type is
/// any semi/anti variant (`LeftSemi`/`LeftAnti`/`RightSemi`/`RightAnti`).
///
/// Such a join intrinsically filters its LEFT input by membership in
/// the RIGHT input. The build-side output cardinality is therefore
/// upper-bounded by `min(|left|, distinct_keys(right))` — typically
/// much smaller than DataFusion's `partition_statistics` estimate
/// (which doesn't see through HashJoinExec).
///
/// Use case (Q18 SF=10): top Inner HJ's build = customer ⋈
/// LeftSemi(orders, sum-filtered-lineitem). Build output is 624 rows
/// at runtime, but the estimator says ~1.5M. Without this gate, L9's
/// default `allow_inner_join=false` skips the join entirely. With
/// this gate, we recognize the semi-filtered build and allow L9 fire.
pub(crate) fn build_subtree_has_semi_filter(plan: &Arc<dyn ExecutionPlan>) -> bool {
    use datafusion::common::JoinType;
    if let Some(hj) = plan.as_any().downcast_ref::<HashJoinExec>() {
        // REV.2 (2026-05-30): match all four semi/anti variants, not
        // just Left*. `SwapSemiJoinBuildSideRule` flips Q18's semi to
        // `RightSemi` (so the ~624-key agg-bounded side becomes the
        // build), and that `RightSemi` sits inside the top Inner join's
        // build subtree. Matching only `LeftSemi`/`LeftAnti` made the
        // `EMAT_L9_INNER_WITH_SEMI` path a silent no-op on Q18 (verified
        // via `EMAT_L9_TRACE=1`). Symmetric with the swap rule, which
        // already handles all four variants.
        if matches!(
            hj.join_type(),
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::RightSemi | JoinType::RightAnti
        ) {
            return true;
        }
    }
    plan.children()
        .iter()
        .any(|c| build_subtree_has_semi_filter(c))
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
    estimate_build_rows_mode(plan, tight_cardinality_enabled())
}

/// L9.ADAPT — is the tight (NDV) cardinality estimate enabled?
/// **Still OPT-IN** (`EMAT_L9_TIGHT_CARDINALITY=1`). L9.ADAPT hardened
/// the opt-in mode with two scoped guards — the tight-only width floor
/// (fixes Q17's narrow-scan wrap, +16%→−10% dedicated) and the
/// total-probe-rows publish-wait budget (bounds Q02's stall mechanism)
/// — and the dedicated A/Bs looked shippable (Q08 −19.5%, Q17 −10%,
/// Q07 +2%). The default-ON flip was then REVERTED by the 22q sweep
/// gate: Q19 +102% (its lineitem bundle carries STRING statics, so the
/// fused-probe arm can't engage and the wrap drives the scan down the
/// legacy masked path with per-predicate string re-decodes) and Q17
/// +27% IN-SWEEP (mechanism unresolved; contradicts its own dedicated
/// A/B). Plan-time guard composition keeps surfacing new failure modes
/// — default-on needs the runtime per-RG disarm + a fused-arm
/// engageability precondition, gated on the full sweep per iteration.
pub(crate) fn tight_cardinality_enabled() -> bool {
    std::env::var("EMAT_L9_TIGHT_CARDINALITY").as_deref() == Ok("1")
}

/// L9.ADAPT — a wrap is "tight-admitted" iff the tight estimate passes
/// the probe/build ratio gate while the default estimate would have been
/// rejected by it. Only those wraps carry the stricter tight guards;
/// anything the default estimator already admits keeps today's behavior
/// (Q07's winning 2-col nation blooms must not see the width floor).
fn admitted_only_by_tight(
    tight_est: Option<usize>,
    default_est: Option<usize>,
    probe_rows: Option<usize>,
    ratio: usize,
) -> bool {
    if ratio == 0 {
        return false;
    }
    let (Some(t), Some(d), Some(p)) = (tight_est, default_est, probe_rows) else {
        return false;
    };
    t.saturating_mul(ratio) < p && d.saturating_mul(ratio) >= p
}

/// L9.ADAPT — width floor for tight-admitted wraps. Measured boundary:
/// Q17's 2-3-col wraps lose (+16% — the probe key is decoded anyway, so
/// a narrow scan has almost no masked-decode relief to pay for the
/// probe), Q08's 5-col wrap wins (−18%). Default-admitted wraps keep the
/// configured value (global floor measured +88% on Q07).
const TIGHT_MIN_PROBE_PROJ_COLS: usize = 3;

fn effective_min_proj_cols(tight_admitted: bool, configured: usize) -> usize {
    if tight_admitted {
        configured.max(TIGHT_MIN_PROBE_PROJ_COLS)
    } else {
        configured
    }
}

fn estimate_build_rows_mode(plan: &dyn ExecutionPlan, tight: bool) -> Option<usize> {
    // L9.EST (2026-06-10): structural caps for builds whose true output the
    // statistics CANNOT see. The fresh Q18 SF=10 trace showed the estimator
    // crediting a `Filter(sum(qty)>300)`-over-Aggregate build with the raw
    // 60M-row lineitem leaf (true output: 624 rows — off by 10^5), and a
    // semi-join output with its probe child's 15M. Both shapes are
    // *intrinsically* selective: a HAVING-style value-filter over a GROUP BY
    // keeps few groups, and a semi/anti join exists to filter. Cap them at
    // the Σ.AM.1 constant (10K — generous for any selective subquery, tight
    // enough that the ratio gate still only admits large probes: 10K × 1024
    // ≈ 10.2M). Mirrors the Σ.AM.1 `build_has_semi` cap but lives in the
    // estimator so every consumer (ratio gate, expected_keys sizing) agrees.
    const SELECTIVE_SHAPE_CAP: usize = 10_000;
    if build_top_is_selective_shape(plan) {
        let est = estimate_build_rows_uncapped(plan, tight).unwrap_or(SELECTIVE_SHAPE_CAP);
        return Some(est.min(SELECTIVE_SHAPE_CAP));
    }
    estimate_build_rows_uncapped(plan, tight)
}

/// Walk through pass-through operators (projection / coalesce / repartition)
/// and report whether the first row-shaping operator of the build subtree is
/// a semi/anti join or a HAVING-style `FilterExec` directly over an
/// `AggregateExec` — the two shapes whose output cardinality plan-time
/// statistics structurally over-estimate (they see through to the leaf).
fn build_top_is_selective_shape(plan: &dyn ExecutionPlan) -> bool {
    use datafusion::common::JoinType;
    use datafusion::physical_plan::filter::FilterExec;
    if is_pass_through_node(plan) {
        return plan
            .children()
            .first()
            .is_some_and(|c| build_top_is_selective_shape(c.as_ref()));
    }
    if let Some(hj) = plan.as_any().downcast_ref::<HashJoinExec>() {
        return matches!(
            hj.join_type(),
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::RightSemi | JoinType::RightAnti
        );
    }
    if let Some(f) = plan.as_any().downcast_ref::<FilterExec>() {
        // HAVING shape: the filter's input (through pass-throughs) is an
        // aggregate — i.e. a value-filter on aggregated groups.
        return first_shaping_node_is_aggregate(f.input().as_ref());
    }
    false
}

fn is_pass_through_node(p: &dyn ExecutionPlan) -> bool {
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    p.as_any().downcast_ref::<ProjectionExec>().is_some()
        || p.as_any()
            .downcast_ref::<CoalescePartitionsExec>()
            .is_some()
        || p.as_any().downcast_ref::<RepartitionExec>().is_some()
}

fn first_shaping_node_is_aggregate(p: &dyn ExecutionPlan) -> bool {
    use datafusion::physical_plan::aggregates::AggregateExec;
    if is_pass_through_node(p) {
        return p
            .children()
            .first()
            .is_some_and(|c| first_shaping_node_is_aggregate(c.as_ref()));
    }
    p.as_any().downcast_ref::<AggregateExec>().is_some()
}

fn estimate_build_rows_uncapped(plan: &dyn ExecutionPlan, tight: bool) -> Option<usize> {
    // L9.EST (2026-06-10) / L9.ADAPT (2026-06-11): the emat-stats path
    // (NDV-corrected string-eq selectivity, backed by the rule-local lazy
    // dict-page walk — `local_dict_ndv` — so the global stats
    // JoinSelection reads stay untouched) remains OPT-IN — see
    // `tight_cardinality_enabled` for the default-on attempt's post-
    // mortem. When enabled, it admits Inner-join wraps whose payoff
    // plan-time shape alone could not predict (Q08 −19.5% WIN, but
    // Q17 +16% / Q02 +85% regressed); the gate site therefore computes
    // BOTH estimates and marks wraps the default estimator would have
    // rejected as tight-admitted, applying two scoped guards: the
    // `TIGHT_MIN_PROBE_PROJ_COLS` width floor (Q17-class: narrow scans
    // have no masked-decode relief to pay for the probe) and the
    // `tight_peek_budget_ms` publish-wait budget (Q02-class: a fast scan
    // must not stall behind a slow subquery build).
    if tight {
        if let Some(tight_est) = estimate_build_rows_via_emat_stats(plan) {
            return Some(tight_est);
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
    let (raw, column_stats, scan_meta) = find_emat_scan_stats(plan)?;
    let sel = estimate_filter_selectivity_via_emat_stats(&predicate, &column_stats, &|proj_idx| {
        let file_idx = scan_meta.projection.get(proj_idx).copied()?;
        local_dict_ndv(&scan_meta.path, raw, file_idx)
    });
    Some(((raw as f64 * sel).round() as usize).max(1))
}

/// L9.NDV (2026-06-10): on-demand, **rule-local** dict-page NDV for one
/// column of one parquet file. The global dict-distinct walk
/// (`EMAT_DICT_DISTINCT`) stays default-OFF because populating
/// `ColumnStatistics::distinct_count` perturbs DataFusion's JoinSelection
/// (Σ.Q06.SF10.5.h; re-confirmed 2026-06-10: it flips Q08's c⋈o and o⋈l to
/// CollectLeft and regresses Q08 +15%). This helper gives the L9 gate the
/// same truth WITHOUT writing it into the stats the planner reads: it walks
/// the dict-page headers lazily (header-only, no decompress), memoizes per
/// file, and refuses files larger than `EMAT_L9_NDV_MAX_ROWS` (default 10M
/// — dimensions yes, facts no) so plan-time cost stays microscopic.
fn local_dict_ndv(path: &str, file_rows: usize, col_idx: usize) -> Option<usize> {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::OnceLock;
    let max_rows: usize = std::env::var("EMAT_L9_NDV_MAX_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000_000);
    if max_rows == 0 || file_rows > max_rows {
        return None;
    }
    // One memoized walk per file, sized at a fixed generous width so every
    // later column lookup hits the same cached Vec (the walker clamps to the
    // row groups' real column count; surplus entries stay None). Columns
    // beyond `WALK_WIDTH` simply can't resolve NDV — acceptable for the
    // selectivity heuristic this feeds.
    const WALK_WIDTH: usize = 256;
    if col_idx >= WALK_WIDTH {
        return None;
    }
    /// Per-file memo of the walk result (None = walk failed for that file).
    type NdvCacheEntry = Option<std::sync::Arc<Vec<Option<usize>>>>;
    static CACHE: OnceLock<Mutex<HashMap<String, NdvCacheEntry>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let per_col = {
        let mut guard = cache.lock().ok()?;
        guard
            .entry(path.to_string())
            .or_insert_with(|| {
                crate::emat_parquet_metadata::dict_distinct_max_per_column(path, WALK_WIDTH)
                    .ok()
                    .map(std::sync::Arc::new)
            })
            .clone()
    }?;
    per_col.get(col_idx).copied().flatten().filter(|n| *n > 0)
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
/// `(raw_row_count, column_stats, scan_meta)`. `scan_meta` carries the
/// file path + projected→file column mapping so the selectivity calc can
/// resolve dict-page NDV lazily (L9.NDV) when `distinct_count` is Absent.
struct EmatScanMeta {
    path: String,
    projection: Vec<usize>,
}

fn find_emat_scan_stats(
    plan: &dyn ExecutionPlan,
) -> Option<(
    usize,
    Vec<datafusion::common::stats::ColumnStatistics>,
    EmatScanMeta,
)> {
    if let Some(s) = plan.as_any().downcast_ref::<EmatixFastParquetExec>() {
        return Some((
            s.num_rows(),
            s.column_stats().to_vec(),
            EmatScanMeta {
                path: s.path().to_string(),
                projection: s.projection().to_vec(),
            },
        ));
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
/// - `col = literal` → `1 / distinct_count(col)` when available, else
///   `1 / lazy_ndv(col)` (L9.NDV — the rule-local dict-page walk; keeps
///   the global stats Absent so JoinSelection's plan choices are
///   untouched)
/// - `expr AND expr` → product of children's selectivity
/// - `expr OR expr`  → conservative max of children's selectivity
/// - anything else   → 0.2 (same conservative default as DataFusion)
fn estimate_filter_selectivity_via_emat_stats(
    predicate: &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    column_stats: &[datafusion::common::stats::ColumnStatistics],
    lazy_ndv: &dyn Fn(usize) -> Option<usize>,
) -> f64 {
    use datafusion::common::stats::Precision;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Literal};
    if let Some(bin) = predicate.as_any().downcast_ref::<BinaryExpr>() {
        match bin.op() {
            Operator::And => {
                let l =
                    estimate_filter_selectivity_via_emat_stats(bin.left(), column_stats, lazy_ndv);
                let r =
                    estimate_filter_selectivity_via_emat_stats(bin.right(), column_stats, lazy_ndv);
                return (l * r).clamp(0.0, 1.0);
            }
            Operator::Or => {
                let l =
                    estimate_filter_selectivity_via_emat_stats(bin.left(), column_stats, lazy_ndv);
                let r =
                    estimate_filter_selectivity_via_emat_stats(bin.right(), column_stats, lazy_ndv);
                return l.max(r).clamp(0.0, 1.0);
            }
            Operator::Eq => {
                if let Some(col) = bin.left().as_any().downcast_ref::<Column>() {
                    if bin.right().as_any().downcast_ref::<Literal>().is_some() {
                        let mut n = column_stats
                            .get(col.index())
                            .map(|cs| match cs.distinct_count {
                                Precision::Exact(n) | Precision::Inexact(n) => n,
                                _ => 0,
                            })
                            .unwrap_or(0);
                        if n == 0 {
                            n = lazy_ndv(col.index()).unwrap_or(0);
                        }
                        if n > 0 {
                            return 1.0 / n as f64;
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

    /// L9.ADAPT — a wrap is "tight-admitted" iff the tight (NDV) estimate
    /// passes the probe/build ratio gate while the default estimate would
    /// have been rejected. Those wraps get the stricter width floor +
    /// publish-wait budget; wraps the default estimator already admits
    /// keep today's exact behavior.
    #[test]
    fn admitted_only_by_tight_truth_table() {
        // Q08 shape: default 400K rejected (400K×1024 ≥ 60M), tight 13.3K admitted.
        assert!(admitted_only_by_tight(
            Some(13_333),
            Some(400_000),
            Some(59_986_052),
            1024
        ));
        // Both estimates admit (Q18 HAVING-cap shape) → not tight-only.
        assert!(!admitted_only_by_tight(
            Some(2_000),
            Some(2_000),
            Some(59_986_052),
            1024
        ));
        // Default estimate unavailable → the gate would have skipped in
        // default mode too (it only rejects when both sides are Some).
        assert!(!admitted_only_by_tight(
            Some(13_333),
            None,
            Some(59_986_052),
            1024
        ));
        // No probe estimate → gate never rejected anyone.
        assert!(!admitted_only_by_tight(
            Some(13_333),
            Some(400_000),
            None,
            1024
        ));
        // Ratio gate disabled → nothing is tight-only.
        assert!(!admitted_only_by_tight(
            Some(13_333),
            Some(400_000),
            Some(59_986_052),
            0
        ));
    }

    /// L9.ADAPT — tight-admitted wraps get a width floor of 3 projected
    /// columns (measured: Q17's 2-3-col wraps lose +16%, Q08's 5-col wrap
    /// wins −18%); default-admitted wraps keep the configured value
    /// (Q07's winning 2-col nation blooms must not be blocked: a global
    /// width gate measured +88% on Q07).
    #[test]
    fn tight_width_floor() {
        assert_eq!(effective_min_proj_cols(false, 0), 0);
        assert_eq!(effective_min_proj_cols(false, 4), 4);
        assert_eq!(effective_min_proj_cols(true, 0), 3);
        assert_eq!(effective_min_proj_cols(true, 5), 5);
    }

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
                // Story 2a: disable the absolute-build-size gate for
                // these wrap-mechanism tests (tiny inputs anyway).
                max_expected_keys_per_partition: 0,
                // Width gate off: fixtures are 1-2 column synthetic tables
                // exercising the wrap mechanics, not the payoff heuristic.
                min_probe_proj_cols: 0,
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

    /// KEYS.5.c — write a parquet with a UTF-8 string column (+ optional
    /// i64 payload) via the arrow `ArrowWriter`, which emits the UTF8
    /// converted_type so `arrow_type_for` surfaces it as `Utf8` (the in-tree
    /// `write_table_to_path` writes byte arrays with no marker → `Binary`,
    /// which wouldn't exercise the string-key path).
    fn write_utf8_parquet(
        path: &std::path::Path,
        str_col: (&str, &[&str]),
        i64_col: Option<(&str, &[i64])>,
    ) {
        use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use datafusion::parquet::arrow::ArrowWriter;

        let mut fields = vec![Field::new(str_col.0, DataType::Utf8, false)];
        let mut arrays: Vec<ArrayRef> = vec![Arc::new(StringArray::from(str_col.1.to_vec()))];
        if let Some((name, vals)) = i64_col {
            fields.push(Field::new(name, DataType::Int64, false));
            arrays.push(Arc::new(Int64Array::from(vals.to_vec())));
        }
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut w = ArrowWriter::try_new(file, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }

    /// KEYS.5.c — the rule must route a STRING-keyed join through the
    /// sideband: wrap the build with a BuildSideBloomEmitterExec (which emits
    /// StringInBloom/StringInSet for a string key) and thread it into the
    /// probe-side Emat scan. RED before the gate relaxation: `widens_to_i64`
    /// declined Utf8 on both sides, so the equi-key found no match → no wrap.
    #[tokio::test]
    async fn rule_wraps_string_keyed_join() {
        let dim = tmp_parquet("dim_str");
        let fact = tmp_parquet("fact_str");
        // dim: 3 distinct string keys (small → becomes the build side).
        write_utf8_parquet(&dim, ("d_key", &["FRANCE", "GERMANY", "BRAZIL"]), None);
        // fact: 30 rows; f_key cycles 6 values (3 match dim, 3 don't).
        let cycle = ["FRANCE", "GERMANY", "BRAZIL", "JAPAN", "PERU", "CHINA"];
        let keys: Vec<&str> = (0..30).map(|i| cycle[i % 6]).collect();
        let vals: Vec<i64> = (0..30).collect();
        write_utf8_parquet(&fact, ("f_key", &keys), Some(("f_val", &vals)));

        let cfg = SessionConfig::new().with_target_partitions(4);
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(cfg)
            .with_physical_optimizer_rule(Arc::new(EnableRuntimeBloomSidebandRule {
                min_probe_to_build_ratio: 0,
                allow_inner_join: true,
                require_filtered_build: false,
                max_expected_keys_per_partition: 0,
                // Width gate off: fixtures are 1-2 column synthetic tables
                // exercising the wrap mechanics, not the payoff heuristic.
                min_probe_proj_cols: 0,
            }))
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table(
            "dim",
            Arc::new(EmatixFastParquetTableProvider::try_new(dim.to_string_lossy()).unwrap()),
        )
        .unwrap();
        ctx.register_table(
            "fact",
            Arc::new(EmatixFastParquetTableProvider::try_new(fact.to_string_lossy()).unwrap()),
        )
        .unwrap();

        let df = ctx
            .sql("SELECT f_val FROM dim JOIN fact ON d_key = f_key")
            .await
            .unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let s = format!("{plan:?}");
        assert!(
            s.contains("BuildSideBloomEmitterExec"),
            "expected the L9 wrapper for a string-keyed join:\n{s}"
        );
        // Correctness: 3 matching keys × 5 occurrences each (30/6) = 15 rows.
        // The sideband is selectivity-only — the residual join still runs, so
        // the count is identical with or without the wrap.
        let batches = df.collect().await.unwrap();
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            row_count, 15,
            "wrong row count for string-keyed join with L9 active"
        );
    }

    /// KEYS.5.d — a string build above EMAT_L9_SET_THRESHOLD overflows
    /// the exact set, so the emitter publishes a StringInBloom. This
    /// drives the StringInBloom branch of build_bitmap end-to-end and
    /// proves bloom false-positives are caught by the residual join (the
    /// final row count stays exact). Both sides exceed the threshold,
    /// so the path is the bloom regardless of which side becomes the
    /// build. Sized off `l9_set_threshold()` so it tracks the default
    /// (L9.SETCAP raised it).
    #[tokio::test]
    async fn rule_string_join_uses_bloom_above_threshold() {
        let dim = tmp_parquet("dim_bloom_str");
        let fact = tmp_parquet("fact_bloom_str");
        // dim: threshold+16 distinct keys (set overflows → bloom).
        let n_over = crate::build_side_bloom_emitter_exec::l9_set_threshold() + 16;
        let dim_keys: Vec<String> = (0..n_over).map(|i| format!("d{i:06}")).collect();
        let dim_refs: Vec<&str> = dim_keys.iter().map(|s| s.as_str()).collect();
        write_utf8_parquet(&dim, ("d_key", &dim_refs), None);
        // fact: n_over+more rows; first half match dim, rest don't (z...).
        // Also above the threshold so the bloom path holds if it ends up
        // the build side.
        let n_fact = n_over + 16;
        let half_fact = n_fact / 2;
        let fact_keys: Vec<String> = (0..n_fact)
            .map(|i| {
                if i < half_fact {
                    format!("d{i:06}")
                } else {
                    format!("z{i:06}")
                }
            })
            .collect();
        let fact_refs: Vec<&str> = fact_keys.iter().map(|s| s.as_str()).collect();
        let fact_vals: Vec<i64> = (0..n_fact as i64).collect();
        write_utf8_parquet(&fact, ("f_key", &fact_refs), Some(("f_val", &fact_vals)));

        let cfg = SessionConfig::new().with_target_partitions(4);
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(cfg)
            .with_physical_optimizer_rule(Arc::new(EnableRuntimeBloomSidebandRule {
                min_probe_to_build_ratio: 0,
                allow_inner_join: true,
                require_filtered_build: false,
                max_expected_keys_per_partition: 0,
                // Width gate off: fixtures are 1-2 column synthetic tables
                // exercising the wrap mechanics, not the payoff heuristic.
                min_probe_proj_cols: 0,
            }))
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table(
            "dim",
            Arc::new(EmatixFastParquetTableProvider::try_new(dim.to_string_lossy()).unwrap()),
        )
        .unwrap();
        ctx.register_table(
            "fact",
            Arc::new(EmatixFastParquetTableProvider::try_new(fact.to_string_lossy()).unwrap()),
        )
        .unwrap();

        let df = ctx
            .sql("SELECT f_val FROM dim JOIN fact ON d_key = f_key")
            .await
            .unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let s = format!("{plan:?}");
        assert!(
            s.contains("BuildSideBloomEmitterExec"),
            "expected the L9 wrapper on the bloom-path string join:\n{s}"
        );
        // Exactly the matching fact rows — any bloom false-positives on
        // the z-keys are dropped by the residual HashJoin equality test.
        let batches = df.collect().await.unwrap();
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            row_count, half_fact,
            "bloom-path string join must stay exact (residual join filters FPs)"
        );
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
                // Story 2a: disable the absolute-build-size gate for
                // these wrap-mechanism tests (tiny inputs anyway).
                max_expected_keys_per_partition: 0,
                // Width gate off: fixtures are 1-2 column synthetic tables
                // exercising the wrap mechanics, not the payoff heuristic.
                min_probe_proj_cols: 0,
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

    /// Σ.AH.3 Story 2a — verify the absolute per-partition build-size
    /// gate skips bloom emit when the estimated build exceeds the
    /// threshold. Uses the same lineitem/supplier mini-parquets as the
    /// wrap test but sets `max_expected_keys_per_partition: 4` — below
    /// supplier's 25-row build (per-partition ≈ 25/4=6 rounded to
    /// .max(64)=64 floor, but with threshold=4 the .max(64) per-part
    /// floor still exceeds the gate so wrap is rejected).
    #[tokio::test]
    async fn gate_skips_when_build_exceeds_max_expected_keys() {
        let li = tmp_parquet("lineitem_gate");
        let sp = tmp_parquet("supplier_gate");
        write_lineitem(&li);
        write_supplier(&sp);

        let cfg = SessionConfig::new().with_target_partitions(4);
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(cfg)
            .with_physical_optimizer_rule(Arc::new(EnableRuntimeBloomSidebandRule {
                min_probe_to_build_ratio: 0,
                allow_inner_join: true,
                require_filtered_build: false,
                // Per-partition computation uses .max(64) floor inside
                // BuildSideBloomEmitterExec; setting the gate to 4
                // forces rejection (any emit would exceed 4 per partition
                // because of the 64 floor).
                max_expected_keys_per_partition: 4,
                // Width gate off: narrow synthetic fixtures.
                min_probe_proj_cols: 0,
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
        // Gate should fire — no wrap installed.
        assert!(
            !s.contains("BuildSideBloomEmitterExec"),
            "expected the L9 wrapper to be SKIPPED by the gate, but found it:\n{s}"
        );
        // Output correctness — query still produces 500 rows.
        let batches = df.collect().await.unwrap();
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(row_count, 500, "wrong row count when gate skips emit");
    }

    /// Σ.AH.3 Story 2a — converse: with a generous threshold (1_000_000)
    /// the gate is no-op and the wrap fires. Guards against the gate
    /// firing too aggressively.
    #[tokio::test]
    async fn gate_allows_emit_when_build_below_max_expected_keys() {
        let li = tmp_parquet("lineitem_gate2");
        let sp = tmp_parquet("supplier_gate2");
        write_lineitem(&li);
        write_supplier(&sp);

        let cfg = SessionConfig::new().with_target_partitions(4);
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(cfg)
            .with_physical_optimizer_rule(Arc::new(EnableRuntimeBloomSidebandRule {
                min_probe_to_build_ratio: 0,
                allow_inner_join: true,
                require_filtered_build: false,
                max_expected_keys_per_partition: 1_000_000,
                // Width gate off: narrow synthetic fixtures.
                min_probe_proj_cols: 0,
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
        let plan = df.create_physical_plan().await.unwrap();
        let s = format!("{plan:?}");
        assert!(
            s.contains("BuildSideBloomEmitterExec"),
            "expected the L9 wrapper to fire under the generous gate; plan:\n{s}"
        );
    }

    /// Σ.AM.1 (2026-05-27): unit-level verification of the helper —
    /// `build_subtree_has_semi_filter` must return true for a plan
    /// containing a LeftSemi or LeftAnti HashJoinExec, and false
    /// otherwise. End-to-end validation lives in the Q18 SF=10 bench;
    /// the helper test guards against regressions of the walker
    /// itself.
    #[tokio::test]
    async fn helper_detects_left_semi_in_build_subtree() {
        let li = tmp_parquet("lineitem_am1");
        let sp = tmp_parquet("supplier_am1");
        write_lineitem(&li);
        write_supplier(&sp);

        let cfg = SessionConfig::new().with_target_partitions(4);
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(cfg)
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

        // Plain join: no LeftSemi or LeftAnti.
        let plain = ctx
            .sql("SELECT l_orderkey FROM supplier JOIN lineitem ON s_suppkey = l_suppkey")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let plain_arc: Arc<dyn ExecutionPlan> = plain;
        assert!(
            !build_subtree_has_semi_filter(&plain_arc),
            "plain inner join must not be detected as semi-filtered"
        );

        // EXISTS subquery typically produces LeftSemi (or RightSemi /
        // LeftAnti depending on shape). The helper only matches
        // LeftSemi/LeftAnti — that's the Q18 shape after Σ.Q.L10.
        let semi = ctx
            .sql(
                "SELECT s_suppkey FROM supplier \
                 WHERE EXISTS (SELECT 1 FROM lineitem WHERE l_suppkey = s_suppkey)",
            )
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let semi_arc: Arc<dyn ExecutionPlan> = semi;
        let dump = format!("{semi_arc:?}");
        // The plan must contain SOME semi shape — DataFusion may
        // produce LeftSemi or RightSemi depending on cost. We only
        // assert the helper agrees with itself.
        let has_left_semi_in_dump = dump.contains("LeftSemi") || dump.contains("LeftAnti");
        assert_eq!(
            build_subtree_has_semi_filter(&semi_arc),
            has_left_semi_in_dump,
            "helper output must match presence of LeftSemi/LeftAnti in plan dump"
        );
    }

    /// REV.2 (2026-05-30) — `build_subtree_has_semi_filter` must also
    /// detect `RightSemi` / `RightAnti`, not just the Left* variants.
    ///
    /// Q18's selective IN-subquery semi-join, after
    /// `SwapSemiJoinBuildSideRule` flips it (LeftSemi → RightSemi so the
    /// 624-key agg becomes the build side), lands as a `RightSemi`
    /// nested inside the TOP Inner join's build subtree. The detector
    /// previously matched only `LeftSemi | LeftAnti`, so the Σ.AM.1
    /// `EMAT_L9_INNER_WITH_SEMI` path silently no-op'd on Q18: the top
    /// Inner join was skipped at the `allow_inner_join` gate and its
    /// 60M-row lineitem probe was never bloom-pruned before the hash
    /// repartition. (Confirmed via `EMAT_L9_TRACE=1`: combo B printed
    /// `skip Inner — no LeftSemi/LeftAnti in build` despite the
    /// RightSemi being present.) This is the load-bearing gap behind
    /// the Q18 SF=100 2.53× loss vs DuckDB.
    #[test]
    fn helper_detects_right_semi_in_build_subtree() {
        use datafusion::arrow::array::{Int64Array, RecordBatch};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::common::{JoinType, NullEquality};
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::physical_expr::PhysicalExpr;
        use datafusion::physical_plan::joins::PartitionMode;

        fn mem_table(name: &str) -> Arc<dyn ExecutionPlan> {
            let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
            )
            .unwrap();
            MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap()
        }

        let on = vec![(
            Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
        )];
        let right_semi: Arc<dyn ExecutionPlan> = Arc::new(
            HashJoinExec::try_new(
                mem_table("a"),
                mem_table("a"),
                on,
                None,
                &JoinType::RightSemi,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        );

        assert!(
            build_subtree_has_semi_filter(&right_semi),
            "build_subtree_has_semi_filter must detect a RightSemi join \
             (Q18's post-swap semi shape). Without it, the Σ.AM.1 \
             inner-with-semi path no-ops and the top Inner join's lineitem \
             probe is never bloom-pruned."
        );

        // Symmetric negative: a plain Inner join (no semi/anti anywhere)
        // must still return false so the gate doesn't over-fire.
        let inner_on = vec![(
            Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
        )];
        let inner: Arc<dyn ExecutionPlan> = Arc::new(
            HashJoinExec::try_new(
                mem_table("a"),
                mem_table("a"),
                inner_on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        );
        assert!(
            !build_subtree_has_semi_filter(&inner),
            "a plain Inner join must not be detected as semi-filtered"
        );
    }

    /// L9.EST — `build_top_is_selective_shape` must flag the two shapes
    /// whose plan-time row estimates are structurally wrong by orders of
    /// magnitude (fresh Q18 SF=10 trace: semi build estimated 15M, true
    /// 624; HAVING build estimated 60M, true 624), and must NOT flag a
    /// plain Inner join or a filter over a leaf.
    #[test]
    fn selective_shape_detects_semi_and_having_builds() {
        use datafusion::arrow::array::{Int64Array, RecordBatch};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::common::{JoinType, NullEquality};
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::physical_expr::PhysicalExpr;
        use datafusion::physical_expr::aggregate::AggregateExprBuilder;
        use datafusion::physical_plan::aggregates::{
            AggregateExec, AggregateMode, PhysicalGroupBy,
        };
        use datafusion::physical_plan::expressions::{BinaryExpr, lit};
        use datafusion::physical_plan::filter::FilterExec;
        use datafusion::physical_plan::joins::PartitionMode;

        fn mem_table() -> Arc<dyn ExecutionPlan> {
            let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
            )
            .unwrap();
            MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap()
        }
        fn join(jt: JoinType) -> Arc<dyn ExecutionPlan> {
            let on = vec![(
                Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
                Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
            )];
            Arc::new(
                HashJoinExec::try_new(
                    mem_table(),
                    mem_table(),
                    on,
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

        // Semi/anti joins at the top of the build → selective.
        assert!(build_top_is_selective_shape(
            join(JoinType::RightSemi).as_ref()
        ));
        assert!(build_top_is_selective_shape(
            join(JoinType::LeftSemi).as_ref()
        ));
        // Inner join → not selective (its output can exceed both inputs).
        assert!(!build_top_is_selective_shape(
            join(JoinType::Inner).as_ref()
        ));

        // HAVING shape: Filter over Aggregate → selective.
        let input = mem_table();
        let sum = AggregateExprBuilder::new(
            datafusion::functions_aggregate::sum::sum_udaf(),
            vec![Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>],
        )
        .schema(input.schema())
        .alias("sum_a")
        .build()
        .map(Arc::new)
        .unwrap();
        let group_by = PhysicalGroupBy::new_single(vec![(
            Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
            "a".to_string(),
        )]);
        let agg: Arc<dyn ExecutionPlan> = Arc::new(
            AggregateExec::try_new(
                AggregateMode::Single,
                group_by,
                vec![sum],
                vec![None],
                input.clone(),
                input.schema(),
            )
            .unwrap(),
        );
        let having_pred = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("sum_a", 1)),
            datafusion::logical_expr::Operator::Gt,
            lit(300i64),
        )) as Arc<dyn PhysicalExpr>;
        let having: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(having_pred, agg).unwrap());
        assert!(
            build_top_is_selective_shape(having.as_ref()),
            "Filter over Aggregate (HAVING) must be detected as selective"
        );

        // Filter over a plain leaf → NOT the HAVING shape.
        let leaf_pred = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            datafusion::logical_expr::Operator::Gt,
            lit(1i64),
        )) as Arc<dyn PhysicalExpr>;
        let leaf_filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(leaf_pred, mem_table()).unwrap());
        assert!(!build_top_is_selective_shape(leaf_filter.as_ref()));

        // And the estimator must cap a selective-shape build at the
        // Σ.AM.1 constant even though the underlying stats say 3 rows
        // (here) or 60M (Q18) — the cap is what unlocks the ratio gate.
        let est = estimate_build_rows(having.as_ref());
        assert!(
            est.is_some() && est.unwrap() <= 10_000,
            "HAVING build estimate must be capped at 10K, got {est:?}"
        );
    }

    /// L9.NDV — when `distinct_count` is Absent (the global dict-distinct
    /// walk stays default-off because it perturbs JoinSelection), the
    /// emat-stats estimate path must fall back to the rule-local lazy
    /// dict-page NDV: a 150-distinct string filter over 3000 rows
    /// estimates ~20 rows (3000/150), not 600 (3000 × the 0.2 default).
    /// The global stats the planner reads must stay Absent. (The path is
    /// opt-in via `EMAT_L9_TIGHT_CARDINALITY=1` — see
    /// `estimate_build_rows_uncapped` for the measured win/loss split —
    /// so the test exercises the inner fn directly.)
    #[tokio::test]
    async fn estimate_build_rows_uses_lazy_dict_ndv() {
        use datafusion::physical_plan::filter::FilterExec;

        let path = tmp_parquet("ndv_part");
        // 150 distinct ptype values × 20 reps = 3000 rows, plus an i64 key.
        let vals: Vec<String> = (0..3000).map(|i| format!("v{:03}", i % 150)).collect();
        let val_refs: Vec<&str> = vals.iter().map(|s| s.as_str()).collect();
        let keys: Vec<i64> = (0..3000).collect();
        write_utf8_parquet(&path, ("ptype", &val_refs), Some(("pkey", &keys)));

        let ctx = SessionContext::new();
        let provider =
            EmatixFastParquetTableProvider::try_new(path.to_string_lossy().to_string()).unwrap();
        ctx.register_table("part_t", Arc::new(provider)).unwrap();
        let df = ctx
            .sql("SELECT pkey FROM part_t WHERE ptype = 'v007'")
            .await
            .unwrap();
        let plan = ctx
            .state()
            .create_physical_plan(&df.into_optimized_plan().unwrap())
            .await
            .unwrap();

        // Locate the FilterExec (the provider declares string-eq Inexact, so
        // a FilterExec survives above the scan).
        fn find_filter(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
            if plan.as_any().downcast_ref::<FilterExec>().is_some() {
                return Some(plan.clone());
            }
            for c in plan.children() {
                if let Some(f) = find_filter(c) {
                    return Some(f);
                }
            }
            None
        }
        let filter = find_filter(&plan).expect("plan must keep a FilterExec for ptype='v007'");

        let est =
            estimate_build_rows_via_emat_stats(filter.as_ref()).expect("estimate must resolve");
        assert!(
            est <= 60,
            "NDV-corrected estimate must be ~3000/150 = 20 (≤60 with rounding), got {est} — \
             the 0.2 fallback would give 600"
        );

        // Isolation: the scan's globally-visible stats must still carry an
        // Absent distinct_count (JoinSelection's view is untouched).
        let (_, column_stats, _) = find_emat_scan_stats(filter.as_ref()).unwrap();
        for cs in &column_stats {
            assert!(
                matches!(
                    cs.distinct_count,
                    datafusion::common::stats::Precision::Absent
                ),
                "global distinct_count must stay Absent (got {:?}) — populating it \
                 flips JoinSelection plans (the Σ.Q06.SF10.5.h regression)",
                cs.distinct_count
            );
        }
    }
}
