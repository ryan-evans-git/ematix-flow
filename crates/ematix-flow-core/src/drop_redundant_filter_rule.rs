//! Σ.AE — surgically drop FilterExec conjuncts that are already
//! handled exactly by the underlying `EmatixFastParquetExec`'s
//! `BridgeFilter`.
//!
//! ## Motivation
//!
//! When `EmatixFastParquetTableProvider::supports_filters_pushdown`
//! declares `Inexact`, DataFusion's planner keeps a `FilterExec`
//! above the scan that re-evaluates the same predicate `BridgeFilter`
//! already evaluated to build its bitmap. The redundancy shows up
//! in samply as a double Snappy-decompress on the predicate column
//! (once in `BridgeFilter::build_bitmap`, once in
//! `masked_decode_one_column` because the FilterExec's re-eval
//! requires the column to be projected through the scan).
//!
//! See `docs/PI_16_Q06_PROFILE.md` for the profile evidence.
//!
//! ## Why we don't just declare `Exact`
//!
//! `EMAT_EXACT_PUSHDOWN=1` exists and would solve this (line ~1835
//! of `ematix_fast_parquet.rs`) but a wholesale switch regresses
//! Q03 +144%, Q04 +100%, Q07 +59%, Q17 +43%, Q21 +124% at SF=10
//! (measured 2026-05-26 with `EMAT_EXACT_PUSHDOWN=1`). The
//! mechanism is that when FilterExec disappears upstream, downstream
//! physical optimizer rules — `InjectFilterSumRule`,
//! `InjectFilterMultiAggRule`, repartition placement, join-order
//! decisions — pattern-match on the `FilterExec → Scan` shape and
//! lose their fast paths. Declaring Exact at `supports_filters_pushdown`
//! time happens TOO EARLY — before those rules run.
//!
//! This rule runs LATE in the physical-optimizer pipeline. By then
//! every Inject* rule has had its chance to fire on the original
//! shape. Anything that's still a `FilterExec → EmatixFastParquetExec`
//! is, by definition, NOT eligible for any fast-path fusion — so
//! removing the FilterExec's redundant conjuncts is a pure win for
//! those leftover scans.
//!
//! ## Partial drop
//!
//! We do not require the FilterExec to be entirely redundant. We
//! walk its AND-conjuncts, match each against a `BridgeFilter`
//! predicate that is both `is_exact_safe()` and whose column has no
//! nulls, and remove just those matched conjuncts. The remaining
//! conjuncts (e.g. F64 predicates that BridgeFilter refuses to
//! push) keep the FilterExec alive. This minimises the surface
//! that downstream Inject*-style assumptions could break on.
//!
//! ## Status — SPIKE COMPLETE, RULE DOESN'T FIRE
//!
//! 22q SF=10 trace (2026-05-26): **zero rule fires across all 22
//! queries**. The reason: `InjectFilterSumRule` and
//! `InjectFilterMultiAggRule` consume every `Aggregate → FilterExec
//! → EmatixFastParquetExec` pattern BEFORE this rule runs. The
//! resulting physical plan is `FusedAggregateExec → Scan` — no
//! FilterExec for our late rule to drop.
//!
//! ## Architectural finding from the spike
//!
//! The Π.16 double-Snappy on Q06's l_shipdate isn't from FilterExec
//! re-evaluating. It's from the SCAN PROJECTING the filter-only
//! columns so the FUSED KERNEL above can evaluate the predicate:
//!
//! ```text
//! FusedAggregateExec<FilterSumSpec>   # ← INSIDE: reads all 4 cols,
//!   EmatixFastParquetExec(projection=[                    re-evals preds, sums
//!     l_quantity,      # filter-only — PROJECTED so kernel can read
//!     l_extendedprice, # in SELECT
//!     l_discount,      # in WHERE + SELECT
//!     l_shipdate,      # filter-only — PROJECTED so kernel can read
//!   ])
//! ```
//!
//! Eliminating the redundant Snappy decompress of `l_shipdate` /
//! `l_quantity` requires ALL of:
//!
//! 1. Declare Exact pushdown for the predicate (so the kernel above
//!    doesn't re-eval),
//! 2. Teach `InjectFilterSumRule` to ALSO match the Exact-shape
//!    (`Aggregate → Scan` with predicate on the scan), so the fused
//!    kernel still fires,
//! 3. Let DataFusion's projection pruning drop the filter-only
//!    columns from the scan's output.
//!
//! `EMAT_EXACT_PUSHDOWN=1` does (1) and (3) but skips (2). That's
//! why Q06 wins under Exact (the FilterSumRule pattern was already
//! Exact-aware per the 2026-05-19 docstring at line 1369-1375) but
//! Q03/Q07/Q17/Q21 regress catastrophically — those don't have the
//! FilterSumRule Exact-shape, so they fall off the fast path.
//!
//! See `docs/PI_16_Q06_PROFILE.md` for the full profile evidence.
//!
//! ## Next direction (if pursued)
//!
//! The architectural fix is **per-rule Exact-shape support across
//! all Inject* rules** — make each one match `Aggregate → Scan(pred)`
//! in addition to `Aggregate → FilterExec → Scan`. That's a
//! per-rule investment with its own correctness audit. The 4 ms
//! recoverable on Q06 doesn't justify the cross-cutting change
//! without a broader analysis of which queries actually benefit.
//!
//! Spike rule kept as opt-in dead code for the day someone needs to
//! probe a `FilterExec → Scan` pattern (e.g. a custom physical
//! optimizer that doesn't go through Inject*).

use std::sync::Arc;

use datafusion::common::Result as DfResult;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_expr::utils::split_conjunction;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::scalar::ScalarValue;

use crate::ematix_fast_parquet::{ColumnPredicate, EmatixFastParquetExec};

/// Number of FilterExec→scan fusions `fuse_redundant_bridge_filters`
/// has performed this process. Lets the 22q A/B harness detect, per
/// query, whether the PV.M.7 path actually fired (so the geomean is
/// gated to queries that exercised it and inertness is asserted on the
/// rest) — the analog of `CSE_PARALLEL_POPULATES` for the Phase-2 lever.
pub static PV_M7_FUSES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Install the Σ.AE rule. Only registers the rule if
/// `EMAT_DROP_REDUNDANT_FILTER=1` is set in the environment at
/// builder time — keeps the rule out of the default optimizer
/// pipeline so the Inexact-pushdown regime stays unchanged.
pub fn install_drop_redundant_filter_rule(builder: SessionStateBuilder) -> SessionStateBuilder {
    if std::env::var_os("EMAT_DROP_REDUNDANT_FILTER").is_some() {
        builder.with_physical_optimizer_rule(Arc::new(DropRedundantBridgeFilterRule))
    } else {
        builder
    }
}

#[derive(Debug, Default)]
pub struct DropRedundantBridgeFilterRule;

impl PhysicalOptimizerRule for DropRedundantBridgeFilterRule {
    fn name(&self) -> &str {
        "ematix_flow_drop_redundant_bridge_filter"
    }

    fn schema_check(&self) -> bool {
        // We may drop the FilterExec entirely, returning the scan
        // directly. The scan's output schema equals the FilterExec's
        // output schema (FilterExec preserves schema), so the
        // overall plan schema is unchanged.
        true
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // RECONCILED (#308): delegate to the canonical
        // `fuse_redundant_bridge_filters` so there is ONE audited
        // implementation. That helper supersedes this rule's original
        // spike body in two load-bearing ways: (1) it checks
        // `column_has_no_nulls` (the masked-decode kernels carry no
        // def-levels, so dropping a range predicate on a nullable
        // column could leak NULLs SQL range semantics reject — the old
        // body skipped this), and (2) on a full drop it projection-PRUNES
        // the filter-only column out of the scan's decode projection
        // (the actual win — avoids the double Snappy-decompress), where
        // the old body returned the bare scan and silently widened the
        // schema. It is full-drop-only (no partial-drop / leftover
        // rebuild), which is strictly safer: a leftover conjunct leaves
        // the whole FilterExec untouched. This rule stays opt-in
        // (`EMAT_DROP_REDUNDANT_FILTER=1`) for probing a *direct*
        // `FilterExec → EmatixScan` (not CSE-sealed); the default path is
        // the DedupeAgg call site. Trace via `EMAT_CSE_FILTER_FUSION_TRACE`.
        fuse_redundant_bridge_filters(plan)
    }
}

/// PV.M.7 — projection-aware, null-safe masked-fusion of a
/// `FilterExec → EmatixFastParquetExec` subtree, callable from a
/// host rule that has the **unwrapped** subtree in hand (the CSE
/// `SharedSubtreeExec` seals its child off, so the standalone
/// `DropRedundantBridgeFilterRule` can never reach Q15's revenue
/// scan — `DedupeAggregateForFloatDeterminism` calls this on `node`
/// BEFORE wrapping).
///
/// Fixes two gaps in the spike rule above: (1) checks
/// `column_has_no_nulls` (the masked-decode kernels carry no
/// def-levels, so dropping a range predicate on a nullable column
/// could leak NULLs that SQL range semantics reject); (2) preserves
/// the `FilterExec`'s projection by leaving a `ProjectionExec` in
/// place (the spike returned the bare scan, which silently widened
/// the output schema when the FilterExec projected filter-only
/// columns away — Q15's FilterExec drops `l_shipdate`).
///
/// Conservative: only fuses when EVERY conjunct is covered by an
/// `is_exact_safe()` + null-free BridgeFilter predicate (full drop).
/// A leftover conjunct (e.g. an F64 range, or a nullable column)
/// leaves the whole FilterExec untouched — never a partial drop,
/// so there is no residual-vs-dropped double-count to reason about.
pub fn fuse_redundant_bridge_filters(
    plan: Arc<dyn ExecutionPlan>,
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let trace = std::env::var_os("EMAT_CSE_FILTER_FUSION_TRACE").is_some();
    let out = plan.transform_up(|node| {
        let Some(filter) = node.as_any().downcast_ref::<FilterExec>() else {
            return Ok(Transformed::no(node));
        };
        let input = filter.input();
        // Look through transparent single-child wrappers (CooperativeExec from
        // EnsureCooperative, CoalesceBatchesExec) that may sit between the
        // FilterExec and the scan depending on physical-rule ordering. They
        // preserve schema + row count, so the FilterExec's projection indices
        // still address the scan's output columns.
        let Some((scan_node, wrappers)) = peel_to_emat_scan(input) else {
            return Ok(Transformed::no(node));
        };
        let scan = scan_node
            .as_any()
            .downcast_ref::<EmatixFastParquetExec>()
            .expect("peel_to_emat_scan guarantees an EmatixFastParquetExec");
        let Some(bridge) = scan.filter() else {
            return Ok(Transformed::no(node));
        };
        let conjuncts: Vec<Arc<dyn PhysicalExpr>> = split_conjunction(filter.predicate())
            .into_iter()
            .cloned()
            .collect();
        if conjuncts.is_empty() {
            return Ok(Transformed::no(node));
        }
        // Every conjunct must be covered by an exact-safe bridge predicate
        // on a NULL-FREE column, else bail (no partial drop). The null-free
        // requirement is load-bearing: the fused path drops the residual
        // FilterExec that would otherwise re-reject NULLs the masked-decode
        // kernels (no def-levels) could leak. `column_has_no_nulls` is
        // file-schema-indexed, matching the predicate's `col_idx`.
        let no_nulls = scan.column_has_no_nulls();
        let all_covered = conjuncts.iter().all(|c| {
            matches!(
                match_against_bridge(c, bridge, scan),
                Some(p) if p.is_exact_safe()
                    && no_nulls.get(p.col_idx()).copied().unwrap_or(false)
            )
        });
        if !all_covered {
            if trace {
                eprintln!(
                    "[PV.M.7] skip: not-all-covered conjuncts={} bridge_preds={}",
                    conjuncts.len(),
                    bridge.predicates().len()
                );
            }
            return Ok(Transformed::no(node));
        }
        // Full drop. THE WIN is pruning the filter-only column out of the
        // scan's DECODE projection (so the predicate column is decoded once
        // for the bitmap, not a second time to materialise it through the
        // scan's output) — NOT merely dropping the FilterExec. Rebuild the
        // scan via `try_new` with its output narrowed to exactly the
        // FilterExec's kept columns; the BridgeFilter keeps referencing the
        // now-unprojected predicate column, which the decode path reads for
        // the bitmap only (the EXACT-pushdown shape).
        let result: Arc<dyn ExecutionPlan> = match filter.projection().as_ref() {
            // v1 declines scans carrying a runtime sideband (L9 resolves it
            // on first poll); CSE'd revenue subtrees don't have one.
            Some(proj) if scan.runtime_sideband().is_none() => {
                let keep: Vec<usize> = proj.to_vec(); // output indices into scan schema
                let new_projection: Vec<usize> =
                    keep.iter().map(|&j| scan.projection()[j]).collect();
                let new_schema = Arc::new(input.schema().project(&keep)?);
                let new_decode = Arc::new(scan.decode_schema_ref().project(&keep)?);
                let new_stats: Vec<_> = keep
                    .iter()
                    .map(|&j| scan.column_stats()[j].clone())
                    .collect();
                let pruned = EmatixFastParquetExec::try_new(
                    scan.path().to_string(),
                    new_schema,
                    new_decode,
                    scan.file_schema().clone(),
                    new_projection,
                    scan.assignments().to_vec(),
                    scan.num_rows(),
                    scan.rg_num_rows_arc().clone(),
                    scan.filter().cloned(),
                    scan.exec_late_mat(),
                    scan.exec_streaming_arrow_reader(),
                    new_stats,
                    Arc::new(scan.column_has_no_nulls().to_vec()),
                )?;
                // Re-apply the transparent wrappers over the pruned scan.
                rewrap_transparent(Arc::new(pruned), &wrappers)?
            }
            // Sideband present (rare here) — keep the safe ProjectionExec
            // fallback: drops the FilterExec but not the double-decompress.
            Some(proj) => {
                let child_schema = input.schema();
                let exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = proj
                    .iter()
                    .map(|p| {
                        let f = child_schema.field(*p);
                        (
                            Arc::new(Column::new(f.name(), *p)) as Arc<dyn PhysicalExpr>,
                            f.name().to_string(),
                        )
                    })
                    .collect();
                Arc::new(ProjectionExec::try_new(exprs, input.clone())?)
            }
            // No projection: scan already outputs exactly what's needed.
            None => input.clone(),
        };
        PV_M7_FUSES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if trace {
            let pruned = scan.runtime_sideband().is_none() && filter.projection().is_some();
            eprintln!(
                "[PV.M.7] FUSED: dropped FilterExec ({} conjuncts) → {}",
                conjuncts.len(),
                if pruned {
                    "projection-pruned masked scan"
                } else if filter.projection().is_some() {
                    "masked scan + ProjectionExec (sideband fallback)"
                } else {
                    "masked scan"
                }
            );
        }
        Ok(Transformed::yes(result))
    })?;
    Ok(out.data)
}

/// Peel transparent single-child wrappers (`CooperativeExec` from
/// `EnsureCooperative`, `CoalesceBatchesExec`) off `input` to reach the
/// `EmatixFastParquetExec`. Returns the scan node plus the wrappers
/// (top-to-bottom) to re-apply over a rewritten scan. Both wrappers preserve
/// schema + row count, so looking through them is safe and the FilterExec's
/// projection indices still address the scan's output columns.
#[allow(clippy::type_complexity)]
fn peel_to_emat_scan(
    input: &Arc<dyn ExecutionPlan>,
) -> Option<(Arc<dyn ExecutionPlan>, Vec<Arc<dyn ExecutionPlan>>)> {
    let mut wrappers: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
    let mut cur = input.clone();
    loop {
        if cur
            .as_any()
            .downcast_ref::<EmatixFastParquetExec>()
            .is_some()
        {
            return Some((cur, wrappers));
        }
        let transparent = matches!(cur.name(), "CooperativeExec" | "CoalesceBatchesExec");
        if transparent && cur.children().len() == 1 {
            let child = cur.children()[0].clone();
            wrappers.push(cur);
            cur = child;
        } else {
            return None;
        }
    }
}

/// Re-apply `wrappers` (outermost-first) over `inner` via `with_new_children`.
fn rewrap_transparent(
    inner: Arc<dyn ExecutionPlan>,
    wrappers: &[Arc<dyn ExecutionPlan>],
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let mut cur = inner;
    for w in wrappers.iter().rev() {
        cur = Arc::clone(w).with_new_children(vec![cur])?;
    }
    Ok(cur)
}

/// Try to recognize `physical_expr` as a known `ColumnPredicate`
/// shape that the BridgeFilter would have parsed. Returns the
/// matching predicate from BridgeFilter if found, else None.
///
/// We only handle the two shapes that map cleanly:
/// - `Column op Literal` — matches `I32Range` (single clause) or
///   `F64Range`. Since we ALWAYS skip F64Range here (its
///   `is_exact_safe()` is false), this effectively matches the i32
///   range case.
/// - `Column = Literal-utf8` / `Column != Literal-utf8` — matches
///   `StringEq`/`StringNotEq`.
fn match_against_bridge<'a>(
    physical_expr: &Arc<dyn PhysicalExpr>,
    bridge: &'a crate::ematix_fast_parquet::BridgeFilter,
    scan: &EmatixFastParquetExec,
) -> Option<&'a ColumnPredicate> {
    let bin = physical_expr.as_any().downcast_ref::<BinaryExpr>()?;
    // Identify the column side and the literal side.
    let (col_expr, lit_expr, op) = match (
        bin.left().as_any().downcast_ref::<Column>(),
        bin.right().as_any().downcast_ref::<Literal>(),
    ) {
        (Some(c), Some(l)) => (c, l, *bin.op()),
        _ => match (
            bin.left().as_any().downcast_ref::<Literal>(),
            bin.right().as_any().downcast_ref::<Column>(),
        ) {
            // Flipped form: `Literal op Column`. Mirror the operator.
            (Some(l), Some(c)) => (c, l, flip_op(*bin.op())?),
            _ => return None,
        },
    };
    let col_idx = scan_col_idx(scan, col_expr)?;
    // Iterate bridge predicates looking for one that matches
    // (col_idx, op, literal).
    for p in bridge.predicates() {
        if p.col_idx() != col_idx {
            continue;
        }
        if predicate_matches(p, op, lit_expr.value()) {
            return Some(p);
        }
    }
    None
}

/// Map FilterExec's `Column` (a projected-schema column) back to the
/// file-schema column index used by `BridgeFilter` predicates.
fn scan_col_idx(scan: &EmatixFastParquetExec, col: &Column) -> Option<usize> {
    // The FilterExec sees the scan's OUTPUT schema (post-projection).
    // We need the file-schema index that the BridgeFilter uses.
    // `scan.projection()` maps proj_idx → file_idx.
    let proj = scan.projection();
    proj.get(col.index()).copied()
}

fn flip_op(op: Operator) -> Option<Operator> {
    Some(match op {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        Operator::Eq => Operator::Eq,
        Operator::NotEq => Operator::NotEq,
        _ => return None,
    })
}

/// Does the `BridgeFilter` predicate cover `(op, literal)` on the
/// matching column?
fn predicate_matches(p: &ColumnPredicate, op: Operator, literal: &ScalarValue) -> bool {
    match p {
        ColumnPredicate::I32Range { clauses, .. } => clauses.iter().any(|c| {
            c.op == op
                && match literal {
                    ScalarValue::Int32(Some(v)) => *v == c.literal_i32,
                    ScalarValue::Date32(Some(v)) => *v == c.literal_i32,
                    _ => false,
                }
        }),
        ColumnPredicate::I32In { values, .. } => {
            if op != Operator::Eq {
                return false;
            }
            match literal {
                ScalarValue::Int32(Some(v)) => values.iter().any(|x| x == v),
                _ => false,
            }
        }
        ColumnPredicate::StringEq { value, .. } => {
            if op != Operator::Eq {
                return false;
            }
            literal_str_eq(literal, value)
        }
        ColumnPredicate::StringNotEq { value, .. } => {
            if op != Operator::NotEq {
                return false;
            }
            literal_str_eq(literal, value)
        }
        // Other shapes intentionally not matched: F64Range (not
        // exact-safe), I32ColumnPair (binary on two columns, not
        // Column op Literal), I64InBloom/Set/Range (inexact by
        // design), StringIn (would require multi-conjunct matching),
        // StringLike (LikeMatcher-specific).
        _ => false,
    }
}

fn literal_str_eq(literal: &ScalarValue, target: &str) -> bool {
    match literal {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::Utf8View(Some(s))
        | ScalarValue::LargeUtf8(Some(s)) => s == target,
        _ => false,
    }
}

#[cfg(test)]
mod pv_m7_tests {
    use super::*;
    use crate::ematix_fast_parquet::EmatixFastParquetTableProvider;
    use datafusion::arrow::array::{Array, Float64Array};
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::physical_plan::{collect, displayable};
    use datafusion::prelude::{SessionConfig, SessionContext};
    use std::path::PathBuf;

    fn sf1_lineitem() -> Option<PathBuf> {
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let p = manifest
            .parent()
            .and_then(|p| p.parent())
            .map(|p| p.join("examples/tpch/data/sf1/lineitem.parquet"))?;
        p.exists().then_some(p)
    }

    fn ptext(p: &Arc<dyn ExecutionPlan>) -> String {
        format!("{}", displayable(p.as_ref()).indent(true))
    }

    async fn sum_col0(plan: Arc<dyn ExecutionPlan>, ctx: &SessionContext) -> f64 {
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        let mut s = 0.0f64;
        for b in &batches {
            let c = b.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
            for i in 0..c.len() {
                if c.is_valid(i) {
                    s += c.value(i);
                }
            }
        }
        s
    }

    /// PV.M.7 — the helper drops a fully-covered i32-range FilterExec and
    /// rebuilds the scan with the filter-only column pruned from the decode
    /// projection (output narrows from 3 → 2 cols), with a byte-identical sum.
    #[tokio::test(flavor = "multi_thread")]
    async fn fuses_i32_range_filter_into_pruned_scan() {
        let Some(path) = sf1_lineitem() else {
            eprintln!("PV.M.7 test skipped: examples/tpch/data/sf1/lineitem.parquet absent");
            return;
        };
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(4))
            .with_default_features()
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table(
            "lineitem",
            Arc::new(EmatixFastParquetTableProvider::try_new(path.to_string_lossy()).unwrap()),
        )
        .unwrap();
        let plan = ctx
            .sql(
                "SELECT l_extendedprice, l_discount FROM lineitem \
                 WHERE l_shipdate >= DATE '1996-01-01' AND l_shipdate < DATE '1996-04-01'",
            )
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();

        // Precondition: an Inexact FilterExec sits over the Ematix scan.
        assert!(
            ptext(&plan).contains("FilterExec"),
            "expected a FilterExec to fuse:\n{}",
            ptext(&plan)
        );
        let before = sum_col0(plan.clone(), &ctx).await;

        let fused = fuse_redundant_bridge_filters(plan).unwrap();
        let txt = ptext(&fused);
        assert!(
            !txt.contains("FilterExec"),
            "FilterExec must be dropped:\n{txt}"
        );
        assert!(
            txt.contains("EmatixFastParquetExec"),
            "the scan must remain:\n{txt}"
        );
        // Filter-only l_shipdate pruned from the scan's decode projection →
        // output is exactly [l_extendedprice, l_discount].
        assert_eq!(
            fused.schema().fields().len(),
            2,
            "output must narrow to [extprice, discount]:\n{txt}"
        );

        // Correctness: same SUM value (no rows gained/lost, no NULL leak).
        // Relative-epsilon, not byte-identical: the original (FilterExec) and
        // fused (pruned-scan) plans legitimately emit survivors in a different
        // partition/batch order, so the f64 accumulation differs in the low
        // bits. Production Q15 IS byte-identical because it carries the
        // DedupeAggregateForFloatDeterminism sort-then-sum wrapper; this raw
        // standalone sum does not, so the order is free.
        let after = sum_col0(fused, &ctx).await;
        let rel = (before - after).abs() / before.abs().max(1.0);
        assert!(
            rel < 1e-9,
            "fusion changed the result beyond float-order noise: {before} vs {after} (rel {rel:.2e})"
        );
    }
}
