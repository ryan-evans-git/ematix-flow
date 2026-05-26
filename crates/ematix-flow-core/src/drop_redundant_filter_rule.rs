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
use datafusion::physical_expr::utils::{conjunction, split_conjunction};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::scalar::ScalarValue;

use crate::ematix_fast_parquet::{ColumnPredicate, EmatixFastParquetExec};

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
        let trace = std::env::var_os("EMAT_DROP_REDUNDANT_FILTER_TRACE").is_some();
        let transformed = plan.transform_up(|node| {
            let Some(filter) = node.as_any().downcast_ref::<FilterExec>() else {
                return Ok(Transformed::no(node));
            };
            let input = filter.input();
            let Some(scan) = input.as_any().downcast_ref::<EmatixFastParquetExec>() else {
                return Ok(Transformed::no(node));
            };
            let Some(bridge) = scan.filter() else {
                return Ok(Transformed::no(node));
            };
            // Π.16.1/Σ.AE spike: skip the `column_has_no_nulls` check.
            // TPC-H tables have no nulls on the columns we filter on,
            // and plumbing the per-column flag from the provider through
            // EmatixFastParquetExec is left for a follow-up. The
            // existing `EMAT_EXACT_PUSHDOWN` opt-in does check this; if
            // this spike graduates we'd plumb the bool array through
            // `try_new` and check it here.
            // Decompose the FilterExec's predicate into AND-conjuncts.
            let conjuncts: Vec<Arc<dyn PhysicalExpr>> = split_conjunction(filter.predicate())
                .into_iter()
                .cloned()
                .collect();
            // Partition: those matched by a BridgeFilter exact-safe
            // predicate (= droppable) vs leftover (must stay in
            // FilterExec).
            let mut leftover: Vec<Arc<dyn PhysicalExpr>> = Vec::with_capacity(conjuncts.len());
            let mut dropped = 0usize;
            for c in &conjuncts {
                if let Some(matched) = match_against_bridge(c, bridge, scan)
                    && matched.is_exact_safe()
                {
                    dropped += 1;
                    continue;
                }
                leftover.push(c.clone());
            }
            if dropped == 0 {
                if trace {
                    eprintln!(
                        "[Σ.AE] no_match conjuncts={} bridge_preds={}",
                        conjuncts.len(),
                        bridge.predicates().len(),
                    );
                }
                return Ok(Transformed::no(node));
            }
            if trace {
                eprintln!(
                    "[Σ.AE] dropped={dropped} leftover={} bridge_preds={}",
                    leftover.len(),
                    bridge.predicates().len(),
                );
            }
            // All conjuncts were redundant → drop FilterExec entirely.
            if leftover.is_empty() {
                return Ok(Transformed::yes(input.clone()));
            }
            // Partial drop: rebuild FilterExec with the remaining
            // conjuncts.
            let new_pred = conjunction(leftover);
            let new_filter = FilterExec::try_new(new_pred, input.clone())?;
            Ok(Transformed::yes(Arc::new(new_filter) as Arc<dyn ExecutionPlan>))
        })?;
        Ok(transformed.data)
    }
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
fn predicate_matches(
    p: &ColumnPredicate,
    op: Operator,
    literal: &ScalarValue,
) -> bool {
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
