//! Σ.F — declarative shape catalog for the physical-plan optimiser.
//!
//! v1 surface. The catalog itself (entries + dispatcher) lands in a
//! follow-up bite; this module ships the substrate — the `Shape` AST,
//! its named captures, and `Shape::try_match` against a DataFusion
//! `ExecutionPlan` tree.
//!
//! Goal: replace the per-rule plan walkers (`dict_filter_rule`,
//! `dict_aggregate_rule`, `fused_aggregate_filter_sum_rule`,
//! `fused_aggregate_filter_multi_agg_rule`) with a single declarative
//! catalog where each entry is a `Shape` + a rewriter fn. See
//! `docs/PHASE_SIGMA_F_SHAPE_CATALOG.md`.
//!
//! Design points worth knowing:
//!
//! - **Named captures**. Each `Shape` variant carries an optional
//!   `&'static str` capture name. A successful match populates a
//!   `HashMap<&'static str, Arc<dyn ExecutionPlan>>` that the
//!   rewriter reads to pull out the specific nodes it needs (e.g.
//!   the `FilterExec`, the `AggregateExec(Final*)`, the `body`).
//! - **No DSL macro yet**. The `shape!` macro from the plan doc is
//!   sugar over these builder functions. The macro lands once the
//!   AST is proven by migrating at least one real rule.
//! - **Optional wrappers**. DataFusion occasionally elides
//!   `CoalescePartitionsExec` (when input is already single-partition)
//!   and inserts `ProjectionExec` for CSE. The `optional(...)` shape
//!   matches whether the wrapper is present or skipped.
//! - **Structural match only**. The matcher checks operator class +
//!   coarse attributes (`AggregateMode`, group-by arity, agg arity).
//!   Predicate-shape inspection, scan-column type checking, and
//!   ScalarValue extraction stay in the per-rule rewriter — they're
//!   too domain-specific to live in the matcher AST.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_expr::Partitioning;

/// Aggregate-mode filter used by [`Shape::Aggregate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggMode {
    Final,
    FinalPartitioned,
    Partial,
}

impl AggMode {
    fn matches(self, mode: AggregateMode) -> bool {
        matches!(
            (self, mode),
            (AggMode::Final, AggregateMode::Final)
                | (AggMode::FinalPartitioned, AggregateMode::FinalPartitioned)
                | (AggMode::Partial, AggregateMode::Partial)
        )
    }
}

/// Pattern AST. Built via the helper constructors below; future work
/// adds a `shape!` macro that desugars to these.
#[derive(Debug, Clone)]
pub enum Shape {
    /// Match any node; capture optionally. Terminates a pattern at the
    /// "body" — the subtree the rule treats as input to the rewrite.
    Any { capture: Option<&'static str> },
    /// `FilterExec` with the given children matched in order.
    Filter {
        capture: Option<&'static str>,
        children: Vec<Shape>,
    },
    /// `AggregateExec` with the given mode and arity constraints. A
    /// `None` arity means "don't care".
    Aggregate {
        capture: Option<&'static str>,
        mode: AggMode,
        group_by: Option<usize>,
        aggs: Option<usize>,
        children: Vec<Shape>,
    },
    /// `ProjectionExec` with the given children.
    Projection {
        capture: Option<&'static str>,
        children: Vec<Shape>,
    },
    /// `CoalescePartitionsExec` with the given children.
    CoalescePartitions {
        capture: Option<&'static str>,
        children: Vec<Shape>,
    },
    /// `RepartitionExec` whose partitioning is `Partitioning::Hash(..)`.
    /// Matches any number of expressions in the hash key.
    RepartitionHash {
        capture: Option<&'static str>,
        children: Vec<Shape>,
    },
    /// Wrap an `inner` shape under an `optional` outer wrapper. Tries
    /// `wrapper(inner)` first; on miss, falls back to matching `inner`
    /// directly. The wrapper's capture (if any) is populated only when
    /// the wrapper is present.
    Optional {
        wrapper: Box<Shape>,
        inner: Box<Shape>,
    },
}

/// Builder helpers — what the `shape!` macro will desugar to.

pub fn any() -> Shape {
    Shape::Any { capture: None }
}

pub fn any_capture(name: &'static str) -> Shape {
    Shape::Any { capture: Some(name) }
}

pub fn filter(capture: Option<&'static str>, children: Vec<Shape>) -> Shape {
    Shape::Filter { capture, children }
}

pub fn aggregate(
    capture: Option<&'static str>,
    mode: AggMode,
    group_by: Option<usize>,
    aggs: Option<usize>,
    children: Vec<Shape>,
) -> Shape {
    Shape::Aggregate {
        capture,
        mode,
        group_by,
        aggs,
        children,
    }
}

pub fn projection(capture: Option<&'static str>, children: Vec<Shape>) -> Shape {
    Shape::Projection { capture, children }
}

pub fn coalesce_partitions(capture: Option<&'static str>, children: Vec<Shape>) -> Shape {
    Shape::CoalescePartitions { capture, children }
}

pub fn repartition_hash(capture: Option<&'static str>, children: Vec<Shape>) -> Shape {
    Shape::RepartitionHash { capture, children }
}

pub fn optional(wrapper: Shape, inner: Shape) -> Shape {
    Shape::Optional {
        wrapper: Box::new(wrapper),
        inner: Box::new(inner),
    }
}

/// Result of a successful match. The rewriter reads
/// `captures` by name to extract specific matched nodes.
#[derive(Debug, Clone)]
pub struct MatchedSubtree {
    /// The plan node the match started at.
    pub root: Arc<dyn ExecutionPlan>,
    /// Captured nodes, keyed by the `capture` name in the `Shape`.
    pub captures: HashMap<&'static str, Arc<dyn ExecutionPlan>>,
}

impl MatchedSubtree {
    pub fn get(&self, name: &'static str) -> Option<&Arc<dyn ExecutionPlan>> {
        self.captures.get(name)
    }
}

impl Shape {
    /// Try to match this pattern against `plan`. Returns
    /// `Some(MatchedSubtree)` with populated captures on match,
    /// `None` if any operator class or arity diverges.
    pub fn try_match(&self, plan: &Arc<dyn ExecutionPlan>) -> Option<MatchedSubtree> {
        let mut captures = HashMap::new();
        if self.match_node(plan, &mut captures) {
            Some(MatchedSubtree {
                root: plan.clone(),
                captures,
            })
        } else {
            None
        }
    }

    fn match_node(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        captures: &mut HashMap<&'static str, Arc<dyn ExecutionPlan>>,
    ) -> bool {
        match self {
            Shape::Any { capture } => {
                capture_if(captures, *capture, plan);
                true
            }
            Shape::Filter { capture, children } => {
                if plan.as_any().downcast_ref::<FilterExec>().is_none() {
                    return false;
                }
                if !match_children(children, plan, captures) {
                    return false;
                }
                capture_if(captures, *capture, plan);
                true
            }
            Shape::Aggregate {
                capture,
                mode,
                group_by,
                aggs,
                children,
            } => {
                let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() else {
                    return false;
                };
                if !mode.matches(*agg.mode()) {
                    return false;
                }
                if let Some(g) = group_by {
                    if agg.group_expr().expr().len() != *g {
                        return false;
                    }
                }
                if let Some(a) = aggs {
                    if agg.aggr_expr().len() != *a {
                        return false;
                    }
                }
                if !match_children(children, plan, captures) {
                    return false;
                }
                capture_if(captures, *capture, plan);
                true
            }
            Shape::Projection { capture, children } => {
                if plan.as_any().downcast_ref::<ProjectionExec>().is_none() {
                    return false;
                }
                if !match_children(children, plan, captures) {
                    return false;
                }
                capture_if(captures, *capture, plan);
                true
            }
            Shape::CoalescePartitions { capture, children } => {
                if plan
                    .as_any()
                    .downcast_ref::<CoalescePartitionsExec>()
                    .is_none()
                {
                    return false;
                }
                if !match_children(children, plan, captures) {
                    return false;
                }
                capture_if(captures, *capture, plan);
                true
            }
            Shape::RepartitionHash { capture, children } => {
                let Some(rep) = plan.as_any().downcast_ref::<RepartitionExec>() else {
                    return false;
                };
                if !matches!(rep.partitioning(), Partitioning::Hash(_, _)) {
                    return false;
                }
                if !match_children(children, plan, captures) {
                    return false;
                }
                capture_if(captures, *capture, plan);
                true
            }
            Shape::Optional { wrapper, inner } => {
                // First try the wrapper. If it matches, captures already
                // hold its bindings — no need to also try the bare inner.
                // Snapshot captures so a failed wrapper attempt doesn't
                // pollute the map.
                let snapshot: HashMap<&'static str, Arc<dyn ExecutionPlan>> = captures.clone();
                if wrapper.match_node(plan, captures) {
                    return true;
                }
                *captures = snapshot;
                inner.match_node(plan, captures)
            }
        }
    }
}

fn capture_if(
    captures: &mut HashMap<&'static str, Arc<dyn ExecutionPlan>>,
    name: Option<&'static str>,
    plan: &Arc<dyn ExecutionPlan>,
) {
    if let Some(n) = name {
        captures.insert(n, plan.clone());
    }
}

/// Match `children` shapes against `plan.children()` in order. Returns
/// false if arities differ or any child mismatches.
fn match_children(
    children: &[Shape],
    plan: &Arc<dyn ExecutionPlan>,
    captures: &mut HashMap<&'static str, Arc<dyn ExecutionPlan>>,
) -> bool {
    let plan_children = plan.children();
    if plan_children.len() != children.len() {
        return false;
    }
    for (child_shape, child_plan) in children.iter().zip(plan_children.iter()) {
        let owned: Arc<dyn ExecutionPlan> = (*child_plan).clone();
        if !child_shape.match_node(&owned, captures) {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    async fn simple_int_ctx() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2, 3, 4, 5])) as _,
                Arc::new(Int64Array::from(vec![10i64, 20, 30, 40, 50])) as _,
            ],
        )
        .unwrap();
        let mem = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(mem)).unwrap();
        ctx
    }

    /// Plan with a `FilterExec`. The Filter > Any pattern must match
    /// at the FilterExec and capture its body.
    #[tokio::test]
    async fn filter_any_matches_filter_plan() {
        let ctx = simple_int_ctx().await;
        let physical = ctx
            .sql("SELECT * FROM t WHERE a > 2")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();

        // Walk down to find the FilterExec; SessionContext puts a
        // CoalesceBatchesExec / RepartitionExec / etc. on top, so the
        // shape matcher's job here is *not* to walk — it's the rule's
        // job (transform_up) to drive the walk. The matcher is a yes/no
        // function applied at each candidate node.
        let pattern = filter(Some("f"), vec![any_capture("body")]);
        let mut found_match = false;
        walk(&physical, &mut |node| {
            if pattern.try_match(node).is_some() {
                found_match = true;
            }
        });
        assert!(
            found_match,
            "Filter > Any should match the FilterExec in `SELECT * FROM t WHERE a > 2`"
        );
    }

    /// Filter > Any does NOT match a plan without a FilterExec.
    #[tokio::test]
    async fn filter_any_does_not_match_unfiltered_plan() {
        let ctx = simple_int_ctx().await;
        let physical = ctx
            .sql("SELECT * FROM t")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();

        let pattern = filter(Some("f"), vec![any_capture("body")]);
        let mut found_match = false;
        walk(&physical, &mut |node| {
            if pattern.try_match(node).is_some() {
                found_match = true;
            }
        });
        assert!(
            !found_match,
            "Filter > Any must not match a plan with no FilterExec"
        );
    }

    /// Aggregate(Final, group_by=1) > optional(Repartition, Aggregate(Partial))
    /// matches a GROUP BY plan. Asserts named captures are populated.
    #[tokio::test]
    async fn aggregate_with_optional_repartition_matches_group_by() {
        let ctx = simple_int_ctx().await;
        let physical = ctx
            .sql("SELECT a, SUM(b) FROM t GROUP BY a")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();

        // Shape:
        //   AggregateExec(Final*, group_by=1) capture="final"
        //     > optional(
        //         RepartitionHash > AggregateExec(Partial) capture="partial" > Any capture="body",
        //         AggregateExec(Partial) capture="partial" > Any capture="body",
        //       )
        let inner_partial = aggregate(
            Some("partial"),
            AggMode::Partial,
            Some(1),
            None,
            vec![any_capture("body")],
        );
        let wrapped = repartition_hash(None, vec![inner_partial.clone()]);
        let bridge = optional(wrapped, inner_partial);

        let final_final_partitioned = aggregate(
            Some("final"),
            AggMode::FinalPartitioned,
            Some(1),
            None,
            vec![bridge.clone()],
        );
        let final_plain = aggregate(
            Some("final"),
            AggMode::Final,
            Some(1),
            None,
            vec![bridge],
        );

        // Either Final or FinalPartitioned at the top — try both.
        let mut got: Option<MatchedSubtree> = None;
        walk(&physical, &mut |node| {
            if got.is_none() {
                got = final_final_partitioned
                    .try_match(node)
                    .or_else(|| final_plain.try_match(node));
            }
        });

        let m = got.expect("Aggregate(Final*)>Partial shape should match SUM(b) GROUP BY a");
        assert!(m.get("final").is_some(), "final agg captured");
        assert!(m.get("partial").is_some(), "partial agg captured");
        assert!(m.get("body").is_some(), "body captured");
    }

    fn walk(plan: &Arc<dyn ExecutionPlan>, f: &mut dyn FnMut(&Arc<dyn ExecutionPlan>)) {
        f(plan);
        for child in plan.children() {
            let owned: Arc<dyn ExecutionPlan> = (*child).clone();
            walk(&owned, f);
        }
    }
}
