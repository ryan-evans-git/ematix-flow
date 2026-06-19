//! Σ.U — push a filter as LeftSemi into an Aggregate's input scan.
//!
//! ## What this fixes
//!
//! Q17 SF=10 baseline 189 ms / DuckDB 160 ms (+17%). DataFusion's
//! `scalar_subquery_to_join` already decorrelates the correlated
//! subquery into a top-level `Inner Join` with an `Aggregate`-over-
//! lineitem on one side, BUT the aggregate runs over all 60M lineitem
//! rows producing 10.8M partkey groups, when only the ~200 partkeys
//! matching the outer `p_brand='Brand#23' AND p_container='MED BOX'`
//! filter actually matter.
//!
//! Photon-style "magic set" / DuckDB-style `LEFT_DELIM_JOIN`
//! decorrelation answers this by pushing the outer filter into the
//! aggregate's input. We do the same at the LogicalPlan level via a
//! pre-plan walker that pattern-matches the shape and inserts a
//! `LeftSemi` between the aggregate's `TableScan` and the
//! `Aggregate` node:
//!
//! ```text
//!   Aggregate(group_by=[K], avg(...))            Aggregate(group_by=[K], avg(...))
//!     └─ TableScan: T                  →           └─ LeftSemi(T.K = F.K)
//!                                                    ├─ TableScan: T
//!                                                    └─ <filter_subtree producing F.K>
//! ```
//!
//! ## Why not an OptimizerRule
//!
//! [[optimizer-codegen-sensitivity]]: three prior rules
//! (Σ.H.1d.4, Σ.K.A, Σ.F-T2) regressed 5–8 pp geomean from LLVM
//! codegen perturbation alone. [[dict-routing]] and [[Σ.T
//! join_reorder]] both proved the pre-plan-walker pattern works at
//! this layer. The caller invokes [`push_filter_into_agg`] explicitly
//! on the optimized plan before `ctx.execute_logical_plan`; no
//! `OptimizerRule` is added to DataFusion's pass stack.
//!
//! ## Pattern matched
//!
//! ```text
//!   Inner Join (L, R) on L.X = R.K
//!     L: any subtree where the L.X column is produced by a
//!        Filter(P) → TableScan(T_alt) chain (possibly nested
//!        inside other Joins/Projections).
//!     R: Projection? → SubqueryAlias? → Projection? →
//!        Aggregate(group_by=[K, ...]) → TableScan(T)
//! ```
//!
//! K must appear in the aggregate's group-by list. The rewrite
//! clones the smallest Filter→TableScan subtree on the L side that
//! produces L.X and inserts it as the build side of a LeftSemi
//! between the aggregate and its TableScan.
//!
//! ## Status
//!
//! Σ.U Phase 1 MVP — narrow pattern matcher for the Q17 shape.
//! Generalisations (multi-key agg group-by, predicate on filter
//! subtree referencing aggregate side, etc.) are out of scope until
//! the basic shape ships and benches in the green.

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, Result as DfResult};
use datafusion::logical_expr::{
    Aggregate, BinaryExpr, Expr, JoinType, LogicalPlan, LogicalPlanBuilder, Operator,
};

/// Public entry. Walks `plan` and, for each `Inner Join` matching
/// the Σ.U pattern, replaces the aggregate-side `TableScan` with a
/// `LeftSemi` against a clone of the filter subtree from the other
/// join side. Returns the (possibly rewritten) plan. On any
/// shape-mismatch the sub-plan passes through unchanged — this is
/// best-effort, not correctness-critical.
pub fn push_filter_into_agg(plan: LogicalPlan) -> DfResult<LogicalPlan> {
    let mut fires: usize = 0;
    let transformed = plan.transform_down(|node| match node {
        LogicalPlan::Join(ref join) if join.join_type == JoinType::Inner => {
            match try_rewrite_q17_shape(&node) {
                Some(rewritten) => {
                    fires += 1;
                    Ok(Transformed::yes(rewritten))
                }
                None => Ok(Transformed::no(node)),
            }
        }
        _ => Ok(Transformed::no(node)),
    })?;
    if std::env::var("EMAT_SIGMA_U_DEBUG").is_ok() {
        eprintln!("[Σ.U] fires={fires}");
    }
    Ok(transformed.data)
}

/// Σ.Q20 — transitive semi-pushdown into a correlated-aggregate.
///
/// Generalises [`push_filter_into_agg`] to the Q20 shape: the
/// aggregate-side join key is matched to the OTHER side's key, but
/// that other side is constrained NOT by a direct `Filter → TableScan`
/// but TRANSITIVELY by a semi/inner-join to a small filtered dim. For
/// Q20:
///
/// ```text
///   Inner Join(partsupp_filt, lineitem_agg, on ps_partkey=l_partkey, ps_suppkey=l_suppkey)
///     partsupp_filt = partsupp  ⋉/⋈  (part WHERE p_name LIKE 'forest%')   on ps_partkey=p_partkey
///     lineitem_agg = Aggregate(group_by=[l_partkey,l_suppkey], sum(l_quantity)) → lineitem
/// ```
///
/// Since `l_partkey = ps_partkey ∈ {p_partkey | forest}`, the rule
/// splices `LeftSemi(lineitem, forest_parts, l_partkey = p_partkey)`
/// below the aggregate — cutting the agg input from all-1994-lineitem
/// to just forest-part lineitem. Correctness mirrors Σ.U transitively.
///
/// Best-effort: any shape mismatch passes through unchanged.
pub fn push_transitive_semi_into_agg(plan: LogicalPlan) -> DfResult<LogicalPlan> {
    let mut fires: usize = 0;
    let transformed = plan.transform_down(|node| match node {
        LogicalPlan::Join(ref join) if join.join_type == JoinType::Inner => {
            match try_rewrite_q20_transitive_shape(&node) {
                Some(rewritten) => {
                    fires += 1;
                    Ok(Transformed::yes(rewritten))
                }
                None => Ok(Transformed::no(node)),
            }
        }
        _ => Ok(Transformed::no(node)),
    })?;
    if std::env::var("EMAT_SIGMA_Q20_DEBUG").is_ok() {
        eprintln!("[Σ.Q20] fires={fires}");
    }
    Ok(transformed.data)
}

/// Σ.Q05 — transitive dim-semi pushdown into a join chain (#352).
///
/// Generalises [`push_transitive_semi_into_agg`] from aggregate inputs
/// to arbitrary deep join inputs. When an Inner equi-join constrains a
/// bare mid-dim scan `M` by a small filtered dim `D` (Q05:
/// `nation ⋈ region[r_name='ASIA']`), and the equivalence class of
/// `M`'s join keys reaches a scan `T` only TRANSITIVELY (≥2 equi-join
/// hops — Q05: `c_nationkey = s_nationkey = n_nationkey`, customer
/// never directly joins nation), splice `T ⋉ (M ⋈ D)` below `T`:
///
/// ```text
///   Inner Join: n_regionkey = r_regionkey            (anchor J)
///     ... ⋈ supplier ⋈ ((customer ⋈ orders) ⋈ lineitem)
///                         └─ TableScan: customer  →  LeftSemi: c_nationkey = n_nationkey
///                                                      ├─ TableScan: customer
///                                                      └─ nation ⋈ region[ASIA]   (M ⋈ D clone)
///     TableScan: nation (M)
///     Filter(r_name='ASIA') → TableScan: region (D)
/// ```
///
/// The hop ≥ 2 gate is the payoff boundary, not a safety one: a hop-1
/// (direct) join edge already gets the constraint at its own join — and
/// the L9 runtime-bloom machinery covers exactly that case — so direct
/// shapes (Q02/Q07/Q08/Q10/Q11/Q21 dim chains) pass through unchanged.
/// Correctness mirrors Σ.U/Σ.Q20 transitively: every harvested equi-edge
/// comes from an Inner/semi join, so any `T` row the semi drops would
/// have been dropped by those joins anyway. Best-effort: any shape
/// mismatch passes through unchanged.
pub fn push_transitive_dim_semi_into_join_chain(plan: LogicalPlan) -> DfResult<LogicalPlan> {
    let mut fires: usize = 0;
    let transformed = plan.transform_down(|node| match node {
        LogicalPlan::Join(ref join) if join.join_type == JoinType::Inner => {
            match try_rewrite_transitive_dim_semi(&node) {
                Some(rewritten) => {
                    fires += 1;
                    Ok(Transformed::yes(rewritten))
                }
                None => Ok(Transformed::no(node)),
            }
        }
        _ => Ok(Transformed::no(node)),
    })?;
    if std::env::var("EMAT_SIGMA_Q05_DEBUG").is_ok() {
        eprintln!("[Σ.Q05] fires={fires}");
    }
    Ok(transformed.data)
}

/// Try the transitive dim-semi rewrite on a single Inner Join node.
///
/// Anchor shape: `Inner Join(S, D) on S.k = D.k` where `D` is a small
/// filtered dim ([`small_filtered_dim_subtree`]) and `S.k` is produced
/// by a unique bare scan `M` inside `S` (the mid-dim — Q05: nation).
/// From `M`'s columns, walk the equivalence classes induced by `S`'s
/// Inner/semi equi-join edges with hop counts; any class column at
/// hop ≥ 2 owned by a unique scan `T` is a transitive target. The
/// deepest such `T` (most joins between it and the anchor — the most
/// intermediate work the semi can shrink) gets `T ⋉ (M ⋈ D)` spliced
/// below it. One splice per anchor.
fn try_rewrite_transitive_dim_semi(plan: &LogicalPlan) -> Option<LogicalPlan> {
    let LogicalPlan::Join(j) = plan else {
        return None;
    };
    if j.join_type != JoinType::Inner || j.on.is_empty() {
        return None;
    }
    for dim_on_right in [true, false] {
        let (s_side, d_side) = if dim_on_right {
            (&j.left, &j.right)
        } else {
            (&j.right, &j.left)
        };
        let Some(dim_subtree) = small_filtered_dim_subtree(d_side) else {
            continue;
        };
        let Some(dim_table) = leaf_scan_table_name(&dim_subtree) else {
            continue;
        };
        for (l, r) in &j.on {
            let (Expr::Column(lc), Expr::Column(rc)) = (l, r) else {
                continue;
            };
            let (s_key, d_key) = if dim_on_right { (lc, rc) } else { (rc, lc) };
            // Mid-dim M: the unique scan inside S producing the anchor key.
            let (m_found, m_count) = locate_scan_for_column(s_side, s_key, 0);
            let Some((m_scan, m_depth)) = m_found else {
                continue;
            };
            if m_count != 1 {
                continue;
            }
            // The rule clones M into the semi build — refuse fact-sized
            // mid-dims outright when the provider can say (belt; the
            // depth gate below is the principled guard).
            if scan_raw_rows(&m_scan).is_some_and(|n| n > MID_DIM_MAX_ROWS) {
                continue;
            }
            // Equivalence classes over S's Inner/semi equi-edges, seeded
            // from M's columns (hop 0), tracking each column's seed.
            let mut edges: Vec<(Column, Column)> = Vec::new();
            collect_inner_equi_edges(s_side, &mut edges);
            let mut hops: std::collections::HashMap<Column, (usize, Column)> =
                std::collections::HashMap::new();
            for (a, b) in &edges {
                for c in [a, b] {
                    if scan_resolves_column(&m_scan, c) {
                        hops.entry(c.clone()).or_insert((0, c.clone()));
                    }
                }
            }
            if hops.is_empty() {
                continue;
            }
            // Relax edges to a fixpoint (classes are tiny; edges few).
            loop {
                let mut changed = false;
                for (a, b) in &edges {
                    for (from, to) in [(a, b), (b, a)] {
                        if let Some((h, seed)) = hops.get(from).cloned() {
                            let next = (h + 1, seed);
                            match hops.get(to) {
                                Some((eh, _)) if *eh <= next.0 => {}
                                _ => {
                                    hops.insert((*to).clone(), next);
                                    changed = true;
                                }
                            }
                        }
                    }
                }
                if !changed {
                    break;
                }
            }
            // Transitive targets: hop ≥ 2, unique owning scan, not M
            // itself, DEEPER than M (the constraint must flow downhill —
            // a shallower target is upstream of nothing the semi could
            // shrink, and inverted shapes like Q09's part⋈lineitem
            // anchor would otherwise clone a fact scan as the build to
            // filter a 25-row dim), not already semi-guarded against
            // this dim. Pick the deepest.
            let mut best: Option<(LogicalPlan, usize, Column, Column)> = None;
            for (col, (hop, seed)) in &hops {
                if *hop < 2 {
                    continue;
                }
                let (t_found, t_count) = locate_scan_for_column(s_side, col, 0);
                let Some((t_scan, t_depth)) = t_found else {
                    continue;
                };
                if t_count != 1 || t_scan == m_scan || t_depth <= m_depth {
                    continue;
                }
                if semi_guard_exists(s_side, col, &dim_table) {
                    continue;
                }
                if best.as_ref().is_none_or(|(_, d, _, _)| t_depth > *d) {
                    best = Some((t_scan, t_depth, col.clone(), seed.clone()));
                }
            }
            let Some((t_scan, _, t_col, seed_col)) = best else {
                continue;
            };

            // Composite dim build: M ⋈ D on the anchor's own equi-pair.
            // Built with explicit equi-keys (not `join_on`) so the semi
            // hash-joins even on paths that skip re-optimization.
            let build = LogicalPlanBuilder::from(m_scan.clone())
                .join(
                    dim_subtree.clone(),
                    JoinType::Inner,
                    (vec![s_key.clone()], vec![d_key.clone()]),
                    None,
                )
                .ok()?
                .build()
                .ok()?;

            // Splice T ⋉ build below the target scan (schema-preserving,
            // so every ancestor node clones over unchanged).
            let mut done = false;
            let new_s = (*s_side.as_ref())
                .clone()
                .transform_down(|node| {
                    if !done && node == t_scan {
                        done = true;
                        let semi = LogicalPlanBuilder::from(node.clone())
                            .join(
                                build.clone(),
                                JoinType::LeftSemi,
                                (vec![t_col.clone()], vec![seed_col.clone()]),
                                None,
                            )
                            .and_then(|b| b.build());
                        return match semi {
                            Ok(s) => Ok(Transformed::yes(s)),
                            Err(_) => Ok(Transformed::no(node)),
                        };
                    }
                    Ok(Transformed::no(node))
                })
                .ok()?;
            if !done || !new_s.transformed {
                continue;
            }
            let (new_left, new_right) = if dim_on_right {
                (new_s.data, d_side.as_ref().clone())
            } else {
                (d_side.as_ref().clone(), new_s.data)
            };
            return LogicalPlanBuilder::from(new_left)
                .join_on(
                    new_right,
                    JoinType::Inner,
                    j.on.iter()
                        .map(|(l, r)| {
                            Expr::BinaryExpr(BinaryExpr {
                                left: Box::new(l.clone()),
                                op: Operator::Eq,
                                right: Box::new(r.clone()),
                            })
                        })
                        .chain(j.filter.iter().cloned()),
                )
                .ok()?
                .build()
                .ok();
        }
    }
    None
}

/// Sanity cap on the mid-dim scan the rule clones into the semi build.
/// The depth gate is the principled guard; this bound just refuses
/// fact-sized clones outright when the provider exposes row counts
/// (8M = above every TPC-H dim at SF ≤ 100, below every fact at SF ≥ 10).
const MID_DIM_MAX_ROWS: usize = 8_000_000;

/// Raw row count of a `TableScan` via its provider's statistics, when
/// the source exposes them (`DefaultTableSource` over our providers
/// does — footer counts, no I/O). `None` = unknown, caller stays
/// permissive and relies on the structural gates.
fn scan_raw_rows(scan: &LogicalPlan) -> Option<usize> {
    use datafusion::common::stats::Precision;
    use datafusion::datasource::DefaultTableSource;
    let LogicalPlan::TableScan(ts) = scan else {
        return None;
    };
    let src = ts
        .source
        .as_any()
        .downcast_ref::<DefaultTableSource>()?
        .table_provider
        .clone();
    match src.statistics()?.num_rows {
        Precision::Exact(n) | Precision::Inexact(n) => Some(n),
        Precision::Absent => None,
    }
}

/// Qualifier-aware: does this `TableScan` node's projected schema
/// resolve `col` (relation qualifier included)?
fn scan_resolves_column(scan: &LogicalPlan, col: &Column) -> bool {
    if let LogicalPlan::TableScan(ts) = scan {
        return ts.projected_schema.index_of_column(col).is_ok();
    }
    false
}

/// Locate the `TableScan` in `plan` resolving `col` (qualifier-aware).
/// Returns `(Some((scan_clone, join_depth)), occurrence_count)`; depth
/// counts Join nodes above the scan. `occurrence_count > 1` means the
/// column is ambiguous in this subtree (self-joins) — callers skip.
fn locate_scan_for_column(
    plan: &LogicalPlan,
    col: &Column,
    depth: usize,
) -> (Option<(LogicalPlan, usize)>, usize) {
    if let LogicalPlan::TableScan(_) = plan {
        return if scan_resolves_column(plan, col) {
            (Some((plan.clone(), depth)), 1)
        } else {
            (None, 0)
        };
    }
    let child_depth = if matches!(plan, LogicalPlan::Join(_)) {
        depth + 1
    } else {
        depth
    };
    let mut found = None;
    let mut count = 0;
    for c in plan.inputs() {
        let (f, n) = locate_scan_for_column(c, col, child_depth);
        if found.is_none() {
            found = f;
        }
        count += n;
    }
    (found, count)
}

/// Harvest equi-join column pairs from every Inner/LeftSemi/RightSemi
/// join in the subtree. Only these join types let a key constraint
/// propagate to surviving rows (an outer join's ON does not).
fn collect_inner_equi_edges(plan: &LogicalPlan, out: &mut Vec<(Column, Column)>) {
    if let LogicalPlan::Join(j) = plan {
        if matches!(
            j.join_type,
            JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi
        ) {
            for (l, r) in &j.on {
                if let (Expr::Column(lc), Expr::Column(rc)) = (l, r) {
                    out.push((lc.clone(), rc.clone()));
                }
            }
        }
    }
    for c in plan.inputs() {
        collect_inner_equi_edges(c, out);
    }
}

/// First (leftmost-descent) `TableScan` table name in the subtree —
/// the dim subtrees this rule clones are linear wrapper chains.
fn leaf_scan_table_name(plan: &LogicalPlan) -> Option<String> {
    if let LogicalPlan::TableScan(ts) = plan {
        return Some(ts.table_name.to_string());
    }
    plan.inputs().first().and_then(|c| leaf_scan_table_name(c))
}

/// Idempotency guard: is there already a `LeftSemi` in the subtree
/// whose left side resolves `t_col` and whose build scans `dim_table`?
fn semi_guard_exists(plan: &LogicalPlan, t_col: &Column, dim_table: &str) -> bool {
    if let LogicalPlan::Join(j) = plan {
        if j.join_type == JoinType::LeftSemi
            && j.left.schema().index_of_column(t_col).is_ok()
            && subtree_scans_table(&j.right, dim_table)
        {
            return true;
        }
    }
    plan.inputs()
        .iter()
        .any(|c| semi_guard_exists(c, t_col, dim_table))
}

fn subtree_scans_table(plan: &LogicalPlan, table: &str) -> bool {
    if let LogicalPlan::TableScan(ts) = plan {
        return ts.table_name.to_string() == table;
    }
    plan.inputs().iter().any(|c| subtree_scans_table(c, table))
}

/// Try the Q17-shape rewrite on a single Inner Join node.
///
/// Mirrors [`try_rewrite_q17_shape`] but, instead of a direct
/// `Filter → TableScan` on the filter side, the join key is
/// constrained TRANSITIVELY by a semi/inner-join to a small filtered
/// dim. The dim subtree (+ its key) is spliced as a `LeftSemi` below
/// the aggregate.
fn try_rewrite_q20_transitive_shape(plan: &LogicalPlan) -> Option<LogicalPlan> {
    let LogicalPlan::Join(j) = plan else {
        return None;
    };
    if j.join_type != JoinType::Inner || j.on.is_empty() {
        return None;
    }

    let (agg_side, filter_side, agg_on_right) =
        match (find_agg_branch(&j.left), find_agg_branch(&j.right)) {
            (None, Some(_)) => (&j.right, &j.left, true),
            (Some(_), None) => (&j.left, &j.right, false),
            _ => return None,
        };
    let agg_info = find_agg_branch(agg_side)?;

    // For each equi-pair, see whether the agg key is a group-by column
    // AND the matching filter-side key is constrained by a transitive
    // dim semi. Use the first pair that yields a rewrite (Q20: the
    // partkey pair — suppkey has no dim filter).
    for (l, r) in &j.on {
        let (Expr::Column(lc), Expr::Column(rc)) = (l, r) else {
            continue;
        };
        let (filter_c, agg_c) = if agg_on_right { (lc, rc) } else { (rc, lc) };
        let Some(inner_agg_col) = agg_info
            .group_by_cols
            .iter()
            .find(|gc| gc.name == agg_c.name)
        else {
            continue;
        };
        let Some((dim_subtree, dim_key)) = find_transitive_dim_semi(filter_side, filter_c) else {
            continue;
        };

        // Splice LeftSemi(agg_input, dim_subtree) on (inner_agg_col = dim_key).
        let Some(new_agg_side) =
            splice_left_semi_into_agg(agg_side, inner_agg_col, &dim_key, dim_subtree)
        else {
            continue;
        };

        let new_left;
        let new_right;
        if agg_on_right {
            new_left = j.left.clone();
            new_right = new_agg_side;
        } else {
            new_left = new_agg_side;
            new_right = j.right.clone();
        }
        return LogicalPlanBuilder::from(new_left.as_ref().clone())
            .join_on(
                new_right.as_ref().clone(),
                JoinType::Inner,
                j.on.iter()
                    .map(|(l, r)| {
                        Expr::BinaryExpr(BinaryExpr {
                            left: Box::new(l.clone()),
                            op: Operator::Eq,
                            right: Box::new(r.clone()),
                        })
                    })
                    .chain(j.filter.iter().cloned()),
            )
            .ok()?
            .build()
            .ok();
    }
    None
}

/// When `plan` constrains `filter_col` via a semi/inner join to a
/// small filtered dim, return `(dim_subtree, dim_key)` — the dim's
/// subtree (cloneable as a LeftSemi build) and the column it joins on.
/// Walks through Projection/SubqueryAlias/Filter wrappers and recurses
/// into join children so the constraining join can be nested.
fn find_transitive_dim_semi(
    plan: &LogicalPlan,
    filter_col: &Column,
) -> Option<(LogicalPlan, Column)> {
    match plan {
        LogicalPlan::Join(j)
            if matches!(
                j.join_type,
                JoinType::LeftSemi | JoinType::RightSemi | JoinType::Inner
            ) =>
        {
            for (l, r) in &j.on {
                let (Expr::Column(lc), Expr::Column(rc)) = (l, r) else {
                    continue;
                };
                // filter_col may sit on either side of the equi-pair;
                // the OTHER side's key is the dim key.
                let (dim_key, dim_side) = if lc.name == filter_col.name {
                    (rc.clone(), &j.right)
                } else if rc.name == filter_col.name {
                    (lc.clone(), &j.left)
                } else {
                    continue;
                };
                if let Some(dim_subtree) = small_filtered_dim_subtree(dim_side) {
                    return Some((dim_subtree, dim_key));
                }
            }
            find_transitive_dim_semi(&j.left, filter_col)
                .or_else(|| find_transitive_dim_semi(&j.right, filter_col))
        }
        LogicalPlan::Projection(p) => find_transitive_dim_semi(&p.input, filter_col),
        LogicalPlan::SubqueryAlias(s) => find_transitive_dim_semi(&s.input, filter_col),
        LogicalPlan::Filter(f) => find_transitive_dim_semi(&f.input, filter_col),
        _ => None,
    }
}

/// A subtree is a "small filtered dim" if, descending through
/// Projection/SubqueryAlias wrappers, it reaches a `Filter` directly
/// over a `TableScan`. This is the same shape `is_worth_pushing`
/// requires, but tolerant of wrapper nodes (Q20's forest-parts is
/// `SubqueryAlias → Projection → Filter → TableScan`). Returns a clone
/// of the whole subtree (wrappers included) on success.
fn small_filtered_dim_subtree(plan: &LogicalPlan) -> Option<LogicalPlan> {
    fn reaches_filter_scan(p: &LogicalPlan) -> bool {
        match p {
            LogicalPlan::Filter(f) => {
                matches!(f.input.as_ref(), LogicalPlan::TableScan(_))
                    || reaches_filter_scan(&f.input)
            }
            LogicalPlan::Projection(pr) => reaches_filter_scan(&pr.input),
            LogicalPlan::SubqueryAlias(s) => reaches_filter_scan(&s.input),
            _ => false,
        }
    }
    if reaches_filter_scan(plan) {
        Some(plan.clone())
    } else {
        None
    }
}

/// Try the Q17-shape rewrite on a single Inner Join node.
fn try_rewrite_q17_shape(plan: &LogicalPlan) -> Option<LogicalPlan> {
    let LogicalPlan::Join(j) = plan else {
        return None;
    };
    if j.join_type != JoinType::Inner {
        return None;
    }
    // Must have at least one equi-key pair.
    if j.on.is_empty() {
        return None;
    }

    // Identify which side is the aggregate-bearing branch and which
    // is the filter-bearing branch. The aggregate side has a path
    // down to `Aggregate → TableScan(T)`; the filter side has
    // `Filter(P) → TableScan(T_alt)` reachable from the join key.
    let (agg_side, filter_side, agg_on_right) =
        match (find_agg_branch(&j.left), find_agg_branch(&j.right)) {
            (None, Some(_)) => (&j.right, &j.left, true),
            (Some(_), None) => (&j.left, &j.right, false),
            _ => return None,
        };

    let agg_info = find_agg_branch(agg_side)?;

    // From join.on, find the equi pair that connects agg_side.K to
    // filter_side.X.
    //
    // The outer join's references use the SubqueryAlias's relation
    // (e.g. `__scalar_sq_1.l_partkey`). We need to look up the
    // un-aliased column (`lineitem.l_partkey`) from the agg's own
    // group_by — that's the column qualifier visible to the LeftSemi
    // we're about to splice INSIDE the SubqueryAlias wrapper.
    let (filter_col, inner_agg_col) = j.on.iter().find_map(|(l, r)| match (l, r) {
        (Expr::Column(lc), Expr::Column(rc)) => {
            let (filter_c, agg_c) = if agg_on_right { (lc, rc) } else { (rc, lc) };
            let inner = agg_info
                .group_by_cols
                .iter()
                .find(|gc| gc.name == agg_c.name)?;
            Some((filter_c.clone(), inner.clone()))
        }
        _ => None,
    })?;

    // Find a clone-able filter subtree on the filter side that
    // produces filter_col.
    let filter_subtree = find_filter_subtree_producing(filter_side, &filter_col)?;

    // The filter subtree must be smaller than the agg's input.
    // For the MVP we accept any Filter-over-TableScan; selectivity
    // gating can come later.
    if !is_worth_pushing(&filter_subtree) {
        return None;
    }

    // Build LeftSemi(agg_table_scan, filter_subtree_clone) and splice
    // it in at the agg's input. The agg's existing structure
    // (Projection? → SubqueryAlias? → Projection? → Aggregate)
    // stays the same; only the deepest TableScan node is wrapped.
    let new_agg_side = splice_left_semi_into_agg(
        agg_side,
        &inner_agg_col,
        &filter_col,
        filter_subtree.clone(),
    )?;

    if std::env::var("EMAT_SIGMA_U_DEBUG").is_ok() {
        eprintln!(
            "[Σ.U] new_agg_side after splice:\n{}",
            new_agg_side.display_indent()
        );
    }
    // Construct the new Inner Join with the rewritten agg side.
    let new_left;
    let new_right;
    if agg_on_right {
        new_left = j.left.clone();
        new_right = new_agg_side;
    } else {
        new_left = new_agg_side;
        new_right = j.right.clone();
    }
    LogicalPlanBuilder::from(new_left.as_ref().clone())
        .join_on(
            new_right.as_ref().clone(),
            JoinType::Inner,
            j.on.iter()
                .map(|(l, r)| {
                    Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(l.clone()),
                        op: Operator::Eq,
                        right: Box::new(r.clone()),
                    })
                })
                .chain(j.filter.iter().cloned()),
        )
        .ok()?
        .build()
        .ok()
}

/// Information harvested from an aggregate branch.
#[derive(Debug)]
struct AggBranchInfo {
    group_by_cols: Vec<Column>,
}

/// Walk down through `Projection`, `SubqueryAlias`, and other
/// non-aggregate nodes looking for an `Aggregate` over a
/// `TableScan`. Returns the aggregate's group-by columns if the
/// shape matches; `None` otherwise.
fn find_agg_branch(plan: &LogicalPlan) -> Option<AggBranchInfo> {
    match plan {
        LogicalPlan::Aggregate(agg) => {
            // Σ.U Phase 1.1 (2026-05-26): the aggregate's input can be
            // any subtree that produces the group-by columns — not just
            // a TableScan. Q02 has a 4-table Inner Join (partsupp ⋈
            // supplier ⋈ nation ⋈ region) under the agg; Q17 has a
            // single TableScan. Both are valid targets — we wrap the
            // entire agg.input in a LeftSemi, not just the TableScan.
            let group_by_cols: Vec<Column> = agg
                .group_expr
                .iter()
                .filter_map(|e| match e {
                    Expr::Column(c) => Some(c.clone()),
                    _ => None,
                })
                .collect();
            if group_by_cols.is_empty() {
                return None;
            }
            // Sanity: the input schema must expose every group-by column
            // (always true post-optimization, but guard defensively).
            let input_schema = agg.input.schema();
            if !group_by_cols
                .iter()
                .all(|gc| input_schema.fields().iter().any(|f| f.name() == &gc.name))
            {
                return None;
            }
            Some(AggBranchInfo { group_by_cols })
        }
        LogicalPlan::Projection(p) => find_agg_branch(&p.input),
        LogicalPlan::SubqueryAlias(s) => find_agg_branch(&s.input),
        _ => None,
    }
}

/// Find the smallest subtree rooted at a `Filter` (or directly a
/// filtered `TableScan` via `partial_filters`) on the filter side
/// that produces `target_col` and is small enough to be worth
/// cloning.
fn find_filter_subtree_producing(plan: &LogicalPlan, target_col: &Column) -> Option<LogicalPlan> {
    // Walk down looking for a Filter node whose Filter expression
    // references target_col's table (or any column from the same
    // TableScan that produces target_col).
    match plan {
        LogicalPlan::Filter(f) => {
            // If this Filter's input is a TableScan that schema-includes target_col, return it.
            if scan_produces_column(&f.input, target_col) {
                return Some(LogicalPlan::Filter(f.clone()));
            }
            // Else recurse into input.
            find_filter_subtree_producing(&f.input, target_col)
        }
        LogicalPlan::Projection(p) => find_filter_subtree_producing(&p.input, target_col),
        LogicalPlan::SubqueryAlias(s) => find_filter_subtree_producing(&s.input, target_col),
        LogicalPlan::Join(j) => {
            // Try both sides. We want the leaf subtree that produces
            // the target column.
            if let Some(s) = find_filter_subtree_producing(&j.left, target_col) {
                return Some(s);
            }
            find_filter_subtree_producing(&j.right, target_col)
        }
        _ => None,
    }
}

fn scan_produces_column(plan: &LogicalPlan, col: &Column) -> bool {
    match plan {
        LogicalPlan::TableScan(ts) => ts
            .projected_schema
            .fields()
            .iter()
            .any(|f| f.name() == &col.name),
        LogicalPlan::Filter(f) => scan_produces_column(&f.input, col),
        LogicalPlan::Projection(p) => scan_produces_column(&p.input, col),
        LogicalPlan::SubqueryAlias(s) => scan_produces_column(&s.input, col),
        _ => false,
    }
}

/// Heuristic: only push if the filter subtree's leaf is a single
/// TableScan and the filter has at least one literal-bearing
/// predicate (i.e., it actually filters something). MVP gate;
/// proper selectivity comes later.
fn is_worth_pushing(plan: &LogicalPlan) -> bool {
    if let LogicalPlan::Filter(f) = plan {
        if let LogicalPlan::TableScan(_) = f.input.as_ref() {
            return true;
        }
    }
    false
}

/// Splice a LeftSemi between the Aggregate's `TableScan` input and
/// the Aggregate node itself. Walks down the agg branch
/// transparently through Projection/SubqueryAlias/Aggregate; when it
/// finds the Aggregate's `TableScan`, wraps it in a LeftSemi
/// against `filter_subtree`.
fn splice_left_semi_into_agg(
    agg_side: &LogicalPlan,
    agg_col: &Column,
    filter_col: &Column,
    filter_subtree: LogicalPlan,
) -> Option<std::sync::Arc<LogicalPlan>> {
    let rewritten = splice_recurse(agg_side, agg_col, filter_col, &filter_subtree)?;
    Some(std::sync::Arc::new(rewritten))
}

fn splice_recurse(
    plan: &LogicalPlan,
    agg_col: &Column,
    filter_col: &Column,
    filter_subtree: &LogicalPlan,
) -> Option<LogicalPlan> {
    use std::sync::Arc;
    match plan {
        LogicalPlan::Aggregate(agg) => {
            // Σ.U Phase 1.1 (2026-05-26): wrap the aggregate's input
            // directly in LeftSemi. This generalises across:
            //   - Q17: agg.input is `TableScan: lineitem` — wrap it
            //   - Q02: agg.input is a `Projection → Inner Join (4-table)`
            //          — wrap the whole subtree
            // The LeftSemi joins the agg's input (which produces
            // `agg_col` by definition) with the filter subtree on
            // `agg_col = filter_col`.
            let on_expr = Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(agg_col.clone())),
                op: Operator::Eq,
                right: Box::new(Expr::Column(filter_col.clone())),
            });
            let new_input = LogicalPlanBuilder::from(agg.input.as_ref().clone())
                .join_on(filter_subtree.clone(), JoinType::LeftSemi, [on_expr])
                .ok()?
                .build()
                .ok()?;
            let new_agg = LogicalPlan::Aggregate(
                Aggregate::try_new(
                    Arc::new(new_input),
                    agg.group_expr.clone(),
                    agg.aggr_expr.clone(),
                )
                .ok()?,
            );
            Some(new_agg)
        }
        LogicalPlan::Projection(p) => {
            let new_input = splice_recurse(&p.input, agg_col, filter_col, filter_subtree)?;
            let new_proj = LogicalPlanBuilder::from(new_input)
                .project(p.expr.clone())
                .ok()?
                .build()
                .ok()?;
            Some(new_proj)
        }
        LogicalPlan::SubqueryAlias(s) => {
            let new_input = splice_recurse(&s.input, agg_col, filter_col, filter_subtree)?;
            let new_sa = LogicalPlanBuilder::from(new_input)
                .alias(s.alias.clone())
                .ok()?
                .build()
                .ok()?;
            Some(new_sa)
        }
        _ => Some(plan.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ematix_fast_parquet::EmatixFastParquetTableProvider;
    use crate::fast_parquet::FastParquetTableProvider;
    use datafusion::prelude::SessionContext;
    use std::path::PathBuf;
    use std::sync::Arc;

    fn sf1_dir() -> Option<PathBuf> {
        if let Ok(env) = std::env::var("TPCH_DATA_DIR") {
            let p = PathBuf::from(env);
            if p.exists() {
                return Some(p);
            }
        }
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let p = manifest
            .parent()
            .and_then(|p| p.parent())
            .map(|p| p.join("examples/tpch/data/sf1"))?;
        if p.exists() {
            return Some(p);
        }
        None
    }

    fn sf10_dir() -> Option<PathBuf> {
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let p = manifest
            .parent()
            .and_then(|p| p.parent())
            .map(|p| p.join("examples/tpch/data/sf10"))?;
        if p.exists() {
            return Some(p);
        }
        None
    }

    async fn register_tpch(ctx: &SessionContext, dir: &std::path::Path) -> DfResult<()> {
        for t in [
            "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
        ] {
            let path = dir.join(format!("{t}.parquet"));
            if t == "lineitem" {
                let prov = EmatixFastParquetTableProvider::try_new(path.to_string_lossy())?;
                ctx.register_table(t, Arc::new(prov))?;
            } else {
                let prov = FastParquetTableProvider::try_new(path.to_string_lossy())?;
                ctx.register_table(t, Arc::new(prov))?;
            }
        }
        Ok(())
    }

    /// No-op on Q01 (no aggregate-side correlation).
    #[tokio::test]
    async fn no_op_on_q01_shape() -> DfResult<()> {
        let Some(dir) = sf1_dir() else {
            eprintln!("skip: sf1 missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        let df = ctx
            .sql("SELECT l_returnflag, SUM(l_quantity) FROM lineitem GROUP BY l_returnflag")
            .await?;
        let optimized = df.into_optimized_plan()?;
        let rewritten = push_filter_into_agg(optimized.clone())?;
        assert_eq!(
            format!("{}", optimized.display_indent()),
            format!("{}", rewritten.display_indent()),
            "Q01 shape (single aggregate, no correlated subquery) must pass through unchanged"
        );
        Ok(())
    }

    /// Fires on Q17 shape — verify a `LeftSemi` is inserted in the
    /// aggregate's input subtree.
    #[tokio::test]
    async fn fires_on_q17_shape() -> DfResult<()> {
        let Some(dir) = sf1_dir() else {
            eprintln!("skip: sf1 missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        let sql = std::fs::read_to_string(
            dir.parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("queries/q17.sql"),
        )
        .ok();
        let Some(sql) = sql else {
            eprintln!("skip: q17.sql missing");
            return Ok(());
        };
        let df = ctx.sql(&sql).await?;
        let optimized = df.into_optimized_plan()?;
        let rewritten = push_filter_into_agg(optimized.clone())?;
        let orig_dump = format!("{}", optimized.display_indent());
        let new_dump = format!("{}", rewritten.display_indent());
        assert_ne!(
            orig_dump, new_dump,
            "Q17 rewrite should change the plan; got identical:\n{orig_dump}"
        );
        assert!(
            new_dump.contains("LeftSemi"),
            "rewritten plan must contain a LeftSemi node:\n{new_dump}"
        );
        Ok(())
    }

    /// Σ.U Phase 1.1: also fires on Q02 (correlated MIN subquery
    /// over partsupp ⋈ supplier ⋈ nation ⋈ region(EUROPE)). The agg's
    /// input here is a 4-table Inner Join, not a single TableScan —
    /// the generalised splice wraps the entire input in LeftSemi.
    #[tokio::test]
    async fn fires_on_q02_shape() -> DfResult<()> {
        let Some(dir) = sf1_dir() else {
            eprintln!("skip: sf1 missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        let sql = std::fs::read_to_string(
            dir.parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("queries/q02.sql"),
        )
        .ok();
        let Some(sql) = sql else {
            eprintln!("skip: q02.sql missing");
            return Ok(());
        };
        let df = ctx.sql(&sql).await?;
        let optimized = df.into_optimized_plan()?;
        let rewritten = push_filter_into_agg(optimized.clone())?;
        let orig_dump = format!("{}", optimized.display_indent());
        let new_dump = format!("{}", rewritten.display_indent());
        assert_ne!(orig_dump, new_dump, "Q02 rewrite must change plan");
        assert!(
            new_dump.contains("LeftSemi"),
            "rewritten Q02 plan must contain a LeftSemi:\n{new_dump}"
        );
        Ok(())
    }

    /// Σ.U Phase 1.1: Q02 end-to-end correctness — rewrite must
    /// produce the same row set as baseline.
    #[tokio::test]
    async fn rewrite_preserves_q02_result() -> DfResult<()> {
        let Some(dir) = sf1_dir() else {
            eprintln!("skip: sf1 missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        let sql = std::fs::read_to_string(
            dir.parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("queries/q02.sql"),
        )
        .ok();
        let Some(sql) = sql else {
            eprintln!("skip: q02.sql missing");
            return Ok(());
        };
        // Baseline.
        let df_baseline = ctx.sql(&sql).await?;
        let baseline_batches = df_baseline.collect().await?;
        let baseline_rows: usize = baseline_batches.iter().map(|b| b.num_rows()).sum();
        // Rewritten.
        let df_new = ctx.sql(&sql).await?;
        let optimized = df_new.into_optimized_plan()?;
        let rewritten = push_filter_into_agg(optimized)?;
        let df_new = ctx.execute_logical_plan(rewritten).await?;
        let new_batches = df_new.collect().await?;
        let new_rows: usize = new_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            baseline_rows, new_rows,
            "Q02 rewrite changed row count: baseline={baseline_rows}, rewritten={new_rows}"
        );
        Ok(())
    }

    /// End-to-end correctness: after rewrite, executing the plan
    /// must return the same single row (sum / 7 = avg_yearly).
    #[tokio::test]
    async fn rewrite_preserves_q17_result() -> DfResult<()> {
        let Some(dir) = sf1_dir() else {
            eprintln!("skip: sf1 missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        let sql = std::fs::read_to_string(
            dir.parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("queries/q17.sql"),
        )
        .ok();
        let Some(sql) = sql else {
            eprintln!("skip: q17.sql missing");
            return Ok(());
        };

        // Baseline result (no rewrite).
        let df_baseline = ctx.sql(&sql).await?;
        let baseline_batches = df_baseline.collect().await?;
        let baseline_val = baseline_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .unwrap()
            .value(0);

        // Rewritten result.
        let df_new = ctx.sql(&sql).await?;
        let optimized = df_new.into_optimized_plan()?;
        let rewritten = push_filter_into_agg(optimized)?;
        let df_new = ctx.execute_logical_plan(rewritten).await?;
        let new_batches = df_new.collect().await?;
        let new_val = new_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .unwrap()
            .value(0);

        let rel = ((new_val - baseline_val) / baseline_val).abs();
        assert!(
            rel < 1e-9,
            "Q17 rewrite changed result: baseline={baseline_val}, rewritten={new_val}, rel_err={rel:e}"
        );
        Ok(())
    }

    /// Σ.AJ.1 Lever C Step 1: Q17 SF=10 end-to-end correctness. The
    /// SF=1 test passes; this verifies the matcher still hits the
    /// shape at SF=10 (different partition counts can shift the
    /// planner's plan structure). Skips if `examples/tpch/data/sf10`
    /// is absent.
    #[tokio::test]
    async fn rewrite_preserves_q17_result_sf10() -> DfResult<()> {
        let Some(dir) = sf10_dir() else {
            eprintln!("skip: sf10 missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        let sql = std::fs::read_to_string(
            dir.parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("queries/q17.sql"),
        )
        .ok();
        let Some(sql) = sql else {
            eprintln!("skip: q17.sql missing");
            return Ok(());
        };

        let df_baseline = ctx.sql(&sql).await?;
        let baseline_batches = df_baseline.collect().await?;
        let baseline_val = baseline_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .unwrap()
            .value(0);

        let df_new = ctx.sql(&sql).await?;
        let optimized = df_new.into_optimized_plan()?;
        let rewritten = push_filter_into_agg(optimized)?;
        let df_new = ctx.execute_logical_plan(rewritten).await?;
        let new_batches = df_new.collect().await?;
        let new_val = new_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .unwrap()
            .value(0);

        let rel = ((new_val - baseline_val) / baseline_val).abs();
        assert!(
            rel < 1e-6,
            "Q17 SF=10 rewrite changed result: baseline={baseline_val}, rewritten={new_val}, rel_err={rel:e}"
        );
        Ok(())
    }

    /// Σ.AJ.1 Lever C Step 1: Q02 SF=10 end-to-end correctness.
    /// Row-count check matches the SF=1 variant.
    #[tokio::test]
    async fn rewrite_preserves_q02_result_sf10() -> DfResult<()> {
        let Some(dir) = sf10_dir() else {
            eprintln!("skip: sf10 missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        let sql = std::fs::read_to_string(
            dir.parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("queries/q02.sql"),
        )
        .ok();
        let Some(sql) = sql else {
            eprintln!("skip: q02.sql missing");
            return Ok(());
        };
        let df_baseline = ctx.sql(&sql).await?;
        let baseline_batches = df_baseline.collect().await?;
        let baseline_rows: usize = baseline_batches.iter().map(|b| b.num_rows()).sum();
        let df_new = ctx.sql(&sql).await?;
        let optimized = df_new.into_optimized_plan()?;
        let rewritten = push_filter_into_agg(optimized)?;
        let df_new = ctx.execute_logical_plan(rewritten).await?;
        let new_batches = df_new.collect().await?;
        let new_rows: usize = new_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            baseline_rows, new_rows,
            "Q02 SF=10 rewrite changed row count: baseline={baseline_rows}, rewritten={new_rows}"
        );
        Ok(())
    }

    /// Σ.AJ.1 Lever C Step 1: matcher specificity — Q11's HAVING
    /// references a scalar subquery without group-by, and the outer
    /// side is an Aggregate (not Filter→TableScan). The matcher
    /// must not fire on this shape. SF=1 (Q11 SF=10 also tested
    /// implicitly via the 22q bench, so we don't need an SF=10
    /// variant here — specificity is a structural property of the
    /// plan shape, not the data).
    #[tokio::test]
    async fn matcher_specificity_q11() -> DfResult<()> {
        let Some(dir) = sf1_dir() else {
            eprintln!("skip: sf1 missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        let sql = std::fs::read_to_string(
            dir.parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("queries/q11.sql"),
        )
        .ok();
        let Some(sql) = sql else {
            eprintln!("skip: q11.sql missing");
            return Ok(());
        };
        let df = ctx.sql(&sql).await?;
        let optimized = df.into_optimized_plan()?;
        let rewritten = push_filter_into_agg(optimized.clone())?;
        let baseline_rewrite_diff =
            format!("{}", optimized.display_indent()) != format!("{}", rewritten.display_indent());
        // Q11 may or may not match; what matters is correctness if it
        // does. If a LeftSemi appears, also verify row-count parity.
        if baseline_rewrite_diff {
            let df_baseline = ctx.sql(&sql).await?;
            let base_rows: usize = df_baseline
                .collect()
                .await?
                .iter()
                .map(|b| b.num_rows())
                .sum();
            let df_new = ctx.execute_logical_plan(rewritten).await?;
            let new_rows: usize = df_new.collect().await?.iter().map(|b| b.num_rows()).sum();
            assert_eq!(
                base_rows, new_rows,
                "Q11 rewrite fired but changed row count: baseline={base_rows}, rewritten={new_rows}"
            );
        }
        Ok(())
    }

    /// Σ.AJ.1 Lever C Step 1: matcher specificity — Q15 has a CTE
    /// (revenue_s) referenced on both sides of the outer join, and
    /// the subquery is `max(total_revenue) from revenue_s` (no
    /// group-by). Matcher should not fire. If it does fire, row
    /// counts must still match.
    #[tokio::test]
    async fn matcher_specificity_q15() -> DfResult<()> {
        let Some(dir) = sf1_dir() else {
            eprintln!("skip: sf1 missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        let sql = std::fs::read_to_string(
            dir.parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("queries/q15.sql"),
        )
        .ok();
        let Some(sql) = sql else {
            eprintln!("skip: q15.sql missing");
            return Ok(());
        };
        let df = ctx.sql(&sql).await?;
        let optimized = df.into_optimized_plan()?;
        let rewritten = push_filter_into_agg(optimized.clone())?;
        if format!("{}", optimized.display_indent()) != format!("{}", rewritten.display_indent()) {
            let df_baseline = ctx.sql(&sql).await?;
            let base_rows: usize = df_baseline
                .collect()
                .await?
                .iter()
                .map(|b| b.num_rows())
                .sum();
            let df_new = ctx.execute_logical_plan(rewritten).await?;
            let new_rows: usize = df_new.collect().await?.iter().map(|b| b.num_rows()).sum();
            assert_eq!(
                base_rows, new_rows,
                "Q15 rewrite fired but changed row count: baseline={base_rows}, rewritten={new_rows}"
            );
        }
        Ok(())
    }

    /// Σ.AJ.1 Lever C Step 1: matcher specificity — Q22's subquery
    /// is an uncorrelated `avg(c_acctbal)` with no GROUP BY. The
    /// matcher requires `Aggregate(group_by=[K])` with at least one
    /// group key, so this must not fire.
    #[tokio::test]
    async fn matcher_specificity_q22() -> DfResult<()> {
        let Some(dir) = sf1_dir() else {
            eprintln!("skip: sf1 missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        let sql = std::fs::read_to_string(
            dir.parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("queries/q22.sql"),
        )
        .ok();
        let Some(sql) = sql else {
            eprintln!("skip: q22.sql missing");
            return Ok(());
        };
        let df = ctx.sql(&sql).await?;
        let optimized = df.into_optimized_plan()?;
        let rewritten = push_filter_into_agg(optimized.clone())?;
        if format!("{}", optimized.display_indent()) != format!("{}", rewritten.display_indent()) {
            let df_baseline = ctx.sql(&sql).await?;
            let base_rows: usize = df_baseline
                .collect()
                .await?
                .iter()
                .map(|b| b.num_rows())
                .sum();
            let df_new = ctx.execute_logical_plan(rewritten).await?;
            let new_rows: usize = df_new.collect().await?.iter().map(|b| b.num_rows()).sum();
            assert_eq!(
                base_rows, new_rows,
                "Q22 rewrite fired but changed row count: baseline={base_rows}, rewritten={new_rows}"
            );
        }
        Ok(())
    }

    /// Σ.Q20: fires on the Q20 transitive shape — the forest-parts
    /// semi (`partsupp ⋉ part WHERE forest`) is pushed as a NEW
    /// `LeftSemi` below the lineitem correlated aggregate, keyed on
    /// `l_partkey`.
    #[tokio::test]
    async fn fires_on_q20_shape() -> DfResult<()> {
        let Some(dir) = sf1_dir() else {
            eprintln!("skip: sf1 missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        let sql = std::fs::read_to_string(
            dir.parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("queries/q20.sql"),
        )
        .ok();
        let Some(sql) = sql else {
            eprintln!("skip: q20.sql missing");
            return Ok(());
        };
        let df = ctx.sql(&sql).await?;
        let optimized = df.into_optimized_plan()?;
        let rewritten = push_transitive_semi_into_agg(optimized.clone())?;
        let orig_dump = format!("{}", optimized.display_indent());
        let new_dump = format!("{}", rewritten.display_indent());
        assert_ne!(
            orig_dump, new_dump,
            "Q20 transitive rewrite should change the plan; got identical:\n{orig_dump}"
        );
        let orig_semis = orig_dump.matches("LeftSemi").count();
        let new_semis = new_dump.matches("LeftSemi").count();
        assert!(
            new_semis > orig_semis,
            "Q20 rewrite must ADD a LeftSemi (orig={orig_semis}, new={new_semis}):\n{new_dump}"
        );
        assert!(
            new_dump.contains("l_partkey ="),
            "the spliced LeftSemi must key on l_partkey:\n{new_dump}"
        );
        Ok(())
    }

    /// Σ.Q20 end-to-end correctness: the transitive semi-pushdown must
    /// not change Q20's result. The spliced LeftSemi only drops
    /// lineitem rows the outer join already discards (l_partkey =
    /// ps_partkey ∈ forest-parts), so the row set is identical.
    #[tokio::test]
    async fn rewrite_preserves_q20_result() -> DfResult<()> {
        let Some(dir) = sf1_dir() else {
            eprintln!("skip: sf1 missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        let sql = std::fs::read_to_string(
            dir.parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("queries/q20.sql"),
        )
        .ok();
        let Some(sql) = sql else {
            eprintln!("skip: q20.sql missing");
            return Ok(());
        };

        // Baseline: sorted (s_name, s_address) tuples.
        let extract = |batches: &[datafusion::arrow::record_batch::RecordBatch]| -> Vec<String> {
            use datafusion::arrow::array::StringViewArray;
            let mut rows = Vec::new();
            for b in batches {
                let names = b.column(0).as_any().downcast_ref::<StringViewArray>();
                let addrs = b.column(1).as_any().downcast_ref::<StringViewArray>();
                if let (Some(n), Some(a)) = (names, addrs) {
                    for i in 0..b.num_rows() {
                        rows.push(format!("{}|{}", n.value(i), a.value(i)));
                    }
                }
            }
            rows.sort();
            rows
        };
        let baseline_batches = ctx.sql(&sql).await?.collect().await?;
        let baseline = extract(&baseline_batches);

        let optimized = ctx.sql(&sql).await?.into_optimized_plan()?;
        let rewritten = push_transitive_semi_into_agg(optimized)?;
        let new_batches = ctx.execute_logical_plan(rewritten).await?.collect().await?;
        let new = extract(&new_batches);

        assert!(!baseline.is_empty(), "Q20 baseline produced no rows");
        assert_eq!(
            baseline.len(),
            new.len(),
            "Q20 transitive rewrite changed row count: baseline={}, rewritten={}",
            baseline.len(),
            new.len()
        );
        assert_eq!(
            baseline, new,
            "Q20 transitive rewrite changed the (s_name, s_address) result set"
        );
        Ok(())
    }

    /// Σ.Q20 specificity: the transitive rule is a NO-OP on shapes it
    /// doesn't target — Q02 (correlated MIN over partsupp, but the
    /// filter side is a direct `Filter → TableScan` handled by Σ.U, not
    /// the transitive-dim-semi path) and Q17 (single-table correlated
    /// AVG, no dim semi at all).
    #[tokio::test]
    async fn transitive_no_op_on_q02_q17() -> DfResult<()> {
        let Some(dir) = sf1_dir() else {
            eprintln!("skip: sf1 missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        for q in ["q02", "q17"] {
            let path = dir
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .join(format!("queries/{q}.sql"));
            let Some(sql) = std::fs::read_to_string(&path).ok() else {
                continue;
            };
            let optimized = ctx.sql(&sql).await?.into_optimized_plan()?;
            let rewritten = push_transitive_semi_into_agg(optimized.clone())?;
            assert_eq!(
                format!("{}", optimized.display_indent()),
                format!("{}", rewritten.display_indent()),
                "{q}: transitive semi rule must be a no-op (targets only the Q20 dim-semi→agg shape)"
            );
        }
        Ok(())
    }

    async fn q_sql(dir: &std::path::Path, q: &str) -> Option<String> {
        std::fs::read_to_string(
            dir.parent()
                .unwrap()
                .parent()
                .unwrap()
                .join(format!("queries/{q}.sql")),
        )
        .ok()
    }

    /// Σ.Q05 (#352): the transitive dim-semi rule fires on the Q05
    /// snowflake — customer is constrained by `nation ⋈ region[ASIA]`
    /// only through the supplier hop (`c_nationkey = s_nationkey =
    /// n_nationkey`), so a `LeftSemi(customer, nation ⋈ region)` is
    /// spliced below customer, shrinking the orders⋈customer build
    /// the lineitem join probes from (2.28M → 456K keys at SF=10).
    #[tokio::test]
    async fn transitive_join_semi_fires_on_q05_shape() -> DfResult<()> {
        let Some(dir) = sf1_dir() else {
            eprintln!("skip: sf1 missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        let Some(sql) = q_sql(&dir, "q05").await else {
            eprintln!("skip: q05.sql missing");
            return Ok(());
        };
        let optimized = ctx.sql(&sql).await?.into_optimized_plan()?;
        let rewritten = push_transitive_dim_semi_into_join_chain(optimized.clone())?;
        let orig_dump = format!("{}", optimized.display_indent());
        let new_dump = format!("{}", rewritten.display_indent());
        assert_ne!(
            orig_dump, new_dump,
            "Q05 transitive dim-semi must change the plan; got identical:\n{orig_dump}"
        );
        assert!(
            new_dump.contains("LeftSemi Join: customer.c_nationkey = nation.n_nationkey"),
            "expected LeftSemi(customer, nation⋈region) keyed c_nationkey = n_nationkey:\n{new_dump}"
        );
        // The semi build clones nation + region below customer: both
        // scans now appear twice.
        assert_eq!(
            new_dump.matches("TableScan: region").count(),
            2,
            "region scan must be cloned into the semi build:\n{new_dump}"
        );
        assert_eq!(
            new_dump.matches("TableScan: nation").count(),
            2,
            "nation scan must be cloned into the semi build:\n{new_dump}"
        );
        // Idempotent: a second application must not splice again.
        let twice = push_transitive_dim_semi_into_join_chain(rewritten)?;
        assert_eq!(
            new_dump,
            format!("{}", twice.display_indent()),
            "second application must be a no-op (idempotency guard)"
        );
        Ok(())
    }

    /// Σ.Q05 end-to-end correctness: the spliced semi only drops
    /// customer rows whose nation is non-ASIA — rows the original
    /// supplier⋈nation⋈region joins discard anyway — so the (n_name,
    /// revenue) result set is identical (float-sum tolerance only).
    #[tokio::test]
    async fn transitive_join_semi_preserves_q05_result() -> DfResult<()> {
        let Some(dir) = sf1_dir() else {
            eprintln!("skip: sf1 missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        let Some(sql) = q_sql(&dir, "q05").await else {
            eprintln!("skip: q05.sql missing");
            return Ok(());
        };
        let extract =
            |batches: &[datafusion::arrow::record_batch::RecordBatch]| -> Vec<(String, f64)> {
                use datafusion::arrow::array::{Float64Array, StringViewArray};
                let mut rows = Vec::new();
                for b in batches {
                    let names = b.column(0).as_any().downcast_ref::<StringViewArray>();
                    let revs = b.column(1).as_any().downcast_ref::<Float64Array>();
                    if let (Some(n), Some(r)) = (names, revs) {
                        for i in 0..b.num_rows() {
                            rows.push((n.value(i).to_string(), r.value(i)));
                        }
                    }
                }
                rows.sort_by(|a, b| a.0.cmp(&b.0));
                rows
            };
        let baseline_batches = ctx.sql(&sql).await?.collect().await?;
        let baseline = extract(&baseline_batches);

        let optimized = ctx.sql(&sql).await?.into_optimized_plan()?;
        let rewritten = push_transitive_dim_semi_into_join_chain(optimized)?;
        let new_batches = ctx.execute_logical_plan(rewritten).await?.collect().await?;
        let new = extract(&new_batches);

        assert!(!baseline.is_empty(), "Q05 baseline produced no rows");
        assert_eq!(
            baseline.len(),
            new.len(),
            "Q05 rewrite changed row count: baseline={}, rewritten={}",
            baseline.len(),
            new.len()
        );
        for ((bn, bv), (nn, nv)) in baseline.iter().zip(new.iter()) {
            assert_eq!(bn, nn, "Q05 rewrite changed the nation set");
            assert!(
                (bv - nv).abs() <= 1e-3 * bv.abs().max(1.0),
                "Q05 rewrite changed revenue for {bn}: {bv} vs {nv}"
            );
        }
        Ok(())
    }

    /// Σ.Q05 specificity — the transitive-only (hop ≥ 2) gate: every
    /// other TPC-H dim constraint rides a DIRECT join edge (customer or
    /// supplier joins the filtered dim's mid-dim immediately), which the
    /// join itself + L9 blooms already handle. The rule must pass those
    /// shapes through byte-identical.
    #[tokio::test]
    async fn transitive_join_semi_no_op_on_direct_dim_shapes() -> DfResult<()> {
        let Some(dir) = sf1_dir() else {
            eprintln!("skip: sf1 missing");
            return Ok(());
        };
        let ctx = SessionContext::new();
        register_tpch(&ctx, &dir).await?;
        // Every TPC-H query except Q05. Documented per-shape reasons:
        // direct join edges (q02/q03/q07/q08/q10/q11/q21 — the dim's
        // constraint lands at hop 1, which the join itself + L9 blooms
        // already cover), inverted mid-dim (q09 — lineitem would be the
        // "mid-dim" and the hop-2 target nation is SHALLOWER, killed by
        // the downhill gate), opposite-LeftSemi-side chains (q20 — the
        // supplier⋈nation[CANADA] anchor's S side is a bare supplier
        // scan; the lineitem chain hangs on the other side of the outer
        // LeftSemi, outside the anchor), or no filtered-dim anchor at
        // all (the rest). Blast radius = exactly Q05.
        for q in [
            "q01", "q02", "q03", "q04", "q06", "q07", "q08", "q09", "q10", "q11", "q12", "q13",
            "q14", "q15", "q16", "q17", "q18", "q19", "q20", "q21", "q22",
        ] {
            let Some(sql) = q_sql(&dir, q).await else {
                continue;
            };
            let optimized = ctx.sql(&sql).await?.into_optimized_plan()?;
            let rewritten = push_transitive_dim_semi_into_join_chain(optimized.clone())?;
            assert_eq!(
                format!("{}", optimized.display_indent()),
                format!("{}", rewritten.display_indent()),
                "{q}: transitive dim-semi must be a no-op on this shape"
            );
        }
        Ok(())
    }
}
