//! PV.3b — push-fusion recognizer (logical-plan join-reorder).
//!
//! Lands the Q08 −16% win (de-risked: `pv3b_q08_ab.rs`, −10.8% even with a
//! MemTable round-trip the in-plan splice avoids). The production Q08 plan is
//! a left-deep inner-join chain with the dimension-group joins **interleaved**
//! (supplier at level 2 but its `n2` partner at level 6; orders at 3,
//! customer/n1/region at 4/5/7) — so the win can't be a local node swap. It's
//! a join REORDER: flatten the chain, regroup the non-fact leaves into
//! dimension groups keyed by a fact FK, pre-reduce the i64/membership groups
//! to dense FK→payload probes against a single fact pass, and leave the
//! string-carrying group (`supplier⋈n2`, the `n_name` the agg's CASE reads)
//! stock.
//!
//! ## This module: the ANALYSIS layer (architect S1–S5)
//!
//! [`analyze`] is a pure function `&LogicalPlan -> Option<FusionShape>`:
//!   S1  locate the aggregate + the inner-join region under it; flatten it via
//!       [`crate::join_reorder::flatten_inner_join_chain`] (descends through
//!       the optimizer's column-pruning Projections; keeps `SubqueryAlias`
//!       leaves distinct so `n1`/`n2` stay separate and edges are qualified).
//!   S2  find the FACT leaf = the leaf with the most equi-edges (lineitem:
//!       l_partkey/l_suppkey/l_orderkey), tie-broken by estimated cardinality.
//!   S3  BFS-group the non-fact leaves by which fact-FK they hang off, not
//!       crossing the fact. Bail if a leaf lands in ≥2 groups (B4).
//!   S4  reject cross-group equi-edges (B5 — the Q07 n1×n2 OR shape) and any
//!       residual non-equi join filter (conservative).
//!   S5  every fact-incident edge must be a single i64-widenable equi-key.
//!
//! Classification (membership vs i64-payload vs stock) and the plan
//! RECONSTRUCTION (S6) build on this; they are the next layer. Gated
//! `EMAT_PUSH_PIPELINE=1`, default OFF.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{Column, DFSchema, DFSchemaRef, ScalarValue};
use datafusion::logical_expr::{
    Cast, Expr, Extension, JoinType, LogicalPlan, LogicalPlanBuilder, Projection, col,
};

use crate::fused_probe_node::{BuildSpec, EmitSpec, FusedProbeNode};
use crate::join_reorder::flatten_inner_join_chain;

/// `true` iff `EMAT_PUSH_PIPELINE=1` (shared with the PV.3 physical fuse).
pub fn enabled() -> bool {
    crate::flags::opt_in("EMAT_PUSH_PIPELINE")
}

/// One dimension group: the non-fact leaves reachable from a single fact FK.
#[derive(Debug, Clone)]
pub struct DimGroup {
    /// Indices (into the flattened `leaves`) of this group's leaves.
    pub leaf_idxs: Vec<usize>,
    /// The fact (probe) column the group binds to (e.g. `lineitem.l_partkey`).
    pub fact_fk: Column,
    /// The dimension column the fact FK joins to (e.g. `part.p_partkey`).
    pub dim_key: Column,
}

/// The recognized star/snowflake shape: one fact + N dimension groups. Carries
/// the flattened leaves and resolved edges so the later layers (classify /
/// reconstruct) need not re-flatten the chain.
#[derive(Debug, Clone)]
pub struct FusionShape {
    /// The flattened join leaves (fact + every dimension table/alias).
    pub leaves: Vec<LogicalPlan>,
    /// Every equi-edge as `(leaf_a, leaf_b, col_a, col_b)` — columns qualified.
    pub edges: Vec<(usize, usize, Column, Column)>,
    /// Index (into `leaves`) of the fact leaf.
    pub fact_idx: usize,
    /// One group per fact-FK edge.
    pub groups: Vec<DimGroup>,
}

/// Descend through the post-join wrappers (Sort/Projection/Aggregate/
/// SubqueryAlias/Filter/Limit) to the first Inner-Join-rooted subtree.
fn find_join_chain_root(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    match plan {
        LogicalPlan::Join(_) => Some(plan),
        LogicalPlan::Sort(_)
        | LogicalPlan::Projection(_)
        | LogicalPlan::Aggregate(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Limit(_) => find_join_chain_root(plan.inputs().first()?),
        _ => None,
    }
}

/// Which leaf (by index) does `col` belong to? Matches the qualified column
/// against each leaf's output schema.
fn leaf_of_column(leaves: &[LogicalPlan], col: &Column) -> Option<usize> {
    leaves
        .iter()
        .position(|l| l.schema().columns().iter().any(|c| c == col))
}

/// Architect S1–S5: structural analysis of the inner-join region under the
/// aggregate. Returns the fact + dimension groups, or `None` (bail to stock).
pub fn analyze(plan: &LogicalPlan) -> Option<FusionShape> {
    // S1: locate + flatten the inner-join region under the agg.
    let root = find_join_chain_root(plan)?;
    let chain = flatten_inner_join_chain(root)?;
    if chain.extra_filter.is_some() {
        return None; // residual non-equi join filter → bail (conservative)
    }
    // Own the leaves + preds so the shape is self-contained (S5/S6 reuse it).
    let leaves = chain.leaves;
    let equi_preds = chain.equi_preds;
    let n = leaves.len();
    if n < 3 {
        return None; // need a fact + ≥2 dims to be worth fusing
    }

    // Resolve every edge to a (leaf, leaf) pair. Drop edges we can't place
    // (defensive — a column not in any leaf means our leaf model is off).
    let mut edges: Vec<(usize, usize, Column, Column)> = Vec::with_capacity(equi_preds.len());
    for (a, b) in &equi_preds {
        let (Some(la), Some(lb)) = (leaf_of_column(&leaves, a), leaf_of_column(&leaves, b)) else {
            return None;
        };
        if la == lb {
            return None; // self-edge — unexpected shape
        }
        edges.push((la, lb, a.clone(), b.clone()));
    }

    // S2: fact = leaf with the most incident edges (tie → lowest index;
    // cardinality tie-break is a refinement, but TPC-H facts win on degree).
    let mut degree = vec![0usize; n];
    for (la, lb, _, _) in &edges {
        degree[*la] += 1;
        degree[*lb] += 1;
    }
    let fact_idx = (0..n).max_by_key(|&i| degree[i])?;
    if degree[fact_idx] < 2 {
        return None; // a fact joins ≥2 dims
    }

    // S3: BFS-group the non-fact leaves by which fact-FK they hang off.
    // group_of[leaf] = Some(group index) once assigned.
    let mut group_of: Vec<Option<usize>> = vec![None; n];
    group_of[fact_idx] = Some(usize::MAX); // sentinel: the fact itself
    let mut groups: Vec<DimGroup> = Vec::new();

    // Seed one group per fact-incident edge.
    for (la, lb, ca, cb) in &edges {
        let (dim_leaf, fact_col, dim_col) = if *la == fact_idx {
            (*lb, ca.clone(), cb.clone())
        } else if *lb == fact_idx {
            (*la, cb.clone(), ca.clone())
        } else {
            continue;
        };
        if group_of[dim_leaf].is_some() {
            return None; // a dim leaf hangs off two fact FKs (B4) → bail
        }
        let gid = groups.len();
        group_of[dim_leaf] = Some(gid);
        groups.push(DimGroup {
            leaf_idxs: vec![dim_leaf],
            fact_fk: fact_col,
            dim_key: dim_col,
        });
        // BFS along non-fact edges to pull the rest of the group in.
        let mut q: VecDeque<usize> = VecDeque::from([dim_leaf]);
        while let Some(cur) = q.pop_front() {
            for (ea, eb, _, _) in &edges {
                let other = if *ea == cur {
                    *eb
                } else if *eb == cur {
                    *ea
                } else {
                    continue;
                };
                if other == fact_idx {
                    continue; // don't cross the fact
                }
                match group_of[other] {
                    None => {
                        group_of[other] = Some(gid);
                        groups[gid].leaf_idxs.push(other);
                        q.push_back(other);
                    }
                    Some(g) if g != gid => return None, // cross-group edge (B5) → bail
                    _ => {}
                }
            }
        }
    }

    // S3b: every non-fact leaf must belong to exactly one group.
    if (0..n).any(|i| i != fact_idx && group_of[i].is_none()) {
        return None;
    }
    // S4: no cross-group equi-edge (re-check: an edge between two assigned
    // groups that BFS didn't catch because both ends were pre-seeded).
    for (la, lb, _, _) in &edges {
        if *la == fact_idx || *lb == fact_idx {
            continue;
        }
        if let (Some(ga), Some(gb)) = (group_of[*la], group_of[*lb]) {
            if ga != gb {
                return None;
            }
        }
    }

    // S5 (architect) type-gate: every fact-incident edge must be an
    // i64-widenable integer key — the only thing the fused probe can test.
    // Without this, a pseudo-star whose fact FK is non-integer (e.g. Q15's
    // decorrelated `revenue.total_revenue = max(...)` Float64 edge) would fuse
    // into a probe that resolves every key to None and drops EVERY row.
    let fact_schema = leaves[fact_idx].schema();
    for g in &groups {
        let ty = fact_schema
            .field_with_unqualified_name(&g.fact_fk.name)
            .ok()?
            .data_type();
        if !matches!(
            ty,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        ) {
            return None;
        }
    }

    Some(FusionShape {
        leaves,
        edges,
        fact_idx,
        groups,
    })
}

/// How a dimension group contributes above the fact join — drives whether the
/// recognizer fuses it (into a probe) or leaves it as a stock join.
#[derive(Debug, Clone)]
pub enum GroupClass {
    /// Nothing escapes but the join key → fuse as a LeftSemi membership probe.
    Membership,
    /// Exactly one i64-densifiable value escapes → fuse as an Inner probe that
    /// carries that payload. `expr` is evaluated in the dim subquery (all its
    /// columns live in the group); `alias`/`out_type` are the original
    /// projection output. The operator emits i64, so S6 casts back to
    /// `out_type` in the shape-adapter (e.g. o_year i64 → Int32).
    Payload {
        expr: Expr,
        alias: String,
        out_type: DataType,
    },
    /// A string / multi-column / non-densifiable contribution → leave it stock.
    Stock,
}

fn is_densifiable_int(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

/// Descend the post-join wrappers and return the Projection sitting closest to
/// the join region (the "emit projection" — the boundary naming every value the
/// world above the joins consumes: `o_year`, `volume`, `nation`).
fn emit_projection_above_join(plan: &LogicalPlan) -> Option<&Projection> {
    let mut cur = plan;
    let mut last_proj: Option<&Projection> = None;
    loop {
        match cur {
            LogicalPlan::Projection(p) => {
                last_proj = Some(p);
                cur = p.input.as_ref();
            }
            LogicalPlan::Sort(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Limit(_) => cur = cur.inputs().first()?,
            LogicalPlan::Join(_) => return last_proj,
            _ => return None,
        }
    }
}

/// Map each leaf index → its group index (`None` = the fact leaf).
fn leaf_to_group(shape: &FusionShape) -> Vec<Option<usize>> {
    let mut v = vec![None; shape.leaves.len()];
    for (gi, g) in shape.groups.iter().enumerate() {
        for &li in &g.leaf_idxs {
            v[li] = Some(gi);
        }
    }
    v
}

/// Which single source does an emit expr read from?
enum EmitSrc {
    /// Only fact (probe) columns (e.g. `volume = price*(1-disc)`).
    Fact,
    /// Exactly one dimension group (e.g. `o_year` from orders, `nation` from n2).
    Group(usize),
}

/// Classify an emit expr's source. `None` → it straddles two groups or mixes
/// fact + dim columns (neither splits cleanly into a probe → bail).
fn emit_source(
    expr: &Expr,
    leaves: &[LogicalPlan],
    group_of_leaf: &[Option<usize>],
) -> Option<EmitSrc> {
    let mut touches_fact = false;
    let mut group: Option<usize> = None;
    for c in expr.column_refs() {
        let li = leaf_of_column(leaves, c)?;
        match group_of_leaf[li] {
            None => touches_fact = true,
            Some(g) => match group {
                Some(prev) if prev != g => return None,
                _ => group = Some(g),
            },
        }
    }
    match group {
        None => Some(EmitSrc::Fact),
        Some(g) if !touches_fact => Some(EmitSrc::Group(g)),
        _ => None,
    }
}

/// Architect S4: classify each dim group by its WHOLE above-fact contribution.
/// Returns one [`GroupClass`] per `shape.groups` (aligned by index), or `None`
/// to bail (an emit expr straddles two groups, or mixes fact + dim columns —
/// neither splits cleanly into a probe).
///
/// Rule (architect cut **c**): fuse iff a group contributes only membership, or
/// exactly ONE densifiable-i64 payload; a string / float / multi-value
/// contribution stays stock (Q08: part→Membership, orders→Payload(o_year),
/// supplier+n2→Stock because the agg's CASE reads the Utf8 `n_name`).
pub fn classify(plan: &LogicalPlan, shape: &FusionShape) -> Option<Vec<GroupClass>> {
    let emit = emit_projection_above_join(plan)?;
    let group_of = leaf_to_group(shape);

    // Bucket each emit expr onto the single group it reads from (fact-only
    // exprs are handled by the fused emit, not a group payload).
    let mut per_group: Vec<Vec<(Expr, String, DataType)>> = vec![Vec::new(); shape.groups.len()];
    for (i, expr) in emit.expr.iter().enumerate() {
        match emit_source(expr, &shape.leaves, &group_of)? {
            EmitSrc::Fact => continue,
            EmitSrc::Group(g) => {
                let alias = emit.schema.field(i).name().to_string();
                let out_type = emit.schema.field(i).data_type().clone();
                // Strip the outer alias; S6 re-aliases inside the dim subquery.
                let inner = match expr {
                    Expr::Alias(a) => (*a.expr).clone(),
                    other => other.clone(),
                };
                per_group[g].push((inner, alias, out_type));
            }
        }
    }

    Some(
        per_group
            .into_iter()
            .map(|exprs| match exprs.len() {
                0 => GroupClass::Membership,
                1 if is_densifiable_int(&exprs[0].2) => {
                    let (expr, alias, out_type) = exprs.into_iter().next().unwrap();
                    GroupClass::Payload {
                        expr,
                        alias,
                        out_type,
                    }
                }
                _ => GroupClass::Stock,
            })
            .collect(),
    )
}

// ===================== S5 + S6: validate + reconstruct =====================

/// Number of edges with both endpoints inside `group` (the group's internal
/// reduction tree). A fused group must have exactly `leaves - 1` (a tree; no
/// cycle/ambiguity). Fact-incident edges have one endpoint outside → excluded.
fn count_intra_edges(shape: &FusionShape, group: &DimGroup) -> usize {
    let set: HashSet<usize> = group.leaf_idxs.iter().copied().collect();
    shape
        .edges
        .iter()
        .filter(|(a, b, _, _)| set.contains(a) && set.contains(b))
        .count()
}

/// Build a group's internal join subtree, anchored at `anchor_leaf` (the leaf
/// holding the dim key the fact FK binds to), joining the rest on intra-group
/// edges. Leaf filters already live inside the leaf subtrees (optimized plan).
fn build_group_subtree(
    shape: &FusionShape,
    group: &DimGroup,
    anchor_leaf: usize,
) -> Option<LogicalPlan> {
    let in_group: HashSet<usize> = group.leaf_idxs.iter().copied().collect();
    let mut included: HashSet<usize> = HashSet::from([anchor_leaf]);
    let mut builder = LogicalPlanBuilder::from(shape.leaves[anchor_leaf].clone());
    while included.len() < group.leaf_idxs.len() {
        let mut progressed = false;
        for (a, b, ca, cb) in &shape.edges {
            if !in_group.contains(a) || !in_group.contains(b) {
                continue;
            }
            let (new_leaf, inc_col, new_col) = if included.contains(a) && !included.contains(b) {
                (*b, ca.clone(), cb.clone())
            } else if included.contains(b) && !included.contains(a) {
                (*a, cb.clone(), ca.clone())
            } else {
                continue;
            };
            // Use `join` with explicit equi-keys (NOT `join_on`, which buries
            // the predicate in a join FILTER → the physical planner emits a
            // NestedLoopJoinExec = O(n·m) catastrophe). `on` keys → HashJoinExec.
            builder = builder
                .join(
                    shape.leaves[new_leaf].clone(),
                    JoinType::Inner,
                    (vec![inc_col], vec![new_col]),
                    None,
                )
                .ok()?;
            included.insert(new_leaf);
            progressed = true;
            break;
        }
        if !progressed {
            return None; // disconnected (tree-check should have caught this)
        }
    }
    builder.build().ok()
}

/// Match the TPC-H revenue idiom `price * (1 - disc)` → `(price_col, disc_col)`.
fn match_revenue(expr: &Expr) -> Option<(String, String)> {
    use datafusion::logical_expr::{BinaryExpr, Operator};
    let e = match expr {
        Expr::Alias(a) => a.expr.as_ref(),
        other => other,
    };
    let Expr::BinaryExpr(BinaryExpr {
        left,
        op: Operator::Multiply,
        right,
    }) = e
    else {
        return None;
    };
    let price = bare_column_name(left)?;
    let Expr::BinaryExpr(BinaryExpr {
        left: one,
        op: Operator::Minus,
        right: disc,
    }) = right.as_ref()
    else {
        return None;
    };
    if !is_one(one) {
        return None;
    }
    Some((price, bare_column_name(disc)?))
}

fn bare_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(c) => Some(c.name.clone()),
        _ => None,
    }
}

fn is_one(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(ScalarValue::Float64(Some(v)), _) if *v == 1.0)
        || matches!(expr, Expr::Literal(ScalarValue::Int64(Some(1)), _))
}

/// Architect S6: rebuild the join region as a fused push pipeline.
///
/// Replaces the emit-projection (the boundary the agg reads) with
/// `Projection[adapter] → (FusedProbe ⋈ stock-group subtrees)`, where the
/// `FusedProbe` node makes ONE fact pass probing every pre-reduced dimension.
/// Everything above the emit-projection (SubqueryAlias / Aggregate / Sort) is
/// preserved byte-for-byte and just re-pointed at the new input.
///
/// Best-effort: returns `None` on any unsupported shape (caller keeps stock).
pub fn reconstruct(plan: &LogicalPlan) -> Option<LogicalPlan> {
    let shape = analyze(plan)?;
    let classes = classify(plan, &shape)?;
    let group_of = leaf_to_group(&shape);

    // S5: ≥1 group fuses, and every fused group reduces on a tree (k-1 intra
    // edges → no cycle/ambiguity). Payload key-uniqueness is enforced at build
    // time by the operator's `require_unique` guard.
    let fused_count = classes
        .iter()
        .filter(|c| !matches!(c, GroupClass::Stock))
        .count();
    if fused_count == 0 {
        return None;
    }
    for (g, c) in shape.groups.iter().zip(&classes) {
        if !matches!(c, GroupClass::Stock)
            && count_intra_edges(&shape, g) != g.leaf_idxs.len().saturating_sub(1)
        {
            return None;
        }
    }

    let emit_proj = emit_projection_above_join(plan)?;
    let fact_leaf = &shape.leaves[shape.fact_idx];
    let fact_schema = fact_leaf.schema();

    // --- Fused node: inputs = [fact, build_0, …]; builds + payload mapping. ---
    let mut inputs: Vec<LogicalPlan> = vec![fact_leaf.clone()];
    let mut builds: Vec<BuildSpec> = Vec::new();
    let mut build_idx_of_group: Vec<Option<usize>> = vec![None; shape.groups.len()];

    for (gi, (g, c)) in shape.groups.iter().zip(&classes).enumerate() {
        let anchor = leaf_of_column(&shape.leaves, &g.dim_key)?;
        let (payload, sub) = match c {
            GroupClass::Membership => {
                let subtree = build_group_subtree(&shape, g, anchor)?;
                let sub = LogicalPlanBuilder::from(subtree)
                    .project([Expr::Column(g.dim_key.clone())])
                    .ok()?
                    .build()
                    .ok()?;
                (None, sub)
            }
            GroupClass::Payload { expr, alias, .. } => {
                let subtree = build_group_subtree(&shape, g, anchor)?;
                let sub = LogicalPlanBuilder::from(subtree)
                    .project([Expr::Column(g.dim_key.clone()), expr.clone().alias(alias)])
                    .ok()?
                    .build()
                    .ok()?;
                (Some(alias.clone()), sub)
            }
            GroupClass::Stock => continue,
        };
        build_idx_of_group[gi] = Some(builds.len());
        builds.push(BuildSpec {
            probe_fk: g.fact_fk.name.clone(),
            build_key: g.dim_key.name.clone(),
            payload,
            require_unique: true, // collapsing INNER joins
        });
        inputs.push(sub);
    }

    // --- Emit + fused output schema, in a fixed order:
    //     [stock-group fact FKs] ++ [fact-only emit exprs] ++ [payloads].   ---
    let mut emit: Vec<EmitSpec> = Vec::new();
    let mut fields: Vec<Field> = Vec::new();

    for (g, c) in shape.groups.iter().zip(&classes) {
        if matches!(c, GroupClass::Stock) {
            let name = g.fact_fk.name.clone();
            let ty = fact_schema
                .field_with_unqualified_name(&name)
                .ok()?
                .data_type()
                .clone();
            emit.push(EmitSpec::ProbeColumn(name.clone()));
            fields.push(Field::new(name, ty, true));
        }
    }
    for (i, expr) in emit_proj.expr.iter().enumerate() {
        if !matches!(emit_source(expr, &shape.leaves, &group_of)?, EmitSrc::Fact) {
            continue;
        }
        let inner = match expr {
            Expr::Alias(a) => a.expr.as_ref(),
            other => other,
        };
        if let Some((price, disc)) = match_revenue(inner) {
            emit.push(EmitSpec::ProbeRevenue { price, disc });
        } else if let Some(name) = bare_column_name(inner) {
            emit.push(EmitSpec::ProbeColumn(name));
        } else {
            return None; // unsupported fact-only emit expr
        }
        fields.push(Field::new(
            emit_proj.schema.field(i).name().clone(),
            emit_proj.schema.field(i).data_type().clone(),
            true,
        ));
    }
    for (gi, c) in classes.iter().enumerate() {
        if let GroupClass::Payload { alias, .. } = c {
            let build_idx = build_idx_of_group[gi]?;
            emit.push(EmitSpec::BuildPayload { build_idx });
            // The operator recovers payloads as Int64 regardless of source type.
            fields.push(Field::new(alias.clone(), DataType::Int64, true));
        }
    }

    let fused_schema: DFSchemaRef = Arc::new(DFSchema::try_from(Schema::new(fields)).ok()?);
    let fused_node = FusedProbeNode {
        inputs,
        builds,
        emit,
        schema: fused_schema,
    };
    let mut result = LogicalPlan::Extension(Extension {
        node: Arc::new(fused_node),
    });

    // --- Join each stock group onto the fused output (fact_fk = dim_key). ---
    for (g, c) in shape.groups.iter().zip(&classes) {
        if !matches!(c, GroupClass::Stock) {
            continue;
        }
        let anchor = leaf_of_column(&shape.leaves, &g.dim_key)?;
        let sub = build_group_subtree(&shape, g, anchor)?;
        // Equi-keys in `on` (fused fact-FK = dim key) → HashJoinExec, not NLJ.
        result = LogicalPlanBuilder::from(result)
            .join(
                sub,
                JoinType::Inner,
                (
                    vec![Column::new_unqualified(g.fact_fk.name.clone())],
                    vec![g.dim_key.clone()],
                ),
                None,
            )
            .ok()?
            .build()
            .ok()?;
    }

    // --- Adapter projection: reproduce the emit-projection's exact columns. ---
    let mut adapter: Vec<Expr> = Vec::with_capacity(emit_proj.expr.len());
    for (i, expr) in emit_proj.expr.iter().enumerate() {
        let alias = emit_proj.schema.field(i).name().clone();
        let out_type = emit_proj.schema.field(i).data_type().clone();
        match emit_source(expr, &shape.leaves, &group_of)? {
            EmitSrc::Fact => adapter.push(col(alias.as_str()).alias(alias)),
            EmitSrc::Group(g) => match &classes[g] {
                GroupClass::Payload { .. } => {
                    // operator emitted Int64 → cast back to the original type.
                    let casted = Expr::Cast(Cast::new(Box::new(col(alias.as_str())), out_type));
                    adapter.push(casted.alias(alias));
                }
                // reuse the original expr — its stock columns are present.
                GroupClass::Stock => adapter.push(expr.clone()),
                GroupClass::Membership => return None,
            },
        }
    }
    let result = LogicalPlanBuilder::from(result)
        .project(adapter)
        .ok()?
        .build()
        .ok()?;

    splice_emit_projection(plan, result)
}

/// Replace the emit-projection (the Projection directly over the join region)
/// with `replacement`, preserving every wrapper above it.
fn splice_emit_projection(plan: &LogicalPlan, replacement: LogicalPlan) -> Option<LogicalPlan> {
    match plan {
        LogicalPlan::Projection(p) if matches!(p.input.as_ref(), LogicalPlan::Join(_)) => {
            Some(replacement)
        }
        LogicalPlan::Sort(_)
        | LogicalPlan::Projection(_)
        | LogicalPlan::Aggregate(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Limit(_) => {
            let input = plan.inputs().first().copied()?;
            let new_input = splice_emit_projection(input, replacement)?;
            plan.with_new_exprs(plan.expressions(), vec![new_input])
                .ok()
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ematix_fast_parquet::EmatixFastParquetTableProvider;
    use crate::fast_parquet::FastParquetTableProvider;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use std::path::PathBuf;
    use std::sync::Arc;

    fn sf1_dir() -> Option<PathBuf> {
        if let Ok(env) = std::env::var("TPCH_DATA_DIR") {
            let p = PathBuf::from(env);
            if p.join("lineitem.parquet").exists() {
                return Some(p);
            }
        }
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let p = manifest.parent()?.parent()?.join("examples/tpch/data/sf1");
        p.join("lineitem.parquet").exists().then_some(p)
    }

    async fn ctx(dir: &std::path::Path) -> SessionContext {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
        for t in [
            "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
        ] {
            let p = dir.join(format!("{t}.parquet"));
            if t == "lineitem" || t == "orders" {
                ctx.register_table(
                    t,
                    Arc::new(EmatixFastParquetTableProvider::try_new(p.to_string_lossy()).unwrap()),
                )
                .unwrap();
            } else {
                ctx.register_table(
                    t,
                    Arc::new(FastParquetTableProvider::try_new(p.to_string_lossy()).unwrap()),
                )
                .unwrap();
            }
        }
        ctx
    }

    /// Q08's optimized logical plan must analyze to: fact = lineitem, three
    /// dim-groups keyed by l_partkey / l_suppkey / l_orderkey, with leaf sets
    /// {part}, {supplier, n2}, {orders, customer, n1, region}.
    #[tokio::test]
    async fn analyzes_q08_star_into_fact_and_three_dim_groups() {
        let Some(dir) = sf1_dir() else {
            eprintln!("skip: no SF=1 data");
            return;
        };
        let ctx = ctx(&dir).await;
        let sql = std::fs::read_to_string(dir.join("../../queries/q08.sql"))
            .or_else(|_| std::fs::read_to_string("examples/tpch/queries/q08.sql"))
            .unwrap();
        let logical = ctx.sql(&sql).await.unwrap().into_optimized_plan().unwrap();

        let shape = analyze(&logical).expect("Q08 must analyze into a fusion shape");

        // We don't hardcode leaf order; identify by the fact-FK column names.
        let fk_names: std::collections::HashSet<&str> = shape
            .groups
            .iter()
            .map(|g| g.fact_fk.name.as_str())
            .collect();
        assert_eq!(
            fk_names,
            ["l_partkey", "l_suppkey", "l_orderkey"]
                .into_iter()
                .collect(),
            "three dim groups keyed by the lineitem FKs"
        );
        // The orders group is the snowflake: 4 leaves {orders,customer,n1,region}.
        let orders_grp = shape
            .groups
            .iter()
            .find(|g| g.fact_fk.name == "l_orderkey")
            .unwrap();
        assert_eq!(
            orders_grp.leaf_idxs.len(),
            4,
            "orders group = orders+customer+n1+region"
        );
        // part group is a single membership leaf.
        let part_grp = shape
            .groups
            .iter()
            .find(|g| g.fact_fk.name == "l_partkey")
            .unwrap();
        assert_eq!(part_grp.leaf_idxs.len(), 1, "part group = {{part}}");
        // supplier group = supplier + n2.
        let supp_grp = shape
            .groups
            .iter()
            .find(|g| g.fact_fk.name == "l_suppkey")
            .unwrap();
        assert_eq!(
            supp_grp.leaf_idxs.len(),
            2,
            "supplier group = {{supplier, n2}}"
        );
    }

    /// A plain 2-table query (no fact-with-≥2-dims star) must NOT analyze.
    #[tokio::test]
    async fn rejects_non_star() {
        let Some(dir) = sf1_dir() else {
            return;
        };
        let ctx = ctx(&dir).await;
        let logical = ctx
            .sql("SELECT l_orderkey FROM lineitem JOIN orders ON l_orderkey = o_orderkey LIMIT 5")
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap();
        assert!(
            analyze(&logical).is_none(),
            "2-table join is not a fusable star"
        );
    }

    /// Q08 S4 classification: part = Membership (only its key escapes),
    /// orders = Payload(o_year, Int32 — an extract(year) i64), supplier+n2 =
    /// Stock (the agg's CASE reads the Utf8 `n_name`).
    #[tokio::test]
    async fn classifies_q08_groups_membership_payload_stock() {
        let Some(dir) = sf1_dir() else {
            eprintln!("skip: no SF=1 data");
            return;
        };
        let ctx = ctx(&dir).await;
        let sql = std::fs::read_to_string(dir.join("../../queries/q08.sql"))
            .or_else(|_| std::fs::read_to_string("examples/tpch/queries/q08.sql"))
            .unwrap();
        let logical = ctx.sql(&sql).await.unwrap().into_optimized_plan().unwrap();

        let shape = analyze(&logical).expect("Q08 analyzes");
        let classes = classify(&logical, &shape).expect("Q08 classifies");
        assert_eq!(classes.len(), shape.groups.len());

        for (g, c) in shape.groups.iter().zip(&classes) {
            match g.fact_fk.name.as_str() {
                "l_partkey" => assert!(
                    matches!(c, GroupClass::Membership),
                    "part group is membership, got {c:?}"
                ),
                "l_orderkey" => assert!(
                    matches!(
                        c,
                        GroupClass::Payload { alias, out_type, .. }
                            if alias == "o_year"
                                && matches!(out_type, datafusion::arrow::datatypes::DataType::Int32)
                    ),
                    "orders group carries the o_year i64 payload, got {c:?}"
                ),
                "l_suppkey" => assert!(
                    matches!(c, GroupClass::Stock),
                    "supplier group (Utf8 n_name) stays stock, got {c:?}"
                ),
                other => panic!("unexpected fact FK {other}"),
            }
        }
        // part + orders fuse; supplier stays stock.
        let fused = classes
            .iter()
            .filter(|c| !matches!(c, GroupClass::Stock))
            .count();
        assert_eq!(
            fused, 2,
            "exactly two groups fuse (part membership + orders payload)"
        );
    }

    /// Q08 S6: reconstruct must produce a plan containing the FusedProbe node
    /// AND preserve the original top-level output schema exactly (names + types
    /// — so the agg/sort above splice in unchanged).
    #[tokio::test]
    async fn reconstructs_q08_into_fused_probe_preserving_schema() {
        let Some(dir) = sf1_dir() else {
            eprintln!("skip: no SF=1 data");
            return;
        };
        let ctx = ctx(&dir).await;
        let sql = std::fs::read_to_string(dir.join("../../queries/q08.sql"))
            .or_else(|_| std::fs::read_to_string("examples/tpch/queries/q08.sql"))
            .unwrap();
        let logical = ctx.sql(&sql).await.unwrap().into_optimized_plan().unwrap();
        let original_schema = logical.schema().clone();

        let rebuilt = reconstruct(&logical).expect("Q08 reconstructs");

        let rendered = format!("{}", rebuilt.display_indent());
        assert!(
            rendered.contains("FusedProbe"),
            "plan must contain the FusedProbe node:\n{rendered}"
        );

        let new_schema = rebuilt.schema();
        assert_eq!(
            new_schema.fields().len(),
            original_schema.fields().len(),
            "output arity preserved"
        );
        for (a, b) in original_schema
            .fields()
            .iter()
            .zip(new_schema.fields().iter())
        {
            assert_eq!(a.name(), b.name(), "output column name preserved");
            assert_eq!(
                a.data_type(),
                b.data_type(),
                "output type preserved for `{}`",
                a.name()
            );
        }
    }

    /// Q15 is a pseudo-star: its top join `revenue.total_revenue = max(...)` is
    /// an equi-edge on a Float64 column (a decorrelated scalar subquery), which
    /// the fused probe cannot test. The S5 integer-key gate MUST reject it —
    /// else reconstruct fuses a Float64 key, resolves every key to None, and
    /// drops every row (0 rows instead of 1). Regression for the prod-A/A miss.
    #[tokio::test]
    async fn rejects_q15_float_keyed_pseudo_star() {
        let Some(dir) = sf1_dir() else {
            eprintln!("skip: no SF=1 data");
            return;
        };
        let ctx = ctx(&dir).await;
        let sql = std::fs::read_to_string(dir.join("../../queries/q15.sql"))
            .or_else(|_| std::fs::read_to_string("examples/tpch/queries/q15.sql"))
            .unwrap();
        let logical = ctx.sql(&sql).await.unwrap().into_optimized_plan().unwrap();
        assert!(
            analyze(&logical).is_none(),
            "Q15's Float64-keyed pseudo-star must not analyze as fusable"
        );
        assert!(
            reconstruct(&logical).is_none(),
            "Q15 must bail out of reconstruction"
        );
    }
}
