//! Q10 wide-string late-materialization (prod-B) — the SOUND core: computing the
//! FD-minimal grouping key from proven functional dependencies.
//!
//! The late-materialization rewrite groups on a compact key (the FD-minimal subset
//! of the group-by columns) and re-attaches the functionally-determined columns at
//! the aggregate output. Correctness HINGES on the reduced key `K` being a true
//! superkey of the original group columns — i.e. `K` functionally determines every
//! original group column. This module computes `K` from the declared-PK-derived
//! functional dependencies (see [`crate::ematix_fast_parquet::EmatixFastParquetTableProvider::with_primary_key`])
//! using closure under the FDs. If no PROPER reduction is provable, it returns
//! `None` and the rule must not fire — a wrong `K` would silently change results.
//!
//! This file is the detector + its tests only; the recognizer rule, the logical
//! extension node, and the physical `ExtensionPlanner` (prod-B/C) build on it.

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashSet, VecDeque};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{
    Column, Constraint, DFSchemaRef, DataFusionError, FunctionalDependencies, Result,
};
use datafusion::logical_expr::{
    Aggregate, Expr, Extension, JoinType, LogicalPlan, LogicalPlanBuilder, TableScan,
    UserDefinedLogicalNodeCore,
};

use crate::join_reorder::flatten_inner_join_chain;

/// Functional-dependency closure of `seed` under `fds`: the full set of columns
/// determined by `seed` (fixpoint — adds an FD's targets whenever its full source
/// is already in the set). Indices are positions in the schema the FDs describe.
pub(crate) fn fd_closure(seed: &BTreeSet<usize>, fds: &FunctionalDependencies) -> BTreeSet<usize> {
    let mut closure = seed.clone();
    loop {
        let mut grew = false;
        for fd in fds.iter() {
            if fd.source_indices.iter().all(|s| closure.contains(s)) {
                for &t in &fd.target_indices {
                    if closure.insert(t) {
                        grew = true;
                    }
                }
            }
        }
        if !grew {
            break;
        }
    }
    closure
}

/// Compute the FD-minimal grouping key: the smallest subset `K` of `group_cols`
/// (schema column indices) that functionally determines ALL of `group_cols` under
/// the proven dependencies `fds`. Returns `Some(K)` (ascending, a PROPER subset of
/// `group_cols`) when a reduction is provable, else `None`.
///
/// SOUND: a group column is dropped from the key only when the REMAINING key still
/// determines it via `fds` (closure check) — so grouping by `K` yields exactly the
/// same groups as grouping by all of `group_cols`. Greedy minimal-key reduction:
/// each column is removed iff the rest remain a superkey. Returns `None` if
/// `group_cols` has duplicates-collapsed size < 2, if there are no usable FDs, or
/// if no column can be removed (no reduction possible).
pub fn fd_minimal_group_key(
    group_cols: &[usize],
    fds: &FunctionalDependencies,
) -> Option<Vec<usize>> {
    let gset: BTreeSet<usize> = group_cols.iter().copied().collect();
    if gset.len() < 2 || fds.is_empty() {
        return None;
    }
    // Greedy reduction: drop a column whenever the remaining key still determines
    // the FULL original group set. Iterate in a stable order for determinism.
    let mut key = gset.clone();
    for &g in gset.iter() {
        if !key.contains(&g) {
            continue;
        }
        let mut trial = key.clone();
        trial.remove(&g);
        if !trial.is_empty() && fd_closure(&trial, fds).is_superset(&gset) {
            key = trial;
        }
    }
    // Only fire on a real reduction, and re-verify the soundness invariant.
    if key.len() < gset.len() && fd_closure(&key, fds).is_superset(&gset) {
        Some(key.into_iter().collect())
    } else {
        None
    }
}

// ===================================================================
// prod-B.2: the late-materialization recognizer + logical extension node.
//
// `analyze` recognizes the sound late-mat shape on the optimized logical plan;
// `reconstruct` rewrites the recognized `Aggregate` into a [`LateMatAggNode`]
// the [`crate::late_mat_agg_planner::LateMatAggPlanner`] turns into the proven
// physical subtree (EmatixHashJoinExec[BuildRowId] → Repartition(rowid) →
// AggregateExec(rowid) → LateGatherExec). Modeled on the `push_fusion_rule`
// recognizer + `FusedProbeNode`/`FusedProbePlanner` pair.
// ===================================================================

/// `true` unless `EMAT_LATE_MAT_AGG=0` — DEFAULT-ON (opt-out) as of 2026-06-23,
/// after the gates cleared: fires Q10-only (shape gate ≥3 wide strings), correct
/// everywhere, SF=10/SF=100 22q regression-free (declaring PKs is plan-inert on
/// all 22 — `fd_plandiff`), Q10 SF=100 −24% in-sweep / −31% isolated (beats
/// DuckDB). Inert without declared PKs + the wide-string star shape, so default-on
/// is safe for any catalog. Read once at physical-planning time, never hot.
pub fn enabled() -> bool {
    crate::flags::enabled("EMAT_LATE_MAT_AGG")
}

/// The recognized + validated late-materialization shape. SOUND by construction:
/// the build is 1:1 with the anchor (every folded dim joins via its own declared
/// PK → many-to-one), every group column lives in the build, and the anchor PK
/// (the rowid spine) is a group column — so grouping the build-rowid is identical
/// to grouping the full wide key, and the wide columns are gathered at the (far
/// smaller) aggregate outputs from the resident build instead of carried through
/// the join.
pub struct LateMatShape {
    /// Build subplan = anchor ⋈ folded dims, projected to the group columns in
    /// group-by order (so build column `i` ↔ aggregate output column `i`).
    pub build: LogicalPlan,
    /// Probe subplan = the fact chain, projected to `[probe_fk, agg-arg cols…]`.
    pub probe: LogicalPlan,
    /// Index of the anchor PK (the rowid spine) within the build projection.
    pub build_key_pos: usize,
    /// Number of group-by columns (the leading build/aggregate output columns).
    pub n_group: usize,
    /// The aggregate functions (over probe columns), rebuilt physically over the
    /// join output by the planner.
    pub aggr_expr: Vec<Expr>,
    /// The original `Aggregate` output schema — the node's output, so the
    /// Projection/Sort above splice in unchanged.
    pub schema: DFSchemaRef,
}

/// Descend the post-aggregate wrappers to the first `Aggregate`.
fn first_aggregate(plan: &LogicalPlan) -> Option<&Aggregate> {
    match plan {
        LogicalPlan::Aggregate(a) => Some(a),
        LogicalPlan::Sort(_)
        | LogicalPlan::Projection(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Limit(_) => first_aggregate(plan.inputs().first()?),
        _ => None,
    }
}

/// Descend the post-join wrappers (Projection/Filter/SubqueryAlias) to the first
/// Inner-Join-rooted subtree.
pub(crate) fn join_root(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    match plan {
        LogicalPlan::Join(_) => Some(plan),
        LogicalPlan::Projection(_) | LogicalPlan::Filter(_) | LogicalPlan::SubqueryAlias(_) => {
            join_root(plan.inputs().first()?)
        }
        _ => None,
    }
}

/// Which leaf (by index) owns `col`? Matches the qualified column against each
/// leaf's output schema (the flattened-chain leaves carry qualified columns).
pub(crate) fn leaf_of_column(leaves: &[LogicalPlan], col: &Column) -> Option<usize> {
    leaves
        .iter()
        .position(|l| l.schema().columns().iter().any(|c| c == col))
}

/// Descend a chain leaf (Filter/Projection/SubqueryAlias/…) to its `TableScan`.
fn table_scan_of(plan: &LogicalPlan) -> Option<&TableScan> {
    match plan {
        LogicalPlan::TableScan(t) => Some(t),
        LogicalPlan::Filter(_)
        | LogicalPlan::Projection(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::Sort(_) => table_scan_of(plan.inputs().first()?),
        _ => None,
    }
}

/// The declared primary-key column NAMES of a chain leaf (empty if none). Reads
/// the `TableSource::constraints()` PrimaryKey (indices into the table's FULL
/// schema) → column names — the soundness witness for many-to-one dim folds.
pub(crate) fn leaf_pk_names(leaf: &LogicalPlan) -> Vec<String> {
    let Some(scan) = table_scan_of(leaf) else {
        return vec![];
    };
    let Some(cons) = scan.source.constraints() else {
        return vec![];
    };
    let full = scan.source.schema();
    for c in cons.iter() {
        if let Constraint::PrimaryKey(idxs) = c {
            return idxs
                .iter()
                .filter_map(|&i| full.fields().get(i).map(|f| f.name().clone()))
                .collect();
        }
    }
    vec![]
}

/// Build a connected join subtree over the leaf indices in `set`, anchored at
/// `anchor`, joining the rest on intra-set equi-edges (explicit equi-keys so the
/// planner emits HashJoinExec, never a NestedLoopJoin). Mirrors
/// `push_fusion_rule::build_group_subtree`.
fn build_subtree(
    leaves: &[LogicalPlan],
    edges: &[(usize, usize, Column, Column)],
    set: &[usize],
    anchor: usize,
) -> Option<LogicalPlan> {
    let in_set: HashSet<usize> = set.iter().copied().collect();
    let mut included: HashSet<usize> = HashSet::from([anchor]);
    let mut builder = LogicalPlanBuilder::from(leaves[anchor].clone());
    while included.len() < set.len() {
        let mut progressed = false;
        for (a, b, ca, cb) in edges {
            if !in_set.contains(a) || !in_set.contains(b) {
                continue;
            }
            let (newl, inc_col, new_col) = if included.contains(a) && !included.contains(b) {
                (*b, ca.clone(), cb.clone())
            } else if included.contains(b) && !included.contains(a) {
                (*a, cb.clone(), ca.clone())
            } else {
                continue;
            };
            builder = builder
                .join(
                    leaves[newl].clone(),
                    JoinType::Inner,
                    (vec![inc_col], vec![new_col]),
                    None,
                )
                .ok()?;
            included.insert(newl);
            progressed = true;
            break;
        }
        if !progressed {
            return None; // disconnected set
        }
    }
    builder.build().ok()
}

/// Recognize the sound late-materialization shape on `agg` (or `None` to bail).
fn analyze_agg(agg: &Aggregate) -> Option<LateMatShape> {
    let in_schema = agg.input.schema();
    let fds = in_schema.functional_dependencies();

    // Group columns → indices + qualified columns; bail on any non-column group.
    let mut group_idx = Vec::with_capacity(agg.group_expr.len());
    let mut group_col = Vec::with_capacity(agg.group_expr.len());
    for ge in &agg.group_expr {
        let Expr::Column(c) = ge else {
            return None;
        };
        group_idx.push(in_schema.index_of_column(c).ok()?);
        group_col.push(c.clone());
    }
    if group_idx.len() < 2 {
        return None;
    }

    // A real FD reduction must exist (else nothing wide to late-materialize).
    fd_minimal_group_key(&group_idx, fds)?;

    // SHAPE GATE: late-materialization pays only when SEVERAL wide string columns
    // would otherwise be carried through the join intermediate. Count the
    // string-typed group columns; require ≥ EMAT_LM_MIN_WIDE_COLS (default 3).
    // Q10 carries 5 wide strings (the win); a 1-wide-string aggregate whose other
    // group cols are numeric (e.g. Q18 after the preset walkers reshape it) does
    // NOT pay for the CollectLeft build + reattach (measured SF=10: +28%), so it
    // is gated out. Shape-based + general — NOT TPC-H-keyed.
    let min_wide = crate::flags::usize_or("EMAT_LM_MIN_WIDE_COLS", 3);
    let wide_cols = group_idx
        .iter()
        .filter(|&&i| {
            matches!(
                in_schema.field(i).data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            )
        })
        .count();
    if wide_cols < min_wide {
        return None;
    }

    // Anchor PK = the group column whose FD-closure covers the most group columns
    // (≥2 → it actually determines wide columns to drop from the carried key).
    let gset: BTreeSet<usize> = group_idx.iter().copied().collect();
    let mut best: Option<(usize, usize)> = None;
    for &pk in &group_idx {
        let clos = fd_closure(&BTreeSet::from([pk]), fds);
        let covered = group_idx.iter().filter(|g| clos.contains(g)).count();
        if covered >= 2 && best.map(|(_, c)| covered > c).unwrap_or(true) {
            best = Some((pk, covered));
        }
    }
    let (anchor_pk_idx, _) = best?;
    let anchor_pk_col = group_col[group_idx.iter().position(|&g| g == anchor_pk_idx)?].clone();
    let _ = gset; // (kept for clarity of the closure-coverage intent)

    // Flatten the inner-join region under the aggregate.
    let chain = flatten_inner_join_chain(join_root(&agg.input)?)?;
    if chain.extra_filter.is_some() {
        return None; // residual non-equi join filter → bail (conservative)
    }
    let leaves = chain.leaves;
    if leaves.len() < 2 {
        return None;
    }

    // Resolve every equi-edge to a (leaf, leaf, col, col) tuple.
    let mut edges: Vec<(usize, usize, Column, Column)> = Vec::with_capacity(chain.equi_preds.len());
    for (a, b) in &chain.equi_preds {
        let (la, lb) = (leaf_of_column(&leaves, a)?, leaf_of_column(&leaves, b)?);
        if la == lb {
            return None;
        }
        edges.push((la, lb, a.clone(), b.clone()));
    }

    // Anchor leaf = the leaf carrying the anchor PK column.
    let anchor_leaf = leaf_of_column(&leaves, &anchor_pk_col)?;

    // BFS the BUILD component: fold a neighbor into the build iff it joins via its
    // OWN single-column declared PK (a many-to-one edge → the build stays 1:1 with
    // the anchor, so the build-rowid functionally determines every build column).
    let mut in_build = vec![false; leaves.len()];
    in_build[anchor_leaf] = true;
    let mut q = VecDeque::from([anchor_leaf]);
    while let Some(cur) = q.pop_front() {
        for (a, b, ca, cb) in &edges {
            let (other, other_col) = if *a == cur {
                (*b, cb)
            } else if *b == cur {
                (*a, ca)
            } else {
                continue;
            };
            if in_build[other] {
                continue;
            }
            let pks = leaf_pk_names(&leaves[other]);
            if pks.len() == 1 && pks[0] == other_col.name {
                in_build[other] = true;
                q.push_back(other);
            }
        }
    }

    // Every group column must live in the BUILD (else it can't be gathered).
    for gc in &group_col {
        if !in_build[leaf_of_column(&leaves, gc)?] {
            return None;
        }
    }

    // Aggregate-argument columns must all live in the PROBE (fact) side.
    let mut agg_cols: Vec<Column> = Vec::new();
    for ae in &agg.aggr_expr {
        for c in ae.column_refs() {
            if in_build[leaf_of_column(&leaves, c)?] {
                return None; // aggregate reads a build column → degenerate
            }
            if !agg_cols.contains(c) {
                agg_cols.push(c.clone());
            }
        }
    }
    if agg_cols.is_empty() {
        return None;
    }

    // Exactly ONE build↔probe equi-edge = the anchor-PK = fact-FK join key
    // (a single-key CollectLeft; the build cross column must be the anchor PK).
    let mut cross: Option<(Column, Column)> = None;
    for (a, b, ca, cb) in &edges {
        if in_build[*a] == in_build[*b] {
            continue; // intra-build or intra-probe
        }
        let pair = if in_build[*a] {
            (ca.clone(), cb.clone())
        } else {
            (cb.clone(), ca.clone())
        };
        if cross.is_some() {
            return None; // >1 cross edge → unsupported in v1
        }
        cross = Some(pair);
    }
    let (build_cross, probe_cross) = cross?;
    if build_cross.name != anchor_pk_col.name {
        return None;
    }

    // ---- Build subtree (build leaves) projected to the group columns. ----
    let build_idxs: Vec<usize> = (0..leaves.len()).filter(|&i| in_build[i]).collect();
    let build_tree = build_subtree(&leaves, &edges, &build_idxs, anchor_leaf)?;
    let build = LogicalPlanBuilder::from(build_tree)
        .project(group_col.iter().cloned().map(Expr::Column))
        .ok()?
        .build()
        .ok()?;
    let build_key_pos = group_col
        .iter()
        .position(|c| c.name == anchor_pk_col.name)?;

    // ---- Probe subtree (probe leaves) projected to [probe_fk, agg-arg cols…]. ----
    let probe_idxs: Vec<usize> = (0..leaves.len()).filter(|&i| !in_build[i]).collect();
    let probe_anchor = leaf_of_column(&leaves, &probe_cross)?;
    let probe_tree = build_subtree(&leaves, &edges, &probe_idxs, probe_anchor)?;
    let mut probe_proj: Vec<Expr> = vec![Expr::Column(probe_cross.clone())];
    for c in &agg_cols {
        if c.name != probe_cross.name {
            probe_proj.push(Expr::Column(c.clone()));
        }
    }
    let probe = LogicalPlanBuilder::from(probe_tree)
        .project(probe_proj)
        .ok()?
        .build()
        .ok()?;

    Some(LateMatShape {
        build,
        probe,
        build_key_pos,
        n_group: group_col.len(),
        aggr_expr: agg.aggr_expr.clone(),
        schema: agg.schema.clone(),
    })
}

/// Recognize the late-mat shape on the (optimized) plan: descend to the first
/// `Aggregate` and analyze it. Pure; the entry point for tests + reconstruct.
pub fn analyze(plan: &LogicalPlan) -> Option<LateMatShape> {
    analyze_agg(first_aggregate(plan)?)
}

/// The late-materialization logical node. Inputs are `[build, probe]`; the
/// planner expands it into the EmatixHashJoinExec(BuildRowId)+agg+LateGather
/// subtree. Output schema = the original `Aggregate` output (group cols ++ agg
/// cols), so the wrappers above splice in unchanged. Modeled on `FusedProbeNode`.
#[derive(Clone)]
pub struct LateMatAggNode {
    /// `[build, probe]`.
    pub inputs: Vec<LogicalPlan>,
    /// Index of the anchor PK within the build projection (the join build key).
    pub build_key_pos: usize,
    /// Number of leading group-by output columns (gathered from the build).
    pub n_group: usize,
    /// The aggregate functions over probe columns (rebuilt physically).
    pub aggr_expr: Vec<Expr>,
    /// Output schema (= the replaced `Aggregate`'s schema).
    pub schema: DFSchemaRef,
}

impl fmt::Debug for LateMatAggNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LateMatAggNode(groups={}, aggs={}, build_key@{})",
            self.n_group,
            self.aggr_expr.len(),
            self.build_key_pos
        )
    }
}

// Comparison traits over the SEMANTIC fields only (schema is derived). Expr is
// Eq+Hash but not Ord, so PartialOrd folds in a string rendering of aggr_expr.
impl PartialEq for LateMatAggNode {
    fn eq(&self, o: &Self) -> bool {
        self.inputs == o.inputs
            && self.build_key_pos == o.build_key_pos
            && self.n_group == o.n_group
            && self.aggr_expr == o.aggr_expr
    }
}
impl Eq for LateMatAggNode {}
impl PartialOrd for LateMatAggNode {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        let render = |n: &Self| {
            n.aggr_expr
                .iter()
                .map(|e| format!("{e}"))
                .collect::<Vec<_>>()
        };
        (&self.inputs, self.build_key_pos, self.n_group, render(self)).partial_cmp(&(
            &o.inputs,
            o.build_key_pos,
            o.n_group,
            render(o),
        ))
    }
}
impl Hash for LateMatAggNode {
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.inputs.hash(h);
        self.build_key_pos.hash(h);
        self.n_group.hash(h);
        for e in &self.aggr_expr {
            e.hash(h);
        }
    }
}

impl UserDefinedLogicalNodeCore for LateMatAggNode {
    fn name(&self) -> &str {
        "LateMatAggregate"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs.iter().collect()
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<Expr> {
        // The agg exprs reference probe columns internal to a planned child; the
        // node exposes none for the optimizer to rewrite (it is created post-
        // optimization and planned immediately, like FusedProbeNode).
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LateMatAggregate: groups={}, aggs={}, build_key@{}",
            self.n_group,
            self.aggr_expr.len(),
            self.build_key_pos
        )
    }
    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if inputs.len() != 2 {
            return Err(DataFusionError::Internal(format!(
                "LateMatAggregate expects 2 inputs, got {}",
                inputs.len()
            )));
        }
        Ok(Self {
            inputs,
            build_key_pos: self.build_key_pos,
            n_group: self.n_group,
            aggr_expr: self.aggr_expr.clone(),
            schema: self.schema.clone(),
        })
    }
}

/// Rewrite the recognized `Aggregate` into a [`LateMatAggNode`], preserving every
/// wrapper above it (Sort/Projection/Limit/…). Best-effort: returns `None` on any
/// unsupported shape (caller keeps the stock plan). Mirrors
/// `push_fusion_rule::reconstruct`.
pub fn reconstruct(plan: &LogicalPlan) -> Option<LogicalPlan> {
    replace_aggregate(plan)
}

fn replace_aggregate(plan: &LogicalPlan) -> Option<LogicalPlan> {
    match plan {
        LogicalPlan::Aggregate(agg) => {
            let shape = analyze_agg(agg)?;
            let node = LateMatAggNode {
                inputs: vec![shape.build, shape.probe],
                build_key_pos: shape.build_key_pos,
                n_group: shape.n_group,
                aggr_expr: shape.aggr_expr,
                schema: shape.schema,
            };
            Some(LogicalPlan::Extension(Extension {
                node: Arc::new(node),
            }))
        }
        LogicalPlan::Sort(_)
        | LogicalPlan::Projection(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Limit(_) => {
            let input = plan.inputs().first().copied()?;
            let new_input = replace_aggregate(input)?;
            plan.with_new_exprs(plan.expressions(), vec![new_input])
                .ok()
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::FunctionalDependence;

    fn fds(deps: Vec<(Vec<usize>, Vec<usize>)>) -> FunctionalDependencies {
        FunctionalDependencies::new(
            deps.into_iter()
                .map(|(s, t)| FunctionalDependence::new(s, t, false))
                .collect(),
        )
    }

    /// Q10 shape: group cols [c_custkey=0, c_name=1, c_acctbal=2, c_phone=3,
    /// n_name=4, c_address=5, c_comment=6]; FD {0} -> {1,2,3,5,6} (customer PK;
    /// n_name@4 NOT covered — it's from nation). FD-minimal key = {0, 4}.
    #[test]
    fn q10_shape_reduces_to_custkey_plus_nname() {
        let group = [0usize, 1, 2, 3, 4, 5, 6];
        let f = fds(vec![(vec![0], vec![1, 2, 3, 5, 6])]);
        let k = fd_minimal_group_key(&group, &f).expect("reducible");
        assert_eq!(k, vec![0, 4], "key = c_custkey + the un-covered n_name");
    }

    /// Full coverage: {0} -> {1,2,3,4,5,6} (a single-table PK group) → key = {0}.
    #[test]
    fn full_pk_coverage_reduces_to_single_key() {
        let group = [0usize, 1, 2, 3, 4, 5, 6];
        let f = fds(vec![(vec![0], vec![1, 2, 3, 4, 5, 6])]);
        assert_eq!(fd_minimal_group_key(&group, &f), Some(vec![0]));
    }

    /// No FD → no reduction.
    #[test]
    fn no_fd_no_reduction() {
        let group = [0usize, 1, 2];
        assert_eq!(fd_minimal_group_key(&group, &fds(vec![])), None);
    }

    /// FD exists but its source is OUTSIDE the group set → cannot reduce (the
    /// determinant isn't grouped). {7} -> {1} but 7 not in group.
    #[test]
    fn fd_source_outside_group_no_reduction() {
        let group = [0usize, 1, 2];
        let f = fds(vec![(vec![7], vec![1])]);
        assert_eq!(fd_minimal_group_key(&group, &f), None);
    }

    /// All group cols mutually independent (FD targets none of them) → no reduction.
    #[test]
    fn independent_cols_no_reduction() {
        let group = [0usize, 1, 2];
        let f = fds(vec![(vec![0], vec![9, 10])]); // determines non-group cols only
        assert_eq!(fd_minimal_group_key(&group, &f), None);
    }

    /// Single group column never reduces.
    #[test]
    fn single_col_no_reduction() {
        let f = fds(vec![(vec![0], vec![1])]);
        assert_eq!(fd_minimal_group_key(&[0], &f), None);
    }

    /// Transitive chain: {0}->{1}, {1}->{2}. Closure of {0} covers {0,1,2} → key={0}.
    #[test]
    fn transitive_chain_reduces_via_closure() {
        let group = [0usize, 1, 2];
        let f = fds(vec![(vec![0], vec![1]), (vec![1], vec![2])]);
        assert_eq!(fd_minimal_group_key(&group, &f), Some(vec![0]));
    }

    // ============== prod-B.2: recognizer + reconstruct (real Q10 plan) ==============

    use crate::ematix_fast_parquet::EmatixFastParquetTableProvider;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use std::path::{Path, PathBuf};

    fn sf1_dir() -> Option<PathBuf> {
        if let Ok(env) = std::env::var("TPCH_DATA_DIR") {
            let p = PathBuf::from(env);
            if p.join("customer.parquet").exists() {
                return Some(p);
            }
        }
        let m = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let p = m.parent()?.parent()?.join("examples/tpch/data/sf1");
        p.join("customer.parquet").exists().then_some(p)
    }

    fn q10_sql(dir: &Path) -> String {
        let s = std::fs::read_to_string("examples/tpch/queries/q10.sql")
            .or_else(|_| std::fs::read_to_string(dir.join("../../queries/q10.sql")))
            .unwrap();
        s.trim().trim_end_matches(';').to_string()
    }

    fn prov(dir: &Path, t: &str, pk: Option<usize>) -> EmatixFastParquetTableProvider {
        let p = dir.join(format!("{t}.parquet"));
        let mut prov = EmatixFastParquetTableProvider::try_new(p.to_string_lossy()).unwrap();
        if let Some(i) = pk {
            prov = prov.with_primary_key(vec![i]);
        }
        prov
    }

    /// Register the four Q10 tables with declared PKs (customer.c_custkey@0,
    /// orders.o_orderkey@0, nation.n_nationkey@0; lineitem = the unconstrained fact).
    async fn q10_ctx(dir: &Path) -> SessionContext {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
        ctx.register_table("customer", Arc::new(prov(dir, "customer", Some(0))))
            .unwrap();
        ctx.register_table("orders", Arc::new(prov(dir, "orders", Some(0))))
            .unwrap();
        ctx.register_table("lineitem", Arc::new(prov(dir, "lineitem", None)))
            .unwrap();
        ctx.register_table("nation", Arc::new(prov(dir, "nation", Some(0))))
            .unwrap();
        ctx
    }

    /// Q10 recognizes: build = customer ⋈ nation (carries the 7 group cols),
    /// probe = orders ⋈ lineitem (o_custkey + the revenue cols), build key = c_custkey.
    #[tokio::test]
    async fn analyzes_q10_into_customer_nation_build_and_orders_lineitem_probe() {
        let Some(dir) = sf1_dir() else {
            eprintln!("skip: no SF=1 data");
            return;
        };
        let ctx = q10_ctx(&dir).await;
        let logical = ctx
            .sql(&q10_sql(&dir))
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap();

        let shape = analyze(&logical).expect("Q10 must recognize a late-mat shape");
        assert_eq!(shape.n_group, 7, "Q10 groups on 7 columns");
        assert_eq!(shape.aggr_expr.len(), 1, "one SUM aggregate");

        let build_cols: Vec<String> = shape
            .build
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(
            build_cols.len(),
            7,
            "build projects exactly the 7 group cols"
        );
        assert!(build_cols.iter().any(|c| c == "c_custkey"));
        assert!(build_cols.iter().any(|c| c == "n_name"), "n_name folded in");
        let build_dump = format!("{}", shape.build.display_indent());
        assert!(
            build_dump.contains("customer") && build_dump.contains("nation"),
            "build = customer ⋈ nation:\n{build_dump}"
        );
        assert!(
            !build_dump.contains("orders") && !build_dump.contains("lineitem"),
            "build excludes the fact side:\n{build_dump}"
        );

        let probe_cols: Vec<String> = shape
            .probe
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert!(probe_cols.iter().any(|c| c == "o_custkey"));
        assert!(probe_cols.iter().any(|c| c == "l_extendedprice"));
        assert!(probe_cols.iter().any(|c| c == "l_discount"));
        let probe_dump = format!("{}", shape.probe.display_indent());
        assert!(probe_dump.contains("orders") && probe_dump.contains("lineitem"));
        assert!(
            !probe_dump.contains("nation"),
            "probe excludes the build dims"
        );

        assert_eq!(
            shape.build.schema().field(shape.build_key_pos).name(),
            "c_custkey",
            "build join key = the anchor PK"
        );
    }

    /// `reconstruct` replaces the Aggregate with the node and preserves the exact
    /// top-level output schema (names + types) so Projection/Sort splice unchanged.
    #[tokio::test]
    async fn reconstruct_q10_preserves_output_schema() {
        let Some(dir) = sf1_dir() else {
            return;
        };
        let ctx = q10_ctx(&dir).await;
        let logical = ctx
            .sql(&q10_sql(&dir))
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap();
        let orig = logical.schema().clone();

        let rebuilt = reconstruct(&logical).expect("Q10 reconstructs");
        let dump = format!("{}", rebuilt.display_indent());
        assert!(
            dump.contains("LateMatAggregate"),
            "plan must contain the node:\n{dump}"
        );

        let new = rebuilt.schema();
        assert_eq!(new.fields().len(), orig.fields().len(), "arity preserved");
        for (a, b) in orig.fields().iter().zip(new.fields().iter()) {
            assert_eq!(a.name(), b.name(), "output column name preserved");
            assert_eq!(
                a.data_type(),
                b.data_type(),
                "output type preserved for `{}`",
                a.name()
            );
        }
    }

    /// No declared PKs → no FD on the aggregate input → no reduction → bail. An
    /// un-annotated catalog is left entirely on the stock path.
    #[tokio::test]
    async fn no_pk_no_recognition() {
        let Some(dir) = sf1_dir() else {
            return;
        };
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
        for t in ["customer", "orders", "lineitem", "nation"] {
            ctx.register_table(t, Arc::new(prov(&dir, t, None)))
                .unwrap();
        }
        let logical = ctx
            .sql(&q10_sql(&dir))
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap();
        assert!(
            analyze(&logical).is_none(),
            "no PK → no FD → no late-mat recognition"
        );
    }

    /// SOUNDNESS GATE: with customer's PK but WITHOUT nation's PK, n_name's dim
    /// cannot be proven many-to-one, so it can't be folded into the build → n_name
    /// is neither build nor fact → recognition MUST bail (a wrong fold would risk
    /// fanning the build out of 1:1 with the anchor and corrupting the groups).
    #[tokio::test]
    async fn requires_dim_pk_to_fold_nation() {
        let Some(dir) = sf1_dir() else {
            return;
        };
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
        ctx.register_table("customer", Arc::new(prov(&dir, "customer", Some(0))))
            .unwrap();
        ctx.register_table("orders", Arc::new(prov(&dir, "orders", Some(0))))
            .unwrap();
        ctx.register_table("lineitem", Arc::new(prov(&dir, "lineitem", None)))
            .unwrap();
        // nation WITHOUT a PK → the customer⋈nation fold is unprovable.
        ctx.register_table("nation", Arc::new(prov(&dir, "nation", None)))
            .unwrap();
        let logical = ctx
            .sql(&q10_sql(&dir))
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap();
        assert!(
            analyze(&logical).is_none(),
            "n_name's dim has no PK → cannot fold → must bail"
        );
    }
}
