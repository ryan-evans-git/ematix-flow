//! P3 physical planner + executor: orient a [`BoundQuery`]'s join graph
//! into a tree and run it — the last leg of `SQL → AST → bind → plan →
//! execute`.
//!
//! Physical decisions made here (not at bind time):
//! - **Root selection**: the join tree is rooted at the largest table by
//!   parquet row count — the fact side rows flow through; every other table
//!   becomes a dim subtree consumed into a key→(count, payload) map. A tiny
//!   cost-based decision, the seed of the optimizer to come.
//! - **Join execution**: a dim subtree with no referenced output columns
//!   reduces to key→match-count (the no-materialization semijoin narrow,
//!   with selection-index multiplicity for duplicate keys). A dim whose
//!   columns feed SELECT/GROUP BY carries them as **payloads** that bubble
//!   up the tree and attach to the root's chunks as full-length columns —
//!   payload dims require unique join keys (checked at run time; the
//!   duplicate-key payload join is a labelled follow-on).
//! - **Scan routing**: tables whose projection includes strings decode via
//!   the stock low-level reader (`scan.rs` — dimension tables); numeric
//!   scans use the native ematix-parquet path.
//!
//! Sequential and interpreted, on purpose — correctness gates first; the
//! parallel morsel driver and the spilling/parallel join machinery exist
//! (`exec.rs`) and the planner grows into them next.

use std::collections::{BTreeMap, HashMap};

use ematix_parquet_io::ParquetFile;

use crate::chunk::{DataChunk, Selection};
use crate::expr::{Expr, ScalarValue, filter_expr, sum_expr_f64};
use crate::logical::{AggFunc, BoundQuery, Slot, TableInput};
use crate::scan::{ColKind, scan_columns};
use crate::scan_native::{NativeColKind, scan_row_groups};
use crate::vector::{LogicalType, Vector};

/// A query result: named columns, row-major values.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<ScalarValue>>,
}

/// Execute a bound query on the engine. Uncorrelated subqueries run first
/// (recursively), then substitute into the outer query as constants /
/// membership sets before the main pipeline runs.
pub fn execute(q: &BoundQuery) -> Result<QueryResult, String> {
    if q.subqueries.is_empty() {
        return Executor { q }.run();
    }
    let mut q2 = q.clone();
    resolve_subqueries(&mut q2)?;
    Executor { q: &q2 }.run()
}

/// Execute every subquery and substitute its result into the outer query's
/// expressions: `ScalarSub(i)` → the computed literal, `InSub(i)` → a
/// materialized [`Expr::InSet`].
fn resolve_subqueries(q: &mut BoundQuery) -> Result<(), String> {
    use std::collections::HashSet;
    use std::sync::Arc;

    let subs = std::mem::take(&mut q.subqueries);
    let mut scalars: Vec<Option<ScalarValue>> = vec![None; subs.len()];
    let mut sets: Vec<Option<Arc<HashSet<i64>>>> = vec![None; subs.len()];

    // Which index is used how (a sub could in principle be used both ways).
    let mut want_scalar = vec![false; subs.len()];
    let mut want_set = vec![false; subs.len()];
    visit_query_exprs(q, &mut |e| match e {
        Expr::ScalarSub(i) => want_scalar[*i] = true,
        Expr::InSub { sub, .. } => want_set[*sub] = true,
        _ => {}
    });

    for (i, sub) in subs.iter().enumerate() {
        if !want_scalar[i] && !want_set[i] {
            continue;
        }
        let r = execute(sub)?; // recursion handles subs-of-subs
        if want_scalar[i] {
            let [row] = r.rows.as_slice() else {
                return Err(format!(
                    "scalar subquery returned {} rows (want 1)",
                    r.rows.len()
                ));
            };
            scalars[i] = Some(row[0].clone());
        }
        if want_set[i] {
            let mut set = HashSet::with_capacity(r.rows.len());
            for row in &r.rows {
                match &row[0] {
                    ScalarValue::Int64(v) => set.insert(*v),
                    ScalarValue::Int32(v) => set.insert(*v as i64),
                    ScalarValue::Date32(v) => set.insert(*v as i64),
                    other => {
                        return Err(format!("IN subquery must yield integers (got {other:?})"));
                    }
                };
            }
            sets[i] = Some(Arc::new(set));
        }
    }

    rewrite_query_exprs(q, &mut |e| match e {
        Expr::ScalarSub(i) => {
            *e = Expr::Literal(scalars[*i].clone().expect("scalar computed"));
        }
        Expr::InSub { expr, sub, negated } => {
            *e = Expr::InSet {
                expr: std::mem::replace(expr, Box::new(Expr::Column(0))),
                set: sets[*sub].clone().expect("set computed"),
                negated: *negated,
            };
        }
        _ => {}
    });
    Ok(())
}

/// Visit every expression in a query (filters, post-filter, group keys, agg
/// args, having, outputs) — read-only.
fn visit_query_exprs(q: &BoundQuery, f: &mut impl FnMut(&Expr)) {
    fn walk(e: &Expr, f: &mut impl FnMut(&Expr)) {
        f(e);
        match e {
            Expr::Column(_) | Expr::Literal(_) | Expr::ScalarSub(_) => {}
            Expr::Binary { lhs, rhs, .. } => {
                walk(lhs, f);
                walk(rhs, f);
            }
            Expr::ExtractYear(i) => walk(i, f),
            Expr::Like { expr, .. } | Expr::InSub { expr, .. } | Expr::InSet { expr, .. } => {
                walk(expr, f)
            }
            Expr::Case { whens, else_ } => {
                for (c, v) in whens {
                    walk(c, f);
                    walk(v, f);
                }
                walk(else_, f);
            }
        }
    }
    for t in &q.tables {
        if let Some(p) = &t.filter {
            walk(p, f);
        }
    }
    if let Some(p) = &q.post_filter {
        walk(p, f);
    }
    for g in &q.group {
        walk(&g.expr, f);
    }
    for a in &q.aggs {
        walk(&a.arg, f);
    }
    if let Some(h) = &q.having {
        walk(h, f);
    }
    for o in &q.output {
        walk(&o.expr, f);
    }
}

/// Rewrite every expression in a query bottom-up (children first, then the
/// node itself — so a substitution sees resolved children).
fn rewrite_query_exprs(q: &mut BoundQuery, f: &mut impl FnMut(&mut Expr)) {
    fn walk(e: &mut Expr, f: &mut impl FnMut(&mut Expr)) {
        match e {
            Expr::Column(_) | Expr::Literal(_) | Expr::ScalarSub(_) => {}
            Expr::Binary { lhs, rhs, .. } => {
                walk(lhs, f);
                walk(rhs, f);
            }
            Expr::ExtractYear(i) => walk(i, f),
            Expr::Like { expr, .. } | Expr::InSub { expr, .. } | Expr::InSet { expr, .. } => {
                walk(expr, f)
            }
            Expr::Case { whens, else_ } => {
                for (c, v) in whens {
                    walk(c, f);
                    walk(v, f);
                }
                walk(else_, f);
            }
        }
        f(e);
    }
    for t in &mut q.tables {
        if let Some(p) = &mut t.filter {
            walk(p, f);
        }
    }
    if let Some(p) = &mut q.post_filter {
        walk(p, f);
    }
    for g in &mut q.group {
        walk(&mut g.expr, f);
    }
    for a in &mut q.aggs {
        walk(&mut a.arg, f);
    }
    if let Some(h) = &mut q.having {
        walk(h, f);
    }
    for o in &mut q.output {
        walk(&mut o.expr, f);
    }
}

struct Executor<'q> {
    q: &'q BoundQuery,
}

/// A processed dim subtree: join key → (match count, payload values aligned
/// with `payload_slots`).
struct DimResult {
    payload_slots: Vec<usize>,
    map: HashMap<i64, (u64, Vec<ScalarValue>)>,
}

impl Executor<'_> {
    fn run(&self) -> Result<QueryResult, String> {
        let q = self.q;

        // ---- Orient the join graph into a tree rooted at the largest
        // table (edges validated connected at bind time).
        let n = q.tables.len();
        let root = if n == 1 {
            0
        } else {
            let mut best = (0usize, 0u64);
            for (t, ti) in q.tables.iter().enumerate() {
                let rows = table_rows(ti)?;
                if rows > best.1 {
                    best = (t, rows);
                }
            }
            best.0
        };
        // children[t] = (child_table, parent_local_key_col, child_local_key_col)
        // A spanning tree over the join edges; an edge whose two tables are
        // already connected (a join CYCLE, e.g. Q5's customer-nation =
        // supplier-nation constraint) becomes a **residual equality**
        // evaluated post-join at the root.
        let mut children: Vec<Vec<(usize, usize, usize)>> = vec![Vec::new(); n];
        let mut residual_eq: Vec<Expr> = Vec::new();
        {
            let mut seen = vec![false; n];
            seen[root] = true;
            // Breadth-first in WHERE-declaration order: when a table is
            // reachable through several edges (Q5's customer via the
            // custkey join AND the nationkey constraint), the join listed
            // first wins the tree slot and the other becomes residual —
            // matching the query's natural lookup structure.
            let mut frontier = std::collections::VecDeque::from([root]);
            let mut used_edges = vec![false; q.edges.len()];
            while let Some(t) = frontier.pop_front() {
                for (ei, e) in q.edges.iter().enumerate() {
                    if used_edges[ei] {
                        continue;
                    }
                    let (sa, sb) = (q.slots[e.a], q.slots[e.b]);
                    let (parent_slot_col, child) = if sa.table == t && !seen[sb.table] {
                        (sa.col, (sb.table, sb.col))
                    } else if sb.table == t && !seen[sa.table] {
                        (sb.col, (sa.table, sa.col))
                    } else {
                        continue;
                    };
                    used_edges[ei] = true;
                    seen[child.0] = true;
                    children[t].push((child.0, parent_slot_col, child.1));
                    frontier.push_back(child.0);
                }
            }
            // Any unused edge connects two already-seen tables: a cycle.
            for (ei, e) in q.edges.iter().enumerate() {
                if !used_edges[ei] {
                    residual_eq.push(Expr::Binary {
                        op: crate::expr::BinaryOp::Eq,
                        lhs: Box::new(Expr::Column(e.a)),
                        rhs: Box::new(Expr::Column(e.b)),
                    });
                }
            }
        }

        // The full post-join predicate: the bound multi-table filter plus
        // any residual cycle equalities.
        let post: Option<Expr> =
            residual_eq
                .into_iter()
                .chain(q.post_filter.clone())
                .reduce(|l, r| Expr::Binary {
                    op: crate::expr::BinaryOp::And,
                    lhs: Box::new(l),
                    rhs: Box::new(r),
                });

        // Slots whose values must survive to the root (group keys, agg
        // args, and the post-join predicate reference them). Per non-root
        // table these become payloads.
        let mut needed: Vec<usize> = Vec::new();
        for g in &q.group {
            collect_slots(&g.expr, &mut needed);
        }
        for a in &q.aggs {
            collect_slots(&a.arg, &mut needed);
        }
        if let Some(p) = &post {
            collect_slots(p, &mut needed);
        }
        needed.sort_unstable();
        needed.dedup();

        // ---- Process every dim subtree bottom-up.
        let mut dim_results: Vec<Option<DimResult>> = Vec::with_capacity(n);
        for _ in 0..n {
            dim_results.push(None);
        }
        for &(child, _, child_key) in &children[root] {
            dim_results[child] = Some(self.build_dim(child, child_key, &children, &needed)?);
        }

        // ---- Root: scan + filter, then per child narrow (with
        // multiplicity) and attach payload columns.
        let chunks = self.filtered_chunks(root)?;
        let mut view_chunks: Vec<DataChunk> = Vec::with_capacity(chunks.len());
        let mut sels: Vec<Selection> = Vec::with_capacity(chunks.len());
        for (chunk, sel) in chunks {
            let mut view = self.slot_view(root, &chunk);
            let mut sel = sel;
            for &(child, parent_key, _) in &children[root] {
                let dim = dim_results[child].as_ref().expect("dim built");
                let key = Expr::Column(self.local_to_slot(root, parent_key));
                // Narrow with multiplicity.
                let mut out = Vec::new();
                sel.for_each(|i| {
                    if let Some((cnt, _)) = dim.map.get(&key.eval_i64(&view, i as usize)) {
                        for _ in 0..*cnt {
                            out.push(i);
                        }
                    }
                });
                sel = Selection::Indices(out);
                // Attach the subtree's payload slots as full-length columns.
                if !dim.payload_slots.is_empty() {
                    let nrows = chunk.cols.first().map_or(0, |c| c.len());
                    let mut row_vals: HashMap<u32, Vec<ScalarValue>> = HashMap::new();
                    sel.for_each(|i| {
                        if row_vals.contains_key(&i) {
                            return;
                        }
                        let (_, pay) = dim
                            .map
                            .get(&key.eval_i64(&view, i as usize))
                            .expect("row just matched");
                        row_vals.insert(i, pay.clone());
                    });
                    for (j, &slot) in dim.payload_slots.iter().enumerate() {
                        let ty = self.slot_ty(slot);
                        view.cols[slot] = build_column(ty, nrows, |row| {
                            row_vals.get(&(row as u32)).map(|v| v[j].clone())
                        });
                    }
                }
            }
            // Post-join predicate (multi-table conjuncts + cycle
            // residuals): evaluated once every payload is attached.
            if let Some(p) = &post {
                let scoped = DataChunk {
                    cols: view.cols.clone(),
                    sel,
                };
                sel = filter_expr(&scoped, p);
            }
            view_chunks.push(view);
            sels.push(sel);
        }

        // ---- Aggregate, project, then HAVING → ORDER BY → LIMIT.
        let (columns, mut rows) = self.aggregate(&view_chunks, &sels)?;
        if !q.order_by.is_empty() {
            rows.sort_by(|a, b| {
                for k in &q.order_by {
                    let ord = cmp_scalar(&a[k.output], &b[k.output]);
                    let ord = if k.desc { ord.reverse() } else { ord };
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });
        }
        if let Some(l) = q.limit {
            rows.truncate(l);
        }
        Ok(QueryResult { columns, rows })
    }

    /// Recursively process dim table `t` (joined to its parent via its
    /// local column `link_col`) into a key → (count, payloads) map.
    fn build_dim(
        &self,
        t: usize,
        link_col: usize,
        children: &[Vec<(usize, usize, usize)>],
        needed: &[usize],
    ) -> Result<DimResult, String> {
        let q = self.q;
        // This subtree's own payload slots…
        let mut payload_slots: Vec<usize> = needed
            .iter()
            .copied()
            .filter(|&s| q.slots[s].table == t)
            .collect();
        // …plus every child's, bubbled up.
        let mut child_results: Vec<(usize, DimResult)> = Vec::new(); // (parent-local key col, result)
        for &(child, parent_key, child_key) in &children[t] {
            let r = self.build_dim(child, child_key, children, needed)?;
            payload_slots.extend(r.payload_slots.iter().copied());
            child_results.push((parent_key, r));
        }

        let has_payload = !payload_slots.is_empty();
        let mut map: HashMap<i64, (u64, Vec<ScalarValue>)> = HashMap::new();
        let own_payload: Vec<usize> = payload_slots
            .iter()
            .copied()
            .filter(|&s| q.slots[s].table == t)
            .collect();

        for (chunk, sel) in self.filtered_chunks(t)? {
            let view = self.slot_view(t, &chunk);
            let link = Expr::Column(self.local_to_slot(t, link_col));
            let mut err: Option<String> = None;
            sel.for_each(|i| {
                if err.is_some() {
                    return;
                }
                let i = i as usize;
                // Probe each child; a miss drops the row, hits multiply.
                let mut weight = 1u64;
                let mut bubbled: Vec<(usize, Vec<ScalarValue>)> = Vec::new();
                for (parent_key, r) in &child_results {
                    let k = Expr::Column(self.local_to_slot(t, *parent_key)).eval_i64(&view, i);
                    match r.map.get(&k) {
                        None => {
                            weight = 0;
                            break;
                        }
                        Some((cnt, pay)) => {
                            weight *= cnt;
                            if !r.payload_slots.is_empty() {
                                bubbled.push((bubbled.len(), pay.clone()));
                            }
                        }
                    }
                }
                if weight == 0 {
                    return;
                }
                // Assemble payloads in `payload_slots` order: own first,
                // then each child's block.
                let mut pay: Vec<ScalarValue> = Vec::with_capacity(payload_slots.len());
                for &s in &own_payload {
                    pay.push(Expr::Column(s).eval_value(&view, i));
                }
                for (_, block) in &bubbled {
                    pay.extend(block.iter().cloned());
                }
                let key = link.eval_i64(&view, i);
                match map.entry(key) {
                    std::collections::hash_map::Entry::Vacant(e) => {
                        e.insert((weight, pay));
                    }
                    std::collections::hash_map::Entry::Occupied(mut e) => {
                        if has_payload {
                            err = Some(format!(
                                "duplicate join key {key} in table '{}' with payload columns — \
                                 not yet supported",
                                q.tables[t].name
                            ));
                        } else {
                            e.get_mut().0 += weight;
                        }
                    }
                }
            });
            if let Some(e) = err {
                return Err(e);
            }
        }
        Ok(DimResult { payload_slots, map })
    }

    /// Scan table `t` and apply its own filter, yielding (chunk, live
    /// selection) pairs. Chunks are in the table's LOCAL column space.
    fn filtered_chunks(&self, t: usize) -> Result<Vec<(DataChunk, Selection)>, String> {
        let ti = &self.q.tables[t];
        let chunks = scan_table(ti)?;
        let mut out = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            let sel = match &ti.filter {
                None => chunk.sel.clone(),
                Some(pred) => {
                    let view = self.slot_view(t, &chunk);
                    filter_expr(&view, pred)
                }
            };
            out.push((chunk, sel));
        }
        Ok(out)
    }

    /// Arrange a table-local chunk into the global slot space: this table's
    /// slots map to its columns, every other slot gets a placeholder that
    /// panics if touched (a wiring bug, not a data condition).
    fn slot_view(&self, t: usize, chunk: &DataChunk) -> DataChunk {
        let cols = self
            .q
            .slots
            .iter()
            .map(|s| {
                if s.table == t {
                    chunk.col(s.col).clone()
                } else {
                    Vector::i64(Vec::new()) // placeholder
                }
            })
            .collect();
        DataChunk {
            cols,
            sel: chunk.sel.clone(),
        }
    }

    fn local_to_slot(&self, t: usize, col: usize) -> usize {
        self.q
            .slots
            .iter()
            .position(|s| s.table == t && s.col == col)
            .expect("every join key has a slot")
    }

    fn slot_ty(&self, s: usize) -> LogicalType {
        let Slot { table, col } = self.q.slots[s];
        self.q.tables[table].projection[col].ty
    }

    /// Grouped / scalar aggregation over slot-space chunks, then the output
    /// projection over the per-group result rows.
    fn aggregate(
        &self,
        chunks: &[DataChunk],
        sels: &[Selection],
    ) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), String> {
        let q = self.q;
        let nkeys = q.group.len();

        // key tuple → per-agg accumulator state.
        let mut groups: BTreeMap<Vec<GroupKey>, Vec<AggState>> = BTreeMap::new();
        if nkeys == 0 {
            // Scalar aggregate: keep SUM's per-chunk partial association
            // (what makes the Q6 gate bit-identical to the hand kernel).
            let mut states = vec![AggState::default(); q.aggs.len()];
            for (chunk, sel) in chunks.iter().zip(sels) {
                for (j, agg) in q.aggs.iter().enumerate() {
                    match agg.func {
                        AggFunc::Sum => states[j].sum += sum_expr_f64(chunk, sel, &agg.arg),
                        AggFunc::Count => states[j].count += sel.len() as u64,
                        AggFunc::CountDistinct => sel.for_each(|i| {
                            states[j]
                                .distinct
                                .insert(agg.arg.eval_i64(chunk, i as usize));
                        }),
                        _ => {
                            sel.for_each(|i| states[j].update(agg.arg.eval_f64(chunk, i as usize)))
                        }
                    }
                }
            }
            groups.insert(Vec::new(), states);
        } else {
            for (chunk, sel) in chunks.iter().zip(sels) {
                sel.for_each(|i| {
                    let i = i as usize;
                    let key: Vec<GroupKey> = q
                        .group
                        .iter()
                        .map(|g| GroupKey::from(g.expr.eval_value(chunk, i)))
                        .collect();
                    let states = groups
                        .entry(key)
                        .or_insert_with(|| vec![AggState::default(); q.aggs.len()]);
                    for (j, agg) in q.aggs.iter().enumerate() {
                        match agg.func {
                            AggFunc::Count => states[j].count += 1,
                            AggFunc::CountDistinct => {
                                states[j].distinct.insert(agg.arg.eval_i64(chunk, i));
                            }
                            _ => states[j].update(agg.arg.eval_f64(chunk, i)),
                        }
                    }
                });
            }
        }

        // ---- Build the row-space chunk [keys…, agg values…] with typed
        // key columns (Int / Float / Utf8, from the key values themselves).
        let ngroups = groups.len();
        let mut cols: Vec<Vector> = Vec::with_capacity(nkeys + q.aggs.len());
        for k in 0..nkeys {
            cols.push(build_key_column(groups.keys().map(|key| &key[k]), ngroups));
        }
        for (j, agg) in q.aggs.iter().enumerate() {
            match agg.func {
                AggFunc::Count => cols.push(Vector::i64(
                    groups.values().map(|st| st[j].count as i64).collect(),
                )),
                AggFunc::CountDistinct => cols.push(Vector::i64(
                    groups
                        .values()
                        .map(|st| st[j].distinct.len() as i64)
                        .collect(),
                )),
                _ => cols.push(Vector::f64(
                    groups.values().map(|st| st[j].finalize(agg.func)).collect(),
                )),
            }
        }
        let row_chunk = DataChunk::new(cols);

        // HAVING filters groups; the output projection runs per survivor.
        let keep: Vec<usize> = (0..ngroups)
            .filter(|&r| match &q.having {
                None => true,
                Some(h) => h.eval_bool(&row_chunk, r),
            })
            .collect();
        let columns: Vec<String> = q.output.iter().map(|o| o.name.clone()).collect();
        let rows = keep
            .into_iter()
            .map(|r| {
                q.output
                    .iter()
                    .map(|o| o.expr.eval_value(&row_chunk, r))
                    .collect()
            })
            .collect();
        Ok((columns, rows))
    }
}

/// A typed group-key value with a total order (BTreeMap grouping ⇒
/// deterministic key-sorted output). Floats order by `total_cmp` and group
/// by bit pattern — exact, NaN-safe.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum GroupKey {
    Int(i64),
    Float(FOrd),
    Str(std::sync::Arc<str>),
}

/// An f64 with `total_cmp` ordering (bits stored, so `Eq` is exact).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FOrd(u64);

impl PartialOrd for FOrd {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for FOrd {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        f64::from_bits(self.0).total_cmp(&f64::from_bits(other.0))
    }
}

impl From<ScalarValue> for GroupKey {
    fn from(v: ScalarValue) -> Self {
        match v {
            ScalarValue::Int64(i) => GroupKey::Int(i),
            ScalarValue::Int32(i) => GroupKey::Int(i as i64),
            ScalarValue::Date32(d) => GroupKey::Int(d as i64),
            ScalarValue::Boolean(b) => GroupKey::Int(i64::from(b)),
            ScalarValue::Float64(f) => GroupKey::Float(FOrd(f.to_bits())),
            ScalarValue::Utf8(s) => GroupKey::Str(s),
        }
    }
}

/// Build a typed row-space column from one group-key position across all
/// groups (every value in a position shares a type by construction).
fn build_key_column<'k>(keys: impl Iterator<Item = &'k GroupKey>, ngroups: usize) -> Vector {
    let keys: Vec<&GroupKey> = keys.collect();
    debug_assert_eq!(keys.len(), ngroups);
    match keys.first() {
        Some(GroupKey::Float(_)) => Vector::f64(
            keys.iter()
                .map(|k| match k {
                    GroupKey::Float(f) => f64::from_bits(f.0),
                    other => panic!("mixed group-key types: {other:?}"),
                })
                .collect(),
        ),
        Some(GroupKey::Str(_)) => {
            let mut offsets = Vec::with_capacity(ngroups + 1);
            let mut data = Vec::new();
            offsets.push(0u32);
            for k in &keys {
                let GroupKey::Str(s) = k else {
                    panic!("mixed group-key types: {k:?}");
                };
                data.extend_from_slice(s.as_bytes());
                offsets.push(data.len() as u32);
            }
            Vector::utf8(offsets, data)
        }
        _ => Vector::i64(
            keys.iter()
                .map(|k| match k {
                    GroupKey::Int(i) => *i,
                    other => panic!("mixed group-key types: {other:?}"),
                })
                .collect(),
        ),
    }
}

/// Total order over same-typed output scalars — the ORDER BY comparator.
fn cmp_scalar(a: &ScalarValue, b: &ScalarValue) -> std::cmp::Ordering {
    use ScalarValue::*;
    match (a, b) {
        (Int64(x), Int64(y)) => x.cmp(y),
        (Int32(x), Int32(y)) => x.cmp(y),
        (Date32(x), Date32(y)) => x.cmp(y),
        (Float64(x), Float64(y)) => x.total_cmp(y),
        (Utf8(x), Utf8(y)) => x.cmp(y),
        (Boolean(x), Boolean(y)) => x.cmp(y),
        _ => panic!("ORDER BY over mixed types: {a:?} vs {b:?}"),
    }
}

/// Per-aggregate universal accumulator.
#[derive(Clone, Debug)]
struct AggState {
    sum: f64,
    count: u64,
    min: f64,
    max: f64,
    /// Distinct integer values (only fed by `COUNT(DISTINCT …)`).
    distinct: std::collections::HashSet<i64>,
}

impl Default for AggState {
    fn default() -> Self {
        AggState {
            sum: 0.0,
            count: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            distinct: std::collections::HashSet::new(),
        }
    }
}

impl AggState {
    #[inline]
    fn update(&mut self, v: f64) {
        self.sum += v;
        self.count += 1;
        if v < self.min {
            self.min = v;
        }
        if v > self.max {
            self.max = v;
        }
    }

    fn finalize(&self, func: AggFunc) -> f64 {
        match func {
            AggFunc::Sum => self.sum,
            AggFunc::Count => self.count as f64,
            AggFunc::Min => self.min,
            AggFunc::Max => self.max,
            AggFunc::Avg => self.sum / self.count as f64,
            AggFunc::CountDistinct => self.distinct.len() as f64,
        }
    }
}

/// Scan a table by its projection. Strings route through the stock
/// low-level reader (dimension tables); numeric-only scans use the native
/// ematix-parquet path.
fn scan_table(ti: &TableInput) -> Result<Vec<DataChunk>, String> {
    let has_strings = ti
        .projection
        .iter()
        .any(|c| matches!(c.ty, LogicalType::Utf8));
    if has_strings {
        let cols: Vec<(&str, ColKind)> = ti
            .projection
            .iter()
            .map(|c| {
                let kind = match c.ty {
                    LogicalType::Utf8 => ColKind::Utf8,
                    LogicalType::Int64 => ColKind::I64,
                    LogicalType::Float64 => ColKind::F64,
                    LogicalType::Int32 | LogicalType::Date32 => ColKind::I32(c.ty),
                };
                (c.name.as_str(), kind)
            })
            .collect();
        scan_columns(&ti.path, &cols)
    } else {
        let cols: Vec<(usize, NativeColKind)> = ti
            .projection
            .iter()
            .map(|c| {
                let kind = match c.ty {
                    LogicalType::Int32 | LogicalType::Date32 => NativeColKind::I32(c.ty),
                    LogicalType::Int64 => NativeColKind::I64,
                    LogicalType::Float64 => NativeColKind::F64,
                    LogicalType::Utf8 => unreachable!("string scans routed above"),
                };
                (c.leaf, kind)
            })
            .collect();
        scan_row_groups(&ti.path, &cols)
    }
}

/// Total row count of a table's parquet file (footer metadata only).
fn table_rows(ti: &TableInput) -> Result<u64, String> {
    let f = ParquetFile::open(&ti.path).map_err(|e| format!("open {}: {e}", ti.path.display()))?;
    let md = f.metadata().map_err(|e| format!("metadata: {e}"))?;
    Ok(md.row_groups.iter().map(|rg| rg.num_rows as u64).sum())
}

/// Build a full-length attached column of type `ty`; `get(row)` yields the
/// payload value for live rows (`None` rows get a default — they are never
/// selected).
fn build_column(
    ty: LogicalType,
    nrows: usize,
    get: impl Fn(usize) -> Option<ScalarValue>,
) -> Vector {
    match ty {
        LogicalType::Utf8 => {
            let mut offsets = Vec::with_capacity(nrows + 1);
            let mut data = Vec::new();
            offsets.push(0u32);
            for r in 0..nrows {
                if let Some(ScalarValue::Utf8(s)) = get(r) {
                    data.extend_from_slice(s.as_bytes());
                }
                offsets.push(data.len() as u32);
            }
            Vector::utf8(offsets, data)
        }
        LogicalType::Float64 => {
            let mut v = vec![0.0f64; nrows];
            for (r, slot) in v.iter_mut().enumerate() {
                if let Some(ScalarValue::Float64(x)) = get(r) {
                    *slot = x;
                }
            }
            Vector::f64(v)
        }
        // Integer family attaches as i64 (Date32 payload values are day
        // numbers; EXTRACT and comparisons treat them identically).
        _ => {
            let mut v = vec![0i64; nrows];
            for (r, slot) in v.iter_mut().enumerate() {
                match get(r) {
                    Some(ScalarValue::Int64(x)) => *slot = x,
                    Some(ScalarValue::Int32(x)) | Some(ScalarValue::Date32(x)) => *slot = x as i64,
                    _ => {}
                }
            }
            Vector::i64(v)
        }
    }
}

/// Collect every slot referenced by a slot-space expression.
fn collect_slots(e: &Expr, out: &mut Vec<usize>) {
    match e {
        Expr::Column(s) => out.push(*s),
        Expr::Literal(_) => {}
        Expr::Binary { lhs, rhs, .. } => {
            collect_slots(lhs, out);
            collect_slots(rhs, out);
        }
        Expr::ExtractYear(i) => collect_slots(i, out),
        Expr::Like { expr, .. } => collect_slots(expr, out),
        Expr::ScalarSub(_) => {}
        Expr::InSub { expr, .. } | Expr::InSet { expr, .. } => collect_slots(expr, out),
        Expr::Case { whens, else_ } => {
            for (c, v) in whens {
                collect_slots(c, out);
                collect_slots(v, out);
            }
            collect_slots(else_, out);
        }
    }
}
