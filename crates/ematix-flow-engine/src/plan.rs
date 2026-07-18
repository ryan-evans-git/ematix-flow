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
use std::sync::Mutex;

use ematix_parquet_io::ParquetFile;

use crate::chunk::{DataChunk, Selection};
use crate::expr::{Expr, ScalarValue, filter_expr, sum_expr_f64};
use crate::logical::{AggFunc, BoundQuery, Slot, TableInput, TableSource};
use crate::scan::{ColKind, StockScan};
use crate::scan_native::{NativeColKind, decode_row_group};
use crate::sched::MorselQueue;
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
    // Worker count: EMAT_ENGINE_THREADS overrides; default = all cores.
    // Results are BIT-IDENTICAL at any thread count — each row group's
    // partial is computed independently and partials merge in row-group
    // order, so parallelism changes wall-clock, never the answer.
    let nthreads = std::env::var("EMAT_ENGINE_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        });
    if q.subqueries.is_empty() {
        return Executor { q, nthreads }.run();
    }
    let mut q2 = q.clone();
    resolve_subqueries(&mut q2)?;
    Executor { q: &q2, nthreads }.run()
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
            Expr::Like { expr, .. }
            | Expr::InSub { expr, .. }
            | Expr::InSet { expr, .. }
            | Expr::Substr { expr, .. } => walk(expr, f),
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
            Expr::Like { expr, .. }
            | Expr::InSub { expr, .. }
            | Expr::InSet { expr, .. }
            | Expr::Substr { expr, .. } => walk(expr, f),
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
    nthreads: usize,
}

/// The (parent_local_col, child_local_col) equi-join pairs linking a dim to
/// its parent — several pairs form a composite key.
type Links = Vec<(usize, usize)>;

/// A dim map keyed by the join key — specialized for the ubiquitous
/// single-column key (no per-row `Vec` allocation on probe or build);
/// composite keys use slice-keyed lookups (`Vec<i64>: Borrow<[i64]>`), so
/// probing never allocates either way.
enum DimMap {
    Single(HashMap<i64, (u64, Vec<ScalarValue>)>),
    Multi(HashMap<Vec<i64>, (u64, Vec<ScalarValue>)>),
}

impl DimMap {
    #[inline]
    fn get(&self, k: &[i64]) -> Option<&(u64, Vec<ScalarValue>)> {
        match self {
            DimMap::Single(m) => m.get(&k[0]),
            DimMap::Multi(m) => m.get(k),
        }
    }
}

/// A processed dim subtree: (composite) join key → (match count, payload
/// values aligned with `payload_slots`).
struct DimResult {
    payload_slots: Vec<usize>,
    map: DimMap,
}

/// One row group's aggregation partial — merged in row-group order, which
/// keeps results deterministic and (for the scalar-SUM path) bit-identical
/// to the sequential execution at any thread count.
enum RgOut {
    Scalar(Vec<AggState>),
    Grouped(BTreeMap<Vec<GroupKey>, Vec<AggState>>),
    Rows(Vec<Vec<ScalarValue>>),
}

/// Shared, read-only context for per-row-group root processing.
struct RootCtx<'a> {
    root: usize,
    children: &'a [(usize, Links, bool)],
    dims: &'a [Option<DimResult>],
    post: &'a Option<Expr>,
    matched_cols: &'a HashMap<usize, usize>,
    filter: &'a Option<Expr>,
}

/// Where a table's row groups come from.
enum RootSrc {
    /// A materialized derived input (single chunk).
    One(Vec<DataChunk>),
    /// The native ematix-parquet path (numeric scans; `&file` is shared
    /// lock-free across workers).
    Native {
        file: Box<ParquetFile>,
        cols: Vec<(usize, NativeColKind)>,
        nrows: Vec<usize>,
    },
    /// The stock low-level reader (string-bearing scans; each worker opens
    /// its own handle).
    Stock {
        path: std::path::PathBuf,
        cols: Vec<(String, ColKind)>,
        n_rg: usize,
    },
}

impl RootSrc {
    fn n_rg(&self) -> usize {
        match self {
            RootSrc::One(c) => c.len(),
            RootSrc::Native { nrows, .. } => nrows.len(),
            RootSrc::Stock { n_rg, .. } => *n_rg,
        }
    }

    /// Worker-local stock reader for string-bearing scans (None otherwise).
    fn open_local_stock(&self) -> Result<Option<StockScan>, String> {
        match self {
            RootSrc::Stock { path, cols, .. } => {
                let refs: Vec<(&str, ColKind)> =
                    cols.iter().map(|(n, k)| (n.as_str(), *k)).collect();
                Ok(Some(StockScan::open(path, &refs)?))
            }
            _ => Ok(None),
        }
    }

    fn decode(&self, rg: usize, local_stock: Option<&StockScan>) -> Result<DataChunk, String> {
        match self {
            RootSrc::One(chunks) => Ok(chunks[rg].clone()),
            RootSrc::Native { file, cols, nrows } => decode_row_group(file, rg, nrows[rg], cols),
            RootSrc::Stock { .. } => local_stock.expect("stock reader").decode_rg(rg),
        }
    }
}

impl Executor<'_> {
    fn run(&self) -> Result<QueryResult, String> {
        let q = self.q;

        // ---- Orient the join graph into a tree rooted at the largest
        // table (edges validated connected at bind time).
        let n = q.tables.len();
        // A LEFT OUTER join forces the root to the preserved table (its
        // unmatched rows must flow through); otherwise root at the largest
        // table by parquet row count.
        let forced = q.edges.iter().find_map(|e| e.preserved);
        let root = if let Some(r) = forced {
            r
        } else if n == 1 {
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
        // children[t] = (child_table, links) where links are the
        // (parent_local_col, child_local_col) pairs joining them — several
        // pairs = a COMPOSITE key (Q9's lineitem⋈partsupp on suppkey AND
        // partkey). A spanning tree over the join edges; an edge whose two
        // tables are already connected through OTHER pairs (a join CYCLE,
        // e.g. Q5's customer-nation = supplier-nation constraint) becomes a
        // **residual equality** evaluated post-join at the root.
        // (child_table, links, left) — left marks a LEFT OUTER child whose
        // misses keep the root row (once) instead of dropping it.
        let mut children: Vec<Vec<(usize, Links, bool)>> = vec![Vec::new(); n];
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
                    // Absorb every other edge between this same pair into
                    // one composite-key link.
                    let mut links: Links = vec![(parent_slot_col, child.1)];
                    for (ej, e2) in q.edges.iter().enumerate() {
                        if used_edges[ej] {
                            continue;
                        }
                        let (x, y) = (q.slots[e2.a], q.slots[e2.b]);
                        if x.table == t && y.table == child.0 {
                            links.push((x.col, y.col));
                            used_edges[ej] = true;
                        } else if y.table == t && x.table == child.0 {
                            links.push((y.col, x.col));
                            used_edges[ej] = true;
                        }
                    }
                    let is_left = q.edges[ei].preserved.is_some();
                    children[t].push((child.0, links, is_left));
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
        if q.group.is_empty() && q.aggs.is_empty() {
            // Plain row query: output projections are slot-space and
            // evaluate at the root — their dim columns must attach too.
            for o in &q.output {
                collect_slots(&o.expr, &mut needed);
            }
        }
        needed.sort_unstable();
        needed.dedup();

        // ---- Process every dim subtree bottom-up.
        let mut dim_results: Vec<Option<DimResult>> = Vec::with_capacity(n);
        for _ in 0..n {
            dim_results.push(None);
        }
        for (child, links, _) in &children[root] {
            let child_cols: Vec<usize> = links.iter().map(|&(_, c)| c).collect();
            dim_results[*child] = Some(self.build_dim(*child, &child_cols, &children, &needed)?);
        }

        // ---- Root: MORSEL-PARALLEL per row group. Each worker decodes a
        // row group, applies the root filter, probes/attaches the dim
        // subtrees, evaluates the post-join predicate, and aggregates a
        // per-RG partial. Partials merge in ROW-GROUP ORDER, so the result
        // is deterministic — and bit-identical to sequential — at any
        // thread count.
        let ti = &q.tables[root];
        // Matched-flag columns (LEFT children) append past the slot space
        // in child order — a fixed layout every chunk.
        let mut matched_cols: HashMap<usize, usize> = HashMap::new();
        {
            let mut next = q.slots.len();
            for (child, _, left) in &children[root] {
                if *left {
                    matched_cols.insert(*child, next);
                    next += 1;
                }
            }
        }
        let ctx = RootCtx {
            root,
            children: &children[root],
            dims: &dim_results,
            post: &post,
            matched_cols: &matched_cols,
            filter: &ti.filter,
        };

        let src = self.table_src(root)?;
        let n_rg = src.n_rg();

        let outputs: Mutex<Vec<Option<RgOut>>> = Mutex::new((0..n_rg).map(|_| None).collect());
        let queue = MorselQueue::new(n_rg);
        let nworkers = self.nthreads.clamp(1, n_rg.max(1));
        let (src_ref, ctx_ref, queue_ref, out_ref) = (&src, &ctx, &queue, &outputs);
        std::thread::scope(|scope| -> Result<(), String> {
            let mut handles = Vec::with_capacity(nworkers);
            for _ in 0..nworkers {
                handles.push(scope.spawn(move || -> Result<(), String> {
                    // String-bearing scans open a worker-local reader (the
                    // footer parse is cheap; no shared-reader locking).
                    let local_stock = src_ref.open_local_stock()?;
                    while let Some(rg) = queue_ref.next() {
                        let chunk = src_ref.decode(rg, local_stock.as_ref())?;
                        let out = self.process_root_rg(chunk, ctx_ref)?;
                        out_ref.lock().expect("lock")[rg] = Some(out);
                    }
                    Ok(())
                }));
            }
            for h in handles {
                h.join()
                    .map_err(|_| "worker thread panicked".to_string())??;
            }
            Ok(())
        })?;

        // ---- Merge partials in row-group order, then HAVING → ORDER BY →
        // LIMIT.
        let outputs = outputs.into_inner().expect("no poisoned lock");
        let (columns, mut rows) = if q.group.is_empty() && q.aggs.is_empty() {
            let columns: Vec<String> = q.output.iter().map(|o| o.name.clone()).collect();
            let mut rows = Vec::new();
            for out in outputs.into_iter().flatten() {
                if let RgOut::Rows(mut r) = out {
                    rows.append(&mut r);
                }
            }
            (columns, rows)
        } else {
            let mut groups: BTreeMap<Vec<GroupKey>, Vec<AggState>> = BTreeMap::new();
            for out in outputs.into_iter().flatten() {
                match out {
                    RgOut::Scalar(states) => {
                        let entry = groups
                            .entry(Vec::new())
                            .or_insert_with(|| vec![AggState::default(); q.aggs.len()]);
                        for (a, b) in entry.iter_mut().zip(&states) {
                            a.merge(b);
                        }
                    }
                    RgOut::Grouped(map) => {
                        for (k, states) in map {
                            match groups.entry(k) {
                                std::collections::btree_map::Entry::Vacant(e) => {
                                    e.insert(states);
                                }
                                std::collections::btree_map::Entry::Occupied(mut e) => {
                                    for (a, b) in e.get_mut().iter_mut().zip(&states) {
                                        a.merge(b);
                                    }
                                }
                            }
                        }
                    }
                    RgOut::Rows(_) => unreachable!("plain-row handled above"),
                }
            }
            // A scalar aggregate over zero surviving row groups still
            // yields one (default) row — matching sequential semantics.
            if groups.is_empty() && q.group.is_empty() {
                groups.insert(Vec::new(), vec![AggState::default(); q.aggs.len()]);
            }
            self.finalize_groups(groups)?
        };
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
    /// local columns `link_cols` — a composite key when several) into a
    /// key → (count, payloads) map.
    fn build_dim(
        &self,
        t: usize,
        link_cols: &[usize],
        children: &[Vec<(usize, Links, bool)>],
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
        // (parent-local key cols, result) per child.
        let mut child_results: Vec<(Vec<usize>, DimResult)> = Vec::new();
        for (child, links, left) in &children[t] {
            if *left {
                return Err(
                    "LEFT JOIN below another join is not yet supported (the preserved side \
                     must be the root)"
                        .into(),
                );
            }
            let child_cols: Vec<usize> = links.iter().map(|&(_, c)| c).collect();
            let r = self.build_dim(*child, &child_cols, children, needed)?;
            payload_slots.extend(r.payload_slots.iter().copied());
            child_results.push((links.iter().map(|&(p, _)| p).collect(), r));
        }

        let has_payload = !payload_slots.is_empty();
        let mut map = if link_cols.len() == 1 {
            DimMap::Single(HashMap::new())
        } else {
            DimMap::Multi(HashMap::new())
        };
        let own_payload: Vec<usize> = payload_slots
            .iter()
            .copied()
            .filter(|&s| q.slots[s].table == t)
            .collect();
        // Pre-resolve key slots; probe/build keys fill a reused scratch
        // buffer (zero per-row allocation).
        let link_slots: Vec<usize> = link_cols
            .iter()
            .map(|&c| self.local_to_slot(t, c))
            .collect();
        let child_key_slots: Vec<Vec<usize>> = child_results
            .iter()
            .map(|(parent_keys, _)| {
                parent_keys
                    .iter()
                    .map(|&c| self.local_to_slot(t, c))
                    .collect()
            })
            .collect();

        for (chunk, sel) in self.filtered_chunks(t)? {
            let view = self.slot_view(t, &chunk);
            let mut err: Option<String> = None;
            let mut kbuf: Vec<i64> = Vec::with_capacity(8);
            sel.for_each(|i| {
                if err.is_some() {
                    return;
                }
                let i = i as usize;
                // Probe each child; a miss drops the row, hits multiply.
                let mut weight = 1u64;
                let mut bubbled: Vec<&Vec<ScalarValue>> = Vec::new();
                for (slots, (_, r)) in child_key_slots.iter().zip(&child_results) {
                    kbuf.clear();
                    for &sl in slots {
                        kbuf.push(Expr::Column(sl).eval_i64(&view, i));
                    }
                    match r.map.get(&kbuf) {
                        None => {
                            weight = 0;
                            break;
                        }
                        Some((cnt, pay)) => {
                            weight *= cnt;
                            if !r.payload_slots.is_empty() {
                                bubbled.push(pay);
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
                for block in &bubbled {
                    pay.extend(block.iter().cloned());
                }
                kbuf.clear();
                for &sl in &link_slots {
                    kbuf.push(Expr::Column(sl).eval_i64(&view, i));
                }
                use std::collections::hash_map::Entry;
                let dup = match &mut map {
                    DimMap::Single(m) => match m.entry(kbuf[0]) {
                        Entry::Vacant(e) => {
                            e.insert((weight, pay));
                            false
                        }
                        Entry::Occupied(mut e) => {
                            if has_payload {
                                true
                            } else {
                                e.get_mut().0 += weight;
                                false
                            }
                        }
                    },
                    DimMap::Multi(m) => match m.entry(kbuf.clone()) {
                        Entry::Vacant(e) => {
                            e.insert((weight, pay));
                            false
                        }
                        Entry::Occupied(mut e) => {
                            if has_payload {
                                true
                            } else {
                                e.get_mut().0 += weight;
                                false
                            }
                        }
                    },
                };
                if dup {
                    err = Some(format!(
                        "duplicate join key {kbuf:?} in table '{}' with payload columns — \
                         not yet supported",
                        q.tables[t].name
                    ));
                }
            });
            if let Some(e) = err {
                return Err(e);
            }
        }
        Ok(DimResult { payload_slots, map })
    }

    /// Build a table's row-group source (materialized / native / stock).
    fn table_src(&self, t: usize) -> Result<RootSrc, String> {
        let q = self.q;
        let ti = &q.tables[t];
        Ok(match &ti.source {
            TableSource::Derived(i) => {
                let r = execute(&q.derived[*i])?;
                RootSrc::One(vec![result_to_chunk(&r, ti)?])
            }
            TableSource::Parquet(path) => {
                let has_strings = ti
                    .projection
                    .iter()
                    .any(|c| matches!(c.ty, LogicalType::Utf8));
                if has_strings {
                    let cols: Vec<(String, ColKind)> = ti
                        .projection
                        .iter()
                        .map(|c| {
                            let kind = match c.ty {
                                LogicalType::Utf8 => ColKind::Utf8,
                                LogicalType::Int64 => ColKind::I64,
                                LogicalType::Float64 => ColKind::F64,
                                LogicalType::Int32 | LogicalType::Date32 => ColKind::I32(c.ty),
                            };
                            (c.name.clone(), kind)
                        })
                        .collect();
                    let refs: Vec<(&str, ColKind)> =
                        cols.iter().map(|(n, k)| (n.as_str(), *k)).collect();
                    let n_rg = StockScan::open(path, &refs)?.n_row_groups();
                    RootSrc::Stock {
                        path: path.clone(),
                        cols,
                        n_rg,
                    }
                } else {
                    let cols: Vec<(usize, NativeColKind)> = ti
                        .projection
                        .iter()
                        .map(|c| {
                            let kind = match c.ty {
                                LogicalType::Int32 | LogicalType::Date32 => {
                                    NativeColKind::I32(c.ty)
                                }
                                LogicalType::Int64 => NativeColKind::I64,
                                LogicalType::Float64 => NativeColKind::F64,
                                LogicalType::Utf8 => unreachable!("string scans routed above"),
                            };
                            (c.leaf, kind)
                        })
                        .collect();
                    let file = ParquetFile::open(path)
                        .map_err(|e| format!("open {}: {e}", path.display()))?;
                    let nrows: Vec<usize> = {
                        let md = file.metadata().map_err(|e| format!("metadata: {e}"))?;
                        md.row_groups
                            .iter()
                            .map(|rg| rg.num_rows as usize)
                            .collect()
                    };
                    RootSrc::Native {
                        file: Box::new(file),
                        cols,
                        nrows,
                    }
                }
            }
        })
    }

    /// Scan table `t` and apply its own filter, yielding (chunk, live
    /// selection) pairs in row-group order. Decode + filter run
    /// MORSEL-PARALLEL per row group (the dim-side scans — e.g. Q08's 15M
    /// orders — were the sequential tail once the root went parallel).
    fn filtered_chunks(&self, t: usize) -> Result<Vec<(DataChunk, Selection)>, String> {
        let ti = &self.q.tables[t];
        let src = self.table_src(t)?;
        let n_rg = src.n_rg();
        let outputs: Mutex<Vec<Option<(DataChunk, Selection)>>> =
            Mutex::new((0..n_rg).map(|_| None).collect());
        let queue = MorselQueue::new(n_rg);
        let nworkers = self.nthreads.clamp(1, n_rg.max(1));
        let (src_ref, queue_ref, out_ref) = (&src, &queue, &outputs);
        std::thread::scope(|scope| -> Result<(), String> {
            let mut handles = Vec::with_capacity(nworkers);
            for _ in 0..nworkers {
                handles.push(scope.spawn(move || -> Result<(), String> {
                    let local_stock = src_ref.open_local_stock()?;
                    while let Some(rg) = queue_ref.next() {
                        let chunk = src_ref.decode(rg, local_stock.as_ref())?;
                        let sel = match &ti.filter {
                            None => chunk.sel.clone(),
                            Some(pred) => {
                                let view = self.slot_view(t, &chunk);
                                filter_expr(&view, pred)
                            }
                        };
                        out_ref.lock().expect("lock")[rg] = Some((chunk, sel));
                    }
                    Ok(())
                }));
            }
            for h in handles {
                h.join()
                    .map_err(|_| "worker thread panicked".to_string())??;
            }
            Ok(())
        })?;
        Ok(outputs
            .into_inner()
            .expect("no poisoned lock")
            .into_iter()
            .map(|o| o.expect("every row group decoded"))
            .collect())
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

    /// Process one root row group end-to-end: slot view → root filter →
    /// per-child probe/attach (scratch keys, zero per-row allocation) →
    /// post-join predicate → this RG's aggregation partial.
    fn process_root_rg(&self, chunk: DataChunk, ctx: &RootCtx) -> Result<RgOut, String> {
        let q = self.q;
        let mut view = self.slot_view(ctx.root, &chunk);
        let mut sel = match ctx.filter {
            None => chunk.sel.clone(),
            Some(pred) => filter_expr(&view, pred),
        };
        let nrows = chunk.cols.first().map_or(0, |c| c.len());
        let mut kbuf: Vec<i64> = Vec::with_capacity(8);
        for (child, links, left) in ctx.children {
            let dim = ctx.dims[*child].as_ref().expect("dim built");
            let key_slots: Vec<usize> = links
                .iter()
                .map(|&(p, _)| self.local_to_slot(ctx.root, p))
                .collect();
            // Narrow with multiplicity; a LEFT child keeps misses once.
            let mut out = Vec::new();
            let mut matched: Vec<i64> = if *left { vec![0; nrows] } else { Vec::new() };
            sel.for_each(|i| {
                kbuf.clear();
                for &sl in &key_slots {
                    kbuf.push(Expr::Column(sl).eval_i64(&view, i as usize));
                }
                match dim.map.get(&kbuf) {
                    Some((cnt, _)) => {
                        for _ in 0..*cnt {
                            out.push(i);
                        }
                        if *left {
                            matched[i as usize] = 1;
                        }
                    }
                    None if *left => out.push(i),
                    None => {}
                }
            });
            sel = Selection::Indices(out);
            // Attach the subtree's payload slots as full-length columns
            // (LEFT misses take type defaults — 0 / 0.0 / "").
            if !dim.payload_slots.is_empty() {
                let mut row_vals: HashMap<u32, Vec<ScalarValue>> = HashMap::new();
                sel.for_each(|i| {
                    if row_vals.contains_key(&i) {
                        return;
                    }
                    kbuf.clear();
                    for &sl in &key_slots {
                        kbuf.push(Expr::Column(sl).eval_i64(&view, i as usize));
                    }
                    match dim.map.get(&kbuf) {
                        Some((_, pay)) => {
                            row_vals.insert(i, pay.clone());
                        }
                        None => {
                            debug_assert!(*left, "non-left miss survived narrowing");
                            let defaults = dim
                                .payload_slots
                                .iter()
                                .map(|&sl| default_value(self.slot_ty(sl)))
                                .collect();
                            row_vals.insert(i, defaults);
                        }
                    }
                });
                for (j, &slot) in dim.payload_slots.iter().enumerate() {
                    let ty = self.slot_ty(slot);
                    view.cols[slot] = build_column(ty, nrows, |row| {
                        row_vals.get(&(row as u32)).map(|v| v[j].clone())
                    });
                }
            }
            if *left {
                debug_assert_eq!(ctx.matched_cols[child], view.cols.len());
                view.cols.push(Vector::i64(matched));
            }
        }
        // Post-join predicate (multi-table conjuncts + cycle residuals).
        if let Some(p) = ctx.post {
            let scoped = DataChunk {
                cols: view.cols.clone(),
                sel,
            };
            sel = filter_expr(&scoped, p);
        }

        // ---- This RG's aggregation partial.
        if q.group.is_empty() && q.aggs.is_empty() {
            let mut rows = Vec::new();
            sel.for_each(|i| {
                rows.push(
                    q.output
                        .iter()
                        .map(|o| o.expr.eval_value(&view, i as usize))
                        .collect(),
                );
            });
            return Ok(RgOut::Rows(rows));
        }
        if q.group.is_empty() {
            let mut states = vec![AggState::default(); q.aggs.len()];
            for (j, agg) in q.aggs.iter().enumerate() {
                match agg.func {
                    AggFunc::Sum => states[j].sum += sum_expr_f64(&view, &sel, &agg.arg),
                    AggFunc::Count => states[j].count += sel.len() as u64,
                    AggFunc::CountMatched(t) => {
                        let flags = view.col(ctx.matched_cols[&t]).as_i64();
                        sel.for_each(|i| states[j].count += flags[i as usize] as u64);
                    }
                    AggFunc::CountDistinct => sel.for_each(|i| {
                        states[j]
                            .distinct
                            .insert(agg.arg.eval_i64(&view, i as usize));
                    }),
                    _ => sel.for_each(|i| states[j].update(agg.arg.eval_f64(&view, i as usize))),
                }
            }
            return Ok(RgOut::Scalar(states));
        }
        let mut groups: BTreeMap<Vec<GroupKey>, Vec<AggState>> = BTreeMap::new();
        sel.for_each(|i| {
            let i = i as usize;
            let key: Vec<GroupKey> = q
                .group
                .iter()
                .map(|g| GroupKey::from(g.expr.eval_value(&view, i)))
                .collect();
            let states = groups
                .entry(key)
                .or_insert_with(|| vec![AggState::default(); q.aggs.len()]);
            for (j, agg) in q.aggs.iter().enumerate() {
                match agg.func {
                    AggFunc::Count => states[j].count += 1,
                    AggFunc::CountMatched(t) => {
                        states[j].count += view.col(ctx.matched_cols[&t]).as_i64()[i] as u64;
                    }
                    AggFunc::CountDistinct => {
                        states[j].distinct.insert(agg.arg.eval_i64(&view, i));
                    }
                    _ => states[j].update(agg.arg.eval_f64(&view, i)),
                }
            }
        });
        Ok(RgOut::Grouped(groups))
    }

    /// Merged groups → the row-space chunk → HAVING → output projection.
    fn finalize_groups(
        &self,
        groups: BTreeMap<Vec<GroupKey>, Vec<AggState>>,
    ) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), String> {
        let q = self.q;
        let nkeys = q.group.len();
        // ---- Build the row-space chunk [keys…, agg values…] with typed
        // key columns (Int / Float / Utf8, from the key values themselves).
        let ngroups = groups.len();
        let mut cols: Vec<Vector> = Vec::with_capacity(nkeys + q.aggs.len());
        for k in 0..nkeys {
            cols.push(build_key_column(groups.keys().map(|key| &key[k]), ngroups));
        }
        for (j, agg) in q.aggs.iter().enumerate() {
            match agg.func {
                AggFunc::Count | AggFunc::CountMatched(_) => cols.push(Vector::i64(
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
    /// Merge another partial into this one (row-group-order folding). SUM
    /// adds partials — with a fixed merge order this reproduces the
    /// sequential per-chunk association exactly.
    fn merge(&mut self, o: &AggState) {
        self.sum += o.sum;
        self.count += o.count;
        if o.min < self.min {
            self.min = o.min;
        }
        if o.max > self.max {
            self.max = o.max;
        }
        self.distinct.extend(o.distinct.iter().copied());
    }

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
            AggFunc::CountMatched(_) => self.count as f64,
        }
    }
}

/// Convert a materialized [`QueryResult`] into a chunk shaped like `ti`'s
/// projection (`leaf` = the result's output-column position; values coerced
/// to the declared type).
fn result_to_chunk(r: &QueryResult, ti: &TableInput) -> Result<DataChunk, String> {
    let nrows = r.rows.len();
    let mut cols = Vec::with_capacity(ti.projection.len());
    for c in &ti.projection {
        let get = |row: usize| &r.rows[row][c.leaf];
        let v = match c.ty {
            LogicalType::Utf8 => {
                let mut offsets = Vec::with_capacity(nrows + 1);
                let mut data = Vec::new();
                offsets.push(0u32);
                for row in 0..nrows {
                    match get(row) {
                        ScalarValue::Utf8(s) => data.extend_from_slice(s.as_bytes()),
                        other => {
                            return Err(format!(
                                "derived column '{}' expected Utf8, got {other:?}",
                                c.name
                            ));
                        }
                    }
                    offsets.push(data.len() as u32);
                }
                Vector::utf8(offsets, data)
            }
            LogicalType::Float64 => {
                let mut v = Vec::with_capacity(nrows);
                for row in 0..nrows {
                    v.push(match get(row) {
                        ScalarValue::Float64(x) => *x,
                        ScalarValue::Int64(x) => *x as f64,
                        other => {
                            return Err(format!(
                                "derived column '{}' expected Float64, got {other:?}",
                                c.name
                            ));
                        }
                    });
                }
                Vector::f64(v)
            }
            _ => {
                let mut v = Vec::with_capacity(nrows);
                for row in 0..nrows {
                    v.push(match get(row) {
                        ScalarValue::Int64(x) => *x,
                        ScalarValue::Int32(x) => *x as i64,
                        ScalarValue::Date32(x) => *x as i64,
                        other => {
                            return Err(format!(
                                "derived column '{}' expected an integer, got {other:?}",
                                c.name
                            ));
                        }
                    });
                }
                Vector::i64(v)
            }
        };
        cols.push(v);
    }
    Ok(DataChunk::new(cols))
}

/// Total row count of a table's parquet file (footer metadata only).
fn table_rows(ti: &TableInput) -> Result<u64, String> {
    let TableSource::Parquet(path) = &ti.source else {
        // Derived inputs are aggregates of base tables — never the largest;
        // rank them below any parquet table for root selection.
        return Ok(0);
    };
    let f = ParquetFile::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
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

/// The type default a LEFT-join miss attaches (the no-NULL engine's stand-in).
fn default_value(ty: LogicalType) -> ScalarValue {
    match ty {
        LogicalType::Utf8 => ScalarValue::Utf8("".into()),
        LogicalType::Float64 => ScalarValue::Float64(0.0),
        _ => ScalarValue::Int64(0),
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
        Expr::InSub { expr, .. } | Expr::InSet { expr, .. } | Expr::Substr { expr, .. } => {
            collect_slots(expr, out)
        }
        Expr::Case { whens, else_ } => {
            for (c, v) in whens {
                collect_slots(c, out);
                collect_slots(v, out);
            }
            collect_slots(else_, out);
        }
    }
}
