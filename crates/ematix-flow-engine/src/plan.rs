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
//! Parallelism: the root scan, every dim scan+emit, and the per-shard dim
//! merges are all morsel-parallel (`sched::MorselQueue`). Per-row-group
//! partials merge in row-group order and dim shards merge their row groups
//! in row-group order, so results are deterministic — and the scalar-SUM
//! path bit-identical to sequential — at any thread count.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use ematix_parquet_io::ParquetFile;

use crate::chunk::{DataChunk, Selection};
use crate::expr::{Expr, ScalarValue, eval_num_col, filter_expr, sum_expr_f64};
use crate::logical::{AggExpr, AggFunc, BoundQuery, OrderByKey, Slot, TableInput, TableSource};
use crate::scan::{ColKind, StockScan};
use crate::scan_native::{NativeColKind, decode_row_group};
use crate::sched::MorselQueue;
use crate::vector::{LogicalType, Utf8View, Vector};

/// A query result: named columns, row-major values.
#[derive(Clone, Debug)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<ScalarValue>>,
    /// COLUMNAR hand-off for derived consumers: when a derived execution's
    /// outputs are plain columns/literals (no ORDER BY / LIMIT / DISTINCT
    /// / windows), the result carries bounded output chunks instead of
    /// materialized rows (`rows` is then empty). Skips the per-cell
    /// `ScalarValue` round-trip — q4's 3.26M-row `year_total` paid 2.7s
    /// building rows plus a re-columnization per reference.
    pub col_chunks: Option<Vec<DataChunk>>,
}

/// Logical row count of a result in either representation.
fn result_len(r: &QueryResult) -> usize {
    match &r.col_chunks {
        Some(chunks) => chunks.iter().map(DataChunk::n_rows).sum(),
        None => r.rows.len(),
    }
}

/// Semantic equality: same columns and the same logical rows, whichever
/// representation each side carries.
impl PartialEq for QueryResult {
    fn eq(&self, other: &Self) -> bool {
        fn rows_of(r: &QueryResult) -> std::borrow::Cow<'_, [Vec<ScalarValue>]> {
            match &r.col_chunks {
                Some(chunks) => std::borrow::Cow::Owned(rows_from_chunks(chunks)),
                None => std::borrow::Cow::Borrowed(&r.rows),
            }
        }
        self.columns == other.columns && *rows_of(self) == *rows_of(other)
    }
}

/// Execute a bound query on the engine. Uncorrelated subqueries run first
/// (recursively), then substitute into the outer query as constants /
/// membership sets before the main pipeline runs.
pub fn execute(q: &BoundQuery) -> Result<QueryResult, String> {
    execute_with(q, &DerivedMemo::default())
}

fn execute_with(q: &BoundQuery, memo: &DerivedMemo) -> Result<QueryResult, String> {
    execute_mode(q, memo, false)
}

/// `columnar` requests the chunked result representation — legal only for
/// derived consumers ([`result_to_chunks`] handles both forms); the
/// top-level entry always uses rows.
fn execute_mode(q: &BoundQuery, memo: &DerivedMemo, columnar: bool) -> Result<QueryResult, String> {
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
    if !q.set_ops.is_empty() {
        return execute_set(q, memo, columnar);
    }
    if q.subqueries.is_empty() {
        return Executor {
            q,
            nthreads,
            interner: StrInterner::default(),
            memo,
            columnar,
        }
        .run();
    }
    let mut q2 = q.clone();
    resolve_subqueries(&mut q2, memo)?;
    Executor {
        q: &q2,
        nthreads,
        interner: StrInterner::default(),
        memo,
        columnar,
    }
    .run()
}

/// Execute a set-operation query: run the base block (its ORDER BY /
/// LIMIT withheld), fold each side's rows in with the set semantics, then
/// order and limit the COMBINED rows.
fn execute_set(q: &BoundQuery, memo: &DerivedMemo, columnar: bool) -> Result<QueryResult, String> {
    use crate::logical::SetOp;
    let mut base = q.clone();
    let set_ops = std::mem::take(&mut base.set_ops);
    let order_by = std::mem::take(&mut base.order_by);
    let limit = base.limit.take();

    let row_cmp = |a: &Vec<ScalarValue>, b: &Vec<ScalarValue>| {
        a.iter()
            .zip(b)
            .map(|(x, y)| cmp_scalar(x, y))
            .find(|o| *o != std::cmp::Ordering::Equal)
            .unwrap_or(std::cmp::Ordering::Equal)
    };
    let dedup = |mut rows: Vec<Vec<ScalarValue>>| {
        rows.sort_by(row_cmp);
        rows.dedup();
        rows
    };

    // A columnar consumer + pure UNION ALL (no dedup, no ordering) lets
    // each side hand off chunks and the union just CONCATENATE them.
    let col_mode = columnar
        && order_by.is_empty()
        && limit.is_none()
        && set_ops.iter().all(|(op, _)| matches!(op, SetOp::UnionAll));
    let mut r = execute_mode(&base, memo, col_mode)?;
    for (op, side) in &set_ops {
        let mut rs = execute_mode(side, memo, col_mode)?;
        if matches!(op, SetOp::UnionAll) && r.col_chunks.is_some() && rs.col_chunks.is_some() {
            if let (Some(dst), Some(src)) = (r.col_chunks.as_mut(), rs.col_chunks.take()) {
                dst.extend(src);
            }
            continue;
        }
        // A side whose own shape didn't qualify returned rows — downgrade
        // any chunked partner so the fold stays uniform.
        if let Some(chunks) = r.col_chunks.take() {
            r.rows = rows_from_chunks(&chunks);
        }
        let rs_rows = match rs.col_chunks.take() {
            Some(chunks) => rows_from_chunks(&chunks),
            None => rs.rows,
        };
        r.rows = match op {
            SetOp::UnionAll => {
                let mut rows = r.rows;
                rows.extend(rs_rows);
                rows
            }
            SetOp::Union => {
                let mut rows = r.rows;
                rows.extend(rs_rows);
                dedup(rows)
            }
            SetOp::Intersect => {
                let right = dedup(rs_rows);
                dedup(r.rows)
                    .into_iter()
                    .filter(|row| right.binary_search_by(|x| row_cmp(x, row)).is_ok())
                    .collect()
            }
            SetOp::Except => {
                let right = dedup(rs_rows);
                dedup(r.rows)
                    .into_iter()
                    .filter(|row| right.binary_search_by(|x| row_cmp(x, row)).is_err())
                    .collect()
            }
        };
    }
    order_rows(&mut r.rows, &order_by);
    if let Some(l) = limit {
        r.rows.truncate(l);
    }
    Ok(r)
}

/// Execute every subquery and substitute its result into the outer query's
/// expressions: `ScalarSub(i)` → the computed literal, `InSub(i)` → a
/// materialized [`Expr::InSet`].
fn resolve_subqueries(q: &mut BoundQuery, memo: &DerivedMemo) -> Result<(), String> {
    use std::collections::HashSet;
    use std::sync::Arc;

    let subs = std::mem::take(&mut q.subqueries);
    let mut scalars: Vec<Option<ScalarValue>> = vec![None; subs.len()];
    let mut sets: Vec<Option<Arc<HashSet<i64>>>> = vec![None; subs.len()];
    let mut str_sets: Vec<Option<Arc<HashSet<Box<str>>>>> = vec![None; subs.len()];

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
        let r = execute_with(sub, memo)?; // recursion handles subs-of-subs
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
            let mut strs: HashSet<Box<str>> = HashSet::new();
            for row in &r.rows {
                match &row[0] {
                    ScalarValue::Int64(v) => {
                        set.insert(*v);
                    }
                    ScalarValue::Int32(v) => {
                        set.insert(*v as i64);
                    }
                    ScalarValue::Date32(v) => {
                        set.insert(*v as i64);
                    }
                    ScalarValue::Utf8(v) => {
                        strs.insert(v.as_ref().into());
                    }
                    // A NULL element matches nothing by equality. (Strict
                    // SQL: `x NOT IN (…, NULL)` is UNKNOWN for every x —
                    // that full three-valued NOT IN is a labelled
                    // follow-on; membership tests here treat the set as
                    // its non-NULL elements.)
                    ScalarValue::Null => continue,
                    other => {
                        return Err(format!(
                            "IN subquery must yield integers or strings (got {other:?})"
                        ));
                    }
                };
            }
            if !strs.is_empty() && !set.is_empty() {
                return Err("IN subquery mixes integer and string values".into());
            }
            if !strs.is_empty() {
                str_sets[i] = Some(Arc::new(strs));
            } else {
                sets[i] = Some(Arc::new(set));
            }
        }
    }

    rewrite_query_exprs(q, &mut |e| match e {
        Expr::ScalarSub(i) => {
            *e = Expr::Literal(scalars[*i].clone().expect("scalar computed"));
        }
        Expr::InSub { expr, sub, negated } => {
            let inner = std::mem::replace(expr, Box::new(Expr::Column(0)));
            *e = if let Some(strs) = &str_sets[*sub] {
                Expr::InSetStr {
                    expr: inner,
                    set: strs.clone(),
                    negated: *negated,
                }
            } else {
                Expr::InSet {
                    expr: inner,
                    set: sets[*sub].clone().expect("set computed"),
                    negated: *negated,
                }
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
            Expr::ExtractYear(i)
            | Expr::CastInt(i)
            | Expr::Round { expr: i, .. }
            | Expr::Upper(i) => walk(i, f),
            Expr::Like { expr, .. }
            | Expr::InSub { expr, .. }
            | Expr::InSet { expr, .. }
            | Expr::InSetStr { expr, .. }
            | Expr::IsNull { expr, .. }
            | Expr::Substr { expr, .. } => walk(expr, f),
            Expr::Concat(parts) => {
                for p in parts {
                    walk(p, f);
                }
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
            Expr::ExtractYear(i)
            | Expr::CastInt(i)
            | Expr::Round { expr: i, .. }
            | Expr::Upper(i) => walk(i, f),
            Expr::Like { expr, .. }
            | Expr::InSub { expr, .. }
            | Expr::InSet { expr, .. }
            | Expr::InSetStr { expr, .. }
            | Expr::IsNull { expr, .. }
            | Expr::Substr { expr, .. } => walk(expr, f),
            Expr::Concat(parts) => {
                for p in parts {
                    walk(p, f);
                }
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
    nthreads: usize,
    /// String join keys intern to i64 ids for the duration of one run —
    /// string equi-joins ride the engine's integer key machinery. The
    /// per-lookup lock is fine: string-keyed joins are dim-sized in
    /// practice (fact-side hot keys stay integer, lock untouched).
    interner: StrInterner,
    /// The top-level call's derived-result memo (see [`DerivedMemo`]).
    memo: &'q DerivedMemo,
    /// The consumer accepts the chunked result representation (derived
    /// executions — see [`QueryResult::col_chunks`]).
    columnar: bool,
}

/// One top-level `execute` call's derived-query memo, keyed by the
/// `Arc<BoundQuery>` POINTER: every reference to a CTE shares one Arc (the
/// CteMap clones it per reference), so a CTE referenced N times — q4's six
/// `year_total` aliases, q23's cross-branch CTEs — materializes ONCE and
/// the other N−1 references reuse the rows. Non-CTE deriveds are unique
/// Arcs and simply miss. Dropped with the call, so no cross-query reuse.
#[derive(Default)]
struct DerivedMemo(Mutex<HashMap<usize, std::sync::Arc<QueryResult>>>);

/// One run's string-key interner (equal strings ⇒ equal ids).
#[derive(Default)]
struct StrInterner(Mutex<HashMap<Box<str>, i64>>);

impl StrInterner {
    fn intern(&self, s: &str) -> i64 {
        let mut m = self.0.lock().expect("lock");
        match m.get(s) {
            Some(&id) => id,
            None => {
                let id = m.len() as i64;
                m.insert(s.into(), id);
                id
            }
        }
    }
}

/// The (parent_local_col, child_local_col) equi-join pairs linking a dim to
/// its parent — several pairs form a composite key.
type Links = Vec<(usize, usize)>;

/// A fast non-cryptographic hasher for the engine's integer join keys
/// (multiplicative mixing + a murmur fmix64 finish). The std SipHash
/// default is DoS-resistant but several× slower on the probe-heavy join
/// paths; keys here are the user's own data, not attacker-controlled
/// protocol input.
#[derive(Default)]
struct FastHasher(u64);

impl std::hash::Hasher for FastHasher {
    #[inline]
    fn finish(&self) -> u64 {
        // fmix64: full avalanche, so hashbrown's low index bits and top-7
        // control bits are both well mixed.
        let mut h = self.0;
        h ^= h >> 33;
        h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
        h ^= h >> 33;
        h
    }
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0 ^ b as u64).wrapping_mul(0x0100_0000_01b3);
        }
    }
    #[inline]
    fn write_u64(&mut self, v: u64) {
        self.0 = (self.0.rotate_left(5) ^ v).wrapping_mul(0x517c_c1b7_2722_0a95);
    }
    #[inline]
    fn write_i64(&mut self, v: i64) {
        self.write_u64(v as u64);
    }
    #[inline]
    fn write_u32(&mut self, v: u32) {
        self.write_u64(v as u64);
    }
    #[inline]
    fn write_u8(&mut self, v: u8) {
        self.write_u64(v as u64);
    }
    #[inline]
    fn write_usize(&mut self, v: usize) {
        self.write_u64(v as u64);
    }
}

type FastMap<K, V> = HashMap<K, V, std::hash::BuildHasherDefault<FastHasher>>;

/// Pick the shard for a (composite) key — an independent multiplicative
/// mix taking TOP bits, uncorrelated with [`FastHasher`]'s in-map
/// placement. `nshards` must be a power of two.
#[inline]
fn shard_of(k: &[i64], nshards: usize) -> usize {
    let mut h = 0u64;
    for &x in k {
        h = (h.rotate_left(29) ^ x as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    }
    (h >> 33) as usize & (nshards - 1)
}

/// One shard of a dim map: key → (match count, HEAD payload row), with
/// the shard's payload values stored COLUMNAR (`pay[j]` aligned with the
/// subtree's `payload_slots[j]`). A probe returns a row index; the attach
/// step gathers typed values directly — no per-row `ScalarValue`.
///
/// `chain` is present only when this shard held a **duplicate payload
/// key** — several dim rows sharing one join key, each with its own
/// payload. It threads those rows into a per-key singly-linked list so
/// the root can fan a matching row out to one output row per dim row.
/// Absent (the common case) = every key maps to one payload row; the
/// probe's `count` is that row's multiplicity.
struct Shard<K> {
    map: FastMap<K, (u64, u32)>,
    pay: Vec<PayCol>,
    chain: Option<ShardChain>,
}

/// Per-key payload chains for a duplicate-key shard (indexed by payload
/// row). `next[r]` links to the next dim row sharing `r`'s key
/// (`NO_NEXT` = list end); `weight[r]` is that row's own multiplicity
/// (grandchild fan-out — 1 in the common case).
struct ShardChain {
    next: Vec<u32>,
    weight: Vec<u32>,
}

/// End-of-list sentinel for [`ShardChain::next`].
const NO_NEXT: u32 = u32::MAX;

/// A columnar payload store (one column of one shard): typed values plus
/// a lazily-materialized validity (`None` = no NULLs seen — the common
/// case pays nothing).
struct PayCol {
    data: PayData,
    valid: Option<Vec<bool>>,
}

enum PayData {
    /// Integer family — `Int32`/`Date32` widen to `i64` on entry (the
    /// representation the attach step has always produced).
    I64(Vec<i64>),
    F64(Vec<f64>),
    Str {
        offsets: Vec<u32>,
        data: Vec<u8>,
    },
}

impl PayCol {
    fn new(ty: LogicalType) -> PayCol {
        let data = match ty {
            LogicalType::Utf8 => PayData::Str {
                offsets: vec![0],
                data: Vec::new(),
            },
            LogicalType::Float64 => PayData::F64(Vec::new()),
            _ => PayData::I64(Vec::new()),
        };
        PayCol { data, valid: None }
    }

    fn len(&self) -> usize {
        match &self.data {
            PayData::I64(v) => v.len(),
            PayData::F64(v) => v.len(),
            PayData::Str { offsets, .. } => offsets.len() - 1,
        }
    }

    /// Is stored row `i` valid (non-NULL)?
    #[inline]
    fn row_valid(&self, i: usize) -> bool {
        self.valid.as_ref().is_none_or(|v| v[i])
    }

    /// Record one appended row's validity (call AFTER pushing the value).
    #[inline]
    fn note_valid(&mut self, ok: bool) {
        match (&mut self.valid, ok) {
            (Some(v), _) => v.push(ok),
            (None, true) => {}
            (None, false) => {
                let mut v = vec![true; self.len() - 1];
                v.push(false);
                self.valid = Some(v);
            }
        }
    }

    /// Append row `i` of a source chunk column (typed, no boxing; a NULL
    /// source row stores the type default with a false validity bit).
    #[inline]
    fn push_src(&mut self, src: &PaySrc, i: usize) {
        let ok = src.valid.is_none_or(|v| v[i]);
        match (&mut self.data, &src.view) {
            (PayData::I64(v), PayView::I64(s)) => v.push(if ok { s[i] } else { 0 }),
            (PayData::I64(v), PayView::I32(s)) => v.push(if ok { s[i] as i64 } else { 0 }),
            (PayData::F64(v), PayView::F64(s)) => v.push(if ok { s[i] } else { 0.0 }),
            (PayData::Str { offsets, data }, PayView::Str(view)) => {
                if ok {
                    data.extend_from_slice(view.get(i).as_bytes());
                }
                offsets.push(data.len() as u32);
            }
            _ => panic!("payload type mismatch"),
        }
        self.note_valid(ok);
    }

    /// Append row `i` of another payload column (a bubbled child value —
    /// NULL rows already store type defaults, so values copy verbatim).
    #[inline]
    fn push_from(&mut self, other: &PayCol, i: usize) {
        match (&mut self.data, &other.data) {
            (PayData::I64(v), PayData::I64(o)) => v.push(o[i]),
            (PayData::F64(v), PayData::F64(o)) => v.push(o[i]),
            (
                PayData::Str { offsets, data },
                PayData::Str {
                    offsets: oo,
                    data: od,
                },
            ) => {
                data.extend_from_slice(&od[oo[i] as usize..oo[i + 1] as usize]);
                offsets.push(data.len() as u32);
            }
            _ => panic!("payload type mismatch"),
        }
        self.note_valid(other.row_valid(i));
    }

    /// Bulk-append a whole emit buffer's column (the shard-merge step).
    fn append(&mut self, other: &PayCol) {
        let len_before = self.len();
        match (&mut self.data, &other.data) {
            (PayData::I64(v), PayData::I64(o)) => v.extend_from_slice(o),
            (PayData::F64(v), PayData::F64(o)) => v.extend_from_slice(o),
            (
                PayData::Str { offsets, data },
                PayData::Str {
                    offsets: oo,
                    data: od,
                },
            ) => {
                let base = data.len() as u32;
                data.extend_from_slice(od);
                offsets.extend(oo.iter().skip(1).map(|&e| base + e));
            }
            _ => panic!("payload type mismatch"),
        }
        match (&mut self.valid, &other.valid) {
            (Some(v), Some(o)) => v.extend_from_slice(o),
            (Some(v), None) => v.resize(v.len() + other.len(), true),
            (None, Some(o)) => {
                let mut v = vec![true; len_before];
                v.extend_from_slice(o);
                self.valid = Some(v);
            }
            (None, None) => {}
        }
    }
}

/// A borrowed typed view of a chunk column feeding payload emission.
struct PaySrc<'a> {
    view: PayView<'a>,
    valid: Option<&'a [bool]>,
}

enum PayView<'a> {
    I64(&'a [i64]),
    I32(&'a [i32]),
    F64(&'a [f64]),
    Str(Utf8View<'a>),
}

fn pay_src(chunk: &DataChunk, col: usize) -> PaySrc<'_> {
    let v = chunk.col(col);
    let view = match v.logical {
        LogicalType::Int64 => PayView::I64(v.as_i64()),
        LogicalType::Int32 | LogicalType::Date32 => PayView::I32(v.as_i32()),
        LogicalType::Float64 => PayView::F64(v.as_f64()),
        LogicalType::Utf8 => PayView::Str(v.as_utf8()),
    };
    PaySrc {
        view,
        valid: v.validity.as_deref(),
    }
}

/// A borrowed integer join-key column — probe loops read keys by direct
/// slice index instead of per-row interpreter dispatch. `get` returns
/// `None` for a NULL key (equality with NULL never matches, so a NULL
/// key is a guaranteed probe miss / dropped build row).
enum KeyCol<'a> {
    I64(&'a [i64], Option<&'a [bool]>),
    I32(&'a [i32], Option<&'a [bool]>),
    /// String keys intern to i64 through the run's [`StrInterner`].
    Str(Utf8View<'a>, Option<&'a [bool]>, &'a StrInterner),
    /// A constant key component — the keyless (cross-join) probe's `0`
    /// when widened keys follow it (mirrors `fill_key`'s empty-key
    /// convention on the build side).
    Const(i64),
}

impl KeyCol<'_> {
    #[inline]
    fn get(&self, i: usize) -> Option<i64> {
        match self {
            KeyCol::I64(s, valid) => match valid {
                Some(v) if !v[i] => None,
                _ => Some(s[i]),
            },
            KeyCol::I32(s, valid) => match valid {
                Some(v) if !v[i] => None,
                _ => Some(s[i] as i64),
            },
            KeyCol::Str(view, valid, interner) => match valid {
                Some(v) if !v[i] => None,
                _ => Some(interner.intern(view.get(i))),
            },
            KeyCol::Const(v) => Some(*v),
        }
    }
}

fn key_col<'a>(chunk: &'a DataChunk, col: usize, interner: &'a StrInterner) -> KeyCol<'a> {
    let v = chunk.col(col);
    let valid = v.validity.as_deref();
    match v.logical {
        LogicalType::Int64 => KeyCol::I64(v.as_i64(), valid),
        LogicalType::Int32 | LogicalType::Date32 => KeyCol::I32(v.as_i32(), valid),
        LogicalType::Utf8 => KeyCol::Str(v.as_utf8(), valid, interner),
        other => panic!("join key must be integer-family or string, got {other:?}"),
    }
}

/// Read a precomputed agg-argument column at row `i` as the aggregate's
/// f64 input, or `None` on SQL NULL — bit-identical to
/// [`Expr::eval_opt_f64`] (integer family widens, an invalid row is NULL).
#[inline]
fn col_opt_f64(v: &Vector, i: usize) -> Option<f64> {
    if v.validity.as_ref().is_some_and(|m| !m[i]) {
        return None;
    }
    Some(match v.logical {
        LogicalType::Float64 => v.as_f64()[i],
        _ => v.as_i64()[i] as f64,
    })
}

/// Where a widened key component's value comes from during a dim build.
enum ExtraKeySrc {
    /// The dim table's own chunk column.
    OwnCol(usize),
    /// Payload column `jc` of child subtree `ci` — read from the probed
    /// hit row during emission.
    Child { ci: usize, jc: usize },
}

/// Read a payload value as a key component (`i64`, strings interned).
/// `None` = NULL payload (can never key-match) — mirrors [`KeyCol::get`].
#[inline]
fn pay_key(pc: &PayCol, i: usize, interner: &StrInterner) -> Option<i64> {
    if !pc.row_valid(i) {
        return None;
    }
    match &pc.data {
        PayData::I64(v) => Some(v[i]),
        PayData::Str { offsets, data } => {
            let s = std::str::from_utf8(&data[offsets[i] as usize..offsets[i + 1] as usize])
                .expect("payload strings are valid utf8");
            Some(interner.intern(s))
        }
        PayData::F64(_) => None,
    }
}

/// Fill `kbuf` from `key_cols` at row `i`; `false` = a NULL key
/// component (the row can never join).
#[inline]
fn fill_key(kbuf: &mut Vec<i64>, key_cols: &[KeyCol], i: usize) -> bool {
    kbuf.clear();
    for kc in key_cols {
        match kc.get(i) {
            Some(v) => kbuf.push(v),
            None => return false,
        }
    }
    // A KEYLESS (cross-join) link uses the constant key 0 — every row on
    // both sides shares it, so a probe matches the whole dim (the fan-out's
    // early residual then prunes during expansion).
    if key_cols.is_empty() {
        kbuf.push(0);
    }
    true
}

/// A dim map sharded by key hash — shards scan-emit AND merge in
/// parallel; a probe pays one extra multiply to pick its shard.
enum DimMap {
    Single(Vec<Shard<i64>>),
    Multi(Vec<Shard<Vec<i64>>>),
}

impl DimMap {
    /// Probe: `Some((match weight, shard, payload row))`.
    #[inline]
    fn get(&self, k: &[i64]) -> Option<(u64, u32, u32)> {
        match self {
            DimMap::Single(shards) => {
                let s = shard_of(k, shards.len());
                shards[s].map.get(&k[0]).map(|&(w, r)| (w, s as u32, r))
            }
            DimMap::Multi(shards) => {
                let s = shard_of(k, shards.len());
                shards[s].map.get(k).map(|&(w, r)| (w, s as u32, r))
            }
        }
    }

    /// Payload column `j` of shard `s`.
    #[inline]
    fn pay_col(&self, s: usize, j: usize) -> &PayCol {
        match self {
            DimMap::Single(shards) => &shards[s].pay[j],
            DimMap::Multi(shards) => &shards[s].pay[j],
        }
    }

    /// Shard `s`'s duplicate-key chain, if it has one.
    #[inline]
    fn chain_of(&self, s: usize) -> Option<&ShardChain> {
        match self {
            DimMap::Single(shards) => shards[s].chain.as_ref(),
            DimMap::Multi(shards) => shards[s].chain.as_ref(),
        }
    }

    /// Any shard held a duplicate payload key (⇒ the root must fan out).
    fn multi(&self) -> bool {
        match self {
            DimMap::Single(s) => s.iter().any(|x| x.chain.is_some()),
            DimMap::Multi(s) => s.iter().any(|x| x.chain.is_some()),
        }
    }

    fn nshards(&self) -> usize {
        match self {
            DimMap::Single(s) => s.len(),
            DimMap::Multi(s) => s.len(),
        }
    }
}

/// Encoded (shard << 32 | payload row) probe result per root row;
/// `NO_REF` marks a LEFT miss (the attach writes the type default).
const NO_REF: u64 = u64::MAX;

/// Gather payload column `j` across shards into a full-length root column
/// (rows without a ref get the type default — 0 / 0.0 / "").
fn gather_payload(map: &DimMap, j: usize, refs: &[u64], ty: LogicalType) -> Vector {
    let cols: Vec<&PayCol> = (0..map.nshards()).map(|s| map.pay_col(s, j)).collect();
    // Rows without a ref (unselected, or LEFT-join misses) and rows whose
    // stored payload value is NULL both get a false validity bit; a fully
    // valid gather drops the mask (branch-free downstream).
    let mut valid: Vec<bool> = Vec::with_capacity(refs.len());
    let mut any_null = false;
    let mut ok = |b: bool, valid: &mut Vec<bool>| {
        valid.push(b);
        any_null |= !b;
    };
    let out = match ty {
        LogicalType::Float64 => {
            let mut v = vec![0.0f64; refs.len()];
            for (r, &e) in refs.iter().enumerate() {
                if e != NO_REF {
                    let col = cols[(e >> 32) as usize];
                    let i = (e & 0xffff_ffff) as usize;
                    let PayData::F64(c) = &col.data else {
                        panic!("payload type mismatch");
                    };
                    v[r] = c[i];
                    ok(col.row_valid(i), &mut valid);
                } else {
                    ok(false, &mut valid);
                }
            }
            Vector::f64(v)
        }
        LogicalType::Utf8 => {
            let mut offsets = Vec::with_capacity(refs.len() + 1);
            let mut data = Vec::new();
            offsets.push(0u32);
            for &e in refs {
                if e != NO_REF {
                    let col = cols[(e >> 32) as usize];
                    let i = (e & 0xffff_ffff) as usize;
                    let PayData::Str {
                        offsets: oo,
                        data: od,
                    } = &col.data
                    else {
                        panic!("payload type mismatch");
                    };
                    data.extend_from_slice(&od[oo[i] as usize..oo[i + 1] as usize]);
                    ok(col.row_valid(i), &mut valid);
                } else {
                    ok(false, &mut valid);
                }
                offsets.push(data.len() as u32);
            }
            Vector::utf8(offsets, data)
        }
        // Integer family attaches as i64 (Date32 payloads are day numbers;
        // EXTRACT and comparisons treat them identically).
        _ => {
            let mut v = vec![0i64; refs.len()];
            for (r, &e) in refs.iter().enumerate() {
                if e != NO_REF {
                    let col = cols[(e >> 32) as usize];
                    let i = (e & 0xffff_ffff) as usize;
                    let PayData::I64(c) = &col.data else {
                        panic!("payload type mismatch");
                    };
                    v[r] = c[i];
                    ok(col.row_valid(i), &mut valid);
                } else {
                    ok(false, &mut valid);
                }
            }
            Vector::i64(v)
        }
    };
    out.with_validity(any_null.then_some(valid))
}

/// A processed dim subtree: (composite) join key → (match count, columnar
/// payload row(s)) across shards; `payload_slots` names the attach
/// targets. `multi` = some key has several payload rows (the root fans
/// out one output row per row via [`DimMap::chain_of`]).
struct DimResult {
    payload_slots: Vec<usize>,
    map: DimMap,
    multi: bool,
}

/// Per-(row-group, shard) emission from a dim scan — keys flat with
/// stride `key_len`, weights aligned, payload values columnar.
struct EmitBuf {
    keys: Vec<i64>,
    weights: Vec<u64>,
    pay: Vec<PayCol>,
}

/// One row group's shard-routed emissions.
type RgEmits = Vec<EmitBuf>;

/// One row group's aggregation partial — merged in row-group order, which
/// keeps results deterministic and (for the scalar-SUM path) bit-identical
/// to the sequential execution at any thread count.
enum RgOut {
    Scalar(Vec<AggState>),
    Grouped(BTreeMap<Vec<GroupKey>, Vec<AggState>>),
    Rows(Vec<Vec<ScalarValue>>),
    /// A no-GROUP-BY window query: this row group's surviving rows, gathered
    /// into a slot-indexed chunk. The chunks concatenate across row groups
    /// into the single input the (global) window stage runs over.
    Chunk(DataChunk),
}

/// Shared, read-only context for per-row-group root processing.
struct RootCtx<'a> {
    root: usize,
    children: &'a [(usize, Links, bool)],
    dims: &'a [Option<DimResult>],
    post: &'a Option<Expr>,
    matched_cols: &'a HashMap<usize, usize>,
    filter: &'a Option<Expr>,
    /// Fan-out key widening per child: ordered (root-side slot,
    /// subtree-side slot) pairs whose root-side columns append to the
    /// probe key (the dim was built with the matching subtree-side
    /// values appended).
    widen: &'a HashMap<usize, Vec<(usize, usize)>>,
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
            loop {
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
                // A DISCONNECTED component (no join edge to anything seen —
                // a deliberate cross join, q2/q8): its first table attaches
                // as a KEYLESS child of the root (empty links = constant
                // key, every root row × every component row; the fan-out's
                // early residual prunes during expansion), then its own
                // component joins on normally below it.
                match seen.iter().position(|s| !s) {
                    Some(t) => {
                        seen[t] = true;
                        children[root].push((t, Vec::new(), false));
                        frontier.push_back(t);
                    }
                    None => break,
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
            // A no-GROUP-BY window query also reads its ORDER BY / PARTITION
            // BY / argument expressions from the slot-space chunk.
            for w in &q.windows {
                collect_slots(&w.arg, &mut needed);
                for p in &w.partition {
                    collect_slots(p, &mut needed);
                }
                for (o, _) in &w.order {
                    collect_slots(o, &mut needed);
                }
            }
        }
        needed.sort_unstable();
        needed.dedup();

        // ---- FAN-OUT KEY WIDENING: a post-join conjunct
        // `Eq(Column(x), Column(y))` whose sides live in DIFFERENT root
        // subtrees promotes into the subtree-side child's composite probe
        // key — the expansion then never emits the rows the residual
        // would kill (q72's cs⋈inventory fans ~1300× on item_sk alone;
        // with d2's week_seq in the key it fans ~5×). Sound because a
        // key mismatch drops exactly the rows the (still-applied,
        // idempotent) Eq conjunct drops, and a NULL on either side is a
        // probe miss = the conjunct's UNKNOWN. INNER children only: for
        // a LEFT child a key miss would keep the root row where the
        // post-filter drops it. Float slots are excluded (key equality
        // is exact i64 / interned-string identity; the interpreter
        // compares floats via f64 — keep those in the filter).
        // widen[c] = ordered (root-side slot, subtree-side slot) pairs;
        // widen_order = (must-attach-first child, widened child).
        let mut widen: HashMap<usize, Vec<(usize, usize)>> = HashMap::new();
        let mut widen_order: Vec<(usize, usize)> = Vec::new();
        {
            // owner[t] = the root child whose subtree contains table t.
            let mut owner: Vec<Option<usize>> = vec![None; n];
            for (c, _, _) in &children[root] {
                let mut stack = vec![*c];
                while let Some(t) = stack.pop() {
                    owner[t] = Some(*c);
                    for (cc, _, _) in &children[t] {
                        stack.push(*cc);
                    }
                }
            }
            let left_of: HashMap<usize, bool> =
                children[root].iter().map(|(c, _, l)| (*c, *l)).collect();
            // Same key family on both sides: 0 = integer, 1 = string,
            // 2 = float (never widened).
            let fam = |s: usize| match self.slot_ty(s) {
                LogicalType::Utf8 => 1u8,
                LogicalType::Float64 => 2,
                _ => 0,
            };
            // Would constraint (before, after) close a cycle?
            let reaches = |order: &[(usize, usize)], from: usize, to: usize| -> bool {
                let mut stack = vec![from];
                let mut seen: Vec<usize> = Vec::new();
                while let Some(x) = stack.pop() {
                    if x == to {
                        return true;
                    }
                    if seen.contains(&x) {
                        continue;
                    }
                    seen.push(x);
                    stack.extend(order.iter().filter(|&&(b, _)| b == x).map(|&(_, a)| a));
                }
                false
            };
            // Subtree size hints (parquet footer row counts): widening
            // pays on the side that would otherwise FAN OUT, so the
            // larger subtree wins the key and the smaller side stays the
            // probe-time column. (Widening the small side is not just
            // pointless — its ordering constraint would drag the big
            // fan-out ahead of the narrowing dims: q72 went 5s → 12s
            // when d1 won the week equality instead of inventory.)
            // A widened dim LARGER than the probe side is excluded
            // entirely: rebuilding its map at the widened cardinality
            // costs more than the expansion it saves (q72's 133M-row
            // inventory vs the 14M-row catalog_sales root — build went
            // 1.0s → 7.1s while the probe saved ~1.5s).
            let root_rows = self.table_rows_hint(root).max(1);
            let mut subtree_hint: HashMap<usize, u64> = HashMap::new();
            for (t, o) in owner.iter().enumerate() {
                if let Some(c) = o {
                    let rows = self.table_rows_hint(t);
                    let e = subtree_hint.entry(*c).or_insert(0);
                    *e = (*e).max(rows);
                }
            }
            let mut conjuncts: Vec<&Expr> = Vec::new();
            if let Some(p) = &post {
                split_and_expr(p, &mut conjuncts);
            }
            for cj in conjuncts {
                let Expr::Binary {
                    op: crate::expr::BinaryOp::Eq,
                    lhs,
                    rhs,
                } = cj
                else {
                    continue;
                };
                let (Expr::Column(a), Expr::Column(b)) = (lhs.as_ref(), rhs.as_ref()) else {
                    continue;
                };
                if fam(*a) != fam(*b) || fam(*a) == 2 {
                    continue;
                }
                // Of the (up to two) legal orientations, the SUBTREE side
                // with the larger size hint widens.
                let mut best: Option<(u64, usize, Option<usize>, usize, usize)> = None;
                for (sub, other) in [(*a, *b), (*b, *a)] {
                    let Some(c) = owner[q.slots[sub].table] else {
                        continue; // sub side is the root itself
                    };
                    let d = owner[q.slots[other].table]; // None = root
                    if d == Some(c) || left_of.get(&c).copied().unwrap_or(false) {
                        continue;
                    }
                    if let Some(d) = d
                        && (left_of.get(&d).copied().unwrap_or(false)
                            || reaches(&widen_order, c, d))
                    {
                        continue;
                    }
                    let hint = subtree_hint.get(&c).copied().unwrap_or(0);
                    if best.is_none_or(|(h, ..)| hint > h) {
                        best = Some((hint, c, d, other, sub));
                    }
                }
                // The size gate applies to the CHOSEN side, with no
                // fallback: when the fan-out side is too big to widen,
                // widening the small side instead would drag the big
                // fan-out ahead of the narrowing dims (the q72 5s → 12s
                // failure mode) — better to widen nothing.
                if best.is_some_and(|(h, ..)| h > root_rows) {
                    best = None;
                }
                if let Some((hint, c, d, other, sub)) = best {
                    if std::env::var("EMAT_TRACE_JOIN").is_ok() {
                        eprintln!(
                            "join: widen child '{}' key with slot {sub} (= root-side {other}), \
                             size hint {hint}",
                            q.tables[c].name
                        );
                    }
                    if let Some(d) = d {
                        widen_order.push((d, c));
                    }
                    widen.entry(c).or_default().push((other, sub));
                }
            }
        }

        // Debug/bench escape hatch.
        if std::env::var("EMAT_NO_WIDEN").is_ok() {
            widen.clear();
            widen_order.clear();
        }

        // ---- Process every dim subtree bottom-up.
        let mut dim_results: Vec<Option<DimResult>> = Vec::with_capacity(n);
        for _ in 0..n {
            dim_results.push(None);
        }
        for (child, links, _) in &children[root] {
            let child_cols: Vec<usize> = links.iter().map(|&(_, c)| c).collect();
            let extra: Vec<usize> = widen
                .get(child)
                .map(|v| v.iter().map(|&(_, sub)| sub).collect())
                .unwrap_or_default();
            let t0 = std::time::Instant::now();
            dim_results[*child] =
                Some(self.build_dim(*child, &child_cols, &children, &needed, &extra)?);
            if std::env::var("EMAT_TRACE_JOIN").is_ok() {
                eprintln!(
                    "join: built dim '{}' in {:?}",
                    q.tables[*child].name,
                    t0.elapsed()
                );
            }
        }

        // ---- Root: MORSEL-PARALLEL per row group. Each worker decodes a
        // row group, applies the root filter, probes/attaches the dim
        // subtrees, evaluates the post-join predicate, and aggregates a
        // per-RG partial. Partials merge in ROW-GROUP ORDER, so the result
        // is deterministic — and bit-identical to sequential — at any
        // thread count.
        let ti = &q.tables[root];
        // Multi (duplicate-key fan-out) children EXPAND the view — process
        // them LAST (stable partition) so every single-key payload (dates,
        // demographics) attaches first and the fan-out's early residual
        // filter can see those columns (q72's week-match + qty predicates).
        let mut root_children = children[root].clone();
        root_children.sort_by_key(|(c, _, _)| dim_results[*c].as_ref().is_some_and(|d| d.multi));
        // A widened child's root-side key slots come from other subtrees'
        // payloads — those children MUST attach first (overrides the
        // multi-last preference; widen_order is acyclic by construction).
        for _ in 0..widen_order.len() {
            for &(before, after) in &widen_order {
                let pb = root_children.iter().position(|(t, _, _)| *t == before);
                let pa = root_children.iter().position(|(t, _, _)| *t == after);
                if let (Some(pb), Some(pa)) = (pb, pa)
                    && pb > pa
                {
                    let item = root_children.remove(pb);
                    root_children.insert(pa, item);
                }
            }
        }
        // Matched-flag columns (LEFT children) append past the slot space
        // in (processing-order) child order — a fixed layout every chunk.
        let mut matched_cols: HashMap<usize, usize> = HashMap::new();
        {
            let mut next = q.slots.len();
            for (child, _, left) in &root_children {
                if *left {
                    matched_cols.insert(*child, next);
                    next += 1;
                }
            }
        }
        let ctx = RootCtx {
            root,
            children: &root_children,
            dims: &dim_results,
            post: &post,
            matched_cols: &matched_cols,
            filter: &ti.filter,
            widen: &widen,
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
        let (columns, mut rows, col_chunks) =
            if q.group.is_empty() && q.aggs.is_empty() && !q.windows.is_empty() {
                // No-GROUP-BY window query: concatenate every RG's slot chunk
                // into the single input, append one column per window expression
                // (row space extends to [slots…, windows…]), then project.
                let columns: Vec<String> = q.output.iter().map(|o| o.name.clone()).collect();
                let chunks: Vec<DataChunk> = outputs
                    .into_iter()
                    .flatten()
                    .filter_map(|out| match out {
                        RgOut::Chunk(c) => Some(c),
                        _ => None,
                    })
                    .collect();
                let mut base = concat_chunks(chunks);
                let mut n = base.n_rows();
                // A lone rank-family window armed with top-K (an outer
                // `rk <= K` filter): prune each partition to its K best
                // rows BEFORE the sort/projection — q67's dw2 shrinks a
                // 5.8M-row window input to ~K·partitions.
                if let [w] = &q.windows[..] {
                    if let Some(k) = w.top_k {
                        let keep = rank_topk_keep(w, &base, n, k);
                        if keep.len() < n {
                            base = DataChunk::new(
                                base.cols.iter().map(|c| gather_rows(c, &keep)).collect(),
                            );
                            n = keep.len();
                        }
                    }
                }
                let mut cols = base.cols.clone();
                for w in &q.windows {
                    cols.push(compute_window(w, &base, n));
                }
                let full = DataChunk::new(cols);
                let rows = (0..n)
                    .map(|r| {
                        q.output
                            .iter()
                            .map(|o| o.expr.eval_value(&full, r))
                            .collect()
                    })
                    .collect();
                (columns, rows, None)
            } else if q.group.is_empty() && q.aggs.is_empty() {
                let columns: Vec<String> = q.output.iter().map(|o| o.name.clone()).collect();
                let mut rows = Vec::new();
                for out in outputs.into_iter().flatten() {
                    if let RgOut::Rows(mut r) = out {
                        rows.append(&mut r);
                    }
                }
                (columns, rows, None)
            } else {
                let trace = std::env::var("EMAT_TRACE_AGG").is_ok();
                let t0 = std::time::Instant::now();
                let mut maps: Vec<BTreeMap<Vec<GroupKey>, Vec<AggState>>> = Vec::new();
                let mut scalar: Option<Vec<AggState>> = None;
                for out in outputs.into_iter().flatten() {
                    match out {
                        RgOut::Scalar(states) => {
                            let entry = scalar
                                .get_or_insert_with(|| vec![AggState::default(); q.aggs.len()]);
                            for (a, b) in entry.iter_mut().zip(&states) {
                                a.merge(b);
                            }
                        }
                        RgOut::Grouped(map) => maps.push(map),
                        RgOut::Rows(_) | RgOut::Chunk(_) => {
                            unreachable!("plain-row / window-chunk handled above")
                        }
                    }
                }
                let mut groups = kway_merge_groups(maps, self.nthreads);
                if let Some(states) = scalar {
                    // Scalar partials only exist when `group` is empty, so
                    // the merged vec holds at most the one empty-key entry.
                    match groups.first_mut() {
                        Some((k, s)) if k.is_empty() => {
                            for (a, b) in s.iter_mut().zip(&states) {
                                a.merge(b);
                            }
                        }
                        _ => groups.push((Vec::new(), states)),
                    }
                }
                // A scalar aggregate over zero surviving row groups still
                // yields one (default) row — matching sequential semantics.
                if groups.is_empty() && q.group.is_empty() {
                    groups.push((Vec::new(), vec![AggState::default(); q.aggs.len()]));
                }
                if trace {
                    eprintln!("agg: merged {} groups in {:?}", groups.len(), t0.elapsed());
                }
                let t1 = std::time::Instant::now();
                if !q.rollup_terms.is_empty() {
                    add_rollup_levels(&mut groups, &q.rollup_terms);
                    if trace {
                        eprintln!(
                            "agg: rollup -> {} groups in {:?}",
                            groups.len(),
                            t1.elapsed()
                        );
                    }
                }
                let t2 = std::time::Instant::now();
                let r = self.finalize_groups(groups)?;
                if trace {
                    let n =
                        r.2.as_ref()
                            .map_or(r.1.len(), |c| c.iter().map(DataChunk::n_rows).sum());
                    eprintln!("agg: finalize {n} rows in {:?}", t2.elapsed());
                }
                r
            };
        // `SELECT DISTINCT` grouping did not fold away (see BoundQuery::
        // distinct): dedup the result rows before ORDER BY / LIMIT so a
        // LIMIT counts distinct rows, not raw ones.
        if q.distinct {
            rows.sort_by(|a, b| {
                a.iter()
                    .zip(b)
                    .map(|(x, y)| cmp_scalar(x, y))
                    .find(|o| o.is_ne())
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            rows.dedup();
        }
        order_rows(&mut rows, &q.order_by);
        if let Some(l) = q.limit {
            rows.truncate(l);
        }
        // Drop ORDER-BY-only hidden outputs now that sorting is done.
        let mut columns = columns;
        if q.hidden_outputs > 0 {
            let keep = q.output.len() - q.hidden_outputs;
            columns.truncate(keep);
            for row in &mut rows {
                row.truncate(keep);
            }
        }
        Ok(QueryResult {
            columns,
            rows,
            col_chunks,
        })
    }

    /// Recursively process dim table `t` (joined to its parent via its
    /// local columns `link_cols` — a composite key when several) into a
    /// sharded key → (weight, columnar payload) map. Two parallel phases:
    /// scan+emit (morsel-parallel per row group, each surviving row routed
    /// to a shard bucket) and per-shard merge (morsel-parallel per shard)
    /// — the multi-million-insert sequential dim build was the wall-clock
    /// tail once the root scan went parallel.
    fn build_dim(
        &self,
        t: usize,
        link_cols: &[usize],
        children: &[Vec<(usize, Links, bool)>],
        needed: &[usize],
        extra_key_slots: &[usize],
    ) -> Result<DimResult, String> {
        let q = self.q;
        // This subtree's own payload slots…
        let mut payload_slots: Vec<usize> = needed
            .iter()
            .copied()
            .filter(|&s| q.slots[s].table == t)
            .collect();
        let own_payload = payload_slots.clone();
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
            let r = self.build_dim(*child, &child_cols, children, needed, &[])?;
            payload_slots.extend(r.payload_slots.iter().copied());
            child_results.push((links.iter().map(|&(p, _)| p).collect(), r));
        }

        // Widened-key sources: each extra key slot is either this table's
        // own column or a descendant's payload (its value arrives with
        // the child probe hit during emission).
        let extra_srcs: Vec<ExtraKeySrc> = extra_key_slots
            .iter()
            .map(|&s| {
                if q.slots[s].table == t {
                    return ExtraKeySrc::OwnCol(q.slots[s].col);
                }
                let (ci, (_, r)) = child_results
                    .iter()
                    .enumerate()
                    .find(|(_, (_, r))| r.payload_slots.contains(&s))
                    .expect("widened key slot must live in this subtree");
                let jc = r
                    .payload_slots
                    .iter()
                    .position(|&x| x == s)
                    .expect("position exists");
                ExtraKeySrc::Child { ci, jc }
            })
            .collect();

        let pay_tys: Vec<LogicalType> = payload_slots.iter().map(|&s| self.slot_ty(s)).collect();
        // A keyless cross-join child stores the constant key `0` (stride 1;
        // see fill_key) — chunks_exact(0) in the merge would panic.
        // Widened keys extend the stride.
        let key_len = link_cols.len().max(1) + extra_srcs.len();
        let nshards = self.nthreads.next_power_of_two().clamp(1, 64);

        // ---- Phase 1: morsel-parallel scan+emit. Each worker decodes a
        // row group, applies the table filter, probes the child maps
        // (direct-slice keys, scratch buffer, zero per-row allocation),
        // and routes surviving rows into per-shard buffers.
        let ti = &q.tables[t];
        let src = self.table_src(t)?;
        let n_rg = src.n_rg();
        let emits: Mutex<Vec<Option<RgEmits>>> = Mutex::new((0..n_rg).map(|_| None).collect());
        let queue = MorselQueue::new(n_rg);
        let nworkers = self.nthreads.clamp(1, n_rg.max(1));
        let (src_ref, queue_ref, emits_ref) = (&src, &queue, &emits);
        let (cr_ref, tys_ref, own_ref) = (&child_results, &pay_tys, &own_payload);
        let ex_ref = &extra_srcs;
        // A multi (fan-out) child forces the general cartesian-product
        // emit: this table's row expands to one dim row per COMBINATION of
        // its children's payload rows (a child chain contributes several).
        // The common case (no multi child) keeps the single-row fast path.
        let any_multi = child_results.iter().any(|(_, r)| r.multi);
        std::thread::scope(|scope| -> Result<(), String> {
            let mut handles = Vec::with_capacity(nworkers);
            for _ in 0..nworkers {
                handles.push(scope.spawn(move || -> Result<(), String> {
                    let local_stock = src_ref.open_local_stock()?;
                    while let Some(rg) = queue_ref.next() {
                        let chunk = src_ref.decode(rg, local_stock.as_ref())?;
                        let sel = match &ti.filter {
                            None => chunk.sel.clone(),
                            Some(pred) => filter_expr(&self.slot_view(t, &chunk), pred),
                        };
                        let mut bufs: RgEmits = (0..nshards)
                            .map(|_| EmitBuf {
                                keys: Vec::new(),
                                weights: Vec::new(),
                                pay: tys_ref.iter().map(|&ty| PayCol::new(ty)).collect(),
                            })
                            .collect();
                        let key_cols: Vec<KeyCol> = link_cols
                            .iter()
                            .map(|&c| key_col(&chunk, c, &self.interner))
                            .collect();
                        let child_keys: Vec<Vec<KeyCol>> = cr_ref
                            .iter()
                            .map(|(pk, _)| {
                                pk.iter()
                                    .map(|&c| key_col(&chunk, c, &self.interner))
                                    .collect()
                            })
                            .collect();
                        let own_srcs: Vec<PaySrc> = own_ref
                            .iter()
                            .map(|&s| pay_src(&chunk, q.slots[s].col))
                            .collect();
                        // Widened extras: own-column sources read this
                        // chunk directly; child-payload sources read the
                        // probed hit during emission.
                        let extra_own: Vec<Option<KeyCol>> = ex_ref
                            .iter()
                            .map(|e| match e {
                                ExtraKeySrc::OwnCol(c) => Some(key_col(&chunk, *c, &self.interner)),
                                ExtraKeySrc::Child { .. } => None,
                            })
                            .collect();
                        let mut kbuf: Vec<i64> = Vec::with_capacity(8);
                        if !any_multi {
                            // FAST PATH: every child contributes one payload
                            // row, so each surviving row emits exactly one
                            // dim row (weight = product of match counts).
                            let mut hits: Vec<(u32, u32)> = Vec::with_capacity(cr_ref.len());
                            sel.for_each(|i| {
                                let i = i as usize;
                                let mut weight = 1u64;
                                hits.clear();
                                for (kcs, (_, r)) in child_keys.iter().zip(cr_ref) {
                                    match fill_key(&mut kbuf, kcs, i)
                                        .then(|| r.map.get(&kbuf))
                                        .flatten()
                                    {
                                        None => {
                                            weight = 0;
                                            break;
                                        }
                                        Some((w, s, row)) => {
                                            weight *= w;
                                            hits.push((s, row));
                                        }
                                    }
                                }
                                if weight == 0 {
                                    return;
                                }
                                // A NULL own-key row can never be matched.
                                if !fill_key(&mut kbuf, &key_cols, i) {
                                    return;
                                }
                                // Widened extras append after the base key
                                // (a NULL extra can never match either).
                                for (ex, own) in ex_ref.iter().zip(&extra_own) {
                                    let v = match ex {
                                        ExtraKeySrc::OwnCol(_) => {
                                            own.as_ref().expect("own key col").get(i)
                                        }
                                        ExtraKeySrc::Child { ci, jc } => {
                                            let (s, row) = hits[*ci];
                                            pay_key(
                                                cr_ref[*ci].1.map.pay_col(s as usize, *jc),
                                                row as usize,
                                                &self.interner,
                                            )
                                        }
                                    };
                                    match v {
                                        Some(v) => kbuf.push(v),
                                        None => return,
                                    }
                                }
                                let buf = &mut bufs[shard_of(&kbuf, nshards)];
                                buf.keys.extend_from_slice(&kbuf);
                                buf.weights.push(weight);
                                let mut j = 0;
                                for src in &own_srcs {
                                    buf.pay[j].push_src(src, i);
                                    j += 1;
                                }
                                for ((_, r), &(s, row)) in cr_ref.iter().zip(&hits) {
                                    for jc in 0..r.payload_slots.len() {
                                        buf.pay[j]
                                            .push_from(r.map.pay_col(s as usize, jc), row as usize);
                                        j += 1;
                                    }
                                }
                            });
                        } else {
                            // GENERAL PATH: a child may contribute SEVERAL
                            // payload rows (its chain); this row emits the
                            // cartesian product of the children's payload
                            // rows — walked with an odometer over per-child
                            // match lists (shard, payload row, weight).
                            let ncr = cr_ref.len();
                            let mut matches: Vec<Vec<(u32, u32, u64)>> = vec![Vec::new(); ncr];
                            let mut idx: Vec<usize> = vec![0; ncr];
                            sel.for_each(|i| {
                                let i = i as usize;
                                let mut ok = true;
                                for (ci, (kcs, (_, r))) in child_keys.iter().zip(cr_ref).enumerate()
                                {
                                    matches[ci].clear();
                                    let hit = fill_key(&mut kbuf, kcs, i)
                                        .then(|| r.map.get(&kbuf))
                                        .flatten();
                                    let Some((count, s, head)) = hit else {
                                        ok = false;
                                        break;
                                    };
                                    match r.map.chain_of(s as usize) {
                                        Some(ch) if ch.next[head as usize] != NO_NEXT => {
                                            let mut rr = head;
                                            loop {
                                                matches[ci].push((
                                                    s,
                                                    rr,
                                                    ch.weight[rr as usize] as u64,
                                                ));
                                                let nx = ch.next[rr as usize];
                                                if nx == NO_NEXT {
                                                    break;
                                                }
                                                rr = nx;
                                            }
                                        }
                                        _ => matches[ci].push((s, head, count)),
                                    }
                                }
                                if !ok || !fill_key(&mut kbuf, &key_cols, i) {
                                    return;
                                }
                                // Own-column widened extras are per-row
                                // constants; child-payload extras vary per
                                // odometer combination and rebuild inside
                                // the loop (in extras order).
                                let mut own_vals: Vec<i64> = Vec::new();
                                for (ex, own) in ex_ref.iter().zip(&extra_own) {
                                    match ex {
                                        ExtraKeySrc::OwnCol(_) => {
                                            match own.as_ref().expect("own key col").get(i) {
                                                Some(v) => own_vals.push(v),
                                                None => return,
                                            }
                                        }
                                        ExtraKeySrc::Child { .. } => own_vals.push(0),
                                    }
                                }
                                let base_len = kbuf.len();
                                let fixed_shard = if ex_ref.is_empty() {
                                    Some(shard_of(&kbuf, nshards))
                                } else {
                                    None
                                };
                                for x in idx.iter_mut() {
                                    *x = 0;
                                }
                                loop {
                                    kbuf.truncate(base_len);
                                    let mut dead = false;
                                    for (xi, ex) in ex_ref.iter().enumerate() {
                                        let v = match ex {
                                            ExtraKeySrc::OwnCol(_) => Some(own_vals[xi]),
                                            ExtraKeySrc::Child { ci, jc } => {
                                                let (s, row, _) = matches[*ci][idx[*ci]];
                                                pay_key(
                                                    cr_ref[*ci].1.map.pay_col(s as usize, *jc),
                                                    row as usize,
                                                    &self.interner,
                                                )
                                            }
                                        };
                                        match v {
                                            Some(v) => kbuf.push(v),
                                            None => {
                                                dead = true;
                                                break;
                                            }
                                        }
                                    }
                                    if !dead {
                                        let mut weight = 1u64;
                                        for ci in 0..ncr {
                                            weight *= matches[ci][idx[ci]].2;
                                        }
                                        let shard =
                                            fixed_shard.unwrap_or_else(|| shard_of(&kbuf, nshards));
                                        let buf = &mut bufs[shard];
                                        buf.keys.extend_from_slice(&kbuf);
                                        buf.weights.push(weight);
                                        let mut j = 0;
                                        for src in &own_srcs {
                                            buf.pay[j].push_src(src, i);
                                            j += 1;
                                        }
                                        for (ci, (_, r)) in cr_ref.iter().enumerate() {
                                            let (s, row, _) = matches[ci][idx[ci]];
                                            for jc in 0..r.payload_slots.len() {
                                                buf.pay[j].push_from(
                                                    r.map.pay_col(s as usize, jc),
                                                    row as usize,
                                                );
                                                j += 1;
                                            }
                                        }
                                    }
                                    // Advance the odometer (least-significant
                                    // child first); carry-out = done.
                                    let mut ci = ncr;
                                    let mut carry = true;
                                    while ci > 0 {
                                        ci -= 1;
                                        idx[ci] += 1;
                                        if idx[ci] < matches[ci].len() {
                                            carry = false;
                                            break;
                                        }
                                        idx[ci] = 0;
                                    }
                                    if carry {
                                        break;
                                    }
                                }
                            });
                        }
                        emits_ref.lock().expect("lock")[rg] = Some(bufs);
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
        let emits: Vec<RgEmits> = emits
            .into_inner()
            .expect("no poisoned lock")
            .into_iter()
            .map(|o| o.expect("every row group emitted"))
            .collect();

        // ---- Phase 2: per-shard merge, shards in parallel. Each shard
        // folds its buffers in row-group order, so the layout (and the
        // per-key chain order) is deterministic at any thread count.
        let map = if key_len == 1 {
            DimMap::Single(self.merge_shards(&emits, key_len, nshards, &pay_tys, |k| k[0])?)
        } else {
            DimMap::Multi(self.merge_shards(&emits, key_len, nshards, &pay_tys, |k| k.to_vec())?)
        };
        let multi = map.multi();
        Ok(DimResult {
            payload_slots,
            map,
            multi,
        })
    }

    /// Merge every row group's emit buffers into per-shard maps + columnar
    /// payload stores, shards in parallel. A duplicate key with no payload
    /// accumulates its match count (the semijoin narrow); a duplicate key
    /// WITH payload threads the extra dim rows onto a per-key chain (the
    /// duplicate-key payload fan-out).
    fn merge_shards<K: std::hash::Hash + Eq + Send>(
        &self,
        emits: &[RgEmits],
        key_len: usize,
        nshards: usize,
        pay_tys: &[LogicalType],
        make_key: impl Fn(&[i64]) -> K + Sync,
    ) -> Result<Vec<Shard<K>>, String> {
        let has_payload = !pay_tys.is_empty();
        let out: Mutex<Vec<Option<Shard<K>>>> = Mutex::new((0..nshards).map(|_| None).collect());
        let queue = MorselQueue::new(nshards);
        let nworkers = self.nthreads.clamp(1, nshards);
        let (queue_ref, out_ref, mk) = (&queue, &out, &make_key);
        std::thread::scope(|scope| -> Result<(), String> {
            let mut handles = Vec::with_capacity(nworkers);
            for _ in 0..nworkers {
                handles.push(scope.spawn(move || -> Result<(), String> {
                    while let Some(s) = queue_ref.next() {
                        let total: usize = emits.iter().map(|rg| rg[s].weights.len()).sum();
                        let mut map: FastMap<K, (u64, u32)> =
                            FastMap::with_capacity_and_hasher(total, Default::default());
                        let mut pay: Vec<PayCol> =
                            pay_tys.iter().map(|&ty| PayCol::new(ty)).collect();
                        // Built lazily on the first duplicate payload key.
                        let mut chain: Option<ShardChain> = None;
                        for rg in emits {
                            let buf = &rg[s];
                            let base = pay.first().map_or(0, PayCol::len);
                            let buf_len = buf.weights.len();
                            if let Some(ch) = &mut chain {
                                ch.next.resize(base + buf_len, NO_NEXT);
                                ch.weight.resize(base + buf_len, 0);
                            }
                            for (idx, key) in buf.keys.chunks_exact(key_len).enumerate() {
                                let gidx = (base + idx) as u32;
                                let w = buf.weights[idx];
                                use std::collections::hash_map::Entry;
                                match map.entry(mk(key)) {
                                    Entry::Vacant(e) => {
                                        e.insert((w, gidx));
                                        if let Some(ch) = &mut chain {
                                            ch.weight[gidx as usize] = w as u32;
                                        }
                                    }
                                    Entry::Occupied(mut e) => {
                                        if !has_payload {
                                            e.get_mut().0 += w;
                                            continue;
                                        }
                                        let (old_cnt, old_head) = *e.get();
                                        let ch = chain.get_or_insert_with(|| ShardChain {
                                            next: vec![NO_NEXT; base + buf_len],
                                            weight: vec![0; base + buf_len],
                                        });
                                        // A length-1 head's own weight equals
                                        // its stored count (unique until now);
                                        // record it before it becomes interior.
                                        if ch.next[old_head as usize] == NO_NEXT {
                                            ch.weight[old_head as usize] = old_cnt as u32;
                                        }
                                        // Prepend the new row as the head.
                                        ch.next[gidx as usize] = old_head;
                                        ch.weight[gidx as usize] = w as u32;
                                        e.insert((old_cnt + w, gidx));
                                    }
                                }
                            }
                            for (p, o) in pay.iter_mut().zip(&buf.pay) {
                                p.append(o);
                            }
                        }
                        out_ref.lock().expect("lock")[s] = Some(Shard { map, pay, chain });
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
        Ok(out
            .into_inner()
            .expect("no poisoned lock")
            .into_iter()
            .map(|o| o.expect("every shard merged"))
            .collect())
    }

    /// Build a table's row-group source (materialized / native / stock).
    /// Cheap plan-time row-count hint for a table (parquet footer; 0 for
    /// derived inputs and unreadable files — "unknown, don't prefer").
    fn table_rows_hint(&self, t: usize) -> u64 {
        match &self.q.tables[t].source {
            TableSource::Parquet(path) => ParquetFile::open(path)
                .ok()
                .and_then(|f| {
                    f.metadata()
                        .ok()
                        .map(|md| md.row_groups.iter().map(|rg| rg.num_rows as u64).sum())
                })
                .unwrap_or(0),
            TableSource::Derived(_) => 0,
        }
    }

    fn table_src(&self, t: usize) -> Result<RootSrc, String> {
        let q = self.q;
        let ti = &q.tables[t];
        Ok(match &ti.source {
            TableSource::Derived(i) => {
                // Memoized by Arc pointer: every reference to one CTE shares
                // an Arc, so it materializes once per top-level execute. A
                // derived referenced only ONCE (strong_count 1 — its Arc
                // lives nowhere else) skips the memo entirely: caching it
                // would pin the fat row vector until query end for nothing.
                let shared = std::sync::Arc::strong_count(&q.derived[*i]) > 1;
                let key = std::sync::Arc::as_ptr(&q.derived[*i]) as usize;
                let cached = if shared {
                    self.memo.0.lock().expect("memo lock").get(&key).cloned()
                } else {
                    None
                };
                let r = match cached {
                    Some(r) => {
                        if std::env::var("EMAT_TRACE_DERIVED").is_ok() {
                            eprintln!("derived '{}': memo hit ({} rows)", ti.name, result_len(&r));
                        }
                        r
                    }
                    None => {
                        let t0 = std::time::Instant::now();
                        let r = std::sync::Arc::new(execute_mode(&q.derived[*i], self.memo, true)?);
                        if std::env::var("EMAT_TRACE_DERIVED").is_ok() {
                            eprintln!(
                                "derived '{}': {} rows in {:?}",
                                ti.name,
                                result_len(&r),
                                t0.elapsed()
                            );
                        }
                        if shared {
                            self.memo
                                .0
                                .lock()
                                .expect("memo lock")
                                .insert(key, r.clone());
                        }
                        r
                    }
                };
                RootSrc::One(result_to_chunks(&r, ti)?)
            }
            TableSource::Parquet(path) => {
                // The native ematix-parquet path handles the required
                // numeric fast case (the TPC-H hot scans); strings,
                // `optional` columns (validity), and INT-backed decimals
                // route through the def-level-aware stock reader.
                let needs_stock = ti.projection.iter().any(|c| {
                    matches!(c.ty, LogicalType::Utf8) || c.dec_scale.is_some() || c.nullable
                });
                if needs_stock {
                    let cols: Vec<(String, ColKind)> = ti
                        .projection
                        .iter()
                        .map(|c| {
                            let kind = match (c.dec_scale, c.ty) {
                                // The reader resolves the INT32/INT64
                                // backing width from the footer itself.
                                (Some(s), _) => ColKind::Dec(s),
                                (None, LogicalType::Utf8) => ColKind::Utf8,
                                (None, LogicalType::Int64) => ColKind::I64,
                                (None, LogicalType::Float64) => ColKind::F64,
                                (None, LogicalType::Int32 | LogicalType::Date32) => {
                                    ColKind::I32(c.ty)
                                }
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

    fn slot_ty(&self, s: usize) -> LogicalType {
        let Slot { table, col } = self.q.slots[s];
        self.q.tables[table].projection[col].ty
    }

    fn local_to_slot(&self, t: usize, col: usize) -> usize {
        self.q
            .slots
            .iter()
            .position(|s| s.table == t && s.col == col)
            .expect("every join key has a slot")
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
        // `vlen` is the live column length. It equals the row-group size
        // until a fan-out child materializes an EXPANDED view (one output
        // row per matched dim row) — after which keys read straight from
        // the view's own (root) slot columns, uniformly.
        let mut vlen = chunk.n_rows();
        let mut kbuf: Vec<i64> = Vec::with_capacity(8);
        // Which slots hold real data so far: the root's own columns now,
        // each child's payloads as it attaches. Drives the fan-out's early
        // residual filter (only conjuncts over available columns apply).
        let mut avail: Vec<bool> = q.slots.iter().map(|s| s.table == ctx.root).collect();
        for (child, links, left) in ctx.children {
            let dim = ctx.dims[*child].as_ref().expect("dim built");
            // Root-side keys read from the view's slot columns (survives
            // an earlier fan-out's rematerialization).
            let key_slots: Vec<usize> = links
                .iter()
                .map(|&(p, _)| self.local_to_slot(ctx.root, p))
                .collect();
            let mut key_cols: Vec<KeyCol> = key_slots
                .iter()
                .map(|&s| key_col(&view, s, &self.interner))
                .collect();
            // Widened keys: the root-side columns of promoted equalities
            // (payloads of children attached earlier — ordering enforced
            // at planning) extend the probe key in widen-list order,
            // mirroring the dim build.
            if let Some(w) = ctx.widen.get(child) {
                // A widened KEYLESS child: mirror the build side's
                // constant-0 base key before the extras.
                if key_slots.is_empty() {
                    key_cols.push(KeyCol::Const(0));
                }
                for &(root_slot, _) in w {
                    key_cols.push(key_col(&view, root_slot, &self.interner));
                }
            }

            if dim.multi {
                if *left {
                    return Err("LEFT duplicate-key payload join is not yet supported".into());
                }
                // Early residual: the post-join conjuncts evaluable over
                // {already-attached columns} ∪ {this dim's payloads} filter
                // DURING expansion, so only survivors materialize — the
                // difference between q72's ~1B-row blowup and a small
                // result. (The full post predicate still runs afterwards —
                // re-applying these conjuncts is idempotent.)
                let residual: Option<Expr> = ctx.post.as_ref().and_then(|p| {
                    let mut cs: Vec<&Expr> = Vec::new();
                    split_and_expr(p, &mut cs);
                    cs.into_iter()
                        .filter(|c| {
                            let mut sl = Vec::new();
                            collect_slots(c, &mut sl);
                            sl.iter()
                                .all(|&s| avail[s] || dim.payload_slots.contains(&s))
                        })
                        .cloned()
                        .reduce(|l, r| Expr::Binary {
                            op: crate::expr::BinaryOp::And,
                            lhs: Box::new(l),
                            rhs: Box::new(r),
                        })
                });
                let (nv, ns, nl) = self.fanout_child(
                    &view,
                    &sel,
                    vlen,
                    dim,
                    &key_cols,
                    &mut kbuf,
                    residual.as_ref(),
                );
                view = nv;
                sel = ns;
                vlen = nl;
                for &s in &dim.payload_slots {
                    avail[s] = true;
                }
                continue;
            }

            let has_pay = !dim.payload_slots.is_empty();
            // Narrow with multiplicity; a LEFT child keeps misses once. A
            // hit's (shard, payload row) is recorded during the SAME probe
            // — the attach below gathers without re-probing the map.
            let mut out = Vec::new();
            let mut refs: Vec<u64> = if has_pay {
                vec![NO_REF; vlen]
            } else {
                Vec::new()
            };
            let mut matched: Vec<i64> = if *left { vec![0; vlen] } else { Vec::new() };
            sel.for_each(|i| {
                let iu = i as usize;
                // A NULL root key is a guaranteed miss (kept once by a
                // LEFT child, dropped by an inner one).
                let hit = fill_key(&mut kbuf, &key_cols, iu)
                    .then(|| dim.map.get(&kbuf))
                    .flatten();
                match hit {
                    Some((cnt, s, row)) => {
                        for _ in 0..cnt {
                            out.push(i);
                        }
                        if has_pay {
                            refs[iu] = ((s as u64) << 32) | row as u64;
                        }
                        if *left {
                            matched[iu] = 1;
                        }
                    }
                    None if *left => out.push(i),
                    None => {}
                }
            });
            sel = Selection::Indices(out);
            // Attach the subtree's payload slots as full-length typed
            // columns gathered straight from the shards' columnar stores
            // (LEFT misses keep NO_REF → type defaults — 0 / 0.0 / "").
            for (j, &slot) in dim.payload_slots.iter().enumerate() {
                view.cols[slot] = gather_payload(&dim.map, j, &refs, self.slot_ty(slot));
                avail[slot] = true;
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
            // No-GROUP-BY window query: gather this RG's surviving rows into
            // a slot-indexed chunk. The window stage runs over the whole
            // concatenation, so we cannot emit projected rows per RG here.
            if !q.windows.is_empty() {
                let mut idx: Vec<usize> = Vec::new();
                sel.for_each(|i| idx.push(i as usize));
                let cols: Vec<Vector> = view.cols.iter().map(|c| gather_rows(c, &idx)).collect();
                return Ok(RgOut::Chunk(DataChunk::new(cols)));
            }
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
        // Vectorized agg-argument columns: a COMPOUND numeric arg (an
        // arithmetic tree) evaluates once as a whole typed column here
        // instead of walking the recursive interpreter per row inside the
        // aggregation loop (q4's `sum(((a-b-c)+d)/2)` over 3.26M rows).
        // Only the f64-consuming aggs, only when the arg vectorizes
        // (`eval_num_col` = Some — bare columns and non-arith stay on the
        // cheap per-row path), and only when the selection is DENSE (a
        // full-column pass would waste work on a sparse post-filter
        // selection; the selective scalar aggregates — Q6 — keep the
        // exact per-row path, preserving the bit-equality gate).
        // EMAT_NO_VEC_AGG forces the per-row path (A/B escape hatch,
        // like EMAT_NO_WIDEN).
        let n_view = view.n_rows();
        let dense = sel.len() * 2 >= n_view && std::env::var("EMAT_NO_VEC_AGG").is_err();
        let arg_cols: Vec<Option<Vector>> = q
            .aggs
            .iter()
            .map(|a| {
                if dense
                    && matches!(
                        a.func,
                        AggFunc::Sum
                            | AggFunc::Min
                            | AggFunc::Max
                            | AggFunc::Avg
                            | AggFunc::StddevSamp
                    )
                {
                    eval_num_col(&view, &a.arg)
                } else {
                    None
                }
            })
            .collect();
        if q.group.is_empty() {
            let mut states = vec![AggState::default(); q.aggs.len()];
            for (j, agg) in q.aggs.iter().enumerate() {
                match agg.func {
                    AggFunc::Sum => {
                        // `count` doubles as the SUM's non-NULL tally so
                        // finalize can distinguish 0.0 from an all-NULL
                        // (SQL-NULL) sum; the summation order is unchanged
                        // (the Q6 bit-equality gate depends on it).
                        if let Some(col) = &arg_cols[j] {
                            sel.for_each(|i| {
                                if let Some(x) = col_opt_f64(col, i as usize) {
                                    states[j].sum += x;
                                    states[j].count += 1;
                                }
                            });
                        } else {
                            states[j].sum += sum_expr_f64(&view, &sel, &agg.arg);
                            sel.for_each(|i| {
                                states[j].count +=
                                    u64::from(!agg.arg.eval_is_null(&view, i as usize));
                            });
                        }
                    }
                    // count(*) counts rows; count(<expr>) skips NULLs.
                    AggFunc::Count if matches!(agg.arg, Expr::Literal(_)) => {
                        states[j].count += sel.len() as u64;
                    }
                    AggFunc::Count => sel.for_each(|i| {
                        states[j].count += u64::from(!agg.arg.eval_is_null(&view, i as usize));
                    }),
                    AggFunc::CountMatched(t) => {
                        let flags = view.col(ctx.matched_cols[&t]).as_i64();
                        sel.for_each(|i| states[j].count += flags[i as usize] as u64);
                    }
                    AggFunc::CountDistinct => sel.for_each(|i| {
                        if let Some(v) = agg.arg.eval_opt_distinct_key(&view, i as usize) {
                            states[j].distinct.insert(v);
                        }
                    }),
                    _ => {
                        if let Some(col) = &arg_cols[j] {
                            sel.for_each(|i| {
                                if let Some(x) = col_opt_f64(col, i as usize) {
                                    states[j].update(x);
                                }
                            });
                        } else {
                            sel.for_each(|i| {
                                if let Some(v) = agg.arg.eval_opt_f64(&view, i as usize) {
                                    states[j].update(v);
                                }
                            });
                        }
                    }
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
                    AggFunc::Count if matches!(agg.arg, Expr::Literal(_)) => states[j].count += 1,
                    AggFunc::Count => {
                        states[j].count += u64::from(!agg.arg.eval_is_null(&view, i));
                    }
                    AggFunc::CountMatched(t) => {
                        states[j].count += view.col(ctx.matched_cols[&t]).as_i64()[i] as u64;
                    }
                    AggFunc::CountDistinct => {
                        if let Some(v) = agg.arg.eval_opt_distinct_key(&view, i) {
                            states[j].distinct.insert(v);
                        }
                    }
                    _ => {
                        let v = match &arg_cols[j] {
                            Some(col) => col_opt_f64(col, i),
                            None => agg.arg.eval_opt_f64(&view, i),
                        };
                        if let Some(v) = v {
                            states[j].update(v);
                        }
                    }
                }
            }
        });
        Ok(RgOut::Grouped(groups))
    }

    /// Fan a matched-set out through a duplicate-key payload dim: each live
    /// row expands to one output row per dim row sharing its key (walking
    /// the shard chain), and the whole view REMATERIALIZES to the expanded
    /// length — existing columns gathered by the source root row, this
    /// dim's payload columns gathered by each output row's own dim row.
    /// Returns the new (view, all-selected, length).
    #[allow(clippy::too_many_arguments)]
    fn fanout_child(
        &self,
        view: &DataChunk,
        sel: &Selection,
        vlen: usize,
        dim: &DimResult,
        key_cols: &[KeyCol],
        kbuf: &mut Vec<i64>,
        residual: Option<&Expr>,
    ) -> (DataChunk, Selection, usize) {
        // Candidate expansions are checked against `residual` in BOUNDED
        // BATCHES: only the residual's own columns materialize per batch,
        // and only surviving (source row, dim row) pairs are kept — the
        // full-width view materializes once, survivors only. Without this
        // a low-selectivity key (q72's cs⋈inventory on item_sk) inflates
        // the intermediate ~660× before the taming predicates ever run.
        const BATCH: usize = 1 << 18;
        // Per output row: the source view row it came from, and the dim
        // payload row it carries (`NO_REF` is unused here — inner only).
        let mut rows: Vec<usize> = Vec::new();
        let mut pay_ref: Vec<u64> = Vec::new();
        let mut keep_rows: Vec<usize> = Vec::new();
        let mut keep_ref: Vec<u64> = Vec::new();
        let mut src_rows: Vec<usize> = Vec::with_capacity(sel.len());
        sel.for_each(|i| src_rows.push(i as usize));
        for &iu in &src_rows {
            let hit = fill_key(kbuf, key_cols, iu)
                .then(|| dim.map.get(kbuf))
                .flatten();
            let Some((count, s, head)) = hit else {
                continue;
            };
            let enc = |r: u32| ((s as u64) << 32) | r as u64;
            match dim.map.chain_of(s as usize) {
                // A real chain (head has a successor): walk it, emitting
                // each dim row `weight` times (grandchild fan-out).
                Some(ch) if ch.next[head as usize] != NO_NEXT => {
                    let mut r = head;
                    loop {
                        for _ in 0..ch.weight[r as usize] {
                            rows.push(iu);
                            pay_ref.push(enc(r));
                        }
                        let nx = ch.next[r as usize];
                        if nx == NO_NEXT {
                            break;
                        }
                        r = nx;
                    }
                }
                // Single dim row (unique key, or a chain of length one):
                // `count` identical copies.
                _ => {
                    for _ in 0..count {
                        rows.push(iu);
                        pay_ref.push(enc(head));
                    }
                }
            }
            if let Some(res) = residual
                && rows.len() >= BATCH
            {
                self.flush_fanout_batch(
                    view,
                    vlen,
                    dim,
                    res,
                    &mut rows,
                    &mut pay_ref,
                    &mut keep_rows,
                    &mut keep_ref,
                );
            }
        }
        if let Some(res) = residual {
            self.flush_fanout_batch(
                view,
                vlen,
                dim,
                res,
                &mut rows,
                &mut pay_ref,
                &mut keep_rows,
                &mut keep_ref,
            );
            rows = keep_rows;
            pay_ref = keep_ref;
        }

        let len = rows.len();
        let cols = self.materialize_fanout(view, vlen, dim, &rows, &pay_ref, None);
        (DataChunk::new(cols), Selection::All(len), len)
    }

    /// Filter one fan-out candidate batch against the early residual: only
    /// the residual's columns materialize, survivors append to `keep_*`,
    /// and the batch buffers clear for reuse.
    #[allow(clippy::too_many_arguments)]
    fn flush_fanout_batch(
        &self,
        view: &DataChunk,
        vlen: usize,
        dim: &DimResult,
        residual: &Expr,
        rows: &mut Vec<usize>,
        pay_ref: &mut Vec<u64>,
        keep_rows: &mut Vec<usize>,
        keep_ref: &mut Vec<u64>,
    ) {
        if rows.is_empty() {
            return;
        }
        let mut need: Vec<usize> = Vec::new();
        collect_slots(residual, &mut need);
        let cols = self.materialize_fanout(view, vlen, dim, rows, pay_ref, Some(&need));
        let chunk = DataChunk {
            cols,
            sel: Selection::All(rows.len()),
        };
        let survivors = filter_expr(&chunk, residual);
        survivors.for_each(|i| {
            keep_rows.push(rows[i as usize]);
            keep_ref.push(pay_ref[i as usize]);
        });
        rows.clear();
        pay_ref.clear();
    }

    /// Materialize the expanded fan-out view for `(rows, pay_ref)` pairs:
    /// this dim's payload columns gather by dim row, populated columns by
    /// source row, placeholders stay empty. `only` restricts to the listed
    /// slots (the batch-filter path) — everything else stays a placeholder.
    fn materialize_fanout(
        &self,
        view: &DataChunk,
        vlen: usize,
        dim: &DimResult,
        rows: &[usize],
        pay_ref: &[u64],
        only: Option<&[usize]>,
    ) -> Vec<Vector> {
        view.cols
            .iter()
            .enumerate()
            .map(|(c, col)| {
                if only.is_some_and(|o| !o.contains(&c)) {
                    return Vector::i64(Vec::new());
                }
                if let Some(j) = dim.payload_slots.iter().position(|&s| s == c) {
                    // This dim's payload column: gather by each output
                    // row's own dim row.
                    gather_payload(&dim.map, j, pay_ref, self.slot_ty(c))
                } else if col.len() == vlen {
                    // A populated column (root data, an earlier payload, a
                    // matched flag): gather by the source root row.
                    gather_rows(col, rows)
                } else {
                    // A not-yet-attached placeholder — stays empty until
                    // its own child attaches it full-length.
                    Vector::i64(Vec::new())
                }
            })
            .collect()
    }

    /// Merged groups → the row-space chunk → HAVING → output projection.
    #[allow(clippy::type_complexity)]
    fn finalize_groups(
        &self,
        mut groups: GroupsVec,
    ) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>, Option<Vec<DataChunk>>), String> {
        let q = self.q;
        let nkeys = q.group.len();
        // ---- A HAVING that references only AGGREGATE slots pre-filters
        // the groups BEFORE the key columns materialize: evaluating it
        // needs just the (numeric) agg columns, so the expensive key
        // materialization — string clones per group — runs only for
        // survivors. q23a's frequent_ss_items at sf10: 13.8M groups,
        // HAVING count(*) > 4 keeps 4,202.
        if let Some(h) = &q.having {
            let mut only_aggs = true;
            h.for_each_col(&mut |c| only_aggs &= c >= nkeys && c < nkeys + q.aggs.len());
            if only_aggs && groups.len() > (1 << 16) {
                // Probe chunk: empty placeholders where the keys would sit
                // (never read — the slot analysis above guarantees it).
                let mut pcols: Vec<Vector> = (0..nkeys).map(|_| Vector::i64(Vec::new())).collect();
                pcols.extend(agg_columns(&q.aggs, &groups));
                let probe = DataChunk::new(pcols);
                let keep: Vec<bool> = (0..groups.len()).map(|r| h.eval_bool(&probe, r)).collect();
                // Consume-and-split rather than retain(): into_iter keeps
                // the probe's sorted order (so `keep` zips positionally),
                // survivors bulk-rebuild, and the rejected majority — often
                // millions of string keys — deallocates on a detached
                // thread, off the query path.
                let mut live = Vec::new();
                let mut dead = Vec::with_capacity(groups.len());
                for (pair, f) in std::mem::take(&mut groups).into_iter().zip(keep) {
                    if f {
                        live.push(pair);
                    } else {
                        dead.push(pair);
                    }
                }
                groups = live;
                std::thread::spawn(move || drop(dead));
                // The main HAVING pass below re-evaluates over survivors —
                // idempotent, and cheap at survivor count.
            }
        }
        // ---- Build the row-space chunk [keys…, agg values…] with typed
        // key columns (Int / Float / Utf8, from the key values themselves).
        // GROUPING flags: one 0/1 column per key (1 = this key is a ROLLUP
        // subtotal, i.e. aggregated away), appended after the aggs so row
        // space is [keys…, aggs…, grouping flags…] — the positions the
        // binder's GROUPING references were remapped to.
        // Columns are independent reads of the shared groups vec, so large
        // group counts build them in PARALLEL (q67 sf10: 3.4s serial
        // finalize was mostly 8 sequential key-column walks over 5.8M
        // groups).
        let ngroups = groups.len();
        let nag = q.aggs.len();
        let nflag = if q.has_grouping { nkeys } else { 0 };
        let ncols = nkeys + nag + nflag;
        let build_col = |c: usize| -> Vector {
            if c < nkeys {
                build_key_column(groups.iter().map(|(key, _)| &key[c]), ngroups)
            } else if c < nkeys + nag {
                agg_column(&q.aggs[c - nkeys], c - nkeys, &groups)
            } else {
                let k = c - nkeys - nag;
                Vector::i64(
                    groups
                        .iter()
                        .map(|(key, _)| i64::from(matches!(key[k], GroupKey::Rollup)))
                        .collect(),
                )
            }
        };
        let cols: Vec<Vector> = if ngroups > (1 << 16) && ncols > 1 {
            let mut out: Vec<Option<Vector>> = (0..ncols).map(|_| None).collect();
            std::thread::scope(|scope| {
                for (c, slot) in out.iter_mut().enumerate() {
                    let build_col = &build_col;
                    scope.spawn(move || *slot = Some(build_col(c)));
                }
            });
            out.into_iter().map(|v| v.expect("column built")).collect()
        } else {
            (0..ncols).map(build_col).collect()
        };
        let row_chunk = DataChunk::new(cols);
        // The groups vec is fully materialized into columns — free its
        // millions of heap keys (Vec + Arc<str> each) on a detached
        // thread, off the query path (q67 sf10: ~1.5s of drop glue).
        if ngroups > (1 << 16) {
            std::thread::spawn(move || drop(groups));
        }

        // HAVING filters groups; the output projection runs per survivor.
        let keep: Vec<usize> = (0..ngroups)
            .filter(|&r| match &q.having {
                None => true,
                Some(h) => h.eval_bool(&row_chunk, r),
            })
            .collect();
        let columns: Vec<String> = q.output.iter().map(|o| o.name.clone()).collect();
        // COLUMNAR hand-off (derived consumers): outputs that are plain
        // columns / literals — no windows, ordering, limit, distinct or
        // hidden outputs — project by selecting VECTORS from the row-space
        // chunk (an Arc clone when HAVING kept everything) instead of
        // evaluating 3M+ ScalarValue cells. Bounded chunks keep downstream
        // per-row-group parallelism.
        if self.columnar
            && q.windows.is_empty()
            && q.order_by.is_empty()
            && q.limit.is_none()
            && !q.distinct
            && q.hidden_outputs == 0
            && q.output.iter().all(|o| match &o.expr {
                Expr::Column(_) => true,
                Expr::Literal(v) => matches!(
                    v,
                    ScalarValue::Int64(_)
                        | ScalarValue::Int32(_)
                        | ScalarValue::Date32(_)
                        | ScalarValue::Boolean(_)
                        | ScalarValue::Float64(_)
                        | ScalarValue::Utf8(_)
                ),
                _ => false,
            })
        {
            const CHUNK: usize = 1 << 21;
            let all_kept = keep.len() == ngroups;
            let project = |batch: &[usize]| -> DataChunk {
                let out_cols: Vec<Vector> = q
                    .output
                    .iter()
                    .map(|o| match &o.expr {
                        Expr::Column(i) if all_kept && keep.len() <= CHUNK => {
                            row_chunk.cols[*i].clone()
                        }
                        Expr::Column(i) => gather_rows(&row_chunk.cols[*i], batch),
                        Expr::Literal(v) => literal_column(v, batch.len()),
                        _ => unreachable!("qualified above"),
                    })
                    .collect();
                DataChunk::new(out_cols)
            };
            // Chunk projections are independent gathers — run in parallel.
            let batches: Vec<&[usize]> = keep.chunks(CHUNK.max(1)).collect();
            let mut chunks: Vec<Option<DataChunk>> =
                (0..batches.len().max(1)).map(|_| None).collect();
            if batches.len() > 1 {
                std::thread::scope(|scope| {
                    for (batch, slot) in batches.iter().zip(chunks.iter_mut()) {
                        let project = &project;
                        scope.spawn(move || *slot = Some(project(batch)));
                    }
                });
            } else {
                chunks[0] = Some(project(batches.first().copied().unwrap_or(&[])));
            }
            let chunks: Vec<DataChunk> = chunks
                .into_iter()
                .map(|c| c.expect("chunk built"))
                .collect();
            return Ok((columns, Vec::new(), Some(chunks)));
        }
        // Window stage: compact the surviving groups into a dense chunk,
        // append one computed column per window expression (row space
        // extends to [keys…, aggs…, windows…]), then project.
        if !q.windows.is_empty() {
            let mut cols: Vec<Vector> = row_chunk
                .cols
                .iter()
                .map(|c| gather_rows(c, &keep))
                .collect();
            let n = keep.len();
            let base = DataChunk::new(cols.clone());
            for w in &q.windows {
                cols.push(compute_window(w, &base, n));
            }
            let full = DataChunk::new(cols);
            let rows = (0..n)
                .map(|r| {
                    q.output
                        .iter()
                        .map(|o| o.expr.eval_value(&full, r))
                        .collect()
                })
                .collect();
            return Ok((columns, rows, None));
        }
        let rows = keep
            .into_iter()
            .map(|r| {
                q.output
                    .iter()
                    .map(|o| o.expr.eval_value(&row_chunk, r))
                    .collect()
            })
            .collect();
        Ok((columns, rows, None))
    }
}

/// A length-`n` constant column for a literal output (the columnar
/// hand-off's `'s' sale_type`-style projections).
fn literal_column(v: &ScalarValue, n: usize) -> Vector {
    match v {
        ScalarValue::Utf8(s) => {
            let bytes = s.as_bytes();
            let mut offsets = Vec::with_capacity(n + 1);
            let mut data = Vec::with_capacity(bytes.len() * n);
            offsets.push(0u32);
            for _ in 0..n {
                data.extend_from_slice(bytes);
                offsets.push(data.len() as u32);
            }
            Vector::utf8(offsets, data)
        }
        ScalarValue::Float64(f) => Vector::f64(vec![*f; n]),
        ScalarValue::Int64(i) => Vector::i64(vec![*i; n]),
        ScalarValue::Int32(i) => Vector::i64(vec![*i as i64; n]),
        ScalarValue::Date32(d) => Vector::i64(vec![*d as i64; n]),
        ScalarValue::Boolean(b) => Vector::i64(vec![i64::from(*b); n]),
        other => unreachable!("literal_column on unqualified literal {other:?}"),
    }
}

/// Materialize rows from a chunked result — the set-operation downgrade
/// path when one UNION side qualified for the columnar hand-off and a
/// partner (or the operation itself) did not.
fn rows_from_chunks(chunks: &[DataChunk]) -> Vec<Vec<ScalarValue>> {
    let mut rows = Vec::with_capacity(chunks.iter().map(DataChunk::n_rows).sum());
    for c in chunks {
        let exprs: Vec<Expr> = (0..c.cols.len()).map(Expr::Column).collect();
        for r in 0..c.n_rows() {
            rows.push(exprs.iter().map(|e| e.eval_value(c, r)).collect());
        }
    }
    rows
}

/// The typed aggregate-value columns of the merged groups, in agg order —
/// SUM/MIN/MAX/AVG/STDDEV over zero contributing (non-NULL) values is SQL
/// NULL, not 0.0, so those carry validity.
fn agg_columns(aggs: &[AggExpr], groups: &[(Vec<GroupKey>, Vec<AggState>)]) -> Vec<Vector> {
    aggs.iter()
        .enumerate()
        .map(|(j, agg)| agg_column(agg, j, groups))
        .collect()
}

/// One typed aggregate-value column (agg position `j`) of the merged
/// groups.
fn agg_column(agg: &AggExpr, j: usize, groups: &[(Vec<GroupKey>, Vec<AggState>)]) -> Vector {
    let states = || groups.iter().map(|(_, st)| &st[j]);
    match agg.func {
        AggFunc::Count | AggFunc::CountMatched(_) => {
            Vector::i64(states().map(|st| st.count as i64).collect())
        }
        AggFunc::CountDistinct => {
            Vector::i64(states().map(|st| st.distinct.len() as i64).collect())
        }
        _ => {
            let vals: Vec<f64> = states().map(|st| st.finalize(agg.func)).collect();
            let valid: Vec<bool> = states().map(|st| !st.is_null(agg.func)).collect();
            let any_null = valid.iter().any(|&b| !b);
            Vector::f64(vals).with_validity(any_null.then_some(valid))
        }
    }
}

/// Gather rows `idx` of a column into a new dense vector (typed, validity
/// preserved).
fn gather_rows(v: &Vector, idx: &[usize]) -> Vector {
    let valid = v
        .validity
        .as_ref()
        .map(|m| idx.iter().map(|&r| m[r]).collect::<Vec<bool>>());
    let out = match v.logical {
        LogicalType::Float64 => Vector::f64(idx.iter().map(|&r| v.as_f64()[r]).collect()),
        LogicalType::Utf8 => {
            let view = v.as_utf8();
            let mut offsets = Vec::with_capacity(idx.len() + 1);
            let mut data = Vec::new();
            offsets.push(0u32);
            for &r in idx {
                data.extend_from_slice(view.get(r).as_bytes());
                offsets.push(data.len() as u32);
            }
            Vector::utf8(offsets, data)
        }
        LogicalType::Int32 | LogicalType::Date32 => {
            Vector::i32(idx.iter().map(|&r| v.as_i32()[r]).collect(), v.logical)
        }
        LogicalType::Int64 => Vector::i64(idx.iter().map(|&r| v.as_i64()[r]).collect()),
    };
    out.with_validity(valid.filter(|m| m.iter().any(|&b| !b)))
}

/// Concatenate row-group chunks (identical column layout) into one dense
/// chunk — the single input a no-GROUP-BY window stage runs over. Validity
/// is merged (a column with no mask contributes all-valid rows).
fn concat_chunks(chunks: Vec<DataChunk>) -> DataChunk {
    let Some(first) = chunks.first() else {
        return DataChunk::new(Vec::new());
    };
    let ncols = first.cols.len();
    let total: usize = chunks.iter().map(DataChunk::n_rows).sum();
    let mut cols = Vec::with_capacity(ncols);
    for j in 0..ncols {
        let logical = first.cols[j].logical;
        let any_valid = chunks.iter().any(|c| c.cols[j].validity.is_some());
        let mut valid: Option<Vec<bool>> = any_valid.then(|| Vec::with_capacity(total));
        if let Some(v) = &mut valid {
            for c in &chunks {
                let col = &c.cols[j];
                match &col.validity {
                    Some(m) => v.extend_from_slice(m),
                    None => v.extend(std::iter::repeat_n(true, col.len())),
                }
            }
        }
        let out = match logical {
            LogicalType::Float64 => {
                let mut o = Vec::with_capacity(total);
                for c in &chunks {
                    o.extend_from_slice(c.cols[j].as_f64());
                }
                Vector::f64(o)
            }
            LogicalType::Int64 => {
                let mut o = Vec::with_capacity(total);
                for c in &chunks {
                    o.extend_from_slice(c.cols[j].as_i64());
                }
                Vector::i64(o)
            }
            LogicalType::Int32 | LogicalType::Date32 => {
                let mut o = Vec::with_capacity(total);
                for c in &chunks {
                    o.extend_from_slice(c.cols[j].as_i32());
                }
                Vector::i32(o, logical)
            }
            LogicalType::Utf8 => {
                let mut offsets = Vec::with_capacity(total + 1);
                let mut data = Vec::new();
                offsets.push(0u32);
                for c in &chunks {
                    let view = c.cols[j].as_utf8();
                    for r in 0..c.cols[j].len() {
                        data.extend_from_slice(view.get(r).as_bytes());
                        offsets.push(data.len() as u32);
                    }
                }
                Vector::utf8(offsets, data)
            }
        };
        cols.push(out.with_validity(valid));
    }
    DataChunk::new(cols)
}

/// Evaluate one window expression over the block's result chunk (`n`
/// rows): partition, optionally order, then aggregate / rank.
/// Row indices (in original order) surviving a rank-family top-K prune:
/// per partition, the K-th best row under the window ordering is found by
/// LINEAR selection (`select_nth_unstable_by`, no full sort) and every
/// row ordering at-or-before it is kept. This is a superset of the rows
/// with rank ≤ K — rank depends only on strictly-better rows (all kept),
/// row_number additionally on original-order tie position (gathering by
/// ascending index preserves it, and the stable sort in
/// [`compute_window`] then reproduces the same tie order) — so the
/// recomputed values match the full-input values exactly, and the outer
/// filter trims threshold ties. NOT valid for dense_rank (its rank-≤-K
/// frontier extends past the K-th best row); the binder never arms it.
fn rank_topk_keep(
    w: &crate::logical::WindowExpr,
    chunk: &DataChunk,
    n: usize,
    k: usize,
) -> Vec<usize> {
    let mut parts: BTreeMap<Vec<GroupKey>, Vec<usize>> = BTreeMap::new();
    for r in 0..n {
        let key: Vec<GroupKey> = w
            .partition
            .iter()
            .map(|e| GroupKey::from(e.eval_value(chunk, r)))
            .collect();
        parts.entry(key).or_default().push(r);
    }
    let order_cmp = |a: usize, b: usize| {
        for (e, desc) in &w.order {
            let ord = cmp_scalar(&e.eval_value(chunk, a), &e.eval_value(chunk, b));
            let ord = if *desc { ord.reverse() } else { ord };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    };
    let mut keep = Vec::new();
    for rows in parts.values_mut() {
        if rows.len() <= k {
            keep.extend_from_slice(rows);
            continue;
        }
        rows.select_nth_unstable_by(k - 1, |&a, &b| order_cmp(a, b));
        let kth = rows[k - 1];
        for &r in rows.iter() {
            if order_cmp(r, kth) != std::cmp::Ordering::Greater {
                keep.push(r);
            }
        }
    }
    keep.sort_unstable();
    keep
}

fn compute_window(w: &crate::logical::WindowExpr, chunk: &DataChunk, n: usize) -> Vector {
    use crate::logical::WindowFunc;
    // Partition → member row indices, in row order (deterministic: the
    // grouped rows come out of a BTreeMap).
    let mut parts: BTreeMap<Vec<GroupKey>, Vec<usize>> = BTreeMap::new();
    for r in 0..n {
        let key: Vec<GroupKey> = w
            .partition
            .iter()
            .map(|e| GroupKey::from(e.eval_value(chunk, r)))
            .collect();
        parts.entry(key).or_default().push(r);
    }
    let order_cmp = |a: usize, b: usize| {
        for (e, desc) in &w.order {
            let ord = cmp_scalar(&e.eval_value(chunk, a), &e.eval_value(chunk, b));
            let ord = if *desc { ord.reverse() } else { ord };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    };
    match w.func {
        WindowFunc::Agg(af) => {
            // A frame with zero non-NULL inputs is SQL NULL (q51's
            // `max(store_sales) OVER …` over a FULL-OUTER NULL run), so the
            // finalized value carries validity, like the grouped path.
            let mut out = vec![0.0f64; n];
            let mut valid = vec![true; n];
            for rows in parts.values_mut() {
                if w.order.is_empty() {
                    // Whole-partition aggregate.
                    let mut st = AggState::default();
                    for &r in rows.iter() {
                        if let Some(v) = w.arg.eval_opt_f64(chunk, r) {
                            st.update(v);
                        }
                    }
                    let v = st.finalize(af);
                    let ok = !st.is_null(af);
                    for &r in rows.iter() {
                        out[r] = v;
                        valid[r] = ok;
                    }
                } else {
                    rows.sort_by(|&a, &b| order_cmp(a, b));
                    let mut st = AggState::default();
                    if w.rows_frame {
                        // ROWS …CURRENT ROW: strict running value.
                        for &r in rows.iter() {
                            if let Some(v) = w.arg.eval_opt_f64(chunk, r) {
                                st.update(v);
                            }
                            out[r] = st.finalize(af);
                            valid[r] = !st.is_null(af);
                        }
                    } else {
                        // RANGE (the ordered default): peers of the
                        // current row are included in its frame.
                        let mut i = 0;
                        while i < rows.len() {
                            let mut j = i + 1;
                            while j < rows.len()
                                && order_cmp(rows[i], rows[j]) == std::cmp::Ordering::Equal
                            {
                                j += 1;
                            }
                            for &r in &rows[i..j] {
                                if let Some(v) = w.arg.eval_opt_f64(chunk, r) {
                                    st.update(v);
                                }
                            }
                            let v = st.finalize(af);
                            let ok = !st.is_null(af);
                            for &r in &rows[i..j] {
                                out[r] = v;
                                valid[r] = ok;
                            }
                            i = j;
                        }
                    }
                }
            }
            let any_null = valid.iter().any(|&b| !b);
            Vector::f64(out).with_validity(any_null.then_some(valid))
        }
        WindowFunc::Rank | WindowFunc::DenseRank | WindowFunc::RowNumber => {
            let mut out = vec![0i64; n];
            for rows in parts.values_mut() {
                rows.sort_by(|&a, &b| order_cmp(a, b));
                let mut dense = 0i64;
                let mut i = 0;
                while i < rows.len() {
                    let mut j = i + 1;
                    while j < rows.len() && order_cmp(rows[i], rows[j]) == std::cmp::Ordering::Equal
                    {
                        j += 1;
                    }
                    dense += 1;
                    for (k, &r) in rows[i..j].iter().enumerate() {
                        out[r] = match w.func {
                            WindowFunc::Rank => (i + 1) as i64,
                            WindowFunc::DenseRank => dense,
                            WindowFunc::RowNumber => (i + k + 1) as i64,
                            WindowFunc::Agg(_) => unreachable!(),
                        };
                    }
                    i = j;
                }
            }
            Vector::i64(out)
        }
    }
}

/// A typed group-key value with a total order (per-row-group BTreeMap
/// partials + sorted-run merge ⇒ deterministic key-sorted output, with
/// ROLLUP levels appended after the base). Floats order by `total_cmp`
/// and group by bit pattern — exact, NaN-safe.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum GroupKey {
    /// SQL NULL groups with itself and sorts first (declared first so the
    /// derived order puts NULL groups ahead — NULLS FIRST ascending).
    Null,
    /// A ROLLUP subtotal placeholder for a rolled-up column. Renders as SQL
    /// NULL like [`GroupKey::Null`] but is a DISTINCT map key, so a subtotal
    /// row (`[a, Rollup]`) never merges with a genuine NULL group
    /// (`[a, Null]`) that shares the same display.
    Rollup,
    Int(i64),
    Float(FOrd),
    Str(std::sync::Arc<str>),
}

/// Expand base groups (keyed by all `ROLLUP(t₁, …)` term columns) into the
/// coarser grouping sets: drop the last term, the last two, …, down to the
/// grand total. Each coarser set re-aggregates the base groups that share
/// its surviving prefix (SUM/COUNT/MIN/MAX/AVG/… all merge exactly), with
/// the dropped columns keyed `GroupKey::Rollup` (renders NULL, but distinct
/// from a genuine NULL group so the subtotal never merges into one).
/// The post-merge grouped representation: (key, states) pairs in KEY
/// ORDER. The merge produces sorted output by construction, and every
/// consumer — the ROLLUP cascade, the HAVING pre-filter, column
/// materialization — only needs sorted ITERATION, so nothing rebuilds a
/// tree (q67 sf10 spent ~40% of its merge phase re-sorting and
/// re-bulk-building a BTreeMap from already-sorted pairs).
type GroupsVec = Vec<(Vec<GroupKey>, Vec<AggState>)>;

/// Merge per-row-group aggregation partials into one sorted vec by K-WAY
/// run merge: each partial iterates in key order, a small heap of run
/// heads yields globally sorted (key, states) pairs, equal keys merge on
/// the fly. Replaces per-entry tree probing into an ever-growing map
/// (log-n multi-string key comparisons per insert — 8s of q67's sf10
/// time; the heap pass is ~log-k).
fn kway_merge_groups(
    maps: Vec<BTreeMap<Vec<GroupKey>, Vec<AggState>>>,
    nthreads: usize,
) -> GroupsVec {
    if maps.len() <= 1 {
        return maps
            .into_iter()
            .next()
            .map(|m| m.into_iter().collect())
            .unwrap_or_default();
    }
    // Large inputs merge in parallel by KEY RANGE: pivots sampled from the
    // largest run split every map (BTreeMap::split_off — O(log n), no data
    // copy) into disjoint ranges, each range k-way merges on its own
    // thread, and the concatenation is globally sorted because the ranges
    // are. q23a's 13.8M-group frequent_ss_items merge: 9s serial → the
    // slowest range.
    let total: usize = maps.iter().map(BTreeMap::len).sum();
    let nparts = nthreads.clamp(1, 32);
    if total >= 1 << 18 && nparts > 1 {
        let largest = maps
            .iter()
            .max_by_key(|m| m.len())
            .expect("maps nonempty here");
        let step = (largest.len() / nparts).max(1);
        let pivots: Vec<Vec<GroupKey>> = largest
            .keys()
            .skip(step)
            .step_by(step)
            .take(nparts - 1)
            .cloned()
            .collect();
        let mut parts: Vec<Vec<BTreeMap<Vec<GroupKey>, Vec<AggState>>>> =
            (0..=pivots.len()).map(|_| Vec::new()).collect();
        for mut m in maps {
            for (i, p) in pivots.iter().enumerate().rev() {
                let hi = m.split_off(p);
                if !hi.is_empty() {
                    parts[i + 1].push(hi);
                }
            }
            if !m.is_empty() {
                parts[0].push(m);
            }
        }
        let mut outs: Vec<GroupsVec> = (0..parts.len()).map(|_| Vec::new()).collect();
        std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for (part, out) in parts.into_iter().zip(outs.iter_mut()) {
                handles.push(scope.spawn(move || *out = kway_merge_runs(part)));
            }
            for h in handles {
                h.join().expect("merge worker panicked");
            }
        });
        // Ranges are disjoint and internally sorted — concatenation is
        // globally sorted. No tree rebuild.
        return outs.into_iter().flatten().collect();
    }
    kway_merge_runs(maps)
}

/// Serial k-way run merge: sorted maps → globally sorted, unique
/// (key, states) pairs, equal keys merged on the fly.
fn kway_merge_runs(maps: Vec<BTreeMap<Vec<GroupKey>, Vec<AggState>>>) -> GroupsVec {
    if maps.len() == 1 {
        return maps
            .into_iter()
            .next()
            .expect("len 1")
            .into_iter()
            .collect();
    }
    // Min-heap of run heads, ordered by (key, run). States ride along —
    // moved, never cloned.
    struct Head {
        key: Vec<GroupKey>,
        run: usize,
        states: Vec<AggState>,
    }
    impl PartialEq for Head {
        fn eq(&self, o: &Self) -> bool {
            self.key == o.key && self.run == o.run
        }
    }
    impl Eq for Head {}
    impl Ord for Head {
        fn cmp(&self, o: &Self) -> std::cmp::Ordering {
            // Reversed: BinaryHeap is a max-heap, we need the SMALLEST key.
            o.key.cmp(&self.key).then(o.run.cmp(&self.run))
        }
    }
    impl PartialOrd for Head {
        fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(o))
        }
    }
    let mut runs: Vec<_> = maps.into_iter().map(|m| m.into_iter()).collect();
    let mut heap = std::collections::BinaryHeap::with_capacity(runs.len());
    for (i, r) in runs.iter_mut().enumerate() {
        if let Some((key, states)) = r.next() {
            heap.push(Head {
                key,
                run: i,
                states,
            });
        }
    }
    let mut out: Vec<(Vec<GroupKey>, Vec<AggState>)> = Vec::new();
    while let Some(h) = heap.pop() {
        if let Some((key, states)) = runs[h.run].next() {
            heap.push(Head {
                key,
                run: h.run,
                states,
            });
        }
        match out.last_mut() {
            Some((lk, ls)) if *lk == h.key => {
                for (a, b) in ls.iter_mut().zip(&h.states) {
                    a.merge(b);
                }
            }
            _ => out.push((h.key, h.states)),
        }
    }
    out
}

fn add_rollup_levels(groups: &mut GroupsVec, term_sizes: &[usize]) {
    // The base grouping set (all terms) is already present; derive the rest
    // by CASCADE: level t re-aggregates level t+1 (not the base), and each
    // step is a single linear run-merge — the source iterates in key order,
    // truncating a key to its prefix preserves that order, so groups
    // sharing the kept prefix are CONTIGUOUS. q67's 8-level rollup over
    // 4.8M string-keyed base groups at sf10: 38.8M tree probes → one
    // sorted pass per level over an ever-shrinking input (16.5s → sub-s).
    let mut kept_cols = vec![0usize; term_sizes.len() + 1];
    for (i, &sz) in term_sizes.iter().enumerate() {
        kept_cols[i + 1] = kept_cols[i] + sz;
    }
    // Absorb one source group into the level being built: merge into the
    // current run if the kept prefix matches, else start a new subtotal.
    fn absorb(
        level: &mut Vec<(Vec<GroupKey>, Vec<AggState>)>,
        keep: usize,
        k: &[GroupKey],
        states: &[AggState],
    ) {
        if let Some((lk, ls)) = level.last_mut() {
            if lk[..keep] == k[..keep] {
                for (a, b) in ls.iter_mut().zip(states) {
                    a.merge(b);
                }
                return;
            }
        }
        let mut key = k.to_vec();
        for slot in key.iter_mut().skip(keep) {
            *slot = GroupKey::Rollup;
        }
        level.push((key, states.to_vec()));
    }
    let mut levels: Vec<GroupsVec> = Vec::new();
    for t in (0..term_sizes.len()).rev() {
        let keep = kept_cols[t];
        let mut level = Vec::new();
        match levels.last() {
            None => {
                for (k, s) in groups.iter() {
                    absorb(&mut level, keep, k, s);
                }
            }
            Some(prev) => {
                for (k, s) in prev {
                    absorb(&mut level, keep, k, s);
                }
            }
        }
        levels.push(level);
    }
    // Level keys carry a Rollup suffix the base never has, and each
    // level's suffix pattern is unique — levels APPEND after the sorted
    // base groups (finest first, grand total last), each level itself in
    // key order. Deterministic; nothing downstream needs global sort.
    for level in levels {
        groups.extend(level);
    }
}

impl GroupKey {
    /// A NULL-rendering key: a genuine SQL NULL or a ROLLUP subtotal
    /// placeholder — both emit an invalid (NULL) cell.
    fn is_null_like(&self) -> bool {
        matches!(self, GroupKey::Null | GroupKey::Rollup)
    }
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
            ScalarValue::Null => GroupKey::Null,
        }
    }
}

/// Build a typed row-space column from one group-key position across all
/// groups (every value in a position shares a type by construction).
fn build_key_column<'k>(keys: impl Iterator<Item = &'k GroupKey>, ngroups: usize) -> Vector {
    let keys: Vec<&GroupKey> = keys.collect();
    debug_assert_eq!(keys.len(), ngroups);
    // NULL keys are their own group: the column stores a type default with
    // a false validity bit; the type witness is the first non-NULL key.
    let any_null = keys.iter().any(|k| k.is_null_like());
    let valid = any_null.then(|| {
        keys.iter()
            .map(|k| !k.is_null_like())
            .collect::<Vec<bool>>()
    });
    let witness = keys.iter().find(|k| !k.is_null_like());
    let out = match witness {
        Some(GroupKey::Float(_)) => Vector::f64(
            keys.iter()
                .map(|k| match k {
                    GroupKey::Float(f) => f64::from_bits(f.0),
                    k if k.is_null_like() => 0.0,
                    other => panic!("mixed group-key types: {other:?}"),
                })
                .collect(),
        ),
        Some(GroupKey::Str(_)) => {
            let mut offsets = Vec::with_capacity(ngroups + 1);
            let mut data = Vec::new();
            offsets.push(0u32);
            for k in &keys {
                match k {
                    GroupKey::Str(s) => data.extend_from_slice(s.as_bytes()),
                    k if k.is_null_like() => {}
                    other => panic!("mixed group-key types: {other:?}"),
                }
                offsets.push(data.len() as u32);
            }
            Vector::utf8(offsets, data)
        }
        _ => Vector::i64(
            keys.iter()
                .map(|k| match k {
                    GroupKey::Int(i) => *i,
                    k if k.is_null_like() => 0,
                    other => panic!("mixed group-key types: {other:?}"),
                })
                .collect(),
        ),
    };
    out.with_validity(valid)
}

/// Sort result rows by an ORDER BY key list, **NULLS LAST** in both
/// directions (the DuckDB / Postgres default) — a NULL sorts after every
/// value regardless of ASC/DESC, so it never displaces a real row from a
/// `LIMIT`.
fn order_rows(rows: &mut [Vec<ScalarValue>], order_by: &[OrderByKey]) {
    if order_by.is_empty() {
        return;
    }
    rows.sort_by(|a, b| {
        for k in order_by {
            let (x, y) = (&a[k.output], &b[k.output]);
            let ord = match (
                matches!(x, ScalarValue::Null),
                matches!(y, ScalarValue::Null),
            ) {
                (true, true) => std::cmp::Ordering::Equal,
                (true, false) => std::cmp::Ordering::Greater,
                (false, true) => std::cmp::Ordering::Less,
                (false, false) => {
                    let o = cmp_scalar(x, y);
                    if k.desc { o.reverse() } else { o }
                }
            };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
}

/// Total order over same-typed output scalars — the ORDER BY comparator.
fn cmp_scalar(a: &ScalarValue, b: &ScalarValue) -> std::cmp::Ordering {
    use ScalarValue::*;
    match (a, b) {
        // SQL NULL sorts first ascending (NULLS FIRST — matching the
        // grouped path's BTreeMap order).
        (Null, Null) => std::cmp::Ordering::Equal,
        (Null, _) => std::cmp::Ordering::Less,
        (_, Null) => std::cmp::Ordering::Greater,
        (Int64(x), Int64(y)) => x.cmp(y),
        // Cross-type numeric compare (set-op sides may produce Int64 in
        // one block and Float64 in another for the same column).
        (Int64(x), Float64(y)) => (*x as f64).total_cmp(y),
        (Float64(x), Int64(y)) => x.total_cmp(&(*y as f64)),
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
    /// Sum of squares (stddev only).
    sumsq: f64,
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
            sumsq: 0.0,
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
        self.sumsq += o.sumsq;
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
        self.sumsq += v * v;
        self.count += 1;
        if v < self.min {
            self.min = v;
        }
        if v > self.max {
            self.max = v;
        }
    }

    /// SQL NULL result: SUM/MIN/MAX/AVG over zero non-NULL values, or a
    /// sample stddev of fewer than two. (COUNT family is never NULL.)
    fn is_null(&self, func: AggFunc) -> bool {
        match func {
            AggFunc::Sum | AggFunc::Min | AggFunc::Max | AggFunc::Avg => self.count == 0,
            AggFunc::StddevSamp => self.count < 2,
            AggFunc::Count | AggFunc::CountDistinct | AggFunc::CountMatched(_) => false,
        }
    }

    fn finalize(&self, func: AggFunc) -> f64 {
        match func {
            AggFunc::Sum => self.sum,
            AggFunc::Count => self.count as f64,
            AggFunc::Min => self.min,
            AggFunc::Max => self.max,
            AggFunc::Avg => self.sum / self.count as f64,
            // Sample variance via the sum-of-squares identity.
            AggFunc::StddevSamp => {
                let n = self.count as f64;
                ((self.sumsq - self.sum * self.sum / n) / (n - 1.0))
                    .max(0.0)
                    .sqrt()
            }
            AggFunc::CountDistinct => self.distinct.len() as f64,
            AggFunc::CountMatched(_) => self.count as f64,
        }
    }
}

/// Convert a materialized [`QueryResult`] into BOUNDED chunks shaped like
/// `ti`'s projection. Emitting several chunks (not one) lets the per-row-
/// group machinery downstream parallelize scans over big deriveds — q95's
/// 74.8M-row CTE dedup ran as ONE row group on one thread (a 10s
/// single-threaded BTreeMap build in the sample profile).
fn result_to_chunks(r: &QueryResult, ti: &TableInput) -> Result<Vec<DataChunk>, String> {
    // Columnar hand-off: select/coerce vectors per stored chunk — an Arc
    // clone per matching column, no per-cell work.
    if let Some(chunks) = &r.col_chunks {
        return chunks.iter().map(|c| chunk_project(c, ti)).collect();
    }
    const CHUNK: usize = 1 << 21;
    let n = r.rows.len();
    let mut chunks = Vec::with_capacity(n / CHUNK + 1);
    let mut lo = 0;
    loop {
        let hi = (lo + CHUNK).min(n);
        chunks.push(result_to_chunk(r, ti, lo, hi)?);
        if hi == n {
            break;
        }
        lo = hi;
    }
    Ok(chunks)
}

/// Shape one stored columnar chunk to `ti`'s projection. The producer's
/// vector universe is {i64, f64, utf8} (group keys, agg values, literal
/// columns), so coercions mirror the rows path exactly: an i64 vector
/// under a Float64 declaration widens; type clashes error just as the
/// per-cell conversion would.
fn chunk_project(chunk: &DataChunk, ti: &TableInput) -> Result<DataChunk, String> {
    let mut cols = Vec::with_capacity(ti.projection.len());
    for c in &ti.projection {
        let v = chunk
            .cols
            .get(c.leaf)
            .ok_or_else(|| format!("derived column '{}' out of range", c.name))?;
        let out = match (c.ty, v.logical) {
            (LogicalType::Utf8, LogicalType::Utf8) => v.clone(),
            (LogicalType::Utf8, other) => {
                return Err(format!(
                    "derived column '{}' expected Utf8, got {other:?}",
                    c.name
                ));
            }
            (LogicalType::Float64, LogicalType::Float64) => v.clone(),
            (LogicalType::Float64, LogicalType::Int64) => {
                let valid = v.validity.as_ref().map(|m| m.to_vec());
                Vector::f64(v.as_i64().iter().map(|&x| x as f64).collect()).with_validity(valid)
            }
            (LogicalType::Float64, other) => {
                return Err(format!(
                    "derived column '{}' expected Float64, got {other:?}",
                    c.name
                ));
            }
            (_, LogicalType::Int64) => v.clone(),
            (_, other) => {
                return Err(format!(
                    "derived column '{}' expected an integer, got {other:?}",
                    c.name
                ));
            }
        };
        cols.push(out);
    }
    Ok(DataChunk::new(cols))
}

/// Rows `lo..hi` of a materialized [`QueryResult`] as a chunk shaped like
/// `ti`'s projection (`leaf` = the result's output-column position; values
/// coerced to the declared type).
fn result_to_chunk(
    r: &QueryResult,
    ti: &TableInput,
    lo: usize,
    hi: usize,
) -> Result<DataChunk, String> {
    let nrows = hi - lo;
    let mut cols = Vec::with_capacity(ti.projection.len());
    for c in &ti.projection {
        let get = |row: usize| &r.rows[row][c.leaf];
        // NULL result values store the type default with a false validity
        // bit; an all-valid column drops the mask.
        let mut valid: Vec<bool> = Vec::with_capacity(nrows);
        let mut any_null = false;
        let v = match c.ty {
            LogicalType::Utf8 => {
                let mut offsets = Vec::with_capacity(nrows + 1);
                let mut data = Vec::new();
                offsets.push(0u32);
                for row in lo..hi {
                    let ok = match get(row) {
                        ScalarValue::Utf8(s) => {
                            data.extend_from_slice(s.as_bytes());
                            true
                        }
                        ScalarValue::Null => false,
                        other => {
                            return Err(format!(
                                "derived column '{}' expected Utf8, got {other:?}",
                                c.name
                            ));
                        }
                    };
                    offsets.push(data.len() as u32);
                    valid.push(ok);
                    any_null |= !ok;
                }
                Vector::utf8(offsets, data)
            }
            LogicalType::Float64 => {
                let mut v = Vec::with_capacity(nrows);
                for row in lo..hi {
                    let (x, ok) = match get(row) {
                        ScalarValue::Float64(x) => (*x, true),
                        ScalarValue::Int64(x) => (*x as f64, true),
                        ScalarValue::Null => (0.0, false),
                        other => {
                            return Err(format!(
                                "derived column '{}' expected Float64, got {other:?}",
                                c.name
                            ));
                        }
                    };
                    v.push(x);
                    valid.push(ok);
                    any_null |= !ok;
                }
                Vector::f64(v)
            }
            _ => {
                let mut v = Vec::with_capacity(nrows);
                for row in lo..hi {
                    let (x, ok) = match get(row) {
                        ScalarValue::Int64(x) => (*x, true),
                        ScalarValue::Int32(x) => (*x as i64, true),
                        ScalarValue::Date32(x) => (*x as i64, true),
                        ScalarValue::Null => (0, false),
                        other => {
                            return Err(format!(
                                "derived column '{}' expected an integer, got {other:?}",
                                c.name
                            ));
                        }
                    };
                    v.push(x);
                    valid.push(ok);
                    any_null |= !ok;
                }
                Vector::i64(v)
            }
        };
        cols.push(v.with_validity(any_null.then_some(valid)));
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

/// Flatten a bound `AND` tree into its conjuncts (source order).
fn split_and_expr<'e>(e: &'e Expr, out: &mut Vec<&'e Expr>) {
    if let Expr::Binary {
        op: crate::expr::BinaryOp::And,
        lhs,
        rhs,
    } = e
    {
        split_and_expr(lhs, out);
        split_and_expr(rhs, out);
    } else {
        out.push(e);
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
        Expr::ExtractYear(i) | Expr::CastInt(i) | Expr::Round { expr: i, .. } | Expr::Upper(i) => {
            collect_slots(i, out)
        }
        Expr::Like { expr, .. } => collect_slots(expr, out),
        Expr::ScalarSub(_) => {}
        Expr::InSub { expr, .. }
        | Expr::InSet { expr, .. }
        | Expr::InSetStr { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::Substr { expr, .. } => collect_slots(expr, out),
        Expr::Concat(parts) => {
            for p in parts {
                collect_slots(p, out);
            }
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
