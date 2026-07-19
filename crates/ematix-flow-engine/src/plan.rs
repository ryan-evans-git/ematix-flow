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
use crate::expr::{Expr, ScalarValue, filter_expr, sum_expr_f64};
use crate::logical::{AggFunc, BoundQuery, Slot, TableInput, TableSource};
use crate::scan::{ColKind, StockScan};
use crate::scan_native::{NativeColKind, decode_row_group};
use crate::sched::MorselQueue;
use crate::vector::{LogicalType, Utf8View, Vector};

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
    if !q.set_ops.is_empty() {
        return execute_set(q);
    }
    if q.subqueries.is_empty() {
        return Executor { q, nthreads }.run();
    }
    let mut q2 = q.clone();
    resolve_subqueries(&mut q2)?;
    Executor { q: &q2, nthreads }.run()
}

/// Execute a set-operation query: run the base block (its ORDER BY /
/// LIMIT withheld), fold each side's rows in with the set semantics, then
/// order and limit the COMBINED rows.
fn execute_set(q: &BoundQuery) -> Result<QueryResult, String> {
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

    let mut r = execute(&base)?;
    for (op, side) in &set_ops {
        let rs = execute(side)?;
        r.rows = match op {
            SetOp::UnionAll => {
                let mut rows = r.rows;
                rows.extend(rs.rows);
                rows
            }
            SetOp::Union => {
                let mut rows = r.rows;
                rows.extend(rs.rows);
                dedup(rows)
            }
            SetOp::Intersect => {
                let right = dedup(rs.rows);
                dedup(r.rows)
                    .into_iter()
                    .filter(|row| right.binary_search_by(|x| row_cmp(x, row)).is_ok())
                    .collect()
            }
            SetOp::Except => {
                let right = dedup(rs.rows);
                dedup(r.rows)
                    .into_iter()
                    .filter(|row| right.binary_search_by(|x| row_cmp(x, row)).is_err())
                    .collect()
            }
        };
    }
    if !order_by.is_empty() {
        r.rows.sort_by(|a, b| {
            for k in &order_by {
                let ord = cmp_scalar(&a[k.output], &b[k.output]);
                let ord = if k.desc { ord.reverse() } else { ord };
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        });
    }
    if let Some(l) = limit {
        r.rows.truncate(l);
    }
    Ok(r)
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
                    // A NULL element matches nothing by equality. (Strict
                    // SQL: `x NOT IN (…, NULL)` is UNKNOWN for every x —
                    // that full three-valued NOT IN is a labelled
                    // follow-on; membership tests here treat the set as
                    // its non-NULL elements.)
                    ScalarValue::Null => continue,
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
            | Expr::IsNull { expr, .. }
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
            | Expr::IsNull { expr, .. }
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

/// One shard of a dim map: key → (match weight, payload row), with the
/// shard's payload values stored COLUMNAR (`pay[j]` aligned with the
/// subtree's `payload_slots[j]`). A probe returns a row index; the attach
/// step gathers typed values directly — no per-row `ScalarValue`.
struct Shard<K> {
    map: FastMap<K, (u64, u32)>,
    pay: Vec<PayCol>,
}

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
        }
    }
}

fn key_col(chunk: &DataChunk, col: usize) -> KeyCol<'_> {
    let v = chunk.col(col);
    let valid = v.validity.as_deref();
    match v.logical {
        LogicalType::Int64 => KeyCol::I64(v.as_i64(), valid),
        LogicalType::Int32 | LogicalType::Date32 => KeyCol::I32(v.as_i32(), valid),
        other => panic!("join key must be integer-family, got {other:?}"),
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

/// A processed dim subtree: (composite) join key → (match weight,
/// columnar payload row) across shards; `payload_slots` names the attach
/// targets.
struct DimResult {
    payload_slots: Vec<usize>,
    map: DimMap,
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
        // Drop ORDER-BY-only hidden outputs now that sorting is done.
        let mut columns = columns;
        if q.hidden_outputs > 0 {
            let keep = q.output.len() - q.hidden_outputs;
            columns.truncate(keep);
            for row in &mut rows {
                row.truncate(keep);
            }
        }
        Ok(QueryResult { columns, rows })
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
            let r = self.build_dim(*child, &child_cols, children, needed)?;
            payload_slots.extend(r.payload_slots.iter().copied());
            child_results.push((links.iter().map(|&(p, _)| p).collect(), r));
        }

        let pay_tys: Vec<LogicalType> = payload_slots.iter().map(|&s| self.slot_ty(s)).collect();
        let key_len = link_cols.len();
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
                        let key_cols: Vec<KeyCol> =
                            link_cols.iter().map(|&c| key_col(&chunk, c)).collect();
                        let child_keys: Vec<Vec<KeyCol>> = cr_ref
                            .iter()
                            .map(|(pk, _)| pk.iter().map(|&c| key_col(&chunk, c)).collect())
                            .collect();
                        let own_srcs: Vec<PaySrc> = own_ref
                            .iter()
                            .map(|&s| pay_src(&chunk, q.slots[s].col))
                            .collect();
                        let mut kbuf: Vec<i64> = Vec::with_capacity(8);
                        let mut hits: Vec<(u32, u32)> = Vec::with_capacity(cr_ref.len());
                        sel.for_each(|i| {
                            let i = i as usize;
                            // Probe each child; a miss (including a NULL
                            // key) drops the row, hits multiply.
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
                            // A NULL own-key row can never be matched by
                            // any probe — skip it entirely.
                            if !fill_key(&mut kbuf, &key_cols, i) {
                                return;
                            }
                            let buf = &mut bufs[shard_of(&kbuf, nshards)];
                            buf.keys.extend_from_slice(&kbuf);
                            buf.weights.push(weight);
                            // Payload in `payload_slots` order: own first,
                            // then each child's block.
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
        // folds its buffers in row-group order, so the layout (and any
        // duplicate-key error) is deterministic at any thread count.
        let name = &q.tables[t].name;
        let map = if key_len == 1 {
            DimMap::Single(self.merge_shards(&emits, key_len, nshards, &pay_tys, name, |k| k[0])?)
        } else {
            DimMap::Multi(
                self.merge_shards(&emits, key_len, nshards, &pay_tys, name, |k| k.to_vec())?,
            )
        };
        Ok(DimResult { payload_slots, map })
    }

    /// Merge every row group's emit buffers into per-shard maps + columnar
    /// payload stores, shards in parallel. A duplicate key accumulates
    /// weight for a key-only dim and errors for a payload dim (the
    /// duplicate-key payload join is a labelled follow-on).
    fn merge_shards<K: std::hash::Hash + Eq + Send>(
        &self,
        emits: &[RgEmits],
        key_len: usize,
        nshards: usize,
        pay_tys: &[LogicalType],
        table_name: &str,
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
                        let mut shard = Shard {
                            map: FastMap::with_capacity_and_hasher(total, Default::default()),
                            pay: pay_tys.iter().map(|&ty| PayCol::new(ty)).collect(),
                        };
                        for rg in emits {
                            let buf = &rg[s];
                            let base = shard.pay.first().map_or(0, PayCol::len);
                            for (idx, key) in buf.keys.chunks_exact(key_len).enumerate() {
                                use std::collections::hash_map::Entry;
                                match shard.map.entry(mk(key)) {
                                    Entry::Vacant(e) => {
                                        e.insert((buf.weights[idx], (base + idx) as u32));
                                    }
                                    Entry::Occupied(mut e) => {
                                        if has_payload {
                                            return Err(format!(
                                                "duplicate join key {key:?} in table \
                                                 '{table_name}' with payload columns — not yet \
                                                 supported"
                                            ));
                                        }
                                        e.get_mut().0 += buf.weights[idx];
                                    }
                                }
                            }
                            for (p, o) in shard.pay.iter_mut().zip(&buf.pay) {
                                p.append(o);
                            }
                        }
                        out_ref.lock().expect("lock")[s] = Some(shard);
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
    fn table_src(&self, t: usize) -> Result<RootSrc, String> {
        let q = self.q;
        let ti = &q.tables[t];
        Ok(match &ti.source {
            TableSource::Derived(i) => {
                let r = execute(&q.derived[*i])?;
                RootSrc::One(vec![result_to_chunk(&r, ti)?])
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
        let nrows = chunk.n_rows();
        let mut kbuf: Vec<i64> = Vec::with_capacity(8);
        for (child, links, left) in ctx.children {
            let dim = ctx.dims[*child].as_ref().expect("dim built");
            // Root-side keys read by direct slice index.
            let key_cols: Vec<KeyCol> = links.iter().map(|&(p, _)| key_col(&chunk, p)).collect();
            let has_pay = !dim.payload_slots.is_empty();
            // Narrow with multiplicity; a LEFT child keeps misses once. A
            // hit's (shard, payload row) is recorded during the SAME probe
            // — the attach below gathers without re-probing the map.
            let mut out = Vec::new();
            let mut refs: Vec<u64> = if has_pay {
                vec![NO_REF; nrows]
            } else {
                Vec::new()
            };
            let mut matched: Vec<i64> = if *left { vec![0; nrows] } else { Vec::new() };
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
                        if let Some(v) = agg.arg.eval_opt_i64(&view, i as usize) {
                            states[j].distinct.insert(v);
                        }
                    }),
                    _ => sel.for_each(|i| {
                        if let Some(v) = agg.arg.eval_opt_f64(&view, i as usize) {
                            states[j].update(v);
                        }
                    }),
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
                        if let Some(v) = agg.arg.eval_opt_i64(&view, i) {
                            states[j].distinct.insert(v);
                        }
                    }
                    _ => {
                        if let Some(v) = agg.arg.eval_opt_f64(&view, i) {
                            states[j].update(v);
                        }
                    }
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
            return Ok((columns, rows));
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
        Ok((columns, rows))
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

/// Evaluate one window expression over the block's result chunk (`n`
/// rows): partition, optionally order, then aggregate / rank.
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
            let mut out = vec![0.0f64; n];
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
                    for &r in rows.iter() {
                        out[r] = v;
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
                            for &r in &rows[i..j] {
                                out[r] = v;
                            }
                            i = j;
                        }
                    }
                }
            }
            Vector::f64(out)
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

/// A typed group-key value with a total order (BTreeMap grouping ⇒
/// deterministic key-sorted output). Floats order by `total_cmp` and group
/// by bit pattern — exact, NaN-safe.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum GroupKey {
    /// SQL NULL groups with itself and sorts first (declared first so the
    /// derived order puts NULL groups ahead — NULLS FIRST ascending).
    Null,
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
    let any_null = keys.iter().any(|k| matches!(k, GroupKey::Null));
    let valid = any_null.then(|| {
        keys.iter()
            .map(|k| !matches!(k, GroupKey::Null))
            .collect::<Vec<bool>>()
    });
    let witness = keys.iter().find(|k| !matches!(k, GroupKey::Null));
    let out = match witness {
        Some(GroupKey::Float(_)) => Vector::f64(
            keys.iter()
                .map(|k| match k {
                    GroupKey::Float(f) => f64::from_bits(f.0),
                    GroupKey::Null => 0.0,
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
                    GroupKey::Null => {}
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
                    GroupKey::Null => 0,
                    other => panic!("mixed group-key types: {other:?}"),
                })
                .collect(),
        ),
    };
    out.with_validity(valid)
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

/// Convert a materialized [`QueryResult`] into a chunk shaped like `ti`'s
/// projection (`leaf` = the result's output-column position; values coerced
/// to the declared type).
fn result_to_chunk(r: &QueryResult, ti: &TableInput) -> Result<DataChunk, String> {
    let nrows = r.rows.len();
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
                for row in 0..nrows {
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
                for row in 0..nrows {
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
                for row in 0..nrows {
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
        Expr::InSub { expr, .. }
        | Expr::InSet { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::Substr { expr, .. } => collect_slots(expr, out),
        Expr::Case { whens, else_ } => {
            for (c, v) in whens {
                collect_slots(c, out);
                collect_slots(v, out);
            }
            collect_slots(else_, out);
        }
    }
}
