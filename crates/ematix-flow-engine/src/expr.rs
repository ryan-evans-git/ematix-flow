//! P3 expression layer: a bound, typed scalar/predicate expression IR and a
//! tree-walking interpreter over the engine's [`DataChunk`].
//!
//! This is the general expression capability the engine lacked — until P3
//! every query's filter and aggregate arithmetic was hand-coded. A planned
//! `Filter` narrows the deferred selection with [`filter_expr`]; aggregate
//! arguments and output projections evaluate through [`Expr::eval_f64`] /
//! [`Expr::eval_value`].
//!
//! **Bound** means columns are indices (into a slot space or a result row —
//! the binder decides which; see `logical.rs`), never names, so evaluation
//! never touches a catalog. **Interpreted-first** (the program's stance):
//! this walks the tree per row. Correct and simple; a vectorized / compiled
//! evaluator is a labelled follow-on, and the hand-coded fast paths stay
//! until the general path is measured against them.

use std::sync::Arc;

use crate::chunk::{DataChunk, Selection};
use crate::pipeline::filter;
use crate::vector::LogicalType;

/// A literal scalar value.
#[derive(Clone, Debug, PartialEq)]
pub enum ScalarValue {
    Int32(i32),
    Int64(i64),
    Float64(f64),
    /// Days since the Unix epoch — same encoding as [`LogicalType::Date32`].
    Date32(i32),
    Boolean(bool),
    Utf8(Arc<str>),
    /// SQL NULL — produced by NULL-bearing columns (validity) and the
    /// `NULL` literal. Comparisons with NULL are not-satisfied (filters
    /// drop the row), arithmetic propagates it, aggregates skip it.
    Null,
}

/// A binary operator: arithmetic, comparison, or logical.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    /// Division always evaluates in `f64` (the TPC-H decimal-division
    /// shapes); exact integer/decimal division is a labelled follow-on.
    Div,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
}

/// A bound expression tree. `Column(i)` reads the evaluation context's
/// `i`-th column (a table/global slot, or a result-row position — see
/// `logical.rs` for the two spaces).
#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Column(usize),
    Literal(ScalarValue),
    Binary {
        op: BinaryOp,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
    },
    /// `EXTRACT(YEAR FROM <date expr>)` — Date32 days → calendar year.
    ExtractYear(Box<Expr>),
    /// `CAST(<expr> AS INT/INTEGER/BIGINT/SMALLINT)` on a fractional value —
    /// rounds to the nearest integer (DuckDB `CAST` semantics: `10714.82` →
    /// `10715`). An already-integer operand is unchanged.
    CastInt(Box<Expr>),
    /// `round(<expr>, digits)` — round half away from zero at `digits`
    /// decimal places (q2/q78's ratio projections).
    Round {
        expr: Box<Expr>,
        digits: i32,
    },
    /// `upper(<expr>)` — an OWNED string, so like [`Expr::Concat`] it is
    /// evaluated via `eval_value` (projections), and comparisons containing
    /// it are handled inline in the `Binary` arm (no owned value escapes
    /// the borrowed `Val` path). q24's `c_birth_country = upper(ca_country)`.
    Upper(Box<Expr>),
    /// `CASE WHEN c₁ THEN v₁ [WHEN c₂ THEN v₂ …] ELSE e END` (the `ELSE` is
    /// required by the binder — no NULLs yet).
    Case {
        whens: Vec<(Expr, Expr)>,
        else_: Box<Expr>,
    },
    /// `<expr> [NOT] LIKE '<pattern>'` — SQL pattern match (`%` = any run,
    /// `_` = any one byte).
    Like {
        expr: Box<Expr>,
        pattern: String,
        negated: bool,
    },
    /// An unresolved scalar subquery — index into
    /// [`BoundQuery::subqueries`](crate::logical::BoundQuery). The executor
    /// substitutes the computed constant before evaluation; evaluating one
    /// directly is a wiring bug.
    ScalarSub(usize),
    /// An unresolved `[NOT] IN (<subquery>)` — substituted with [`Expr::InSet`]
    /// at execution.
    InSub {
        expr: Box<Expr>,
        sub: usize,
        negated: bool,
    },
    /// A materialized integer membership test (from an IN-subquery).
    InSet {
        expr: Box<Expr>,
        set: std::sync::Arc<std::collections::HashSet<i64>>,
        negated: bool,
    },
    /// A materialized STRING membership test (from an IN-subquery over a
    /// string column).
    InSetStr {
        expr: Box<Expr>,
        set: std::sync::Arc<std::collections::HashSet<Box<str>>>,
        negated: bool,
    },
    /// `<expr> IS [NOT] NULL`.
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    /// `SUBSTRING(<expr> FROM <start> [FOR <len>])` — 1-based, byte
    /// positions (TPC-H strings are ASCII).
    Substr {
        expr: Box<Expr>,
        from: i64,
        len: Option<i64>,
    },
    /// `concat(a, b, …)` — string concatenation producing an OWNED string,
    /// so it is only evaluated via [`Expr::eval_value`] (projection outputs
    /// and group keys), never in the borrowed `Val` path. NULL arguments
    /// are skipped (DuckDB `concat` semantics), so the result is never NULL.
    Concat(Vec<Expr>),
}

/// A value produced during evaluation. Integer-family logical types
/// (`Int32`, `Int64`, `Date32`) collapse to `Int(i64)`, so numeric
/// promotion has one integer case and one float case. Strings borrow from
/// the chunk (or the expression's own literal).
#[derive(Clone, Copy, Debug, PartialEq)]
enum Val<'a> {
    Int(i64),
    Float(f64),
    Bool(bool),
    Str(&'a str),
    /// SQL NULL (three-valued-logic lite: a NULL condition is
    /// not-satisfied, NULL arithmetic propagates).
    Null,
}

impl Val<'_> {
    #[inline]
    fn as_f64(self) -> f64 {
        match self {
            Val::Int(i) => i as f64,
            Val::Float(f) => f,
            Val::Bool(b) => i64::from(b) as f64,
            Val::Str(_) => panic!("string value in numeric context"),
            Val::Null => panic!("NULL in a must-be-non-null numeric context"),
        }
    }

    /// A condition's truth: NULL counts as not-satisfied (the filter /
    /// CASE-branch semantics of SQL's UNKNOWN).
    #[inline]
    fn expect_bool(self) -> bool {
        match self {
            Val::Bool(b) => b,
            Val::Null => false,
            other => panic!("expected a boolean operand, got {other:?}"),
        }
    }

    #[inline]
    fn expect_int(self, what: &str) -> i64 {
        match self {
            Val::Int(i) => i,
            other => panic!("{what}: expected an integer, got {other:?}"),
        }
    }
}

impl Expr {
    /// Visit every column slot this expression references. Exhaustive over
    /// the variants (no wildcard arm) so a new variant fails to compile
    /// here rather than silently escaping a slot analysis.
    pub fn for_each_col(&self, f: &mut impl FnMut(usize)) {
        match self {
            Expr::Column(c) => f(*c),
            Expr::Literal(_) | Expr::ScalarSub(_) => {}
            Expr::Binary { lhs, rhs, .. } => {
                lhs.for_each_col(f);
                rhs.for_each_col(f);
            }
            Expr::ExtractYear(e) | Expr::CastInt(e) | Expr::Upper(e) => e.for_each_col(f),
            Expr::Round { expr, .. }
            | Expr::Like { expr, .. }
            | Expr::InSub { expr, .. }
            | Expr::InSet { expr, .. }
            | Expr::InSetStr { expr, .. }
            | Expr::IsNull { expr, .. }
            | Expr::Substr { expr, .. } => expr.for_each_col(f),
            Expr::Case { whens, else_ } => {
                for (c, v) in whens {
                    c.for_each_col(f);
                    v.for_each_col(f);
                }
                else_.for_each_col(f);
            }
            Expr::Concat(es) => {
                for e in es {
                    e.for_each_col(f);
                }
            }
        }
    }

    /// Evaluate this expression at `row` of `chunk`.
    #[inline]
    fn eval<'e>(&'e self, chunk: &'e DataChunk, row: usize) -> Val<'e> {
        match self {
            Expr::Column(i) => {
                let v = chunk.col(*i);
                if let Some(valid) = &v.validity
                    && !valid[row]
                {
                    return Val::Null;
                }
                match v.logical {
                    LogicalType::Int32 | LogicalType::Date32 => Val::Int(v.as_i32()[row] as i64),
                    LogicalType::Int64 => Val::Int(v.as_i64()[row]),
                    LogicalType::Float64 => Val::Float(v.as_f64()[row]),
                    LogicalType::Utf8 => Val::Str(v.as_utf8().get(row)),
                }
            }
            Expr::Literal(s) => match s {
                ScalarValue::Int32(x) => Val::Int(*x as i64),
                ScalarValue::Int64(x) => Val::Int(*x),
                ScalarValue::Date32(x) => Val::Int(*x as i64),
                ScalarValue::Float64(x) => Val::Float(*x),
                ScalarValue::Boolean(b) => Val::Bool(*b),
                ScalarValue::Utf8(s) => Val::Str(s),
                ScalarValue::Null => Val::Null,
            },
            Expr::Binary { op, lhs, rhs } => {
                // upper() yields an OWNED string; a comparison containing it
                // evaluates both sides here (transform applied inline) so no
                // owned value escapes the borrowed path.
                if matches!(lhs.as_ref(), Expr::Upper(_)) || matches!(rhs.as_ref(), Expr::Upper(_))
                {
                    let side = |e: &Expr| -> Option<String> {
                        match e {
                            Expr::Upper(inner) => match inner.eval(chunk, row) {
                                Val::Str(s) => Some(s.to_uppercase()),
                                Val::Null => None,
                                other => panic!("upper needs a string, got {other:?}"),
                            },
                            _ => match e.eval(chunk, row) {
                                Val::Str(s) => Some(s.to_string()),
                                Val::Null => None,
                                other => panic!("string comparison got {other:?}"),
                            },
                        }
                    };
                    let (Some(a), Some(b)) = (side(lhs), side(rhs)) else {
                        return Val::Null;
                    };
                    return match op {
                        BinaryOp::Eq => Val::Bool(a == b),
                        BinaryOp::NotEq => Val::Bool(a != b),
                        other => panic!("upper() only supports =/<> comparisons, got {other:?}"),
                    };
                }
                let l = lhs.eval(chunk, row);
                let r = rhs.eval(chunk, row);
                eval_binary(*op, l, r)
            }
            Expr::ExtractYear(e) => match e.eval(chunk, row) {
                Val::Int(days) => Val::Int(year_of_days(days as i32) as i64),
                Val::Null => Val::Null,
                other => panic!("EXTRACT(YEAR) needs a date operand, got {other:?}"),
            },
            Expr::CastInt(e) => match e.eval(chunk, row) {
                Val::Int(i) => Val::Int(i),
                Val::Float(f) => Val::Int(f.round() as i64),
                Val::Null => Val::Null,
                other => panic!("CAST AS INT needs a numeric operand, got {other:?}"),
            },
            Expr::Round { expr, digits } => match expr.eval(chunk, row) {
                Val::Int(i) => Val::Int(i),
                Val::Float(f) => {
                    let p = 10f64.powi(*digits);
                    Val::Float((f * p).round() / p)
                }
                Val::Null => Val::Null,
                other => panic!("round needs a numeric operand, got {other:?}"),
            },
            Expr::Upper(_) => {
                panic!("upper() must be evaluated via eval_value or inside a comparison")
            }
            Expr::Case { whens, else_ } => {
                for (cond, val) in whens {
                    if cond.eval(chunk, row).expect_bool() {
                        return val.eval(chunk, row);
                    }
                }
                else_.eval(chunk, row)
            }
            Expr::Like {
                expr,
                pattern,
                negated,
            } => match expr.eval(chunk, row) {
                Val::Str(s) => Val::Bool(like_match(s.as_bytes(), pattern.as_bytes()) != *negated),
                Val::Null => Val::Null,
                other => panic!("LIKE needs a string operand, got {other:?}"),
            },
            Expr::ScalarSub(_) | Expr::InSub { .. } => {
                panic!("unresolved subquery in evaluation — executor substitution missed it")
            }
            Expr::InSetStr { expr, set, negated } => match expr.eval(chunk, row) {
                Val::Null => Val::Null,
                Val::Str(v) => Val::Bool(set.contains(v) != *negated),
                other => panic!("string IN needs a string operand, got {other:?}"),
            },
            Expr::IsNull { expr, negated } => {
                Val::Bool(matches!(expr.eval(chunk, row), Val::Null) != *negated)
            }
            Expr::InSet { expr, set, negated } => match expr.eval(chunk, row) {
                // NULL [NOT] IN (…) is UNKNOWN — not satisfied either way.
                Val::Null => Val::Null,
                v => Val::Bool(set.contains(&v.expect_int("IN operand")) != *negated),
            },
            Expr::Substr { expr, from, len } => match expr.eval(chunk, row) {
                Val::Str(s) => {
                    let start = ((*from - 1).max(0) as usize).min(s.len());
                    let end = match len {
                        Some(l) => (start + (*l).max(0) as usize).min(s.len()),
                        None => s.len(),
                    };
                    Val::Str(&s[start..end])
                }
                Val::Null => Val::Null,
                other => panic!("SUBSTRING needs a string operand, got {other:?}"),
            },
            // `concat` yields an owned string; the binder only ever places
            // it in a projection / group-key slot (evaluated by eval_value),
            // never inside a filter or arithmetic (the borrowed Val path).
            Expr::Concat(_) => {
                panic!("concat must be evaluated via eval_value, not the borrowed Val path")
            }
        }
    }

    /// Evaluate to `f64` — the numeric path (an aggregate argument like
    /// `extendedprice * discount`).
    #[inline]
    pub fn eval_f64(&self, chunk: &DataChunk, row: usize) -> f64 {
        self.eval(chunk, row).as_f64()
    }

    /// Evaluate a boolean predicate at `row`. Panics if the expression is not
    /// boolean — a binder wiring bug, not a data condition.
    #[inline]
    pub fn eval_bool(&self, chunk: &DataChunk, row: usize) -> bool {
        self.eval(chunk, row).expect_bool()
    }

    /// Evaluate an integer expression at `row` — the group-key path (keys
    /// route through the engine's i64 hash aggregation). Panics on a
    /// non-integer result: the binder guarantees integer-family keys, so a
    /// miss here is a wiring bug, not a data condition.
    #[inline]
    pub fn eval_i64(&self, chunk: &DataChunk, row: usize) -> i64 {
        match self.eval(chunk, row) {
            Val::Int(i) => i,
            other => panic!("expected an integer group key, got {other:?}"),
        }
    }

    /// Evaluate to a typed [`ScalarValue`] — the output-projection path,
    /// preserving integer-ness (a passed-through group key stays `Int64`).
    pub fn eval_value(&self, chunk: &DataChunk, row: usize) -> ScalarValue {
        // `concat` builds an owned string — evaluate it here (not in the
        // borrowed `Val` path). NULL arguments are skipped (DuckDB
        // semantics), numbers render as their decimal text.
        if let Expr::Concat(parts) = self {
            let mut out = String::new();
            for p in parts {
                match p.eval_value(chunk, row) {
                    ScalarValue::Null => {}
                    ScalarValue::Utf8(s) => out.push_str(&s),
                    ScalarValue::Int64(i) => out.push_str(&i.to_string()),
                    ScalarValue::Int32(i) => out.push_str(&i.to_string()),
                    ScalarValue::Date32(d) => out.push_str(&d.to_string()),
                    ScalarValue::Boolean(b) => out.push_str(if b { "true" } else { "false" }),
                    ScalarValue::Float64(f) => out.push_str(&f.to_string()),
                }
            }
            return ScalarValue::Utf8(Arc::from(out.as_str()));
        }
        // upper() builds an owned string — evaluate here like concat.
        if let Expr::Upper(inner) = self {
            return match inner.eval_value(chunk, row) {
                ScalarValue::Utf8(s) => ScalarValue::Utf8(Arc::from(s.to_uppercase().as_str())),
                ScalarValue::Null => ScalarValue::Null,
                other => panic!("upper needs a string, got {other:?}"),
            };
        }
        match self.eval(chunk, row) {
            Val::Int(i) => ScalarValue::Int64(i),
            Val::Float(f) => ScalarValue::Float64(f),
            Val::Bool(b) => ScalarValue::Boolean(b),
            Val::Str(s) => ScalarValue::Utf8(Arc::from(s)),
            Val::Null => ScalarValue::Null,
        }
    }

    /// Evaluate to `f64`, or `None` on SQL NULL — the aggregate-input
    /// path (aggregates skip NULL arguments).
    #[inline]
    pub fn eval_opt_f64(&self, chunk: &DataChunk, row: usize) -> Option<f64> {
        match self.eval(chunk, row) {
            Val::Null => None,
            v => Some(v.as_f64()),
        }
    }

    /// Evaluate to `i64`, or `None` on SQL NULL (COUNT DISTINCT skips
    /// NULLs like every aggregate).
    #[inline]
    pub fn eval_opt_i64(&self, chunk: &DataChunk, row: usize) -> Option<i64> {
        match self.eval(chunk, row) {
            Val::Null => None,
            v => Some(v.expect_int("integer aggregate input")),
        }
    }

    /// Evaluate to a COUNT(DISTINCT …) set key, or `None` on SQL NULL.
    /// Integers key as themselves; floats by bit pattern (`-0.0`
    /// normalized so it groups with `0.0`) — q28's
    /// `count(DISTINCT ss_list_price)` over a decimal column.
    #[inline]
    pub fn eval_opt_distinct_key(&self, chunk: &DataChunk, row: usize) -> Option<i64> {
        match self.eval(chunk, row) {
            Val::Null => None,
            Val::Int(i) => Some(i),
            Val::Float(f) => Some((if f == 0.0 { 0.0f64 } else { f }).to_bits() as i64),
            other => panic!("COUNT(DISTINCT) key: unsupported value {other:?}"),
        }
    }

    /// Is this expression SQL NULL at `row`?
    #[inline]
    pub fn eval_is_null(&self, chunk: &DataChunk, row: usize) -> bool {
        matches!(self.eval(chunk, row), Val::Null)
    }
}

#[inline]
fn eval_binary<'a>(op: BinaryOp, l: Val<'a>, r: Val<'a>) -> Val<'a> {
    use BinaryOp::*;
    // NULL propagation: arithmetic and comparison with a NULL operand
    // yield NULL (a comparison's UNKNOWN — `expect_bool` maps it to
    // not-satisfied). AND/OR treat NULL conditions as not-satisfied.
    if matches!(l, Val::Null) || matches!(r, Val::Null) {
        return match op {
            And => Val::Bool(l.expect_bool() && r.expect_bool()),
            Or => Val::Bool(l.expect_bool() || r.expect_bool()),
            _ => Val::Null,
        };
    }
    match op {
        Add | Sub | Mul | Div => arith(op, l, r),
        Eq | NotEq | Lt | LtEq | Gt | GtEq => Val::Bool(compare(op, l, r)),
        And => Val::Bool(l.expect_bool() && r.expect_bool()),
        Or => Val::Bool(l.expect_bool() || r.expect_bool()),
    }
}

/// Arithmetic with numeric promotion: integer stays integer (except `Div`,
/// which always evaluates in `f64`); any float operand promotes the whole
/// operation to `f64`.
#[inline]
fn arith<'a>(op: BinaryOp, l: Val<'a>, r: Val<'a>) -> Val<'a> {
    use BinaryOp::*;
    if let (Val::Int(a), Val::Int(b), false) = (l, r, matches!(op, Div)) {
        return Val::Int(match op {
            Add => a + b,
            Sub => a - b,
            Mul => a * b,
            _ => unreachable!("non-arithmetic op in arith()"),
        });
    }
    let (a, b) = (l.as_f64(), r.as_f64());
    Val::Float(match op {
        Add => a + b,
        Sub => a - b,
        Mul => a * b,
        Div => a / b,
        _ => unreachable!("non-arithmetic op in arith()"),
    })
}

/// Comparison with the same promotion rule: both integer ⇒ exact integer
/// compare; strings compare as strings; otherwise compare as `f64`.
#[inline]
fn compare(op: BinaryOp, l: Val<'_>, r: Val<'_>) -> bool {
    use BinaryOp::*;
    use std::cmp::Ordering;
    let ord = match (l, r) {
        (Val::Int(a), Val::Int(b)) => a.cmp(&b),
        (Val::Str(a), Val::Str(b)) => a.cmp(b),
        (Val::Str(_), _) | (_, Val::Str(_)) => {
            panic!("cannot compare a string with a non-string")
        }
        _ => match l.as_f64().partial_cmp(&r.as_f64()) {
            Some(o) => o,
            // NaN-free in practice; an unordered compare matches nothing.
            None => return false,
        },
    };
    match op {
        Eq => ord == Ordering::Equal,
        NotEq => ord != Ordering::Equal,
        Lt => ord == Ordering::Less,
        LtEq => ord != Ordering::Greater,
        Gt => ord == Ordering::Greater,
        GtEq => ord != Ordering::Less,
        _ => unreachable!("non-comparison op in compare()"),
    }
}

/// SQL LIKE match over bytes: `%` matches any run (including empty), `_`
/// matches exactly one byte. Classic greedy matcher with single-`%`
/// backtracking — linear for the TPC-H patterns (`PROMO%`, `%special%`).
fn like_match(s: &[u8], p: &[u8]) -> bool {
    let (mut si, mut pi) = (0usize, 0usize);
    let (mut star_p, mut star_s) = (usize::MAX, 0usize);
    while si < s.len() {
        if pi < p.len() && (p[pi] == b'_' || p[pi] == s[si]) {
            si += 1;
            pi += 1;
        } else if pi < p.len() && p[pi] == b'%' {
            star_p = pi;
            star_s = si;
            pi += 1;
        } else if star_p != usize::MAX {
            star_s += 1;
            si = star_s;
            pi = star_p + 1;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == b'%' {
        pi += 1;
    }
    pi == p.len()
}

/// Days since the Unix epoch → calendar year (proleptic Gregorian; the
/// inverse direction of the binder's `days_from_civil`).
fn year_of_days(days: i32) -> i32 {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i32 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    if m <= 2 { y + 1 } else { y }
}

/// Narrow `chunk`'s current selection to the rows satisfying boolean
/// predicate `pred`. No-materialization: columns are never compacted — this
/// is the general-expression form of [`crate::pipeline::filter`].
///
/// COLUMN-AT-A-TIME fast path: the common predicate algebra (And/Or,
/// comparisons against literals and columns, IS NULL, IN sets, LIKE)
/// evaluates as typed whole-column boolean masks — tight monomorphized
/// loops — instead of walking the recursive [`Val`] interpreter per row
/// (q28 sf10: per-row eval was 20× the next-hottest symbol). Unsupported
/// subtrees fall back to per-row evaluation, restricted to the rows the
/// surrounding mask still leaves live. Masks encode SQL three-valued
/// logic as NULL→false, which composes exactly through And/Or (there is
/// no NOT node — negation lives in per-leaf flags, handled per leaf).
/// Skipped when the live selection is sparse (whole-column loops would
/// out-cost the survivors).
pub fn filter_expr(chunk: &DataChunk, pred: &Expr) -> Selection {
    // The row domain comes from the SELECTION for the dense case:
    // fan-out views carry empty placeholder columns, so `n_rows()` (the
    // first column's length) can read 0 there while the live columns are
    // full-length — which silently disabled the mask path (every leaf's
    // length check failed).
    let n = match &chunk.sel {
        Selection::All(k) => *k,
        Selection::Indices(_) => chunk.n_rows(),
    };
    if chunk.sel.len() * 4 >= n
        && let Some(mask) = try_mask(chunk, pred, n)
    {
        return filter(chunk, |i| mask[i]);
    }
    filter(chunk, |i| pred.eval_bool(chunk, i))
}

/// Build the boolean mask for `e` over rows `0..n`, or `None` if some
/// part of the subtree has no columnar kernel (the caller decides where
/// to fall back — a maskable sibling under And/Or still pays off).
fn try_mask(chunk: &DataChunk, e: &Expr, n: usize) -> Option<Vec<bool>> {
    match e {
        Expr::Binary {
            op: BinaryOp::And,
            lhs,
            rhs,
        } => match (try_mask(chunk, lhs, n), try_mask(chunk, rhs, n)) {
            (Some(mut a), Some(b)) => {
                for (x, y) in a.iter_mut().zip(&b) {
                    *x &= y;
                }
                Some(a)
            }
            // Hybrid: the unmaskable side evaluates per row, but only
            // where the masked side already passed.
            (Some(mut a), None) => {
                for (i, x) in a.iter_mut().enumerate() {
                    if *x {
                        *x = rhs.eval_bool(chunk, i);
                    }
                }
                Some(a)
            }
            (None, Some(mut b)) => {
                for (i, x) in b.iter_mut().enumerate() {
                    if *x {
                        *x = lhs.eval_bool(chunk, i);
                    }
                }
                Some(b)
            }
            (None, None) => None,
        },
        Expr::Binary {
            op: BinaryOp::Or,
            lhs,
            rhs,
        } => match (try_mask(chunk, lhs, n), try_mask(chunk, rhs, n)) {
            (Some(mut a), Some(b)) => {
                for (x, y) in a.iter_mut().zip(&b) {
                    *x |= y;
                }
                Some(a)
            }
            (Some(mut a), None) => {
                for (i, x) in a.iter_mut().enumerate() {
                    if !*x {
                        *x = rhs.eval_bool(chunk, i);
                    }
                }
                Some(a)
            }
            (None, Some(mut b)) => {
                for (i, x) in b.iter_mut().enumerate() {
                    if !*x {
                        *x = lhs.eval_bool(chunk, i);
                    }
                }
                Some(b)
            }
            (None, None) => None,
        },
        Expr::Binary { op, lhs, rhs }
            if matches!(
                op,
                BinaryOp::Eq
                    | BinaryOp::NotEq
                    | BinaryOp::Lt
                    | BinaryOp::LtEq
                    | BinaryOp::Gt
                    | BinaryOp::GtEq
            ) =>
        {
            cmp_mask(chunk, *op, lhs, rhs, n)
        }
        Expr::IsNull { expr, negated } => {
            let Expr::Column(c) = expr.as_ref() else {
                return None;
            };
            let v = chunk.col(*c);
            if v.len() != n {
                return None;
            }
            Some(match &v.validity {
                Some(m) => m.iter().map(|&ok| ok == *negated).collect(),
                None => vec![*negated; n],
            })
        }
        Expr::InSet { expr, set, negated } => {
            let Expr::Column(c) = expr.as_ref() else {
                return None;
            };
            let v = chunk.col(*c);
            if v.len() != n {
                return None;
            }
            let get = IntGet::of(v)?;
            let (set, neg) = (set.as_ref(), *negated);
            Some(mask_valid(v.validity.as_deref(), n, |i| {
                set.contains(&get.at(i)) != neg
            }))
        }
        Expr::InSetStr { expr, set, negated } => {
            let Expr::Column(c) = expr.as_ref() else {
                return None;
            };
            let v = chunk.col(*c);
            if v.len() != n || v.logical != LogicalType::Utf8 {
                return None;
            }
            let view = v.as_utf8();
            let (set, neg) = (set.as_ref(), *negated);
            Some(mask_valid(v.validity.as_deref(), n, |i| {
                set.contains(view.get(i)) != neg
            }))
        }
        Expr::Like {
            expr,
            pattern,
            negated,
        } => {
            let Expr::Column(c) = expr.as_ref() else {
                return None;
            };
            let v = chunk.col(*c);
            if v.len() != n || v.logical != LogicalType::Utf8 {
                return None;
            }
            let view = v.as_utf8();
            let (pat, neg) = (pattern.as_bytes(), *negated);
            Some(mask_valid(v.validity.as_deref(), n, |i| {
                like_match(view.get(i).as_bytes(), pat) != neg
            }))
        }
        _ => None,
    }
}

/// `f(i)` gated by validity: an invalid (NULL) row is `false` (a NULL
/// operand makes every leaf here UNKNOWN → not-satisfied, matching the
/// interpreter's `Val::Null` handling — including the negated forms,
/// where the interpreter also yields `Val::Null` before negation applies).
#[inline]
fn mask_valid(valid: Option<&[bool]>, n: usize, f: impl Fn(usize) -> bool) -> Vec<bool> {
    match valid {
        None => (0..n).map(f).collect(),
        Some(m) => (0..n).map(|i| m[i] && f(i)).collect(),
    }
}

/// Typed access to an integer-family column as `i64` (the interpreter's
/// promotion rule).
#[derive(Clone, Copy)]
enum IntGet<'a> {
    I32(&'a [i32]),
    I64(&'a [i64]),
}

impl<'a> IntGet<'a> {
    fn of(v: &'a crate::vector::Vector) -> Option<Self> {
        match v.logical {
            LogicalType::Int32 | LogicalType::Date32 => Some(IntGet::I32(v.as_i32())),
            LogicalType::Int64 => Some(IntGet::I64(v.as_i64())),
            _ => None,
        }
    }
    #[inline]
    fn at(self, i: usize) -> i64 {
        match self {
            IntGet::I32(s) => s[i] as i64,
            IntGet::I64(s) => s[i],
        }
    }
}

/// Typed access to any numeric column as `f64` (the promotion target when
/// a float is involved).
#[derive(Clone, Copy)]
enum NumGet<'a> {
    F64(&'a [f64]),
    Int(IntGet<'a>),
}

impl<'a> NumGet<'a> {
    fn of(v: &'a crate::vector::Vector) -> Option<Self> {
        match v.logical {
            LogicalType::Float64 => Some(NumGet::F64(v.as_f64())),
            _ => IntGet::of(v).map(NumGet::Int),
        }
    }
    #[inline]
    fn at(self, i: usize) -> f64 {
        match self {
            NumGet::F64(s) => s[i],
            NumGet::Int(g) => g.at(i) as f64,
        }
    }
}

/// Comparison-op test over an `Ordering`, monomorphized per op so each
/// mask loop is a tight closure (`None` = unordered float compare —
/// matches nothing, like the interpreter).
#[inline]
fn ord_mask(
    op: BinaryOp,
    valid: Option<&[bool]>,
    n: usize,
    f: impl Fn(usize) -> Option<std::cmp::Ordering> + Copy,
) -> Vec<bool> {
    use std::cmp::Ordering::*;
    match op {
        BinaryOp::Eq => mask_valid(valid, n, |i| f(i) == Some(Equal)),
        BinaryOp::NotEq => mask_valid(valid, n, |i| f(i).is_some_and(|o| o != Equal)),
        BinaryOp::Lt => mask_valid(valid, n, |i| f(i) == Some(Less)),
        BinaryOp::LtEq => mask_valid(valid, n, |i| f(i).is_some_and(|o| o != Greater)),
        BinaryOp::Gt => mask_valid(valid, n, |i| f(i) == Some(Greater)),
        BinaryOp::GtEq => mask_valid(valid, n, |i| f(i).is_some_and(|o| o != Less)),
        _ => unreachable!("non-comparison op in ord_mask"),
    }
}

/// Flip a comparison for `literal op column` → `column op' literal`.
fn flip_cmp(op: BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::LtEq => BinaryOp::GtEq,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::GtEq => BinaryOp::LtEq,
        other => other,
    }
}

/// Columnar kernel for a comparison node: column-vs-literal (either
/// side) and column-vs-column, with the interpreter's exact promotion
/// rules — int/int exact in `i64`, strings byte-ordered, any float
/// involvement compares in `f64` via `partial_cmp`.
fn cmp_mask(
    chunk: &DataChunk,
    op: BinaryOp,
    lhs: &Expr,
    rhs: &Expr,
    n: usize,
) -> Option<Vec<bool>> {
    let (c, lit, op) = match (lhs, rhs) {
        (Expr::Column(c), Expr::Literal(v)) => (*c, v, op),
        (Expr::Literal(v), Expr::Column(c)) => (*c, v, flip_cmp(op)),
        (Expr::Column(a), Expr::Column(b)) => return cmp_cols_mask(chunk, op, *a, *b, n),
        _ => return None,
    };
    let v = chunk.col(c);
    if v.len() != n {
        return None;
    }
    let valid = v.validity.as_deref();
    let int_lit = match lit {
        ScalarValue::Int64(x) => Some(*x),
        ScalarValue::Int32(x) => Some(*x as i64),
        ScalarValue::Date32(x) => Some(*x as i64),
        _ => None,
    };
    match (v.logical, lit) {
        (LogicalType::Utf8, ScalarValue::Utf8(b)) => {
            let view = v.as_utf8();
            let b: &str = b;
            Some(ord_mask(op, valid, n, |i| Some(view.get(i).cmp(b))))
        }
        (LogicalType::Float64, _) => {
            let b = match lit {
                ScalarValue::Float64(x) => *x,
                _ => int_lit? as f64,
            };
            let s = v.as_f64();
            Some(ord_mask(op, valid, n, |i| s[i].partial_cmp(&b)))
        }
        (_, ScalarValue::Float64(b)) => {
            let g = IntGet::of(v)?;
            let b = *b;
            Some(ord_mask(op, valid, n, |i| (g.at(i) as f64).partial_cmp(&b)))
        }
        _ => {
            let g = IntGet::of(v)?;
            let b = int_lit?;
            Some(ord_mask(op, valid, n, |i| Some(g.at(i).cmp(&b))))
        }
    }
}

/// Column-vs-column comparison mask. A NULL on either side is UNKNOWN →
/// false, so the two validity masks AND together.
fn cmp_cols_mask(
    chunk: &DataChunk,
    op: BinaryOp,
    a: usize,
    b: usize,
    n: usize,
) -> Option<Vec<bool>> {
    let (va, vb) = (chunk.col(a), chunk.col(b));
    if va.len() != n || vb.len() != n {
        return None;
    }
    let combined: Option<Vec<bool>> = match (&va.validity, &vb.validity) {
        (None, None) => None,
        (Some(m), None) | (None, Some(m)) => Some(m.to_vec()),
        (Some(x), Some(y)) => Some(x.iter().zip(y.iter()).map(|(&p, &q)| p && q).collect()),
    };
    let valid = combined.as_deref();
    match (va.logical, vb.logical) {
        (LogicalType::Utf8, LogicalType::Utf8) => {
            let (x, y) = (va.as_utf8(), vb.as_utf8());
            Some(ord_mask(op, valid, n, |i| Some(x.get(i).cmp(y.get(i)))))
        }
        (LogicalType::Utf8, _) | (_, LogicalType::Utf8) => None,
        (LogicalType::Float64, _) | (_, LogicalType::Float64) => {
            let (x, y) = (NumGet::of(va)?, NumGet::of(vb)?);
            Some(ord_mask(op, valid, n, |i| x.at(i).partial_cmp(&y.at(i))))
        }
        _ => {
            let (x, y) = (IntGet::of(va)?, IntGet::of(vb)?);
            Some(ord_mask(op, valid, n, |i| Some(x.at(i).cmp(&y.at(i)))))
        }
    }
}

/// Sum a numeric expression `arg` over the live rows of `sel`, as `f64` — the
/// scalar-aggregate sink for `sum(<expr>)` (e.g. Q6's `sum(extendedprice *
/// discount)`).
pub fn sum_expr_f64(chunk: &DataChunk, sel: &Selection, arg: &Expr) -> f64 {
    let mut acc = 0.0_f64;
    sel.for_each(|i| {
        if let Some(v) = arg.eval_opt_f64(chunk, i as usize) {
            acc += v;
        }
    });
    acc
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::Vector;

    #[test]
    fn literals_and_columns_carry_type() {
        let chunk = DataChunk::new(vec![Vector::i64(vec![7]), Vector::f64(vec![1.5])]);
        assert_eq!(Expr::Column(0).eval_f64(&chunk, 0), 7.0);
        assert_eq!(Expr::Column(1).eval_f64(&chunk, 0), 1.5);
        assert_eq!(
            Expr::Literal(ScalarValue::Int32(42)).eval_f64(&chunk, 0),
            42.0
        );
    }

    #[test]
    fn and_or_short_circuit_semantics() {
        let chunk = DataChunk::new(vec![Vector::i64(vec![1])]);
        let t = Expr::Literal(ScalarValue::Boolean(true));
        let f = Expr::Literal(ScalarValue::Boolean(false));
        let and = |a: &Expr, b: &Expr| Expr::Binary {
            op: BinaryOp::And,
            lhs: Box::new(a.clone()),
            rhs: Box::new(b.clone()),
        };
        assert!(and(&t, &t).eval_bool(&chunk, 0));
        assert!(!and(&t, &f).eval_bool(&chunk, 0));
    }

    #[test]
    fn like_patterns() {
        assert!(like_match(b"PROMO BURNISHED", b"PROMO%"));
        assert!(!like_match(b"STANDARD BRUSHED", b"PROMO%"));
        assert!(like_match(b"abcXdef", b"abc_def"));
        assert!(like_match(b"special packages", b"%special%"));
        assert!(like_match(b"", b"%"));
        assert!(!like_match(b"ab", b"a_c"));
        assert!(like_match(b"aXbYc", b"a%b%c"));
    }

    #[test]
    fn extract_year_inverts_days_from_civil() {
        let chunk = DataChunk::new(vec![Vector::i32(
            vec![0, 8766, 9131, 9495, 9496, 9861],
            LogicalType::Date32,
        )]);
        let e = Expr::ExtractYear(Box::new(Expr::Column(0)));
        let years: Vec<i64> = (0..6).map(|r| e.eval_i64(&chunk, r)).collect();
        // 1970-01-01, 1994-01-01, 1995-01-01, 1995-12-31, 1996-01-01,
        // 1996-12-31.
        assert_eq!(years, vec![1970, 1994, 1995, 1995, 1996, 1996]);
    }

    #[test]
    fn case_and_strings_and_division() {
        let chunk = DataChunk::new(vec![
            Vector::utf8(vec![0, 6, 12], b"BRAZILCANADA".to_vec()),
            Vector::f64(vec![10.0, 20.0]),
        ]);
        // CASE WHEN col0 = 'BRAZIL' THEN col1 ELSE 0 END
        let case = Expr::Case {
            whens: vec![(
                Expr::Binary {
                    op: BinaryOp::Eq,
                    lhs: Box::new(Expr::Column(0)),
                    rhs: Box::new(Expr::Literal(ScalarValue::Utf8("BRAZIL".into()))),
                },
                Expr::Column(1),
            )],
            else_: Box::new(Expr::Literal(ScalarValue::Int64(0))),
        };
        assert_eq!(case.eval_f64(&chunk, 0), 10.0);
        assert_eq!(case.eval_f64(&chunk, 1), 0.0);

        // Division always evaluates f64, including int/int.
        let div = Expr::Binary {
            op: BinaryOp::Div,
            lhs: Box::new(Expr::Literal(ScalarValue::Int64(7))),
            rhs: Box::new(Expr::Literal(ScalarValue::Int64(2))),
        };
        assert_eq!(div.eval_f64(&chunk, 0), 3.5);
    }
}
