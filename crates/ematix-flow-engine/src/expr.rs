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
                let l = lhs.eval(chunk, row);
                let r = rhs.eval(chunk, row);
                eval_binary(*op, l, r)
            }
            Expr::ExtractYear(e) => match e.eval(chunk, row) {
                Val::Int(days) => Val::Int(year_of_days(days as i32) as i64),
                Val::Null => Val::Null,
                other => panic!("EXTRACT(YEAR) needs a date operand, got {other:?}"),
            },
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
pub fn filter_expr(chunk: &DataChunk, pred: &Expr) -> Selection {
    filter(chunk, |i| pred.eval_bool(chunk, i))
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
