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
use crate::vector::{LogicalType, Vector};

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
    /// `EXTRACT(<field> FROM <date expr>)` — Date32 days → a calendar field
    /// (year/month/day/quarter/dow/doy/week), matching DuckDB's `extract`.
    Extract {
        field: DateField,
        arg: Box<Expr>,
    },
    /// `date_trunc('<unit>', <date expr>)` — Date32 truncated down to the
    /// start of its year/quarter/month/week/day (a Date32 result).
    DateTrunc {
        unit: DateTruncUnit,
        arg: Box<Expr>,
    },
    /// `NOT <bool expr>` — three-valued: `NOT TRUE`=FALSE, `NOT FALSE`=TRUE,
    /// `NOT NULL`=NULL. Stays on the per-row path (no typed mask kernel), so
    /// a filter containing it falls back to `eval_bool` (NULL→drop) — correct
    /// because the inner AND/OR/compare are all three-valued.
    Not(Box<Expr>),
    /// `CAST(<expr> AS INT/INTEGER/BIGINT/SMALLINT)` on a fractional value —
    /// rounds to the nearest integer (DuckDB `CAST` semantics: `10714.82` →
    /// `10715`). An already-integer operand is unchanged.
    CastInt(Box<Expr>),
    /// `CAST(<expr> AS VARCHAR/CHAR/TEXT/STRING)` — renders the operand to
    /// text: integers/floats as their decimal form, a string operand
    /// unchanged. `from_date` is set by the binder when the operand is
    /// Date32-typed (the value path represents a date as its raw day-number
    /// `Val::Int`, which is indistinguishable from an integer at eval time),
    /// so the string is rendered ISO `YYYY-MM-DD`. Like [`Expr::Upper`] it is
    /// an OWNED string, so it evaluates via `eval_value` and comparisons
    /// containing it are handled inline in the `Binary` arm.
    CastStr { arg: Box<Expr>, from_date: bool },
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
    /// A numeric-returning scalar builtin (`floor`/`ceil`/`mod`/`length`) —
    /// flows through the borrowed `Val` path like arithmetic.
    NumFn {
        func: NumFn,
        args: Vec<Expr>,
    },
    /// A string-returning scalar builtin (`lower`/`trim`/`replace`) — yields
    /// an OWNED string, so like [`Expr::Upper`] it evaluates via `eval_value`
    /// and comparisons containing it are handled inline in the `Binary` arm.
    StrFn {
        func: StrFn,
        args: Vec<Expr>,
    },
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
        /// Case-insensitive (`ILIKE`) — matches ASCII case-insensitively.
        ci: bool,
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

/// A `COUNT(DISTINCT …)` key: a numeric key (int / float-bits / bool) on the
/// fast path, or a string — borrowed from the chunk for a bare column, owned
/// for a string-valued expression (`trim(x)`, `x || y`, …).
pub enum DistinctKey<'a> {
    Num(i64),
    Str(std::borrow::Cow<'a, str>),
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
            Expr::Extract { arg: e, .. }
            | Expr::DateTrunc { arg: e, .. }
            | Expr::CastInt(e)
            | Expr::Not(e)
            | Expr::Upper(e)
            | Expr::CastStr { arg: e, .. } => e.for_each_col(f),
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
            Expr::Concat(es) | Expr::NumFn { args: es, .. } | Expr::StrFn { args: es, .. } => {
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
                if is_owned_str_fn(lhs) || is_owned_str_fn(rhs) {
                    let side = |e: &Expr| -> Option<String> {
                        if is_owned_str_fn(e) {
                            match e.eval_value(chunk, row) {
                                ScalarValue::Utf8(s) => Some(s.to_string()),
                                ScalarValue::Null => None,
                                other => panic!("string fn needs a string, got {other:?}"),
                            }
                        } else {
                            match e.eval(chunk, row) {
                                Val::Str(s) => Some(s.to_string()),
                                Val::Null => None,
                                other => panic!("string comparison got {other:?}"),
                            }
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
            Expr::Extract { field, arg } => match arg.eval(chunk, row) {
                Val::Int(days) => Val::Int(extract_field(*field, days as i32)),
                Val::Null => Val::Null,
                other => panic!("EXTRACT needs a date operand, got {other:?}"),
            },
            // Borrowed path yields the day-number (comparisons vs date
            // literals, which are also day-numbers); `eval_value` types it
            // back to Date32 for projection/group-key output.
            Expr::DateTrunc { unit, arg } => match arg.eval(chunk, row) {
                Val::Int(days) => Val::Int(trunc_days(*unit, days as i32) as i64),
                Val::Null => Val::Null,
                other => panic!("date_trunc needs a date operand, got {other:?}"),
            },
            Expr::Not(e) => match e.eval(chunk, row) {
                Val::Bool(b) => Val::Bool(!b),
                Val::Null => Val::Null,
                other => panic!("NOT needs a boolean operand, got {other:?}"),
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
            Expr::Upper(_) | Expr::StrFn { .. } | Expr::CastStr { .. } => {
                panic!("string fn must be evaluated via eval_value or inside a comparison")
            }
            Expr::NumFn { func, args } => eval_num_fn(*func, args, chunk, row),
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
                ci,
            } => match expr.eval(chunk, row) {
                Val::Str(s) => {
                    Val::Bool(like_match(s.as_bytes(), pattern.as_bytes(), *ci) != *negated)
                }
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
        if let Expr::StrFn { func, args } = self {
            return eval_str_fn(*func, args, chunk, row);
        }
        // CAST(x AS VARCHAR) — render the operand to text. A string operand
        // is unchanged; numerics take their decimal form; a Date32 renders
        // ISO `YYYY-MM-DD` (not the raw day-number).
        if let Expr::CastStr { arg, from_date } = self {
            let render_date = |days: i32| {
                let (y, m, day) = civil_of_days(days);
                format!("{y:04}-{m:02}-{day:02}")
            };
            let s = match arg.eval_value(chunk, row) {
                ScalarValue::Null => return ScalarValue::Null,
                ScalarValue::Utf8(s) => return ScalarValue::Utf8(s),
                // A Date32 operand flows as an Int on the value path; render
                // ISO iff the binder tagged it as a date.
                ScalarValue::Int32(i) if *from_date => render_date(i),
                ScalarValue::Int64(i) if *from_date => render_date(i as i32),
                ScalarValue::Int32(i) => i.to_string(),
                ScalarValue::Int64(i) => i.to_string(),
                ScalarValue::Float64(f) => f.to_string(),
                ScalarValue::Boolean(b) => if b { "true" } else { "false" }.to_string(),
                ScalarValue::Date32(d) => render_date(d),
            };
            return ScalarValue::Utf8(Arc::from(s.as_str()));
        }
        // date_trunc yields a Date32 (not a bare day-number) in value space.
        if let Expr::DateTrunc { unit, arg } = self {
            return match arg.eval(chunk, row) {
                Val::Int(days) => ScalarValue::Date32(trunc_days(*unit, days as i32)),
                Val::Null => ScalarValue::Null,
                other => panic!("date_trunc needs a date operand, got {other:?}"),
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

    /// Evaluate to a COUNT(DISTINCT …) key, or `None` on SQL NULL. Numeric
    /// keys stay on the borrowed-`Val` fast path (integers as themselves,
    /// floats by bit pattern with `-0.0` normalized to `0.0` — q28's
    /// `count(DISTINCT ss_list_price)` over a decimal column); strings borrow
    /// the chunk's `&str` (the caller interns them into a separate set), so
    /// `count(DISTINCT <string>)` is exact.
    #[inline]
    pub fn eval_opt_distinct<'a>(
        &'a self,
        chunk: &'a DataChunk,
        row: usize,
    ) -> Option<DistinctKey<'a>> {
        // An owned-string expression (trim/lower/upper/replace/concat) can't
        // take the borrowed `Val` path — evaluate it to an owned string.
        if is_owned_str_fn(self) {
            return match self.eval_value(chunk, row) {
                ScalarValue::Null => None,
                ScalarValue::Utf8(s) => {
                    Some(DistinctKey::Str(std::borrow::Cow::Owned(s.to_string())))
                }
                other => panic!("COUNT(DISTINCT) string arg produced {other:?}"),
            };
        }
        match self.eval(chunk, row) {
            Val::Null => None,
            Val::Int(i) => Some(DistinctKey::Num(i)),
            Val::Float(f) => Some(DistinctKey::Num(
                (if f == 0.0 { 0.0f64 } else { f }).to_bits() as i64,
            )),
            Val::Bool(b) => Some(DistinctKey::Num(i64::from(b))),
            Val::Str(s) => Some(DistinctKey::Str(std::borrow::Cow::Borrowed(s))),
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
    // AND/OR are THREE-VALUED (so a NOT over them is correct): AND is FALSE
    // if either side is FALSE (even when the other is NULL), TRUE only if
    // both TRUE, else NULL; OR is the dual. Every boolean consumer collapses
    // NULL→false via `expect_bool`, so filter/CASE behavior is unchanged —
    // only a projected boolean value is now correctly NULL instead of false.
    if matches!(op, And | Or) {
        let b = |v: Val| match v {
            Val::Bool(x) => Some(x),
            Val::Null => None,
            other => panic!("expected a boolean operand, got {other:?}"),
        };
        let (lb, rb) = (b(l), b(r));
        return match op {
            And => match (lb, rb) {
                (Some(false), _) | (_, Some(false)) => Val::Bool(false),
                (Some(true), Some(true)) => Val::Bool(true),
                _ => Val::Null,
            },
            _ => match (lb, rb) {
                (Some(true), _) | (_, Some(true)) => Val::Bool(true),
                (Some(false), Some(false)) => Val::Bool(false),
                _ => Val::Null,
            },
        };
    }
    // Arithmetic and comparison with a NULL operand yield NULL (a
    // comparison's UNKNOWN — `expect_bool` maps it to not-satisfied).
    if matches!(l, Val::Null) || matches!(r, Val::Null) {
        return Val::Null;
    }
    match op {
        Add | Sub | Mul | Div => arith(op, l, r),
        Eq | NotEq | Lt | LtEq | Gt | GtEq => Val::Bool(compare(op, l, r)),
        And | Or => unreachable!("handled above"),
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
fn like_match(s: &[u8], p: &[u8], ci: bool) -> bool {
    let eq = |a: u8, b: u8| {
        if ci {
            a.eq_ignore_ascii_case(&b)
        } else {
            a == b
        }
    };
    let (mut si, mut pi) = (0usize, 0usize);
    let (mut star_p, mut star_s) = (usize::MAX, 0usize);
    while si < s.len() {
        if pi < p.len() && (p[pi] == b'_' || eq(p[pi], s[si])) {
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

/// Numeric-returning scalar builtins.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NumFn {
    Floor,
    Ceil,
    /// `mod(a, b)` — truncated remainder (SQL `%`), integer or float.
    Mod,
    /// `length(s)` — character count (bytes for ASCII).
    Length,
    /// `sqrt(x)`, `ln(x)`, `exp(x)`, `sign(x)`, `trunc(x)` — single-arg f64.
    Sqrt,
    Ln,
    Exp,
    Sign,
    Trunc,
    /// `power(x, y)` / `pow(x, y)` — `x` raised to `y` (f64).
    Power,
    /// `greatest(…)` / `least(…)` — the max / min of the (numeric) arguments,
    /// skipping NULL (all-NULL → NULL, matching DuckDB).
    Greatest,
    Least,
}

/// String-returning scalar builtins.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StrFn {
    Lower,
    /// `trim(s)` — strip leading/trailing ASCII whitespace.
    Trim,
    /// `replace(s, from, to)` — replace every occurrence.
    Replace,
}

/// A calendar field pulled out of a Date32 by `EXTRACT` (DuckDB semantics).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DateField {
    Year,
    Month,
    Day,
    Quarter,
    /// Day of week, 0 = Sunday … 6 = Saturday (DuckDB `dow`).
    Dow,
    /// ISO day of week, 1 = Monday … 7 = Sunday (DuckDB `isodow`).
    IsoDow,
    /// Day of year, 1-based (DuckDB `doy`).
    Doy,
    /// ISO-8601 week number, 1..=53 (DuckDB `week`).
    Week,
    /// Seconds since the Unix epoch (DuckDB `epoch`) — a date at UTC
    /// midnight, so `days * 86400`.
    Epoch,
}

/// `date_trunc` granularity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DateTruncUnit {
    Year,
    Quarter,
    Month,
    Week,
    Day,
}

/// Truncate a Date32 (days since epoch) down to the start of `unit` (ISO
/// week = Monday), matching DuckDB's `date_trunc`.
fn trunc_days(unit: DateTruncUnit, days: i32) -> i32 {
    let (y, m, _) = civil_of_days(days);
    match unit {
        DateTruncUnit::Day => days,
        DateTruncUnit::Year => days_from_civil(y, 1, 1),
        DateTruncUnit::Month => days_from_civil(y, m, 1),
        DateTruncUnit::Quarter => days_from_civil(y, (m - 1) / 3 * 3 + 1, 1),
        DateTruncUnit::Week => {
            let iso_dow = {
                let dow = (days as i64 + 4).rem_euclid(7);
                if dow == 0 { 7 } else { dow }
            };
            days - (iso_dow as i32 - 1)
        }
    }
}

/// Days since the Unix epoch → proleptic-Gregorian `(year, month, day)` (the
/// inverse of the binder's `days_from_civil`).
fn civil_of_days(days: i32) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i32 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn year_of_days(days: i32) -> i32 {
    civil_of_days(days).0
}

/// 1-based day-of-year for a Date32.
fn day_of_year(days: i32) -> i32 {
    let (y, _, _) = civil_of_days(days);
    days - days_from_civil(y, 1, 1) + 1
}

/// Days since the Unix epoch for a calendar date (Howard Hinnant's algorithm;
/// mirrors the binder's own `days_from_civil`).
fn days_from_civil(y: i32, m: u32, d: u32) -> i32 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe as i32 - 719_468
}

/// ISO weeks in a year: 53 if it starts on Thursday or is a leap year
/// starting on Wednesday, else 52 (via the p(y) parity of Dec 31).
fn iso_weeks_in_year(y: i32) -> i32 {
    let p = |y: i32| (y + y / 4 - y / 100 + y / 400).rem_euclid(7);
    if p(y) == 4 || p(y - 1) == 3 { 53 } else { 52 }
}

/// Extract `field` from a Date32 (days since epoch), matching DuckDB.
fn extract_field(field: DateField, days: i32) -> i64 {
    let dow = (days as i64 + 4).rem_euclid(7); // epoch (day 0) is a Thursday
    let iso_dow = if dow == 0 { 7 } else { dow }; // Mon=1..Sun=7
    let v: i64 = match field {
        DateField::Year => year_of_days(days) as i64,
        DateField::Month => civil_of_days(days).1 as i64,
        DateField::Day => civil_of_days(days).2 as i64,
        DateField::Quarter => ((civil_of_days(days).1 - 1) / 3 + 1) as i64,
        DateField::Dow => dow,
        DateField::IsoDow => iso_dow,
        DateField::Doy => day_of_year(days) as i64,
        DateField::Week => {
            let (y, _, _) = civil_of_days(days);
            let doy = day_of_year(days) as i64;
            let week = (doy - iso_dow + 10) / 7;
            if week < 1 {
                iso_weeks_in_year(y - 1) as i64
            } else if week > iso_weeks_in_year(y) as i64 {
                1
            } else {
                week
            }
        }
        DateField::Epoch => days as i64 * 86_400,
    };
    v
}

/// Does `e` produce an OWNED string that only `eval_value` can build (so a
/// comparison containing it must route through the inline string path)?
fn is_owned_str_fn(e: &Expr) -> bool {
    matches!(
        e,
        Expr::Upper(_) | Expr::StrFn { .. } | Expr::Concat(_) | Expr::CastStr { .. }
    )
}

/// Evaluate a numeric-returning scalar builtin on the borrowed `Val` path.
fn eval_num_fn<'a>(func: NumFn, args: &'a [Expr], chunk: &'a DataChunk, row: usize) -> Val<'a> {
    match func {
        NumFn::Floor | NumFn::Ceil => match args[0].eval(chunk, row) {
            Val::Int(i) => Val::Int(i),
            Val::Float(f) => Val::Float(if matches!(func, NumFn::Floor) {
                f.floor()
            } else {
                f.ceil()
            }),
            Val::Null => Val::Null,
            other => panic!("{func:?} needs a numeric operand, got {other:?}"),
        },
        NumFn::Mod => match (args[0].eval(chunk, row), args[1].eval(chunk, row)) {
            (Val::Null, _) | (_, Val::Null) => Val::Null,
            (Val::Int(a), Val::Int(b)) => {
                if b == 0 {
                    Val::Null
                } else {
                    Val::Int(a % b)
                }
            }
            (a, b) => {
                let (a, b) = (a.as_f64(), b.as_f64());
                if b == 0.0 {
                    Val::Null
                } else {
                    Val::Float(a % b)
                }
            }
        },
        NumFn::Length => match args[0].eval(chunk, row) {
            Val::Str(s) => Val::Int(s.chars().count() as i64),
            Val::Null => Val::Null,
            other => panic!("length needs a string, got {other:?}"),
        },
        NumFn::Sqrt | NumFn::Ln | NumFn::Exp | NumFn::Sign | NumFn::Trunc => {
            match args[0].eval(chunk, row) {
                Val::Null => Val::Null,
                v => {
                    let x = v.as_f64();
                    Val::Float(match func {
                        NumFn::Sqrt => x.sqrt(),
                        NumFn::Ln => x.ln(),
                        NumFn::Exp => x.exp(),
                        NumFn::Sign => x.signum() * f64::from(x != 0.0),
                        NumFn::Trunc => x.trunc(),
                        _ => unreachable!(),
                    })
                }
            }
        }
        NumFn::Power => match (args[0].eval(chunk, row), args[1].eval(chunk, row)) {
            (Val::Null, _) | (_, Val::Null) => Val::Null,
            (a, b) => Val::Float(a.as_f64().powf(b.as_f64())),
        },
        NumFn::Greatest | NumFn::Least => {
            let want_max = matches!(func, NumFn::Greatest);
            let mut acc: Option<f64> = None;
            for a in args {
                if let v @ (Val::Int(_) | Val::Float(_) | Val::Bool(_)) = a.eval(chunk, row) {
                    let x = v.as_f64();
                    acc = Some(match acc {
                        None => x,
                        Some(cur) if want_max => cur.max(x),
                        Some(cur) => cur.min(x),
                    });
                }
            }
            match acc {
                Some(x) => Val::Float(x),
                None => Val::Null,
            }
        }
    }
}

/// Evaluate a string-returning scalar builtin, producing an owned value.
fn eval_str_fn(func: StrFn, args: &[Expr], chunk: &DataChunk, row: usize) -> ScalarValue {
    let s0 = || match args[0].eval_value(chunk, row) {
        ScalarValue::Utf8(s) => Some(s),
        ScalarValue::Null => None,
        other => panic!("{func:?} needs a string, got {other:?}"),
    };
    match func {
        StrFn::Lower => match s0() {
            Some(s) => ScalarValue::Utf8(Arc::from(s.to_lowercase().as_str())),
            None => ScalarValue::Null,
        },
        StrFn::Trim => match s0() {
            Some(s) => ScalarValue::Utf8(Arc::from(s.trim())),
            None => ScalarValue::Null,
        },
        StrFn::Replace => {
            let (Some(s), from, to) = (
                s0(),
                args[1].eval_value(chunk, row),
                args[2].eval_value(chunk, row),
            ) else {
                return ScalarValue::Null;
            };
            let (ScalarValue::Utf8(from), ScalarValue::Utf8(to)) = (from, to) else {
                return ScalarValue::Null;
            };
            ScalarValue::Utf8(Arc::from(s.replace(&*from, &to).as_str()))
        }
    }
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
            ci,
        } => {
            let Expr::Column(c) = expr.as_ref() else {
                return None;
            };
            let v = chunk.col(*c);
            if v.len() != n || v.logical != LogicalType::Utf8 {
                return None;
            }
            let view = v.as_utf8();
            let (pat, neg, ci) = (pattern.as_bytes(), *negated, *ci);
            Some(mask_valid(v.validity.as_deref(), n, |i| {
                like_match(view.get(i).as_bytes(), pat, ci) != neg
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

/// Evaluate a numeric expression to a whole typed column (`Int64` or
/// `Float64`, with validity), or `None` if it is not a vectorizable
/// numeric producer (a string/boolean leaf, CASE, LIKE, a bare
/// Column/Literal — those stay on the cheap per-row path). This is the
/// aggregate-argument analog of the [`filter_expr`] mask: the grouped and
/// scalar agg loops precompute a COMPOUND arg once per chunk instead of
/// walking the interpreter per row (q4's `sum(((a-b-c)+d)/2)`).
///
/// BIT-IDENTICAL to per-row [`Expr::eval_opt_f64`]: integer arithmetic
/// stays in `i64` until consumed (a large sum converted before vs after
/// diverges), any float operand promotes the node to `f64`, `Div` is
/// always `f64`, and a NULL operand propagates (validity ANDs). Only
/// `Column`, `Literal`, and `Binary(Add|Sub|Mul|Div)` participate.
pub fn eval_num_col(chunk: &DataChunk, e: &Expr) -> Option<Vector> {
    // Bare leaves aren't worth a column materialization — the per-row
    // path reads them directly. Compound expressions rooted at a Binary
    // recurse into leaves here (so a leaf CALL still resolves).
    match e {
        Expr::Binary { op, lhs, rhs }
            if matches!(
                op,
                BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div
            ) =>
        {
            let l = num_col_leaf(chunk, lhs)?;
            let r = num_col_leaf(chunk, rhs)?;
            Some(num_binary(*op, &l, &r))
        }
        _ => None,
    }
}

/// Recurse into a numeric node (leaf or nested arithmetic), returning its
/// typed column. Unlike [`eval_num_col`], a bare Column/Literal DOES
/// resolve here — it is a child of some arithmetic node.
fn num_col_leaf(chunk: &DataChunk, e: &Expr) -> Option<Vector> {
    match e {
        Expr::Column(i) => {
            let v = chunk.col(*i);
            match v.logical {
                LogicalType::Int64 => Some(v.clone()),
                LogicalType::Int32 | LogicalType::Date32 => {
                    let out = Vector::i64(v.as_i32().iter().map(|&x| x as i64).collect());
                    Some(out.with_validity(v.validity.as_ref().map(|m| m.to_vec())))
                }
                LogicalType::Float64 => Some(v.clone()),
                LogicalType::Utf8 => None,
            }
        }
        Expr::Literal(s) => match s {
            ScalarValue::Int64(x) => Some(Vector::i64(vec![*x])),
            ScalarValue::Int32(x) => Some(Vector::i64(vec![*x as i64])),
            ScalarValue::Date32(x) => Some(Vector::i64(vec![*x as i64])),
            ScalarValue::Float64(x) => Some(Vector::f64(vec![*x])),
            // A broadcast literal is length-1; num_binary broadcasts it.
            _ => None,
        },
        Expr::Binary { op, lhs, rhs }
            if matches!(
                op,
                BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div
            ) =>
        {
            let l = num_col_leaf(chunk, lhs)?;
            let r = num_col_leaf(chunk, rhs)?;
            Some(num_binary(*op, &l, &r))
        }
        _ => None,
    }
}

/// Element-wise arithmetic over two typed columns (length-1 = broadcast),
/// mirroring [`arith`]: both integer ⇒ integer result (except `Div`);
/// any float ⇒ f64; validity ANDs.
fn num_binary(op: BinaryOp, l: &Vector, r: &Vector) -> Vector {
    use BinaryOp::*;
    let n = l.len().max(r.len());
    let at_valid = |v: &Vector, i: usize| -> bool {
        let idx = if v.len() == 1 { 0 } else { i };
        v.validity.as_ref().is_none_or(|m| m[idx])
    };
    let valid: Option<Vec<bool>> = if l.validity.is_some() || r.validity.is_some() {
        Some((0..n).map(|i| at_valid(l, i) && at_valid(r, i)).collect())
    } else {
        None
    };
    let both_int = l.logical != LogicalType::Float64
        && r.logical != LogicalType::Float64
        && !matches!(op, Div);
    let out = if both_int {
        let li = l.as_i64();
        let ri = r.as_i64();
        let get = |s: &[i64], i: usize| s[if s.len() == 1 { 0 } else { i }];
        Vector::i64(
            (0..n)
                .map(|i| {
                    let (a, b) = (get(li, i), get(ri, i));
                    match op {
                        Add => a + b,
                        Sub => a - b,
                        Mul => a * b,
                        _ => unreachable!("Div is not both_int"),
                    }
                })
                .collect(),
        )
    } else {
        let lf = col_as_f64(l);
        let rf = col_as_f64(r);
        let get = |s: &[f64], i: usize| s[if s.len() == 1 { 0 } else { i }];
        Vector::f64(
            (0..n)
                .map(|i| {
                    let (a, b) = (get(&lf, i), get(&rf, i));
                    match op {
                        Add => a + b,
                        Sub => a - b,
                        Mul => a * b,
                        Div => a / b,
                        _ => unreachable!("non-arith op in num_binary"),
                    }
                })
                .collect(),
        )
    };
    out.with_validity(valid)
}

/// A typed numeric column as `f64` (integer family widens — matching the
/// interpreter's `Val::as_f64`).
fn col_as_f64(v: &Vector) -> std::borrow::Cow<'_, [f64]> {
    match v.logical {
        LogicalType::Float64 => std::borrow::Cow::Borrowed(v.as_f64()),
        _ => std::borrow::Cow::Owned(v.as_i64().iter().map(|&x| x as f64).collect()),
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
        assert!(like_match(b"PROMO BURNISHED", b"PROMO%", false));
        assert!(!like_match(b"STANDARD BRUSHED", b"PROMO%", false));
        assert!(like_match(b"abcXdef", b"abc_def", false));
        assert!(like_match(b"special packages", b"%special%", false));
        assert!(like_match(b"", b"%", false));
        assert!(!like_match(b"ab", b"a_c", false));
        assert!(like_match(b"aXbYc", b"a%b%c", false));
    }

    #[test]
    fn extract_year_inverts_days_from_civil() {
        let chunk = DataChunk::new(vec![Vector::i32(
            vec![0, 8766, 9131, 9495, 9496, 9861],
            LogicalType::Date32,
        )]);
        let e = Expr::Extract {
            field: DateField::Year,
            arg: Box::new(Expr::Column(0)),
        };
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
