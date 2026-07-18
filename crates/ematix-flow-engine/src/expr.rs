//! P3 expression layer: a bound, typed scalar/predicate expression IR and a
//! tree-walking interpreter over the engine's [`DataChunk`].
//!
//! This is the general expression capability the engine lacked — until now
//! every query's filter and aggregate arithmetic was hand-coded (Q6's
//! `q6_over_chunks`, Q08's `Q08Agg`). A planned `Filter` narrows the deferred
//! selection with [`filter_expr`]; a scalar `Aggregate` sums a numeric
//! expression with [`sum_expr_f64`] — both driven by an [`Expr`] tree the
//! binder (P3 slice 2) will build from SQL.
//!
//! **Bound** means columns are indices into the decoded chunk, not names —
//! name/type resolution is the binder's job, so evaluation never touches a
//! catalog. **Interpreted-first** (the program's stance): this walks the
//! tree per row. Correct and simple; a vectorized / compiled evaluator is a
//! labelled follow-on, and the hand-coded fast paths stay until the general
//! path is measured against them.

use crate::chunk::{DataChunk, Selection};
use crate::pipeline::filter;
use crate::vector::LogicalType;

/// A literal scalar value. `Decimal` and `Utf8` join once the binder needs
/// them (decimal constant-folding is a slice-2 concern); the evaluator today
/// covers the numeric + boolean + date surface Q6 requires.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ScalarValue {
    Int32(i32),
    Int64(i64),
    Float64(f64),
    /// Days since the Unix epoch — same encoding as [`LogicalType::Date32`].
    Date32(i32),
    Boolean(bool),
}

/// A binary operator: arithmetic, comparison, or logical.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
}

/// A bound expression tree. `Column(i)` reads the chunk's `i`-th column.
#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Column(usize),
    Literal(ScalarValue),
    Binary {
        op: BinaryOp,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
    },
}

/// A value produced during evaluation. Integer-family logical types (`Int32`,
/// `Int64`, `Date32`) collapse to `Int(i64)`, so numeric promotion has one
/// integer case and one float case.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Val {
    Int(i64),
    Float(f64),
    Bool(bool),
}

impl Val {
    #[inline]
    fn as_f64(self) -> f64 {
        match self {
            Val::Int(i) => i as f64,
            Val::Float(f) => f,
            Val::Bool(b) => i64::from(b) as f64,
        }
    }

    #[inline]
    fn expect_bool(self) -> bool {
        match self {
            Val::Bool(b) => b,
            other => panic!("expected a boolean operand, got {other:?}"),
        }
    }
}

impl Expr {
    /// Evaluate this expression at `row` of `chunk`.
    #[inline]
    fn eval(&self, chunk: &DataChunk, row: usize) -> Val {
        match self {
            Expr::Column(i) => {
                let v = chunk.col(*i);
                match v.logical {
                    LogicalType::Int32 | LogicalType::Date32 => Val::Int(v.as_i32()[row] as i64),
                    LogicalType::Int64 => Val::Int(v.as_i64()[row]),
                    LogicalType::Float64 => Val::Float(v.as_f64()[row]),
                    LogicalType::Utf8 => {
                        panic!("string columns are not yet supported in expr evaluation")
                    }
                }
            }
            Expr::Literal(s) => match *s {
                ScalarValue::Int32(x) => Val::Int(x as i64),
                ScalarValue::Int64(x) => Val::Int(x),
                ScalarValue::Date32(x) => Val::Int(x as i64),
                ScalarValue::Float64(x) => Val::Float(x),
                ScalarValue::Boolean(b) => Val::Bool(b),
            },
            Expr::Binary { op, lhs, rhs } => {
                let l = lhs.eval(chunk, row);
                let r = rhs.eval(chunk, row);
                eval_binary(*op, l, r)
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
}

#[inline]
fn eval_binary(op: BinaryOp, l: Val, r: Val) -> Val {
    use BinaryOp::*;
    match op {
        Add | Sub | Mul => arith(op, l, r),
        Eq | NotEq | Lt | LtEq | Gt | GtEq => Val::Bool(compare(op, l, r)),
        And => Val::Bool(l.expect_bool() && r.expect_bool()),
        Or => Val::Bool(l.expect_bool() || r.expect_bool()),
    }
}

/// Arithmetic with numeric promotion: integer stays integer; any float
/// operand promotes the whole operation to `f64`.
#[inline]
fn arith(op: BinaryOp, l: Val, r: Val) -> Val {
    use BinaryOp::*;
    match (l, r) {
        (Val::Int(a), Val::Int(b)) => Val::Int(match op {
            Add => a + b,
            Sub => a - b,
            Mul => a * b,
            _ => unreachable!("non-arithmetic op in arith()"),
        }),
        _ => {
            let (a, b) = (l.as_f64(), r.as_f64());
            Val::Float(match op {
                Add => a + b,
                Sub => a - b,
                Mul => a * b,
                _ => unreachable!("non-arithmetic op in arith()"),
            })
        }
    }
}

/// Comparison with the same promotion rule: both integer ⇒ exact integer
/// compare; otherwise compare as `f64`.
#[inline]
fn compare(op: BinaryOp, l: Val, r: Val) -> bool {
    use BinaryOp::*;
    use std::cmp::Ordering;
    match (l, r) {
        (Val::Int(a), Val::Int(b)) => match op {
            Eq => a == b,
            NotEq => a != b,
            Lt => a < b,
            LtEq => a <= b,
            Gt => a > b,
            GtEq => a >= b,
            _ => unreachable!("non-comparison op in compare()"),
        },
        _ => {
            let (a, b) = (l.as_f64(), r.as_f64());
            match op {
                Eq => a == b,
                NotEq => a != b,
                // NaN-free in practice; partial_cmp keeps it total-order-safe.
                _ => match a.partial_cmp(&b) {
                    Some(Ordering::Less) => matches!(op, Lt | LtEq),
                    Some(Ordering::Equal) => matches!(op, LtEq | GtEq),
                    Some(Ordering::Greater) => matches!(op, Gt | GtEq),
                    None => false,
                },
            }
        }
    }
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
        acc += arg.eval_f64(chunk, i as usize);
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
}
