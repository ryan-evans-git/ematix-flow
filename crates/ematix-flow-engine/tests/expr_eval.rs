//! P3 slice 1 gate: the bound-expression evaluator. A general `Expr` tree
//! must narrow a chunk's selection by a boolean predicate and sum a numeric
//! expression over the survivors — the two operations a planned `Filter` and
//! scalar `Aggregate` need, replacing Q6's hand-coded `q6_over_chunks`.
//!
//! Shape mirrors Q6 exactly: `shipdate ∈ [lo,hi) ∧ disc ∈ [dlo,dhi] ∧ qty <
//! 24`, aggregate `sum(extendedprice * discount)`. This proves mixed-type
//! numeric promotion (i32 dates vs f64 measures), the six comparisons, `AND`,
//! and multiply — enough to plan Q6 once the binder exists (slice 2).

use ematix_flow_engine::chunk::DataChunk;
use ematix_flow_engine::expr::{BinaryOp, Expr, ScalarValue, filter_expr, sum_expr_f64};
use ematix_flow_engine::vector::{LogicalType, Vector};

fn col(i: usize) -> Expr {
    Expr::Column(i)
}
fn lit(v: ScalarValue) -> Expr {
    Expr::Literal(v)
}
fn bin(op: BinaryOp, lhs: Expr, rhs: Expr) -> Expr {
    Expr::Binary {
        op,
        lhs: Box::new(lhs),
        rhs: Box::new(rhs),
    }
}
fn and(lhs: Expr, rhs: Expr) -> Expr {
    bin(BinaryOp::And, lhs, rhs)
}

/// A Q6-shaped chunk. Columns: 0=shipdate(Date32), 1=discount(F64),
/// 2=extendedprice(F64), 3=quantity(F64). Rows 0,1,2 satisfy the Q6
/// predicate; row 3 fails the shipdate upper bound (at-hi, exclusive); row 4
/// fails the shipdate lower bound.
fn q6_chunk() -> DataChunk {
    DataChunk::new(vec![
        Vector::i32(vec![8766, 9000, 9130, 9131, 8765], LogicalType::Date32),
        Vector::f64(vec![0.05, 0.06, 0.07, 0.06, 0.06]),
        Vector::f64(vec![100.0, 200.0, 300.0, 400.0, 500.0]),
        Vector::f64(vec![10.0, 23.0, 20.0, 5.0, 5.0]),
    ])
}

#[test]
fn evaluates_q6_predicate_and_aggregate() {
    use BinaryOp::*;
    use ScalarValue::*;
    let chunk = q6_chunk();

    // shipdate >= 8766 AND shipdate < 9131 AND disc >= 0.05 AND disc <= 0.07
    //   AND qty < 24
    let pred = and(
        and(
            and(
                and(
                    bin(GtEq, col(0), lit(Date32(8766))),
                    bin(Lt, col(0), lit(Date32(9131))),
                ),
                bin(GtEq, col(1), lit(Float64(0.05))),
            ),
            bin(LtEq, col(1), lit(Float64(0.07))),
        ),
        bin(Lt, col(3), lit(Float64(24.0))),
    );

    let sel = filter_expr(&chunk, &pred);
    assert_eq!(sel.len(), 3, "rows 0,1,2 satisfy the Q6 predicate");

    // sum(extendedprice * discount) over the survivors.
    let arg = bin(Mul, col(2), col(1));
    let revenue = sum_expr_f64(&chunk, &sel, &arg);
    let want = 100.0 * 0.05 + 200.0 * 0.06 + 300.0 * 0.07;
    assert!((revenue - want).abs() < 1e-9, "revenue {revenue} != {want}");
}

#[test]
fn integer_and_float_paths_stay_distinct() {
    use BinaryOp::*;
    use ScalarValue::*;
    // One i64 column, one f64 column.
    let chunk = DataChunk::new(vec![Vector::i64(vec![3, 5]), Vector::f64(vec![2.5, 2.5])]);

    // Integer arithmetic stays integer: col0 * 2 = [6, 10].
    let int_mul = Expr::Binary {
        op: Mul,
        lhs: Box::new(Expr::Column(0)),
        rhs: Box::new(Expr::Literal(Int64(2))),
    };
    assert_eq!(int_mul.eval_f64(&chunk, 0), 6.0);
    assert_eq!(int_mul.eval_f64(&chunk, 1), 10.0);

    // Mixed promotes to float: col0(i64) * col1(f64) = [7.5, 12.5].
    let mixed = Expr::Binary {
        op: Mul,
        lhs: Box::new(Expr::Column(0)),
        rhs: Box::new(Expr::Column(1)),
    };
    assert_eq!(mixed.eval_f64(&chunk, 0), 7.5);
    assert_eq!(mixed.eval_f64(&chunk, 1), 12.5);

    // Integer comparison: col0 >= 5 → [false, true].
    let ge = Expr::Binary {
        op: GtEq,
        lhs: Box::new(Expr::Column(0)),
        rhs: Box::new(Expr::Literal(Int64(5))),
    };
    assert!(!ge.eval_bool(&chunk, 0));
    assert!(ge.eval_bool(&chunk, 1));
}
