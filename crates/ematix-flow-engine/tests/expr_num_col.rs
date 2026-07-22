//! Gate the vectorized numeric-column evaluator (`eval_num_col`): the
//! grouped/scalar aggregate path precomputes a compound agg argument as a
//! whole typed column once per chunk instead of walking the recursive
//! interpreter per row (q4 sf10: `sum(((a-b-c)+d)/2)` over 3.26M rows ×
//! 6 CTEs — Expr::eval was the hottest engine symbol).
//!
//! The producer must be BIT-IDENTICAL to `eval_opt_f64` per row: same
//! Int-vs-Float promotion (integer arithmetic stays integral until
//! consumed — a large-i64 sum converted before vs after diverges), same
//! NULL propagation (any NULL operand ⇒ NULL result). Checked element by
//! element, value bits and null-ness, over ints / floats / mixed with
//! NULL-bearing columns.

use ematix_flow_engine::chunk::DataChunk;
use ematix_flow_engine::expr::{BinaryOp, Expr, ScalarValue, eval_num_col};
use ematix_flow_engine::vector::{LogicalType, Vector};

fn bin(op: BinaryOp, l: Expr, r: Expr) -> Expr {
    Expr::Binary {
        op,
        lhs: Box::new(l),
        rhs: Box::new(r),
    }
}
fn col(i: usize) -> Expr {
    Expr::Column(i)
}

/// 0: f64+NULLs  1: f64  2: i64+NULLs  3: i64  4: i32  5: utf8 (non-numeric)
fn chunk(n: usize) -> DataChunk {
    let f0: Vec<f64> = (0..n).map(|i| i as f64 * 1.5 - 20.0).collect();
    let f1: Vec<f64> = (0..n).map(|i| (i as f64).mul_add(0.3, 2.0)).collect();
    let i2: Vec<i64> = (0..n as i64).map(|i| i * 3 - 7).collect();
    let i3: Vec<i64> = (0..n as i64).map(|i| (i % 13) - 6).collect();
    let i4: Vec<i32> = (0..n as i32).map(|i| i % 5).collect();
    let va: Vec<bool> = (0..n).map(|i| i % 6 != 2).collect();
    let vb: Vec<bool> = (0..n).map(|i| i % 9 != 0).collect();
    let mut off = vec![0u32];
    let mut data = Vec::new();
    for _ in 0..n {
        data.extend_from_slice(b"x");
        off.push(data.len() as u32);
    }
    DataChunk::new(vec![
        Vector::f64(f0).with_validity(Some(va.clone())),
        Vector::f64(f1),
        Vector::i64(i2).with_validity(Some(vb)),
        Vector::i64(i3),
        Vector::i32(i4, LogicalType::Int32),
        Vector::utf8(off, data).with_validity(Some(va)),
    ])
}

/// Read column row `i` as f64 the way the agg consumer does (integer
/// family widens — matching `Val::as_f64`).
fn col_f64(v: &Vector, i: usize) -> f64 {
    match v.logical {
        LogicalType::Float64 => v.as_f64()[i],
        _ => v.as_i64()[i] as f64,
    }
}

fn assert_matches(c: &DataChunk, e: &Expr) {
    let n = c.n_rows();
    let colv = eval_num_col(c, e).expect("expr is vectorizable");
    for i in 0..n {
        let reference = e.eval_opt_f64(c, i);
        let vec_null = colv.validity.as_ref().is_some_and(|m| !m[i]);
        match reference {
            None => assert!(
                vec_null,
                "row {i}: interpreter NULL, vector non-NULL for {e:?}"
            ),
            Some(want) => {
                assert!(
                    !vec_null,
                    "row {i}: interpreter {want}, vector NULL for {e:?}"
                );
                let got = col_f64(&colv, i);
                assert_eq!(
                    got.to_bits(),
                    want.to_bits(),
                    "row {i}: vector {got} != interpreter {want} for {e:?}"
                );
            }
        }
    }
}

#[test]
fn compound_arith_matches_interpreter() {
    let c = chunk(4096);
    // q4's shape: (((a - b - c) + d) / 2), all f64.
    assert_matches(
        &c,
        &bin(
            BinaryOp::Div,
            bin(
                BinaryOp::Add,
                bin(BinaryOp::Sub, bin(BinaryOp::Sub, col(0), col(1)), col(0)),
                col(1),
            ),
            Expr::Literal(ScalarValue::Float64(2.0)),
        ),
    );
    // f64 mul (Q6 shape) — NULL-bearing left operand.
    assert_matches(&c, &bin(BinaryOp::Mul, col(0), col(1)));
    // Integer arithmetic must stay INTEGRAL until consumed.
    assert_matches(&c, &bin(BinaryOp::Sub, col(3), col(4)));
    assert_matches(
        &c,
        &bin(BinaryOp::Add, bin(BinaryOp::Mul, col(2), col(3)), col(4)),
    );
    // Integer Div promotes to f64 (arith rule).
    assert_matches(&c, &bin(BinaryOp::Div, col(3), col(4)));
    // Mixed int/float promotes to f64.
    assert_matches(&c, &bin(BinaryOp::Mul, col(3), col(1)));
    assert_matches(
        &c,
        &bin(BinaryOp::Add, bin(BinaryOp::Sub, col(0), col(2)), col(4)),
    );
    // Literal-only and column-only still evaluate (may or may not
    // vectorize — the caller only uses Some results; when Some, correct).
    if let Some(v) = eval_num_col(&c, &col(0)) {
        for i in 0..c.n_rows() {
            let r = col(0).eval_opt_f64(&c, i);
            let vn = v.validity.as_ref().is_some_and(|m| !m[i]);
            match r {
                None => assert!(vn),
                Some(w) => assert_eq!(col_f64(&v, i).to_bits(), w.to_bits()),
            }
        }
    }
}

#[test]
fn non_numeric_or_unsupported_declines() {
    let c = chunk(64);
    // A string column is not numeric.
    assert!(eval_num_col(&c, &col(5)).is_none());
    // A comparison is boolean, not a numeric producer.
    assert!(eval_num_col(&c, &bin(BinaryOp::Gt, col(0), col(1))).is_none());
    // Arithmetic with a string leaf declines (whole subtree).
    assert!(eval_num_col(&c, &bin(BinaryOp::Add, col(0), col(5))).is_none());
    // CASE is not vectorized here.
    let case = Expr::Case {
        whens: vec![(bin(BinaryOp::Gt, col(0), col(1)), col(0))],
        else_: Box::new(col(1)),
    };
    assert!(eval_num_col(&c, &case).is_none());
}
