//! Gate the column-at-a-time filter fast path: `filter_expr` builds typed
//! boolean masks for the supported predicate algebra (And/Or, comparisons
//! against literals and columns, IS NULL, IN sets, LIKE) instead of
//! walking the recursive `Val` interpreter per row (q28 sf10: Expr::eval
//! was 20x the next-hottest symbol). The mask path must be
//! ROW-EQUIVALENT to the interpreter — every predicate here is checked
//! against a per-row `eval_bool` reference on the same chunk, including
//! NULL rows (validity), mixed int widths, int/float promotion, string
//! ordering, negated IN/LIKE, and a narrowed pre-selection.

use std::collections::HashSet;
use std::sync::Arc;

use ematix_flow_engine::chunk::{DataChunk, Selection};
use ematix_flow_engine::expr::{BinaryOp, Expr, ScalarValue, filter_expr};
use ematix_flow_engine::pipeline::filter;
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
fn int(v: i64) -> Expr {
    Expr::Literal(ScalarValue::Int64(v))
}
fn fl(v: f64) -> Expr {
    Expr::Literal(ScalarValue::Float64(v))
}
fn s(v: &str) -> Expr {
    Expr::Literal(ScalarValue::Utf8(v.into()))
}

/// A chunk wide enough to exercise every leaf kernel:
///   0: i64 (with NULLs)   1: i32    2: f64 (with NULLs)   3: utf8
///   4: i64 dense          5: f64    6: utf8 (with NULLs)
fn chunk(n: usize) -> DataChunk {
    let i64s: Vec<i64> = (0..n as i64).map(|i| (i * 7) % 101).collect();
    let i32s: Vec<i32> = (0..n as i32).map(|i| (i * 13) % 51 - 25).collect();
    let f64s: Vec<f64> = (0..n).map(|i| (i as f64) * 0.75 - 30.0).collect();
    let valid_a: Vec<bool> = (0..n).map(|i| i % 7 != 3).collect();
    let valid_b: Vec<bool> = (0..n).map(|i| i % 11 != 0).collect();
    let words = ["apple", "banana", "cherry", "date", "elderberry", ""];
    let mut offsets = vec![0u32];
    let mut data = Vec::new();
    for i in 0..n {
        data.extend_from_slice(words[i % words.len()].as_bytes());
        offsets.push(data.len() as u32);
    }
    let utf8 = Vector::utf8(offsets.clone(), data.clone());
    DataChunk::new(vec![
        Vector::i64(i64s.clone()).with_validity(Some(valid_a.clone())),
        Vector::i32(i32s, LogicalType::Int32),
        Vector::f64(f64s.clone()).with_validity(Some(valid_b.clone())),
        utf8.clone(),
        Vector::i64(i64s),
        Vector::f64(f64s),
        utf8.with_validity(Some(valid_a)),
    ])
}

fn assert_equiv(c: &DataChunk, pred: &Expr) {
    let fast = filter_expr(c, pred);
    let reference = filter(c, |i| pred.eval_bool(c, i));
    let to_vec = |sel: &Selection| {
        let mut v = Vec::new();
        sel.for_each(|i| v.push(i));
        v
    };
    assert_eq!(
        to_vec(&fast),
        to_vec(&reference),
        "mask path diverged from interpreter for {pred:?}"
    );
}

#[test]
fn leaf_comparisons_match_interpreter() {
    let c = chunk(4096);
    for op in [
        BinaryOp::Eq,
        BinaryOp::NotEq,
        BinaryOp::Lt,
        BinaryOp::LtEq,
        BinaryOp::Gt,
        BinaryOp::GtEq,
    ] {
        assert_equiv(&c, &bin(op, col(0), int(50))); // i64+NULLs vs int
        assert_equiv(&c, &bin(op, int(50), col(0))); // flipped
        assert_equiv(&c, &bin(op, col(1), int(0))); // i32 vs int
        assert_equiv(&c, &bin(op, col(0), fl(49.5))); // int col vs float lit
        assert_equiv(&c, &bin(op, col(2), fl(3.75))); // f64+NULLs vs float
        assert_equiv(&c, &bin(op, col(2), int(4))); // float col vs int lit
        assert_equiv(&c, &bin(op, col(3), s("cherry"))); // utf8 vs lit
        assert_equiv(&c, &bin(op, col(6), s("date"))); // utf8+NULLs vs lit
        assert_equiv(&c, &bin(op, col(0), col(4))); // i64 vs i64
        assert_equiv(&c, &bin(op, col(1), col(0))); // i32 vs i64+NULLs
        assert_equiv(&c, &bin(op, col(2), col(5))); // f64 vs f64
        assert_equiv(&c, &bin(op, col(0), col(5))); // int vs float col
        assert_equiv(&c, &bin(op, col(3), col(6))); // utf8 vs utf8+NULLs
    }
}

#[test]
fn boolean_algebra_and_special_leaves_match_interpreter() {
    let c = chunk(4096);
    let between = |e: Expr, lo: i64, hi: i64| {
        bin(
            BinaryOp::And,
            bin(BinaryOp::GtEq, e.clone(), int(lo)),
            bin(BinaryOp::LtEq, e, int(hi)),
        )
    };
    // q28's OR-of-BETWEENs shape.
    assert_equiv(
        &c,
        &bin(
            BinaryOp::And,
            between(col(0), 0, 5),
            bin(
                BinaryOp::Or,
                bin(
                    BinaryOp::Or,
                    between(col(1), -10, 10),
                    between(col(4), 90, 100),
                ),
                bin(BinaryOp::Gt, col(2), fl(0.0)),
            ),
        ),
    );
    for negated in [false, true] {
        assert_equiv(
            &c,
            &Expr::IsNull {
                expr: Box::new(col(0)),
                negated,
            },
        );
        assert_equiv(
            &c,
            &Expr::IsNull {
                expr: Box::new(col(4)),
                negated,
            },
        );
        let set: HashSet<i64> = [3, 14, 15, 92, 65].into_iter().collect();
        assert_equiv(
            &c,
            &Expr::InSet {
                expr: Box::new(col(0)),
                set: Arc::new(set),
                negated,
            },
        );
        let sset: HashSet<Box<str>> = ["banana".into(), "date".into()].into_iter().collect();
        assert_equiv(
            &c,
            &Expr::InSetStr {
                expr: Box::new(col(6)),
                set: Arc::new(sset),
                negated,
            },
        );
        assert_equiv(
            &c,
            &Expr::Like {
                expr: Box::new(col(3)),
                pattern: "%err%".into(),
                negated,
                ci: false,
            },
        );
    }
    // A non-maskable side (CASE) under And/Or exercises the hybrid path.
    let case = Expr::Case {
        whens: vec![(
            bin(BinaryOp::Lt, col(1), int(0)),
            Expr::Literal(ScalarValue::Boolean(true)),
        )],
        else_: Box::new(Expr::Literal(ScalarValue::Boolean(false))),
    };
    assert_equiv(
        &c,
        &bin(
            BinaryOp::And,
            bin(BinaryOp::Gt, col(0), int(20)),
            case.clone(),
        ),
    );
    assert_equiv(
        &c,
        &bin(
            BinaryOp::And,
            case.clone(),
            bin(BinaryOp::Gt, col(0), int(20)),
        ),
    );
    assert_equiv(
        &c,
        &bin(
            BinaryOp::Or,
            bin(BinaryOp::Gt, col(0), int(80)),
            case.clone(),
        ),
    );
    assert_equiv(
        &c,
        &bin(BinaryOp::Or, case, bin(BinaryOp::Gt, col(0), int(80))),
    );
}

#[test]
fn narrowed_selection_is_respected() {
    let mut c = chunk(2048);
    let keep: Vec<u32> = (0..2048u32).filter(|i| i % 3 == 0).collect();
    c.sel = Selection::Indices(keep);
    assert_equiv(&c, &bin(BinaryOp::Gt, col(0), int(30)));
    // Very sparse selection (mask path should defer, result identical).
    let mut c2 = chunk(2048);
    c2.sel = Selection::Indices(vec![5, 900, 1999]);
    assert_equiv(&c2, &bin(BinaryOp::Lt, col(2), fl(100.0)));
}
