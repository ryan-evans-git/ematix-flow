//! P3 slice 2 gate: the binder. Q6's SQL text must bind against a catalog
//! into the expected **typed** logical plan — names resolved to (leaf index,
//! type), `BETWEEN` desugared, and the discount bounds **constant-folded in
//! decimal**: `0.06 - 0.01 → 0.05` and `0.06 + 0.01 → 0.07` *exactly*.
//! Folding in f64 yields 0.069999999999999996 — one ULP below the stored
//! 0.07 — and silently drops ~1/3 of Q6's matches (the `lib.rs:62` lesson).
//! This is the binder's first real correctness obligation, and the reason
//! `assert_eq!` on the literal bits is the right gate.

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::expr::{BinaryOp, Expr, ScalarValue};
use ematix_flow_engine::logical::{AggFunc, LogicalPlan};
use ematix_flow_engine::vector::LogicalType;

const Q6_SQL: &str = "select sum(l_extendedprice * l_discount) as revenue \
                      from lineitem \
                      where l_shipdate >= date '1994-01-01' \
                        and l_shipdate < date '1995-01-01' \
                        and l_discount between 0.06 - 0.01 and 0.06 + 0.01 \
                        and l_quantity < 24";

/// A lineitem catalog with the real SF-parquet leaf indices — registered by
/// the harness, never hardcoded in engine code.
fn catalog() -> Catalog {
    let mut c = Catalog::new();
    c.register_table(
        "lineitem",
        "examples/tpch/data/sf1/lineitem.parquet",
        &[
            ("l_quantity", 4, LogicalType::Float64),
            ("l_extendedprice", 5, LogicalType::Float64),
            ("l_discount", 6, LogicalType::Float64),
            ("l_shipdate", 10, LogicalType::Date32),
        ],
    );
    c
}

/// Walk an expression tree collecting every literal.
fn literals(e: &Expr, out: &mut Vec<ScalarValue>) {
    match e {
        Expr::Literal(s) => out.push(*s),
        Expr::Binary { lhs, rhs, .. } => {
            literals(lhs, out);
            literals(rhs, out);
        }
        Expr::Column(_) => {}
    }
}

/// Count comparisons `Column op Literal` in a predicate tree.
fn comparisons(e: &Expr, out: &mut Vec<(usize, BinaryOp, ScalarValue)>) {
    if let Expr::Binary { op, lhs, rhs } = e {
        if let (Expr::Column(c), Expr::Literal(v)) = (lhs.as_ref(), rhs.as_ref()) {
            out.push((*c, *op, *v));
            return;
        }
        comparisons(lhs, out);
        comparisons(rhs, out);
    }
}

#[test]
fn binds_q6_to_typed_plan_with_decimal_folded_bounds() {
    let plan = bind_sql(Q6_SQL, &catalog()).expect("bind failed");

    // Shape: Aggregate([], [Sum(ep * disc)]) → Filter → Scan.
    let LogicalPlan::Aggregate { input, group, aggs } = &plan else {
        panic!("top must be Aggregate, got {plan:?}");
    };
    assert!(group.is_empty(), "Q6 has no GROUP BY");
    assert_eq!(aggs.len(), 1);
    assert_eq!(aggs[0].func, AggFunc::Sum);
    assert_eq!(aggs[0].alias.as_deref(), Some("revenue"));

    let LogicalPlan::Filter { input, predicate } = input.as_ref() else {
        panic!("Aggregate input must be Filter");
    };
    let LogicalPlan::Scan {
        table, projection, ..
    } = input.as_ref()
    else {
        panic!("Filter input must be Scan");
    };
    assert_eq!(table, "lineitem");

    // Scan projection: the referenced columns, resolved to (name, leaf, ty).
    // First-use order: SELECT binds first (ep, disc), then WHERE (shipdate,
    // quantity — discount already used).
    let cols: Vec<(&str, usize, LogicalType)> = projection
        .iter()
        .map(|c| (c.name.as_str(), c.leaf, c.ty))
        .collect();
    assert_eq!(
        cols,
        vec![
            ("l_extendedprice", 5, LogicalType::Float64),
            ("l_discount", 6, LogicalType::Float64),
            ("l_shipdate", 10, LogicalType::Date32),
            ("l_quantity", 4, LogicalType::Float64),
        ]
    );

    // The aggregate argument is bound to chunk positions: col0 * col1.
    assert_eq!(
        aggs[0].arg,
        Expr::Binary {
            op: BinaryOp::Mul,
            lhs: Box::new(Expr::Column(0)),
            rhs: Box::new(Expr::Column(1)),
        }
    );

    // ★ The decimal fold: the BETWEEN bounds must be EXACTLY 0.05 and 0.07.
    let mut lits = Vec::new();
    literals(predicate, &mut lits);
    assert!(
        lits.contains(&ScalarValue::Float64(0.05)),
        "lower bound must fold to exactly 0.05 (decimal), got literals {lits:?}"
    );
    assert!(
        lits.contains(&ScalarValue::Float64(0.07)),
        "upper bound must fold to exactly 0.07 in DECIMAL — an f64 fold gives \
         0.069999999999999996 and drops the whole 0.07 bucket; got {lits:?}"
    );

    // BETWEEN desugared + dates resolved: the predicate is five Column-op-
    // Literal comparisons AND-ed together (1994-01-01 = 8766, 1995-01-01 =
    // 9131 days since epoch).
    let mut cmps = Vec::new();
    comparisons(predicate, &mut cmps);
    assert_eq!(
        cmps,
        vec![
            (2, BinaryOp::GtEq, ScalarValue::Date32(8766)),
            (2, BinaryOp::Lt, ScalarValue::Date32(9131)),
            (1, BinaryOp::GtEq, ScalarValue::Float64(0.05)),
            (1, BinaryOp::LtEq, ScalarValue::Float64(0.07)),
            (3, BinaryOp::Lt, ScalarValue::Int64(24)),
        ]
    );
}

#[test]
fn unknown_names_error_cleanly() {
    let cat = catalog();
    let err = bind_sql("select sum(nope) from lineitem", &cat).unwrap_err();
    assert!(err.contains("nope"), "error should name the column: {err}");

    let err = bind_sql("select sum(l_discount) from nosuch", &cat).unwrap_err();
    assert!(err.contains("nosuch"), "error should name the table: {err}");
}
