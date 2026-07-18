//! P3 slice 2 gate: the binder. Q6's SQL text must bind against a catalog
//! into the expected **typed** bound query — names resolved to (leaf index,
//! type), `BETWEEN` desugared, and the discount bounds **constant-folded in
//! decimal**: `0.06 - 0.01 → 0.05` and `0.06 + 0.01 → 0.07` *exactly*.
//! Folding in f64 yields 0.069999999999999996 — one ULP below the stored
//! 0.07 — and silently drops ~1/3 of Q6's matches. This is the binder's
//! first real correctness obligation, and the reason `assert_eq!` on the
//! literal bits is the right gate.

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::expr::{BinaryOp, Expr, ScalarValue};
use ematix_flow_engine::logical::AggFunc;
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
        Expr::Literal(s) => out.push(s.clone()),
        Expr::Binary { lhs, rhs, .. } => {
            literals(lhs, out);
            literals(rhs, out);
        }
        _ => {}
    }
}

/// Collect comparisons `Column op Literal` in a predicate tree.
fn comparisons(e: &Expr, out: &mut Vec<(usize, BinaryOp, ScalarValue)>) {
    if let Expr::Binary { op, lhs, rhs } = e {
        if let (Expr::Column(c), Expr::Literal(v)) = (lhs.as_ref(), rhs.as_ref()) {
            out.push((*c, *op, v.clone()));
            return;
        }
        comparisons(lhs, out);
        comparisons(rhs, out);
    }
}

#[test]
fn binds_q6_to_typed_query_with_decimal_folded_bounds() {
    let q = bind_sql(Q6_SQL, &catalog()).expect("bind failed");

    // One table, no joins, no group keys, one SUM aggregate projected as
    // `revenue`.
    assert_eq!(q.tables.len(), 1);
    assert!(q.edges.is_empty());
    assert!(q.group.is_empty(), "Q6 has no GROUP BY");
    assert_eq!(q.aggs.len(), 1);
    assert_eq!(q.aggs[0].func, AggFunc::Sum);
    assert_eq!(q.output.len(), 1);
    assert_eq!(q.output[0].name, "revenue");
    // The output projects the (only) aggregate: row-space Column(0).
    assert_eq!(q.output[0].expr, Expr::Column(0));

    // Scan projection: the referenced columns resolved to (name, leaf, ty),
    // in first-use order (SELECT binds first: ep, disc; then WHERE:
    // shipdate, quantity — discount already used).
    let cols: Vec<(&str, usize, LogicalType)> = q.tables[0]
        .projection
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

    // The aggregate argument is bound to slots: slot0 * slot1.
    assert_eq!(
        q.aggs[0].arg,
        Expr::Binary {
            op: BinaryOp::Mul,
            lhs: Box::new(Expr::Column(0)),
            rhs: Box::new(Expr::Column(1)),
        }
    );

    let predicate = q.tables[0].filter.as_ref().expect("Q6 has a WHERE");

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

    // BETWEEN desugared + dates resolved: five slot-op-literal comparisons
    // AND-ed together (1994-01-01 = 8766, 1995-01-01 = 9131).
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
