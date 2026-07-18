//! P3 slice 4b gate: a two-table inner equi-join planned from SQL. The
//! binder must split WHERE conjuncts into join conditions vs per-table
//! filters, attribute expressions to their tables, and the executor must
//! implement **true inner-join semantics** — not just membership.
//!
//! Two directions, because they fail differently:
//! - **A (unique keys)**: lineitem ⋈ orders(1994 window), grouped revenue by
//!   line number. Orders' keys are unique, so the join is a pure semijoin
//!   narrow — the engine's no-materialization shape.
//! - **B (duplicate keys)**: orders ⋈ lineitem, `sum(o_totalprice)`. Every
//!   order matches ~4 lineitems, so each order row must count **once per
//!   match** (selection multiplicity). A membership-only implementation gets
//!   B wrong by ~4× — and B's weighted row total (6,001,215 = every lineitem
//!   weights its parent order exactly once) is a structural invariant.
//!
//! Oracles computed independently with pyarrow.

use std::path::PathBuf;

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::expr::ScalarValue;
use ematix_flow_engine::plan::execute;
use ematix_flow_engine::vector::LogicalType;

fn sf1(table: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(format!("../../examples/tpch/data/sf1/{table}.parquet"))
}

fn catalog() -> Catalog {
    let mut c = Catalog::new();
    c.register_table(
        "lineitem",
        sf1("lineitem"),
        &[
            ("l_orderkey", 0, LogicalType::Int64),
            ("l_linenumber", 3, LogicalType::Int32),
            ("l_extendedprice", 5, LogicalType::Float64),
        ],
    );
    c.register_table(
        "orders",
        sf1("orders"),
        &[
            ("o_orderkey", 0, LogicalType::Int64),
            ("o_totalprice", 3, LogicalType::Float64),
            ("o_orderdate", 4, LogicalType::Date32),
        ],
    );
    c
}

/// Gate A oracle: (l_linenumber, sum(l_extendedprice)) for lineitems whose
/// order is in the 1994 window. 910,519 matched rows.
const ORACLE_A: [(i64, f64); 7] = [
    (1, 8712181780.630001),
    (2, 7445442071.89),
    (3, 6224699730.969999),
    (4, 4969474653.79),
    (5, 3735150812.87),
    (6, 2499704488.34),
    (7, 1244022895.12),
];

#[test]
fn join_with_unique_keys_from_sql() {
    if !sf1("lineitem").exists() || !sf1("orders").exists() {
        eprintln!("SKIP join_with_unique_keys_from_sql: SF1 data absent");
        return;
    }
    let sql = "select l_linenumber, sum(l_extendedprice) as rev \
               from lineitem, orders \
               where l_orderkey = o_orderkey \
                 and o_orderdate >= date '1994-01-01' \
                 and o_orderdate < date '1995-01-01' \
               group by l_linenumber";
    let plan = bind_sql(sql, &catalog()).expect("bind failed");
    let result = execute(&plan).expect("execute failed");

    assert_eq!(
        result.columns,
        vec!["l_linenumber".to_string(), "rev".to_string()]
    );
    assert_eq!(result.rows.len(), 7);
    for (row, &(want_key, want_sum)) in result.rows.iter().zip(&ORACLE_A) {
        let &[ScalarValue::Int64(key), ScalarValue::Float64(sum)] = row.as_slice() else {
            panic!("expected (Int64, Float64), got {row:?}");
        };
        assert_eq!(key, want_key);
        let rel = (sum - want_sum).abs() / want_sum;
        assert!(
            rel < 1e-9,
            "group {key}: {sum} != oracle {want_sum} (rel {rel:.3e})"
        );
    }
}

#[test]
fn join_with_duplicate_keys_multiplies_rows() {
    if !sf1("lineitem").exists() || !sf1("orders").exists() {
        eprintln!("SKIP join_with_duplicate_keys_multiplies_rows: SF1 data absent");
        return;
    }
    // Fact = orders, dim = lineitem (≈4 matches per order). True inner-join
    // semantics: each order's totalprice counts once per matching lineitem.
    let sql = "select sum(o_totalprice) as weighted \
               from orders, lineitem \
               where o_orderkey = l_orderkey";
    let plan = bind_sql(sql, &catalog()).expect("bind failed");
    let result = execute(&plan).expect("execute failed");

    let &[ScalarValue::Float64(sum)] = result.rows[0].as_slice() else {
        panic!("expected one f64, got {:?}", result.rows[0]);
    };
    // pyarrow oracle: 1,134,436,101,880.19 over 6,001,215 weighted rows —
    // a membership-only (semijoin) implementation lands ~4× low.
    let want = 1134436101880.19_f64;
    let rel = (sum - want).abs() / want;
    assert!(
        rel < 1e-9,
        "weighted sum {sum} != oracle {want} (rel {rel:.3e}) — \
         multiplicity semantics broken?"
    );
}

#[test]
fn cross_table_attribution_errors() {
    let cat = catalog();
    // An arithmetic expression mixing tables is not a join condition and
    // not a single-table filter — must error, not mis-bind.
    let err = bind_sql(
        "select sum(l_extendedprice * o_totalprice) from lineitem, orders \
         where l_orderkey = o_orderkey",
        &cat,
    )
    .unwrap_err();
    assert!(
        err.contains("one table"),
        "cross-table expression should error: {err}"
    );

    // Two tables but no join condition must error, not cross-join.
    let err = bind_sql("select sum(l_extendedprice) from lineitem, orders", &cat).unwrap_err();
    assert!(
        err.to_lowercase().contains("join"),
        "missing join condition should error: {err}"
    );
}
