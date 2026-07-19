//! Gate the TPC-DS two-key outer-join shape (q5/q40/q49/q72/q80):
//!   `LEFT OUTER JOIN cr ON (cs.k1 = cr.k1 AND cs.k2 = cr.k2)`
//! The whole ON is one parenthesized `(a AND b)` node; `split_and` must
//! descend through the parens so the two equijoins become two edges (a
//! composite key), not one multi-table conjunct a LEFT JOIN can't route.
//! Counts are over TPC-DS sf1 catalog_sales/catalog_returns.

use std::path::PathBuf;

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::expr::ScalarValue;
use ematix_flow_engine::plan::execute;

fn catalog() -> Catalog {
    let data = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/tpcds/data/sf1");
    let mut c = Catalog::new();
    for t in ["catalog_sales", "catalog_returns"] {
        c.register_parquet(t, data.join(format!("{t}.parquet")))
            .expect("register");
    }
    c
}

fn count(sql: &str) -> i64 {
    let c = catalog();
    let q = bind_sql(sql, &c).expect("bind");
    match execute(&q).expect("execute").rows[0][0] {
        ScalarValue::Int64(x) => x,
        ref o => panic!("expected count, got {o:?}"),
    }
}

/// Both columns of the composite key are applied: joining on the second
/// key too (item_sk) collapses 1,458,686 order-only matches to 144,067. A
/// bug that dropped the second equijoin would leave the larger count.
#[test]
fn composite_key_applies_both_columns() {
    let one_key = count(
        "select count(*) from catalog_sales, catalog_returns where cs_order_number = cr_order_number",
    );
    let two_key = count(
        "select count(*) from catalog_sales, catalog_returns \
         where cs_order_number = cr_order_number and cs_item_sk = cr_item_sk",
    );
    assert_eq!(one_key, 1_458_686, "order-only inner join");
    assert_eq!(two_key, 144_067, "order+item inner join");
    assert!(two_key < one_key, "the second key must filter matches");
}

/// The parenthesized two-key `LEFT OUTER JOIN ... ON (a AND b)` binds and
/// runs: it preserves every driving (catalog_sales) row — unmatched rows
/// survive, and the ≤1-match returns cause no fan-out — so the LEFT count
/// equals catalog_sales alone.
#[test]
fn left_outer_two_key_preserves_driving_side() {
    let left = count(
        "select count(*) from catalog_sales \
         left outer join catalog_returns \
           on (cs_order_number = cr_order_number and cs_item_sk = cr_item_sk)",
    );
    let cs_alone = count("select count(*) from catalog_sales");
    assert_eq!(cs_alone, 1_441_548, "catalog_sales row count");
    assert_eq!(left, cs_alone, "LEFT join must not drop preserved rows");
}
