//! Gate: a duplicate-key payload join — a dim (here the derived `sc`,
//! store_sales grouped by item_sk × store_sk) joins to the root `item`
//! on item_sk ALONE, so many dim rows share one join key and each
//! carries its own payload (revenue). The root must expand every matching
//! root row to one output row per dim row, each with that row's payload.
//! Oracle computed independently with pyarrow over sf1.

use std::path::PathBuf;

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::expr::ScalarValue;
use ematix_flow_engine::plan::execute;

fn catalog() -> Catalog {
    let data = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/tpcds/data/sf1");
    let mut c = Catalog::new();
    for t in ["store_sales", "item"] {
        c.register_parquet(t, data.join(format!("{t}.parquet")))
            .expect("register");
    }
    c
}

fn f(v: &ScalarValue) -> f64 {
    match v {
        ScalarValue::Float64(x) => *x,
        ScalarValue::Int64(x) => *x as f64,
        other => panic!("expected numeric, got {other:?}"),
    }
}

fn i(v: &ScalarValue) -> i64 {
    match v {
        ScalarValue::Int64(x) => *x,
        other => panic!("expected int, got {other:?}"),
    }
}

/// Each item row fans out to one output row per (item_sk, store_sk) group
/// carrying that group's revenue; count = number of joined dim rows, sum
/// = total revenue over them.
#[test]
fn dup_key_payload_fanout() {
    let c = catalog();
    let q = bind_sql(
        "select count(*) as n, sum(sc.revenue) as total \
         from item, \
           (select ss_item_sk, ss_store_sk, sum(ss_sales_price) as revenue \
            from store_sales group by ss_item_sk, ss_store_sk) sc \
         where i_item_sk = sc.ss_item_sk",
        &c,
    )
    .expect("bind");
    let r = execute(&q).expect("execute");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(i(&r.rows[0][0]), 125_811, "fan-out row count");
    let total = f(&r.rows[0][1]);
    assert!(
        (total - 104_159_495.71).abs() <= 104_159_495.71 * 1e-9 + 1e-2,
        "revenue sum {total} vs 104159495.71"
    );
}

/// Fan-out BELOW the root: `store_sales` (root) ⋈ `item` (unique dim) ⋈
/// the grouped `sc` (many rows per item_sk — a fan-out dim that is now a
/// GRANDCHILD, not a root child). The dim BUILD must cross-product over
/// `sc`'s chain when it consumes it. Filtered to one item (13): 336
/// store_sales rows × 7 stores that sold it = 2352 output rows.
#[test]
fn fanout_below_root() {
    let c = catalog();
    let q = bind_sql(
        "select count(*) as n, sum(sc.revenue) as total \
         from store_sales, item, \
           (select ss_item_sk, ss_store_sk, sum(ss_sales_price) as revenue \
            from store_sales group by ss_item_sk, ss_store_sk) sc \
         where store_sales.ss_item_sk = i_item_sk and i_item_sk = sc.ss_item_sk \
           and i_item_sk = 13",
        &c,
    )
    .expect("bind");
    let r = execute(&q).expect("execute");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(i(&r.rows[0][0]), 2352, "fan-out-below-root row count");
    let total = f(&r.rows[0][1]);
    assert!(
        (total - 4_064_296.32).abs() <= 4_064_296.32 * 1e-9 + 1e-2,
        "revenue sum {total} vs 4064296.32"
    );
}
