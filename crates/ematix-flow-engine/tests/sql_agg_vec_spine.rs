//! Gate the sorted-Vec aggregation spine: after the k-way merge the
//! grouped pipeline runs on a sorted `Vec<(key, states)>` — no BTreeMap
//! rebuild (q67 sf10 spent ~40% of its merge phase re-sorting and
//! re-tree-building already-sorted output). Observable conventions pinned
//! here:
//!   1. ROLLUP subtotal levels APPEND after the key-sorted base groups
//!      (previously they interleaved in key order). No ORDER BY ⇒ this is
//!      the output order.
//!   2. Grouped output stays BIT-IDENTICAL at any thread count.
//!
//! Over TPC-DS sf1.

use std::path::PathBuf;

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::expr::ScalarValue;
use ematix_flow_engine::plan::execute;

fn catalog() -> Catalog {
    let data = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/tpcds/data/sf1");
    let mut c = Catalog::new();
    c.register_parquet("store_sales", data.join("store_sales.parquet"))
        .expect("register");
    c
}

/// One test fn only: it mutates EMAT_ENGINE_THREADS (process-global), so
/// no sibling test may run concurrently in this binary.
#[test]
fn rollup_appends_and_thread_count_invariance() {
    let c = catalog();
    let sql = "SELECT ss_store_sk, ss_promo_sk, sum(ss_quantity) AS q,
                      grouping(ss_store_sk) + grouping(ss_promo_sk) AS lvl
               FROM store_sales
               WHERE ss_sold_date_sk IS NOT NULL AND ss_quantity > 90
               GROUP BY ROLLUP(ss_store_sk, ss_promo_sk)";
    let q = bind_sql(sql, &c).expect("bind");
    let r = execute(&q).expect("exec");
    assert!(r.rows.len() > 50, "shape produces real groups at sf1");

    // 1. Base rows (lvl 0) first, then subtotal levels — once a subtotal
    //    row appears, no base row follows.
    let lvls: Vec<i64> = r
        .rows
        .iter()
        .map(|row| match row[3] {
            ScalarValue::Int64(v) => v,
            ref other => panic!("grouping level not int: {other:?}"),
        })
        .collect();
    let first_subtotal = lvls.iter().position(|&l| l > 0).expect("has subtotals");
    assert!(
        lvls[first_subtotal..].iter().all(|&l| l > 0),
        "rollup subtotal levels append after all base groups"
    );
    // Grand total present exactly once, and the appended levels come in
    // cascade order: the drop-last level wholly before the grand total.
    assert_eq!(lvls.iter().filter(|&&l| l == 2).count(), 1);
    assert!(
        lvls[first_subtotal..].windows(2).all(|w| w[0] <= w[1]),
        "levels append coarsest-last"
    );

    // 2. Bit-identical at 1 thread vs many.
    // SAFETY: single-test binary — no concurrent env access.
    unsafe { std::env::set_var("EMAT_ENGINE_THREADS", "1") };
    let serial = execute(&q).expect("exec serial");
    unsafe { std::env::set_var("EMAT_ENGINE_THREADS", "13") };
    let wide = execute(&q).expect("exec wide");
    unsafe { std::env::remove_var("EMAT_ENGINE_THREADS") };
    assert_eq!(r.rows, serial.rows, "default == 1 thread");
    assert_eq!(r.rows, wide.rows, "default == 13 threads");
}
