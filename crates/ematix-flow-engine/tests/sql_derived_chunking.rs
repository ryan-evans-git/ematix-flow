//! Gate the bounded-chunk derived scan (the q95 sf10 lever): a
//! materialized derived table bigger than the chunk bound (2^21 rows)
//! feeds downstream operators as SEVERAL row groups — so aggregation over
//! it parallelizes — and must produce exactly the rows of the equivalent
//! direct query. store_sales at sf1 (2.88M rows) spans two chunks.

use std::path::PathBuf;

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::plan::execute;

fn catalog() -> Catalog {
    let data = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/tpcds/data/sf1");
    let mut c = Catalog::new();
    c.register_parquet(
        "store_sales",
        data.join("store_sales.parquet"),
    )
    .expect("register");
    c
}

#[test]
fn multi_chunk_derived_aggregates_identically() {
    let c = catalog();
    // The inner ORDER BY forces materialization (no inline), so the outer
    // aggregation scans a 2.88M-row derived — two chunks.
    let through = execute(
        &bind_sql(
            "SELECT ss_store_sk, count(*), sum(ss_quantity) FROM
               (SELECT ss_store_sk, ss_quantity FROM store_sales ORDER BY ss_quantity) d
             GROUP BY ss_store_sk ORDER BY ss_store_sk",
            &c,
        )
        .expect("bind"),
    )
    .expect("exec");
    let direct = execute(
        &bind_sql(
            "SELECT ss_store_sk, count(*), sum(ss_quantity) FROM store_sales
             GROUP BY ss_store_sk ORDER BY ss_store_sk",
            &c,
        )
        .expect("bind"),
    )
    .expect("exec");
    assert!(!through.rows.is_empty());
    assert_eq!(through.rows, direct.rows, "chunked derived ≡ direct scan");
}

#[test]
fn multi_chunk_derived_preserves_plain_rows() {
    let c = catalog();
    // Plain-row projection through the big derived — row multiset must
    // survive chunking exactly (count both sides).
    let through = execute(
        &bind_sql(
            "SELECT count(*) FROM
               (SELECT ss_item_sk FROM store_sales ORDER BY ss_item_sk) d",
            &c,
        )
        .expect("bind"),
    )
    .expect("exec");
    let direct = execute(&bind_sql("SELECT count(*) FROM store_sales", &c).expect("bind"))
        .expect("exec");
    assert_eq!(through.rows, direct.rows);
}
