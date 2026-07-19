//! Gate `SELECT DISTINCT`. It was silently dropped before — a plain-row
//! DISTINCT emitted one output row per joined row (duplicates and all),
//! wrong-but-hidden because the queries that used it happened to feed
//! INTERSECT/EXCEPT, which dedup anyway. Two paths are exercised: (1) no
//! GROUP BY → folded into a GROUP BY over the projected columns; (2) over
//! an explicit GROUP BY → deduped from the final rows.
//! Over TPC-DS sf1, `ss_store_sk`'s distinct set is {1,2,4,7,8,10,NULL} = 7
//! values (SQL counts NULL as one distinct value).

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

fn run(sql: &str) -> Vec<Vec<ScalarValue>> {
    let c = catalog();
    let q = bind_sql(sql, &c).expect("bind");
    execute(&q).expect("execute").rows
}

/// `SELECT DISTINCT col` with no GROUP BY folds into `GROUP BY col` — the
/// six store keys plus the NULL bucket = 7 distinct rows (not the millions
/// of raw store_sales rows a plain-row emit would have produced).
#[test]
fn distinct_plain_column_folds_into_group() {
    let rows = run("select distinct ss_store_sk from store_sales");
    assert_eq!(rows.len(), 7, "distinct store keys incl. NULL");
    // Every value is unique.
    let mut seen = std::collections::HashSet::new();
    for r in &rows {
        assert!(seen.insert(format!("{:?}", r[0])), "duplicate: {:?}", r[0]);
    }
}

/// DISTINCT layered on an explicit GROUP BY whose output is narrower than
/// its keys: grouping by (store, item) yields many rows per store, and the
/// projected `ss_store_sk` alone must still collapse to 7 distinct rows —
/// the flag-driven post-dedup (grouping could not fold it away).
#[test]
fn distinct_over_explicit_group_by_dedups_final_rows() {
    let rows = run("select distinct ss_store_sk from store_sales group by ss_store_sk, ss_item_sk");
    assert_eq!(
        rows.len(),
        7,
        "distinct store keys after (store,item) group"
    );
}

/// DISTINCT + ORDER BY + LIMIT: the LIMIT counts distinct rows. Ascending,
/// NULLs sort last, so the first three are the smallest store keys 1,2,4.
#[test]
fn distinct_with_order_and_limit_counts_distinct_rows() {
    let rows = run("select distinct ss_store_sk from store_sales order by ss_store_sk limit 3");
    assert_eq!(
        rows,
        vec![
            vec![ScalarValue::Int64(1)],
            vec![ScalarValue::Int64(2)],
            vec![ScalarValue::Int64(4)],
        ]
    );
}
