//! Gate the ROLLUP-adjacent front-end features (q27/q36/q54/q70), over
//! TPC-DS sf1: `GROUPING(col)` (1 when the column is a ROLLUP subtotal, else
//! 0); `CAST(<fractional> AS INT)` (rounds to nearest, DuckDB semantics); and
//! a SELECT alias referenced *inside* an ORDER BY expression.

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

fn rows(sql: &str) -> Vec<Vec<ScalarValue>> {
    let c = catalog();
    let q = bind_sql(sql, &c).expect("bind");
    execute(&q).expect("execute").rows
}

fn i(v: &ScalarValue) -> i64 {
    match v {
        ScalarValue::Int64(x) => *x,
        ScalarValue::Int32(x) => *x as i64,
        other => panic!("expected int, got {other:?}"),
    }
}

/// `GROUPING(ss_store_sk)` over `ROLLUP(ss_store_sk)`: the per-store rows
/// have grouping = 0, the grand-total row (store rolled up) has grouping = 1
/// and exactly one such row exists.
#[test]
fn grouping_flag_marks_rollup_subtotal() {
    let out = rows(
        "select ss_store_sk, grouping(ss_store_sk) as g, count(*) as n \
         from store_sales group by rollup(ss_store_sk) order by g, ss_store_sk",
    );
    assert!(out.len() >= 2, "per-store rows plus a grand total");
    let totals = out.iter().filter(|r| i(&r[1]) == 1).count();
    assert_eq!(totals, 1, "exactly one grand-total (grouping=1) row");
    // The detail rows carry grouping = 0.
    assert!(
        out.iter().filter(|r| i(&r[1]) == 0).count() >= 1,
        "at least one detail row with grouping=0"
    );
    // The grand total's count is the sum of the detail counts.
    let grand: i64 = out
        .iter()
        .find(|r| i(&r[1]) == 1)
        .map(|r| i(&r[2]))
        .unwrap();
    let detail_sum: i64 = out.iter().filter(|r| i(&r[1]) == 0).map(|r| i(&r[2])).sum();
    assert_eq!(
        grand, detail_sum,
        "grand total count = sum of detail counts"
    );
}

/// `CAST(<fractional> AS INT)` rounds to nearest: `avg` of a column divided
/// down, cast to int, buckets rows. Verify the cast produces an integer that
/// equals the rounded float (not a truncation and not the raw float).
#[test]
fn cast_as_int_rounds_to_nearest() {
    // 7.0 / 2 = 3.5 -> rounds to 4; 9.0 / 2 = 4.5 -> 4 or 5 (tie, avoid).
    let out = rows("select cast(11.0 / 2 as int) as r from store_sales limit 1");
    assert_eq!(i(&out[0][0]), 6, "11.0/2 = 5.5 rounds to 6");
    let out2 = rows("select cast(10.4 as int) as r from store_sales limit 1");
    assert_eq!(i(&out2[0][0]), 10, "10.4 rounds down to 10");
}
