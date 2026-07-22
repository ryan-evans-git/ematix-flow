//! Gate q49's two stacked front-end features over TPC-DS sf1:
//!   1. an aggregate nested inside a CAST — `cast(sum(x) AS DECIMAL(15,4)) /
//!      cast(sum(y) AS DECIMAL(15,4))` (a decimal *ratio* of two sums);
//!   2. `rank() OVER (ORDER BY …)` computed over a *derived* (aggregated)
//!      table with NO GROUP BY at the window level — the passthrough columns
//!      flow through unchanged and the rank spans the whole input.

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

fn f(v: &ScalarValue) -> f64 {
    match v {
        ScalarValue::Float64(x) => *x,
        ScalarValue::Int64(x) => *x as f64,
        other => panic!("expected number, got {other:?}"),
    }
}

/// A ratio of two aggregates, each wrapped in `CAST(_ AS DECIMAL(15,4))` —
/// the sums must be found through the cast (so the query binds as grouped),
/// and the result is a Float64 ratio.
#[test]
fn ratio_of_two_casted_sums() {
    let out = rows(
        "select ss_item_sk as item, \
            cast(sum(ss_quantity) as decimal(15,4)) / \
            cast(sum(ss_ext_sales_price) as decimal(15,4)) as ratio \
         from store_sales where ss_item_sk is not null \
         group by ss_item_sk order by item limit 5",
    );
    assert_eq!(out.len(), 5, "five item groups");
    for r in &out {
        assert!(f(&r[1]).is_finite(), "ratio is a finite float: {:?}", r[1]);
    }
}

/// `rank() OVER (ORDER BY r)` over a derived aggregate with no window-level
/// GROUP BY. The rank must be dense-free 1..=N in ascending `r` order, and
/// the passthrough columns (`item`, `r`) survive.
#[test]
fn rank_over_derived_aggregate() {
    let out = rows(
        "select item, r, rank() over (order by r) as rk \
         from (select ss_item_sk as item, sum(ss_quantity) as r \
               from store_sales where ss_item_sk is not null \
               group by ss_item_sk) x \
         order by rk limit 10",
    );
    assert_eq!(out.len(), 10, "ten smallest-r items");
    // rank ascending by r: first row is rank 1, and rows are r-sorted.
    assert_eq!(f(&out[0][2]), 1.0, "smallest r ranks 1");
    for w in out.windows(2) {
        assert!(f(&w[0][1]) <= f(&w[1][1]), "r is non-decreasing");
        assert!(f(&w[0][2]) <= f(&w[1][2]), "rank is non-decreasing");
    }
}
