//! Gate `date_column ± interval N days` (q72's `d3.d_date > (d1.d_date +
//! interval 5 days)`). A day/week interval is a constant offset on the
//! Date32 (days-since-epoch), so it lowers to integer arithmetic — no new
//! evaluator path. Months/years on a column would need per-row civil
//! arithmetic and stay unsupported. Values over TPC-DS sf1 date_dim.

use std::path::PathBuf;

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::expr::ScalarValue;
use ematix_flow_engine::plan::execute;

fn catalog() -> Catalog {
    let data = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/tpcds/data/sf1");
    let mut c = Catalog::new();
    c.register_parquet("date_dim", data.join("date_dim.parquet"))
        .expect("register");
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

/// Every date is strictly less than itself plus five days, and never
/// greater — so `+ interval 5 days` on a column moves the value by exactly
/// five days. The two counts partition all non-NULL d_date rows.
#[test]
fn interval_days_added_to_date_column() {
    let all = count("select count(*) from date_dim where d_date is not null");
    let less = count("select count(*) from date_dim where d_date < (d_date + interval 5 days)");
    let ge = count("select count(*) from date_dim where d_date >= (d_date + interval 5 days)");
    assert_eq!(less, all, "d_date < d_date + 5 days holds for every row");
    assert_eq!(ge, 0, "d_date >= d_date + 5 days never holds");
}

/// `- interval` on a column moves the other way: d_date is always greater
/// than d_date minus five days.
#[test]
fn interval_days_subtracted_from_date_column() {
    let all = count("select count(*) from date_dim where d_date is not null");
    let gt = count("select count(*) from date_dim where d_date > (d_date - interval 5 days)");
    assert_eq!(gt, all, "d_date > d_date - 5 days holds for every row");
}
