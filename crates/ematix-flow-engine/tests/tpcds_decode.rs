//! Gate: TPC-DS parquet decode through the engine — INT32-backed
//! DECIMAL(7,2) measures scale to f64, `optional` columns decode
//! definition levels into validity, NULL aggregate inputs are skipped,
//! and NULL join keys never match. Oracles computed independently with
//! pyarrow over sf1.

use std::path::PathBuf;

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::expr::ScalarValue;
use ematix_flow_engine::plan::execute;

fn catalog() -> Catalog {
    let data = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/tpcds/data/sf1");
    let mut c = Catalog::new();
    for t in ["store_sales", "date_dim"] {
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

fn close(a: f64, b: f64) {
    assert!((a - b).abs() <= b.abs() * 1e-9 + 1e-6, "{a} vs {b}");
}

/// count(*) counts all rows; sum skips the 130,137 NULL prices; the
/// decimal decode must scale INT32 cents into dollars exactly.
#[test]
fn decimal_and_null_skipping_aggregate() {
    let c = catalog();
    let q = bind_sql(
        "select count(*) as n, sum(ss_ext_sales_price) as s from store_sales",
        &c,
    )
    .expect("bind");
    let r = execute(&q).expect("execute");
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0], ScalarValue::Int64(2_880_404));
    close(f(&r.rows[0][1]), 5_262_470_646.52);
}

/// NULL ss_sold_date_sk rows (129,850) silently drop out of the inner
/// join; per-year sums match the pyarrow join+group oracle.
#[test]
fn null_join_keys_never_match() {
    let c = catalog();
    let q = bind_sql(
        "select d_year, sum(ss_ext_sales_price) as s \
         from store_sales, date_dim \
         where d_date_sk = ss_sold_date_sk \
         group by d_year",
        &c,
    )
    .expect("bind");
    let r = execute(&q).expect("execute");
    let oracle = [
        (1998, 1_031_347_638.75),
        (1999, 1_013_380_204.69),
        (2000, 1_036_555_271.90),
        (2001, 1_019_195_414.78),
        (2002, 1_026_128_587.44),
        (2003, 11_306_814.35),
    ];
    assert_eq!(r.rows.len(), oracle.len());
    for (row, (y, s)) in r.rows.iter().zip(oracle) {
        assert_eq!(row[0], ScalarValue::Int64(y));
        close(f(&row[1]), s);
    }
}
