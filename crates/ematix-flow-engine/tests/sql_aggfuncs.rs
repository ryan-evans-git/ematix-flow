//! Stage-B gate: the full aggregate-function set — COUNT(*) / MIN / MAX /
//! AVG / SUM in one query over the Q6 filter, against a pyarrow oracle.
//! The COUNT (114,160) is the same matched-row count the Q6 kill-gate
//! pins, so the two gates cross-check each other.

use std::path::PathBuf;

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::expr::ScalarValue;
use ematix_flow_engine::plan::execute;
use ematix_flow_engine::vector::LogicalType;

fn sf1_lineitem() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/tpch/data/sf1/lineitem.parquet")
}

#[test]
fn count_min_max_avg_sum_match_oracle() {
    let path = sf1_lineitem();
    if !path.exists() {
        eprintln!(
            "SKIP count_min_max_avg_sum_match_oracle: {} absent",
            path.display()
        );
        return;
    }
    let mut catalog = Catalog::new();
    catalog.register_table(
        "lineitem",
        &path,
        &[
            ("l_quantity", 4, LogicalType::Float64),
            ("l_extendedprice", 5, LogicalType::Float64),
            ("l_discount", 6, LogicalType::Float64),
            ("l_shipdate", 10, LogicalType::Date32),
        ],
    );
    let sql = "select count(*) as n, min(l_extendedprice) as lo, \
               max(l_extendedprice) as hi, avg(l_quantity) as aq, \
               sum(l_discount) as sd \
               from lineitem \
               where l_shipdate >= date '1994-01-01' \
                 and l_shipdate < date '1995-01-01' \
                 and l_discount between 0.06 - 0.01 and 0.06 + 0.01 \
                 and l_quantity < 24";
    let q = bind_sql(sql, &catalog).expect("bind failed");
    let result = execute(&q).expect("execute failed");

    assert_eq!(result.columns, vec!["n", "lo", "hi", "aq", "sd"]);
    let row = &result.rows[0];
    assert_eq!(
        row[0],
        ScalarValue::Int64(114_160),
        "count == Q6 matched rows"
    );
    assert_eq!(row[1], ScalarValue::Float64(906.0), "min is exact");
    assert_eq!(row[2], ScalarValue::Float64(48092.77), "max is exact");
    let (ScalarValue::Float64(aq), ScalarValue::Float64(sd)) = (&row[3], &row[4]) else {
        panic!("expected f64 avg/sum, got {row:?}");
    };
    let want_aq = 12.001384022424666_f64;
    let want_sd = 6849.520000000002_f64;
    assert!(
        (aq - want_aq).abs() / want_aq < 1e-9,
        "avg {aq} != {want_aq}"
    );
    assert!(
        (sd - want_sd).abs() / want_sd < 1e-9,
        "sum {sd} != {want_sd}"
    );
}
