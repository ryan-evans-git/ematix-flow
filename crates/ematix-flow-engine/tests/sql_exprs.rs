//! Stage-D gates: the expression surface a real TPC-H query needs —
//! **division of two aggregates** (a row-space output projection over
//! computed sums) and **CASE WHEN** inside an aggregate argument. pyarrow
//! oracles.

use std::path::PathBuf;

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::expr::ScalarValue;
use ematix_flow_engine::plan::execute;
use ematix_flow_engine::vector::LogicalType;

fn sf1_lineitem() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/tpch/data/sf1/lineitem.parquet")
}

fn catalog() -> Catalog {
    let mut c = Catalog::new();
    c.register_table(
        "lineitem",
        sf1_lineitem(),
        &[
            ("l_linenumber", 3, LogicalType::Int32),
            ("l_quantity", 4, LogicalType::Float64),
            ("l_extendedprice", 5, LogicalType::Float64),
            ("l_discount", 6, LogicalType::Float64),
        ],
    );
    c
}

/// `sum(a)/sum(b)` per group: the output projection evaluates over the two
/// computed aggregates — the exact shape of Q08's `mkt_share`.
#[test]
fn division_of_aggregates_per_group() {
    if !sf1_lineitem().exists() {
        eprintln!("SKIP division_of_aggregates_per_group: SF1 data absent");
        return;
    }
    let sql = "select l_linenumber, sum(l_extendedprice) / sum(l_quantity) as ratio \
               from lineitem group by l_linenumber";
    let q = bind_sql(sql, &catalog()).expect("bind failed");
    let result = execute(&q).expect("execute failed");

    let spot = [
        (1i64, 1499.6003497809954_f64),
        (2, 1499.8702549180257),
        (3, 1499.8568475372529),
    ];
    for &(want_key, want_ratio) in &spot {
        let row = result
            .rows
            .iter()
            .find(|r| r[0] == ScalarValue::Int64(want_key))
            .unwrap_or_else(|| panic!("group {want_key} missing"));
        let ScalarValue::Float64(ratio) = row[1] else {
            panic!("expected f64 ratio");
        };
        let rel = (ratio - want_ratio).abs() / want_ratio;
        assert!(
            rel < 1e-9,
            "group {want_key}: {ratio} != oracle {want_ratio} (rel {rel:.3e})"
        );
    }
}

/// CASE WHEN inside an aggregate argument — Q08's conditional numerator.
#[test]
fn case_when_in_aggregate_argument() {
    if !sf1_lineitem().exists() {
        eprintln!("SKIP case_when_in_aggregate_argument: SF1 data absent");
        return;
    }
    let sql = "select sum(case when l_discount > 0.05 then l_extendedprice else 0 end) as x \
               from lineitem";
    let q = bind_sql(sql, &catalog()).expect("bind failed");
    let result = execute(&q).expect("execute failed");
    let [ScalarValue::Float64(x)] = result.rows[0].as_slice() else {
        panic!("expected one f64, got {:?}", result.rows[0]);
    };
    let want = 104280539333.36_f64;
    let rel = (*x - want).abs() / want;
    assert!(rel < 1e-9, "{x} != oracle {want} (rel {rel:.3e})");
}
