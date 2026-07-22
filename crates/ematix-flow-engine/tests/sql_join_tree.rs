//! Stage-A gates: N-table join **trees** planned from SQL — dim chains
//! (dims joining dims, not the fact) and **payload bubbling** (a dim column
//! selected/grouped at the root must flow up through intermediate link
//! tables). Oracles computed independently with pyarrow.

use std::path::PathBuf;

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::expr::ScalarValue;
use ematix_flow_engine::plan::execute;
use ematix_flow_engine::vector::LogicalType;

fn sf1(table: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(format!("../../examples/tpch/data/sf1/{table}.parquet"))
}

fn catalog() -> Catalog {
    let mut c = Catalog::new();
    c.register_table(
        "lineitem",
        sf1("lineitem"),
        &[
            ("l_orderkey", 0, LogicalType::Int64),
            ("l_linenumber", 3, LogicalType::Int32),
            ("l_extendedprice", 5, LogicalType::Float64),
        ],
    );
    c.register_table(
        "orders",
        sf1("orders"),
        &[
            ("o_orderkey", 0, LogicalType::Int64),
            ("o_custkey", 1, LogicalType::Int64),
            ("o_orderdate", 4, LogicalType::Date32),
        ],
    );
    c.register_table(
        "customer",
        sf1("customer"),
        &[
            ("c_custkey", 0, LogicalType::Int64),
            ("c_nationkey", 3, LogicalType::Int64),
        ],
    );
    c
}

fn have_data() -> bool {
    ["lineitem", "orders", "customer"]
        .iter()
        .all(|t| sf1(t).exists())
}

/// A dim column (`c_nationkey`) grouped at the root must bubble through the
/// chain lineitem ← orders ← customer. pyarrow oracle: 25 nations,
/// 910,519 rows in the 1994 order window; spot-check four groups + the last.
#[test]
fn payload_bubbles_through_dim_chain() {
    if !have_data() {
        eprintln!("SKIP payload_bubbles_through_dim_chain: SF1 data absent");
        return;
    }
    let sql = "select c_nationkey, sum(l_extendedprice) as rev \
               from lineitem, orders, customer \
               where l_orderkey = o_orderkey and o_custkey = c_custkey \
                 and o_orderdate >= date '1994-01-01' \
                 and o_orderdate < date '1995-01-01' \
               group by c_nationkey";
    let q = bind_sql(sql, &catalog()).expect("bind failed");
    let result = execute(&q).expect("execute failed");

    assert_eq!(
        result.columns,
        vec!["c_nationkey".to_string(), "rev".to_string()]
    );
    assert_eq!(result.rows.len(), 25, "25 nations");

    let spot = [
        (0i64, 1398299757.3399978_f64),
        (1, 1394462849.539998),
        (2, 1390746092.520003),
        (3, 1400702288.939997),
        (24, 1393162008.0099933),
    ];
    for &(want_key, want_sum) in &spot {
        let row = result
            .rows
            .iter()
            .find(|r| r[0] == ScalarValue::Int64(want_key))
            .unwrap_or_else(|| panic!("group {want_key} missing"));
        let ScalarValue::Float64(sum) = row[1] else {
            panic!("expected f64 sum");
        };
        let rel = (sum - want_sum).abs() / want_sum;
        assert!(
            rel < 1e-9,
            "nation {want_key}: {sum} != oracle {want_sum} (rel {rel:.3e})"
        );
    }
}

/// A key-only chain of depth 3: lineitem ← orders ← customer, filtered on
/// the far end (`c_nationkey < 5`), grouped on the fact. pyarrow oracle:
/// 1,197,114 qualifying rows in 7 groups.
#[test]
fn key_only_chain_filters_the_fact() {
    if !have_data() {
        eprintln!("SKIP key_only_chain_filters_the_fact: SF1 data absent");
        return;
    }
    let sql = "select l_linenumber, sum(l_extendedprice) as rev \
               from lineitem, orders, customer \
               where l_orderkey = o_orderkey and o_custkey = c_custkey \
                 and c_nationkey < 5 \
               group by l_linenumber";
    let q = bind_sql(sql, &catalog()).expect("bind failed");
    let result = execute(&q).expect("execute failed");

    const ORACLE: [(i64, f64); 7] = [
        (1, 11443687183.259865),
        (2, 9800500943.46004),
        (3, 8187040522.859911),
        (4, 6550181690.640042),
        (5, 4903241626.100059),
        (6, 3278114200.2300024),
        (7, 1650722435.4599993),
    ];
    assert_eq!(result.rows.len(), 7);
    for (row, &(want_key, want_sum)) in result.rows.iter().zip(&ORACLE) {
        let [ScalarValue::Int64(key), ScalarValue::Float64(sum)] = row.as_slice() else {
            panic!("expected (Int64, Float64), got {row:?}");
        };
        assert_eq!(*key, want_key);
        let rel = (*sum - want_sum).abs() / want_sum;
        assert!(
            rel < 1e-9,
            "group {key}: {sum} != oracle {want_sum} (rel {rel:.3e})"
        );
    }
}
