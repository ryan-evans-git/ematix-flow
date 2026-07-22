//! **The second P3 kill-gate: TPC-H Q08 planned from SQL.**
//!
//! The flattened Q08 (the FROM-subquery inlined — same relational content):
//! an 8-table join tree with a self-joined dimension (`nation n1` for the
//! customer's region chain, `nation n2` for the supplier's BRAZIL flag),
//! string filters, a date window, `EXTRACT(YEAR)` as the group key, a CASE
//! numerator, and a division of two sums as the output. Everything the
//! hand-built harness wired by hand, now planned from SQL text.
//!
//! Oracle (independent python join over SF-1): 2,603 qualifying lineitem
//! rows; mkt_share 1995 = 0.03443589040665483, 1996 = 0.04148552129353034
//! (the canonical published SF-1 Q08 answer, 0.0344/0.0415).

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

/// The full TPC-H SF schema slices Q08 touches, with real parquet leaf
/// indices — registered by the harness, never hardcoded in the engine.
fn catalog() -> Catalog {
    let mut c = Catalog::new();
    c.register_table(
        "part",
        sf1("part"),
        &[
            ("p_partkey", 0, LogicalType::Int64),
            ("p_type", 4, LogicalType::Utf8),
        ],
    );
    c.register_table(
        "supplier",
        sf1("supplier"),
        &[
            ("s_suppkey", 0, LogicalType::Int64),
            ("s_nationkey", 3, LogicalType::Int64),
        ],
    );
    c.register_table(
        "lineitem",
        sf1("lineitem"),
        &[
            ("l_orderkey", 0, LogicalType::Int64),
            ("l_partkey", 1, LogicalType::Int64),
            ("l_suppkey", 2, LogicalType::Int64),
            ("l_extendedprice", 5, LogicalType::Float64),
            ("l_discount", 6, LogicalType::Float64),
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
    c.register_table(
        "nation",
        sf1("nation"),
        &[
            ("n_nationkey", 0, LogicalType::Int64),
            ("n_name", 1, LogicalType::Utf8),
            ("n_regionkey", 2, LogicalType::Int64),
        ],
    );
    c.register_table(
        "region",
        sf1("region"),
        &[
            ("r_regionkey", 0, LogicalType::Int64),
            ("r_name", 1, LogicalType::Utf8),
        ],
    );
    c
}

const Q08_SQL: &str = "\
select extract(year from o_orderdate) as o_year, \
       sum(case when n2.n_name = 'BRAZIL' \
                then l_extendedprice * (1 - l_discount) else 0 end) \
         / sum(l_extendedprice * (1 - l_discount)) as mkt_share \
from part, supplier, lineitem, orders, customer, nation n1, nation n2, region \
where p_partkey = l_partkey \
  and s_suppkey = l_suppkey \
  and l_orderkey = o_orderkey \
  and o_custkey = c_custkey \
  and c_nationkey = n1.n_nationkey \
  and n1.n_regionkey = r_regionkey \
  and s_nationkey = n2.n_nationkey \
  and r_name = 'AMERICA' \
  and o_orderdate between date '1995-01-01' and date '1996-12-31' \
  and p_type = 'ECONOMY ANODIZED STEEL' \
group by extract(year from o_orderdate)";

#[test]
fn q08_from_sql_matches_oracle() {
    let tables = [
        "part", "supplier", "lineitem", "orders", "customer", "nation", "region",
    ];
    if tables.iter().any(|t| !sf1(t).exists()) {
        eprintln!("SKIP q08_from_sql_matches_oracle: SF1 data absent");
        return;
    }

    let q = bind_sql(Q08_SQL, &catalog()).expect("bind failed");
    let result = execute(&q).expect("execute failed");

    assert_eq!(
        result.columns,
        vec!["o_year".to_string(), "mkt_share".to_string()]
    );
    assert_eq!(result.rows.len(), 2, "two years in the window");

    let oracle = [
        (1995i64, 0.03443589040665483_f64),
        (1996, 0.04148552129353034),
    ];
    for (row, &(want_year, want_share)) in result.rows.iter().zip(&oracle) {
        let [ScalarValue::Int64(year), ScalarValue::Float64(share)] = row.as_slice() else {
            panic!("expected (Int64 year, Float64 share), got {row:?}");
        };
        assert_eq!(*year, want_year);
        let rel = (*share - want_share).abs() / want_share;
        assert!(
            rel < 1e-9,
            "{year} share {share} != oracle {want_share} (rel {rel:.3e})"
        );
    }
}
