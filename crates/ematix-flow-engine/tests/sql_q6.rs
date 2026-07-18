//! P3 slice 3 gate — **the first P3 kill-gate**: a query planned from a SQL
//! string, executed on the engine with zero hand-assembly, must equal the
//! hand-built native kernel.
//!
//! `bind_sql(Q6) → execute` vs `run_tpch_q6_native` over SF-1 lineitem. The
//! executor accumulates per-chunk partials in the same association as the
//! hand kernel (chunk subtotal, then add), and the sequential scan decodes
//! row groups in the same order — so the comparison is **exact f64
//! equality**, not a tolerance: the planned path must compute the *same*
//! sum, not a similar one. The canonical DuckDB oracle
//! (123141078.2283) is asserted independently so the gate can't pass by
//! both paths sharing a bug that moves the value.

use std::path::PathBuf;

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::expr::ScalarValue;
use ematix_flow_engine::plan::execute;
use ematix_flow_engine::run_tpch_q6_native;
use ematix_flow_engine::vector::LogicalType;

const Q6_SQL: &str = "select sum(l_extendedprice * l_discount) as revenue \
                      from lineitem \
                      where l_shipdate >= date '1994-01-01' \
                        and l_shipdate < date '1995-01-01' \
                        and l_discount between 0.06 - 0.01 and 0.06 + 0.01 \
                        and l_quantity < 24";

fn sf1_lineitem() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/tpch/data/sf1/lineitem.parquet")
}

#[test]
fn q6_from_sql_equals_hand_built_kernel() {
    let path = sf1_lineitem();
    if !path.exists() {
        eprintln!(
            "SKIP q6_from_sql_equals_hand_built_kernel: {} absent",
            path.display()
        );
        return;
    }

    // The harness registers the schema (leaf indices from the parquet
    // layout) — the engine hardcodes nothing about TPC-H.
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

    // SQL → bound plan → engine execution. No hand-assembly anywhere.
    let plan = bind_sql(Q6_SQL, &catalog).expect("bind failed");
    let result = execute(&plan).expect("execute failed");

    assert_eq!(result.columns, vec!["revenue".to_string()]);
    assert_eq!(result.rows.len(), 1, "scalar aggregate returns one row");
    let &[ScalarValue::Float64(revenue)] = result.rows[0].as_slice() else {
        panic!("expected one f64 sum, got {:?}", result.rows[0]);
    };

    // Exact equality with the hand-built kernel: same filter, same
    // association, same chunk order ⇒ the same f64, bit for bit.
    let hand = run_tpch_q6_native(&path).expect("hand kernel failed");
    assert_eq!(
        revenue, hand.revenue,
        "planned Q6 must equal the hand-built kernel exactly"
    );

    // Independent anchor: the canonical DuckDB SF-1 oracle.
    let oracle = 123_141_078.229_299_72_f64;
    let rel = (revenue - oracle).abs() / oracle;
    assert!(
        rel < 1e-9,
        "planned Q6 revenue {revenue} != DuckDB oracle {oracle} (rel {rel:.3e})"
    );
}
