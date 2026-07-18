//! P3 slice 4a gate: GROUP BY from SQL. A grouped aggregate planned from a
//! SQL string — filter + group keys + `sum` — must match an independent
//! pyarrow oracle, group for group.
//!
//! Query: revenue by line number over the 1994 ship window on SF-1 lineitem.
//! Exercises what the scalar Q6 gate couldn't: group-key binding (a
//! non-aggregate select item matched to GROUP BY), grouped hash aggregation
//! in the executor, a multi-row result, and the Int32 column path
//! (`l_linenumber` is int32 in the parquet).
//!
//! Oracle (pyarrow, computed independently): 909,455 filtered rows in 7
//! groups; per-group `sum(l_extendedprice * l_discount)` below (rel 1e-9 —
//! summation order differs from the engine's, so bit-equality is not the
//! contract here; the exact row/group structure is).

use std::path::PathBuf;

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::expr::ScalarValue;
use ematix_flow_engine::plan::execute;
use ematix_flow_engine::vector::LogicalType;

const SQL: &str = "select l_linenumber, sum(l_extendedprice * l_discount) as rev \
                   from lineitem \
                   where l_shipdate >= date '1994-01-01' \
                     and l_shipdate < date '1995-01-01' \
                   group by l_linenumber";

/// (l_linenumber, sum) from pyarrow over the same filter.
const ORACLE: [(i64, f64); 7] = [
    (1, 434578640.1955),
    (2, 370799126.1696),
    (3, 310488191.754),
    (4, 248280106.4442),
    (5, 185840463.7091),
    (6, 124410512.31690001),
    (7, 61842071.2516),
];

fn sf1_lineitem() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/tpch/data/sf1/lineitem.parquet")
}

#[test]
fn group_by_from_sql_matches_pyarrow_oracle() {
    let path = sf1_lineitem();
    if !path.exists() {
        eprintln!(
            "SKIP group_by_from_sql_matches_pyarrow_oracle: {} absent",
            path.display()
        );
        return;
    }

    let mut catalog = Catalog::new();
    catalog.register_table(
        "lineitem",
        &path,
        &[
            ("l_linenumber", 3, LogicalType::Int32),
            ("l_extendedprice", 5, LogicalType::Float64),
            ("l_discount", 6, LogicalType::Float64),
            ("l_shipdate", 10, LogicalType::Date32),
        ],
    );

    let plan = bind_sql(SQL, &catalog).expect("bind failed");
    let result = execute(&plan).expect("execute failed");

    assert_eq!(
        result.columns,
        vec!["l_linenumber".to_string(), "rev".to_string()]
    );
    assert_eq!(result.rows.len(), 7, "seven line numbers in the window");

    // Rows arrive sorted by group key; each must match the oracle group.
    for (row, &(want_key, want_sum)) in result.rows.iter().zip(&ORACLE) {
        let &[ScalarValue::Int64(key), ScalarValue::Float64(sum)] = row.as_slice() else {
            panic!("expected (Int64 key, Float64 sum), got {row:?}");
        };
        assert_eq!(key, want_key);
        let rel = (sum - want_sum).abs() / want_sum;
        assert!(
            rel < 1e-9,
            "group {key}: sum {sum} != oracle {want_sum} (rel {rel:.3e})"
        );
    }
}

#[test]
fn group_key_binding_is_validated() {
    let path = sf1_lineitem();
    let mut catalog = Catalog::new();
    catalog.register_table(
        "lineitem",
        &path,
        &[
            ("l_linenumber", 3, LogicalType::Int32),
            ("l_discount", 6, LogicalType::Float64),
        ],
    );

    // A non-aggregate select item that is NOT in GROUP BY must error.
    let err = bind_sql(
        "select l_linenumber, sum(l_discount) from lineitem group by l_discount",
        &catalog,
    )
    .unwrap_err();
    assert!(
        err.contains("l_linenumber"),
        "error should name the unmatched select item: {err}"
    );

    // Float-typed group keys are not yet supported — error by name.
    let err = bind_sql(
        "select l_discount, sum(l_discount) from lineitem group by l_discount",
        &catalog,
    )
    .unwrap_err();
    assert!(
        err.contains("l_discount"),
        "error should name the non-integer key: {err}"
    );
}
