//! Gate case-insensitive identifier resolution — unquoted SQL identifiers
//! are case-insensitive (Spark/DuckDB semantics). q5 aliases
//! `sum(return_amt) AS RETURNS` in a CTE, then references `returns` in the
//! outer query; the two must resolve to the same column. Over TPC-DS sf1.

use std::path::PathBuf;

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::expr::ScalarValue;
use ematix_flow_engine::plan::execute;

fn catalog() -> Catalog {
    let data = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/tpcds/data/sf1");
    let mut c = Catalog::new();
    for t in ["store", "store_sales"] {
        c.register_parquet(t, data.join(format!("{t}.parquet")))
            .expect("register");
    }
    c
}

fn rows(sql: &str) -> Vec<Vec<ScalarValue>> {
    let c = catalog();
    let q = bind_sql(sql, &c).expect("bind");
    execute(&q).expect("execute").rows
}

/// A base-table column referenced in a different case than the catalog
/// stores it (`s_store_id` referenced as `S_STORE_ID`). Unquoted idents
/// fold, so this resolves.
#[test]
fn base_column_reference_is_case_insensitive() {
    let out = rows("select S_STORE_ID from Store order by s_store_id");
    assert!(!out.is_empty(), "at least one store");
}

/// The q5 shape: alias defined in one case (`AS RETURNS`), referenced in
/// another (`returns`) through a derived table. Both must bind to the same
/// column, and referencing it twice must not duplicate the projection.
#[test]
fn derived_alias_reference_is_case_insensitive() {
    let out = rows(
        "select returns, returns + 1 as r2 from \
           (select s_store_id, count(*) AS RETURNS from store group by s_store_id) x \
         order by returns",
    );
    assert!(!out.is_empty(), "at least one store group");
    for r in &out {
        // r2 = returns + 1 — the two references resolved to the same column.
        let (a, b) = match (&r[0], &r[1]) {
            (ScalarValue::Int64(a), ScalarValue::Int64(b)) => (*a, *b),
            other => panic!("expected int counts, got {other:?}"),
        };
        assert_eq!(b, a + 1, "returns and returns+1 agree");
    }
}
