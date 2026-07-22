//! Gate `concat(...)` — string concatenation (q5/q80's
//! `concat('store', s_store_id) AS id`). concat builds an OWNED string, so
//! it is evaluated via `eval_value` (projection / group-key slots). NULL
//! arguments are skipped (DuckDB semantics). Over TPC-DS sf1.

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

fn s(v: &ScalarValue) -> String {
    match v {
        ScalarValue::Utf8(x) => x.to_string(),
        other => panic!("expected string, got {other:?}"),
    }
}

/// A plain-row projection carrying `concat('store_', s_store_id)` — the
/// q5/q80 shape. A scalar function no longer forces the grouped path, so
/// this binds as plain rows and concat prefixes each store id.
#[test]
fn concat_literal_and_column_in_plain_projection() {
    let out = rows("select concat('store_', s_store_id) as id from store order by id");
    assert!(!out.is_empty(), "at least one store");
    for r in &out {
        assert!(
            s(&r[0]).starts_with("store_"),
            "concat prefixes the literal: {:?}",
            r[0]
        );
    }
}

/// concat over three parts, two literals around a column.
#[test]
fn concat_three_parts_plain_row() {
    let out = rows("select concat('[', s_store_id, ']') as id from store order by id limit 3");
    assert!(!out.is_empty());
    for r in &out {
        let v = s(&r[0]);
        assert!(v.starts_with('[') && v.ends_with(']'), "bracketed: {v}");
    }
}

/// The q5/q80 pattern end to end: concat in a plain-row branch feeding an
/// outer query that groups by the materialized string column. Grouping by
/// the concat's *result column* works (grouping by the concat *expression*
/// in a grouped SELECT is a separate, unneeded path).
#[test]
fn concat_column_then_outer_group_by() {
    let out = rows(
        "select id, count(*) as n from \
           (select concat('store_', s_store_id) as id from store) x \
         group by id order by id",
    );
    assert!(!out.is_empty());
    for r in &out {
        assert!(s(&r[0]).starts_with("store_"), "grouped id: {:?}", r[0]);
    }
}
