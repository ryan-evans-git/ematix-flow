//! Gate the ROLLUP grouping-set expansion against its definition: the
//! rollup result must be EXACTLY the union of the plain GROUP BY prefixes
//! (all keys, drop the last, …, grand total), each computed independently.
//! Guards the cascade rewrite of `add_rollup_levels` (the q67 sf10 lever) —
//! including the state-MERGE path where several finest groups collapse into
//! one subtotal, avg over merged states, and NULL-vs-subtotal distinctness.
//! Over TPC-DS sf1.

use std::path::PathBuf;

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::expr::ScalarValue;
use ematix_flow_engine::plan::execute;

fn catalog() -> Catalog {
    let data = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/tpcds/data/sf1");
    let mut c = Catalog::new();
    for t in ["store_sales", "item"] {
        c.register_parquet(t, data.join(format!("{t}.parquet")))
            .expect("register");
    }
    c
}

fn rows(c: &Catalog, sql: &str) -> Vec<Vec<ScalarValue>> {
    execute(&bind_sql(sql, c).expect("bind")).expect("exec").rows
}

/// Render rows for multiset comparison. Floats print at 9 significant
/// digits: a subtotal's avg merges partial sums in a different order than
/// the equivalent flat GROUP BY, so the last couple of ULPs legitimately
/// differ.
fn sorted(mut r: Vec<Vec<ScalarValue>>) -> Vec<String> {
    let cell = |v: &ScalarValue| match v {
        ScalarValue::Float64(f) => format!("F({f:.9e})"),
        other => format!("{other:?}"),
    };
    let mut out: Vec<String> = r
        .drain(..)
        .map(|row| row.iter().map(cell).collect::<Vec<_>>().join(","))
        .collect();
    out.sort();
    out
}

/// Three-level rollup with string keys, an avg (merge-sensitive), and real
/// NULLs in the key columns (i_category is nullable — a genuine NULL key
/// must stay distinct from a subtotal's NULL rendering only via the level
/// structure, which the plain-GROUP-BY union reproduces exactly).
#[test]
fn rollup_equals_union_of_prefix_groupings() {
    let c = catalog();
    let base = "FROM store_sales, item \
                WHERE ss_item_sk = i_item_sk AND ss_sold_date_sk IS NOT NULL \
                  AND ss_item_sk <= 1000";
    let agg = "count(*), sum(ss_quantity), avg(ss_sales_price)";
    let rolled = rows(
        &c,
        &format!(
            "SELECT i_category, i_class, i_brand, {agg} {base} \
             GROUP BY ROLLUP (i_category, i_class, i_brand)"
        ),
    );
    let mut expect = rows(
        &c,
        &format!("SELECT i_category, i_class, i_brand, {agg} {base} GROUP BY i_category, i_class, i_brand"),
    );
    expect.extend(
        rows(
            &c,
            &format!("SELECT i_category, i_class, {agg} {base} GROUP BY i_category, i_class"),
        )
        .into_iter()
        .map(|mut r| {
            r.insert(2, ScalarValue::Null);
            r
        }),
    );
    expect.extend(
        rows(&c, &format!("SELECT i_category, {agg} {base} GROUP BY i_category"))
            .into_iter()
            .map(|mut r| {
                r.insert(1, ScalarValue::Null);
                r.insert(2, ScalarValue::Null);
                r
            }),
    );
    expect.extend(rows(&c, &format!("SELECT {agg} {base}")).into_iter().map(
        |mut r| {
            r.insert(0, ScalarValue::Null);
            r.insert(1, ScalarValue::Null);
            r.insert(2, ScalarValue::Null);
            r
        },
    ));
    assert!(rolled.len() > 100, "shape produces real group counts");
    assert_eq!(
        sorted(rolled),
        sorted(expect),
        "ROLLUP ≡ union of prefix GROUP BYs"
    );
}

/// A multi-column rollup term — `ROLLUP((a, b), c)` drops c first, then
/// (a, b) TOGETHER — exercises the term-width bookkeeping in the cascade.
#[test]
fn rollup_multi_column_term() {
    let c = catalog();
    let base = "FROM store_sales, item \
                WHERE ss_item_sk = i_item_sk AND ss_item_sk <= 500";
    let rolled = rows(
        &c,
        &format!(
            "SELECT i_category, i_class, i_brand, count(*) {base} \
             GROUP BY ROLLUP ((i_category, i_class), i_brand)"
        ),
    );
    let mut expect = rows(
        &c,
        &format!("SELECT i_category, i_class, i_brand, count(*) {base} GROUP BY i_category, i_class, i_brand"),
    );
    expect.extend(
        rows(
            &c,
            &format!("SELECT i_category, i_class, count(*) {base} GROUP BY i_category, i_class"),
        )
        .into_iter()
        .map(|mut r| {
            r.insert(2, ScalarValue::Null);
            r
        }),
    );
    expect.extend(rows(&c, &format!("SELECT count(*) {base}")).into_iter().map(
        |mut r| {
            r.insert(0, ScalarValue::Null);
            r.insert(1, ScalarValue::Null);
            r.insert(2, ScalarValue::Null);
            r
        },
    ));
    assert_eq!(
        sorted(rolled),
        sorted(expect),
        "ROLLUP((a,b),c) ≡ its three grouping sets"
    );
}
