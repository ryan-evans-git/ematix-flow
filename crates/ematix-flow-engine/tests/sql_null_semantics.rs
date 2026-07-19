//! Gate the three SQL-semantics fixes the TPC-DS oracle sweep surfaced:
//! `count(*)` over an otherwise-unreferenced table, SUM/MIN over zero
//! non-NULL values (SQL NULL, not 0), and ORDER BY putting NULLs LAST in
//! both directions (so they never displace a real row from a LIMIT).
//! Values over TPC-DS sf1; store_sk ∈ [1,10] with 129,461 NULL rows.

use std::path::PathBuf;

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::expr::ScalarValue;
use ematix_flow_engine::plan::execute;

fn catalog() -> Catalog {
    let data = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/tpcds/data/sf1");
    let mut c = Catalog::new();
    for t in ["store_sales", "customer", "date_dim"] {
        c.register_parquet(t, data.join(format!("{t}.parquet")))
            .expect("register");
    }
    c
}

fn run(sql: &str) -> Vec<Vec<ScalarValue>> {
    let c = catalog();
    let q = bind_sql(sql, &c).expect("bind");
    execute(&q).expect("execute").rows
}

/// `count(*)` touches no column, but the scan must still report the row
/// count (the empty-projection bug returned 0).
#[test]
fn count_star_over_unreferenced_table() {
    let r = run("select count(*) from customer");
    assert_eq!(r, vec![vec![ScalarValue::Int64(100_000)]]);
}

/// SUM over zero contributing rows is SQL NULL, not 0.0.
#[test]
fn sum_over_empty_is_null() {
    let r = run("select sum(ss_ext_sales_price) from store_sales where ss_item_sk = -1");
    assert_eq!(r, vec![vec![ScalarValue::Null]]);
    // count(*) over the same empty set is 0, never NULL.
    let r = run("select count(*) from store_sales where ss_item_sk = -1");
    assert_eq!(r, vec![vec![ScalarValue::Int64(0)]]);
}

/// A group whose summed column is entirely NULL sums to NULL; a MIN over
/// no non-NULL values is NULL too.
#[test]
fn min_over_empty_is_null() {
    let r = run("select min(ss_ext_sales_price) from store_sales where ss_item_sk = -1");
    assert_eq!(r, vec![vec![ScalarValue::Null]]);
}

/// NULL group keys sort LAST in both directions, so a `LIMIT 1` after an
/// ORDER BY never returns the (large) NULL bucket — ascending yields the
/// smallest store_sk (1), descending the largest (10).
#[test]
fn order_by_nulls_last_both_directions() {
    let asc = run(
        "select ss_store_sk from store_sales group by ss_store_sk order by ss_store_sk limit 1",
    );
    assert_eq!(asc, vec![vec![ScalarValue::Int64(1)]]);
    let desc = run("select ss_store_sk from store_sales group by ss_store_sk \
         order by ss_store_sk desc limit 1");
    assert_eq!(desc, vec![vec![ScalarValue::Int64(10)]]);
}

/// A 'YYYY-MM-DD' string bound compared against a Date column folds to
/// Date32 in BETWEEN and IN (the Spark texts elide `date`), instead of
/// panicking on a string-vs-date compare. January 2000 has 31 days; the
/// three-element IN set matches its three dates.
#[test]
fn date_string_coercion_in_between_and_in() {
    let btw =
        run("select count(*) from date_dim where d_date between '2000-01-01' and '2000-01-31'");
    assert_eq!(btw, vec![vec![ScalarValue::Int64(31)]]);
    let inl = run("select count(*) from date_dim \
         where d_date in ('2000-06-30', '2000-09-27', '2000-11-17')");
    assert_eq!(inl, vec![vec![ScalarValue::Int64(3)]]);
}
