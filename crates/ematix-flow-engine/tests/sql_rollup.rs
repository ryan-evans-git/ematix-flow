//! Gate `GROUP BY ROLLUP(...)` (q5/q80 and q18/22/27/36/67/77). ROLLUP
//! emits the base grouping plus each coarser term-prefix subtotal down to
//! the grand total, with rolled-up columns rendered NULL. A rolled-up NULL
//! is kept DISTINCT from a genuine NULL group (GroupKey::Rollup), so both
//! appear as separate rows. Values over TPC-DS sf1 store_sales, whose
//! ss_store_sk distinct set is {1,2,4,7,8,10,NULL} = 7 groups.

use std::path::PathBuf;

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::expr::ScalarValue;
use ematix_flow_engine::plan::execute;

fn catalog() -> Catalog {
    let data = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/tpcds/data/sf1");
    let mut c = Catalog::new();
    c.register_parquet("store_sales", data.join("store_sales.parquet"))
        .expect("register");
    c
}

fn rows(sql: &str) -> Vec<Vec<ScalarValue>> {
    let c = catalog();
    let q = bind_sql(sql, &c).expect("bind");
    execute(&q).expect("execute").rows
}

fn i(v: &ScalarValue) -> i64 {
    match v {
        ScalarValue::Int64(x) => *x,
        other => panic!("expected int, got {other:?}"),
    }
}

/// One-term ROLLUP adds exactly one grand-total row on top of the base
/// groups, and that row's aggregate is the sum of every base group's — the
/// defining ROLLUP property. store_sales has 7 store groups (incl. the
/// genuine-NULL store), so ROLLUP yields 8 rows; the grand total equals a
/// plain `count(*)`, and it does NOT collide with the genuine-NULL group.
#[test]
fn rollup_one_term_grand_total_is_sum_of_parts() {
    let base = rows("select ss_store_sk, count(*) as n from store_sales group by ss_store_sk");
    let rolled =
        rows("select ss_store_sk, count(*) as n from store_sales group by rollup(ss_store_sk)");
    assert_eq!(base.len(), 7, "distinct store groups incl. NULL");
    assert_eq!(rolled.len(), 8, "base groups + one grand-total row");

    let total = i(&rows("select count(*) as n from store_sales")[0][0]);
    let base_sum: i64 = base.iter().map(|r| i(&r[1])).sum();
    assert_eq!(base_sum, total, "sanity: base counts sum to the total");

    // Exactly one rolled row carries the grand total; its store key is NULL
    // and its count equals the whole table.
    let grand: Vec<&Vec<ScalarValue>> = rolled
        .iter()
        .filter(|r| i(&r[1]) == total && matches!(r[0], ScalarValue::Null))
        .collect();
    assert_eq!(grand.len(), 1, "exactly one NULL-keyed grand-total row");

    // The genuine-NULL store group survives alongside the grand total: two
    // rows have a NULL store key (the real NULL bucket + the subtotal).
    let null_keyed = rolled
        .iter()
        .filter(|r| matches!(r[0], ScalarValue::Null))
        .count();
    assert_eq!(
        null_keyed, 2,
        "genuine-NULL group and grand total both kept"
    );
}

/// Two-term ROLLUP produces exactly three grouping sets — (a,b), (a,·),
/// (·,·) — so its row count is the structural identity
/// `|ROLLUP(a,b)| = |GROUP BY a,b| + |GROUP BY a| + 1`. (Output alone can't
/// tell a rolled-up NULL from a genuine one — that needs GROUPING() — so
/// the count identity, not a NULL filter, is the robust check.) The grand
/// total still counts the whole table.
#[test]
fn rollup_two_terms_row_count_identity() {
    let ab = rows(
        "select ss_store_sk, ss_promo_sk, count(*) as n \
         from store_sales group by ss_store_sk, ss_promo_sk",
    )
    .len();
    let a = rows("select ss_store_sk, count(*) as n from store_sales group by ss_store_sk").len();
    let rolled = rows(
        "select ss_store_sk, ss_promo_sk, count(*) as n \
         from store_sales group by rollup(ss_store_sk, ss_promo_sk)",
    );
    assert_eq!(
        rolled.len(),
        ab + a + 1,
        "ROLLUP(a,b) = base (a,b) + subtotals (a,·) + grand total"
    );

    // The grand total (both keys rolled up) counts the whole table.
    let total = i(&rows("select count(*) as n from store_sales")[0][0]);
    let both_null_max = rolled
        .iter()
        .filter(|r| matches!(r[0], ScalarValue::Null) && matches!(r[1], ScalarValue::Null))
        .map(|r| i(&r[2]))
        .max()
        .expect("a both-NULL grand-total row");
    assert_eq!(both_null_max, total, "grand total counts the whole table");
}
