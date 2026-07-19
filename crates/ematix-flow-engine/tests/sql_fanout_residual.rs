//! Gate the fan-out early-residual filter (the q72 OOM fix): a
//! duplicate-key fan-out join whose taming predicate is a post-join
//! conjunct must produce EXACTLY the rows of the equivalent composite-key
//! join — the residual filters during expansion (bounded batches), not
//! after a full materialization. Over TPC-DS sf1.

use std::path::PathBuf;

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::expr::ScalarValue;
use ematix_flow_engine::plan::execute;

fn catalog() -> Catalog {
    let data = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/tpcds/data/sf1");
    let mut c = Catalog::new();
    for t in ["store_sales", "store_returns"] {
        c.register_parquet(t, data.join(format!("{t}.parquet")))
            .expect("register");
    }
    c
}

fn count(sql: &str) -> i64 {
    let c = catalog();
    let q = bind_sql(sql, &c).expect("bind");
    let r = execute(&q).expect("execute");
    match &r.rows[0][0] {
        ScalarValue::Int64(n) => *n,
        other => panic!("expected count, got {other:?}"),
    }
}

/// Joining on item_sk alone fans out (store_returns has many returns per
/// item); the ticket equality arrives as a separate WHERE conjunct — a
/// post-join residual the fan-out filters DURING expansion. The result
/// must equal the two-key composite join, where both equalities are edges.
#[test]
fn fanout_residual_matches_composite_key_join() {
    let composite = count(
        "select count(*) from store_sales, store_returns \
         where ss_item_sk = sr_item_sk and ss_ticket_number = sr_ticket_number",
    );
    // Force the residual shape: item edge + a ticket ARITHMETIC conjunct
    // (ss_ticket_number - sr_ticket_number = 0 is not an equijoin, so it
    // routes as a post-join filter over the fanned-out view).
    let residual = count(
        "select count(*) from store_sales, store_returns \
         where ss_item_sk = sr_item_sk and ss_ticket_number - sr_ticket_number = 0",
    );
    assert!(composite > 0, "the two-key join matches rows");
    assert_eq!(residual, composite, "early residual ≡ composite-key join");
}
