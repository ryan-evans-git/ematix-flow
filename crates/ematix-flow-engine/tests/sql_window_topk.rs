//! Gate the rank top-K window prune (the q67 sf10 lever): an outer
//! `WHERE rk <= K` on a derived's lone rank()/row_number() window arms a
//! per-partition prune — the K-th best row is found by linear selection
//! and only rows ordering at-or-before it feed the sort/projection, so a
//! 5.8M-row window input shrinks to ~K·partitions before any real work.
//! Rank values are provably identical on the pruned prefix (rank depends
//! only on strictly-better rows, all kept; ties of the threshold row are
//! kept and trimmed by the still-applied outer filter). dense_rank is
//! excluded — its rank-K frontier extends past the K-th best ROW. Over
//! TPC-DS sf1.

use std::path::PathBuf;

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::plan::execute;

fn catalog() -> Catalog {
    let data = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/tpcds/data/sf1");
    let mut c = Catalog::new();
    c.register_parquet("store_sales", data.join("store_sales.parquet"))
        .expect("register");
    c
}

/// q67's outer shape in miniature: rank over a grouped derived, outer
/// rank filter. Ties in sum(ss_quantity) are common at sf1, so the
/// threshold-tie path is exercised.
fn topk_sql(pred: &str) -> String {
    format!(
        "SELECT ss_store_sk, ss_item_sk, sumq, rk FROM (
           SELECT ss_store_sk, ss_item_sk, sumq,
                  rank() OVER (PARTITION BY ss_store_sk ORDER BY sumq DESC) rk
           FROM (SELECT ss_store_sk, ss_item_sk, sum(ss_quantity) sumq
                 FROM store_sales GROUP BY ss_store_sk, ss_item_sk) dw1
         ) dw2
         WHERE {pred}
         ORDER BY ss_store_sk, rk, ss_item_sk, sumq"
    )
}

#[test]
fn rank_filter_arms_topk() {
    let c = catalog();
    let q = bind_sql(&topk_sql("rk <= 10"), &c).expect("bind");
    assert_eq!(q.derived.len(), 1, "dw2 materializes");
    assert_eq!(q.derived[0].windows.len(), 1, "dw2 carries the rank window");
    assert_eq!(
        q.derived[0].windows[0].top_k,
        Some(10),
        "rk <= 10 arms the prune"
    );
    // Strict < arms K-1.
    let q = bind_sql(&topk_sql("rk < 10"), &c).expect("bind");
    assert_eq!(q.derived[0].windows[0].top_k, Some(9));
}

#[test]
fn topk_prune_preserves_results() {
    let c = catalog();
    // `rk + 0 <= 10` filters identically but does not match the pattern —
    // the full-sort path is the ground truth.
    let pruned = execute(&bind_sql(&topk_sql("rk <= 10"), &c).expect("bind")).expect("exec");
    let full = execute(&bind_sql(&topk_sql("rk + 0 <= 10"), &c).expect("bind")).expect("exec");
    assert!(pruned.rows.len() > 50, "several stores × top 10");
    assert_eq!(pruned.rows, full.rows, "top-K prune ≡ full window");
}

/// dense_rank must NOT arm the prune: its rank-≤-K frontier can extend
/// beyond the K-th best row.
#[test]
fn dense_rank_is_left_alone() {
    let c = catalog();
    let sql = "SELECT ss_store_sk, rk FROM (
       SELECT ss_store_sk, dense_rank() OVER (PARTITION BY ss_store_sk ORDER BY sumq DESC) rk
       FROM (SELECT ss_store_sk, ss_item_sk, sum(ss_quantity) sumq
             FROM store_sales GROUP BY ss_store_sk, ss_item_sk) dw1
     ) dw2 WHERE rk <= 5 ORDER BY ss_store_sk, rk";
    let q = bind_sql(sql, &c).expect("bind");
    assert_eq!(
        q.derived[0].windows[0].top_k, None,
        "dense_rank stays unpruned"
    );
}
