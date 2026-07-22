//! Gate the offset-equijoin promotion (the q59/q2 sf10 lever): an outer
//! `WHERE a_col = b_col ± N` between two derived FROM items must become a
//! REAL join edge — the constant folds into a hidden computed column on
//! b's side, both deriveds materialize (inlining one re-opens a join
//! cycle whose broken edge lands back in the fan-out residual path), and
//! the executor runs a composite-key hash join instead of fanning out
//! per-key duplicates through the batched residual. Over TPC-DS sf1.

use std::path::PathBuf;

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::logical::TableSource;
use ematix_flow_engine::plan::execute;

fn catalog() -> Catalog {
    let data = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/tpcds/data/sf1");
    let mut c = Catalog::new();
    for t in ["store_sales", "date_dim"] {
        c.register_parquet(t, data.join(format!("{t}.parquet")))
            .expect("register");
    }
    c
}

/// q59's outer shape in miniature: two deriveds over one CTE, joined on a
/// plain store equality AND a week-offset equality.
const OFFSET: &str = "WITH wss AS (
  SELECT d_week_seq, ss_store_sk, sum(ss_sales_price) wk_sales
  FROM store_sales, date_dim
  WHERE d_date_sk = ss_sold_date_sk
  GROUP BY d_week_seq, ss_store_sk
)
SELECT d_week_seq1, wk1 / wk2
FROM
  (SELECT wss.d_week_seq d_week_seq1, ss_store_sk store1, wk_sales wk1 FROM wss) y,
  (SELECT wss.d_week_seq d_week_seq2, ss_store_sk store2, wk_sales wk2 FROM wss) x
WHERE store1 = store2 AND d_week_seq1 = d_week_seq2 - 52
ORDER BY d_week_seq1, wk1 / wk2";

/// The same join with the offset PRE-COMPUTED inside x by hand — plain
/// two-key equijoin, the ground truth the promotion must reproduce.
const MANUAL: &str = "WITH wss AS (
  SELECT d_week_seq, ss_store_sk, sum(ss_sales_price) wk_sales
  FROM store_sales, date_dim
  WHERE d_date_sk = ss_sold_date_sk
  GROUP BY d_week_seq, ss_store_sk
)
SELECT d_week_seq1, wk1 / wk2
FROM
  (SELECT wss.d_week_seq d_week_seq1, ss_store_sk store1, wk_sales wk1 FROM wss) y,
  (SELECT wss.d_week_seq - 52 jk, ss_store_sk store2, wk_sales wk2 FROM wss) x
WHERE store1 = store2 AND d_week_seq1 = jk
ORDER BY d_week_seq1, wk1 / wk2";

#[test]
fn offset_conjunct_becomes_a_join_edge() {
    let c = catalog();
    let q = bind_sql(OFFSET, &c).expect("bind");
    assert_eq!(q.tables.len(), 2, "both deriveds materialize (no inline)");
    assert!(
        q.tables
            .iter()
            .all(|t| matches!(t.source, TableSource::Derived(_))),
        "outer FROM is exactly the two derived tables"
    );
    assert_eq!(q.edges.len(), 2, "store equality AND week offset are edges");
    assert!(
        q.post_filter.is_none(),
        "no residual — the offset promoted to a composite-key equijoin"
    );
}

#[test]
fn offset_promotion_preserves_results() {
    let c = catalog();
    let auto = execute(&bind_sql(OFFSET, &c).expect("bind")).expect("exec");
    let manual = execute(&bind_sql(MANUAL, &c).expect("bind")).expect("exec");
    assert!(!auto.rows.is_empty(), "the shape matches rows at sf1");
    assert_eq!(auto.rows, manual.rows, "promotion ≡ manual computed key");
}

/// An offset WITHIN one derived (same table both sides) must NOT fire —
/// it is an ordinary filter, not a join key.
#[test]
fn same_table_offset_is_left_alone() {
    let c = catalog();
    let sql = "SELECT d_week_seq FROM
      (SELECT d_week_seq, d_month_seq FROM date_dim) y
    WHERE d_week_seq = d_month_seq - 52
    ORDER BY d_week_seq";
    let q = bind_sql(sql, &c).expect("bind");
    let r = execute(&q).expect("exec");
    // Correctness only — the filter shape may inline; it must not panic
    // or produce a bogus join.
    let manual = execute(
        &bind_sql(
            "SELECT d_week_seq FROM date_dim WHERE d_week_seq = d_month_seq - 52 ORDER BY d_week_seq",
            &c,
        )
        .expect("bind"),
    )
    .expect("exec");
    assert_eq!(r.rows, manual.rows);
}
