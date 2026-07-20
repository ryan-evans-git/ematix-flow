//! Gate LEFT-join WHERE-clause routing over TPC-DS sf1:
//!   - an equijoin conjunct touching the nullable side demotes the LEFT
//!     join to INNER (q93's `sr_reason_sk = r_reason_sk`);
//!   - `IS NULL` on the nullable side is an ANTI-join — it must see the
//!     NULL-filled payload, so it routes as a post-join filter (q78's
//!     `WHERE wr_order_number IS NULL`).

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

/// Anti-join identity: matched + unmatched = every driving row. The
/// two-key LEFT join preserves all store_sales rows; `IS NULL` on the
/// nullable side keeps exactly the unmatched ones, `IS NOT NULL`
/// (NULL-rejecting → demoted to INNER) exactly the matched ones.
#[test]
fn anti_join_plus_semi_join_covers_all_rows() {
    let total = count("select count(*) from store_sales");
    let matched = count(
        "select count(*) from store_sales left outer join store_returns \
           on (ss_ticket_number = sr_ticket_number and ss_item_sk = sr_item_sk) \
         where sr_ticket_number is not null",
    );
    let unmatched = count(
        "select count(*) from store_sales left outer join store_returns \
           on (ss_ticket_number = sr_ticket_number and ss_item_sk = sr_item_sk) \
         where sr_ticket_number is null",
    );
    assert!(matched > 0, "some sales have returns");
    assert!(unmatched > 0, "some sales have no returns");
    // Each sale matches at most one return on (ticket, item), so the LEFT
    // join is row-preserving and the two filters partition it exactly.
    assert_eq!(matched + unmatched, total, "anti + semi partition the rows");
}

/// RIGHT OUTER is a mirrored LEFT: `store_returns RIGHT JOIN store_sales`
/// preserves store_sales exactly as `store_sales LEFT JOIN store_returns`
/// does, so the same anti/semi partition identity must hold, and each
/// RIGHT form must equal its LEFT twin row-for-row (here: the count).
#[test]
fn right_outer_mirrors_left() {
    let total = count("select count(*) from store_sales");
    // Same predicates, sides swapped: store_sales is the preserved side.
    let matched = count(
        "select count(*) from store_returns right outer join store_sales \
           on (ss_ticket_number = sr_ticket_number and ss_item_sk = sr_item_sk) \
         where sr_ticket_number is not null",
    );
    let unmatched = count(
        "select count(*) from store_returns right outer join store_sales \
           on (ss_ticket_number = sr_ticket_number and ss_item_sk = sr_item_sk) \
         where sr_ticket_number is null",
    );
    assert!(matched > 0 && unmatched > 0);
    assert_eq!(matched + unmatched, total, "RIGHT anti + semi partition all rows");

    // RIGHT twin equals the LEFT original exactly (matched-only count).
    let left_matched = count(
        "select count(sr_ticket_number) from store_sales left outer join store_returns \
           on (ss_ticket_number = sr_ticket_number and ss_item_sk = sr_item_sk)",
    );
    let right_matched = count(
        "select count(sr_ticket_number) from store_returns right outer join store_sales \
           on (ss_ticket_number = sr_ticket_number and ss_item_sk = sr_item_sk)",
    );
    assert_eq!(left_matched, right_matched, "RIGHT == mirrored LEFT (matched count)");
}
