//! Gate CTE set-narrowing (the q95 sf10 lever): when EVERY reference to a
//! CTE sits inside an IN-subquery (set semantics — row multiplicity can
//! never matter) and uses only a subset of its columns, the CTE narrows to
//! `SELECT DISTINCT <used cols>`. q95's ws_wh self-join materialized 74.8M
//! (order, wh1, wh2) rows where both consumers needed only the ~600k
//! distinct order numbers. Over TPC-DS sf1.

use std::path::PathBuf;

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::logical::BoundQuery;
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

/// q95's shape in miniature: a fat self-join CTE consumed by two
/// IN-subqueries — one direct, one joined against another table.
const WIDE: &str = "WITH pairs AS (
  SELECT ss1.ss_ticket_number tick, ss1.ss_item_sk i1, ss2.ss_item_sk i2
  FROM store_sales ss1, store_sales ss2
  WHERE ss1.ss_ticket_number = ss2.ss_ticket_number
    AND ss1.ss_item_sk <> ss2.ss_item_sk
)
SELECT count(*), sum(sr_return_quantity)
FROM store_returns
WHERE sr_ticket_number IN (SELECT tick FROM pairs)
  AND sr_ticket_number IN (SELECT sr2.sr_ticket_number
      FROM store_returns sr2, pairs
      WHERE sr2.sr_ticket_number = pairs.tick)";

/// The CTE narrowed by hand — ground truth.
const MANUAL: &str = "WITH pairs AS (
  SELECT DISTINCT ss1.ss_ticket_number tick
  FROM store_sales ss1, store_sales ss2
  WHERE ss1.ss_ticket_number = ss2.ss_ticket_number
    AND ss1.ss_item_sk <> ss2.ss_item_sk
)
SELECT count(*), sum(sr_return_quantity)
FROM store_returns
WHERE sr_ticket_number IN (SELECT tick FROM pairs)
  AND sr_ticket_number IN (SELECT sr2.sr_ticket_number
      FROM store_returns sr2, pairs
      WHERE sr2.sr_ticket_number = pairs.tick)";

/// Output widths of every derived query in the tree whose FROM is the
/// store_sales self-join (the CTE, wherever it bound).
fn cte_widths(q: &BoundQuery, out: &mut Vec<usize>) {
    for d in &q.derived {
        let ss = d
            .tables
            .iter()
            .filter(|t| {
                matches!(&t.source, ematix_flow_engine::logical::TableSource::Parquet(p)
                if p.to_string_lossy().contains("store_sales"))
            })
            .count();
        if ss == 2 {
            out.push(d.output.len());
        }
        cte_widths(d, out);
    }
    for s in &q.subqueries {
        cte_widths(s, out);
    }
}

#[test]
fn cte_narrows_to_distinct_used_columns() {
    let c = catalog();
    let q = bind_sql(WIDE, &c).expect("bind");
    let mut widths = Vec::new();
    cte_widths(&q, &mut widths);
    assert!(!widths.is_empty(), "the self-join CTE binds somewhere");
    assert!(
        widths.iter().all(|&w| w == 1),
        "every reference sees the CTE narrowed to its 1 used column \
         (got widths {widths:?})"
    );
}

#[test]
fn narrowing_preserves_results() {
    let c = catalog();
    let auto = execute(&bind_sql(WIDE, &c).expect("bind")).expect("exec");
    let manual = execute(&bind_sql(MANUAL, &c).expect("bind")).expect("exec");
    assert_eq!(auto.rows, manual.rows, "narrowed ≡ hand-narrowed");
}

/// A CTE also referenced OUTSIDE set-semantics contexts (here: the outer
/// FROM) must keep its full width — dropping duplicate rows there would
/// change row counts.
#[test]
fn row_context_reference_blocks_narrowing() {
    let c = catalog();
    let sql = "WITH pairs AS (
      SELECT ss1.ss_ticket_number tick, ss1.ss_item_sk i1
      FROM store_sales ss1, store_sales ss2
      WHERE ss1.ss_ticket_number = ss2.ss_ticket_number
        AND ss1.ss_item_sk <> ss2.ss_item_sk
    )
    SELECT count(*) FROM pairs
    WHERE tick IN (SELECT tick FROM pairs)";
    let q = bind_sql(sql, &c).expect("bind");
    let mut widths = Vec::new();
    cte_widths(&q, &mut widths);
    assert!(
        widths.iter().all(|&w| w == 2),
        "a row-context reference keeps the CTE unnarrowed (got {widths:?})"
    );
}
