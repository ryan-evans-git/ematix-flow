//! Gate constant pushdown into single-referenced CTE group keys (the q78
//! sf10 lever): an outer `WHERE cte_out = const` on a CTE's GROUP BY key —
//! directly or transitively via join equalities, including into a LEFT
//! join's nullable side — must inject the constant into the CTE's inner
//! WHERE, so the aggregation never materializes the filtered-away groups.
//! Correctness is gated against the manually pre-filtered rewrite; the
//! pushdown itself is asserted on the bound IR (the CTE's date_dim scan
//! carries a filter). Over TPC-DS sf1.

use std::path::PathBuf;

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::logical::BoundQuery;
use ematix_flow_engine::plan::execute;

fn catalog() -> Catalog {
    let data = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/tpcds/data/sf1");
    let mut c = Catalog::new();
    for t in ["store_sales", "store_returns", "date_dim"] {
        c.register_parquet(t, data.join(format!("{t}.parquet")))
            .expect("register");
    }
    c
}

/// q78's shape in miniature: two aggregating CTEs over date_dim-joined
/// facts, LEFT-joined on (year, item), outer constant on the preserved
/// side's year. The constant must reach BOTH CTEs' date_dim scans — ss
/// directly, sr transitively through the LEFT ON equality (nullable side).
const PUSHED: &str = "WITH ss AS (
  SELECT d_year AS ss_sold_year, ss_item_sk item_sk, sum(ss_quantity) ss_qty
  FROM store_sales JOIN date_dim ON ss_sold_date_sk = d_date_sk
  GROUP BY d_year, ss_item_sk
), sr AS (
  SELECT d_year AS sr_ret_year, sr_item_sk ret_item_sk, sum(sr_return_quantity) sr_qty
  FROM store_returns JOIN date_dim ON sr_returned_date_sk = d_date_sk
  GROUP BY d_year, sr_item_sk
)
SELECT item_sk, ss_qty, sr_qty
FROM ss LEFT JOIN sr ON sr_ret_year = ss_sold_year AND ret_item_sk = item_sk
WHERE ss_sold_year = 2000
ORDER BY item_sk, ss_qty";

/// The same query with the constant written inside both CTEs by hand —
/// the ground truth the pushdown must reproduce.
const MANUAL: &str = "WITH ss AS (
  SELECT d_year AS ss_sold_year, ss_item_sk item_sk, sum(ss_quantity) ss_qty
  FROM store_sales JOIN date_dim ON ss_sold_date_sk = d_date_sk
  WHERE d_year = 2000
  GROUP BY d_year, ss_item_sk
), sr AS (
  SELECT d_year AS sr_ret_year, sr_item_sk ret_item_sk, sum(sr_return_quantity) sr_qty
  FROM store_returns JOIN date_dim ON sr_returned_date_sk = d_date_sk
  WHERE d_year = 2000
  GROUP BY d_year, sr_item_sk
)
SELECT item_sk, ss_qty, sr_qty
FROM ss LEFT JOIN sr ON sr_ret_year = ss_sold_year AND ret_item_sk = item_sk
WHERE ss_sold_year = 2000
ORDER BY item_sk, ss_qty";

/// Every derived query in the tree whose FROM includes `date_dim`.
fn date_dim_filters(q: &BoundQuery) -> Vec<bool> {
    let mut out = Vec::new();
    for d in &q.derived {
        for t in &d.tables {
            if t.name.eq_ignore_ascii_case("date_dim") {
                out.push(t.filter.is_some());
            }
        }
        out.extend(date_dim_filters(d));
    }
    out
}

#[test]
fn constant_reaches_both_cte_date_dim_scans() {
    let c = catalog();
    let q = bind_sql(PUSHED, &c).expect("bind");
    let filters = date_dim_filters(&q);
    assert_eq!(filters.len(), 2, "two CTEs scan date_dim");
    assert!(
        filters.iter().all(|f| *f),
        "outer year constant must be pushed into BOTH CTEs' date_dim scans \
         (got {filters:?})"
    );
}

#[test]
fn pushdown_preserves_results() {
    let c = catalog();
    let auto = execute(&bind_sql(PUSHED, &c).expect("bind")).expect("exec");
    let manual = execute(&bind_sql(MANUAL, &c).expect("bind")).expect("exec");
    assert!(!auto.rows.is_empty(), "the shape matches rows at sf1");
    assert_eq!(auto.rows, manual.rows, "pushdown ≡ manual pre-filter");
}

/// A CTE referenced twice must NOT receive a reference-specific constant —
/// the second reference would silently inherit the first's filter.
#[test]
fn shared_cte_is_left_alone() {
    let c = catalog();
    let sql = "WITH ss AS (
      SELECT d_year AS yr, ss_item_sk item_sk, sum(ss_quantity) qty
      FROM store_sales JOIN date_dim ON ss_sold_date_sk = d_date_sk
      GROUP BY d_year, ss_item_sk
    )
    SELECT a.item_sk, a.qty, b.qty
    FROM ss a JOIN ss b ON a.item_sk = b.item_sk AND b.yr = a.yr + 1
    WHERE a.yr = 2000
    ORDER BY a.item_sk";
    let q = bind_sql(sql, &c).expect("bind");
    let filters = date_dim_filters(&q);
    assert!(
        filters.iter().all(|f| !*f),
        "a multiply-referenced CTE keeps an unfiltered date_dim scan \
         (got {filters:?})"
    );
}
