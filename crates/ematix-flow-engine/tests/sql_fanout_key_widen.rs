//! Gate fan-out key widening (the q72 sf10 lever): a post-join equality
//! `Eq(payload-of-a-dim-subtree, column-available-before-that-dim
//! attaches)` promotes into the dim's composite probe key, so the
//! expansion never produces the rows the residual would kill (q72's
//! cs⋈inventory on item_sk alone fans ~1300×; with week_seq in the key
//! it fans ~5×).
//!
//! Ground truths come through DIFFERENT machinery: the same join phrased
//! as two pre-joined derived tables equi-joined on BOTH keys (derived
//! materialization + plain composite-key join — no fan-out residual at
//! all). Over TPC-DS sf1.

use std::path::PathBuf;

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::plan::execute;

fn catalog() -> Catalog {
    let data = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/tpcds/data/sf1");
    let mut c = Catalog::new();
    for t in [
        "catalog_sales",
        "inventory",
        "date_dim",
        "warehouse",
        "item",
    ] {
        c.register_parquet(t, data.join(format!("{t}.parquet")))
            .expect("register");
    }
    c
}

fn run(c: &Catalog, sql: &str) -> Vec<Vec<ematix_flow_engine::expr::ScalarValue>> {
    execute(&bind_sql(sql, c).expect("bind"))
        .expect("exec")
        .rows
}

/// The q72 residual-equality shape: the d1.week = d2.week cycle equality
/// must not change results when promoted into the inventory probe key.
#[test]
fn widened_fanout_matches_prejoined_formulation() {
    let c = catalog();
    let widened = run(
        &c,
        "SELECT d1.d_week_seq, count(*) AS cnt
         FROM catalog_sales
           JOIN inventory ON (cs_item_sk = inv_item_sk)
           JOIN date_dim d1 ON (cs_sold_date_sk = d1.d_date_sk)
           JOIN date_dim d2 ON (inv_date_sk = d2.d_date_sk)
         WHERE d1.d_week_seq = d2.d_week_seq
           AND inv_quantity_on_hand < cs_quantity
           AND d1.d_year = 2000
           AND d1.d_moy = 3
         GROUP BY d1.d_week_seq
         ORDER BY d1.d_week_seq",
    );
    let reference = run(
        &c,
        "SELECT x.wk, count(*) AS cnt
         FROM (SELECT cs_item_sk AS ik, cs_quantity AS q, dd1.d_week_seq AS wk
               FROM catalog_sales JOIN date_dim dd1 ON cs_sold_date_sk = dd1.d_date_sk
               WHERE dd1.d_year = 2000 AND dd1.d_moy = 3) x,
              (SELECT inv_item_sk AS ik, inv_quantity_on_hand AS qoh, dd2.d_week_seq AS wk
                 FROM inventory JOIN date_dim dd2 ON inv_date_sk = dd2.d_date_sk) y
         WHERE x.ik = y.ik AND x.wk = y.wk
           AND y.qoh < x.q
         GROUP BY x.wk
         ORDER BY x.wk",
    );
    assert!(!widened.is_empty(), "shape matches rows at sf1");
    assert_eq!(widened, reference, "widened fan-out ≡ pre-joined join");
}

/// Grouping by a payload of the fan-out subtree (warehouse below
/// inventory) — widening must not disturb payload attachment.
#[test]
fn widened_fanout_subtree_payloads_survive() {
    let c = catalog();
    let widened = run(
        &c,
        "SELECT w_warehouse_name, count(*) AS cnt
         FROM catalog_sales
           JOIN inventory ON (cs_item_sk = inv_item_sk)
           JOIN warehouse ON (w_warehouse_sk = inv_warehouse_sk)
           JOIN date_dim d1 ON (cs_sold_date_sk = d1.d_date_sk)
           JOIN date_dim d2 ON (inv_date_sk = d2.d_date_sk)
         WHERE d1.d_week_seq = d2.d_week_seq
           AND inv_quantity_on_hand < cs_quantity
           AND d1.d_year = 2000
           AND d1.d_moy = 5
         GROUP BY w_warehouse_name
         ORDER BY w_warehouse_name",
    );
    let reference = run(
        &c,
        "SELECT y.wn, count(*) AS cnt
         FROM (SELECT cs_item_sk AS ik, cs_quantity AS q, dd1.d_week_seq AS wk
               FROM catalog_sales JOIN date_dim dd1 ON cs_sold_date_sk = dd1.d_date_sk
               WHERE dd1.d_year = 2000 AND dd1.d_moy = 5) x,
              (SELECT inv_item_sk AS ik, inv_quantity_on_hand AS qoh,
                        dd2.d_week_seq AS wk, w_warehouse_name AS wn
                 FROM inventory
                   JOIN date_dim dd2 ON inv_date_sk = dd2.d_date_sk
                   JOIN warehouse ON w_warehouse_sk = inv_warehouse_sk) y
         WHERE x.ik = y.ik AND x.wk = y.wk
           AND y.qoh < x.q
         GROUP BY y.wn
         ORDER BY y.wn",
    );
    assert!(!widened.is_empty());
    assert_eq!(widened, reference);
}

/// An equality between two NON-fan-out dims' payloads (item vs warehouse
/// would be meaningless; use d1/d3-style same-family compare through the
/// root) — exercises widening of a single-row dim probe, where a widened
/// key turns the probe miss into the filter.
#[test]
fn widened_single_row_dim_matches() {
    let c = catalog();
    let widened = run(
        &c,
        "SELECT count(*) AS cnt
         FROM catalog_sales
           JOIN date_dim d1 ON (cs_sold_date_sk = d1.d_date_sk)
           JOIN date_dim d3 ON (cs_ship_date_sk = d3.d_date_sk)
         WHERE d1.d_week_seq = d3.d_week_seq
           AND d1.d_year = 2000",
    );
    let reference = run(
        &c,
        "SELECT count(*) AS cnt
         FROM (SELECT cs_ship_date_sk AS sd, dd1.d_week_seq AS wk
               FROM catalog_sales JOIN date_dim dd1 ON cs_sold_date_sk = dd1.d_date_sk
               WHERE dd1.d_year = 2000) x
           JOIN (SELECT dd2.d_date_sk AS dsk, dd2.d_week_seq AS wk2 FROM date_dim dd2) y
             ON x.sd = y.dsk AND x.wk = y.wk2",
    );
    assert_eq!(widened, reference, "single-row dim widening ≡ pre-joined");
}
