//! Gate set-flavored side dedup (the q14 sf10 lever): a side combined by
//! UNION / INTERSECT / EXCEPT (anything but UNION ALL) contributes only
//! its DISTINCT rows, so it binds with set semantics — the projection
//! folds into a GROUP BY and dedup runs in the parallel aggregation.
//! Before this, q14's INTERSECT sides arrived at execute_set as ~28.8M
//! raw fact rows and were sorted single-threaded (7s of an 8.2s query).
//! Ground truths use the IN-subquery machinery — a different code path.
//! Over TPC-DS sf1.

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

#[test]
fn flavored_sides_fold_into_group_by() {
    let c = catalog();
    let q = bind_sql(
        "SELECT ss_store_sk, ss_item_sk FROM store_sales WHERE ss_quantity > 40
         INTERSECT
         SELECT ss_store_sk, ss_item_sk FROM store_sales WHERE ss_quantity < 10",
        &c,
    )
    .expect("bind");
    assert_eq!(q.group.len(), 2, "base side dedups via GROUP BY");
    assert_eq!(q.set_ops.len(), 1);
    assert_eq!(
        q.set_ops[0].1.group.len(),
        2,
        "INTERSECT side dedups via GROUP BY"
    );
}

#[test]
fn intersect_matches_in_subquery() {
    let c = catalog();
    let a = execute(
        &bind_sql(
            "SELECT ss_item_sk FROM store_sales WHERE ss_quantity > 40
             INTERSECT
             SELECT ss_item_sk FROM store_sales WHERE ss_quantity < 10
             ORDER BY ss_item_sk",
            &c,
        )
        .expect("bind"),
    )
    .expect("exec");
    let b = execute(
        &bind_sql(
            "SELECT DISTINCT ss_item_sk FROM store_sales
             WHERE ss_quantity > 40
               AND ss_item_sk IN (SELECT ss_item_sk FROM store_sales WHERE ss_quantity < 10)
             ORDER BY ss_item_sk",
            &c,
        )
        .expect("bind"),
    )
    .expect("exec");
    assert!(a.rows.len() > 100, "the shape matches rows at sf1");
    assert_eq!(a.rows, b.rows, "INTERSECT ≡ IN-subquery formulation");
}

#[test]
fn except_matches_not_in_subquery() {
    let c = catalog();
    let a = execute(
        &bind_sql(
            "SELECT ss_store_sk FROM store_sales WHERE ss_quantity > 90
             EXCEPT
             SELECT ss_store_sk FROM store_sales WHERE ss_quantity < 5
             ORDER BY ss_store_sk",
            &c,
        )
        .expect("bind"),
    )
    .expect("exec");
    let b = execute(
        &bind_sql(
            "SELECT DISTINCT ss_store_sk FROM store_sales
             WHERE ss_quantity > 90
               AND ss_store_sk NOT IN (SELECT ss_store_sk FROM store_sales WHERE ss_quantity < 5)
             ORDER BY ss_store_sk",
            &c,
        )
        .expect("bind"),
    )
    .expect("exec");
    assert_eq!(a.rows, b.rows, "EXCEPT ≡ NOT-IN formulation");
}

/// UNION ALL sides must NOT dedup — multiplicities are the semantics.
#[test]
fn union_all_keeps_multiplicity() {
    let c = catalog();
    let q = bind_sql(
        "SELECT ss_item_sk FROM store_sales WHERE ss_quantity > 95
         UNION ALL
         SELECT ss_item_sk FROM store_sales WHERE ss_quantity > 95",
        &c,
    )
    .expect("bind");
    assert!(q.group.is_empty(), "UNION ALL base stays plain rows");
    assert!(
        q.set_ops[0].1.group.is_empty(),
        "UNION ALL side stays plain"
    );
    let r = execute(&q).expect("exec");
    let single = execute(
        &bind_sql(
            "SELECT count(*) FROM store_sales WHERE ss_quantity > 95",
            &c,
        )
        .expect("bind"),
    )
    .expect("exec");
    let n = match single.rows[0][0] {
        ematix_flow_engine::expr::ScalarValue::Int64(n) => n as usize,
        _ => panic!("count"),
    };
    assert_eq!(r.rows.len(), 2 * n, "duplicates preserved");
}
