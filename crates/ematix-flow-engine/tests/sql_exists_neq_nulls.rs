//! Gate the counted `<>`-correlated EXISTS rewrite's NULL semantics
//! (q16's nullable `cs_warehouse_sk`, surfaced at sf10): a NULL outer
//! value makes `s <> outer_s` UNKNOWN for every inner row, so EXISTS is
//! FALSE (and NOT EXISTS TRUE) — the `cd ≥ 2` shortcut must not fire.
//! Expected counts computed independently (python row-walk over the
//! parquet, SQL three-valued semantics).

use std::path::PathBuf;

use ematix_flow_engine::bind::bind_sql;
use ematix_flow_engine::catalog::Catalog;
use ematix_flow_engine::expr::ScalarValue;
use ematix_flow_engine::plan::execute;

fn count(sql: &str) -> i64 {
    let data = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/tpcds/data/sf1");
    let mut c = Catalog::new();
    c.register_parquet("catalog_sales", data.join("catalog_sales.parquet"))
        .expect("register");
    let q = bind_sql(sql, &c).expect("bind");
    let r = execute(&q).expect("execute");
    match &r.rows[0][0] {
        ScalarValue::Int64(n) => *n,
        other => panic!("expected count, got {other:?}"),
    }
}

/// 7,089 catalog_sales rows have a NULL warehouse — every one of them
/// fails the `<>` EXISTS (NULL <> x is UNKNOWN) and lands in NOT EXISTS.
#[test]
fn neq_exists_null_outer_is_false() {
    let ex = count(
        "select count(*) from catalog_sales cs1 \
         where exists (select * from catalog_sales cs2 \
           where cs1.cs_order_number = cs2.cs_order_number \
             and cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)",
    );
    let nex = count(
        "select count(*) from catalog_sales cs1 \
         where not exists (select * from catalog_sales cs2 \
           where cs1.cs_order_number = cs2.cs_order_number \
             and cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)",
    );
    // Independent python oracle over the same parquet.
    assert_eq!(ex, 1_433_848, "EXISTS count");
    assert_eq!(nex, 7_700, "NOT EXISTS count");
    assert_eq!(ex + nex, 1_441_548, "the two partition every row");
}
