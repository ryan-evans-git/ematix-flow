//! v2 S3 (SETOP-sibling) — correlated-subquery contract + dim-push
//! regression guard.
//!
//! Pins that correlated `EXISTS` / `NOT EXISTS` subqueries execute
//! correctly on the **shared v2 session** (`preset::session_context()`),
//! and specifically guards the S3 fix: the `dim_join_pushdown`
//! (`FlowQueryPlanner` `dim_push`) rule used to drop an outer column
//! (`c.c_current_addr_sk`) when it rewrote the q10-shaped join, so
//! physical planning failed with `FieldNotFound`. The fix added a
//! schema-preservation guard that declines the rewrite when it would drop
//! an output column (`dim_join_pushdown.rs`). This test reproduces that
//! exact shape hermetically — a `customer ⋈ customer_address ⋈
//! customer_demographics` chain (with a *filtered* address dim, the
//! dim-push trigger) plus a correlated `EXISTS` over `store_sales` — so a
//! regression re-breaks it in CI without needing TPC-DS data.
//!
//! Hermetic: tiny in-memory tables, no data files.

use std::sync::Arc;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionContext;
use ematix_flow_core::preset;

fn int_table(ctx: &SessionContext, name: &str, cols: &[(&str, Vec<i64>)]) {
    let fields: Vec<Field> = cols
        .iter()
        .map(|(n, _)| Field::new(*n, DataType::Int64, false))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let arrays: Vec<_> = cols
        .iter()
        .map(|(_, v)| Arc::new(Int64Array::from(v.clone())) as _)
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table(name, Arc::new(table)).unwrap();
}

/// The q10 shape on the shared v2 session: `customer` joined to a
/// *filtered* `customer_address` (the dim-push trigger) and to
/// `customer_demographics`, with a correlated `EXISTS` over `store_sales`
/// referencing the outer `c_customer_sk`.
async fn q10_shaped_ctx() -> SessionContext {
    let ctx = preset::session_context();
    // customer: sk 1..4; addr_sk maps 1..4; cdemo maps all → 10.
    int_table(
        &ctx,
        "customer",
        &[
            ("c_customer_sk", vec![1, 2, 3, 4]),
            ("c_current_addr_sk", vec![1, 2, 3, 4]),
            ("c_current_cdemo_sk", vec![10, 10, 10, 10]),
        ],
    );
    int_table(
        &ctx,
        "customer_address",
        &[("ca_address_sk", vec![1, 2, 3, 4])],
    );
    int_table(&ctx, "customer_demographics", &[("cd_demo_sk", vec![10])]);
    // store_sales: customers 1 and 3 have sales.
    int_table(&ctx, "store_sales", &[("ss_customer_sk", vec![1, 1, 3])]);
    ctx
}

const Q10_SQL: &str = "SELECT c_customer_sk \
     FROM customer c, customer_address ca, customer_demographics \
     WHERE c.c_current_addr_sk = ca.ca_address_sk \
       AND cd_demo_sk = c.c_current_cdemo_sk \
       AND ca.ca_address_sk > 0 \
       AND EXISTS (SELECT * FROM store_sales WHERE ss_customer_sk = c.c_customer_sk)";

/// **The regression gate.** Before the fix this query failed at physical
/// planning with `No field named c.c_current_addr_sk`. It must now plan,
/// execute, and return exactly the customers with a store sale: {1, 3}.
#[tokio::test]
async fn correlated_exists_over_dim_join_plans_and_executes() {
    let ctx = q10_shaped_ctx().await;
    // Must build a physical plan (the exact thing that regressed).
    let df = ctx.sql(Q10_SQL).await.expect("logical plan");
    df.clone()
        .create_physical_plan()
        .await
        .expect("physical plan must succeed (dim-push must not drop c_current_addr_sk)");

    let batches = df.collect().await.expect("execute");
    let mut got: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            (0..b.num_rows()).map(|r| col.value(r)).collect::<Vec<_>>()
        })
        .collect();
    got.sort();
    assert_eq!(got, vec![1, 3], "correlated EXISTS result wrong");
}

/// The correlated `EXISTS` stays decorrelated to a **hash** semi/mark
/// join — never a `NestedLoopJoin` carrying a Semi/Anti/Mark mode (the
/// quadratic per-outer-row re-evaluation). Pins the plan-quality half of
/// the S3 verdict on the ematix session.
#[tokio::test]
async fn correlated_exists_stays_hash_not_nested_loop() {
    let ctx = q10_shaped_ctx().await;
    let plan = ctx
        .sql(Q10_SQL)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let tree = format!("{}", displayable(plan.as_ref()).indent(false));
    for line in tree.lines() {
        if line.contains("NestedLoopJoin")
            && (line.contains("Semi") || line.contains("Anti") || line.contains("Mark"))
        {
            panic!("correlated EXISTS lowered to a quadratic NestedLoop semi/anti join:\n{line}");
        }
    }
}
