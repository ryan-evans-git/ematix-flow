//! Story 3 Phase-0 probe: does declaring PK constraints make Q10's GROUP BY columns
//! functionally-determined by `c_custkey` at the aggregate's input?
//!
//! The FD-aware operator (merged, inert) groups by `c_custkey` ALONE — correct only if
//! the other 6 group cols are FD-determined by it. This probe builds Q10's plan over
//! constrained MemTables and prints the aggregate input's functional dependencies +
//! which group cols `{c_custkey}` provably determines. Open question: `n_name` comes
//! from `nation` (source `n_nationkey` is projected out before the aggregate), so it may
//! NOT be covered → the single-key operator wouldn't fire on Q10 (would need a composite
//! {c_custkey, n_name} key). This probe answers that empirically. Read-only; no asserts.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::Constraints;
use datafusion::common::tree_node::TreeNode;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;

fn mt(fields: Vec<(&str, DataType)>, pk_col0: bool) -> Arc<MemTable> {
    let schema = Arc::new(Schema::new(
        fields
            .into_iter()
            .map(|(n, t)| Field::new(n, t, false))
            .collect::<Vec<_>>(),
    ));
    let mut t = MemTable::try_new(schema.clone(), vec![vec![]]).unwrap();
    if pk_col0 {
        // Declare column 0 as the primary key → DataFusion derives the FD
        // {col0} -> {all cols} on the scan.
        t = t.with_constraints(Constraints::new_unverified(vec![
            datafusion::common::Constraint::PrimaryKey(vec![0]),
        ]));
    }
    Arc::new(t)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    use DataType::*;
    // customer: PK c_custkey (col 0).
    ctx.register_table(
        "customer",
        mt(
            vec![
                ("c_custkey", Int64),
                ("c_name", Utf8),
                ("c_address", Utf8),
                ("c_nationkey", Int64),
                ("c_phone", Utf8),
                ("c_acctbal", Float64),
                ("c_comment", Utf8),
            ],
            true,
        ),
    )?;
    ctx.register_table(
        "orders",
        mt(
            vec![
                ("o_orderkey", Int64),
                ("o_custkey", Int64),
                ("o_orderdate", Date32),
            ],
            true,
        ),
    )?;
    ctx.register_table(
        "lineitem",
        mt(
            vec![
                ("l_orderkey", Int64),
                ("l_extendedprice", Float64),
                ("l_discount", Float64),
                ("l_returnflag", Utf8),
            ],
            false,
        ),
    )?;
    // nation: PK n_nationkey (col 0).
    ctx.register_table(
        "nation",
        mt(vec![("n_nationkey", Int64), ("n_name", Utf8)], true),
    )?;

    let sql = "select c_custkey, c_name, sum(l_extendedprice*(1-l_discount)) as revenue, \
        c_acctbal, n_name, c_address, c_phone, c_comment \
        from customer, orders, lineitem, nation \
        where c_custkey=o_custkey and l_orderkey=o_orderkey \
        and o_orderdate>=date '1993-10-01' and o_orderdate<date '1994-01-01' \
        and l_returnflag='R' and c_nationkey=n_nationkey \
        group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment";

    let plan = ctx.sql(sql).await?.into_optimized_plan()?;

    // Find the Aggregate node and inspect its input's functional dependencies.
    let mut found = false;
    plan.apply(|node| {
        if let LogicalPlan::Aggregate(agg) = node {
            found = true;
            let in_schema = agg.input.schema();
            println!("== Aggregate input schema fields ==");
            for (i, f) in in_schema.fields().iter().enumerate() {
                println!("  [{i}] {}", f.name());
            }
            println!("group_expr: {:?}", agg.group_expr);
            println!("\n== functional dependencies on aggregate input ==");
            let fd = in_schema.functional_dependencies();
            println!("{fd:?}");
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    })?;
    if !found {
        println!("no Aggregate node found in optimized plan");
        println!("{}", plan.display_indent());
    }
    Ok(())
}
