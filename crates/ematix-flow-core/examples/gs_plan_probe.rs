//! v2 S1.1 — grouping-set physical-shape probe.
//!
//! The whole S1 planner-interception hinges on reading DataFusion 53's
//! physical representation of `GROUP BY ROLLUP/CUBE/GROUPING SETS`
//! *exactly* as it emits it (see `PHASE_V2_S1_GROUPING_SETS.md` §7 risk
//! "DF's grouping-set physical shape" — "S1.1 must dump a real plan
//! first"). This probe pins that shape against a tiny hermetic table so
//! we don't need SF=1 data to design the operator:
//!
//!   * `PhysicalGroupBy.groups()` — the `Vec<Vec<bool>>` null-masks, one
//!     per grouping set. Bit semantics (is `true` = present or absent?)
//!     are the thing we must NOT guess.
//!   * `PhysicalGroupBy.expr()` — the full group-column universe.
//!   * the output schema — whether/where DF synthesises a `__grouping_id`
//!     column that backs `GROUPING()`/`GROUPING_ID`.
//!
//! Run: `cargo run -p ematix-flow-core --example gs_plan_probe`
//! (no data needed; prints the physical plan + decoded masks for
//! ROLLUP(a,b), CUBE(a,b), and explicit GROUPING SETS, plus a
//! GROUPING()-bearing query).

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use ematix_flow_core::preset;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = preset::session_context();

    // t(a: utf8, b: utf8, v: i64) — two group cols + one measure, with a
    // genuine data-NULL in `b` so we can later tell rolled-up NULL from
    // data NULL. Dictionary/Utf8View are what the fused group-key path
    // supports, but plain Utf8 is enough to pin the *shape*.
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Utf8, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                Some("x"),
                Some("x"),
                Some("y"),
                Some("y"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("p"),
                None, // genuine data NULL
                Some("q"),
                Some("q"),
            ])),
            Arc::new(Int64Array::from(vec![1_i64, 2, 3, 4])),
        ],
    )?;
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(table))?;

    for (label, sql) in [
        (
            "ROLLUP(a,b)",
            "SELECT a, b, sum(v) FROM t GROUP BY ROLLUP(a, b)",
        ),
        (
            "CUBE(a,b)",
            "SELECT a, b, sum(v) FROM t GROUP BY CUBE(a, b)",
        ),
        (
            "GROUPING SETS ((a,b),(a),())",
            "SELECT a, b, sum(v) FROM t GROUP BY GROUPING SETS ((a, b), (a), ())",
        ),
        (
            "ROLLUP + GROUPING()",
            "SELECT a, b, grouping(a) ga, grouping(b) gb, sum(v) FROM t GROUP BY ROLLUP(a, b)",
        ),
    ] {
        println!("\n================ {label} ================");
        println!("SQL: {sql}\n");
        let df = ctx.sql(sql).await?;
        let physical = df.create_physical_plan().await?;

        // Full physical tree (indented).
        println!("--- physical plan ---");
        println!("{}", displayable(physical.as_ref()).indent(true));

        // Walk the tree and decode every AggregateExec's PhysicalGroupBy.
        println!("--- decoded PhysicalGroupBy (per AggregateExec) ---");
        dump_group_by(physical.as_ref(), 0);
    }

    Ok(())
}

/// Recursively find `AggregateExec` nodes and print their group masks +
/// output schema so we can read DF's grouping-set encoding directly.
fn dump_group_by(plan: &dyn ExecutionPlan, depth: usize) {
    let pad = "  ".repeat(depth);
    if let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() {
        let gb = agg.group_expr();
        let names: Vec<String> = gb.expr().iter().map(|(_, name)| name.clone()).collect();
        println!("{pad}AggregateExec mode={:?}", agg.mode());
        println!("{pad}  group universe (expr): {names:?}");
        println!("{pad}  groups() masks (one Vec<bool> per set):");
        for (i, mask) in gb.groups().iter().enumerate() {
            println!("{pad}    set[{i}] = {mask:?}");
        }
        println!(
            "{pad}  output schema fields: {:?}",
            agg.schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>()
        );
    }
    for child in plan.children() {
        dump_group_by(child.as_ref(), depth + 1);
    }
}
