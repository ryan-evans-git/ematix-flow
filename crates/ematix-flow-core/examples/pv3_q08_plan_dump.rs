//! PV.3 measurement — dump the REAL Q08 production physical plan so the
//! fusion recognizer is designed against the actual node tree, not an
//! imagined one. Builds the full production ctx (preset rules + the
//! FlowQueryPlanner logical rewrites), gets Q08's physical plan, and:
//!   1. prints the structural tree (`displayable().indent`)
//!   2. walks it and annotates each node the recognizer must classify:
//!      HashJoinExec (join_type / partition_mode / on-keys),
//!      ProjectionExec (expressions — the emit-spec source),
//!      AggregateExec (mode / group / aggr — the fragment root),
//!      EmatixFastParquetExec (Arc pointer identity + runtime sideband).
//!
//! Usage (SF=1 is plenty — plan SHAPE is scale-invariant):
//!   TPCH_DATA_DIR=examples/tpch/data/sf1 \
//!     cargo run --release -p ematix-flow-core --example pv3_q08_plan_dump

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("examples/tpch/data/sf1"));

    let builder = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(14))
        .with_default_features();
    let ctx = SessionContext::new_with_state(
        ematix_flow_core::preset::with_optimizer_rules(builder).build(),
    );
    register(&ctx, &data_dir)?;

    // Default to Q08; override with PV3_SQL_FILE=<path> to dump another shape.
    let sql = if let Ok(p) = std::env::var("PV3_SQL_FILE") {
        std::fs::read_to_string(&p)?
    } else {
        std::fs::read_to_string(data_dir.join("../../queries/q08.sql"))
            .or_else(|_| std::fs::read_to_string("examples/tpch/queries/q08.sql"))?
    };

    // Optimized LOGICAL plan (what the PV.3b recognizer will analyze).
    let df = ctx.sql(&sql).await?;
    let logical = df.clone().into_optimized_plan()?;
    println!("================ Q08 OPTIMIZED LOGICAL PLAN ================");
    println!("{}", logical.display_indent_schema());
    println!();

    let plan = df.create_physical_plan().await?;

    println!("================ Q08 PHYSICAL PLAN (production) ================");
    println!("{}", displayable(plan.as_ref()).indent(true));

    println!("\n================ STRUCTURAL WALK ================");
    let mut depth = 0;
    walk(plan.as_ref(), &mut depth);
    Ok(())
}

fn walk(node: &dyn ExecutionPlan, depth: &mut usize) {
    let pad = "  ".repeat(*depth);
    let name = node.name();
    let parts = node.output_partitioning().partition_count();

    let annot = if let Some(hj) = node.as_any().downcast_ref::<HashJoinExec>() {
        let on: Vec<String> = hj
            .on()
            .iter()
            .map(|(l, r)| format!("({l} = {r})"))
            .collect();
        format!(
            "  [join_type={:?} partition_mode={:?} on={} filter={} null_eq={:?}]",
            hj.join_type(),
            hj.partition_mode(),
            on.join(","),
            hj.filter().is_some(),
            hj.null_equality(),
        )
    } else if let Some(p) = node.as_any().downcast_ref::<ProjectionExec>() {
        let exprs: Vec<String> = p
            .expr()
            .iter()
            .map(|pe| format!("{}={}", pe.alias, pe.expr))
            .collect();
        format!("  [exprs: {}]", exprs.join(" | "))
    } else if let Some(a) = node.as_any().downcast_ref::<AggregateExec>() {
        let groups: Vec<String> = a
            .group_expr()
            .expr()
            .iter()
            .map(|(e, name)| format!("{name}={e}"))
            .collect();
        let aggs: Vec<String> = a.aggr_expr().iter().map(|e| e.name().to_string()).collect();
        format!(
            "  [mode={:?} group=[{}] aggr=[{}]]",
            a.mode(),
            groups.join(","),
            aggs.join(",")
        )
    } else if let Some(emat) =
        node.as_any()
            .downcast_ref::<ematix_flow_core::ematix_fast_parquet::EmatixFastParquetExec>()
    {
        format!(
            "  [EMAT SCAN cols={} sideband={} filter={}]",
            node.schema().fields().len(),
            emat.runtime_sideband().is_some(),
            emat.filter().is_some(),
        )
    } else if name.contains("FastParquet") || name.contains("Parquet") {
        format!("  [SCAN cols={}]", node.schema().fields().len())
    } else {
        String::new()
    };

    println!("{pad}{name} (parts={parts}){annot}");
    *depth += 1;
    for child in node.children() {
        walk(child.as_ref(), depth);
    }
    *depth -= 1;
}

fn register(ctx: &SessionContext, dir: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    for t in TPCH_TABLES {
        let p = dir.join(format!("{t}.parquet"));
        if *t == "lineitem" || *t == "orders" {
            ctx.register_table(
                *t,
                Arc::new(EmatixFastParquetTableProvider::try_new(
                    p.to_string_lossy(),
                )?),
            )?;
        } else {
            ctx.register_table(
                *t,
                Arc::new(FastParquetTableProvider::try_new(p.to_string_lossy())?),
            )?;
        }
    }
    Ok(())
}
