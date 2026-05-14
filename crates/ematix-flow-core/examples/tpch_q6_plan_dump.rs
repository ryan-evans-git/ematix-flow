//! Σ.D3 phase D (real): dump the DataFusion physical plan for TPC-H Q6
//! against the SF=1 parquet so we know the exact node tree the SQL-
//! pattern-detector needs to match. The detector + rewriter live in
//! `fused_jit_rule.rs`; this example just walks and prints.
//!
//! Usage:
//!   cargo run --release -p ematix-flow-core --example tpch_q6_plan_dump

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::fast_parquet::FastParquetTableProvider;

const Q6_SQL: &str = "
    SELECT sum(l_extendedprice * l_discount) AS revenue
    FROM lineitem
    WHERE l_shipdate >= DATE '1994-01-01'
      AND l_shipdate <  DATE '1995-01-01'
      AND l_discount BETWEEN 0.05 AND 0.07
      AND l_quantity <  24
";

fn data_dir() -> String {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    match std::env::var("TPCH_DATA_DIR") {
        Ok(s) => s,
        Err(_) => manifest
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("examples/tpch/data/sf1")
            .to_string_lossy()
            .into_owned(),
    }
}

fn print_tree(node: &Arc<dyn ExecutionPlan>, depth: usize) {
    let indent = "  ".repeat(depth);
    println!("{indent}{}", node.name());
    for child in node.children() {
        print_tree(child, depth + 1);
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let dir = data_dir();
    let path = format!("{dir}/lineitem.parquet");
    println!("==> Q6 plan dump for lineitem at {path}");
    println!();

    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(14));
    let prov = FastParquetTableProvider::try_new(path).unwrap();
    ctx.register_table("lineitem", Arc::new(prov)).unwrap();

    let df = ctx.sql(Q6_SQL).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();

    println!("--- Tree (names only) ---");
    print_tree(&plan, 0);
    println!();
    println!("--- Full displayable ---");
    println!("{}", displayable(plan.as_ref()).indent(true));
}
