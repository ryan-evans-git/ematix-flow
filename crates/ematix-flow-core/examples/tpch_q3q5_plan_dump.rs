//! Σ.D3 phase D (Q3/Q5): dump DataFusion physical plans for Q3 and Q5
//! so the matchers know the exact node shape.
//!
//! Usage:
//!     cargo run --release -p ematix-flow-core --example tpch_q3q5_plan_dump

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::fast_parquet::FastParquetTableProvider;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

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

async fn dump_query(name: &str, sql_path: &str) {
    let dir = data_dir();
    let sql = std::fs::read_to_string(sql_path)
        .unwrap_or_else(|_| std::fs::read_to_string(format!("../{sql_path}")).unwrap());
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(14));
    for t in TPCH_TABLES {
        let p = format!("{dir}/{t}.parquet");
        let prov = FastParquetTableProvider::try_new(p).unwrap();
        ctx.register_table(*t, Arc::new(prov)).unwrap();
    }
    let plan = ctx
        .sql(&sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    println!("==> {name} tree:");
    print_tree(&plan, 0);
    println!();
    println!("==> {name} full:");
    println!("{}", displayable(plan.as_ref()).indent(true));
    println!();
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    dump_query("Q3", "examples/tpch/queries/q03.sql").await;
    dump_query("Q5", "examples/tpch/queries/q05.sql").await;
}
