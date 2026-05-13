//! Σ.D3 phase D follow-up: dump the DataFusion physical plan for
//! TPC-H Q1 against the SF=1 parquet, so the InjectFusedQ1Rule
//! pattern detector knows the exact node tree it needs to match.
//!
//! Q1 has 2 group-by keys, 4 SUMs, 3 AVGs (= 6 SUMs after `avg(x)` →
//! `sum(x)/count(x)` decomposition), and a COUNT(*). The shape is
//! materially more complex than Q6's single-SUM aggregate.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::fast_parquet::FastParquetTableProvider;

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

    let sql = std::fs::read_to_string("examples/tpch/queries/q01.sql")
        .unwrap_or_else(|_| std::fs::read_to_string("../examples/tpch/queries/q01.sql").unwrap());

    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(14));
    let prov = FastParquetTableProvider::try_new(path).unwrap();
    ctx.register_table("lineitem", Arc::new(prov)).unwrap();

    let df = ctx.sql(&sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();

    println!("--- Tree (names only) ---");
    print_tree(&plan, 0);
    println!();
    println!("--- Full displayable ---");
    println!("{}", displayable(plan.as_ref()).indent(true));
}
