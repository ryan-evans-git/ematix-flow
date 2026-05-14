//! Σ.D3 phase D (Q12): dump DataFusion's physical plan for TPC-H Q12.
//!
//! Q12 is a 2-table join (orders ⋈ lineitem) with a complex 5-condition
//! filter, group-by l_shipmode (2 values: MAIL/SHIP), and two
//! CASE-WHEN aggregates counting urgent vs non-urgent orders.
//! Output is sorted by l_shipmode.

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

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let dir = data_dir();
    let sql = std::fs::read_to_string("examples/tpch/queries/q12.sql")
        .unwrap_or_else(|_| std::fs::read_to_string("../examples/tpch/queries/q12.sql").unwrap());
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
    println!("--- Tree (names only) ---");
    print_tree(&plan, 0);
    println!();
    println!("--- Full ---");
    println!("{}", displayable(plan.as_ref()).indent(true));
}
