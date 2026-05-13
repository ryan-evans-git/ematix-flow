//! Σ.D3 phase D (Q14): dump the DataFusion physical plan for TPC-H Q14
//! against the SF=1 parquet, so the InjectFusedQ14Rule pattern detector
//! knows the exact node tree it needs to match.
//!
//! Q14 = scan(lineitem) + scan(part) + filter(shipdate range) + JOIN
//! + dual-SUM with CASE WHEN p_type LIKE 'PROMO%'. The fused
//! replacement (`FusedQ14FullExec`) owns BOTH scans and replaces the
//! hash join with a direct-indexed promo bitmap probe.

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
    let sql = std::fs::read_to_string("examples/tpch/queries/q14.sql")
        .unwrap_or_else(|_| std::fs::read_to_string("../examples/tpch/queries/q14.sql").unwrap());

    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(14));
    for t in TPCH_TABLES {
        let path = format!("{dir}/{t}.parquet");
        let prov = FastParquetTableProvider::try_new(path).unwrap();
        ctx.register_table(*t, Arc::new(prov)).unwrap();
    }

    let df = ctx.sql(&sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();

    println!("--- Tree (names only) ---");
    print_tree(&plan, 0);
    println!();
    println!("--- Full displayable ---");
    println!("{}", displayable(plan.as_ref()).indent(true));
}
