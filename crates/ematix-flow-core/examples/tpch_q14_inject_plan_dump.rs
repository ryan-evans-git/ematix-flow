//! Diagnostic: dump the Q14 physical plan AFTER InjectFusedQ14Rule
//! runs, to see what (if anything) DataFusion wraps around the fused
//! exec. Used to debug why the rule-on bench gets 17.27 ms but the
//! hand-constructed FusedQ14FullExec gets 15.15 ms.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::fused_jit_rule::InjectFusedQ14Rule;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

const Q14_SQL: &str = "
    SELECT
        100.00 * sum(case when p_type like 'PROMO%'
                          then l_extendedprice * (1 - l_discount)
                          else 0 end)
        / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
    FROM lineitem, part
    WHERE l_partkey = p_partkey
      AND l_shipdate >= DATE '1995-09-01'
      AND l_shipdate <  DATE '1995-10-01'
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
    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(14))
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(InjectFusedQ14Rule))
        .build();
    let ctx = SessionContext::new_with_state(state);
    for t in TPCH_TABLES {
        let p = format!("{dir}/{t}.parquet");
        let prov = FastParquetTableProvider::try_new(p).unwrap();
        ctx.register_table(*t, Arc::new(prov)).unwrap();
    }
    let plan = ctx.sql(Q14_SQL).await.unwrap().create_physical_plan().await.unwrap();
    println!("--- Tree (names only) ---");
    print_tree(&plan, 0);
    println!();
    println!("--- Full ---");
    println!("{}", displayable(plan.as_ref()).indent(true));
}
