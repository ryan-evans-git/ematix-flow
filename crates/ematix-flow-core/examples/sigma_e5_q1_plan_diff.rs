//! Σ.E5 (2026-05-18) diagnostic: side-by-side Q01 physical plan
//! between `FastParquetTableProvider` (kept at parity, ~18 ms) and
//! `EmatixFastParquetTableProvider` (regressed to ~38 ms after typed
//! `partition_statistics` shipped).
//!
//! The bench surfaces the regression but doesn't tell us *which* node
//! diverged. Dumping the two plans side by side lets a human spot
//! whether the planner picked a different agg strategy, repartition
//! count, partial/final split, etc.
//!
//! Run:
//!   cargo run --release -p ematix-flow-core --example sigma_e5_q1_plan_diff
//!
//! Env:
//!   TPCH_DATA_DIR — override the SF=1 data dir (default
//!     `examples/tpch/data/sf1` relative to workspace root).

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::displayable;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
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

async fn dump_plan(label: &str, ctx: &SessionContext, sql: &str) {
    let df = ctx.sql(sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    println!("=== {label} ===");
    println!("{}", displayable(plan.as_ref()).indent(true));
    println!();
}

fn make_ctx() -> SessionContext {
    // Mirror the bench harness's session config: 14 target partitions.
    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(14))
        .with_default_features()
        .build();
    SessionContext::new_with_state(state)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let dir = data_dir();
    let path = format!("{dir}/lineitem.parquet");
    let sql = std::fs::read_to_string("examples/tpch/queries/q01.sql")
        .unwrap_or_else(|_| std::fs::read_to_string("../examples/tpch/queries/q01.sql").unwrap());

    // FastParquet
    let ctx_fp = make_ctx();
    let prov_fp = FastParquetTableProvider::try_new(path.clone()).unwrap();
    ctx_fp
        .register_table("lineitem", Arc::new(prov_fp))
        .unwrap();
    dump_plan("FastParquet (parquet-rs)", &ctx_fp, &sql).await;

    // EmatixFastParquet (defaults: streaming=true, dict_preservation=false,
    // late_mat=true)
    let ctx_emat = make_ctx();
    let prov_emat = EmatixFastParquetTableProvider::try_new(path).unwrap();
    ctx_emat
        .register_table("lineitem", Arc::new(prov_emat))
        .unwrap();
    dump_plan("EmatixFastParquet", &ctx_emat, &sql).await;
}
