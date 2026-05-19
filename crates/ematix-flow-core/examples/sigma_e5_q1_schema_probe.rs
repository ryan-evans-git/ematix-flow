//! Σ.E5 (2026-05-18) diagnostic: probe what data type each provider
//! actually emits for the Q1 string GROUP BY columns
//! (l_returnflag, l_linestatus).
//!
//! Hypothesis: parquet-rs emits `Dictionary(UInt32, Utf8)` internally
//! and only widens to `Utf8View` at the agg boundary, while our
//! streaming reader emits `Utf8View` directly with a 6M-row × 16B
//! view materialisation. That'd account for the ~2× Q1 gap.
//!
//! Run:
//!   cargo run --release -p ematix-flow-core --example sigma_e5_q1_schema_probe

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use futures_util::TryStreamExt;

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

async fn probe(label: &str, ctx: &SessionContext) {
    // Just project the two string columns; no agg. Collect the first
    // batch and inspect its data types.
    let df = ctx
        .sql("SELECT l_returnflag, l_linestatus FROM lineitem LIMIT 1")
        .await
        .unwrap();
    let physical = df.create_physical_plan().await.unwrap();
    let stream = datafusion::physical_plan::execute_stream(physical, ctx.task_ctx()).unwrap();
    let batches = stream.try_collect::<Vec<_>>().await.unwrap();
    println!("=== {label} ===");
    if let Some(b) = batches.first() {
        for (i, f) in b.schema().fields().iter().enumerate() {
            let arr = b.column(i);
            println!("  field[{i}] {:>15} = {:?}", f.name(), f.data_type());
            println!("             arr.data_type() = {:?}", arr.data_type());
        }
    } else {
        println!("  (no batches)");
    }
    println!();
}

fn make_ctx() -> SessionContext {
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

    let ctx_fp = make_ctx();
    ctx_fp
        .register_table(
            "lineitem",
            Arc::new(FastParquetTableProvider::try_new(path.clone()).unwrap()),
        )
        .unwrap();
    probe("FastParquet (parquet-rs)", &ctx_fp).await;

    let ctx_emat = make_ctx();
    ctx_emat
        .register_table(
            "lineitem",
            Arc::new(EmatixFastParquetTableProvider::try_new(path).unwrap()),
        )
        .unwrap();
    probe("EmatixFastParquet", &ctx_emat).await;
}
