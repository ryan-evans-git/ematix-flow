//! Q06.b debug — does the ematix-flow / ematix-parquet path fail on
//! `lineitem_lz4.parquet` only in the masked-decode-i32 specialization,
//! or in the general LZ4_RAW read path too?
//!
//! Runs three increasingly demanding queries via ematix-flow:
//! 1. `SELECT COUNT(*)`  — no column decode at all.
//! 2. `SELECT l_shipdate LIMIT 100` — non-masked i32 column read.
//! 3. `SELECT ... WHERE l_shipdate >= ...` — the failing masked path.
//!
//! If (1) and (2) succeed but (3) fails, the bug is in the masked-
//! decode-i32 path specifically. That narrows the fix surface to
//! ematix-parquet's BridgeFilter / masked_decode_i32 — which means the
//! v0.14.0 LZ4 fix didn't propagate to this specialization.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;

#[tokio::main(flavor = "multi_thread", worker_threads = 14)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir =
        std::env::var("TPCH_DATA_DIR").unwrap_or_else(|_| "examples/tpch/data/sf10".to_string());
    let filename = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("TPCH_LINEITEM_FILE").ok())
        .unwrap_or_else(|| "lineitem_lz4.parquet".to_string());
    let path = PathBuf::from(&dir).join(&filename);
    if !path.exists() {
        return Err(format!("not found: {}", path.display()).into());
    }
    println!("Probing {}\n", path.display());

    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(14))
        .with_default_features()
        .build();
    let ctx = SessionContext::new_with_state(state);
    let prov = EmatixFastParquetTableProvider::try_new(path.to_string_lossy())?;
    ctx.register_table("lineitem", Arc::new(prov))?;

    async fn try_query(ctx: &SessionContext, label: &str, sql: &str) {
        print!("{label:<60}");
        match ctx.sql(sql).await {
            Ok(df) => match df.collect().await {
                Ok(batches) => {
                    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                    println!("OK   {rows} rows");
                }
                Err(e) => println!("FAIL  collect: {}", short(&e.to_string())),
            },
            Err(e) => println!("FAIL  plan: {}", short(&e.to_string())),
        }
    }

    try_query(&ctx, "Q1  COUNT(*)", "SELECT COUNT(*) FROM lineitem").await;
    try_query(
        &ctx,
        "Q2  l_shipdate LIMIT 100 (no filter)",
        "SELECT l_shipdate FROM lineitem LIMIT 100",
    )
    .await;
    try_query(
        &ctx,
        "Q3  l_quantity LIMIT 100 (no filter)",
        "SELECT l_quantity FROM lineitem LIMIT 100",
    )
    .await;
    try_query(
        &ctx,
        "Q4  l_extendedprice LIMIT 100 (no filter)",
        "SELECT l_extendedprice FROM lineitem LIMIT 100",
    )
    .await;
    try_query(
        &ctx,
        "Q5  COUNT(*) WHERE l_shipdate >= '1994-01-01'  (masked i32)",
        "SELECT COUNT(*) FROM lineitem WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'",
    )
    .await;
    try_query(
        &ctx,
        "Q6  COUNT(*) WHERE l_quantity < 24            (masked dec)",
        "SELECT COUNT(*) FROM lineitem WHERE l_quantity < 24",
    )
    .await;
    try_query(
        &ctx,
        "Q7  full Q06",
        &std::fs::read_to_string("examples/tpch/queries/q06.sql")?,
    )
    .await;

    Ok(())
}

fn short(s: &str) -> String {
    if s.len() > 220 {
        format!("{}…", &s[..220])
    } else {
        s.to_string()
    }
}
