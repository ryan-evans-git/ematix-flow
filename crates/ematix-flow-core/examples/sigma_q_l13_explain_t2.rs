//! Σ.Q.L13 follow-up — dump EXPLAIN ANALYZE for T2 (decode + date
//! filter) on both ematix-parquet and DataFusion-native-parquet
//! providers, so we can see whether the filter is being pushed INTO
//! the scan or sits as a FilterExec ABOVE it.

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;

const T2_SQL: &str = "SELECT \
       sum(l_extendedprice * (1.0 - l_discount)), \
       sum(l_orderkey % 7) + sum(l_suppkey % 11) \
     FROM lineitem \
     WHERE l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'";

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir =
        std::env::var("TPCH_DATA_DIR").unwrap_or_else(|_| "examples/tpch/data/sf10".to_string());
    let lineitem_path = PathBuf::from(&dir).join("lineitem.parquet");

    // ----- Emat -----
    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(14))
        .with_default_features()
        .build();
    let ctx_emat = SessionContext::new_with_state(state);
    let prov = EmatixFastParquetTableProvider::try_new(lineitem_path.to_string_lossy())?;
    ctx_emat.register_table("lineitem", Arc::new(prov))?;

    println!("===== EXPLAIN (logical + physical) — EmatixFastParquet =====");
    let df = ctx_emat.sql(&format!("EXPLAIN {T2_SQL}")).await?;
    let batches = df.collect().await?;
    println!("{}", pretty_format_batches(&batches)?);
    println!();
    println!("===== EXPLAIN ANALYZE — EmatixFastParquet =====");
    // warmup
    let _ = ctx_emat.sql(T2_SQL).await?.collect().await?;
    let df = ctx_emat.sql(&format!("EXPLAIN ANALYZE {T2_SQL}")).await?;
    let batches = df.collect().await?;
    println!("{}", pretty_format_batches(&batches)?);

    // ----- FastP -----
    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(14))
        .with_default_features()
        .build();
    let ctx_fp = SessionContext::new_with_state(state);
    let prov = FastParquetTableProvider::try_new(lineitem_path.to_string_lossy())?;
    ctx_fp.register_table("lineitem", Arc::new(prov))?;

    println!();
    println!("===== EXPLAIN (logical + physical) — FastParquet (DataFusion native) =====");
    let df = ctx_fp.sql(&format!("EXPLAIN {T2_SQL}")).await?;
    let batches = df.collect().await?;
    println!("{}", pretty_format_batches(&batches)?);
    println!();
    println!("===== EXPLAIN ANALYZE — FastParquet (DataFusion native) =====");
    let _ = ctx_fp.sql(T2_SQL).await?.collect().await?;
    let df = ctx_fp.sql(&format!("EXPLAIN ANALYZE {T2_SQL}")).await?;
    let batches = df.collect().await?;
    println!("{}", pretty_format_batches(&batches)?);

    Ok(())
}
