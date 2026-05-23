//! Σ.Q diagnostic — run EXPLAIN ANALYZE on a single query so we get
//! per-operator wall-time + row counts. Used to identify which
//! operator is the SF=10 hot path before picking a lever.
//!
//! Usage:
//!   Q=18 TPCH_DATA_DIR=examples/tpch/data/sf10 \
//!     ./target/release/examples/sigma_q_explain_analyze

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::dedupe_aggregate_rule::DedupeAggregateForFloatDeterminism;
use ematix_flow_core::dict_aggregate_rule::EnableDictGroupCountRule;
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::fused_aggregate_filter_multi_agg_rule::InjectFilterMultiAggRule;
use ematix_flow_core::fused_aggregate_filter_sum_rule::InjectFilterSumRule;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let q: u8 = std::env::var("Q")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(18);
    let dir = std::env::var("TPCH_DATA_DIR")
        .unwrap_or_else(|_| "examples/tpch/data/sf10".to_string());

    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(14))
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(DedupeAggregateForFloatDeterminism::default()))
        .with_physical_optimizer_rule(Arc::new(EnableDictGroupCountRule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterMultiAggRule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterSumRule))
        .build();
    let ctx = SessionContext::new_with_state(state);

    for t in TPCH_TABLES {
        let path = PathBuf::from(&dir).join(format!("{t}.parquet"));
        if *t == "lineitem" {
            let prov = EmatixFastParquetTableProvider::try_new(path.to_string_lossy())?;
            ctx.register_table(*t, Arc::new(prov))?;
        } else {
            let prov = FastParquetTableProvider::try_new(path.to_string_lossy())?;
            ctx.register_table(*t, Arc::new(prov))?;
        }
    }

    let sql = std::fs::read_to_string(format!("examples/tpch/queries/q{q:02}.sql"))?;

    // Warmup (3 runs to stabilize)
    for _ in 0..3 {
        ctx.sql(&sql).await?.collect().await?;
    }

    // Now run with EXPLAIN ANALYZE
    let analyze_sql = format!("EXPLAIN ANALYZE {sql}");
    let df = ctx.sql(&analyze_sql).await?;
    let batches = df.collect().await?;
    println!("{}", pretty_format_batches(&batches)?);
    Ok(())
}
