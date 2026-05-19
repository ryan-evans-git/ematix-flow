//! Σ.E5 plan-diff investigation: Q01 with EmatixFastParquet provider
//! under Inexact vs Exact pushdown. Runs the query once per mode,
//! prints `EXPLAIN ANALYZE`, then 8 timed trials + median.
//!
//! Driven by `EMAT_EXACT_PUSHDOWN`. To compare modes, run twice:
//!
//!     cargo run --release -p ematix-flow-core --example tpch_q01_exact_diff
//!     EMAT_EXACT_PUSHDOWN=1 cargo run --release -p ematix-flow-core --example tpch_q01_exact_diff
//!
//! Purpose: identify what DataFusion's planner changes when it sees
//! the filter declared Exact — projection elision, FilterExec removal,
//! join order changes, etc. Findings feed back into a "keep Exact's
//! benefits without its regression" plan.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::dict_aggregate_rule::EnableDictGroupCountRule;
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fused_aggregate_filter_multi_agg_rule::InjectFilterMultiAggRule;
use ematix_flow_core::fused_aggregate_filter_sum_rule::InjectFilterSumRule;

const Q01_SQL: &str = "
select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from lineitem
where l_shipdate <= date '1998-12-01' - interval '90' day
group by l_returnflag, l_linestatus
order by l_returnflag, l_linestatus
";

const TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

async fn build_ctx(data_dir: &Path) -> SessionContext {
    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(14))
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(EnableDictGroupCountRule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterMultiAggRule))
        .with_physical_optimizer_rule(Arc::new(InjectFilterSumRule))
        .build();
    let ctx = SessionContext::new_with_state(state);
    for t in TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        let prov = EmatixFastParquetTableProvider::try_new(path.to_string_lossy().to_string())
            .expect("provider");
        ctx.register_table(*t, Arc::new(prov)).unwrap();
    }
    ctx
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let data_dir: PathBuf = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("examples/tpch/data/sf1"));

    let mode = if std::env::var_os("EMAT_EXACT_PUSHDOWN").is_some() {
        "Exact"
    } else {
        "Inexact"
    };

    println!("==> Σ.E5 Q01 plan-diff: pushdown={mode}");
    println!("==> data: {}", data_dir.display());
    println!();

    let ctx = build_ctx(&data_dir).await;

    // EXPLAIN ANALYZE — run once first so dynamic stats land.
    let _warm = ctx.sql(Q01_SQL).await.unwrap().collect().await.unwrap();

    let explain_sql = format!("EXPLAIN ANALYZE {Q01_SQL}");
    let batches = ctx
        .sql(&explain_sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    println!("--- EXPLAIN ANALYZE ({mode}) ---");
    let formatted = datafusion::arrow::util::pretty::pretty_format_batches(&batches).unwrap();
    println!("{formatted}");
    println!();

    // Also EXPLAIN (no ANALYZE) to see the optimized plan structure
    // without instrumentation noise.
    let explain_sql = format!("EXPLAIN {Q01_SQL}");
    let batches = ctx
        .sql(&explain_sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    println!("--- EXPLAIN ({mode}) ---");
    let formatted = datafusion::arrow::util::pretty::pretty_format_batches(&batches).unwrap();
    println!("{formatted}");
    println!();

    // Timed trials.
    const WARMUPS: usize = 3;
    const TRIALS: usize = 8;
    for _ in 0..WARMUPS {
        let _ = ctx.sql(Q01_SQL).await.unwrap().collect().await.unwrap();
    }
    let mut times_ms = Vec::with_capacity(TRIALS);
    for _ in 0..TRIALS {
        let t = Instant::now();
        let _ = ctx.sql(Q01_SQL).await.unwrap().collect().await.unwrap();
        times_ms.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    times_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = times_ms[times_ms.len() / 2];
    let min = times_ms[0];
    let max = times_ms[times_ms.len() - 1];
    println!("--- Q01 wall-clock ({mode}) ---");
    println!("  trials: {times_ms:?}");
    println!("  median: {median:.2} ms  (min {min:.2}, max {max:.2})");
}
