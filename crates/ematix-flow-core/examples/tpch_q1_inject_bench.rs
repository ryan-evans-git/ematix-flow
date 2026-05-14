//! Σ.D3 phase D (real, Q1 variant): bench TPC-H Q1 with and without
//! `InjectFusedQ1Rule` to measure the gain from SQL-pattern auto-
//! injection on the group-by shape.
//!
//! Q1's downstream Filter+HashAggregate stack in DataFusion's default
//! plan is materially heavier than Q6's (8 aggregates, 2 group keys,
//! hash shuffle, partial→final reduction), so the fused replacement
//! has more headroom than Q6 did.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::fused_jit_rule::InjectFusedQ1Rule;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const Q1_SQL: &str = "
    SELECT
        l_returnflag, l_linestatus,
        sum(l_quantity) AS sum_qty,
        sum(l_extendedprice) AS sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
        avg(l_quantity) AS avg_qty,
        avg(l_extendedprice) AS avg_price,
        avg(l_discount) AS avg_disc,
        count(*) AS count_order
    FROM lineitem
    WHERE l_shipdate <= DATE '1998-09-02'
    GROUP BY l_returnflag, l_linestatus
    ORDER BY l_returnflag, l_linestatus
";

const TRIALS: usize = 15;
const WARMUPS: usize = 2;

fn data_path() -> String {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    match std::env::var("TPCH_DATA_DIR") {
        Ok(s) => format!("{s}/lineitem.parquet"),
        Err(_) => manifest
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("examples/tpch/data/sf1/lineitem.parquet")
            .to_string_lossy()
            .into_owned(),
    }
}

fn median(times: &mut [f64]) -> f64 {
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    times[times.len() / 2]
}

fn stdev(times: &[f64], mean: f64) -> f64 {
    let n = times.len();
    let var = times.iter().map(|t| (t - mean).powi(2)).sum::<f64>() / (n - 1) as f64;
    var.sqrt()
}

async fn build_ctx(path: &str, with_rule: bool) -> SessionContext {
    let cfg = SessionConfig::new().with_target_partitions(14);
    let state = if with_rule {
        SessionStateBuilder::new()
            .with_config(cfg)
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(InjectFusedQ1Rule))
            .build()
    } else {
        SessionStateBuilder::new()
            .with_config(cfg)
            .with_default_features()
            .build()
    };
    let ctx = SessionContext::new_with_state(state);
    let prov = FastParquetTableProvider::try_new(path).unwrap();
    ctx.register_table("lineitem", Arc::new(prov)).unwrap();
    ctx
}

async fn bench(label: &str, ctx: &SessionContext) -> f64 {
    for _ in 0..WARMUPS {
        let _ = ctx.sql(Q1_SQL).await.unwrap().collect().await.unwrap();
    }
    let mut times = Vec::with_capacity(TRIALS);
    for _ in 0..TRIALS {
        let start = Instant::now();
        let _ = ctx.sql(Q1_SQL).await.unwrap().collect().await.unwrap();
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    let med = median(&mut times);
    let mean = times.iter().sum::<f64>() / times.len() as f64;
    let sd = stdev(&times, mean);
    println!("  {label:<48}  median {med:>6.2} ms ± {sd:>5.2}");
    med
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let path = data_path();
    println!("==> Σ.D3 phase D (real, Q1): InjectFusedQ1Rule bench");
    println!("==> data: {path}");
    println!("==> {TRIALS}-trial median after {WARMUPS} warm-ups");
    println!();

    let ctx_off = build_ctx(&path, false).await;
    let ctx_on = build_ctx(&path, true).await;

    let off = bench("FastParquet SQL (rule OFF)", &ctx_off).await;
    let on = bench("FastParquet SQL (rule ON — InjectFusedQ1Rule)", &ctx_on).await;

    let pct = 100.0 * (off - on) / off;
    println!();
    if pct > 0.0 {
        println!("  ✓ rule wins: {on:.2} ms vs {off:.2} ms ({pct:+.1}% faster)");
    } else {
        println!("  ✗ rule loses or neutral: {on:.2} ms vs {off:.2} ms ({pct:+.1}%)");
    }
    println!();
    println!("  Reference:");
    println!("    Polars SF=1 Q1:                     ~35.2 ms (MemTable path)");
    println!("    Hand-constructed FusedFilterMultiAggExec: ~3.1 ms inner-loop");
}
