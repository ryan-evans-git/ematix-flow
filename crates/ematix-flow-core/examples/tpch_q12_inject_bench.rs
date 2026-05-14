//! Σ.D3 phase D (Q12): bench Q12 with and without InjectFusedQ12Rule.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::fused_jit_rule::InjectFusedQ12Rule;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

const Q12_SQL: &str = "
    SELECT l_shipmode,
        sum(case when o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH'
                 then 1 else 0 end) AS high_line_count,
        sum(case when o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH'
                 then 1 else 0 end) AS low_line_count
    FROM orders, lineitem
    WHERE o_orderkey = l_orderkey
      AND l_shipmode IN ('MAIL', 'SHIP')
      AND l_commitdate < l_receiptdate
      AND l_shipdate < l_commitdate
      AND l_receiptdate >= DATE '1994-01-01'
      AND l_receiptdate <  DATE '1995-01-01'
    GROUP BY l_shipmode
    ORDER BY l_shipmode
";

const TRIALS: usize = 15;
const WARMUPS: usize = 2;

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

fn median(times: &mut [f64]) -> f64 {
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    times[times.len() / 2]
}

fn stdev(times: &[f64], mean: f64) -> f64 {
    let n = times.len();
    let var = times.iter().map(|t| (t - mean).powi(2)).sum::<f64>() / (n - 1) as f64;
    var.sqrt()
}

async fn build_ctx(dir: &str, with_rule: bool) -> SessionContext {
    let cfg = SessionConfig::new().with_target_partitions(14);
    let state = if with_rule {
        SessionStateBuilder::new()
            .with_config(cfg)
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(InjectFusedQ12Rule))
            .build()
    } else {
        SessionStateBuilder::new()
            .with_config(cfg)
            .with_default_features()
            .build()
    };
    let ctx = SessionContext::new_with_state(state);
    for t in TPCH_TABLES {
        let p = format!("{dir}/{t}.parquet");
        let prov = FastParquetTableProvider::try_new(p).unwrap();
        ctx.register_table(*t, Arc::new(prov)).unwrap();
    }
    ctx
}

async fn bench(label: &str, ctx: &SessionContext) -> f64 {
    for _ in 0..WARMUPS {
        let _ = ctx.sql(Q12_SQL).await.unwrap().collect().await.unwrap();
    }
    let mut times = Vec::with_capacity(TRIALS);
    for _ in 0..TRIALS {
        let start = Instant::now();
        let _ = ctx.sql(Q12_SQL).await.unwrap().collect().await.unwrap();
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
    let dir = data_dir();
    println!("==> Σ.D3 phase D (Q12): InjectFusedQ12Rule bench");
    println!("==> data: {dir}");
    println!("==> {TRIALS}-trial median after {WARMUPS} warm-ups");
    println!();

    let off = bench("FastParquet SQL (rule OFF)", &build_ctx(&dir, false).await).await;
    let on = bench(
        "FastParquet SQL (rule ON — InjectFusedQ12Rule)",
        &build_ctx(&dir, true).await,
    )
    .await;

    let pct = 100.0 * (off - on) / off;
    println!();
    if pct > 0.0 {
        println!("  ✓ rule wins: {on:.2} ms vs {off:.2} ms ({pct:+.1}% faster)");
    } else {
        println!("  ✗ rule loses or neutral: {on:.2} ms vs {off:.2} ms ({pct:+.1}%)");
    }
    println!();
    println!("  Polars SF=1 Q12 reference: 22.01 ms");
}
