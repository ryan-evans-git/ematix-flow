//! Σ.D3 phase D (Q3/Q5): bench Q3 and Q5 with and without their
//! respective auto-injection rules. Same SQL, only difference is
//! whether the optimiser rule rewrites the post-join aggregate stack
//! into a `FusedPostJoinExec`.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::fused_jit_rule::{InjectFusedQ3Rule, InjectFusedQ5Rule};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

const Q3_SQL: &str = "
    SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) AS revenue,
           o_orderdate, o_shippriority
    FROM customer, orders, lineitem
    WHERE c_mktsegment = 'BUILDING'
      AND c_custkey = o_custkey
      AND l_orderkey = o_orderkey
      AND o_orderdate < DATE '1995-03-15'
      AND l_shipdate > DATE '1995-03-15'
    GROUP BY l_orderkey, o_orderdate, o_shippriority
    ORDER BY revenue DESC, o_orderdate
";

const Q5_SQL: &str = "
    SELECT n_name, sum(l_extendedprice * (1 - l_discount)) AS revenue
    FROM customer, orders, lineitem, supplier, nation, region
    WHERE c_custkey = o_custkey
      AND l_orderkey = o_orderkey
      AND l_suppkey = s_suppkey
      AND c_nationkey = s_nationkey
      AND s_nationkey = n_nationkey
      AND n_regionkey = r_regionkey
      AND r_name = 'ASIA'
      AND o_orderdate >= DATE '1994-01-01'
      AND o_orderdate <  DATE '1995-01-01'
    GROUP BY n_name
    ORDER BY revenue DESC
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

async fn build_ctx_q3(dir: &str, with_rule: bool) -> SessionContext {
    let cfg = SessionConfig::new().with_target_partitions(14);
    let state = if with_rule {
        SessionStateBuilder::new()
            .with_config(cfg)
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(InjectFusedQ3Rule))
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

async fn build_ctx_q5(dir: &str, with_rule: bool) -> SessionContext {
    let cfg = SessionConfig::new().with_target_partitions(14);
    let state = if with_rule {
        SessionStateBuilder::new()
            .with_config(cfg)
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(InjectFusedQ5Rule))
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

async fn bench(label: &str, ctx: &SessionContext, sql: &str) -> f64 {
    for _ in 0..WARMUPS {
        let _ = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    }
    let mut times = Vec::with_capacity(TRIALS);
    for _ in 0..TRIALS {
        let start = Instant::now();
        let _ = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    let med = median(&mut times);
    let mean = times.iter().sum::<f64>() / times.len() as f64;
    let sd = stdev(&times, mean);
    println!("  {label:<48}  median {med:>6.2} ms ± {sd:>5.2}");
    med
}

fn report(name: &str, off: f64, on: f64) {
    let pct = 100.0 * (off - on) / off;
    if pct > 0.0 {
        println!("  ✓ {name} rule wins: {on:.2} ms vs {off:.2} ms ({pct:+.1}% faster)");
    } else {
        println!("  ✗ {name} rule loses or neutral: {on:.2} ms vs {off:.2} ms ({pct:+.1}%)");
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let dir = data_dir();
    println!("==> Σ.D3 phase D (Q3/Q5): InjectFusedQ3Rule / InjectFusedQ5Rule bench");
    println!("==> data: {dir}");
    println!("==> {TRIALS}-trial median after {WARMUPS} warm-ups");
    println!();

    let ctx_q3_off = build_ctx_q3(&dir, false).await;
    let ctx_q3_on = build_ctx_q3(&dir, true).await;
    println!("Q3:");
    let off3 = bench("FastParquet SQL (rule OFF)", &ctx_q3_off, Q3_SQL).await;
    let on3 = bench(
        "FastParquet SQL (rule ON — InjectFusedQ3Rule)",
        &ctx_q3_on,
        Q3_SQL,
    )
    .await;
    println!();
    report("Q3", off3, on3);
    println!();

    let ctx_q5_off = build_ctx_q5(&dir, false).await;
    let ctx_q5_on = build_ctx_q5(&dir, true).await;
    println!("Q5:");
    let off5 = bench("FastParquet SQL (rule OFF)", &ctx_q5_off, Q5_SQL).await;
    let on5 = bench(
        "FastParquet SQL (rule ON — InjectFusedQ5Rule)",
        &ctx_q5_on,
        Q5_SQL,
    )
    .await;
    println!();
    report("Q5", off5, on5);
    println!();
    println!("  Polars references:  Q3 = 44.57 ms,  Q5 = 10936 ms (Polars regresses)");
}
