//! Σ.E5.2 diagnostic: apples-to-apples Q1 SQL gate across three
//! providers, holding the optimiser rule fixed at *off* (DataFusion
//! default). The Σ.G.2f.3 gate (`tpch_q1_e2e_gate`) compares the
//! rule-OFF FastParquet path against the rule-ON EmatDictAware path,
//! which conflates the operator change with the provider change.
//!
//! Six modes total (rule × provider):
//!   * OFF × FastParquet (Utf8View)
//!   * OFF × EmatixFast (Utf8)
//!   * OFF × EmatixFast (Dict)
//!   * MultiAgg × FastParquet (Utf8View)
//!   * MultiAgg × EmatixFast (Utf8)        ← gap candidate
//!   * MultiAgg × EmatixFast (Dict)
//!
//! Reports median wall-clock and stddev per mode at 14 partitions.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::fused_aggregate_filter_multi_agg_rule::InjectFilterMultiAggRule;

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

const TRIALS: usize = 21;
const WARMUPS: usize = 3;

#[derive(Clone, Copy)]
enum Rule {
    Off,
    MultiAgg,
}
#[derive(Clone, Copy)]
enum Provider {
    Fast,        // Utf8View
    EmatUtf8,    // EmatixFast w/o dict-preservation
    EmatDict,    // EmatixFast w/ dict-preservation
}

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

async fn build_ctx(path: &str, rule: Rule, prov: Provider) -> SessionContext {
    let cfg = SessionConfig::new().with_target_partitions(14);
    let builder = SessionStateBuilder::new()
        .with_config(cfg)
        .with_default_features();
    let state = match rule {
        Rule::Off => builder.build(),
        Rule::MultiAgg => builder
            .with_physical_optimizer_rule(Arc::new(InjectFilterMultiAggRule))
            .build(),
    };
    let ctx = SessionContext::new_with_state(state);
    match prov {
        Provider::Fast => {
            let p = FastParquetTableProvider::try_new(path).unwrap();
            ctx.register_table("lineitem", Arc::new(p)).unwrap();
        }
        Provider::EmatUtf8 => {
            let p = EmatixFastParquetTableProvider::try_new(path).unwrap();
            ctx.register_table("lineitem", Arc::new(p)).unwrap();
        }
        Provider::EmatDict => {
            let p = EmatixFastParquetTableProvider::try_new(path)
                .unwrap()
                .with_dict_preservation(true);
            ctx.register_table("lineitem", Arc::new(p)).unwrap();
        }
    }
    ctx
}

async fn bench(label: &str, ctx: &SessionContext) -> (f64, f64) {
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
    println!("  {label:<46}  median {med:>6.2} ms ± {sd:>5.2}");
    (med, sd)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let path = data_path();
    println!("==> Σ.E5.2 apples-to-apples Q1 SQL (rule × provider)");
    println!("==> data: {path}");
    println!("==> {TRIALS}-trial median after {WARMUPS} warm-ups; 14 partitions\n");

    println!("--- Rule OFF (DataFusion default Filter + HashAggregate) ---");
    let ctx_off_fast = build_ctx(&path, Rule::Off, Provider::Fast).await;
    let ctx_off_eutf8 = build_ctx(&path, Rule::Off, Provider::EmatUtf8).await;
    let ctx_off_edict = build_ctx(&path, Rule::Off, Provider::EmatDict).await;
    let (off_fast, _) = bench("FastParquet (Utf8View)", &ctx_off_fast).await;
    let (off_eutf8, _) = bench("EmatixFast (Utf8)", &ctx_off_eutf8).await;
    let (off_edict, _) = bench("EmatixFast (Dict)", &ctx_off_edict).await;

    println!();
    println!("--- Rule ON (InjectFilterMultiAggRule) ---");
    let ctx_on_fast = build_ctx(&path, Rule::MultiAgg, Provider::Fast).await;
    let ctx_on_eutf8 = build_ctx(&path, Rule::MultiAgg, Provider::EmatUtf8).await;
    let ctx_on_edict = build_ctx(&path, Rule::MultiAgg, Provider::EmatDict).await;
    let (on_fast, _) = bench("FastParquet (Utf8View)", &ctx_on_fast).await;
    let (on_eutf8, _) = bench("EmatixFast (Utf8)", &ctx_on_eutf8).await;
    let (on_edict, _) = bench("EmatixFast (Dict)", &ctx_on_edict).await;

    println!();
    println!("--- Apples-to-apples deltas (provider gap, rule held fixed) ---");
    println!("  Rule OFF: EmatUtf8 vs Fast  : {:+.2} ms  ({:+.1}%)",
        off_eutf8 - off_fast, 100.0 * (off_eutf8 - off_fast) / off_fast);
    println!("  Rule OFF: EmatDict vs Fast  : {:+.2} ms  ({:+.1}%)",
        off_edict - off_fast, 100.0 * (off_edict - off_fast) / off_fast);
    println!("  Rule ON : EmatUtf8 vs Fast  : {:+.2} ms  ({:+.1}%)",
        on_eutf8 - on_fast, 100.0 * (on_eutf8 - on_fast) / on_fast);
    println!("  Rule ON : EmatDict vs Fast  : {:+.2} ms  ({:+.1}%)",
        on_edict - on_fast, 100.0 * (on_edict - on_fast) / on_fast);
}
