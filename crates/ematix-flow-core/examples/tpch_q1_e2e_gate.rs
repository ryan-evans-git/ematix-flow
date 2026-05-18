//! Σ.G.2f.3 end-to-end gate: full TPC-H Q1 SQL wall-clock through
//! three optimizer-rule configurations.
//!
//! * **Rule OFF** — DataFusion's default Filter+HashAggregate stack
//!   over our FastParquet scan. Baseline for "what users get without
//!   any of our rules".
//! * **InjectFusedQ1Rule** — Σ.D3-era Q1-specific matcher that
//!   constructs `FusedAggregateExec<Q1Spec>` (Cranelift JIT'd, baked
//!   four-arm `(returnflag, linestatus)` match). The benchmark
//!   ceiling we're trying to retire.
//! * **InjectFilterMultiAggRule** — Σ.G.2f.3's runtime-configured
//!   matcher that constructs `FusedAggregateExec<FilterMultiAggSpec>`
//!   with no per-query baking. Routes through the perfect-hash dict
//!   template once dict preservation lands on the scan.
//!
//! Decision criterion: if the new rule's SQL wall-clock is within
//! ~5–10% of `InjectFusedQ1Rule`'s wall-clock, the .3 deletion of
//! `Q1Spec` + `InjectFusedQ1Rule` is justified — the generic path
//! gives users the same perf without the TPC-H-specific hardcoding.
//!
//! Usage:
//!     cargo run --release -p ematix-flow-core --example tpch_q1_e2e_gate

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::fused_aggregate_filter_multi_agg_rule::InjectFilterMultiAggRule;
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

const TRIALS: usize = 21;
const WARMUPS: usize = 3;

#[derive(Clone, Copy)]
enum Mode {
    /// FastParquet (Utf8View columns), no injection rule — DataFusion default.
    Off,
    /// FastParquet (Utf8View columns), InjectFusedQ1Rule attached.
    Q1Rule,
    /// FastParquet (Utf8View columns), InjectFilterMultiAggRule attached.
    /// Routes through `process_batch_generic` (the generic Utf8View
    /// hash-grouped template) at the spec level.
    MultiAggRuleUtf8View,
    /// Emat (bridge path, Dictionary columns),
    /// InjectFilterMultiAggRule attached. Routes through the
    /// PerfectHashAggregate template because the 3-distinct
    /// returnflag dict is well under the cardinality threshold.
    MultiAggRuleDict,
    /// Σ.E5.1.b: Emat (streaming Arrow reader, Utf8View columns),
    /// InjectFilterMultiAggRule attached. Same spec template as the
    /// FastParquet+MultiAggRule row, but the table provider streams
    /// 65K-row Utf8View batches out of `EmatArrowBatchReader`
    /// (per-column parallel decode) instead of the bridge's whole-RG
    /// emission shape.
    MultiAggRuleStreamingUtf8View,
    /// Σ.E5.1.b: Emat (streaming Arrow reader, Dictionary columns),
    /// InjectFilterMultiAggRule attached. Routes through the
    /// PerfectHashAggregate template. Direct comparison with the
    /// `MultiAggRuleDict` (bridge) row.
    MultiAggRuleStreamingDict,
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

async fn build_ctx(path: &str, mode: Mode) -> SessionContext {
    let cfg = SessionConfig::new().with_target_partitions(14);
    let builder = SessionStateBuilder::new()
        .with_config(cfg)
        .with_default_features();
    let state = match mode {
        Mode::Off => builder.build(),
        Mode::Q1Rule => builder
            .with_physical_optimizer_rule(Arc::new(InjectFusedQ1Rule))
            .build(),
        Mode::MultiAggRuleUtf8View
        | Mode::MultiAggRuleDict
        | Mode::MultiAggRuleStreamingUtf8View
        | Mode::MultiAggRuleStreamingDict => builder
            .with_physical_optimizer_rule(Arc::new(InjectFilterMultiAggRule))
            .build(),
    };
    let ctx = SessionContext::new_with_state(state);
    match mode {
        Mode::MultiAggRuleDict => {
            let prov = EmatixFastParquetTableProvider::try_new(path)
                .unwrap()
                .with_dict_preservation(true);
            ctx.register_table("lineitem", Arc::new(prov)).unwrap();
        }
        Mode::MultiAggRuleStreamingUtf8View => {
            // Σ.E5.1.b: streaming Arrow reader, Utf8View columns (no
            // dict preservation). Apples-to-apples with the FastParquet
            // + MultiAggRule baseline at the schema level.
            let prov = EmatixFastParquetTableProvider::try_new(path)
                .unwrap()
                .with_streaming_arrow_reader(true);
            ctx.register_table("lineitem", Arc::new(prov)).unwrap();
        }
        Mode::MultiAggRuleStreamingDict => {
            // Σ.E5.1.b: streaming Arrow reader, Dictionary columns.
            // Direct comparison with the bridge MultiAggRuleDict row.
            let prov = EmatixFastParquetTableProvider::try_new(path)
                .unwrap()
                .with_dict_preservation(true)
                .with_streaming_arrow_reader(true);
            ctx.register_table("lineitem", Arc::new(prov)).unwrap();
        }
        _ => {
            let prov = FastParquetTableProvider::try_new(path).unwrap();
            ctx.register_table("lineitem", Arc::new(prov)).unwrap();
        }
    }
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
    println!("  {label:<52}  median {med:>6.2} ms ± {sd:>5.2}");
    med
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let path = data_path();
    println!("==> Σ.G.2f.3 end-to-end gate: TPC-H Q1 SQL wall-clock");
    println!("==> data: {path}");
    println!("==> {TRIALS}-trial median after {WARMUPS} warm-ups; 14 partitions");
    println!();

    let ctx_off = build_ctx(&path, Mode::Off).await;
    let ctx_q1 = build_ctx(&path, Mode::Q1Rule).await;
    let ctx_multi_utf8 = build_ctx(&path, Mode::MultiAggRuleUtf8View).await;
    let ctx_multi_dict = build_ctx(&path, Mode::MultiAggRuleDict).await;
    let ctx_multi_stream_utf8 = build_ctx(&path, Mode::MultiAggRuleStreamingUtf8View).await;
    let ctx_multi_stream_dict = build_ctx(&path, Mode::MultiAggRuleStreamingDict).await;

    let off = bench("FastParquet (rule OFF — DataFusion default)", &ctx_off).await;
    let q1 = bench("FastParquet + InjectFusedQ1Rule", &ctx_q1).await;
    let multi_utf8 = bench(
        "FastParquet + InjectFilterMultiAggRule (Utf8View)",
        &ctx_multi_utf8,
    )
    .await;
    let multi_dict = bench(
        "EmatBridge + InjectFilterMultiAggRule (Dict)",
        &ctx_multi_dict,
    )
    .await;
    let multi_stream_utf8 = bench(
        "EmatStreaming + InjectFilterMultiAggRule (Utf8View)",
        &ctx_multi_stream_utf8,
    )
    .await;
    let multi_stream_dict = bench(
        "EmatStreaming + InjectFilterMultiAggRule (Dict)",
        &ctx_multi_stream_dict,
    )
    .await;

    let pct_q1_vs_off = 100.0 * (off - q1) / off;
    let pct_multi_utf8_vs_off = 100.0 * (off - multi_utf8) / off;
    let pct_multi_dict_vs_off = 100.0 * (off - multi_dict) / off;
    let pct_multi_dict_vs_q1 = 100.0 * (multi_dict - q1) / q1;
    // Σ.E5.1.b paired deltas: streaming reader vs the corresponding
    // bridge row (the apples-to-apples comparison the task asked for).
    let pct_stream_utf8_vs_bridge_utf8 = 100.0 * (multi_stream_utf8 - multi_utf8) / multi_utf8;
    let pct_stream_dict_vs_bridge_dict = 100.0 * (multi_stream_dict - multi_dict) / multi_dict;
    let pct_stream_utf8_vs_off = 100.0 * (off - multi_stream_utf8) / off;
    let pct_stream_dict_vs_off = 100.0 * (off - multi_stream_dict) / off;
    println!();
    println!(
        "  InjectFusedQ1Rule (Utf8View)       vs OFF:    {q1:>6.2} ms vs {off:>6.2} ms  ({pct_q1_vs_off:+.1}% faster)"
    );
    println!(
        "  MultiAgg Utf8View (generic templ)  vs OFF:    {multi_utf8:>6.2} ms vs {off:>6.2} ms  ({pct_multi_utf8_vs_off:+.1}% faster)"
    );
    println!(
        "  MultiAgg Dictionary (perfect-hash) vs OFF:    {multi_dict:>6.2} ms vs {off:>6.2} ms  ({pct_multi_dict_vs_off:+.1}% faster)"
    );
    println!(
        "  MultiAgg Dictionary vs Q1Rule:                {multi_dict:>6.2} ms vs {q1:>6.2} ms  ({pct_multi_dict_vs_q1:+.1}%)"
    );
    println!();
    println!("  --- Σ.E5.1.b streaming reader (new path) ---");
    println!("  EmatBridge    (Utf8View, FastParquet path):    {multi_utf8:>6.2} ms");
    println!(
        "  EmatStreaming (Utf8View, new reader):          {multi_stream_utf8:>6.2} ms     ({pct_stream_utf8_vs_bridge_utf8:+.1}% vs FastParquet bridge)"
    );
    println!("  EmatBridge    (Dict):                          {multi_dict:>6.2} ms");
    println!(
        "  EmatStreaming (Dict, new reader):              {multi_stream_dict:>6.2} ms     ({pct_stream_dict_vs_bridge_dict:+.1}% vs Emat bridge)"
    );
    println!(
        "  EmatStreaming (Utf8View) vs OFF:               ({pct_stream_utf8_vs_off:+.1}% faster)"
    );
    println!(
        "  EmatStreaming (Dict)     vs OFF:               ({pct_stream_dict_vs_off:+.1}% faster)"
    );
    println!();
    if pct_multi_dict_vs_q1.abs() <= 10.0 {
        println!("  ✓ MultiAgg (Dict) ≈ Q1Rule (within 10%) — Q1Spec deletion JUSTIFIED.");
    } else if pct_multi_dict_vs_q1 < 0.0 {
        println!("  ✓✓ MultiAgg (Dict) beats Q1Rule — gate PASSES outright.");
    } else {
        println!(
            "  ✗ MultiAgg (Dict) {pct_multi_dict_vs_q1:+.1}% vs Q1Rule — investigate before deletion."
        );
    }
    // Σ.E5.1.b ready-to-flip decision: streaming Utf8View should be
    // within ±5% of the FastParquet+MultiAgg baseline to flip default.
    println!();
    if pct_stream_utf8_vs_bridge_utf8.abs() <= 5.0 {
        println!(
            "  ✓ EmatStreaming (Utf8View) within ±5% of FastParquet baseline — \
             ready to flip default in follow-up."
        );
    } else if pct_stream_utf8_vs_bridge_utf8 < 0.0 {
        println!(
            "  ✓✓ EmatStreaming (Utf8View) BEATS FastParquet baseline — \
             flip default in follow-up."
        );
    } else {
        println!(
            "  ✗ EmatStreaming (Utf8View) {pct_stream_utf8_vs_bridge_utf8:+.1}% vs \
             FastParquet baseline — investigate before flipping default."
        );
    }
}
