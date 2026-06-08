//! REV.18d — gate-calibration sweep for the RobinHood agg kernels.
//!
//! The three rules gate on `est_groups = input_rows / 4` (a row-count proxy
//! assuming ~4 rows/group, the Q18 design shape), firing when
//! `est_groups <= max_groups` (256K). So the gate ONLY fires when the
//! aggregate input has <= ~1.05M rows. The earlier operator sweep
//! (`rh_crossover_sweep_all.rs`) ran at a fixed 6M rows → est_groups = 1.5M
//! at every point → the production gate would never have fired on any of it.
//!
//! This sweep measures the kernels in the regime where the gate ACTUALLY
//! fires. It generates data at ~4 rows/group so that `est_groups` (= rows/4,
//! the gate variable) equals the actual distinct-group count. We sweep
//! `est_groups` across the 256K boundary and report RH/stock per kernel, so
//! we can set a lower bound that confines firing to the win band — or learn
//! that no win band exists in the firing regime (in which case the kernels
//! cannot be made default-on-worthy and should stay opt-in).
//!
//! Run:
//!   cargo run --release -p ematix-flow-core --example rh_gate_calibration_sweep

use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Float64Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::robin_hood_agg_rule::{
    DEFAULT_RH_COUNT_MAX_GROUPS, DEFAULT_RH_COUNT_MIN_GROUPS, EnableRobinHoodAggregateRule,
};
use ematix_flow_core::robin_hood_avg_f64_exec::{
    DEFAULT_RH_AVG_F64_MAX_GROUPS, DEFAULT_RH_AVG_F64_MIN_GROUPS, EnableRobinHoodAvgF64Rule,
};
use ematix_flow_core::robin_hood_sum_f64_exec::{
    DEFAULT_RH_SUM_F64_MAX_GROUPS, DEFAULT_RH_SUM_F64_MIN_GROUPS, EnableRobinHoodSumF64Rule,
};
use futures_util::TryStreamExt;

/// est_groups values to probe (= the gate variable = input_rows / 4). Each
/// row count is 4× this so rows/group ≈ 4 and est_groups ≈ actual groups.
const EST_GROUPS: &[usize] = &[
    8_192, 16_384, 32_768, 65_536, 131_072, 196_608, 262_144, 393_216, 524_288,
];
const TRIALS: usize = 11;
const WARMUPS: usize = 3;
const BATCH: usize = 8192;

fn target_partitions() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8)
}

fn gen_batches(card: usize, n_rows: usize) -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Float64, false),
    ]));
    let mut batches = Vec::new();
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut produced = 0usize;
    while produced < n_rows {
        let take = BATCH.min(n_rows - produced);
        let mut ks = Vec::with_capacity(take);
        let mut vs = Vec::with_capacity(take);
        for i in 0..take {
            x = (x ^ ((produced + i) as u64)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            x = (x ^ (x >> 30)).wrapping_mul(0x94d0_49bb_1331_11eb);
            ks.push((x % card as u64) as i64);
            vs.push(((x >> 11) & 0xfffff) as f64 / 1024.0);
        }
        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ks)),
                    Arc::new(Float64Array::from(vs)),
                ],
            )
            .unwrap(),
        );
        produced += take;
    }
    batches
}

fn build_ctx(
    batches: &[RecordBatch],
    rule: Option<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
) -> SessionContext {
    let schema = batches[0].schema();
    let mut builder = SessionStateBuilder::new()
        .with_default_features()
        .with_config(SessionConfig::new().with_target_partitions(target_partitions()));
    if let Some(rule) = rule {
        builder = builder.with_physical_optimizer_rule(rule);
    }
    let ctx = SessionContext::new_with_state(builder.build());
    let mt = MemTable::try_new(schema, vec![batches.to_vec()]).unwrap();
    ctx.register_table("t", Arc::new(mt)).unwrap();
    ctx
}

async fn time_query(ctx: &SessionContext, sql: &str) -> f64 {
    let t = Instant::now();
    let df = ctx.sql(sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    for p in 0..plan.output_partitioning().partition_count() {
        let mut s = plan.execute(p, ctx.task_ctx()).unwrap();
        while let Some(_b) = s.try_next().await.unwrap() {}
    }
    t.elapsed().as_secs_f64() * 1000.0
}

async fn median(ctx: &SessionContext, sql: &str) -> f64 {
    for _ in 0..WARMUPS {
        time_query(ctx, sql).await;
    }
    let mut xs = Vec::with_capacity(TRIALS);
    for _ in 0..TRIALS {
        xs.push(time_query(ctx, sql).await);
    }
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    xs[xs.len() / 2]
}

// max_groups = MAX so we measure the OPERATOR across the whole range, then
// overlay the proposed [CAND_LOWER, UPPER_GATE] band as a fire-marker.
// min_groups: 0, max_groups: MAX → gate always passes, so we measure the
// OPERATOR across the whole est_groups range; the production fire band is
// overlaid separately via `band_for`.
fn rule_for(kernel: &str) -> Arc<dyn PhysicalOptimizerRule + Send + Sync> {
    match kernel {
        "SUM" => Arc::new(EnableRobinHoodSumF64Rule {
            min_groups: 0,
            max_groups: usize::MAX,
        }),
        "COUNT" => Arc::new(EnableRobinHoodAggregateRule {
            min_groups: 0,
            max_groups: usize::MAX,
        }),
        "AVG" => Arc::new(EnableRobinHoodAvgF64Rule {
            min_groups: 0,
            max_groups: usize::MAX,
        }),
        _ => unreachable!(),
    }
}

/// Production fire band per kernel = [DEFAULT_*_MIN, DEFAULT_*_MAX], now locked
/// into the rule gates. `|FIRE|` marks rows where the gated rule would run;
/// confirm every |FIRE| row is a win (SUM has no win band — harm-reduction
/// only, so its band rows should be ~tie, not losses).
fn band_for(kernel: &str) -> (usize, usize) {
    match kernel {
        "SUM" => (DEFAULT_RH_SUM_F64_MIN_GROUPS, DEFAULT_RH_SUM_F64_MAX_GROUPS),
        "COUNT" => (DEFAULT_RH_COUNT_MIN_GROUPS, DEFAULT_RH_COUNT_MAX_GROUPS),
        "AVG" => (DEFAULT_RH_AVG_F64_MIN_GROUPS, DEFAULT_RH_AVG_F64_MAX_GROUPS),
        _ => unreachable!(),
    }
}

fn sql_for(kernel: &str) -> &'static str {
    match kernel {
        "SUM" => "SELECT k, SUM(v) FROM t GROUP BY k",
        "COUNT" => "SELECT k, COUNT(*) FROM t GROUP BY k",
        "AVG" => "SELECT k, AVG(v) FROM t GROUP BY k",
        _ => unreachable!(),
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    println!("REV.18d gate-calibration — RobinHood{{Sum,Count,Avg}} in the GATE-FIRING regime");
    println!(
        "~4 rows/group so est_groups (= rows/4, the gate variable) ≈ actual groups; \
         {TRIALS} trials +{WARMUPS} warmup, {} partitions",
        target_partitions()
    );
    println!(
        "fire band = the locked [DEFAULT_*_MIN, DEFAULT_*_MAX] gate per kernel; \
         |FIRE| marks where the gated rule would actually run\n"
    );

    for kernel in ["SUM", "COUNT", "AVG"] {
        let (lo, hi) = band_for(kernel);
        println!("=== {kernel}  (fire band [{lo}, {hi}] est_groups) ===");
        println!(
            "{:>11} {:>10} {:>11} {:>11} {:>9}  {:<10} gate",
            "est_groups", "rows", "RH_on ms", "stock ms", "RH/stock", "verdict"
        );
        let sql = sql_for(kernel);
        for &g in EST_GROUPS {
            let n_rows = g * 4;
            let batches = gen_batches(g, n_rows);
            let ctx_on = build_ctx(&batches, Some(rule_for(kernel)));
            let ctx_off = build_ctx(&batches, None);
            let on = median(&ctx_on, sql).await;
            let off = median(&ctx_off, sql).await;
            let ratio = on / off;
            let verdict = if ratio < 0.97 {
                "RH WINS"
            } else if ratio > 1.03 {
                "stock wins"
            } else {
                "~tie"
            };
            let gate = if g >= lo && g <= hi { "|FIRE|" } else { " " };
            println!(
                "{g:>11} {n_rows:>10} {on:>11.3} {off:>11.3} {ratio:>8.2}x  {verdict:<10} {gate}"
            );
        }
        println!();
    }
}
