//! REV.18 — operator-level crossover sweep: `RobinHoodSumF64Exec` vs stock
//! DataFusion `AggregateExec` for `SUM(f64) GROUP BY i64`, across the ACTUAL
//! distinct-group count.
//!
//! The KEYS.3 plan-diff dig-in found `RobinHoodSumF64Exec` ~12× slower than
//! stock on Q18 at SF=1/10/100 (1.5M / 15M / 150M groups). This sweeps BELOW
//! 1.5M — including RobinHood's theoretical sweet spot (small, cache-resident
//! table) — to find any cardinality where the custom operator actually wins,
//! so REV.10's `max_groups` gate can be set to the real crossover (or the
//! kernel demoted to opt-in if it never wins at the operator level).
//!
//! Fixed 6M rows so total work is constant and meaningful; only the distinct
//! group count varies (6000 rows/group at 1K groups → 1 row/group at 6M).
//!
//! Run:
//!   cargo run --release -p ematix-flow-core --example rh_sum_crossover_sweep

use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Float64Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::robin_hood_sum_f64_exec::EnableRobinHoodSumF64Rule;
use futures_util::TryStreamExt;

const N_ROWS: usize = 6_000_000;
const CARDS: &[usize] = &[
    1_000, 10_000, 100_000, 250_000, 500_000, 1_000_000, 2_000_000, 6_000_000,
];
const TRIALS: usize = 7;
const WARMUPS: usize = 2;
const BATCH: usize = 8192;

fn target_partitions() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8)
}

fn gen_batches(card: usize) -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Float64, false),
    ]));
    let mut batches = Vec::new();
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut produced = 0usize;
    while produced < N_ROWS {
        let take = BATCH.min(N_ROWS - produced);
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

fn build_ctx(batches: &[RecordBatch], rh_on: bool) -> SessionContext {
    let schema = batches[0].schema();
    let mut builder = SessionStateBuilder::new()
        .with_default_features()
        .with_config(SessionConfig::new().with_target_partitions(target_partitions()));
    if rh_on {
        // max_groups = MAX so the rule always fires — we measure the OPERATOR,
        // not the gate.
        builder = builder.with_physical_optimizer_rule(Arc::new(EnableRobinHoodSumF64Rule {
            max_groups: usize::MAX,
        }));
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

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let sql = "SELECT k, SUM(v) FROM t GROUP BY k";
    println!("REV.18 operator crossover — RobinHoodSumF64Exec vs stock AggregateExec");
    println!(
        "{N_ROWS} rows fixed, SUM(v) GROUP BY i64-k, {TRIALS} trials +{WARMUPS} warmup, {} partitions\n",
        target_partitions()
    );
    println!(
        "{:>11} {:>10} {:>12} {:>12} {:>10}",
        "groups", "rows/grp", "RH_on ms", "stock ms", "RH/stock"
    );
    for &card in CARDS {
        let batches = gen_batches(card);
        let ctx_on = build_ctx(&batches, true);
        let ctx_off = build_ctx(&batches, false);
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
        println!(
            "{card:>11} {:>10} {on:>12.3} {off:>12.3} {ratio:>8.2}x  {verdict}",
            N_ROWS / card
        );
    }
}
