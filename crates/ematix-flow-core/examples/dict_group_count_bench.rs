//! Σ.E3b.1 bench: `COUNT(*) GROUP BY dict_col` —
//! `DictGroupCountExec` vs DataFusion's default `AggregateExec` on a
//! synthetic dict-encoded fixture sized to amplify the per-row hot
//! loop (no I/O, no parquet decode — measures the agg kernel only).
//!
//! Fixture shape:
//!   - 8 M rows of a single `Dictionary(UInt32, Utf8)` column
//!     `l_shipmode`-style with 7 distinct values (MAIL, AIR, SHIP,
//!     REG AIR, TRUCK, FOB, RAIL).
//!   - One batch (no cross-batch dict merging — single-pass case
//!     where the per-batch code-set translation pays off most).
//!
//! What the bench measures:
//!   - **Default**: register the MemTable as `t`, run
//!     `SELECT l_shipmode, COUNT(*) FROM t GROUP BY l_shipmode`
//!     through DataFusion. The planner produces the two-stage
//!     AggregateExec (Partial → RepartitionExec → FinalPartitioned).
//!   - **Σ.E3b.1**: hand-construct `DictGroupCountExec` over the
//!     same MemTable scan, drain its stream.
//!
//! Run:
//!   cargo run --release -p ematix-flow-core --example dict_group_count_bench

use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{ArrayRef, RecordBatch, StringDictionaryBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, UInt32Type};
use datafusion::datasource::MemTable;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use ematix_flow_core::dict_aggregate::DictGroupCountExec;
use futures_util::stream::TryStreamExt;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const ROWS: usize = 8_000_000;
const TRIALS: usize = 10;
const WARMUPS: usize = 2;
const SHIPMODES: &[&str] = &["MAIL", "AIR", "SHIP", "REG AIR", "TRUCK", "FOB", "RAIL"];

fn make_batch() -> RecordBatch {
    let mut keys: StringDictionaryBuilder<UInt32Type> = StringDictionaryBuilder::new();
    for i in 0..ROWS {
        keys.append(SHIPMODES[i % SHIPMODES.len()]).unwrap();
    }
    let dict = keys.finish();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "l_shipmode",
        DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(dict) as ArrayRef]).unwrap()
}

fn statistics(samples_ms: &[f64]) -> (f64, f64) {
    let mut sorted = samples_ms.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = sorted[sorted.len() / 2];
    let mean = sorted.iter().sum::<f64>() / sorted.len() as f64;
    let var = sorted.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / sorted.len() as f64;
    (median, var.sqrt())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let batch = make_batch();
    let schema = batch.schema();
    let mem = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    let mem = Arc::new(mem);

    println!("=== dict_group_count_bench ===");
    println!("rows={ROWS}, distinct_groups={}, trials={TRIALS}", SHIPMODES.len());

    // --- Default DataFusion path ---
    let mut default_samples: Vec<f64> = Vec::with_capacity(TRIALS);
    for trial in 0..(TRIALS + WARMUPS) {
        let ctx = SessionContext::new();
        ctx.register_table("t", mem.clone()).unwrap();
        let t0 = Instant::now();
        let _ = ctx
            .sql("SELECT l_shipmode, COUNT(*) FROM t GROUP BY l_shipmode")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        if trial >= WARMUPS {
            default_samples.push(ms);
        }
    }

    // --- DictGroupCountExec path ---
    let mut dict_samples: Vec<f64> = Vec::with_capacity(TRIALS);
    for trial in 0..(TRIALS + WARMUPS) {
        let ctx = SessionContext::new();
        ctx.register_table("t", mem.clone()).unwrap();
        let df = ctx.sql("SELECT * FROM t").await.unwrap();
        let input = df.create_physical_plan().await.unwrap();
        let exec = Arc::new(DictGroupCountExec::try_new(input, 0).unwrap());
        let t0 = Instant::now();
        let mut s = exec.execute(0, ctx.task_ctx()).unwrap();
        let mut rows = 0;
        while let Some(b) = s.try_next().await.unwrap() {
            rows += b.num_rows();
        }
        assert_eq!(rows, SHIPMODES.len());
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        if trial >= WARMUPS {
            dict_samples.push(ms);
        }
    }

    let (def_med, def_sd) = statistics(&default_samples);
    let (dict_med, dict_sd) = statistics(&dict_samples);
    let speedup = def_med / dict_med;
    println!(
        "default median ± σ: {def_med:7.2} ms ± {def_sd:5.2}\n\
         dict    median ± σ: {dict_med:7.2} ms ± {dict_sd:5.2}\n\
         speedup           : {speedup:.2}×",
    );
}
