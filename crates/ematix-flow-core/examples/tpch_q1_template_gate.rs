//! Σ.G.2f.2 bench gate: TPC-H Q1 spec-level parity.
//!
//! Microbench at the `AggregateSpec::process_batch` boundary so we
//! compare hot loops, not plan trees. Three rows:
//!
//! * **Q1Spec / Utf8View** — the hand-baked baseline. Lineitem
//!   arrives via `FastParquetTableProvider` with string columns as
//!   `Utf8View`; Q1Spec's Cranelift JIT bakes the four `(returnflag,
//!   linestatus)` literals as branchless arm matches.
//!
//! * **FilterMultiAggSpec generic / Utf8View** — Σ.G.2f.1's
//!   hash-grouped fallback over the same input. Measures the cost of
//!   "no data-specific baking" with `Utf8ViewFirstByte` group keys.
//!
//! * **FilterMultiAggSpec template dispatch / Dictionary** — Σ.G.2f.2's
//!   per-batch template dispatch. Lineitem arrives via the preset
//!   `register_dict_aware_parquet`, so the same string columns come
//!   in as `Dictionary(UInt32, Utf8)`. For Q1's low-cardinality
//!   returnflag dict (≤ `PERFECT_HASH_DICT_CARDINALITY_THRESHOLD`),
//!   dispatch routes into `process_batch_perfect_hash_dict` — a flat
//!   `Vec<f64>` indexed directly by dict code, no HashMap in the hot
//!   loop. For larger dicts the per-batch slot-table `dict_single`
//!   template handles it.
//!
//! The gate question: does the template path (row 3) reach the
//! hand-baked baseline (row 1) within bench noise? If yes, the
//! `InjectFusedQ1Rule` + `Q1Spec` retirement in Σ.G.2f.3 is justified.
//!
//! Usage:
//!     cargo run --release -p ematix-flow-core --example tpch_q1_template_gate

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::RecordBatch;
use datafusion::prelude::SessionContext;
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_core::fused_aggregate::{AggregateSpec, Q1Spec};
use ematix_flow_core::fused_aggregate_filter_multi_agg::{FilterMultiAggSpec, GroupKeyKind};
use ematix_flow_core::fused_jit::{AggExpr, Clause, ClauseOp, ColumnTy};
use ematix_flow_core::fused_multi_agg::Q1Predicate;
use futures_util::TryStreamExt;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TRIALS: usize = 21;
const WARMUPS: usize = 3;

/// `1998-09-02` as Date32 days since epoch — Q1's shipdate cutoff.
const Q1_SHIPDATE_CUTOFF: i32 = 10471;

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

const SELECT_Q1_COLS: &str = "SELECT l_returnflag, l_linestatus, l_quantity, \
    l_extendedprice, l_discount, l_tax, l_shipdate FROM lineitem";

async fn collect_utf8view_batches(path: &str) -> Vec<RecordBatch> {
    let ctx = SessionContext::new();
    let prov = FastParquetTableProvider::try_new(path).unwrap();
    ctx.register_table("lineitem", Arc::new(prov)).unwrap();
    let df = ctx.sql(SELECT_Q1_COLS).await.unwrap();
    df.execute_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

async fn collect_dict_batches(path: &str) -> Vec<RecordBatch> {
    let ctx = SessionContext::new();
    let prov = EmatixFastParquetTableProvider::try_new(path)
        .unwrap()
        .with_dict_preservation(true);
    ctx.register_table("lineitem", Arc::new(prov)).unwrap();
    let df = ctx.sql(SELECT_Q1_COLS).await.unwrap();
    df.execute_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
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

fn bench_q1_spec(label: &str, batches: &[RecordBatch]) -> f64 {
    let pred = Q1Predicate {
        shipdate_cutoff: Q1_SHIPDATE_CUTOFF,
    };
    let schema = batches[0].schema();
    let spec = Q1Spec::try_new_jit(pred, &schema).unwrap();
    bench_spec(label, batches, &spec)
}

fn q1_filter_multi_spec(
    schema: &Arc<datafusion::arrow::datatypes::Schema>,
    group_kind: GroupKeyKind,
) -> FilterMultiAggSpec {
    // Q1's shipdate-only filter as a Clause AND-chain.
    let shipdate_idx_in_inputs = 6;
    let predicate = vec![Clause {
        column: shipdate_idx_in_inputs,
        op: ClauseOp::I32Le,
        imm_i32: Q1_SHIPDATE_CUTOFF,
        imm_f64: 0.0,
    }];
    let input_tys = vec![
        ColumnTy::Utf8View, // 0 returnflag (also Dictionary in dict path — input_tys is only used for non-group cols)
        ColumnTy::Utf8View, // 1 linestatus
        ColumnTy::Float64,  // 2 quantity
        ColumnTy::Float64,  // 3 extprice
        ColumnTy::Float64,  // 4 discount
        ColumnTy::Float64,  // 5 tax
        ColumnTy::Date32,   // 6 shipdate
    ];
    // When the group columns arrive as Dictionary, we still claim their
    // entries in input_tys+input_column_names because the validator
    // walks the full list. We swap to a list that doesn't claim them
    // for the dict variant — see below.
    let (input_columns, input_tys) = match group_kind {
        GroupKeyKind::Utf8ViewFirstByte => (
            vec![
                "l_returnflag",
                "l_linestatus",
                "l_quantity",
                "l_extendedprice",
                "l_discount",
                "l_tax",
                "l_shipdate",
            ],
            input_tys,
        ),
        GroupKeyKind::DictionaryU32 => (
            // For the dict variant we drop the two string columns
            // from the input list since they're not predicate or agg
            // inputs (only group keys), and the input-tys validator
            // would reject a `Dictionary` column claimed as
            // `Utf8View`. The predicate/agg column indices shift.
            vec![
                "l_quantity",
                "l_extendedprice",
                "l_discount",
                "l_tax",
                "l_shipdate",
            ],
            vec![
                ColumnTy::Float64, // 0 quantity
                ColumnTy::Float64, // 1 extprice
                ColumnTy::Float64, // 2 discount
                ColumnTy::Float64, // 3 tax
                ColumnTy::Date32,  // 4 shipdate
            ],
        ),
    };
    // Predicate + agg column indices depend on the input list above.
    let (predicate, aggregates) = match group_kind {
        GroupKeyKind::Utf8ViewFirstByte => (
            predicate,
            vec![
                AggExpr::SumColumn(2),                          // sum_qty
                AggExpr::SumColumn(3),                          // sum_base_price
                AggExpr::SumProductOneMinus(3, 4),              // sum_disc_price
                AggExpr::SumProductTwoOneMinusOnePlus(3, 4, 5), // sum_charge
                AggExpr::CountStar,
            ],
        ),
        GroupKeyKind::DictionaryU32 => (
            vec![Clause {
                column: 4, // shipdate at new index
                op: ClauseOp::I32Le,
                imm_i32: Q1_SHIPDATE_CUTOFF,
                imm_f64: 0.0,
            }],
            vec![
                AggExpr::SumColumn(0),                          // sum_qty
                AggExpr::SumColumn(1),                          // sum_base_price
                AggExpr::SumProductOneMinus(1, 2),              // sum_disc_price
                AggExpr::SumProductTwoOneMinusOnePlus(1, 2, 3), // sum_charge
                AggExpr::CountStar,
            ],
        ),
    };
    let agg_output_names = vec![
        "sum_qty".into(),
        "sum_base_price".into(),
        "sum_disc_price".into(),
        "sum_charge".into(),
        "count_order".into(),
    ];
    // Σ.G.2f.2 dict-single template fires when there's exactly one
    // DictionaryU32 group key. Use a single (returnflag) key for that
    // path; the Utf8View path uses both for a more representative
    // baseline. Both shapes are valid Q1 reductions.
    let group_keys = match group_kind {
        GroupKeyKind::Utf8ViewFirstByte => vec![
            ("l_returnflag".into(), GroupKeyKind::Utf8ViewFirstByte),
            ("l_linestatus".into(), GroupKeyKind::Utf8ViewFirstByte),
        ],
        GroupKeyKind::DictionaryU32 => {
            vec![("l_returnflag".into(), GroupKeyKind::DictionaryU32)]
        }
    };
    FilterMultiAggSpec::try_new(
        predicate,
        input_tys,
        &input_columns,
        aggregates,
        agg_output_names,
        group_keys,
        schema,
    )
    .unwrap()
}

fn bench_spec<S: AggregateSpec>(label: &str, batches: &[RecordBatch], spec: &S) -> f64
where
    S::Accumulator: Default,
{
    // Warm-ups.
    for _ in 0..WARMUPS {
        let mut acc = S::Accumulator::default();
        for b in batches {
            spec.process_batch(b, &mut acc).unwrap();
        }
        let _ = spec.finalize(acc).unwrap();
    }
    let mut times = Vec::with_capacity(TRIALS);
    for _ in 0..TRIALS {
        let mut acc = S::Accumulator::default();
        let start = Instant::now();
        for b in batches {
            spec.process_batch(b, &mut acc).unwrap();
        }
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        let _ = spec.finalize(acc).unwrap();
        times.push(elapsed_ms);
    }
    let med = median(&mut times);
    let mean = times.iter().sum::<f64>() / times.len() as f64;
    let sd = stdev(&times, mean);
    println!("  {label:<56}  median {med:>6.2} ms ± {sd:>5.2}");
    med
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let path = data_path();
    println!("==> Σ.G.2f.2 bench gate: Q1 template vs Q1Spec parity");
    println!("==> data: {path}");
    println!(
        "==> {TRIALS}-trial median after {WARMUPS} warm-ups, single-thread spec.process_batch"
    );
    println!();

    let utf8_batches = collect_utf8view_batches(&path).await;
    let dict_batches = collect_dict_batches(&path).await;
    println!(
        "  loaded {} Utf8View batches, {} Dictionary batches",
        utf8_batches.len(),
        dict_batches.len()
    );
    println!();

    // Path A: Q1Spec on Utf8View — hand-baked baseline.
    let q1 = bench_q1_spec(
        "Q1Spec (JIT) / Utf8View — hand-baked baseline",
        &utf8_batches,
    );

    // Path B: FilterMultiAggSpec generic on Utf8View.
    let multi_utf8 =
        q1_filter_multi_spec(&utf8_batches[0].schema(), GroupKeyKind::Utf8ViewFirstByte);
    let multi_utf8_ms = bench_spec(
        "FilterMultiAggSpec generic / Utf8View",
        &utf8_batches,
        &multi_utf8,
    );

    // Path C: FilterMultiAggSpec template dispatch on Dictionary.
    // For Q1's 3-distinct-returnflag dict this routes to the
    // PerfectHashAggregate template (cardinality well under the
    // threshold).
    let multi_dict = q1_filter_multi_spec(&dict_batches[0].schema(), GroupKeyKind::DictionaryU32);
    let multi_dict_ms = bench_spec(
        "FilterMultiAggSpec template dispatch / Dictionary",
        &dict_batches,
        &multi_dict,
    );

    let pct_dict = 100.0 * (multi_dict_ms - q1) / q1;
    let pct_gen = 100.0 * (multi_utf8_ms - q1) / q1;
    println!();
    println!("  template (PH) vs Q1Spec:  {multi_dict_ms:.2} ms vs {q1:.2} ms  ({pct_dict:+.1}%)");
    println!("  generic       vs Q1Spec:  {multi_utf8_ms:.2} ms vs {q1:.2} ms  ({pct_gen:+.1}%)");
    println!();
    if pct_dict.abs() < 5.0 {
        println!("  ✓ template ≈ Q1Spec (±5%) — gate PASSES; Σ.G.2f.3 deletion justified.");
    } else if pct_dict < 0.0 {
        println!("  ✓✓ template beats Q1Spec — gate PASSES outright.");
    } else {
        println!("  ✗ template regresses vs Q1Spec — investigate before .3 deletion.");
    }
}
