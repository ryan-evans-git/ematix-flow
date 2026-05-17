//! Σ.G.2 perf-equivalence gate for the Q1 hot loop.
//!
//! Same methodology as `sigma_g2_q6_unified_vs_hand.rs`: time the
//! hand path (`fused_multi_agg::process_q1_batch_hand`) and the
//! unified path (`fused_aggregate::Q1Spec::process_batch`) on
//! identical synthetic lineitem-shaped batches and gate at 3 %
//! delta. Run before opening the operator-migration PR.
//!
//! Each trial times BATCHES_PER_TRIAL invocations because a single
//! ~50 µs batch is too close to the system timer floor to give a
//! stable median otherwise — see the Q6 bench for the methodology
//! note that motivated this.
//!
//! Runs:
//!     cargo run --release -p ematix-flow-core \
//!         --example sigma_g2_q1_unified_vs_hand
//!
//! Hermetic — generates its own batches in-process so it produces
//! reproducible numbers without needing TPC-H data on disk.

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Date32Array, Float64Array, RecordBatch, StringViewArray};
use arrow_schema::{DataType, Field, Schema};
use ematix_flow_core::fused_aggregate::{AggregateSpec, Q1Spec};
use ematix_flow_core::fused_multi_agg::{
    Q1Aggs, Q1ColumnIndices, Q1Predicate, process_q1_batch_hand,
};

/// Same tiny deterministic LCG used by the Q6 bench. See that file's
/// header for the numerical-recipes constants.
struct Lcg {
    state: u64,
}
impl Lcg {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }
    fn next_u32(&mut self) -> u32 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.state >> 32) as u32
    }
    fn next_f64(&mut self) -> f64 {
        (self.next_u32() >> 8) as f64 / (1u32 << 24) as f64
    }
    fn next_in_range_f64(&mut self, lo: f64, hi: f64) -> f64 {
        lo + (hi - lo) * self.next_f64()
    }
    fn next_in_range_i32(&mut self, lo: i32, hi: i32) -> i32 {
        let span = (hi - lo) as u32;
        lo + (self.next_u32() % span) as i32
    }
}

const ROWS_PER_BATCH: usize = 65_536;
const BATCHES_PER_TRIAL: usize = 25;
const TRIALS: usize = 21;
const WARMUPS: usize = 5;

fn main() {
    let (batch, schema) = synth_lineitem_batch();
    let predicate = Q1Predicate {
        shipdate_cutoff: 10471, // 1998-09-02
    };

    let hand_idx = Q1ColumnIndices {
        rflag: 0,
        lstatus: 1,
        qty: 2,
        price: 3,
        disc: 4,
        tax: 5,
        ship: 6,
    };
    let spec = Q1Spec::try_new(predicate, &schema).expect("Q1Spec::try_new");

    // ---- Correctness: paths produce identical accumulators ----
    let mut hand_acc: [Q1Aggs; 5] = [Q1Aggs::default(); 5];
    process_q1_batch_hand(&batch, predicate, hand_idx, &mut hand_acc);
    let mut spec_acc: [Q1Aggs; 5] = [Q1Aggs::default(); 5];
    spec.process_batch(&batch, &mut spec_acc).unwrap();
    for g in 0..5 {
        let h = &hand_acc[g];
        let s = &spec_acc[g];
        assert_eq!(h.count, s.count, "group {g} count diverges");
        assert!(
            (h.sum_qty - s.sum_qty).abs() < 1e-6,
            "group {g} sum_qty diverges: {} vs {}",
            h.sum_qty,
            s.sum_qty
        );
        assert!(
            (h.sum_charge - s.sum_charge).abs() < 1e-6,
            "group {g} sum_charge diverges",
        );
    }

    println!("== Σ.G.2 Q1 unified-vs-hand perf gate ==");
    println!(
        "  fixture: {} rows × 7 cols, predicate l_shipdate ≤ 1998-09-02",
        ROWS_PER_BATCH
    );
    println!(
        "  trials:  {} (×{} batches/trial, after {} warmups each)",
        TRIALS, BATCHES_PER_TRIAL, WARMUPS
    );
    println!();

    // ---- Warmup ----
    for _ in 0..WARMUPS {
        let mut a: [Q1Aggs; 5] = [Q1Aggs::default(); 5];
        process_q1_batch_hand(&batch, predicate, hand_idx, &mut a);
        let mut b: [Q1Aggs; 5] = [Q1Aggs::default(); 5];
        spec.process_batch(&batch, &mut b).unwrap();
        std::hint::black_box((a, b));
    }

    // ---- Time hand path ----
    let mut hand_times = Vec::with_capacity(TRIALS);
    for _ in 0..TRIALS {
        let t = Instant::now();
        let mut acc: [Q1Aggs; 5] = [Q1Aggs::default(); 5];
        for _ in 0..BATCHES_PER_TRIAL {
            process_q1_batch_hand(&batch, predicate, hand_idx, &mut acc);
        }
        hand_times.push(t.elapsed().as_nanos() / BATCHES_PER_TRIAL as u128);
        std::hint::black_box(acc);
    }
    hand_times.sort_unstable();
    let hand_median = hand_times[TRIALS / 2];

    // ---- Time unified path ----
    let mut spec_times = Vec::with_capacity(TRIALS);
    for _ in 0..TRIALS {
        let t = Instant::now();
        let mut acc: [Q1Aggs; 5] = [Q1Aggs::default(); 5];
        for _ in 0..BATCHES_PER_TRIAL {
            spec.process_batch(&batch, &mut acc).unwrap();
        }
        spec_times.push(t.elapsed().as_nanos() / BATCHES_PER_TRIAL as u128);
        std::hint::black_box(acc);
    }
    spec_times.sort_unstable();
    let spec_median = spec_times[TRIALS / 2];

    println!("  hand    median {:>8} / batch", fmt_ns(hand_median));
    println!("  unified median {:>8} / batch", fmt_ns(spec_median));
    println!();

    let delta = spec_median as f64 / hand_median as f64;
    let delta_pct = (delta - 1.0) * 100.0;
    println!(
        "  unified / hand = {:.3}  ({}{:.2} %)",
        delta,
        if delta_pct >= 0.0 { "+" } else { "" },
        delta_pct
    );
    println!();

    let gate_ok = delta <= 1.03;
    if gate_ok {
        println!("  GATE PASS — unified within 3 % of hand. Σ.G.2 Q1 migration is safe.");
    } else {
        println!(
            "  GATE FAIL — unified is {:.2} % slower than hand (> 3 %).",
            delta_pct
        );
        println!("  Do NOT migrate FusedFilterMultiAggExec to FusedAggregateExec<Q1Spec>");
        println!(
            "  without investigating: check the loop body is being delegated, not duplicated."
        );
        std::process::exit(1);
    }
}

fn fmt_ns(ns: u128) -> String {
    if ns < 10_000 {
        format!("{} ns", ns)
    } else if ns < 10_000_000 {
        format!("{:.2} µs", ns as f64 / 1_000.0)
    } else {
        format!("{:.2} ms", ns as f64 / 1_000_000.0)
    }
}

/// Build a 65,536-row Q1-shaped batch with realistic-ish distributions:
/// returnflag uniform in {R, N, A}, linestatus uniform in {O, F},
/// shipdate uniform in 1992-1998 (so the cutoff hits ~95 %), quantity
/// uniform in [1, 50], extendedprice in [900, 105_000], discount in
/// [0.0, 0.10], tax in [0.0, 0.08]. Seeded so re-runs are deterministic.
fn synth_lineitem_batch() -> (RecordBatch, Arc<Schema>) {
    let mut rng = Lcg::new(0xC0_FFEE_42_19);

    let flags = ["R", "N", "A"];
    let stati = ["O", "F"];
    let rflag_v: Vec<&'static str> = (0..ROWS_PER_BATCH)
        .map(|_| flags[(rng.next_u32() % 3) as usize])
        .collect();
    let lstatus_v: Vec<&'static str> = (0..ROWS_PER_BATCH)
        .map(|_| stati[(rng.next_u32() % 2) as usize])
        .collect();
    let qty: Vec<f64> = (0..ROWS_PER_BATCH)
        .map(|_| rng.next_in_range_f64(1.0, 50.0))
        .collect();
    let price: Vec<f64> = (0..ROWS_PER_BATCH)
        .map(|_| rng.next_in_range_f64(900.0, 105_000.0))
        .collect();
    let disc: Vec<f64> = (0..ROWS_PER_BATCH)
        .map(|_| rng.next_in_range_f64(0.0, 0.10))
        .collect();
    let tax: Vec<f64> = (0..ROWS_PER_BATCH)
        .map(|_| rng.next_in_range_f64(0.0, 0.08))
        .collect();
    let ship: Vec<i32> = (0..ROWS_PER_BATCH)
        .map(|_| rng.next_in_range_i32(8035, 10592))
        .collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("l_returnflag", DataType::Utf8View, false),
        Field::new("l_linestatus", DataType::Utf8View, false),
        Field::new("l_quantity", DataType::Float64, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_tax", DataType::Float64, false),
        Field::new("l_shipdate", DataType::Date32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringViewArray::from(rflag_v)),
            Arc::new(StringViewArray::from(lstatus_v)),
            Arc::new(Float64Array::from(qty)),
            Arc::new(Float64Array::from(price)),
            Arc::new(Float64Array::from(disc)),
            Arc::new(Float64Array::from(tax)),
            Arc::new(Date32Array::from(ship)),
        ],
    )
    .unwrap();
    (batch, schema)
}
