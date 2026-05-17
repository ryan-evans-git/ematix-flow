//! Σ.G.2 perf-equivalence gate for the Q6 hot loop.
//!
//! Compares two paths on identical synthetic batches:
//!   1. **Hand**: `fused::process_q6_batch_hand` — today's
//!      production path used by `FusedFilterSumExec`.
//!   2. **Unified**: `fused_aggregate::Q6Spec::process_batch` — the
//!      future path that goes through the generic `AggregateSpec`
//!      trait. Concrete impl block, monomorphic dispatch at the
//!      batch boundary.
//!
//! Gate criterion: median wall-clock per batch of the unified path
//! must be within **3 %** of the hand path. The audit + plan
//! (`docs/FUSED_AGGREGATE_SHAPES.md` and
//! `docs/PHASE_SIGMA_G_GENERIC_FUSED_AGGREGATE.md`) call out that
//! generic dispatch *could* break LLVM's per-shape inlining. This
//! example is the empirical check before any operator migration.
//!
//! Runs:
//!     cargo run --release -p ematix-flow-core \
//!         --example sigma_g2_q6_unified_vs_hand
//!
//! The example is hermetic — it generates its own random lineitem-
//! shaped batches in-process so it produces reproducible numbers
//! without needing TPC-H data on disk. The fixture is sized to
//! match a single 65,536-row Parquet row group, which is what
//! `process_q6_batch_hand` sees in production.

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Date32Array, Float64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use ematix_flow_core::fused::{ColumnIndices, Q6Predicate, process_q6_batch_hand};
use ematix_flow_core::fused_aggregate::{AggregateSpec, Q6Spec};

/// Tiny deterministic LCG. Used to populate the synthetic fixture
/// without adding a `rand` workspace dep — the bench needs
/// repeatable distributions, not crypto-grade randomness.
struct Lcg {
    state: u64,
}
impl Lcg {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }
    fn next_u32(&mut self) -> u32 {
        // Numerical Recipes 64-bit LCG constants.
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.state >> 32) as u32
    }
    fn next_f64(&mut self) -> f64 {
        // 24-bit mantissa from the high bits → uniform [0, 1).
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
// Each trial times BATCHES_PER_TRIAL invocations to amortise timer
// overhead — a single ~45 µs batch is too close to the system timer
// resolution to give a stable median otherwise. Pilot runs at K=1
// showed 0.2 % → 7 % swings between runs; K=25 brings the wobble
// under 1 %.
const BATCHES_PER_TRIAL: usize = 25;
const TRIALS: usize = 21;
const WARMUPS: usize = 5;

fn main() {
    let (batch, schema) = synth_lineitem_batch();
    let predicate = canonical_q6_predicate();

    let hand_idx = ColumnIndices {
        qty: 0,
        price: 1,
        disc: 2,
        ship: 3,
    };
    let spec = Q6Spec::try_new(predicate, &schema).expect("Q6Spec::try_new");

    // ---- Correctness: paths produce identical results ----
    let hand_sum = process_q6_batch_hand(&batch, predicate, hand_idx);
    let mut spec_acc = 0.0;
    spec.process_batch(&batch, &mut spec_acc).unwrap();
    assert!(
        (hand_sum - spec_acc).abs() < 1e-9,
        "hand {hand_sum} != unified {spec_acc}",
    );

    println!("== Σ.G.2 Q6 unified-vs-hand perf gate ==");
    println!(
        "  fixture: {} rows × 4 cols, predicate ≈ Q6 SF=1 selectivity",
        ROWS_PER_BATCH
    );
    println!(
        "  trials:  {} (×{} batches/trial, after {} warmups each)",
        TRIALS, BATCHES_PER_TRIAL, WARMUPS
    );
    println!();

    // ---- Warmup ----
    for _ in 0..WARMUPS {
        let _ = process_q6_batch_hand(&batch, predicate, hand_idx);
        let mut acc = 0.0;
        spec.process_batch(&batch, &mut acc).unwrap();
    }

    // ---- Time hand path ----
    let mut hand_times = Vec::with_capacity(TRIALS);
    for _ in 0..TRIALS {
        let t = Instant::now();
        let mut s = 0.0f64;
        for _ in 0..BATCHES_PER_TRIAL {
            s += process_q6_batch_hand(&batch, predicate, hand_idx);
        }
        hand_times.push(t.elapsed().as_nanos() / BATCHES_PER_TRIAL as u128);
        std::hint::black_box(s);
    }
    hand_times.sort_unstable();
    let hand_median = hand_times[TRIALS / 2];

    // ---- Time unified path ----
    let mut spec_times = Vec::with_capacity(TRIALS);
    for _ in 0..TRIALS {
        let t = Instant::now();
        let mut acc = 0.0;
        for _ in 0..BATCHES_PER_TRIAL {
            spec.process_batch(&batch, &mut acc).unwrap();
        }
        spec_times.push(t.elapsed().as_nanos() / BATCHES_PER_TRIAL as u128);
        std::hint::black_box(acc);
    }
    spec_times.sort_unstable();
    let spec_median = spec_times[TRIALS / 2];

    println!("  hand    median {:>8} ns / batch", fmt_ns(hand_median));
    println!("  unified median {:>8} ns / batch", fmt_ns(spec_median));
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
        println!("  GATE PASS — unified within 3 % of hand. Σ.G.2 migration is safe.");
    } else {
        println!(
            "  GATE FAIL — unified is {:.2} % slower than hand (> 3 %).",
            delta_pct
        );
        println!("  Do NOT migrate FusedFilterSumExec to FusedAggregateExec<Q6Spec>");
        println!("  without investigating: try #[inline(always)] on process_batch,");
        println!("  or check the LLVM output of `cargo rustc --release -- --emit=asm`.");
        // Non-zero exit so CI / scripts can detect the failure.
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

/// Canonical TPC-H Q6 predicate:
/// `shipdate ∈ [1994-01-01, 1995-01-01), discount ∈ [0.05, 0.07], quantity < 24`.
/// Selectivity on SF=1 lineitem ≈ 1.9 %.
fn canonical_q6_predicate() -> Q6Predicate {
    Q6Predicate {
        date_lo: 8766, // 1994-01-01 days since 1970-01-01
        date_hi: 9131, // 1995-01-01
        disc_lo: 0.05,
        disc_hi: 0.07,
        qty_hi: 24.0,
    }
}

/// Build a 65,536-row lineitem-shaped batch with realistic-ish
/// distributions: shipdate uniform in 1992-1998 (so the Q6 window
/// hits ~14 %), discount uniform in [0.0, 0.10], quantity uniform
/// in [1, 50]. Random but seeded so re-runs are deterministic.
fn synth_lineitem_batch() -> (RecordBatch, std::sync::Arc<Schema>) {
    let mut rng = Lcg::new(0x517A_603A);

    let qty: Vec<f64> = (0..ROWS_PER_BATCH)
        .map(|_| rng.next_in_range_f64(1.0, 50.0))
        .collect();
    let price: Vec<f64> = (0..ROWS_PER_BATCH)
        .map(|_| rng.next_in_range_f64(900.0, 105_000.0))
        .collect();
    let disc: Vec<f64> = (0..ROWS_PER_BATCH)
        .map(|_| rng.next_in_range_f64(0.0, 0.10))
        .collect();
    // 1992-01-01 == Date32 8035; 1998-12-31 == 10592.
    let ship: Vec<i32> = (0..ROWS_PER_BATCH)
        .map(|_| rng.next_in_range_i32(8035, 10592))
        .collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("l_quantity", DataType::Float64, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_shipdate", DataType::Date32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Float64Array::from(qty)),
            Arc::new(Float64Array::from(price)),
            Arc::new(Float64Array::from(disc)),
            Arc::new(Date32Array::from(ship)),
        ],
    )
    .unwrap();
    (batch, schema)
}
