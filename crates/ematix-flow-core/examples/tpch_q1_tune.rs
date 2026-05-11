//! Σ.D2: TPC-H Q1 (multi-aggregate + group-by) hand-written fused kernel.
//!
//! Q1 is the canonical multi-aggregate-with-filter analytical workload:
//! 5 SUM accumulators + a COUNT, grouped by `(l_returnflag, l_linestatus)`
//! (4 actual groups in TPC-H), filtered by `l_shipdate <= 1998-09-02`. The
//! query is structurally what every dashboard / metrics-rollup workload
//! looks like — one filter, many aggregates, low-cardinality group keys.
//!
//! Today's DataFusion runs the filter once, then dispatches each aggregate
//! kernel independently against the materialized filtered rows. Σ.D1 proved
//! the fused-pass insight closes the simple-SUM gap for Q6; this file is the
//! Σ.D2 day-1 prototype that establishes the same lower bound for Q1's
//! multi-aggregate shape. If a parallel hand-written single-pass beats
//! today's DataFusion path by ~5–10×, the architectural premise is
//! confirmed and we proceed to the `FusedFilterMultiAggExec` wrap (issue
//! #45, phase 2).
//!
//! Sections:
//!   1. DataFusion Q1 default (parquet source)
//!   2. DataFusion Q1, pre-decoded MemTable
//!   3. Hand-written fused, hardcoded 4-arm group match, single-thread
//!   4. Hand-written fused, hardcoded 4-arm group match, parallel
//!   5. Hand-written fused, generic HashMap<(u8,u8), Aggs>, parallel
//!      (measures the cost of "general predicate + generic group lookup"
//!      vs the hardcoded floor — informs the phase-3 generalization)
//!   6. Sanity check: per-group totals vs DataFusion's Q1 output
//!
//! Reference numbers (M3 Pro / SF=1, 2026-05-11 audit):
//!   * DataFusion parquet (May 5 baseline)        ~48.7 ms
//!   * Polars `.polars.sql` parquet               ~40.9 ms
//!   * Polars `.polars.sql` MemTable              ~35.2 ms
//!
//! ### Σ.D2 phase-1 result (2026-05-11)
//!
//! | Path                                                | Median  | vs DF parquet |
//! |-----------------------------------------------------|---------|---------------|
//! | DataFusion Q1, parquet (today)                      | 47.65   | —             |
//! | DataFusion Q1, MemTable                             | 25.61   | 1.86×         |
//! | Polars `.polars.sql`, parquet (ref)                 | 40.9    | 1.17×         |
//! | Polars `.polars.sql`, MemTable (ref)                | 35.2    | 1.35×         |
//! | Hand-written fused, hardcoded match, single-thread  | 20.94   | 2.28×         |
//! | **Hand-written fused, hardcoded match, 14-thread**  | **3.08**| **15.5×**     |
//! | Hand-written fused, HashMap groups, 14-thread       | 7.01    | 6.80×         |
//!
//! Architectural premise — **dramatically** confirmed.
//!
//! - The parallel hardcoded-match floor is **15.5× faster than today's
//!   DataFusion path, 11.4× faster than Polars MemTable**, smashes the
//!   ≤ 5 ms phase-1 success criterion. The shape — 5 SUMs + COUNT +
//!   group-by on 4 dense slots — is exactly where independent kernel
//!   dispatch hurts most.
//! - The HashMap variant at 7.01 ms quantifies "cost of generality":
//!   ~2.3× slower than hardcoded, but still ~3.7× faster than today's
//!   DataFusion MemTable. Phase-3 (generic predicate + group lookup)
//!   can ship the HashMap path and still keep a wide win margin.
//! - Sanity check: per-group totals match canonical TPC-H Q1 SF=1
//!   reference values to the cent (see Section 6 output).
//!
//! Phase 2 follow-on (issue #45 day-2): wrap as `FusedFilterMultiAggExec`
//! `ExecutionPlan`. Expected ~0.3 ms tokio/spawn-blocking hop, same
//! pattern as Σ.D1 — lands the operator at ≈ 3.4 ms inside DataFusion's
//! runtime. The win flows through to every multi-aggregate-with-filter
//! query the planner rule (phase 4) recognizes.
//!
//! Usage:
//!     cargo run --release -p ematix-flow-core --example tpch_q1_tune
//!
//! Requires `examples/tpch/data/sf1/lineitem.parquet`:
//!     cargo run --release -p ematix-flow-core --example tpch_generate \
//!         -- --sf 1 --out examples/tpch/data/sf1

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Array, Date32Array, Float64Array, RecordBatch, StringViewArray};
use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures_util::TryStreamExt;

const Q1: &str = include_str!("../../../examples/tpch/queries/q01.sql");

/// Date32 day count for `1998-09-02` (which is `1998-12-01 - 90 days`,
/// the TPC-H Q1 cutoff). `1998-09-02 - 1970-01-01 = 10471 days`.
const Q1_SHIPDATE_CUTOFF: i32 = 10471;

/// Per-group accumulator. Five running SUMs + a row count cover all 8
/// outputs Q1 reports — AVG(qty), AVG(price), AVG(disc) derive from the
/// sums divided by `count` at finalize time.
#[derive(Default, Debug, Clone, Copy)]
struct Q1Aggs {
    sum_qty: f64,
    sum_price: f64,
    sum_disc_price: f64,
    sum_charge: f64,
    sum_disc: f64,
    count: u64,
}

impl Q1Aggs {
    fn merge(&mut self, other: &Q1Aggs) {
        self.sum_qty += other.sum_qty;
        self.sum_price += other.sum_price;
        self.sum_disc_price += other.sum_disc_price;
        self.sum_charge += other.sum_charge;
        self.sum_disc += other.sum_disc;
        self.count += other.count;
    }
}

/// Hardcoded 4-arm group match for the TPC-H-known group keys. Index 4
/// is a junk catch-all so the inner loop's `groups[g]` indexing stays
/// branchless. Order: (R,F)=0, (N,F)=1, (N,O)=2, (A,F)=3, other=4.
#[inline(always)]
fn q1_group_idx(rflag: u8, lstatus: u8) -> usize {
    match (rflag, lstatus) {
        (b'R', b'F') => 0,
        (b'N', b'F') => 1,
        (b'N', b'O') => 2,
        (b'A', b'F') => 3,
        _ => 4,
    }
}

/// Single-pass fused loop over the seven Q1 columns, indexed in the
/// caller-defined projection order: 0=returnflag, 1=linestatus,
/// 2=quantity, 3=price, 4=discount, 5=tax, 6=shipdate.
fn run_fused_q1_hardcoded(batches: &[RecordBatch], cutoff: i32) -> [Q1Aggs; 5] {
    let mut groups = [Q1Aggs::default(); 5];
    for batch in batches {
        let rflag = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("returnflag StringViewArray");
        let lstatus = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("linestatus StringViewArray");
        let qty = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("quantity f64");
        let price = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("price f64");
        let disc = batch
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("discount f64");
        let tax = batch
            .column(5)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("tax f64");
        let ship = batch
            .column(6)
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("shipdate Date32");
        let qty_v = qty.values();
        let price_v = price.values();
        let disc_v = disc.values();
        let tax_v = tax.values();
        let ship_v = ship.values();
        for i in 0..batch.num_rows() {
            if ship_v[i] > cutoff {
                continue;
            }
            let r = rflag.value(i).as_bytes()[0];
            let l = lstatus.value(i).as_bytes()[0];
            let g = q1_group_idx(r, l);

            let q = qty_v[i];
            let p = price_v[i];
            let d = disc_v[i];
            let t = tax_v[i];

            let omd = 1.0 - d;
            let disc_price = p * omd;
            let charge = disc_price * (1.0 + t);

            let a = &mut groups[g];
            a.sum_qty += q;
            a.sum_price += p;
            a.sum_disc_price += disc_price;
            a.sum_charge += charge;
            a.sum_disc += d;
            a.count += 1;
        }
    }
    groups
}

/// Parallel variant: shard batches across worker threads, run the
/// fused loop per shard, merge partials. Matches the Σ.D1 day-1
/// `std::thread::scope` pattern.
fn run_fused_q1_hardcoded_parallel(
    batches: &[RecordBatch],
    cutoff: i32,
    workers: usize,
) -> [Q1Aggs; 5] {
    let n = batches.len();
    let chunk = n.div_ceil(workers.max(1));
    std::thread::scope(|s| {
        let handles: Vec<_> = (0..workers)
            .map(|w| {
                let lo = (w * chunk).min(n);
                let hi = ((w + 1) * chunk).min(n);
                let slice = &batches[lo..hi];
                s.spawn(move || run_fused_q1_hardcoded(slice, cutoff))
            })
            .collect();
        let mut merged = [Q1Aggs::default(); 5];
        for h in handles {
            let partial = h.join().unwrap();
            for g in 0..merged.len() {
                merged[g].merge(&partial[g]);
            }
        }
        merged
    })
}

/// HashMap-based variant for an honest "general predicate + arbitrary
/// group keys" lower bound. Measures the cost of generality vs the
/// hardcoded 4-arm match: if HashMap parallel is competitive, the
/// phase-3 generalization can keep the simple HashMap approach; if it
/// loses meaningfully, the planner needs a per-shape perfect-hash path.
fn run_fused_q1_hashmap(batches: &[RecordBatch], cutoff: i32) -> HashMap<(u8, u8), Q1Aggs> {
    let mut groups: HashMap<(u8, u8), Q1Aggs> = HashMap::with_capacity(8);
    for batch in batches {
        let rflag = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        let lstatus = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        let qty = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let price = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let disc = batch
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let tax = batch
            .column(5)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let ship = batch
            .column(6)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        let qty_v = qty.values();
        let price_v = price.values();
        let disc_v = disc.values();
        let tax_v = tax.values();
        let ship_v = ship.values();
        for i in 0..batch.num_rows() {
            if ship_v[i] > cutoff {
                continue;
            }
            let r = rflag.value(i).as_bytes()[0];
            let l = lstatus.value(i).as_bytes()[0];
            let key = (r, l);

            let q = qty_v[i];
            let p = price_v[i];
            let d = disc_v[i];
            let t = tax_v[i];
            let omd = 1.0 - d;
            let disc_price = p * omd;
            let charge = disc_price * (1.0 + t);

            let a = groups.entry(key).or_default();
            a.sum_qty += q;
            a.sum_price += p;
            a.sum_disc_price += disc_price;
            a.sum_charge += charge;
            a.sum_disc += d;
            a.count += 1;
        }
    }
    groups
}

fn run_fused_q1_hashmap_parallel(
    batches: &[RecordBatch],
    cutoff: i32,
    workers: usize,
) -> HashMap<(u8, u8), Q1Aggs> {
    let n = batches.len();
    let chunk = n.div_ceil(workers.max(1));
    std::thread::scope(|s| {
        let handles: Vec<_> = (0..workers)
            .map(|w| {
                let lo = (w * chunk).min(n);
                let hi = ((w + 1) * chunk).min(n);
                let slice = &batches[lo..hi];
                s.spawn(move || run_fused_q1_hashmap(slice, cutoff))
            })
            .collect();
        let mut merged: HashMap<(u8, u8), Q1Aggs> = HashMap::with_capacity(8);
        for h in handles {
            let partial = h.join().unwrap();
            for (k, v) in partial {
                merged.entry(k).or_default().merge(&v);
            }
        }
        merged
    })
}

async fn bench_q1_default(label: &str, ctx: &SessionContext) {
    // 1 untimed warm-up.
    let _ = ctx.sql(Q1).await.unwrap().collect().await.unwrap();
    let mut times = Vec::with_capacity(5);
    for _ in 0..5 {
        let start = Instant::now();
        let _: Vec<RecordBatch> = ctx.sql(Q1).await.unwrap().collect().await.unwrap();
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "  {label:<60}  median {:>6.2} ms  (min {:>5.2}  max {:>5.2})",
        times[2], times[0], times[4],
    );
}

async fn make_ctx(cfg: SessionConfig, parquet: &str) -> SessionContext {
    let ctx = SessionContext::new_with_config(cfg);
    ctx.register_parquet("lineitem", parquet, Default::default())
        .await
        .unwrap();
    ctx
}

/// Pre-decode lineitem into a MemTable projecting only the seven Q1
/// columns. Returns both the context (for `bench_q1_default`) and the
/// raw batch list (for the hand-written paths).
async fn make_memtable_ctx(parquet: &str) -> (SessionContext, Vec<RecordBatch>) {
    let staging = SessionContext::new();
    staging
        .register_parquet("lineitem_pq", parquet, Default::default())
        .await
        .unwrap();
    let df = staging
        .sql(
            "select l_returnflag, l_linestatus, l_quantity, \
             l_extendedprice, l_discount, l_tax, l_shipdate \
             from lineitem_pq",
        )
        .await
        .unwrap();
    let schema = Arc::new(df.schema().as_arrow().clone());
    let batches: Vec<RecordBatch> = df
        .execute_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!(
        "  (memtable preload: {} batches, {total} rows decoded once)",
        batches.len()
    );
    let mem = MemTable::try_new(schema, vec![batches.clone()]).unwrap();
    let ctx = SessionContext::new();
    ctx.register_table("lineitem", Arc::new(mem)).unwrap();
    (ctx, batches)
}

fn print_groups(label: &str, groups: &[Q1Aggs; 5]) {
    println!("  {label}");
    for (g, name) in [
        (0, "(R,F)"),
        (1, "(N,F)"),
        (2, "(N,O)"),
        (3, "(A,F)"),
        (4, "(other)"),
    ] {
        let a = &groups[g];
        if a.count == 0 {
            continue;
        }
        let avg_qty = a.sum_qty / a.count as f64;
        let avg_price = a.sum_price / a.count as f64;
        let avg_disc = a.sum_disc / a.count as f64;
        println!(
            "    {name}: sum_qty={:.2} sum_price={:.2} sum_disc_price={:.2} sum_charge={:.2} avg_qty={avg_qty:.2} avg_price={avg_price:.2} avg_disc={avg_disc:.4} count={}",
            a.sum_qty, a.sum_price, a.sum_disc_price, a.sum_charge, a.count
        );
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let parquet = manifest
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("examples/tpch/data/sf1/lineitem.parquet");
    let parquet = parquet.to_str().unwrap();
    println!("==> Σ.D2: Q1 multi-aggregate fused-kernel prototype");
    println!("==> data: {parquet}");
    println!(
        "==> reference: DataFusion ~48.7 ms parquet (May 5) · Polars 40.9 / 35.2 ms parquet / MemTable"
    );
    println!();

    println!("--- Section 1: DataFusion Q1, default config (parquet source) ---");
    let ctx = make_ctx(SessionConfig::new(), parquet).await;
    bench_q1_default("default SessionConfig", &ctx).await;
    println!();

    println!("--- Section 2: DataFusion Q1, pre-decoded MemTable ---");
    let (ctx_mem, batches) = make_memtable_ctx(parquet).await;
    bench_q1_default("MemTable, default SessionConfig", &ctx_mem).await;
    println!();

    println!("--- Section 3: hand-written fused, hardcoded match, single-thread ---");
    let cutoff = Q1_SHIPDATE_CUTOFF;
    // Warm-up
    let _ = run_fused_q1_hardcoded(&batches, cutoff);
    let mut times = Vec::with_capacity(5);
    for _ in 0..5 {
        let start = Instant::now();
        let _ = run_fused_q1_hardcoded(&batches, cutoff);
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "  hardcoded match, single-thread                                median {:>6.2} ms  (min {:>5.2}  max {:>5.2})",
        times[2], times[0], times[4],
    );
    println!();

    let ncpu = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    println!("--- Section 4: hand-written fused, hardcoded match, {ncpu}-thread ---");
    let _ = run_fused_q1_hardcoded_parallel(&batches, cutoff, ncpu);
    let mut times = Vec::with_capacity(5);
    let mut last: Option<[Q1Aggs; 5]> = None;
    for _ in 0..5 {
        let start = Instant::now();
        let groups = run_fused_q1_hardcoded_parallel(&batches, cutoff, ncpu);
        times.push(start.elapsed().as_secs_f64() * 1000.0);
        last = Some(groups);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "  hardcoded match, parallel                                     median {:>6.2} ms  (min {:>5.2}  max {:>5.2})",
        times[2], times[0], times[4],
    );
    println!();

    println!("--- Section 5: hand-written fused, HashMap groups, {ncpu}-thread ---");
    let _ = run_fused_q1_hashmap_parallel(&batches, cutoff, ncpu);
    let mut times = Vec::with_capacity(5);
    for _ in 0..5 {
        let start = Instant::now();
        let _ = run_fused_q1_hashmap_parallel(&batches, cutoff, ncpu);
        times.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "  HashMap groups, parallel                                      median {:>6.2} ms  (min {:>5.2}  max {:>5.2})",
        times[2], times[0], times[4],
    );
    println!();

    println!("--- Section 6: sanity — hand-written totals (compare to DataFusion Q1 output) ---");
    if let Some(g) = last {
        print_groups("hand-written parallel results:", &g);
    }
    println!();

    println!("--- Section 7: FusedFilterMultiAggExec (Σ.D2 phase-2 wrapped operator) ---");
    use datafusion::physical_plan::ExecutionPlan;
    use ematix_flow_core::fused_multi_agg::{FusedFilterMultiAggExec, Q1Predicate};

    let logical = ctx_mem
        .sql(
            "SELECT l_returnflag, l_linestatus, l_quantity, \
             l_extendedprice, l_discount, l_tax, l_shipdate \
             FROM lineitem",
        )
        .await
        .unwrap();
    let child = logical.create_physical_plan().await.unwrap();
    let fused: Arc<dyn ExecutionPlan> = Arc::new(
        FusedFilterMultiAggExec::try_new_q1(
            child,
            Q1Predicate {
                shipdate_cutoff: cutoff,
            },
        )
        .unwrap(),
    );
    let task_ctx = ctx_mem.task_ctx();
    // Warm-up.
    let mut s = fused.execute(0, task_ctx.clone()).unwrap();
    let _ = s.try_next().await.unwrap();

    let mut times = Vec::with_capacity(5);
    for _ in 0..5 {
        let mut s = fused.execute(0, task_ctx.clone()).unwrap();
        let start = Instant::now();
        let b = s.try_next().await.unwrap().expect("Q1 output batch");
        times.push(start.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(b.num_rows(), 4, "Q1 output should have 4 groups");
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "  FusedFilterMultiAggExec (MemTable child, in DataFusion runtime)  median {:>6.2} ms  (min {:>5.2}  max {:>5.2})",
        times[2], times[0], times[4],
    );
    println!();
    println!("==> Phase-2 verdict: wrap overhead is the (Section 7 - Section 4) delta.");
    println!("    Σ.D1 reference: wrap added 0.28 ms to a 0.96 ms floor.");
}
