//! PV-morsel Q15 Phase-0 de-risk (kill-gate). The Q15 SF=10 gap to Polars is
//! ENTIRELY parallel scaling of the fused scan+filter+GROUP BY l_suppkey
//! pipeline: ematix scales it 5.68×, Polars 6.5-8.7× on the same M4 Max.
//! Polars's mechanism is morsel-driven groupby — per-thread LOCAL hash tables
//! combined at the end, NO Partial→hash-RepartitionExec→FinalPartitioned
//! exchange. This spike asks the GO/NO-GO question for that mechanism on Q15's
//! exact shape (2.27M rows → 100K groups) BEFORE building a DataFusion operator:
//!
//!   does a morsel-driven local-hashtable groupby scale ≫5.68× here, or does
//!   the COMBINE of N local tables become the new serial bottleneck?
//!
//! Three arms on the REAL Q15-filtered (l_suppkey, revenue) arrays:
//!   ST_GLOBAL    — 1 thread, one open-addressing table (single-thread floor)
//!   SERIAL_COMB  — P threads build local tables, then SERIAL merge (exposes
//!                  whether the naive combine kills scaling)
//!   PARTITIONED  — Polars mechanism: partition-on-build into R=P shards, then
//!                  R threads combine independent shards in parallel
//!
//! Reads:
//!   - PARTITIONED scaling to P=14 ≫ DataFusion's 5.68% groupby scaling → GO
//!     (the kernel parallelizes; the lever is dropping the RepartitionExec via
//!     the fused morsel pipeline). Compare PARTITIONED P=14 abs-ms to the
//!     ~13ms DataFusion groupby adds (REVENUE 65 − SCALAR 52): if the kernel is
//!     ~2-3ms, ~10ms of DataFusion's cost is pure exchange overhead.
//!   - PARTITIONED ALSO walls → kill-gate (agg genuinely combine/BW-bound at
//!     this scale; morsel won't save Q15 → accept parity, the lever is SF=100).
//!
//! Usage: TRIALS=9 TPCH_DATA_DIR=examples/tpch/data/sf10 \
//!   cargo run --release -p ematix-flow-core --example q15_morsel_agg_spike

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use datafusion::arrow::array::{Array, Float64Array, Int64Array};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

// Q15's CTE input, ungrouped: one row per qualifying lineitem.
const EXTRACT: &str = "\
SELECT l_suppkey, l_extendedprice * (1 - l_discount) AS rev FROM lineitem \
WHERE l_shipdate >= DATE '1996-01-01' AND l_shipdate < DATE '1996-04-01'";

const EMPTY: i64 = i64::MIN;

#[inline(always)]
fn hash(k: i64) -> usize {
    (k as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) as usize
}

#[inline(always)]
fn next_pow2(mut n: usize) -> usize {
    let mut p = 2048usize;
    n = n.max(1);
    while p < n {
        p <<= 1;
    }
    p
}

/// Open-addressing i64→f64 SUM table, linear probe, pow2 capacity (the
/// realistic morsel-agg kernel — no std HashMap / hasher ambiguity).
struct Tbl {
    keys: Vec<i64>,
    vals: Vec<f64>,
}
impl Tbl {
    fn with_groups(distinct_hint: usize) -> Self {
        let cap = next_pow2(distinct_hint * 2); // load < 0.5
        Tbl {
            keys: vec![EMPTY; cap],
            vals: vec![0.0; cap],
        }
    }
    #[inline(always)]
    fn add(&mut self, k: i64, v: f64) {
        let mask = self.keys.len() - 1;
        let mut i = hash(k) & mask;
        loop {
            unsafe {
                let c = *self.keys.get_unchecked(i);
                if c == k {
                    *self.vals.get_unchecked_mut(i) += v;
                    return;
                }
                if c == EMPTY {
                    *self.keys.get_unchecked_mut(i) = k;
                    *self.vals.get_unchecked_mut(i) = v;
                    return;
                }
            }
            i = (i + 1) & mask;
        }
    }
    fn fold(&self) -> (f64, usize) {
        let mut s = 0.0;
        let mut n = 0;
        for i in 0..self.keys.len() {
            if self.keys[i] != EMPTY {
                s += self.vals[i];
                n += 1;
            }
        }
        (s, n)
    }
    #[inline]
    fn for_each<F: FnMut(i64, f64)>(&self, mut f: F) {
        for i in 0..self.keys.len() {
            let k = self.keys[i];
            if k != EMPTY {
                f(k, self.vals[i]);
            }
        }
    }
}

fn st_global(keys: &[i64], vals: &[f64], distinct: usize) -> (f64, usize) {
    let mut t = Tbl::with_groups(distinct);
    for (&k, &v) in keys.iter().zip(vals) {
        t.add(k, v);
    }
    t.fold()
}

fn serial_combine(keys: &[i64], vals: &[f64], distinct: usize, p: usize) -> (f64, usize) {
    let n = keys.len();
    let chunk = n.div_ceil(p);
    let locals: Vec<Tbl> = thread::scope(|s| {
        let mut hs = Vec::with_capacity(p);
        for t in 0..p {
            let lo = (t * chunk).min(n);
            let hi = ((t + 1) * chunk).min(n);
            let (k, v) = (&keys[lo..hi], &vals[lo..hi]);
            hs.push(s.spawn(move || {
                let mut tb = Tbl::with_groups(distinct);
                for (&kk, &vv) in k.iter().zip(v) {
                    tb.add(kk, vv);
                }
                tb
            }));
        }
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    // SERIAL merge of P local tables on the main thread.
    let mut fin = Tbl::with_groups(distinct);
    for tb in &locals {
        tb.for_each(|k, v| fin.add(k, v));
    }
    fin.fold()
}

fn partitioned(keys: &[i64], vals: &[f64], distinct: usize, p: usize) -> (f64, usize) {
    let n = keys.len();
    let r = p; // shards = threads
    let chunk = n.div_ceil(p);
    let per_shard = distinct / r + 1;
    // Build: P threads, each routes its row-range into R shard tables by the
    // HIGH hash bits (probe within a shard uses the low bits — independent).
    let built: Vec<Vec<Tbl>> = thread::scope(|s| {
        let mut hs = Vec::with_capacity(p);
        for t in 0..p {
            let lo = (t * chunk).min(n);
            let hi = ((t + 1) * chunk).min(n);
            let (k, v) = (&keys[lo..hi], &vals[lo..hi]);
            hs.push(s.spawn(move || {
                let mut shards: Vec<Tbl> = (0..r).map(|_| Tbl::with_groups(per_shard)).collect();
                for (&kk, &vv) in k.iter().zip(v) {
                    let sh = (hash(kk) >> 17) % r;
                    shards[sh].add(kk, vv);
                }
                shards
            }));
        }
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    // Combine: R threads, thread `shard` merges built[t][shard] ∀t — disjoint.
    let built = Arc::new(built);
    let parts: Vec<(f64, usize)> = thread::scope(|s| {
        let mut hs = Vec::with_capacity(r);
        for shard in 0..r {
            let built = Arc::clone(&built);
            hs.push(s.spawn(move || {
                let mut fin = Tbl::with_groups(per_shard);
                for t in 0..p {
                    built[t][shard].for_each(|k, v| fin.add(k, v));
                }
                fin.fold()
            }));
        }
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    (
        parts.iter().map(|x| x.0).sum(),
        parts.iter().map(|x| x.1).sum(),
    )
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn build_ctx(data_dir: &Path) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let cfg = SessionConfig::new().with_target_partitions(14);
    let builder = SessionStateBuilder::new()
        .with_config(cfg)
        .with_default_features();
    let state = ematix_flow_core::preset::with_optimizer_rules(builder).build();
    let ctx = SessionContext::new_with_state(state);
    for t in TPCH_TABLES {
        let p = data_dir.join(format!("{t}.parquet"));
        if *t == "lineitem" || *t == "orders" {
            ctx.register_table(
                *t,
                Arc::new(EmatixFastParquetTableProvider::try_new(
                    p.to_string_lossy(),
                )?),
            )?;
        } else {
            ctx.register_table(
                *t,
                Arc::new(FastParquetTableProvider::try_new(p.to_string_lossy())?),
            )?;
        }
    }
    Ok(ctx)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("examples/tpch/data/sf10"));
    let trials: usize = std::env::var("TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(9);
    let warmups = 3;

    // Extract the real Q15-filtered (suppkey, revenue) once.
    let ctx = build_ctx(&data_dir)?;
    let plan = ctx.sql(EXTRACT).await?.create_physical_plan().await?;
    let batches = collect(plan, ctx.task_ctx()).await?;
    let mut keys: Vec<i64> = Vec::new();
    let mut vals: Vec<f64> = Vec::new();
    for b in &batches {
        let k = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("l_suppkey i64");
        let v = b
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("rev f64");
        for i in 0..b.num_rows() {
            keys.push(k.value(i));
            vals.push(v.value(i));
        }
    }
    let n = keys.len();
    let (gt_sum, gt_groups) = st_global(&keys, &vals, 200_000);
    println!("PV-morsel Q15 agg-kernel spike — {n} rows → {gt_groups} groups, Σrev={gt_sum:.1}");
    println!(
        "(DataFusion baseline: REVENUE P=14 ≈ 65ms, of which groupby ADDS ~13ms over SCALAR 52ms, scales 5.68×)\n"
    );
    println!(
        "{:<14} {:>4} {:>10} {:>9} {:>12}",
        "arm", "P", "ms", "scaling", "checksum_ok"
    );

    let run = |arm: &str, f: &dyn Fn(usize) -> (f64, usize), plist: &[usize]| {
        let mut base = 0.0f64;
        for (idx, &p) in plist.iter().enumerate() {
            for _ in 0..warmups {
                let _ = f(p);
            }
            let mut ts = Vec::with_capacity(trials);
            let mut last = (0.0, 0usize);
            for _ in 0..trials {
                let t0 = Instant::now();
                last = f(p);
                ts.push(t0.elapsed().as_secs_f64() * 1e3);
            }
            let m = median(ts);
            if idx == 0 {
                base = m;
            }
            let ok = (last.0 - gt_sum).abs() / gt_sum.abs() < 1e-9 && last.1 == gt_groups;
            println!("{arm:<14} {p:>4} {m:>10.3} {:>8.2}× {ok:>12}", base / m);
        }
        println!();
    };

    let plist = [1usize, 2, 4, 8, 10, 14];
    run("ST_GLOBAL", &|_| st_global(&keys, &vals, 200_000), &[1]);
    run(
        "SERIAL_COMB",
        &|p| serial_combine(&keys, &vals, 200_000, p),
        &plist,
    );
    run(
        "PARTITIONED",
        &|p| partitioned(&keys, &vals, 200_000, p),
        &plist,
    );

    println!(
        "GO if PARTITIONED scales ≫5.68× (and P=14 abs-ms ≪13ms ⇒ DataFusion's groupby cost is mostly exchange)."
    );
    println!(
        "KILL if PARTITIONED also walls ~5-6× ⇒ agg is combine/BW-bound at 100K groups; accept Q15 parity, lever is SF=100."
    );
    Ok(())
}
