//! PV.1 — Phase-0 sizing of the FULL-pipeline push benefit for Q08 SF=10.
//!
//! PV.0 held decode constant and isolated only the join EMIT (take vs append) →
//! tied. This goes the other way: it builds a faithful row-group-parallel **fused**
//! Q08 hot path — decode each RG, then probe part+orders+supplier and accumulate the
//! aggregate INLINE over that RG's rows, discarding before the next RG — exactly the
//! morsel/push model (never materialise the 60M-row intermediate, no RepartitionExec,
//! no per-join `take`, no inter-operator RecordBatch). Compares it to the REAL
//! production pull pipeline (`ctx.sql(Q08)`), same process, interleaved.
//!
//!   push benefit ≈ Q08_pull − PUSH_fused      (the pull-model materialisation/
//!                                               repartition/scheduling overhead)
//!
//! Decisive read:
//!   PUSH ≪ PULL and < DuckDB (167ms)  → GO: the execution model is the lever.
//!   PUSH ≈ PULL                        → NO-GO: the cost is decode+probe (irreducible).
//!
//! Also times decode-only (to confirm the fused decode is fair, ~143ms — the B
//! measurement) and runs the part probe as BOTH a dense bitset (what an optimised
//! integer-FK push engine uses) and a HashSet (DuckDB-like) to bracket probe realism.
//! EMAT_PUSH_THREADS sweeps the fused-pass parallelism (default 14) for a scaling read.
//!
//! Usage:
//!   TPCH_DATA_DIR=examples/tpch/data/sf10 TRIALS=9 \
//!     cargo run --release -p ematix-flow-core --example pv1_fused_q08_pipeline --features triangulation

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Int32Array, Int64Array};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::ematix_parquet_bridge::{masked_decode_f64, masked_decode_i64, open_cached};
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_flow_push::{
    Column, Morsel, ProbePush, ProbeStructure, PushError, PushOperator, Sink, choose, run_pipeline,
};

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

// lineitem column indices (TPC-H schema order): l_orderkey=0, l_partkey=1,
// l_suppkey=2, l_quantity=4, l_extendedprice=5, l_discount=6.
const C_ORDERKEY: usize = 0;
const C_PARTKEY: usize = 1;
const C_SUPPKEY: usize = 2;
const C_EXTPRICE: usize = 5;
const C_DISCOUNT: usize = 6;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Pre-built Q08 dimension side-structures (the small-table reductions).
struct Dims {
    part_bitset: Vec<bool>,        // [p_partkey] = survives p_type filter
    part_set: HashSet<i64>,        // same keys, HashSet variant for probe-realism bracket
    supplier_brazil: Vec<bool>,    // [s_suppkey] = supplier nation is BRAZIL
    orders_year: HashMap<i64, u8>, // o_orderkey -> 0 (1995) / 1 (1996), only AMERICA+date survivors
}

fn all_ones_mask(nrows: usize) -> Vec<u8> {
    let nb = (nrows + 7) / 8;
    let mut m = vec![0xFFu8; nb];
    let rem = nrows % 8;
    if rem != 0 {
        m[nb - 1] = (1u8 << rem) - 1;
    }
    m
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
    let nthreads: usize = std::env::var("EMAT_PUSH_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(14);

    // ---- production pull context (real Q08 plan) ----
    let builder = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(14))
        .with_default_features();
    let builder = ematix_flow_core::preset::with_optimizer_rules(builder);
    let ctx = SessionContext::new_with_state(builder.build());
    register_all(&ctx, &data_dir)?;

    // ---- plain context for the dim-build SQL (production custom rules mis-project
    // these non-canonical helper queries; dims need no optimizer rules) ----
    let ctx_plain =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(14));
    register_all(&ctx_plain, &data_dir)?;
    let q08 = std::fs::read_to_string(data_dir.join("../../queries/q08.sql"))
        .or_else(|_| std::fs::read_to_string("examples/tpch/queries/q08.sql"))?;

    let lineitem_path = data_dir.join("lineitem.parquet");
    eprintln!(
        "PV.1 fused-Q08 sizing — data={}, trials={trials}, push_threads={nthreads}",
        data_dir.display()
    );

    // ---- build Q08 dims (timed inside PUSH each trial; built once here for warmup/correctness) ----
    let dims = build_dims(&ctx_plain).await?;
    eprintln!(
        "dims: part_survivors={}, supplier_brazil={}, orders_survivors={}",
        dims.part_bitset.iter().filter(|b| **b).count(),
        dims.supplier_brazil.iter().filter(|b| **b).count(),
        dims.orders_year.len()
    );

    // ---- warmup + correctness ----
    let pull_rows = {
        let b = ctx.sql(&q08).await?.collect().await?;
        b.iter().map(|x| x.num_rows()).sum::<usize>()
    };
    let (share, _dec) = fused_push(&lineitem_path, &dims, nthreads, true)?;
    eprintln!("PULL Q08 rows={pull_rows}");
    eprintln!(
        "PUSH mkt_share: 1995={:.6} 1996={:.6}  (DuckDB Q08 SF=10: 1995≈0.0345 1996≈0.0395)",
        share[0], share[1]
    );
    // kernel correctness: route through the ematix-flow-push abstraction.
    let kp = build_kernel_probes(&ctx_plain, false).await?;
    eprintln!(
        "kernel probes: part={} orders={} supplier={}",
        kp.part.kind(),
        kp.orders.kind(),
        kp.supplier.kind()
    );
    let kshare = fused_push_kernel(&lineitem_path, &kp, nthreads)?;
    eprintln!(
        "KERNEL mkt_share: 1995={:.6} 1996={:.6}  (must match PUSH above)",
        kshare[0], kshare[1]
    );
    assert!(
        (kshare[0] - share[0]).abs() < 1e-6 && (kshare[1] - share[1]).abs() < 1e-6,
        "kernel result must match hand-fused"
    );

    // ---- interleaved timed trials ----
    let mut pull = Vec::new();
    let mut push_bitset = Vec::new();
    let mut push_hashset = Vec::new();
    let mut decode_only = Vec::new();
    let mut kernel_dense = Vec::new();
    let mut kernel_hash = Vec::new();
    for _ in 0..trials {
        // PULL: real production Q08 pipeline.
        let t = Instant::now();
        let _ = ctx.sql(&q08).await?.collect().await?;
        pull.push(t.elapsed().as_secs_f64() * 1000.0);

        // PUSH (dense bitset part-probe) — dims rebuilt each trial (fair: Q08 does too).
        let d = build_dims(&ctx_plain).await?;
        let t = Instant::now();
        let _ = fused_push(&lineitem_path, &d, nthreads, true)?;
        push_bitset.push(t.elapsed().as_secs_f64() * 1000.0);

        // PUSH (HashSet part-probe) — probe-realism bracket.
        let d = build_dims(&ctx_plain).await?;
        let t = Instant::now();
        let _ = fused_push(&lineitem_path, &d, nthreads, false)?;
        push_hashset.push(t.elapsed().as_secs_f64() * 1000.0);

        // decode-only (no probe/agg) — fairness check vs the 143ms B baseline.
        let t = Instant::now();
        let _ = fused_decode_only(&lineitem_path, nthreads)?;
        decode_only.push(t.elapsed().as_secs_f64() * 1000.0);

        // KERNEL (PV.1 ematix-flow-push abstraction) — THE KILL-GATE. Same
        // fused Q08 loop, routed through Morsel/PushOperator/Sink traits +
        // the adaptive `choose` probe selector. Must still clear ≥12% vs PULL.
        let kp = build_kernel_probes(&ctx_plain, false).await?;
        let t = Instant::now();
        let _ = fused_push_kernel(&lineitem_path, &kp, nthreads)?;
        kernel_dense.push(t.elapsed().as_secs_f64() * 1000.0);

        // OQ-PS-1: the SAME kernel pipeline with probes FORCED to hash (no
        // dense bitset/payload) — does removing materialisation alone, with
        // a hash probe, still beat PULL? Decides whether §5.2 must gate
        // fusion on densifiability.
        let kp = build_kernel_probes(&ctx_plain, true).await?;
        let t = Instant::now();
        let _ = fused_push_kernel(&lineitem_path, &kp, nthreads)?;
        kernel_hash.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    let med = |v: &mut Vec<f64>| {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[v.len() / 2]
    };
    let pull_m = med(&mut pull);
    let pb_m = med(&mut push_bitset);
    let ph_m = med(&mut push_hashset);
    let dec_m = med(&mut decode_only);
    let kd_m = med(&mut kernel_dense);
    let kh_m = med(&mut kernel_hash);

    println!(
        "\n=== PV.1 Q08 SF=10 full-pipeline push sizing ({trials} trials, {nthreads} threads) ==="
    );
    println!("PULL  real production Q08:          {pull_m:7.1} ms");
    println!(
        "PUSH  hand-fused (dense-bitset):    {pb_m:7.1} ms   ({:+.1} ms, {:+.1}%)",
        pull_m - pb_m,
        (pb_m / pull_m - 1.0) * 100.0
    );
    println!(
        "PUSH  hand-fused (HashSet):         {ph_m:7.1} ms   ({:+.1} ms, {:+.1}%)",
        pull_m - ph_m,
        (ph_m / pull_m - 1.0) * 100.0
    );
    println!(
        "KERNEL ematix-flow-push (adaptive): {kd_m:7.1} ms   ({:+.1} ms, {:+.1}%)  <== KILL-GATE (≥12% vs PULL)",
        pull_m - kd_m,
        (kd_m / pull_m - 1.0) * 100.0
    );
    println!(
        "KERNEL forced-hash (OQ-PS-1):       {kh_m:7.1} ms   ({:+.1} ms, {:+.1}%)",
        pull_m - kh_m,
        (kh_m / pull_m - 1.0) * 100.0
    );
    println!(
        "  (of which) fused decode-only:     {dec_m:7.1} ms   [B baseline ≈143ms = fair if close]"
    );
    println!("DuckDB Q08 SF=10 reference:           167.4 ms");
    let gate = (1.0 - kd_m / pull_m) * 100.0;
    println!("\nKILL-GATE: kernel abstraction is {gate:.1}% faster than PULL.");
    if gate >= 12.0 {
        println!("  PASS (≥12%) — the Morsel/PushOperator/Sink indirection did NOT eat the win.");
    } else {
        println!("  FAIL (<12%) — the trait/morsel layering ate the win; redesign the interface");
        println!("  (monomorphize the chain, drop dyn) before PV.2.");
    }
    println!(
        "OQ-PS-1: forced-hash kernel is {:+.1}% vs PULL (negative = still wins) — if it",
        (kh_m / pull_m - 1.0) * 100.0
    );
    println!("  regresses (>0%), §5.2 must gate fusion on a densifiable domain; if neutral or");
    println!("  better, non-dense shapes may still fuse with a hash probe.");
    Ok(())
}

fn register_all(
    ctx: &SessionContext,
    data_dir: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    for t in TPCH_TABLES {
        let path = data_dir.join(format!("{t}.parquet"));
        let use_emat = *t == "lineitem" || *t == "orders";
        if use_emat {
            ctx.register_table(
                *t,
                Arc::new(EmatixFastParquetTableProvider::try_new(
                    path.to_string_lossy(),
                )?),
            )?;
        } else {
            ctx.register_table(
                *t,
                Arc::new(FastParquetTableProvider::try_new(path.to_string_lossy())?),
            )?;
        }
    }
    Ok(())
}

/// Build the three Q08 dimension reductions via SQL on the small tables.
async fn build_dims(ctx: &SessionContext) -> Result<Dims, Box<dyn std::error::Error>> {
    // part survivors (p_type filter). p_partkey ∈ [1, 2_000_000] at SF=10.
    let mut part_bitset = vec![false; 2_000_001];
    let mut part_set = HashSet::new();
    let pb = ctx
        .sql("SELECT p_partkey FROM part WHERE p_type = 'ECONOMY ANODIZED STEEL'")
        .await?
        .collect()
        .await?;
    for b in &pb {
        let k = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..k.len() {
            let v = k.value(i);
            if (v as usize) < part_bitset.len() {
                part_bitset[v as usize] = true;
            }
            part_set.insert(v);
        }
    }

    // supplier nation == BRAZIL. s_suppkey ∈ [1, 100_000] at SF=10.
    let mut supplier_brazil = vec![false; 100_001];
    let sb = ctx
        .sql("SELECT s.s_suppkey, n.n_name FROM supplier s JOIN nation n ON s.s_nationkey = n.n_nationkey")
        .await?
        .collect()
        .await?;
    for b in &sb {
        let sk = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let nm = b
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringViewArray>();
        for i in 0..sk.len() {
            let is_br = match nm {
                Some(a) => a.value(i) == "BRAZIL",
                None => b
                    .column(1)
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::StringArray>()
                    .map(|a| a.value(i) == "BRAZIL")
                    .unwrap_or(false),
            };
            let k = sk.value(i);
            if is_br && (k as usize) < supplier_brazil.len() {
                supplier_brazil[k as usize] = true;
            }
        }
    }

    // orders surviving date + AMERICA-region: o_orderkey -> year-bucket.
    let mut orders_year = HashMap::with_capacity(1_000_000);
    let ob = ctx
        .sql(
            "SELECT o.o_orderkey, extract(year FROM o.o_orderdate) AS y \
             FROM orders o \
             JOIN customer c ON o.o_custkey = c.c_custkey \
             JOIN nation n ON c.c_nationkey = n.n_nationkey \
             JOIN region r ON n.n_regionkey = r.r_regionkey \
             WHERE r.r_name = 'AMERICA' \
               AND o.o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'",
        )
        .await?
        .collect()
        .await?;
    for b in &ob {
        let ok = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        // extract(year ...) is Int32 in DataFusion 53.
        let yr_i32 = b.column(1).as_any().downcast_ref::<Int32Array>();
        let yr_i64 = b.column(1).as_any().downcast_ref::<Int64Array>();
        for i in 0..ok.len() {
            let y = match (yr_i32, yr_i64) {
                (Some(a), _) => a.value(i) as i64,
                (_, Some(a)) => a.value(i),
                _ => continue,
            };
            let bucket = if y == 1995 { 0u8 } else { 1u8 };
            orders_year.insert(ok.value(i), bucket);
        }
    }

    Ok(Dims {
        part_bitset,
        part_set,
        supplier_brazil,
        orders_year,
    })
}

/// Fused row-group-parallel Q08 hot path. Returns ([share_1995, share_1996], decode_ms).
/// `use_bitset` toggles dense-bitset vs HashSet part membership.
fn fused_push(
    lineitem: &std::path::Path,
    dims: &Dims,
    nthreads: usize,
    use_bitset: bool,
) -> Result<([f64; 2], f64), Box<dyn std::error::Error>> {
    let file = open_cached(lineitem)?;
    let md = file.metadata()?;
    let n_rg = md.row_groups.len();

    // thread-local accumulators: [(total, brazil); 2 years]
    let totals = std::sync::Mutex::new([[0.0f64; 2]; 2]); // [year][0=total,1=brazil]
    std::thread::scope(|scope| {
        for t in 0..nthreads {
            let file = Arc::clone(&file);
            let dims = &dims;
            let totals = &totals;
            let md = &md;
            scope.spawn(move || {
                let mut local = [[0.0f64; 2]; 2];
                let mut rg = t;
                while rg < n_rg {
                    let nrows = md.row_groups[rg].num_rows as usize;
                    let mask = all_ones_mask(nrows);
                    let pk = masked_decode_i64(&file, rg, C_PARTKEY, &mask).unwrap();
                    let ok = masked_decode_i64(&file, rg, C_ORDERKEY, &mask).unwrap();
                    let sk = masked_decode_i64(&file, rg, C_SUPPKEY, &mask).unwrap();
                    let ep = masked_decode_f64(&file, rg, C_EXTPRICE, &mask).unwrap();
                    let disc = masked_decode_f64(&file, rg, C_DISCOUNT, &mask).unwrap();
                    let n = pk
                        .len()
                        .min(ok.len())
                        .min(sk.len())
                        .min(ep.len())
                        .min(disc.len());
                    for i in 0..n {
                        let p = pk[i];
                        let hit = if use_bitset {
                            (p as usize) < dims.part_bitset.len() && dims.part_bitset[p as usize]
                        } else {
                            dims.part_set.contains(&p)
                        };
                        if !hit {
                            continue;
                        }
                        if let Some(&yb) = dims.orders_year.get(&ok[i]) {
                            let vol = ep[i] * (1.0 - disc[i]);
                            local[yb as usize][0] += vol;
                            let s = sk[i];
                            if (s as usize) < dims.supplier_brazil.len()
                                && dims.supplier_brazil[s as usize]
                            {
                                local[yb as usize][1] += vol;
                            }
                        }
                    }
                    rg += nthreads;
                }
                let mut g = totals.lock().unwrap();
                for y in 0..2 {
                    g[y][0] += local[y][0];
                    g[y][1] += local[y][1];
                }
            });
        }
    });
    let g = totals.into_inner().unwrap();
    let share = [
        if g[0][0] > 0.0 {
            g[0][1] / g[0][0]
        } else {
            0.0
        },
        if g[1][0] > 0.0 {
            g[1][1] / g[1][0]
        } else {
            0.0
        },
    ];
    Ok((share, 0.0))
}

/// Same parallel decode, but no probe/agg — isolates the decode floor.
fn fused_decode_only(
    lineitem: &std::path::Path,
    nthreads: usize,
) -> Result<u64, Box<dyn std::error::Error>> {
    let file = open_cached(lineitem)?;
    let md = file.metadata()?;
    let n_rg = md.row_groups.len();
    let acc = std::sync::atomic::AtomicU64::new(0);
    std::thread::scope(|scope| {
        for t in 0..nthreads {
            let file = Arc::clone(&file);
            let md = &md;
            let acc = &acc;
            scope.spawn(move || {
                let mut local = 0u64;
                let mut rg = t;
                while rg < n_rg {
                    let nrows = md.row_groups[rg].num_rows as usize;
                    let mask = all_ones_mask(nrows);
                    let pk = masked_decode_i64(&file, rg, C_PARTKEY, &mask).unwrap();
                    let ok = masked_decode_i64(&file, rg, C_ORDERKEY, &mask).unwrap();
                    let sk = masked_decode_i64(&file, rg, C_SUPPKEY, &mask).unwrap();
                    let ep = masked_decode_f64(&file, rg, C_EXTPRICE, &mask).unwrap();
                    let disc = masked_decode_f64(&file, rg, C_DISCOUNT, &mask).unwrap();
                    // touch one value per col so decode isn't elided
                    local = local
                        .wrapping_add(pk.len() as u64)
                        .wrapping_add(ok[0].max(0) as u64)
                        .wrapping_add(sk[0].max(0) as u64)
                        .wrapping_add(ep[0] as u64)
                        .wrapping_add(disc[0] as u64);
                    rg += nthreads;
                }
                acc.fetch_add(local, std::sync::atomic::Ordering::Relaxed);
            });
        }
    });
    Ok(acc.load(std::sync::atomic::Ordering::Relaxed))
}

// ===================== PV.1 kernel arm (ematix-flow-push) =====================

/// The three Q08 dimension reductions as `ematix-flow-push` probe
/// structures — built via the adaptive `choose` selector, or forced
/// to hash (OQ-PS-1). part = membership; orders/supplier carry payloads.
struct KernelProbes {
    part: Arc<ProbeStructure>,     // p_partkey survivors (membership)
    orders: Arc<ProbeStructure>,   // o_orderkey -> year-bucket (0=1995, 1=1996)
    supplier: Arc<ProbeStructure>, // s_suppkey -> is_brazil (1/0)
}

async fn build_kernel_probes(
    ctx: &SessionContext,
    force_hash: bool,
) -> Result<KernelProbes, Box<dyn std::error::Error>> {
    // part survivors.
    let mut part_keys: Vec<i64> = Vec::new();
    for b in &ctx
        .sql("SELECT p_partkey FROM part WHERE p_type = 'ECONOMY ANODIZED STEEL'")
        .await?
        .collect()
        .await?
    {
        let k = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..k.len() {
            part_keys.push(k.value(i));
        }
    }
    // supplier -> is_brazil.
    let mut sup_keys: Vec<i64> = Vec::new();
    let mut sup_pay: Vec<i64> = Vec::new();
    for b in &ctx
        .sql("SELECT s.s_suppkey, n.n_name FROM supplier s JOIN nation n ON s.s_nationkey = n.n_nationkey")
        .await?
        .collect()
        .await?
    {
        let sk = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let nm = b
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringViewArray>();
        for i in 0..sk.len() {
            let is_br = match nm {
                Some(a) => a.value(i) == "BRAZIL",
                None => b
                    .column(1)
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::StringArray>()
                    .map(|a| a.value(i) == "BRAZIL")
                    .unwrap_or(false),
            };
            sup_keys.push(sk.value(i));
            sup_pay.push(if is_br { 1 } else { 0 });
        }
    }
    // orders -> year-bucket.
    let mut ord_keys: Vec<i64> = Vec::new();
    let mut ord_pay: Vec<i64> = Vec::new();
    for b in &ctx
        .sql(
            "SELECT o.o_orderkey, extract(year FROM o.o_orderdate) AS y \
             FROM orders o \
             JOIN customer c ON o.o_custkey = c.c_custkey \
             JOIN nation n ON c.c_nationkey = n.n_nationkey \
             JOIN region r ON n.n_regionkey = r.r_regionkey \
             WHERE r.r_name = 'AMERICA' \
               AND o.o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'",
        )
        .await?
        .collect()
        .await?
    {
        let ok = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let yr_i32 = b.column(1).as_any().downcast_ref::<Int32Array>();
        let yr_i64 = b.column(1).as_any().downcast_ref::<Int64Array>();
        for i in 0..ok.len() {
            let y = match (yr_i32, yr_i64) {
                (Some(a), _) => a.value(i) as i64,
                (_, Some(a)) => a.value(i),
                _ => continue,
            };
            ord_keys.push(ok.value(i));
            ord_pay.push(if y == 1995 { 0 } else { 1 });
        }
    }

    let (part, orders, supplier) = if force_hash {
        // OQ-PS-1: defeat densification — measure materialisation-removal alone.
        (
            ProbeStructure::HashSet(part_keys.iter().copied().collect::<HashSet<i64>>()),
            ProbeStructure::HashTable(
                ord_keys
                    .iter()
                    .copied()
                    .zip(ord_pay.iter().copied())
                    .collect::<HashMap<i64, i64>>(),
            ),
            ProbeStructure::HashTable(
                sup_keys
                    .iter()
                    .copied()
                    .zip(sup_pay.iter().copied())
                    .collect::<HashMap<i64, i64>>(),
            ),
        )
    } else {
        (
            choose(&part_keys, None),
            choose(&ord_keys, Some(&ord_pay)),
            choose(&sup_keys, Some(&sup_pay)),
        )
    };
    Ok(KernelProbes {
        part: Arc::new(part),
        orders: Arc::new(orders),
        supplier: Arc::new(supplier),
    })
}

/// Terminal sink: buckets surviving lineitem rows by orders→year and
/// sums `extprice*(1-discount)`, splitting out the BRAZIL share via the
/// supplier payload. This is the single (per-morsel) compaction point.
struct Q08AggSink {
    orders: Arc<ProbeStructure>,
    supplier: Arc<ProbeStructure>,
    acc: [[f64; 2]; 2], // [year-bucket][0=total, 1=brazil]
}

impl Sink for Q08AggSink {
    fn emit(&mut self, m: Morsel) -> Result<(), PushError> {
        // cols: 0=partkey 1=orderkey 2=suppkey 3=extprice 4=discount.
        let ok = &m.cols[1];
        let sk = &m.cols[2];
        let ep = &m.cols[3];
        let disc = &m.cols[4];
        let orders = self.orders.clone();
        let supplier = self.supplier.clone();
        let mut acc = self.acc;
        m.sel.for_each(|i| {
            let i = i as usize;
            // orders is an INNER join: a lineitem whose order isn't in the
            // AMERICA+date set is dropped (payload None). One lookup yields
            // both membership and the year-bucket — matching the hand-fused
            // loop's single `orders_map.get`.
            if let Some(bucket) = orders.payload(ok.i64_at(i)) {
                let is_br = supplier.payload(sk.i64_at(i)) == Some(1);
                let vol = ep.f64_at(i) * (1.0 - disc.f64_at(i));
                acc[bucket as usize][0] += vol;
                if is_br {
                    acc[bucket as usize][1] += vol;
                }
            }
        });
        self.acc = acc;
        Ok(())
    }
}

/// Q08 hot path routed through the `ematix-flow-push` kernel: per RG,
/// decode 5 cols → `Morsel` → `[ProbePush(part), ProbePush(orders)]` →
/// `Q08AggSink`. RG-parallel; per-thread sink merged at the end.
fn fused_push_kernel(
    lineitem: &std::path::Path,
    kp: &KernelProbes,
    nthreads: usize,
) -> Result<[f64; 2], Box<dyn std::error::Error>> {
    let file = open_cached(lineitem)?;
    let md = file.metadata()?;
    let n_rg = md.row_groups.len();
    let totals = std::sync::Mutex::new([[0.0f64; 2]; 2]);
    std::thread::scope(|scope| {
        for t in 0..nthreads {
            let file = Arc::clone(&file);
            let md = &md;
            let part = Arc::clone(&kp.part);
            let orders = Arc::clone(&kp.orders);
            let supplier = Arc::clone(&kp.supplier);
            let totals = &totals;
            scope.spawn(move || {
                // The 60M-row hot op (part semijoin narrow) goes through the
                // ProbePush trait — morsel col 0 = partkey. orders+supplier
                // are single payload lookups at the sink (one orders probe,
                // no intermediate orders sel), matching the hand-fused loop.
                let _ = &orders;
                let mut ops: Vec<Box<dyn PushOperator>> =
                    vec![Box::new(ProbePush::narrow(0, Arc::clone(&part)))];
                let mut sink = Q08AggSink {
                    orders,
                    supplier,
                    acc: [[0.0; 2]; 2],
                };
                let mut rg = t;
                while rg < n_rg {
                    let nrows = md.row_groups[rg].num_rows as usize;
                    let mask = all_ones_mask(nrows);
                    let pk = masked_decode_i64(&file, rg, C_PARTKEY, &mask).unwrap();
                    let ok = masked_decode_i64(&file, rg, C_ORDERKEY, &mask).unwrap();
                    let sk = masked_decode_i64(&file, rg, C_SUPPKEY, &mask).unwrap();
                    let ep = masked_decode_f64(&file, rg, C_EXTPRICE, &mask).unwrap();
                    let disc = masked_decode_f64(&file, rg, C_DISCOUNT, &mask).unwrap();
                    let morsel = Morsel::new(vec![
                        Column::i64(pk),
                        Column::i64(ok),
                        Column::i64(sk),
                        Column::f64(ep),
                        Column::f64(disc),
                    ]);
                    run_pipeline(&mut ops, [morsel], &mut sink).unwrap();
                    rg += nthreads;
                }
                let mut g = totals.lock().unwrap();
                for y in 0..2 {
                    g[y][0] += sink.acc[y][0];
                    g[y][1] += sink.acc[y][1];
                }
            });
        }
    });
    let g = totals.into_inner().unwrap();
    Ok([
        if g[0][0] > 0.0 {
            g[0][1] / g[0][0]
        } else {
            0.0
        },
        if g[1][0] > 0.0 {
            g[1][1] / g[1][0]
        } else {
            0.0
        },
    ])
}
