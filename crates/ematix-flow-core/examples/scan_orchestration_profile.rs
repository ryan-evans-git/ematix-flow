//! PV.M.6 — orchestration / batch-assembly probe. Decompose the ~5 ms between
//! raw decode (PV.M.5 Arm A = 46 ms) and production scan-through-DataFusion
//! (§3.1 ws_scan_balance = 51 ms) for the Q15 SCALAR stage.
//!
//!   L0 — raw decode → scalar   (14 std threads, static RG round-robin, masked
//!        decode, sum to f64; NO Arrow, NO DataFusion). = PV.M.5 Arm A.
//!   L2 — production scan        (EmatixFastParquetExec.execute() drained for all
//!        partitions; real Arrow RecordBatches + provider spawn_blocking/mpsc).
//!
//! L2 − L0 = the orchestration + Arrow-batch-assembly cost we're probing. Prints
//! the physical plan + the scan's projected schema + partition count first, so
//! we can SEE whether the shipdate filter is pushed into the scan (survivor
//! output) or sits in a FilterExec above it (full-column output) — that decides
//! what "batch assembly" means and where the 5 ms can live.
//!
//! PROFILE=1 → after the ladder, loop the scan ~`PROFILE_TRIALS` times on a
//! shared ctx and print the PID, for `sample <pid> 15 -file out.txt`.
//!
//! Usage:
//!   TPCH_DATA_DIR=examples/tpch/data/sf10 \
//!     cargo run --release -p ematix-flow-core --example scan_orchestration_profile

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Instant;

use datafusion::arrow::array::Float64Array;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, collect, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::{
    EmatixFastParquetExec, EmatixFastParquetTableProvider,
};
use ematix_flow_core::ematix_parquet_bridge::{masked_decode_f64, open_cached};
use ematix_flow_core::fast_parquet::FastParquetTableProvider;
use ematix_parquet_codec::read::read_column_i32;
use ematix_parquet_io::ParquetFile;
use futures_util::StreamExt;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];
const SCALAR_SQL: &str = "select sum(l_extendedprice * (1 - l_discount)) from lineitem \
     where l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-04-01'";
const SHIP: usize = 10;
const EXT: usize = 5;
const DISC: usize = 6;

fn build_ctx(data_dir: &Path, parts: usize) -> Result<SessionContext, Box<dyn std::error::Error>> {
    let builder = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(parts))
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

fn find_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if plan
        .as_any()
        .downcast_ref::<EmatixFastParquetExec>()
        .is_some()
    {
        return Some(plan.clone());
    }
    for c in plan.children() {
        if let Some(s) = find_scan(c) {
            return Some(s);
        }
    }
    None
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

// L0 unit — masked decode + masked-sum, identical to PV.M.5 Arm A.
#[inline]
fn decode_rg_partial(file: &ParquetFile, rg: usize, lo: i32, hi: i32) -> f64 {
    let ship = read_column_i32(file, rg, SHIP).expect("ship");
    let mut bm = vec![0u8; ship.len().div_ceil(8)];
    for (i, &d) in ship.iter().enumerate() {
        if d >= lo && d < hi {
            bm[i >> 3] |= 1 << (i & 7);
        }
    }
    let ext = masked_decode_f64(file, rg, EXT, &bm).expect("ext");
    let disc = masked_decode_f64(file, rg, DISC, &bm).expect("disc");
    let mut s = 0.0f64;
    for i in 0..ext.len() {
        s += ext[i] * (1.0 - disc[i]);
    }
    s
}

fn l0_raw(file: &ParquetFile, n_rg: usize, nt: usize, lo: i32, hi: i32) -> f64 {
    let partials: Vec<AtomicU64> = (0..n_rg).map(|_| AtomicU64::new(0)).collect();
    thread::scope(|s| {
        for t in 0..nt {
            let partials = &partials;
            s.spawn(move || {
                let mut rg = t;
                while rg < n_rg {
                    let p = decode_rg_partial(file, rg, lo, hi);
                    partials[rg].store(p.to_bits(), Ordering::Relaxed);
                    rg += nt;
                }
            });
        }
    });
    partials
        .iter()
        .map(|a| f64::from_bits(a.load(Ordering::Relaxed)))
        .sum()
}

async fn drain_scan(
    scan: &Arc<dyn ExecutionPlan>,
    tctx: Arc<datafusion::execution::TaskContext>,
) -> usize {
    let nparts = scan.output_partitioning().partition_count();
    let mut hs = Vec::new();
    for p in 0..nparts {
        let s = scan.clone();
        let tc = tctx.clone();
        hs.push(tokio::spawn(async move {
            let mut st = s.execute(p, tc).unwrap();
            let mut rows = 0usize;
            while let Some(b) = st.next().await {
                rows += b.unwrap().num_rows();
            }
            rows
        }));
    }
    let mut total = 0;
    for h in hs {
        total += h.await.unwrap();
    }
    total
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("examples/tpch/data/sf10"));
    let nt = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(14);
    let reps: usize = std::env::var("REPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(15);
    let lo = 9496i32;
    let hi = 9587i32;

    let ctx = build_ctx(&data_dir, nt)?;
    let plan = ctx.sql(SCALAR_SQL).await?.create_physical_plan().await?;
    let scan = find_scan(&plan).ok_or("no EmatixFastParquetExec in plan")?;
    let nparts = scan.output_partitioning().partition_count();

    println!("PV.M.6 orchestration probe — target_partitions={nt}\n");
    println!(
        "=== SCALAR physical plan ===\n{}",
        displayable(plan.as_ref()).indent(true)
    );
    println!(
        "=== scan node ===\n{}",
        displayable(scan.as_ref()).indent(true)
    );
    println!(
        "scan emits {nparts} partitions; output schema = {:?}",
        scan.schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>()
    );

    // Correctness: full SCALAR sum (masked-fusion vs dense must agree).
    let exact = std::env::var("EMAT_EXACT_PUSHDOWN").ok().as_deref() == Some("1");
    let batches = collect(plan.clone(), ctx.task_ctx()).await?;
    let q15_sum = batches
        .iter()
        .filter_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .map(|c| c.value(0))
        })
        .next()
        .unwrap_or(f64::NAN);
    println!(
        "Q15 SCALAR sum (EXACT_PUSHDOWN={}) = {q15_sum:.4}  [canonical 82287832911.4087]\n",
        exact as u8
    );

    let lineitem = data_dir.join("lineitem.parquet");
    let file = open_cached(&lineitem)?;
    let n_rg = file
        .cached_metadata()
        .map_err(|e| format!("{e}"))?
        .row_groups
        .len();
    println!("lineitem row_groups={n_rg}\n");

    // Warmup both paths.
    let _ = l0_raw(&file, n_rg, nt, lo, hi);
    let tctx = ctx.task_ctx();
    let _ = drain_scan(&scan, tctx.clone()).await;
    let _ = drain_scan(&scan, tctx.clone()).await;

    // Interleaved ladder: L0 raw-decode-to-scalar vs L2 production scan drain.
    let (mut l0, mut l2) = (Vec::new(), Vec::new());
    let mut rows = 0;
    for _ in 0..reps {
        let t = Instant::now();
        let _ = l0_raw(&file, n_rg, nt, lo, hi);
        l0.push(t.elapsed().as_secs_f64() * 1e3);

        let t = Instant::now();
        rows = drain_scan(&scan, tctx.clone()).await;
        l2.push(t.elapsed().as_secs_f64() * 1e3);
    }
    let (m0, m2) = (median(l0), median(l2));
    println!("=== ladder (median {reps} reps, nthreads={nt}) ===");
    println!("L0 raw decode → scalar (no Arrow, no DF) : {m0:6.1} ms");
    println!("L2 production scan drain (Arrow + DF)    : {m2:6.1} ms   (survivor rows={rows})");
    println!(
        "ORCHESTRATION + ARROW ASSEMBLY = L2 − L0 = {:.1} ms  ({:.0}% of L2)",
        m2 - m0,
        100.0 * (m2 - m0) / m2
    );

    if std::env::var("PROFILE").ok().as_deref() == Some("1") {
        let trials: usize = std::env::var("PROFILE_TRIALS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1500);
        println!(
            "\nPROFILE loop — PID {} — sample now ({trials} scan drains)",
            std::process::id()
        );
        for _ in 0..trials {
            let _ = drain_scan(&scan, tctx.clone()).await;
        }
    }
    Ok(())
}
