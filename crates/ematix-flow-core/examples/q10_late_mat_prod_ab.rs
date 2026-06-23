//! prod-E perf gate: does the PRODUCTION planner path realize the Q10 SF=100
//! wide-string late-materialization win the spike (`q10_lategather_e2e`) proved?
//!
//! Unlike the spike (which hand-grafts the subtree onto the physical plan), this
//! drives the SHIPPED path end to end: tables registered with declared PKs +
//! `FlowQueryPlanner`, and the rewrite fires purely from `EMAT_LATE_MAT_AGG=1`
//! (recognizer → `LateMatAggNode` → `LateMatAggPlanner`, build re-planned at the
//! prod-D 1M batch). The probe keeps the session batch size (8192) — the spike
//! used a GLOBAL 1M batch, so this is the load-bearing check that the win
//! survives a normally-batched probe.
//!
//!   ARM=stock|late   run one arm's trials CONSECUTIVELY (isolated — the
//!                    interleaved A/B thrashes the 36GB box page cache). Default
//!                    ARM=ab runs the correctness check + interleaved timing.
//!   TPCH_DATA_DIR (default examples/tpch/data/sf100), TRIALS (5), WARMUPS (2).
//!   EMAT_HJ_OVERLAP=1 to match the spike's build/probe-overlap win config.

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Array, Float64Array};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::flow_query_planner::FlowQueryPlanner;
use ematix_flow_core::preset;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const Q10: &str = "select c_custkey, c_name, sum(l_extendedprice*(1-l_discount)) as revenue, \
    c_acctbal, n_name, c_address, c_phone, c_comment \
    from customer, orders, lineitem, nation \
    where c_custkey=o_custkey and l_orderkey=o_orderkey \
    and o_orderdate>=date '1993-10-01' and o_orderdate<date '1994-01-01' \
    and l_returnflag='R' and c_nationkey=n_nationkey \
    group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment \
    order by revenue desc";

fn cpu_secs() -> f64 {
    let mut u: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut u) } != 0 {
        return f64::NAN;
    }
    let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    tv(u.ru_utime) + tv(u.ru_stime)
}
fn median(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn build_ctx(data_dir: &Path) -> Result<SessionContext, Box<dyn std::error::Error>> {
    // EMAT_EXEC_BATCH: set the query-GLOBAL execution batch size (the spike's
    // winning lever — batch size is a runtime TaskContext parameter, so this is
    // the only way to make the build emit few large batches; it affects the
    // probe too, which is why prod-D flagged a global bump as a 22q regression).
    let mut cfg = SessionConfig::new();
    if let Ok(n) = std::env::var("EMAT_EXEC_BATCH").map(|s| s.parse::<usize>()) {
        if let Ok(n) = n {
            cfg = cfg.with_batch_size(n);
        }
    }
    let state = preset::with_optimizer_rules(
        SessionStateBuilder::new()
            .with_config(cfg)
            .with_default_features(),
    )
    .with_query_planner(Arc::new(FlowQueryPlanner))
    .build();
    let ctx = SessionContext::new_with_state(state);
    // Declared PKs (a real catalog's DDL) — the recognizer's soundness witness.
    let reg = |t: &str, pk: Option<Vec<usize>>| -> Result<(), Box<dyn std::error::Error>> {
        let p = data_dir.join(format!("{t}.parquet"));
        let mut prov = EmatixFastParquetTableProvider::try_new(p.to_string_lossy().into_owned())?;
        if let Some(k) = pk {
            prov = prov.with_primary_key(k);
        }
        ctx.register_table(t, Arc::new(prov))?;
        Ok(())
    };
    reg("customer", Some(vec![0]))?;
    reg("orders", Some(vec![0]))?;
    reg("lineitem", None)?;
    reg("nation", Some(vec![0]))?;
    Ok(ctx)
}

fn checksum(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> (usize, f64) {
    let mut rows = 0usize;
    let mut sum = 0.0f64;
    for b in batches {
        rows += b.num_rows();
        if let Ok(idx) = b.schema().index_of("revenue") {
            if let Some(a) = b.column(idx).as_any().downcast_ref::<Float64Array>() {
                for i in 0..a.len() {
                    if a.is_valid(i) {
                        sum += a.value(i);
                    }
                }
            }
        }
    }
    (rows, sum)
}

fn set_late(on: bool) {
    // SAFETY: single-threaded setup between collects; no other reader races.
    unsafe {
        if on {
            std::env::set_var("EMAT_LATE_MAT_AGG", "1");
        } else {
            std::env::remove_var("EMAT_LATE_MAT_AGG");
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .unwrap_or_else(|_| "examples/tpch/data/sf100".to_string());
    let data_dir = Path::new(&data_dir);
    let trials: usize = std::env::var("TRIALS").ok().and_then(|s| s.parse().ok()).unwrap_or(5);
    let warmups: usize = std::env::var("WARMUPS").ok().and_then(|s| s.parse().ok()).unwrap_or(2);
    // Plan cache off so the flag toggle re-plans each arm.
    unsafe { std::env::set_var("EMAT_PLAN_CACHE", "0") };
    let ctx = build_ctx(data_dir)?;

    println!(
        "Q10 late-mat PRODUCTION A/B  data={}  warmups={warmups} trials={trials}  overlap={}",
        data_dir.display(),
        std::env::var("EMAT_HJ_OVERLAP").unwrap_or_else(|_| "0".into())
    );

    let run = |on: bool| {
        let ctx = ctx.clone();
        async move {
            set_late(on);
            let p = ctx.sql(Q10).await?.create_physical_plan().await?;
            set_late(false);
            Ok::<_, datafusion::error::DataFusionError>(p)
        }
    };

    // Confirm the late arm actually wires the subtree.
    {
        set_late(true);
        let p = ctx.sql(Q10).await?.create_physical_plan().await?;
        set_late(false);
        let dump = format!("{}", datafusion::physical_plan::displayable(p.as_ref()).indent(true));
        let wired = dump.contains("LateGatherExec") && dump.contains("EmatixHashJoinExec");
        println!("late arm wires late-mat subtree: {wired}");
        if std::env::var_os("DUMP_PLAN").is_some() {
            println!("\n===== LATE-MAT PRODUCTION PLAN =====\n{dump}");
            let sp = ctx.sql(Q10).await?.create_physical_plan().await?;
            println!(
                "===== STOCK PLAN =====\n{}",
                datafusion::physical_plan::displayable(sp.as_ref()).indent(true)
            );
            return Ok(());
        }
        if !wired {
            println!("ABORT: production planner did not wire the late-mat subtree.");
            return Ok(());
        }
    }

    // EMAT_EXPLAIN_ANALYZE: collect the late plan warm, then dump per-node metrics
    // bottom-up to localize where the late-mat CPU goes vs the spike's 21 CPU-s.
    if std::env::var_os("EMAT_EXPLAIN_ANALYZE").is_some() {
        for _ in 0..2 {
            let _ = collect(run(true).await?, ctx.task_ctx()).await?;
        }
        let p = run(true).await?;
        let _ = collect(p.clone(), ctx.task_ctx()).await?;
        fn dump(node: &Arc<dyn datafusion::physical_plan::ExecutionPlan>, depth: usize) {
            let m = node
                .metrics()
                .map(|s| s.aggregate_by_name().to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "metrics=[]".into());
            println!("{}{}  {m}", "  ".repeat(depth), node.name());
            for c in node.children() {
                dump(c, depth + 1);
            }
        }
        println!("\n== late-mat per-node metrics (warm) ==");
        dump(&p, 0);
        return Ok(());
    }

    let arm = std::env::var("ARM").unwrap_or_else(|_| "ab".into());

    // Correctness (ab mode only — isolated arms skip it to avoid polluting the
    // measured arm's page cache with the other arm's 15M build retention).
    if arm == "ab" {
        let stock = collect(run(false).await?, ctx.task_ctx()).await?;
        let late = collect(run(true).await?, ctx.task_ctx()).await?;
        let (sr, ss) = checksum(&stock);
        let (lr, ls) = checksum(&late);
        let ok = sr == lr && (ss - ls).abs() < ss.abs() * 1e-9 + 1e-6;
        println!(
            "correctness: stock=({sr} rows, {ss:.2})  late=({lr} rows, {ls:.2}) => {}",
            if ok { "MATCH ✓" } else { "MISMATCH ✗" }
        );
        if !ok {
            println!("ABORT: production late-mat differs from stock.");
            return Ok(());
        }
    }

    if arm == "stock" || arm == "late" {
        let on = arm == "late";
        for _ in 0..warmups {
            let _ = collect(run(on).await?, ctx.task_ctx()).await?;
        }
        let (mut w, mut c) = (vec![], vec![]);
        for _ in 0..trials {
            let p = run(on).await?;
            let c0 = cpu_secs();
            let t = Instant::now();
            let _ = collect(p, ctx.task_ctx()).await?;
            w.push(t.elapsed().as_secs_f64() * 1000.0);
            c.push(cpu_secs() - c0);
        }
        let (wm, cm) = (median(&mut w), median(&mut c));
        println!(
            "\nARM={arm} (isolated)  wall={wm:.1}ms  cpu={cm:.2}s  eff={:.1}",
            cm / (wm / 1000.0)
        );
        println!("DuckDB SF=100 floor: ~1950-2250ms / ~20.5 CPU-s.");
        return Ok(());
    }

    // Interleaved A/B (small box: prefer the isolated ARM=stock|late invocations).
    for _ in 0..warmups {
        let _ = collect(run(false).await?, ctx.task_ctx()).await?;
        let _ = collect(run(true).await?, ctx.task_ctx()).await?;
    }
    let (mut ws, mut cs, mut wl, mut cl) = (vec![], vec![], vec![], vec![]);
    for _ in 0..trials {
        let p = run(false).await?;
        let c0 = cpu_secs();
        let t = Instant::now();
        let _ = collect(p, ctx.task_ctx()).await?;
        ws.push(t.elapsed().as_secs_f64() * 1000.0);
        cs.push(cpu_secs() - c0);

        let p = run(true).await?;
        let c0 = cpu_secs();
        let t = Instant::now();
        let _ = collect(p, ctx.task_ctx()).await?;
        wl.push(t.elapsed().as_secs_f64() * 1000.0);
        cl.push(cpu_secs() - c0);
    }
    let (wsm, csm, wlm, clm) = (median(&mut ws), median(&mut cs), median(&mut wl), median(&mut cl));
    println!("\n{:<10} {:>9} {:>8} {:>6}", "arm", "wall_ms", "cpu_s", "eff");
    println!("{:<10} {wsm:>9.1} {csm:>8.2} {:>6.1}", "stock", csm / (wsm / 1000.0));
    println!("{:<10} {wlm:>9.1} {clm:>8.2} {:>6.1}", "late-mat", clm / (wlm / 1000.0));
    println!(
        "\nQ10 prod: stock {wsm:.0}ms vs late-mat {wlm:.0}ms => {:.3}x wall ({:.3}x CPU)",
        wsm / wlm,
        csm / clm
    );
    Ok(())
}
