//! P1 GO/NO-GO — the clean-room native engine's whole-engine gate.
//!
//! Reproduces the PV.1 −16% Q08 result, but routes the push arm ENTIRELY
//! through the **`ematix-flow-engine`** crate: its native ematix-parquet
//! scan → `exec::run_scan_pipeline` (RG-parallel) → `ProbeNarrowOp`
//! (deferred selection, no per-join `take`) → an engine `Sink` — versus
//! the **current production DF pull pipeline**
//! (`ctx.sql(q08)` with the full preset rule chain), same process,
//! interleaved. Both arms decode identically, so the delta is purely
//! push-execution vs pull-execution.
//!
//!   ENGINE ≪ PULL  → GO: the execution model survives in the real engine.
//!   ENGINE ≈ PULL  → NO-GO: stop and reassess (the −16% didn't transfer).
//!
//! Usage:
//!   TPCH_DATA_DIR=examples/tpch/data/sf10 TRIALS=7 \
//!     cargo run --release -p ematix-flow-core --example p1_engine_q08_gonogo --features triangulation
//!
//! Kill-gate: ENGINE ≥ 12% faster than PULL (target −16%).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Float64Array, Int32Array, Int64Array};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::fast_parquet::FastParquetTableProvider;

use ematix_flow_engine::agg::{AggBinding, HashAggregateSink};
use ematix_flow_engine::chunk::DataChunk;
use ematix_flow_engine::exec::{ProbeNarrowOp, PushOp, run_scan_pipeline};
use ematix_flow_engine::join::{ProbeStructure, choose};
use ematix_flow_engine::scan_native::NativeColKind;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

// lineitem column indices (TPC-H schema order).
const C_ORDERKEY: usize = 0;
const C_PARTKEY: usize = 1;
const C_SUPPKEY: usize = 2;
const C_EXTPRICE: usize = 5;
const C_DISCOUNT: usize = 6;

/// The three Q08 dimension reductions as engine/push probe structures.
struct Probes {
    part: Arc<ProbeStructure>,     // p_partkey survivors (membership → DenseSet)
    orders: Arc<ProbeStructure>,   // o_orderkey -> year-bucket (0=1995,1=1996)
    supplier: Arc<ProbeStructure>, // s_suppkey -> is_brazil (1/0)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("TPCH_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("examples/tpch/data/sf10"));
    let trials: usize = std::env::var("TRIALS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(7);
    let nthreads: usize = std::env::var("EMAT_PUSH_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(14);

    // Production pull context (real Q08 plan, full preset rule chain).
    let builder = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(14))
        .with_default_features();
    let builder = ematix_flow_core::preset::with_optimizer_rules(builder);
    let ctx = SessionContext::new_with_state(builder.build());
    register_all(&ctx, &data_dir)?;

    // Plain context for the dim-build SQL (helper queries need no rules).
    let ctx_plain =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(14));
    register_all(&ctx_plain, &data_dir)?;
    let q08 = std::fs::read_to_string(data_dir.join("../../queries/q08.sql"))
        .or_else(|_| std::fs::read_to_string("examples/tpch/queries/q08.sql"))?;

    let lineitem_path = data_dir.join("lineitem.parquet");
    eprintln!(
        "P1 GO/NO-GO — engine Q08 vs DF pull. data={}, trials={trials}, threads={nthreads}",
        data_dir.display()
    );

    // ---- warmup + correctness (engine shares must equal the DF pull result) ----
    let probes = build_probes(&ctx_plain, &data_dir).await?;
    eprintln!(
        "probes: part={} orders={} supplier={}",
        probes.part.kind(),
        probes.orders.kind(),
        probes.supplier.kind()
    );
    let engine_share = engine_push_q08(&lineitem_path, &probes, nthreads)?;
    let pull_share = pull_q08_shares(&ctx, &q08).await?;
    eprintln!(
        "ENGINE mkt_share: 1995={:.6} 1996={:.6}",
        engine_share[0], engine_share[1]
    );
    eprintln!(
        "PULL   mkt_share: 1995={:.6} 1996={:.6}",
        pull_share[0], pull_share[1]
    );
    for y in 0..2 {
        assert!(
            (engine_share[y] - pull_share[y]).abs() < 1e-6,
            "CORRECTNESS FAIL year-bucket {y}: engine {} != pull {}",
            engine_share[y],
            pull_share[y]
        );
    }
    eprintln!("correctness: engine == DF pull ✓");

    // ---- interleaved timed trials ----
    let mut pull = Vec::new();
    let mut engine = Vec::new();
    for _ in 0..trials {
        let t = Instant::now();
        let _ = ctx.sql(&q08).await?.collect().await?;
        pull.push(t.elapsed().as_secs_f64() * 1000.0);

        // dims rebuilt each trial (fair: the pull plan rebuilds them too).
        let p = build_probes(&ctx_plain, &data_dir).await?;
        let t = Instant::now();
        let _ = engine_push_q08(&lineitem_path, &p, nthreads)?;
        engine.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    let med = |v: &mut Vec<f64>| {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[v.len() / 2]
    };
    let pull_m = med(&mut pull);
    let eng_m = med(&mut engine);

    println!("\n=== P1 Q08 SF=10 GO/NO-GO ({trials} trials, {nthreads} threads) ===");
    println!("PULL   production DF Q08:      {pull_m:7.1} ms");
    println!(
        "ENGINE clean-room push:        {eng_m:7.1} ms   ({:+.1} ms, {:+.1}%)",
        pull_m - eng_m,
        (eng_m / pull_m - 1.0) * 100.0
    );
    let gate = (1.0 - eng_m / pull_m) * 100.0;
    println!("\nKILL-GATE: engine is {gate:.1}% faster than the DF pull path.");
    if gate >= 12.0 {
        println!("  GO (≥12%) — the −16% no-materialization win survives in the clean-room engine.");
    } else {
        println!("  NO-GO (<12%) — the win did not transfer; stop and reassess before committing.");
    }
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

/// Build the three Q08 dimension reductions, then hand them to the adaptive
/// `choose` selector. The part reduction is built **natively** (engine
/// string decode + filter, no DataFusion); supplier and orders still use SQL
/// (they need joins + a date window — the follow-on native builds).
async fn build_probes(
    ctx: &SessionContext,
    data_dir: &std::path::Path,
) -> Result<Probes, Box<dyn std::error::Error>> {
    // part survivors (p_type filter) — NATIVE: the engine's own string
    // decode + equality filter over part.parquet, zero DataFusion.
    let part_keys = ematix_flow_engine::dim::collect_i64_keys_where_str_eq(
        &data_dir.join("part.parquet"),
        "p_partkey",
        "p_type",
        "ECONOMY ANODIZED STEEL",
    )?;
    // supplier -> is_brazil — NATIVE: supplier ⋈ nation on the engine's
    // adaptive hash join (n_name = 'BRAZIL' flag as payload), zero DataFusion.
    let (sup_keys, sup_pay) = ematix_flow_engine::dim::supplier_nation_flag(
        &data_dir.join("supplier.parquet"),
        &data_dir.join("nation.parquet"),
        "BRAZIL",
    )?;
    // orders -> year-bucket (AMERICA region + date window).
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

    Ok(Probes {
        part: Arc::new(choose(&part_keys, None)),
        orders: Arc::new(choose(&ord_keys, Some(&ord_pay))),
        supplier: Arc::new(choose(&sup_keys, Some(&sup_pay))),
    })
}

/// Q08 hot path through the clean-room engine, end to end: the native scan
/// feeds row-group morsels to `run_scan_pipeline`, which narrows on the
/// part semijoin (`ProbeNarrowOp`, no materialization) and aggregates the
/// market share in the engine's **general** `HashAggregateSink` — Q08
/// supplies only the `Q08Agg` binding, no bespoke aggregation sink. No core
/// `masked_decode`, no DataFusion; per-worker sinks merged here.
fn engine_push_q08(
    lineitem: &std::path::Path,
    probes: &Probes,
    nthreads: usize,
) -> Result<[f64; 2], Box<dyn std::error::Error>> {
    // Chunk cols: 0=partkey 1=orderkey 2=suppkey 3=extprice 4=discount.
    let columns = [
        (C_PARTKEY, NativeColKind::I64),
        (C_ORDERKEY, NativeColKind::I64),
        (C_SUPPKEY, NativeColKind::I64),
        (C_EXTPRICE, NativeColKind::F64),
        (C_DISCOUNT, NativeColKind::F64),
    ];
    // The 60M-row hot op: part semijoin narrow (no take); agg at the sink.
    let ops: Vec<Box<dyn PushOp>> = vec![Box::new(ProbeNarrowOp {
        key_col: 0,
        probe: Arc::clone(&probes.part),
    })];
    let binding = Arc::new(Q08Agg {
        orders: Arc::clone(&probes.orders),
        supplier: Arc::clone(&probes.supplier),
    });
    let make_sink = || HashAggregateSink::<2, Q08Agg>::new(Arc::clone(&binding));

    let sinks = run_scan_pipeline(lineitem, &columns, &ops, make_sink, nthreads)?;

    // Merge per-worker group→[total, brazil], then form the shares. The
    // group key is the year bucket (0=1995, 1=1996).
    let merged = HashAggregateSink::merge(sinks);
    let share = |bucket: i64| {
        merged
            .get(&bucket)
            .map_or(0.0, |m| if m[0] > 0.0 { m[1] / m[0] } else { 0.0 })
    };
    Ok([share(0), share(1)])
}

/// Q08's binding to the engine's general aggregate breaker: derive the
/// year-bucket group key from the orders inner-join payload, and the two
/// market-share measures — total volume `extprice*(1-discount)` and the
/// BRAZIL share of it (flagged by the supplier payload). This is all that
/// stays query-specific; the hash accumulation + parallel merge live in the
/// engine's `HashAggregateSink`. A `None` orders payload drops the row
/// (inner join), so it is simply not emitted.
struct Q08Agg {
    orders: Arc<ProbeStructure>,
    supplier: Arc<ProbeStructure>,
}

impl AggBinding<2> for Q08Agg {
    fn for_each_group(&self, chunk: &DataChunk, mut emit: impl FnMut(i64, [f64; 2])) {
        // cols: 1=orderkey 2=suppkey 3=extprice 4=discount.
        let ok = chunk.col(1).as_i64();
        let sk = chunk.col(2).as_i64();
        let ep = chunk.col(3).as_f64();
        let disc = chunk.col(4).as_f64();
        chunk.sel.for_each(|i| {
            let i = i as usize;
            if let Some(bucket) = self.orders.payload(ok[i]) {
                let vol = ep[i] * (1.0 - disc[i]);
                let brazil = if self.supplier.payload(sk[i]) == Some(1) {
                    vol
                } else {
                    0.0
                };
                emit(bucket, [vol, brazil]);
            }
        });
    }
}

/// Run the real Q08 pull plan and extract `[share_1995, share_1996]`.
async fn pull_q08_shares(
    ctx: &SessionContext,
    q08: &str,
) -> Result<[f64; 2], Box<dyn std::error::Error>> {
    let batches = ctx.sql(q08).await?.collect().await?;
    let mut share = [0.0f64; 2];
    for b in &batches {
        // q08 output: (o_year, mkt_share). o_year is Int32 or Int64.
        let yr_i32 = b.column(0).as_any().downcast_ref::<Int32Array>();
        let yr_i64 = b.column(0).as_any().downcast_ref::<Int64Array>();
        let ms = b
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("mkt_share is f64");
        for i in 0..b.num_rows() {
            let y = match (yr_i32, yr_i64) {
                (Some(a), _) => a.value(i) as i64,
                (_, Some(a)) => a.value(i),
                _ => continue,
            };
            if y == 1995 {
                share[0] = ms.value(i);
            } else if y == 1996 {
                share[1] = ms.value(i);
            }
        }
    }
    Ok(share)
}
