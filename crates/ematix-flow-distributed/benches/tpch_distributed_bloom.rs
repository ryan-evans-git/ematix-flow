//! Σ.J.2.b — distributed TPC-H bench with the bloom-propagation
//! pipeline ON. Pair to `tpch_distributed.rs` (bloom-OFF baseline).
//!
//! ## What this measures
//!
//! Same 4 queries (Q1/Q3/Q6/Q19) under two configurations:
//!
//! - **3 workers + bloom OFF**: control. Should match the bloom-OFF
//!   bench within noise. Confirms wiring overhead is bounded.
//! - **3 workers + bloom ON**: live pipeline. Each iteration:
//!     1. parse SQL → optimize LogicalPlan
//!     2. `attach_blooms_for_plan(&mut ctx, &plan, opts)` — auto-detect
//!        Inner-equijoin build sides, pre-execute, attach to
//!        passthrough headers
//!     3. `ctx.sql(query).collect()` — passthrough headers ride every
//!        Flight call; probe-side workers' `BloomSessionBuilder`
//!        installs `EnableContextBloomRule`; matching parquet scans
//!        get wrapped in `BloomFilterExec`
//!
//! The probe-side wrapper drops rows whose join key isn't in the
//! bloom *before* the next shuffle write — that's where the wall-time
//! win comes from on selective joins (Q3 has a customer ⋈ orders ⋈
//! lineitem chain; small filtered customer means a 99%+ filter on
//! lineitem before shuffle).
//!
//! ## Honest framing
//!
//! - In-process workers + loopback network — measures CPU + shuffle-
//!   byte savings, NOT cross-host wire savings.
//! - SF=1 build sides are tiny (customer = 150K, nation = 25,
//!   supplier = 10K) — every Inner-Int64 equijoin under the default
//!   50K-row cap should bloom. Q19 has zero base-table joins (it's
//!   lineitem ⋈ part on l_partkey = p_partkey, with part filtered to
//!   ~700 rows) — Q19 should benefit most.
//! - The auto-detect emitter pre-executes the build side on the
//!   coordinator, so we ALSO measure the emission cost. If emission
//!   cost > saved shuffle work, bloom-ON loses.
//!
//! ## Env knobs (same as the OFF baseline)
//!
//! - `TPCH_DATA_DIR` — Parquet directory (default
//!   `<workspace>/examples/tpch/data/sf1`)
//! - `TPCH_MEASUREMENT_TIME_S` — criterion measurement window per
//!   query (default 20s)
//!
//! ## Run
//!
//! ```sh
//! cargo bench -p ematix-flow-distributed --bench tpch_distributed_bloom
//! ```
//!
//! Output appears under
//! `target/criterion/tpch_<sf>_3_workers_bloom_{off,on}/`.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::arrow::array::RecordBatch;
use datafusion::prelude::SessionContext;
use datafusion_distributed::{DefaultSessionBuilder, Worker};
use ematix_flow_core::backend::DistributedConfig;
use ematix_flow_distributed::bloom_emitter::{attach_blooms_for_plan, BloomEmitterOptions};
use ematix_flow_distributed::bloom_flight::default_bloom_session_builder;
use ematix_flow_distributed::DistributedBackend;
use futures_util::TryStreamExt;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::task::JoinSet;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

const Q1: &str = include_str!("../../../examples/tpch/queries/q01.sql");
const Q3: &str = include_str!("../../../examples/tpch/queries/q03.sql");
const Q6: &str = include_str!("../../../examples/tpch/queries/q06.sql");
const Q19: &str = include_str!("../../../examples/tpch/queries/q19.sql");

const TPCH_TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

fn data_dir() -> PathBuf {
    if let Ok(env) = std::env::var("TPCH_DATA_DIR") {
        return PathBuf::from(env);
    }
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest
        .parent()
        .and_then(|p| p.parent())
        .map(|root| root.join("examples/tpch/data/sf1"))
        .unwrap_or_else(|| PathBuf::from("examples/tpch/data/sf1"))
}

fn sf_tag(dir: &Path) -> String {
    let basename = dir.file_name().and_then(|s| s.to_str()).unwrap_or("custom");
    if basename.starts_with("sf")
        && basename.len() > 2
        && basename[2..].chars().all(|c| c.is_ascii_digit())
    {
        basename.to_string()
    } else {
        "custom".to_string()
    }
}

async fn register_tpch(ctx: &SessionContext, dir: &Path) -> bool {
    for table in TPCH_TABLES {
        let path = dir.join(format!("{table}.parquet"));
        if !path.exists() {
            eprintln!(
                "tpch_distributed_bloom: missing {} — skip bench (data not generated)",
                path.display()
            );
            return false;
        }
        ctx.register_parquet(*table, path.to_str().unwrap(), Default::default())
            .await
            .unwrap_or_else(|e| panic!("register {table}: {e}"));
    }
    true
}

/// Spawn N in-process workers that use the bloom session builder so
/// they auto-install the probe-side rule from inbound headers. This
/// is the worker side of the closed loop.
async fn spawn_bloom_workers(n: usize) -> (Vec<String>, JoinSet<()>) {
    let mut urls = Vec::with_capacity(n);
    let mut set: JoinSet<()> = JoinSet::new();
    for _ in 0..n {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr: SocketAddr = listener.local_addr().expect("local_addr");
        urls.push(format!("http://{addr}"));
        let incoming = TcpListenerStream::new(listener);
        set.spawn(async move {
            let worker = Worker::from_session_builder(default_bloom_session_builder());
            let _ = Server::builder()
                .add_service(worker.into_worker_server())
                .serve_with_incoming(incoming)
                .await;
        });
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    (urls, set)
}

/// Plain workers (control config — bloom rule not installed).
async fn spawn_plain_workers(n: usize) -> (Vec<String>, JoinSet<()>) {
    let mut urls = Vec::with_capacity(n);
    let mut set: JoinSet<()> = JoinSet::new();
    for _ in 0..n {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr: SocketAddr = listener.local_addr().expect("local_addr");
        urls.push(format!("http://{addr}"));
        let incoming = TcpListenerStream::new(listener);
        set.spawn(async move {
            let worker = Worker::from_session_builder(DefaultSessionBuilder);
            let _ = Server::builder()
                .add_service(worker.into_worker_server())
                .serve_with_incoming(incoming)
                .await;
        });
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    (urls, set)
}

async fn build_backend(rt: &Runtime, peers: Vec<String>) -> Option<Arc<DistributedBackend>> {
    let _enter = rt.enter();
    let backend = Arc::new(
        DistributedBackend::open(DistributedConfig { peers, tls: None }).expect("backend"),
    );
    if !register_tpch(backend.session_context().await, &data_dir()).await {
        return None;
    }
    Some(backend)
}

fn run_query_off(rt: &Runtime, backend: &DistributedBackend, sql: &str) -> Vec<RecordBatch> {
    rt.block_on(async {
        let ctx = backend.session_context().await.clone();
        let df = ctx.sql(sql).await.expect("plan");
        df.execute_stream()
            .await
            .expect("execute_stream")
            .try_collect::<Vec<_>>()
            .await
            .expect("collect")
    })
}

fn run_query_on(rt: &Runtime, backend: &DistributedBackend, sql: &str) -> Vec<RecordBatch> {
    rt.block_on(async {
        // SessionContext::clone shares state; mutations via
        // set_distributed_passthrough_headers propagate to the
        // shared SessionState. We take a fresh clone per iteration
        // so left-over headers from a prior query don't leak in.
        let mut ctx: SessionContext = backend.session_context().await.as_ref().clone();
        let df = ctx.sql(sql).await.expect("plan");
        let plan = df
            .into_optimized_plan()
            .expect("optimize");
        let _attached = attach_blooms_for_plan(&mut ctx, &plan, &BloomEmitterOptions::default())
            .await
            .expect("attach_blooms");
        // Re-issue the SQL through the now-bloom-equipped ctx so the
        // passthrough headers ride the Flight calls.
        let df2 = ctx.sql(sql).await.expect("plan2");
        df2.execute_stream()
            .await
            .expect("execute_stream")
            .try_collect::<Vec<_>>()
            .await
            .expect("collect")
    })
}

fn measurement_time() -> Duration {
    if let Ok(env) = std::env::var("TPCH_MEASUREMENT_TIME_S")
        && let Ok(s) = env.parse::<u64>()
    {
        return Duration::from_secs(s);
    }
    Duration::from_secs(20)
}

fn bench_bloom(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let dir = data_dir();
    let sf = sf_tag(&dir);
    println!("==> TPC-H distributed bloom bench data dir: {}", dir.display());
    println!("==> SF tag (group label): {sf}");
    println!("==> Both configs use 3 in-process workers (loopback)");

    let queries: &[(&str, &str)] = &[("q01", Q1), ("q03", Q3), ("q06", Q6), ("q19", Q19)];

    // --- Configuration A: 3 plain workers, bloom path inactive ---
    {
        let urls = rt.block_on(async {
            let (urls, set) = spawn_plain_workers(3).await;
            std::mem::forget(set);
            urls
        });
        let Some(backend) = rt.block_on(build_backend(&rt, urls)) else {
            return;
        };
        let group_name = format!("tpch_{sf}_3_workers_bloom_off");
        for (name, sql) in queries {
            let mut group = c.benchmark_group(&group_name);
            group.sample_size(10).measurement_time(measurement_time());
            group.bench_function(*name, |b| {
                b.iter(|| {
                    let r = run_query_off(&rt, &backend, sql);
                    std::hint::black_box(r);
                });
            });
            group.finish();
        }
    }

    // --- Configuration B: 3 bloom-aware workers + attach blooms per query ---
    {
        let urls = rt.block_on(async {
            let (urls, set) = spawn_bloom_workers(3).await;
            std::mem::forget(set);
            urls
        });
        let Some(backend) = rt.block_on(build_backend(&rt, urls)) else {
            return;
        };
        let group_name = format!("tpch_{sf}_3_workers_bloom_on");
        for (name, sql) in queries {
            let mut group = c.benchmark_group(&group_name);
            group.sample_size(10).measurement_time(measurement_time());
            group.bench_function(*name, |b| {
                b.iter(|| {
                    let r = run_query_on(&rt, &backend, sql);
                    std::hint::black_box(r);
                });
            });
            group.finish();
        }
    }
}

criterion_group!(benches, bench_bloom);
criterion_main!(benches);
