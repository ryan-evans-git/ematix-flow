//! Σ.C PR 1: TPC-H bench harness for the distributed-execution path.
//!
//! Mirrors `crates/ematix-flow-core/benches/tpch.rs` (which benches
//! the single-node DataFusion path) but routes queries through a
//! `DistributedBackend`. Runs each of Q1 / Q3 / Q6 / Q19 under two
//! configurations:
//!
//!   - **distributed-of-one** (`peers = []`): degenerate single-
//!     worker cluster. Vanilla DataFusion under the hood; this
//!     measures the trait-surface overhead of going through
//!     `DistributedBackend` instead of a raw `SessionContext`.
//!     Should match the single-node bench within noise.
//!   - **3 in-process workers**: spawns 3 `flow-worker`-equivalent
//!     tonic servers on free localhost ports, points the
//!     coordinator at them, exercises the
//!     `DistributedPhysicalOptimizerRule`-wrapped plan + cross-
//!     "pod" Arrow Flight shuffle. At SF=1 the network-RPC overhead
//!     typically exceeds the work; this surfaces the floor cost.
//!
//! Environment knobs:
//!   - `TPCH_DATA_DIR` — Parquet directory (default
//!     `<workspace>/examples/tpch/data/sf1`)
//!   - `TPCH_MEASUREMENT_TIME_S` — criterion measurement window per
//!     query (default 20s)
//!
//! Generate data first:
//!     cargo run --release -p ematix-flow-core --example tpch_generate -- \
//!         --sf 1 --out examples/tpch/data/sf1
//!
//! Then run:
//!     cargo bench -p ematix-flow-distributed --bench tpch_distributed
//!
//! Plan: `docs/PHASE_SIGMA_PLAN.md` Σ.C.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::arrow::array::RecordBatch;
use datafusion::prelude::SessionContext;
use datafusion_distributed::{DefaultSessionBuilder, Worker};
use ematix_flow_core::backend::{Backend, DistributedConfig};
use ematix_flow_distributed::DistributedBackend;
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

async fn register_tpch(ctx: &SessionContext, dir: &Path) {
    for table in TPCH_TABLES {
        let path = dir.join(format!("{table}.parquet"));
        if !path.exists() {
            panic!(
                "TPC-H Parquet missing: {}\nGenerate first:\n  \
                 cargo run --release -p ematix-flow-core --example tpch_generate -- \\\n\
                 \t    --sf 1 --out {}",
                path.display(),
                dir.display()
            );
        }
        ctx.register_parquet(*table, path.to_str().unwrap(), Default::default())
            .await
            .unwrap_or_else(|e| panic!("register {table}: {e}"));
    }
}

/// Spawn `n` tonic-served `Worker`s on free localhost ports in the
/// current tokio runtime. Returns the URLs + a `JoinSet` for clean
/// shutdown. Identical to the cross-pod integration test's helper —
/// duplicated here to avoid coupling the bench to the test module.
async fn spawn_workers(n: usize) -> (Vec<String>, JoinSet<()>) {
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
    // Briefly yield so workers begin accepting connections before
    // the coordinator dials them.
    tokio::time::sleep(Duration::from_millis(50)).await;
    (urls, set)
}

async fn build_backend_with_workers(
    rt: &Runtime,
    peers: Vec<String>,
) -> (Arc<DistributedBackend>, Option<JoinSet<()>>) {
    let _enter = rt.enter();
    let backend = Arc::new(
        DistributedBackend::open(DistributedConfig { peers, tls: None }).expect("backend"),
    );
    register_tpch(backend.session_context().await, &data_dir()).await;
    (backend, None)
}

fn run_query(rt: &Runtime, backend: &DistributedBackend, sql: &str) -> Vec<RecordBatch> {
    rt.block_on(async {
        let stream = backend
            .read_arrow_stream(sql)
            .await
            .expect("read_arrow_stream");
        use futures_util::TryStreamExt;
        stream.try_collect::<Vec<_>>().await.expect("collect")
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

fn bench_distributed(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let dir = data_dir();
    println!("==> TPC-H distributed bench data dir: {}", dir.display());

    let queries: &[(&str, &str)] = &[("q01", Q1), ("q03", Q3), ("q06", Q6), ("q19", Q19)];

    // --- Configuration A: distributed-of-one (peers = []) ---
    {
        let (backend, _) = rt.block_on(build_backend_with_workers(&rt, vec![]));
        for (name, sql) in queries {
            let mut group = c.benchmark_group("tpch_sf1_distributed_of_one");
            group.sample_size(10).measurement_time(measurement_time());
            group.bench_function(*name, |b| {
                b.iter(|| {
                    let r = run_query(&rt, &backend, sql);
                    std::hint::black_box(r);
                });
            });
            group.finish();
        }
    }

    // --- Configuration B: 3 in-process workers ---
    {
        let peers = rt.block_on(async {
            let (urls, set) = spawn_workers(3).await;
            // Leak the JoinSet for the duration of the bench — it
            // owns the worker tasks; dropping it would cancel them.
            // Bench process exits on completion; OS cleans up.
            std::mem::forget(set);
            urls
        });
        let (backend, _) = rt.block_on(build_backend_with_workers(&rt, peers));
        for (name, sql) in queries {
            let mut group = c.benchmark_group("tpch_sf1_distributed_3_workers");
            group.sample_size(10).measurement_time(measurement_time());
            group.bench_function(*name, |b| {
                b.iter(|| {
                    let r = run_query(&rt, &backend, sql);
                    std::hint::black_box(r);
                });
            });
            group.finish();
        }
    }
}

criterion_group!(benches, bench_distributed);
criterion_main!(benches);
