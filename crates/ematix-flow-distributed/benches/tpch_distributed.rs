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
//!     `<workspace>/examples/tpch/data/sf1`). The bench's group
//!     label inherits the SF tag from this path's basename when it
//!     matches `sf<N>`, falling back to `custom` otherwise.
//!   - `TPCH_MEASUREMENT_TIME_S` — criterion measurement window per
//!     query (default 20s)
//!   - `EMATIX_DISTRIBUTED_PEERS` — comma-separated list of external
//!     peer URLs to target instead of spawning in-process workers.
//!     Set this when you want to bench against the docker-compose
//!     stack at `examples/distributed-cluster/`. The in-process
//!     `distributed_of_one` baseline still runs alongside; only
//!     Configuration B (the multi-worker case) switches to the
//!     external peers. **Honest framing**: docker-compose is
//!     single-host (loopback network, shared kernel/CPU/memory) —
//!     the numbers measure multi-process distributed-plan overhead,
//!     not cross-host scaling.
//!
//! Generate data first:
//!     cargo run --release -p ematix-flow-core --example tpch_generate -- \
//!         --sf 1 --out examples/tpch/data/sf1
//!
//! Then run, in-process workers (default):
//!     cargo bench -p ematix-flow-distributed --bench tpch_distributed
//!
//! Or against the docker-compose 3-worker stack:
//!     docker compose -f examples/distributed-cluster/docker-compose.yml up -d
//!     EMATIX_DISTRIBUTED_PEERS=http://localhost:50051,http://localhost:50052,http://localhost:50053 \
//!         cargo bench -p ematix-flow-distributed --bench tpch_distributed
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

/// Derive an SF tag (`sf1`, `sf10`, `sf100`, ...) from the data dir
/// basename so bench group labels reflect what was actually run.
/// Falls back to `custom` for paths that don't match `sf<N>`.
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

/// Σ.C PR 2 prep: external peer URLs override the in-process worker
/// spawn for Configuration B. Comma-separated list, parsed as-is —
/// validation happens at `DistributedBackend::open`. Returns `None`
/// when the env var is unset or empty.
fn external_peers() -> Option<Vec<String>> {
    let raw = std::env::var("EMATIX_DISTRIBUTED_PEERS").ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(
        trimmed
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
    )
}

/// Returns `false` when the SF=1 Parquet directory is missing —
/// CI doesn't generate the data and `cargo test --workspace
/// --all-targets` runs the criterion bench harness's `main()`.
/// Callers handle `false` by `eprintln!`-skipping + early-returning
/// from `bench_distributed`.
async fn register_tpch(ctx: &SessionContext, dir: &Path) -> bool {
    for table in TPCH_TABLES {
        let path = dir.join(format!("{table}.parquet"));
        if !path.exists() {
            eprintln!(
                "skip: TPC-H Parquet missing at {}; generate via\n  \
                 cargo run --release -p ematix-flow-core --example tpch_generate -- \
                 --sf 1 --out {}",
                path.display(),
                dir.display()
            );
            return false;
        }
        ctx.register_parquet(*table, path.to_str().unwrap(), Default::default())
            .await
            .unwrap_or_else(|e| panic!("register {table}: {e}"));
    }
    true
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
) -> Option<Arc<DistributedBackend>> {
    let _enter = rt.enter();
    let backend = Arc::new(
        DistributedBackend::open(DistributedConfig { peers, tls: None }).expect("backend"),
    );
    if !register_tpch(backend.session_context().await, &data_dir()).await {
        return None;
    }
    Some(backend)
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
    let sf = sf_tag(&dir);
    let external = external_peers();
    println!("==> TPC-H distributed bench data dir: {}", dir.display());
    println!("==> SF tag (group label): {sf}");
    if let Some(peers) = external.as_ref() {
        println!(
            "==> Configuration B targets external peers ({}): {:?}\n    \
             (single-host docker-compose / k8s = multi-process, NOT \
             cross-host)",
            peers.len(),
            peers
        );
    } else {
        println!("==> Configuration B uses 3 in-process workers (loopback)");
    }

    let queries: &[(&str, &str)] = &[("q01", Q1), ("q03", Q3), ("q06", Q6), ("q19", Q19)];

    // --- Configuration A: distributed-of-one (peers = []) ---
    {
        let Some(backend) = rt.block_on(build_backend_with_workers(&rt, vec![])) else {
            return;
        };
        let group_name = format!("tpch_{sf}_distributed_of_one");
        for (name, sql) in queries {
            let mut group = c.benchmark_group(&group_name);
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

    // --- Configuration B: 3 in-process workers OR external peers ---
    {
        let (peers, group_name) = match external {
            Some(peers) => {
                let n = peers.len();
                (peers, format!("tpch_{sf}_distributed_external_{n}_peers"))
            }
            None => {
                let urls = rt.block_on(async {
                    let (urls, set) = spawn_workers(3).await;
                    // Leak the JoinSet for the duration of the
                    // bench — it owns the worker tasks; dropping it
                    // would cancel them. Bench process exits on
                    // completion; OS cleans up.
                    std::mem::forget(set);
                    urls
                });
                (urls, format!("tpch_{sf}_distributed_3_workers"))
            }
        };
        let Some(backend) = rt.block_on(build_backend_with_workers(&rt, peers)) else {
            return;
        };
        for (name, sql) in queries {
            let mut group = c.benchmark_group(&group_name);
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
