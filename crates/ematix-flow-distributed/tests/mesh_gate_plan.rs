//! Adaptive-mesh-gate plan-shape tests against a REAL in-process
//! Arrow Flight worker mesh (the `cross_pod.rs` scaffolding).
//!
//! Verifies what the gate decides at physical-plan time:
//! - forced ON → the optimized plan carries the datafusion-distributed
//!   stage wrapper (`DistributedExec`, discovered from the
//!   datafusion-distributed 1.0.0 source: the stage splitter wraps
//!   every distributed plan in that root node, with
//!   `NetworkShuffleExec` / `NetworkCoalesceExec` /
//!   `NetworkBroadcastExec` as the flight boundaries inside).
//! - AUTO + min_bytes=1 over a real parquet table → distributes.
//! - AUTO + min_bytes=u64::MAX → stays byte-identical to the
//!   never-gated single-node plan.

use std::net::SocketAddr;
use std::sync::Arc;

use datafusion::arrow::array::{Float64Array, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_distributed::{
    CompressionType, DefaultSessionBuilder, DistributedExt, Worker, WorkerResolver,
};
use ematix_flow_distributed::mesh_gate::{AdaptiveMeshGateRule, MeshGateConfig};
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use url::Url;

/// `WorkerResolver` over a fixed URL list (the cross_pod pattern —
/// the crate's own `StaticWorkerResolver` is private).
#[derive(Clone)]
struct StaticPeers {
    urls: Vec<Url>,
}

#[async_trait::async_trait]
impl WorkerResolver for StaticPeers {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        Ok(self.urls.clone())
    }
}

/// Spawn `n` in-process workers on free localhost ports (identical to
/// `tests/cross_pod.rs::spawn_workers`).
async fn spawn_workers(n: usize) -> (Vec<Url>, JoinSet<()>) {
    let mut urls = Vec::with_capacity(n);
    let mut join_set: JoinSet<()> = JoinSet::new();
    for i in 0..n {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|e| panic!("worker {i}: bind: {e}"));
        let local: SocketAddr = listener.local_addr().expect("local_addr");
        urls.push(Url::parse(&format!("http://{local}")).expect("worker url"));

        let incoming = TcpListenerStream::new(listener);
        join_set.spawn(async move {
            let worker = Worker::from_session_builder(DefaultSessionBuilder);
            let _ = Server::builder()
                .add_service(worker.into_worker_server())
                .serve_with_incoming(incoming)
                .await;
        });
    }
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (urls, join_set)
}

/// Small parquet fixture: a DIRECTORY of 4 files × 500 rows
/// (k Int64, v Float64). Multiple files matter: the stage splitter's
/// `FileScanConfigTaskEstimator` assigns tasks per file
/// (`files_per_task`), and it elides network boundaries entirely when
/// both sides of a would-be boundary run in a single task — a
/// one-file scan therefore never distributes no matter what the gate
/// decides. Pairing 4 files with `with_distributed_files_per_task(1)`
/// gives the splitter real multi-task stages to split.
fn write_fixture() -> (tempfile::TempDir, String) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Float64, false),
    ]));
    for f in 0..4i64 {
        let path = tmp.path().join(format!("t{f}.parquet"));
        let n = 500i64;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(
                    (0..n).map(|i| (f * n + i) % 16).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    (0..n).map(|i| (f * n + i) as f64).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("batch");
        let file = std::fs::File::create(&path).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
    }
    let dir = tmp.path().to_string_lossy().into_owned();
    (tmp, dir)
}

/// Session with the gate installed LAST + the worker resolver + the
/// production LZ4_FRAME exchange compression — the
/// `DistributedBackend::build_context` peers-path shape (stock
/// DataFusion features; the ematix preset is exercised by the unit
/// tests and the campaign parity pins).
async fn gated_ctx(config: MeshGateConfig, urls: Vec<Url>, parquet_path: &str) -> SessionContext {
    let cfg = SessionConfig::new()
        .with_collect_statistics(true)
        .with_target_partitions(4);
    let builder = SessionStateBuilder::new()
        .with_config(cfg)
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(AdaptiveMeshGateRule::new(config)))
        .with_distributed_worker_resolver(StaticPeers { urls });
    let builder = builder
        .with_distributed_compression(Some(CompressionType::LZ4_FRAME))
        .expect("LZ4_FRAME is a valid compression type")
        // One task per fixture file (see `write_fixture`): the leaf
        // stage spans multiple tasks, so the splitter has boundaries
        // to install when the gate lets it run.
        .with_distributed_files_per_task(1)
        .expect("files_per_task is configurable");
    let ctx = SessionContext::new_with_state(builder.build());
    ctx.register_parquet("t", parquet_path, Default::default())
        .await
        .expect("register parquet");
    ctx
}

/// Same session WITHOUT gate/resolver — the single-node reference.
async fn single_node_ctx(parquet_path: &str) -> SessionContext {
    let cfg = SessionConfig::new()
        .with_collect_statistics(true)
        .with_target_partitions(4);
    let builder = SessionStateBuilder::new()
        .with_config(cfg)
        .with_default_features();
    let ctx = SessionContext::new_with_state(builder.build());
    ctx.register_parquet("t", parquet_path, Default::default())
        .await
        .expect("register parquet");
    ctx
}

/// A query whose optimized plan needs a hash repartition, so the
/// stage splitter has a network boundary to split on.
const SQL: &str = "SELECT k, SUM(v) AS s FROM t GROUP BY k ORDER BY k";

async fn plan_of(ctx: &SessionContext) -> Arc<dyn ExecutionPlan> {
    ctx.sql(SQL)
        .await
        .expect("plan sql")
        .create_physical_plan()
        .await
        .expect("physical plan")
}

fn render(plan: &Arc<dyn ExecutionPlan>) -> String {
    displayable(plan.as_ref()).indent(true).to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forced_on_plan_contains_distributed_stage_wrapper() {
    let (_tmp, path) = write_fixture();
    let (urls, mut workers) = spawn_workers(2).await;

    let ctx = gated_ctx(MeshGateConfig::forced(), urls, &path).await;
    let plan = plan_of(&ctx).await;
    let rendered = render(&plan);
    assert!(
        rendered.contains("DistributedExec"),
        "forced-on plan must carry the datafusion-distributed stage wrapper; got:\n{rendered}"
    );

    workers.abort_all();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_min_bytes_one_distributes_real_table() {
    let (_tmp, path) = write_fixture();
    let (urls, mut workers) = spawn_workers(2).await;

    let ctx = gated_ctx(MeshGateConfig::auto_with_min_bytes(1), urls, &path).await;
    let plan = plan_of(&ctx).await;
    let rendered = render(&plan);
    assert!(
        rendered.contains("DistributedExec"),
        "AUTO with min_bytes=1 over a real table must distribute; got:\n{rendered}"
    );

    workers.abort_all();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_min_bytes_max_keeps_single_node_plan() {
    let (_tmp, path) = write_fixture();

    // Resolver present (peers "configured") but the threshold is
    // unreachable — no worker even needs to exist for the plan to
    // stay local.
    let dummy = vec![Url::parse("http://127.0.0.1:1").expect("url")];
    let ctx = gated_ctx(MeshGateConfig::auto_with_min_bytes(u64::MAX), dummy, &path).await;
    let gated = render(&plan_of(&ctx).await);

    let single = single_node_ctx(&path).await;
    let expected = render(&plan_of(&single).await);

    assert!(
        !gated.contains("DistributedExec"),
        "AUTO below threshold must not distribute; got:\n{gated}"
    );
    assert_eq!(
        gated, expected,
        "AUTO below threshold must produce the exact single-node plan"
    );
}
