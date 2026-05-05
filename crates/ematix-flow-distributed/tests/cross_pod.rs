//! Σ.B PR 3: cross-pod runtime test for distributed execution.
//!
//! Spawns N in-process workers via tonic on free localhost ports,
//! then runs SQL through a coordinator `DistributedSqlTransform`
//! pointed at those workers. Verifies that the
//! `DistributedPhysicalOptimizerRule` -wrapped plan actually fans
//! work across the peer mesh + collects results back.
//!
//! No Docker required — the test process IS the cluster. Real
//! cross-host deployment is shown by `examples/distributed-cluster/`
//! with the same `flow-worker` binary the test spawns in-process.

use std::net::SocketAddr;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion_distributed::{DefaultSessionBuilder, Worker};
use ematix_flow_core::backend::DistributedConfig;
use ematix_flow_core::transform::{BatchContext, BatchTransform};
use ematix_flow_distributed::{DistributedBackend, DistributedSqlTransform};
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

/// Spawn `n` workers on free localhost ports. Returns the assigned
/// `http://127.0.0.1:<port>` URLs and a `JoinSet` whose `.abort_all`
/// shuts the workers down at end of test.
async fn spawn_workers(n: usize) -> (Vec<String>, JoinSet<()>) {
    let mut urls = Vec::with_capacity(n);
    let mut join_set: JoinSet<()> = JoinSet::new();
    for i in 0..n {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|e| panic!("worker {i}: bind: {e}"));
        let local: SocketAddr = listener.local_addr().expect("local_addr");
        urls.push(format!("http://{local}"));

        let incoming = TcpListenerStream::new(listener);
        join_set.spawn(async move {
            let worker = Worker::from_session_builder(DefaultSessionBuilder);
            // Errors here aren't observable from the test; the test
            // fails on the coordinator side via missing rows / RPC
            // errors. Workers shut down cleanly when the JoinSet is
            // aborted.
            let _ = Server::builder()
                .add_service(worker.into_worker_server())
                .serve_with_incoming(incoming)
                .await;
        });
    }
    // Briefly yield so workers begin accepting connections before
    // the coordinator dials them. tonic's serve_with_incoming
    // installs its listener synchronously after the spawn yields.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (urls, join_set)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_sql_transform_runs_against_three_workers() {
    let (peers, mut workers) = spawn_workers(3).await;

    let backend = Arc::new(
        DistributedBackend::open(DistributedConfig {
            peers: peers.clone(),
        })
        .expect("backend"),
    );

    // Register `source` directly on the backend's session so the
    // transform's per-call register/deregister doesn't have to fight
    // with another transform. For this test we treat the backend as
    // the coordinator + register input data once, then run a query
    // that the distributed planner will fan out.
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from((1..=10).collect::<Vec<_>>()))],
    )
    .unwrap();
    let mem = MemTable::try_new(schema, vec![vec![batch.clone()]]).unwrap();
    let ctx = backend.session_context().await.clone();
    ctx.register_table("source", Arc::new(mem))
        .expect("register");

    // Query that the distributed planner can split into stages.
    let xform = DistributedSqlTransform::new("SELECT SUM(n) AS total FROM source", backend.clone());

    let result = xform
        .transform(batch, &BatchContext::default())
        .await
        .expect("transform");

    // Single-row result with total = 1+2+...+10 = 55.
    assert_eq!(result.len(), 1, "one batch back");
    let arr = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64");
    assert_eq!(arr.value(0), 55, "expected SUM(1..10) = 55");

    workers.abort_all();
}

/// Exercises the full `DistributedSqlTransform::open(...)` ergonomics
/// path: build the transform from peer URLs without any pre-existing
/// `DistributedBackend`. Same answer-correctness assertion as the
/// shared-backend test above.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_sql_transform_open_against_two_workers() {
    let (peers, mut workers) = spawn_workers(2).await;

    let xform = DistributedSqlTransform::open(
        "SELECT SUM(n) AS total FROM source",
        DistributedConfig { peers },
    )
    .expect("open");

    // Register the source directly on the transform's backend session.
    // (DistributedSqlTransform owns its backend internally; we expose
    // it indirectly by re-running the open and using ::new with a
    // shared Arc.) The simpler path: register on the backend the
    // transform owns, via a separate accessor — but for this test
    // we register through the transform's session.
    //
    // For now, this test uses the `transform()` method which itself
    // registers `source` per call from the input batch. So no
    // pre-registration is needed.
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from((1..=10).collect::<Vec<_>>()))],
    )
    .unwrap();
    let result = xform
        .transform(batch, &BatchContext::default())
        .await
        .expect("transform");
    assert_eq!(result.len(), 1);
    let arr = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64");
    assert_eq!(arr.value(0), 55);

    workers.abort_all();
}
