//! Adaptive-mesh-gate plan-shape tests against a REAL in-process
//! Arrow Flight worker mesh (the `cross_pod.rs` scaffolding).
//!
//! Verifies what the gate decides at physical-plan time:
//! - forced ON → the optimized plan carries the datafusion-distributed
//!   stage wrapper (`DistributedExec`, discovered from the
//!   datafusion-distributed 1.0.0 source: the stage splitter wraps
//!   every distributed plan in that root node, with
//!   `NetworkShuffleExec` / `NetworkCoalesceExec` /
//!   `NetworkBroadcastExec` as the flight boundaries inside), and its
//!   stage leaves stay STOCK parquet (Σ.Q15.LS must never rewrite a
//!   shipped fragment — stages must stay codec-serializable).
//! - AUTO + min_bytes=1 over a real parquet table → distributes.
//! - AUTO + min_bytes=u64::MAX → commits LOCAL, and (Σ.Q15.LS) the
//!   local plan's stock `DataSourceExec` parquet leaves are rewritten
//!   to `EmatixFastParquetExec` — measured SF1000: Q15 125.7 s in a
//!   distributed session's local plan vs 7.5 s on the fast provider —
//!   with answers byte-identical to a plain single-node session.

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
    // Σ.Q15.LS: the local-commit leaf rewrite must NEVER touch a
    // distributed plan — stage fragments ship over Arrow Flight and
    // must stay codec-serializable, and workers decode stock
    // `DataSourceExec` leaves. A fast-scan leak into a stage is a
    // fleet-wide decode failure, not a perf regression.
    assert!(
        !rendered.contains("EmatixFastParquetExec"),
        "distributed stages must keep stock parquet leaves (Σ.Q15.LS); got:\n{rendered}"
    );
    assert!(
        rendered.contains("DataSourceExec"),
        "distributed stages should still scan via stock DataSourceExec; got:\n{rendered}"
    );

    // And the mesh plan must actually EXECUTE correctly through the
    // live workers: 16 groups, SUM(v) over all groups totalling
    // 0 + 1 + ... + 1999 = 1_999_000. (This is real distributed
    // execution — unlike the tiny MemTable plans in cross_pod.rs,
    // whose single-task stages the splitter leaves local.)
    let batches = ctx
        .sql(SQL)
        .await
        .expect("sql")
        .collect()
        .await
        .expect("distributed execute");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 16, "16 distinct k groups");
    let total: f64 = batches
        .iter()
        .flat_map(|b| {
            let arr = b
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("f64 sums");
            (0..arr.len()).map(|i| arr.value(i)).collect::<Vec<_>>()
        })
        .sum();
    assert_eq!(total, 1_999_000.0, "SUM(v) across all groups");

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

/// Σ.Q15.LS Test A (plan shape): a peers-configured session whose
/// gate commits LOCAL must not carry stock parquet leaves. The
/// distributed registration is stock `DataSourceExec` (shipped
/// fragments must be codec-serializable), but a locally-committed
/// plan never ships — leaving it on the stock decode path cost Q15
/// 125.7 s vs 7.5 s at SF1000. The gate rewrites those leaves to
/// `EmatixFastParquetExec` on its local-commit returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_min_bytes_max_localizes_scans_to_fast_execs() {
    let (_tmp, path) = write_fixture();

    // Resolver present (peers "configured") but the threshold is
    // unreachable — no worker even needs to exist for the plan to
    // stay local.
    let dummy = vec![Url::parse("http://127.0.0.1:1").expect("url")];
    let ctx = gated_ctx(MeshGateConfig::auto_with_min_bytes(u64::MAX), dummy, &path).await;
    let gated = render(&plan_of(&ctx).await);

    assert!(
        !gated.contains("DistributedExec"),
        "AUTO below threshold must not distribute; got:\n{gated}"
    );
    assert!(
        gated.contains("EmatixFastParquetExec"),
        "locally-committed plan must scan via the ematix fast provider (Σ.Q15.LS); got:\n{gated}"
    );
    assert!(
        !gated.contains("DataSourceExec"),
        "no stock parquet leaf may survive a local commit (Σ.Q15.LS); got:\n{gated}"
    );
}

/// Σ.Q15.LS Test B (answers unchanged): the leaf rewrite is an
/// executor swap, not a semantics change — a filter + aggregate over
/// the localized plan must return byte-for-byte what a plain
/// single-node session returns on the same fixture.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn localized_plan_answers_match_single_node() {
    let (_tmp, path) = write_fixture();

    // Filter + aggregate, spanning several fixture files; ORDER BY
    // pins output order so the comparison is byte-exact. v holds
    // integer-valued f64s (< 2^53), so SUM is order-exact too.
    let sql = "SELECT k, COUNT(*) AS c, SUM(v) AS s FROM t \
               WHERE v >= 250 AND v < 1500 GROUP BY k ORDER BY k";

    let dummy = vec![Url::parse("http://127.0.0.1:1").expect("url")];
    let ctx = gated_ctx(MeshGateConfig::auto_with_min_bytes(u64::MAX), dummy, &path).await;
    // Tripwire: this parity run must actually exercise the SWAPPED
    // plan — if the rewrite silently stopped firing, the comparison
    // below would degenerate to stock-vs-stock and prove nothing.
    let gated_plan = ctx
        .sql(sql)
        .await
        .expect("plan sql")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let gated_rendered = render(&gated_plan);
    assert!(
        gated_rendered.contains("EmatixFastParquetExec"),
        "parity test must run on a localized plan; got:\n{gated_rendered}"
    );
    let gated_batches = ctx
        .sql(sql)
        .await
        .expect("sql")
        .collect()
        .await
        .expect("localized execute");

    let single = single_node_ctx(&path).await;
    let single_batches = single
        .sql(sql)
        .await
        .expect("sql")
        .collect()
        .await
        .expect("single-node execute");

    let gated_txt =
        datafusion::arrow::util::pretty::pretty_format_batches(&gated_batches)
            .expect("format")
            .to_string();
    let single_txt =
        datafusion::arrow::util::pretty::pretty_format_batches(&single_batches)
            .expect("format")
            .to_string();
    assert_eq!(
        gated_txt, single_txt,
        "localized answers must match single-node byte-for-byte"
    );
}
