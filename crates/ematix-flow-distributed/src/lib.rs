//! Σ.B PR 2: distributed batch SQL execution for ematix-flow.
//!
//! Adds a `DistributedBackend` peer to the in-process DataFusion
//! transform path. The backend wraps a [`datafusion::prelude::
//! SessionContext`] built with `datafusion_distributed`'s
//! `with_distributed_planner()` extension, so SQL submitted to it
//! fans out across a peer mesh of ematix-flow processes via Arrow
//! Flight.
//!
//! ## Architecture (vs the original Ballista plan)
//!
//! The Σ.B spike originally named Apache Ballista as the engine
//! behind this backend. The pivot to `datafusion-distributed` —
//! triggered by Ballista's DataFusion-^52 pin colliding with our
//! workspace's DataFusion 53.1, plus the better fit of the
//! library-only model — is recorded in
//! `docs/PHASE_SIGMA_B_TRAIT_SPIKE.md` "PR 2 distributed-engine
//! pivot". Net effect: no separate scheduler/executor binaries,
//! no separate cluster service to operate. Any ematix-flow process
//! that links this crate can act as either coordinator or worker.
//!
//! ## What this crate ships in PR 2
//!
//! - `DistributedBackend` struct + minimal `Backend` trait impl
//!   (dialect/connection_info/dsn/ping/config). Execution methods
//!   (read_arrow_stream / write_arrow_stream / strategy executors)
//!   land in subsequent PR-2 commits.
//! - `DistributedConfig` payload for round-trip via
//!   `ematix_flow_core::backend::BackendConfig`.
//! - A trivial `WorkerResolver` impl wrapping a `Vec<Url>`.
//!
//! Future commits will:
//! - wire `read_arrow_stream` / `write_arrow_stream` to the
//!   distributed `SessionContext`;
//! - publish the `DistributedConfig` variant on the parent
//!   `BackendConfig` enum + the `backend_from_config` dispatch;
//! - add `[transform] engine = "distributed"` config plumbing in
//!   the CLI;
//! - ship `examples/distributed-cluster/` docker-compose with
//!   N peer pods.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_distributed::{DistributedExt, DistributedPhysicalOptimizerRule, WorkerResolver};
use ematix_flow_core::backend::{
    ArrowBatchStream, Backend, BackendConfig, BackendError, DeleteHandling, Dialect,
    DistributedConfig, StrategyRunResult, TargetTable, WriteMode,
};
use ematix_flow_core::pg::ConnectionInfo;
use ematix_flow_core::types::TableSpec;
use futures_util::TryStreamExt;
use tokio::sync::OnceCell;
use url::Url;

// Re-export so callers depending only on `ematix-flow-distributed`
// don't have to also import from `ematix-flow-core` for the config
// type. The canonical definition lives in core (see Σ.B PR 2 commit
// 1's pivot rationale).
pub use ematix_flow_core::backend::DistributedConfig as Config;

/// `WorkerResolver` impl backed by a static `Vec<Url>`. Suitable
/// for fixed-membership clusters where peers are known at config-
/// load time. Dynamic membership (k8s pods discovered via DNS,
/// service-mesh integration) is a Σ.B follow-up.
#[derive(Clone)]
struct StaticWorkerResolver {
    urls: Vec<Url>,
}

#[async_trait]
impl WorkerResolver for StaticWorkerResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        Ok(self.urls.clone())
    }
}

/// Backend that executes SQL transforms across a peer mesh of
/// ematix-flow processes. Wraps a [`SessionContext`] that's built
/// with `datafusion_distributed`'s `with_distributed_planner()`
/// when at least one peer URL is configured; the empty-peers
/// degenerate case falls back to a vanilla single-node
/// `SessionContext` (handy for tests + dev).
pub struct DistributedBackend {
    /// Validated peer URLs. Stored as `Url` so URL re-parsing
    /// errors surface at construction, not at first execute.
    peers: Vec<Url>,
    /// Lazily-built DataFusion session. `OnceCell` so the first
    /// access constructs + caches; subsequent calls reuse without
    /// re-running the builder.
    ctx: Arc<OnceCell<Arc<SessionContext>>>,
}

impl std::fmt::Debug for DistributedBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedBackend")
            .field("peer_count", &self.peers.len())
            .finish_non_exhaustive()
    }
}

impl DistributedBackend {
    /// Construct a backend from peer URL strings. Empty `peers`
    /// produces a single-worker degenerate cluster (handy for
    /// tests). Each peer URL is parsed eagerly so misconfiguration
    /// surfaces here, not on first execute.
    pub fn open(cfg: DistributedConfig) -> Result<Self, BackendError> {
        let peers = cfg
            .peers
            .iter()
            .enumerate()
            .map(|(i, raw)| {
                Url::parse(raw)
                    .map_err(|e| BackendError::Connection(format!("peer #{i} ({raw:?}): {e}")))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            peers,
            ctx: Arc::new(OnceCell::new()),
        })
    }

    /// Borrow the configured peer URLs. Mainly for tests + logs.
    pub fn peers(&self) -> &[Url] {
        &self.peers
    }

    /// Lazily construct + return the session. Vanilla DataFusion
    /// when peers is empty (degenerate single-worker case); fully
    /// distributed-planner-enabled when peers has 1+ entries.
    pub async fn session_context(&self) -> &Arc<SessionContext> {
        self.ctx
            .get_or_init(|| async { self.build_context() })
            .await
    }

    fn build_context(&self) -> Arc<SessionContext> {
        if self.peers.is_empty() {
            // Empty-peers degenerate cluster: skip the distributed
            // planner so the context can run plans locally without
            // needing remote workers. Tests + dev use this path.
            return Arc::new(SessionContext::new());
        }
        let resolver = StaticWorkerResolver {
            urls: self.peers.clone(),
        };
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
            .with_distributed_worker_resolver(resolver)
            .build();
        Arc::new(SessionContext::from(state))
    }
}

#[async_trait]
#[allow(clippy::too_many_arguments)]
impl Backend for DistributedBackend {
    fn dialect(&self) -> Dialect {
        // Distributed execution is still DataFusion-shaped SQL on
        // the wire. The dialect describes the SQL surface, not the
        // execution strategy — so Postgres-flavored DataFusion is
        // the right reading. Σ.D will revisit if streaming SQL
        // diverges.
        Dialect::Postgres
    }

    fn connection_info(&self) -> ConnectionInfo {
        // No single host; report the first peer (or "local" for the
        // degenerate empty-peers case) for human-readable logs.
        let host = self
            .peers
            .first()
            .and_then(|u| u.host_str())
            .unwrap_or("local")
            .to_string();
        let port = self.peers.first().and_then(|u| u.port()).unwrap_or(0);
        ConnectionInfo {
            host,
            port,
            dbname: format!("distributed[{}]", self.peers.len()),
            user: "ematix-flow".into(),
        }
    }

    fn dsn(&self) -> Option<String> {
        // Comma-joined peer URLs — distinct from any single-DSN
        // backend, but matches how operators talk about clusters.
        if self.peers.is_empty() {
            return Some("distributed://local".into());
        }
        Some(
            self.peers
                .iter()
                .map(|u| u.as_str())
                .collect::<Vec<_>>()
                .join(","),
        )
    }

    fn config(&self) -> BackendConfig {
        BackendConfig::Distributed(DistributedConfig {
            peers: self.peers.iter().map(|u| u.to_string()).collect(),
        })
    }

    async fn ping(&self) -> Result<(), BackendError> {
        // PR 2 PR-1 commit: no peer-reachability check yet — the
        // pings happen at first execute when the SessionContext
        // tries to resolve peers. Returning Ok(()) keeps the trait
        // contract intact for code paths that gate on `ping()`
        // before initial use; subsequent commits replace this with
        // a real Arrow Flight liveness probe to each peer.
        Ok(())
    }

    async fn execute(&self, _statement: &str) -> Result<u64, BackendError> {
        Err(BackendError::Other(
            "DistributedBackend::execute not yet implemented — Σ.B PR 2 follow-up commit".into(),
        ))
    }

    async fn read_arrow_stream(&self, query: &str) -> Result<ArrowBatchStream, BackendError> {
        // Delegate to the shared SessionContext. Caller is
        // responsible for having registered any tables the query
        // references (via `session_context().register_table(...)`
        // or analogues). DataFusion plans + executes; with
        // `peers.len() > 0` the planner injects ArrowFlightReadExec
        // nodes and fans the work out across peers.
        let ctx = self.session_context().await.clone();
        let df = ctx
            .sql(query)
            .await
            .map_err(|e| BackendError::Query(format!("plan: {e}")))?;
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| BackendError::Query(format!("execute: {e}")))?;
        // Adapt DataFusion's `SendableRecordBatchStream` (yields
        // `Result<RecordBatch, DataFusionError>`) into the trait's
        // `ArrowBatchStream` (yields `Result<RecordBatch, BackendError>`).
        let mapped = stream.map_err(|e| BackendError::Query(format!("stream: {e}")));
        Ok(Box::pin(mapped))
    }

    async fn write_arrow_stream(
        &self,
        _target: &TargetTable,
        _stream: ArrowBatchStream,
        _mode: WriteMode,
    ) -> Result<u64, BackendError> {
        Err(BackendError::Other(
            "DistributedBackend::write_arrow_stream not yet implemented — Σ.B PR 2 follow-up"
                .into(),
        ))
    }

    async fn run_append(
        &self,
        _spec: &TableSpec,
        _source_query: &str,
        _pipeline_name: &str,
        _source_backend: Option<&dyn Backend>,
        _incremental_column: Option<&str>,
        _last_value_literal: Option<&str>,
        _dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        Err(BackendError::Other(
            "DistributedBackend::run_append not yet implemented — Σ.B PR 2 follow-up".into(),
        ))
    }

    async fn run_truncate(
        &self,
        _spec: &TableSpec,
        _source_query: &str,
        _pipeline_name: &str,
        _source_backend: Option<&dyn Backend>,
        _dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        Err(BackendError::Other(
            "DistributedBackend::run_truncate not yet implemented — Σ.B PR 2 follow-up".into(),
        ))
    }

    async fn run_merge(
        &self,
        _spec: &TableSpec,
        _source_query: &str,
        _keys: &[String],
        _update_columns: &[String],
        _pipeline_name: &str,
        _mode_label: &str,
        _source_backend: Option<&dyn Backend>,
        _delete_handling: Option<DeleteHandling>,
        _dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        Err(BackendError::Other(
            "DistributedBackend::run_merge not yet implemented — Σ.B PR 2 follow-up".into(),
        ))
    }

    async fn run_scd2(
        &self,
        _spec: &TableSpec,
        _source_query: &str,
        _keys: &[String],
        _compare_columns: &[String],
        _pipeline_name: &str,
        _source_backend: Option<&dyn Backend>,
        _delete_handling: Option<DeleteHandling>,
        _event_timestamp_column: Option<&str>,
        _ttl_seconds: Option<i64>,
        _dry_run: bool,
    ) -> Result<StrategyRunResult, BackendError> {
        Err(BackendError::Other(
            "DistributedBackend::run_scd2 not yet implemented — Σ.B PR 2 follow-up".into(),
        ))
    }
}

/// Marker / placeholder for symmetry with `Arc<dyn Backend>` consumers.
/// Lets PR-2 code paths use the same `Arc<dyn Backend>` shape they'd
/// use with the in-process DataFusion path; the actual distributed
/// dispatch wires up in subsequent commits.
pub fn open_arc(cfg: DistributedConfig) -> Result<Arc<dyn Backend>, BackendError> {
    Ok(Arc::new(DistributedBackend::open(cfg)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_with_no_peers_constructs_degenerate_backend() {
        let cfg = DistributedConfig::default();
        let backend = DistributedBackend::open(cfg).expect("open");
        assert_eq!(backend.peers().len(), 0);
    }

    #[test]
    fn open_validates_peer_urls_at_construction() {
        let cfg = DistributedConfig {
            peers: vec!["not a url".into()],
        };
        let err = DistributedBackend::open(cfg).expect_err("bad URL must fail");
        assert!(format!("{err}").to_lowercase().contains("peer"));
    }

    #[test]
    fn open_accepts_well_formed_peer_urls() {
        let cfg = DistributedConfig {
            peers: vec![
                "http://flow-01.cluster.local:50051".into(),
                "http://flow-02.cluster.local:50051".into(),
            ],
        };
        let backend = DistributedBackend::open(cfg).expect("open");
        assert_eq!(backend.peers().len(), 2);
        let info = backend.connection_info();
        assert_eq!(info.host, "flow-01.cluster.local");
        assert_eq!(info.port, 50051);
        assert_eq!(info.dbname, "distributed[2]");
    }

    #[test]
    fn dsn_concatenates_peers() {
        let backend = DistributedBackend::open(DistributedConfig {
            peers: vec!["http://a:1".into(), "http://b:2".into()],
        })
        .unwrap();
        let dsn = backend.dsn().unwrap();
        assert!(dsn.contains("a:1"));
        assert!(dsn.contains("b:2"));
        assert!(dsn.contains(','));
    }

    #[test]
    fn dsn_for_empty_peers_uses_local_marker() {
        let backend = DistributedBackend::open(DistributedConfig::default()).unwrap();
        let dsn = backend.dsn().unwrap();
        assert!(dsn.contains("local"));
    }

    /// `Arc<dyn Backend>` is the shape Σ.B's executors use over Arrow
    /// Flight. Compile-only check that the trait remains object-safe
    /// from this crate's perspective.
    #[test]
    fn distributed_backend_is_object_safe_under_dyn_backend() {
        fn assert_object_safe<T: ?Sized>() {}
        assert_object_safe::<dyn Backend>();
    }

    #[tokio::test]
    async fn execute_returns_not_yet_implemented() {
        let backend = DistributedBackend::open(DistributedConfig::default()).unwrap();
        let err = backend.execute("SELECT 1").await.expect_err("not yet");
        assert!(
            format!("{err}")
                .to_lowercase()
                .contains("not yet implemented")
        );
    }

    /// Σ.B PR 2 commit 3: empty-peers degenerate cluster runs the
    /// query through plain DataFusion locally. No network, no
    /// remote workers; round-trips RecordBatches through the trait
    /// surface. Smokes the full pipeline (build context → register
    /// table → SQL plan → execute → collect) end-to-end.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_arrow_stream_runs_locally_with_no_peers() {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;
        use futures_util::TryStreamExt;
        use std::sync::Arc;

        let backend = DistributedBackend::open(DistributedConfig::default()).unwrap();
        let ctx = backend.session_context().await.clone();

        // Register a tiny in-memory table the SQL can reference.
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "n",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();
        let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("nums", Arc::new(mem_table)).unwrap();

        // Execute through the Backend trait surface.
        let stream = backend
            .read_arrow_stream("SELECT SUM(n) AS s FROM nums")
            .await
            .expect("plan + execute");
        let collected: Vec<_> = stream.try_collect().await.expect("collect");

        assert_eq!(collected.len(), 1, "single result batch");
        let column = collected[0].column(0);
        let arr = column
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 sum");
        assert_eq!(arr.value(0), 15);
    }

    /// Σ.B PR 2 commit 2: full round-trip through the shared
    /// `BackendConfig` enum. Construct → `.config()` → serialize →
    /// deserialize → construct again → assert peers match.
    #[test]
    fn config_method_round_trips_through_serde() {
        let original = DistributedBackend::open(DistributedConfig {
            peers: vec!["http://a:50051".into(), "http://b:50051".into()],
        })
        .unwrap();

        let cfg = original.config();
        let json = serde_json::to_string(&cfg).expect("serialize");
        let recovered_cfg: BackendConfig = serde_json::from_str(&json).expect("deserialize");
        let inner = match recovered_cfg {
            BackendConfig::Distributed(c) => c,
            other => panic!("expected Distributed, got {other:?}"),
        };
        let recovered_backend = DistributedBackend::open(inner).expect("rebuild");
        assert_eq!(original.peers().len(), recovered_backend.peers().len());
        for (a, b) in original.peers().iter().zip(recovered_backend.peers()) {
            assert_eq!(a.as_str(), b.as_str());
        }
    }
}
