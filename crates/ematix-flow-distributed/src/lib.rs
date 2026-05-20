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

// Phase Z WorkUnit — ephemeral one-shot worker spec (K8s Job, Lambda
// invocation, or local `flow run-shard` for testing). Distinct from
// the long-running `DistributedBackend` tonic-worker model below; see
// docs/DISTRIBUTED_TPCH_BENCHMARK_PLAN.md.
pub mod work_unit;
// Phase 3 of "What's not shipped": peer auto-discovery. Recognises
// `dns://` and `k8s://` schemes inside the `peers` config and resolves
// them to concrete A-record URLs at backend-open time. Plain `http(s)://`
// URLs continue to work unchanged. See module docs.
pub mod peer_discovery;

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_distributed::{DistributedExt, DistributedPhysicalOptimizerRule, WorkerResolver};
use ematix_flow_core::backend::{
    ArrowBatchStream, Backend, BackendConfig, BackendError, DeleteHandling, Dialect,
    DistributedConfig, DistributedTlsConfig, StrategyRunResult, TargetTable, WriteMode,
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
    /// Σ.B follow-up: client-side TLS config carried verbatim from
    /// `DistributedConfig`. `None` keeps plain-HTTP behaviour.
    tls: Option<DistributedTlsConfig>,
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
    ///
    /// Peer entries may use any of the following schemes (mixed
    /// freely in the same list):
    ///
    /// - `http://host:port` / `https://host:port` — static URL,
    ///   passed through.
    /// - `dns://host:port` — A-record lookup at open time, one URL
    ///   per resolved address.
    /// - `k8s://service.namespace:port` — sugar for
    ///   `dns://service.namespace.svc.cluster.local:port`. Lets a
    ///   single config line replace a whole pod-IP list.
    ///
    /// See [`crate::peer_discovery`] for the resolution contract.
    pub fn open(cfg: DistributedConfig) -> Result<Self, BackendError> {
        let peers = peer_discovery::expand_peer_entries(&cfg.peers)?;
        Ok(Self {
            peers,
            tls: cfg.tls,
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
        let mut builder = SessionStateBuilder::new()
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
            .with_distributed_worker_resolver(resolver);
        // Σ.B follow-up: install the TLS-aware channel resolver
        // when a TLS config is present. `TlsChannelResolver::new`
        // loads the PEM files eagerly so config errors surface here
        // (at first session build) rather than at first peer dial.
        // A bad PEM at this point is fatal — the build_context call
        // path doesn't return Result, so we panic with the loader
        // error. Operators get the same diagnostic they'd get from
        // `flow-worker --tls-cert ... --tls-key ...` failing fast.
        if let Some(tls_cfg) = &self.tls {
            let resolver = tls::TlsChannelResolver::new(tls_cfg.clone()).unwrap_or_else(|e| {
                panic!(
                    "DistributedBackend: failed to load client TLS material from \
                     DistributedTlsConfig: {e}. Check `ca_cert_pem_path` + \
                     `client_identity` paths point at readable PEM files."
                )
            });
            builder = builder.with_distributed_channel_resolver(resolver);
        }
        Arc::new(SessionContext::from(builder.build()))
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
            tls: self.tls.clone(),
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

// ---------------------------------------------------------------
// Σ.B PR 2 commit 5: DistributedSqlTransform.
// ---------------------------------------------------------------

use arrow_schema::SchemaRef;
use datafusion::datasource::MemTable;
use ematix_flow_core::transform::{BatchContext, BatchTransform, LookupTable};

/// A `BatchTransform` that runs the configured SQL through a
/// `DistributedBackend`'s `SessionContext`. Per call:
///
///   1. Register the input `RecordBatch` as a temp table named
///      `source` in the backend's session.
///   2. Run the user's SQL via the session.
///   3. Drain the result stream into `Vec<RecordBatch>`.
///   4. Deregister `source` so the next batch can re-register
///      with potentially different cardinality.
///
/// The first call captures the input + output schemas. Subsequent
/// calls assert input compatibility and return the cached output
/// schema in `output_schema()`. Same lazy-init pattern as
/// `LazySqlTransform`.
///
/// Every transform owns its own `DistributedBackend` so the
/// per-call `register_table("source", ...)` doesn't race with
/// concurrent transforms on a shared session. Avoiding shared-
/// session contention costs one extra session per transform; the
/// session is lightweight (no live network connections until the
/// first execute).
pub struct DistributedSqlTransform {
    sql: String,
    backend: Arc<DistributedBackend>,
    /// Σ.B follow-up: static lookup tables, registered alongside
    /// `source` on the first `transform()` call. The
    /// `DistributedPhysicalOptimizerRule` plans small lookups as
    /// broadcast joins, so each peer worker receives the full
    /// lookup data via Arrow Flight rather than needing a local
    /// copy. Empty `Vec` = no lookups (the common case).
    lookups: Vec<LookupTable>,
    schemas: tokio::sync::OnceCell<DistributedSqlSchemas>,
}

struct DistributedSqlSchemas {
    input: SchemaRef,
    output: SchemaRef,
}

impl std::fmt::Debug for DistributedSqlTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedSqlTransform")
            .field("sql", &self.sql)
            .field("peer_count", &self.backend.peers().len())
            .field("initialized", &self.schemas.initialized())
            .finish()
    }
}

impl DistributedSqlTransform {
    /// Build from SQL + a constructed `DistributedBackend`. Schemas
    /// are captured on the first `transform()` call.
    pub fn new(sql: impl Into<String>, backend: Arc<DistributedBackend>) -> Self {
        Self {
            sql: sql.into(),
            backend,
            lookups: Vec::new(),
            schemas: tokio::sync::OnceCell::new(),
        }
    }

    /// Σ.B follow-up: build with one or more static lookup tables.
    /// Each is registered as a table in the backend's
    /// `SessionContext` on the first `transform()` call (alongside
    /// `source`); the SQL can reference them by their `LookupTable::name`.
    /// Lookups travel to peer workers via Arrow Flight as part of
    /// the distributed plan — `DistributedPhysicalOptimizerRule`
    /// emits broadcast joins for small enough tables.
    pub fn new_with_lookups(
        sql: impl Into<String>,
        backend: Arc<DistributedBackend>,
        lookups: Vec<LookupTable>,
    ) -> Self {
        Self {
            sql: sql.into(),
            backend,
            lookups,
            schemas: tokio::sync::OnceCell::new(),
        }
    }

    /// Convenience: build a fresh backend from `cfg` + wrap it in
    /// the transform. Validates peer URLs eagerly via the backend's
    /// `open` constructor.
    pub fn open(sql: impl Into<String>, cfg: DistributedConfig) -> Result<Self, BackendError> {
        let backend = Arc::new(DistributedBackend::open(cfg)?);
        Ok(Self::new(sql, backend))
    }

    /// Σ.B follow-up: convenience constructor that builds a fresh
    /// backend + attaches lookups in one call.
    pub fn open_with_lookups(
        sql: impl Into<String>,
        cfg: DistributedConfig,
        lookups: Vec<LookupTable>,
    ) -> Result<Self, BackendError> {
        let backend = Arc::new(DistributedBackend::open(cfg)?);
        Ok(Self::new_with_lookups(sql, backend, lookups))
    }

    /// Borrow the SQL — for diagnostics + log lines.
    pub fn sql(&self) -> &str {
        &self.sql
    }
}

#[async_trait]
impl BatchTransform for DistributedSqlTransform {
    fn input_schema(&self) -> SchemaRef {
        self.schemas
            .get()
            .expect(
                "DistributedSqlTransform::input_schema called before first \
                 transform() — schema is captured on first batch",
            )
            .input
            .clone()
    }

    fn output_schema(&self) -> SchemaRef {
        self.schemas
            .get()
            .expect(
                "DistributedSqlTransform::output_schema called before first \
                 transform() — schema is captured on first batch",
            )
            .output
            .clone()
    }

    async fn transform(
        &self,
        input: datafusion::arrow::array::RecordBatch,
        _ctx: &BatchContext,
    ) -> Result<Vec<datafusion::arrow::array::RecordBatch>, BackendError> {
        let input_schema = input.schema();
        let ctx = self.backend.session_context().await.clone();

        // Register the batch as a fresh `source` table. Deregister
        // anything previously bound under that name first so a
        // schema change on a later batch doesn't conflict.
        let _ = ctx.deregister_table("source");
        let mem = MemTable::try_new(input_schema.clone(), vec![vec![input]])
            .map_err(|e| BackendError::Query(format!("memtable: {e}")))?;
        ctx.register_table("source", Arc::new(mem))
            .map_err(|e| BackendError::Query(format!("register source: {e}")))?;

        // Σ.B follow-up: register lookups on first call. Lookups are
        // static data; we register once and let DataFusion +
        // DistributedPhysicalOptimizerRule handle distribution
        // (broadcast joins ship the table data to each peer over
        // Arrow Flight). Subsequent calls reuse the registration.
        // `schemas.initialized()` doubles as the "first-call"
        // sentinel here — true after the first successful
        // transform; no-op after that.
        if !self.schemas.initialized() {
            for lookup in &self.lookups {
                let mem = MemTable::try_new(lookup.schema.clone(), vec![lookup.batches.clone()])
                    .map_err(|e| {
                        BackendError::Query(format!("lookup {:?} memtable: {e}", lookup.name))
                    })?;
                ctx.register_table(&lookup.name, Arc::new(mem))
                    .map_err(|e| {
                        BackendError::Query(format!("register lookup {:?}: {e}", lookup.name))
                    })?;
            }
        }

        let df = ctx
            .sql(&self.sql)
            .await
            .map_err(|e| BackendError::Query(format!("plan: {e}")))?;
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| BackendError::Query(format!("execute: {e}")))?;
        let batches: Vec<datafusion::arrow::array::RecordBatch> = stream
            .map_err(|e| BackendError::Query(format!("stream: {e}")))
            .try_collect()
            .await?;

        // Cache schemas on first successful call.
        if !self.schemas.initialized() {
            let output_schema = batches
                .first()
                .map(|b| b.schema())
                .unwrap_or_else(|| input_schema.clone());
            let _ = self.schemas.set(DistributedSqlSchemas {
                input: input_schema,
                output: output_schema,
            });
        }

        Ok(batches)
    }
}

// ---------------------------------------------------------------
// Σ.B follow-up: TLS support for coordinator ↔ worker traffic.
// ---------------------------------------------------------------

pub mod tls {
    //! TLS plumbing for the worker mesh. Two surfaces:
    //!
    //! - [`load_server_tls_config`]: read PEM files from disk and
    //!   build a `tonic::transport::ServerTlsConfig` for the
    //!   `flow-worker` binary's `Server::tls_config(...)` call.
    //!   When `client_ca_pem_path` is `Some`, the worker requires
    //!   client certs (mTLS); otherwise it terminates a one-way
    //!   server-auth TLS handshake.
    //!
    //! - [`TlsChannelResolver`]: a `datafusion_distributed::ChannelResolver`
    //!   that wraps each peer URL in a TLS-enabled tonic `Channel`.
    //!   Mirrors `DefaultChannelResolver`'s per-URL channel cache so
    //!   we don't redo the TLS handshake on every gRPC method call.
    //!
    //! Both surfaces consume the same `DistributedTlsConfig` shape
    //! that lives in core, so a coordinator's `BackendConfig` JSON
    //! and a worker's CLI flags describe the same trust boundary
    //! from opposite ends.
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use datafusion::common::DataFusionError;
    use datafusion_distributed::{
        BoxCloneSyncChannel, ChannelResolver, WorkerServiceClient, create_worker_client,
    };
    use ematix_flow_core::backend::{BackendError, DistributedTlsConfig};
    use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity, ServerTlsConfig};
    use url::Url;

    /// Read PEM files from disk and assemble a `ServerTlsConfig`
    /// for `tonic::transport::Server::tls_config(...)`.
    ///
    /// `client_ca_pem_path = Some(...)` opts the worker into mTLS:
    /// inbound clients (the coordinator) must present a cert chain
    /// signed by the named CA. `None` keeps server-auth-only TLS,
    /// where the channel is encrypted but anyone who can reach the
    /// port can connect. Pick mTLS when the workers are reachable
    /// from outside the trusted network zone.
    pub fn load_server_tls_config(
        cert_pem_path: &str,
        key_pem_path: &str,
        client_ca_pem_path: Option<&str>,
    ) -> Result<ServerTlsConfig, BackendError> {
        let cert = std::fs::read(cert_pem_path)
            .map_err(|e| BackendError::Other(format!("read tls cert {cert_pem_path:?}: {e}")))?;
        let key = std::fs::read(key_pem_path)
            .map_err(|e| BackendError::Other(format!("read tls key {key_pem_path:?}: {e}")))?;
        let identity = Identity::from_pem(cert, key);
        let mut cfg = ServerTlsConfig::new().identity(identity);
        if let Some(ca_path) = client_ca_pem_path {
            let ca = std::fs::read(ca_path)
                .map_err(|e| BackendError::Other(format!("read client-ca {ca_path:?}: {e}")))?;
            cfg = cfg.client_ca_root(Certificate::from_pem(ca));
        }
        Ok(cfg)
    }

    /// `ChannelResolver` that builds TLS-enabled tonic channels and
    /// caches them per URL. Mirrors `DefaultChannelResolver`'s shape
    /// (5-min TTI cache; per-URL one-shot connect) but threads a
    /// `ClientTlsConfig` derived from `DistributedTlsConfig` into
    /// each `Endpoint::tls_config` call.
    #[derive(Clone)]
    pub struct TlsChannelResolver {
        client_tls: ClientTlsConfig,
        // tonic `Channel` is cheap to clone (it's an Arc inside).
        // Cache keyed by URL so we pay the TLS handshake once per
        // peer regardless of method-call volume.
        cache: Arc<Mutex<HashMap<Url, Channel>>>,
    }

    impl TlsChannelResolver {
        /// Eagerly load the CA + (optional) client identity from
        /// disk. Failing here means the operator's TLS paths are
        /// wrong; the error propagates to the caller (`build_context`)
        /// which turns it into a backend-construction failure.
        pub fn new(cfg: DistributedTlsConfig) -> Result<Self, BackendError> {
            let ca_bytes = std::fs::read(&cfg.ca_cert_pem_path).map_err(|e| {
                BackendError::Other(format!("read ca cert {:?}: {e}", cfg.ca_cert_pem_path))
            })?;
            let mut client_tls =
                ClientTlsConfig::new().ca_certificate(Certificate::from_pem(ca_bytes));
            if let Some(domain) = cfg.domain_name_override.as_ref() {
                client_tls = client_tls.domain_name(domain.clone());
            }
            if let Some(id) = cfg.client_identity.as_ref() {
                let cert = std::fs::read(&id.cert_pem_path).map_err(|e| {
                    BackendError::Other(format!("read client cert {:?}: {e}", id.cert_pem_path))
                })?;
                let key = std::fs::read(&id.key_pem_path).map_err(|e| {
                    BackendError::Other(format!("read client key {:?}: {e}", id.key_pem_path))
                })?;
                client_tls = client_tls.identity(Identity::from_pem(cert, key));
            }
            // Static-membership clusters have a small fixed peer
            // count, so a simple HashMap suffices. Dynamic-membership
            // clusters (k8s pod churn) would warrant a TTI-evicting
            // cache like upstream's `DefaultChannelResolver`; that's
            // a follow-up if it becomes a problem in practice.
            Ok(Self {
                client_tls,
                cache: Arc::new(Mutex::new(HashMap::new())),
            })
        }

        async fn build_channel(&self, url: &Url) -> Result<Channel, DataFusionError> {
            let endpoint = Channel::from_shared(url.to_string()).map_err(|e| {
                DataFusionError::Configuration(format!(
                    "TlsChannelResolver: invalid peer URL {url}: {e}"
                ))
            })?;
            let endpoint = endpoint.tls_config(self.client_tls.clone()).map_err(|e| {
                DataFusionError::Configuration(format!(
                    "TlsChannelResolver: tls_config for {url}: {e}"
                ))
            })?;
            endpoint.connect().await.map_err(|e| {
                DataFusionError::Execution(format!("TlsChannelResolver: connect to {url}: {e}"))
            })
        }
    }

    #[async_trait]
    impl ChannelResolver for TlsChannelResolver {
        async fn get_worker_client_for_url(
            &self,
            url: &Url,
        ) -> Result<WorkerServiceClient<BoxCloneSyncChannel>, DataFusionError> {
            // Cache lookup is cheap; the slow path is only taken on
            // the first call per URL.
            if let Some(channel) = self
                .cache
                .lock()
                .expect("TlsChannelResolver cache mutex poisoned")
                .get(url)
                .cloned()
            {
                return Ok(create_worker_client(BoxCloneSyncChannel::new(channel)));
            }
            let channel = self.build_channel(url).await?;
            self.cache
                .lock()
                .expect("TlsChannelResolver cache mutex poisoned")
                .insert(url.clone(), channel.clone());
            Ok(create_worker_client(BoxCloneSyncChannel::new(channel)))
        }
    }
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
            tls: None,
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
            tls: None,
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
            tls: None,
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

    /// Σ.B PR 2 commit 5: DistributedSqlTransform end-to-end.
    /// Empty-peers degenerate cluster; tests run plain DataFusion
    /// locally through the trait surface. Real cross-pod execution
    /// is exercised by the docker-compose integration test in a
    /// follow-up commit.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn distributed_sql_transform_runs_locally() {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use datafusion::arrow::record_batch::RecordBatch;
        use ematix_flow_core::transform::{BatchContext, BatchTransform};
        use std::sync::Arc;

        let xform = DistributedSqlTransform::open(
            "SELECT n * 2 AS doubled FROM source",
            DistributedConfig::default(),
        )
        .unwrap();

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "n",
            DataType::Int64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();

        let ctx = BatchContext::default();
        let out = xform.transform(batch, &ctx).await.expect("run");
        assert_eq!(out.len(), 1);
        let arr = out[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64");
        assert_eq!(arr.values(), &[2, 4, 6]);

        // Schemas now cached.
        assert!(xform.schemas.initialized());
        assert_eq!(xform.input_schema().fields().len(), 1);
        assert_eq!(xform.output_schema().fields()[0].name(), "doubled");
    }

    /// Σ.B follow-up: lookups visible to the SQL on the
    /// coordinator's session. Empty-peers degenerate cluster keeps
    /// the test offline; the cross-pod variant lives in
    /// `tests/cross_pod.rs`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn distributed_sql_transform_supports_lookups_on_coordinator() {
        use datafusion::arrow::array::{Int64Array, StringArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use datafusion::arrow::record_batch::RecordBatch;
        use ematix_flow_core::transform::{BatchContext, BatchTransform, LookupTable};
        use std::sync::Arc;

        // Lookup: 3 country rows.
        let lookup_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("country", DataType::Utf8, false),
        ]));
        let lookup_batch = RecordBatch::try_new(
            lookup_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["US", "UK", "JP"])),
            ],
        )
        .unwrap();
        let lookup = LookupTable {
            name: "users".into(),
            schema: lookup_schema,
            batches: vec![lookup_batch],
        };

        // Source: 3 user_id rows.
        let source_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "user_id",
            DataType::Int64,
            false,
        )]));
        let source_batch = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        // Transform joins source × lookup on user_id = id.
        let xform = DistributedSqlTransform::open_with_lookups(
            "SELECT s.user_id, u.country FROM source s JOIN users u ON s.user_id = u.id ORDER BY s.user_id",
            DistributedConfig::default(),
            vec![lookup],
        )
        .unwrap();

        let result = xform
            .transform(source_batch, &BatchContext::default())
            .await
            .expect("run");
        assert_eq!(result.len(), 1, "single batch");
        let countries = result[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8");
        assert_eq!(countries.value(0), "US");
        assert_eq!(countries.value(1), "UK");
        assert_eq!(countries.value(2), "JP");
    }

    /// Schema accessors panic before first transform — same lazy-
    /// init contract as LazySqlTransform.
    #[test]
    #[should_panic(expected = "before first transform")]
    fn distributed_sql_transform_schema_panics_before_first_call() {
        let xform =
            DistributedSqlTransform::open("SELECT * FROM source", DistributedConfig::default())
                .unwrap();
        let _ = xform.input_schema();
    }

    /// Σ.B PR 2 commit 2: full round-trip through the shared
    /// `BackendConfig` enum. Construct → `.config()` → serialize →
    /// deserialize → construct again → assert peers match.
    #[test]
    fn config_method_round_trips_through_serde() {
        let original = DistributedBackend::open(DistributedConfig {
            peers: vec!["http://a:50051".into(), "http://b:50051".into()],
            tls: None,
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
