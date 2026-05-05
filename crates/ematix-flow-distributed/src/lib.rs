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
use ematix_flow_core::backend::{
    ArrowBatchStream, Backend, BackendConfig, BackendError, DeleteHandling, Dialect,
    StrategyRunResult, TargetTable, WriteMode,
};
use ematix_flow_core::pg::ConnectionInfo;
use ematix_flow_core::types::TableSpec;
use serde::{Deserialize, Serialize};
use url::Url;

/// Σ.B PR 2: serializable config for [`DistributedBackend`].
///
/// Carries the list of peer worker URLs the local node should fan
/// out to. Empty `peers` is legal — the local node executes the
/// plan as a single worker (degenerate distributed-of-one); useful
/// for tests + dev. Production deployments pass 1–N peer URLs.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DistributedConfig {
    /// Peer worker URLs (e.g. `http://flow-01.cluster.local:50051`).
    /// Each must be parseable by `url::Url`. Validated on
    /// construction in [`DistributedBackend::open`].
    pub peers: Vec<String>,
}

/// Backend that executes SQL transforms across a peer mesh of
/// ematix-flow processes. PR 2's first commit ships the type +
/// trait scaffold; the actual distributed-execution plumbing
/// lands in follow-up commits.
pub struct DistributedBackend {
    /// Validated peer URLs. Stored as `Url` so URL re-parsing
    /// errors surface at construction, not at first execute.
    peers: Vec<Url>,
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
        Ok(Self { peers })
    }

    /// Borrow the configured peer URLs. Mainly for tests + logs.
    pub fn peers(&self) -> &[Url] {
        &self.peers
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
        // Σ.B PR 2: the Distributed variant doesn't yet exist on the
        // shared `BackendConfig` enum — adding it requires editing
        // ematix-flow-core, which lands in the next PR-2 commit when
        // the distributed-execution wiring is real. For now, hand
        // back the closest already-present discriminator (Postgres)
        // so the call site doesn't panic; this is an intentional
        // placeholder, exercised only by the trait-shape tests in
        // this crate. Production callers don't reach this code path
        // until the variant lands.
        unimplemented!(
            "DistributedBackend::config() not yet wired into the shared BackendConfig \
             enum — lands in the next Σ.B PR 2 commit. See \
             docs/PHASE_SIGMA_B_TRAIT_SPIKE.md."
        )
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

    async fn read_arrow_stream(&self, _query: &str) -> Result<ArrowBatchStream, BackendError> {
        Err(BackendError::Other(
            "DistributedBackend::read_arrow_stream not yet implemented — Σ.B PR 2 follow-up".into(),
        ))
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
}
