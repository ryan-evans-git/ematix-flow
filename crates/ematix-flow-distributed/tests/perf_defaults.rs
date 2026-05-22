//! Σ.J — regression guards locking in the perf defaults that ship with
//! DataFusion 53 and `datafusion-distributed`. These are cheap, distributed-
//! safe levers (LZ4 Flight compression, dynamic-filter pushdown from join
//! build-side to probe scan, small-batch coalescing) that materially help
//! the peer-mesh shuffle path and don't affect single-node runs.
//!
//! These tests fail loudly if a future upgrade — or a stray
//! `SessionConfig::set` somewhere in our code — quietly drops one of them.

use ematix_flow_core::backend::DistributedConfig;
use ematix_flow_distributed::DistributedBackend;

/// Build a session context the same way `DistributedBackend` does in
/// production with a non-empty peer list, so we exercise the
/// `DistributedPhysicalOptimizerRule` + LZ4 compression path (not the
/// `peers.is_empty()` degenerate branch).
async fn distributed_session() -> std::sync::Arc<datafusion::prelude::SessionContext> {
    let cfg = DistributedConfig {
        peers: vec!["http://localhost:50051".to_string()],
        tls: None,
    };
    let backend = DistributedBackend::open(cfg).expect("DistributedBackend::open");
    backend.session_context().await.clone()
}

#[tokio::test]
async fn dynamic_filter_pushdown_defaults_on() {
    let ctx = distributed_session().await;
    let opts = ctx.state().config_options().optimizer.clone();
    assert!(
        opts.enable_dynamic_filter_pushdown,
        "enable_dynamic_filter_pushdown must stay on — it powers the \
         hash-join build→probe-scan range filter that's our cheapest \
         distributed win"
    );
    assert!(
        opts.enable_join_dynamic_filter_pushdown,
        "enable_join_dynamic_filter_pushdown must stay on — controls \
         the per-join switch under enable_dynamic_filter_pushdown"
    );
    assert!(
        opts.enable_topk_dynamic_filter_pushdown,
        "enable_topk_dynamic_filter_pushdown must stay on"
    );
    assert!(
        opts.enable_aggregate_dynamic_filter_pushdown,
        "enable_aggregate_dynamic_filter_pushdown must stay on"
    );
}

#[tokio::test]
async fn coalesce_batches_defaults_on() {
    let ctx = distributed_session().await;
    let exec = ctx.state().config_options().execution.clone();
    assert!(
        exec.coalesce_batches,
        "coalesce_batches must stay on — small batches get merged before \
         RepartitionExec / Flight shuffle, otherwise per-batch sync \
         overhead eats the distributed budget"
    );
    assert!(
        exec.batch_size >= 4096,
        "batch_size {} too small for distributed shuffle — Flight per-batch \
         overhead would dominate. DataFusion's default is 8192.",
        exec.batch_size
    );
}

/// The compression pin lives in `build_context` (see `lib.rs`). There's no
/// public getter on the `DistributedExt` trait for the configured compression,
/// so we verify the pin is in the source code itself.
#[test]
fn lz4_frame_compression_pinned_in_source() {
    let src = include_str!("../src/lib.rs");
    assert!(
        src.contains("with_distributed_compression(Some(CompressionType::LZ4_FRAME))"),
        "Σ.J — LZ4_FRAME compression must remain explicitly pinned in \
         DistributedBackend::build_context. The datafusion-distributed \
         default matches it today, but pinning prevents a silent drop on \
         dep upgrade."
    );
}
