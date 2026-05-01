//! CLI.2: Prometheus `/metrics` HTTP endpoint.
//!
//! Tiny axum server that exposes a [`prometheus::Registry`] over
//! HTTP. Used by the `flow consume` subcommand when
//! `--metrics-port <PORT>` is set; the binary spawns the server
//! alongside the pipeline and both share a shutdown signal so they
//! stop together.
//!
//! The endpoint surface is intentionally minimal: one route,
//! `GET /metrics`, returning the standard text/plain
//! Prometheus-exposition format. Other observability concerns
//! (health probes, pprof, dynamic log-level toggles) are
//! follow-ups.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;
use ematix_flow_core::streaming::ShutdownSignal;
use prometheus::{Encoder, Registry, TextEncoder};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tracing::{info, warn};

/// Bind a `TcpListener` on `addr` and serve the metrics endpoint
/// in a background task. Returns a `(SocketAddr, JoinHandle<()>)`
/// pair — the resolved listen address (handy when port `0` is
/// passed for an ephemeral port) and a join handle for the server
/// task. The server stops when `shutdown` triggers or when the
/// returned handle is awaited and aborted.
///
/// The Prometheus `Registry` is shared by reference (via Arc).
/// Callers typically clone the pipeline's
/// `metrics.registry.clone()` and hand it here.
pub async fn spawn_metrics_server(
    addr: SocketAddr,
    registry: Arc<Registry>,
    shutdown: ShutdownSignal,
) -> std::io::Result<(SocketAddr, JoinHandle<()>)> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    info!(addr = %bound, "metrics server listening");

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(registry);

    let handle = tokio::spawn(async move {
        let serve = axum::serve(listener, app).with_graceful_shutdown(async move {
            shutdown.wait().await;
        });
        if let Err(e) = serve.await {
            warn!(error = %e, "metrics server exited with error");
        }
    });
    Ok((bound, handle))
}

async fn metrics_handler(State(registry): State<Arc<Registry>>) -> impl IntoResponse {
    let metric_families = registry.gather();
    let encoder = TextEncoder::new();
    let mut buf = Vec::with_capacity(4096);
    if let Err(e) = encoder.encode(&metric_families, &mut buf) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("metrics encode error: {e}"),
        )
            .into_response();
    }
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; version=0.0.4"),
        )],
        buf,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ematix_flow_core::streaming::ShutdownSignal;
    use prometheus::IntCounter;

    #[tokio::test(flavor = "multi_thread")]
    async fn metrics_endpoint_exposes_registered_counter() {
        let registry = Arc::new(Registry::new());
        let counter = IntCounter::new("ematix_test_counter", "test counter").unwrap();
        registry.register(Box::new(counter.clone())).unwrap();
        counter.inc_by(3);

        let (signal, trigger) = ShutdownSignal::new();
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let (bound, handle) = spawn_metrics_server(addr, registry, signal).await.unwrap();

        // Tiny inline HTTP/1.1 client — see http_get below. Avoids
        // pulling reqwest in for what amounts to one request per
        // test.
        let body = http_get(&bound, "/metrics").await;
        assert!(
            body.contains("ematix_test_counter 3"),
            "expected counter line in body; got:\n{body}"
        );
        assert!(
            body.contains("# HELP ematix_test_counter"),
            "expected HELP line; got:\n{body}"
        );

        trigger.trigger();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn metrics_endpoint_returns_empty_body_for_empty_registry() {
        // An empty registry yields a 200 with empty body. Prometheus
        // is happy with that.
        let registry = Arc::new(Registry::new());
        let (signal, trigger) = ShutdownSignal::new();
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let (bound, handle) = spawn_metrics_server(addr, registry, signal).await.unwrap();

        let body = http_get(&bound, "/metrics").await;
        assert!(body.is_empty(), "expected empty body; got:\n{body}");

        trigger.trigger();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
    }

    /// Minimal HTTP/1.1 GET helper. Returns the response body. We
    /// avoid pulling reqwest into the test deps for what amounts
    /// to one request per test.
    async fn http_get(addr: &SocketAddr, path: &str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        let raw = String::from_utf8_lossy(&buf).to_string();
        // Strip status line + headers; body follows the first \r\n\r\n.
        match raw.split_once("\r\n\r\n") {
            Some((_, body)) => body.to_string(),
            None => raw,
        }
    }
}
