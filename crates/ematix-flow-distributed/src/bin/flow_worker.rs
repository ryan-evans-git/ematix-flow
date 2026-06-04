//! Σ.B PR 3: `flow-worker` — peer worker for distributed batch SQL.
//!
//! Hosts `datafusion_distributed::Worker::into_worker_server()` over
//! tonic on a configurable port. A coordinator process running
//! `DistributedSqlTransform` (or `DistributedBackend::read_arrow_stream`
//! directly) with `peers = ["http://this-worker:port", ...]` will
//! fan stage-level work out to instances of this binary.
//!
//! Usage:
//!     flow-worker [--port PORT] [--bind ADDR]
//!                 [--tls-cert PATH --tls-key PATH [--tls-client-ca PATH]]
//!
//! Defaults: `--bind 0.0.0.0` `--port 50051`, plain HTTP.
//!
//! Σ.B follow-up: pass `--tls-cert` + `--tls-key` (both required
//! together) to terminate TLS at the worker. Adding `--tls-client-ca`
//! switches to mTLS — only clients presenting a cert chain signed
//! by that CA are accepted. The coordinator's matching knobs live
//! in `DistributedTlsConfig` (carried in `BackendConfig`).
//!
//! Deployment: see `examples/distributed-cluster/` for a docker-
//! compose stack with N peer workers + a coordinator.

use std::env;
use std::error::Error;
use std::net::SocketAddr;

use datafusion_distributed::Worker;
use ematix_flow_distributed::bloom_flight::default_bloom_session_builder;
use ematix_flow_distributed::tls::load_server_tls_config;
use tonic::transport::Server;

/// Run the distributed worker on mimalloc, matching the benchmark harness and
/// the other production binaries. The worker executes query-plan stages on its
/// peers; the system allocator is ~10% slower on join/alloc-heavy stages.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(test)]
mod alloc_guard {
    // The shipped `flow-worker` binary must run on mimalloc, not the system
    // allocator. `mi_is_in_heap_region` returns false for a system-malloc
    // pointer, so this is RED without the `#[global_allocator]` above.
    #[test]
    fn global_allocator_is_mimalloc() {
        let buf: Vec<u8> = vec![0xA5u8; 64 * 1024];
        let p = buf.as_ptr() as *const core::ffi::c_void;
        let in_mimalloc = unsafe { libmimalloc_sys::mi_is_in_heap_region(p) };
        core::hint::black_box(&buf);
        assert!(
            in_mimalloc,
            "global allocator is NOT mimalloc — `flow-worker` would ship on the system allocator"
        );
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut bind_addr = "0.0.0.0".to_string();
    let mut port: u16 = 50051;
    let mut tls_cert: Option<String> = None;
    let mut tls_key: Option<String> = None;
    let mut tls_client_ca: Option<String> = None;

    let args: Vec<String> = env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--port" => {
                port = args
                    .get(i + 1)
                    .ok_or("--port needs a value")?
                    .parse()
                    .map_err(|e| format!("--port: {e}"))?;
                i += 2;
            }
            "--bind" => {
                bind_addr = args.get(i + 1).ok_or("--bind needs an addr")?.clone();
                i += 2;
            }
            "--tls-cert" => {
                tls_cert = Some(args.get(i + 1).ok_or("--tls-cert needs a path")?.clone());
                i += 2;
            }
            "--tls-key" => {
                tls_key = Some(args.get(i + 1).ok_or("--tls-key needs a path")?.clone());
                i += 2;
            }
            "--tls-client-ca" => {
                tls_client_ca = Some(
                    args.get(i + 1)
                        .ok_or("--tls-client-ca needs a path")?
                        .clone(),
                );
                i += 2;
            }
            "-h" | "--help" => {
                println!(
                    "flow-worker — distributed batch SQL worker for ematix-flow.\n\
                     \n\
                     Usage: flow-worker [--port PORT] [--bind ADDR]\n\
                                       [--tls-cert PATH --tls-key PATH \
                                        [--tls-client-ca PATH]]\n\
                     \n\
                     Defaults: --bind 0.0.0.0 --port 50051, plain HTTP.\n\
                     \n\
                     TLS: --tls-cert + --tls-key both required to enable.\n\
                       --tls-client-ca additionally requires mTLS (clients \
                     present a cert)."
                );
                return Ok(());
            }
            other => {
                eprintln!("unknown argument: {other}");
                std::process::exit(2);
            }
        }
    }

    // Cross-validate: cert/key must come together. Client-CA without
    // server cert/key is meaningless (mTLS layers on top of TLS).
    let server_tls = match (tls_cert.as_deref(), tls_key.as_deref()) {
        (None, None) => {
            if tls_client_ca.is_some() {
                return Err(
                    "--tls-client-ca requires --tls-cert + --tls-key (mTLS layers on TLS)".into(),
                );
            }
            None
        }
        (Some(_), None) | (None, Some(_)) => {
            return Err("--tls-cert and --tls-key must be set together".into());
        }
        (Some(cert), Some(key)) => {
            Some(load_server_tls_config(cert, key, tls_client_ca.as_deref())?)
        }
    };

    let addr: SocketAddr = format!("{bind_addr}:{port}").parse()?;
    // Σ.J.2.b.viii — wrap the default session builder so inbound
    // `x-ematix-bloom-*` headers automatically install the
    // EnableContextBloomRule on the per-request SessionState. No-op
    // for queries without inbound blooms.
    let worker = Worker::from_session_builder(default_bloom_session_builder());
    let scheme = if server_tls.is_some() {
        "https"
    } else {
        "http"
    };
    println!("flow-worker listening on {scheme}://{addr}");

    let mut builder = Server::builder();
    if let Some(tls_cfg) = server_tls {
        builder = builder.tls_config(tls_cfg)?;
    }
    builder
        .add_service(worker.into_worker_server())
        .serve(addr)
        .await?;
    Ok(())
}
