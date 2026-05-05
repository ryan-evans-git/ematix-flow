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
//!
//! Defaults: `--bind 0.0.0.0` `--port 50051`.
//!
//! Deployment: see `examples/distributed-cluster/` for a docker-
//! compose stack with N peer workers + a coordinator.

use std::env;
use std::error::Error;
use std::net::SocketAddr;

use datafusion_distributed::{DefaultSessionBuilder, Worker};
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut bind_addr = "0.0.0.0".to_string();
    let mut port: u16 = 50051;

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
            "-h" | "--help" => {
                println!(
                    "flow-worker — distributed batch SQL worker for ematix-flow.\n\
                     \n\
                     Usage: flow-worker [--port PORT] [--bind ADDR]\n\
                     \n\
                     Defaults: --bind 0.0.0.0 --port 50051"
                );
                return Ok(());
            }
            other => {
                eprintln!("unknown argument: {other}");
                std::process::exit(2);
            }
        }
    }

    let addr: SocketAddr = format!("{bind_addr}:{port}").parse()?;
    let worker = Worker::from_session_builder(DefaultSessionBuilder);

    println!("flow-worker listening on http://{addr}");
    Server::builder()
        .add_service(worker.into_worker_server())
        .serve(addr)
        .await?;
    Ok(())
}
