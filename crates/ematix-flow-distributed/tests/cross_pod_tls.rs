//! Σ.B follow-up: end-to-end TLS test for the worker mesh.
//!
//! Generates a self-signed CA + server cert (SAN: localhost +
//! 127.0.0.1) at runtime via rcgen, writes them to a tempdir,
//! launches a tonic worker that terminates TLS via `ServerTlsConfig`,
//! and runs a SQL query through `DistributedSqlTransform` whose
//! coordinator is configured with `DistributedTlsConfig` pointing at
//! the same CA. Confirms the full coordinator → TLS handshake →
//! peer-worker dispatch → result-stream-back path actually works
//! end-to-end without any pre-built PEM fixtures.
//!
//! No Docker dependency. No rustls knobs in the test surface beyond
//! what rcgen + tonic already expose. The test is hermetic — it
//! starts and tears down the worker in-process per run.

use std::net::SocketAddr;
use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion_distributed::{DefaultSessionBuilder, Worker};
use ematix_flow_core::backend::{
    DistributedClientIdentityConfig, DistributedConfig, DistributedTlsConfig,
};
use ematix_flow_core::transform::{BatchContext, BatchTransform};
use ematix_flow_distributed::tls::load_server_tls_config;
use ematix_flow_distributed::{DistributedBackend, DistributedSqlTransform};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

/// Materialize a CA + leaf cert pair on disk inside `dir`. Leaf has
/// `localhost` + `127.0.0.1` as SANs so tonic's hostname check is
/// satisfied by either form. Returns the absolute paths the worker
/// + coordinator can read.
struct PemPaths {
    ca_pem: std::path::PathBuf,
    server_cert_pem: std::path::PathBuf,
    server_key_pem: std::path::PathBuf,
    client_cert_pem: std::path::PathBuf,
    client_key_pem: std::path::PathBuf,
}

fn issue_ca() -> (rcgen::Certificate, Issuer<'static, KeyPair>) {
    let mut params = CertificateParams::new(Vec::<String>::new()).expect("ca params");
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(DnType::CommonName, "ematix-flow-test-ca");
    params.key_usages.push(KeyUsagePurpose::KeyCertSign);
    params.key_usages.push(KeyUsagePurpose::DigitalSignature);
    let key = KeyPair::generate().expect("ca keypair");
    let cert = params.self_signed(&key).expect("self-sign ca");
    (cert, Issuer::new(params, key))
}

fn issue_leaf(
    issuer: &Issuer<'static, KeyPair>,
    common_name: &str,
    sans: Vec<SanType>,
    is_client: bool,
) -> (rcgen::Certificate, KeyPair) {
    let mut params = CertificateParams::new(Vec::<String>::new()).expect("leaf params");
    params.subject_alt_names = sans;
    params
        .distinguished_name
        .push(DnType::CommonName, common_name);
    params.use_authority_key_identifier_extension = true;
    params.key_usages.push(KeyUsagePurpose::DigitalSignature);
    params.extended_key_usages.push(if is_client {
        ExtendedKeyUsagePurpose::ClientAuth
    } else {
        ExtendedKeyUsagePurpose::ServerAuth
    });
    let key = KeyPair::generate().expect("leaf keypair");
    let cert = params.signed_by(&key, issuer).expect("sign leaf");
    (cert, key)
}

fn materialize_pems(dir: &TempDir) -> PemPaths {
    let (ca_cert, ca_issuer) = issue_ca();
    let (server_cert, server_key) = issue_leaf(
        &ca_issuer,
        "localhost",
        vec![
            SanType::DnsName("localhost".try_into().unwrap()),
            SanType::IpAddress("127.0.0.1".parse().unwrap()),
        ],
        false,
    );
    let (client_cert, client_key) =
        issue_leaf(&ca_issuer, "ematix-flow-coordinator", Vec::new(), true);

    let ca_pem = dir.path().join("ca.pem");
    let server_cert_pem = dir.path().join("server.pem");
    let server_key_pem = dir.path().join("server.key");
    let client_cert_pem = dir.path().join("client.pem");
    let client_key_pem = dir.path().join("client.key");

    std::fs::write(&ca_pem, ca_cert.pem()).unwrap();
    std::fs::write(&server_cert_pem, server_cert.pem()).unwrap();
    std::fs::write(&server_key_pem, server_key.serialize_pem()).unwrap();
    std::fs::write(&client_cert_pem, client_cert.pem()).unwrap();
    std::fs::write(&client_key_pem, client_key.serialize_pem()).unwrap();

    PemPaths {
        ca_pem,
        server_cert_pem,
        server_key_pem,
        client_cert_pem,
        client_key_pem,
    }
}

/// Spawn a single TLS-terminating worker on a free localhost port.
/// Returns the `https://127.0.0.1:<port>` URL and a handle whose
/// `.abort_all` shuts the worker down.
async fn spawn_tls_worker(pems: &PemPaths, require_client_certs: bool) -> (String, JoinSet<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let local: SocketAddr = listener.local_addr().expect("local_addr");
    let url = format!("https://127.0.0.1:{}", local.port());

    let server_tls = load_server_tls_config(
        pems.server_cert_pem.to_str().unwrap(),
        pems.server_key_pem.to_str().unwrap(),
        if require_client_certs {
            Some(pems.ca_pem.to_str().unwrap())
        } else {
            None
        },
    )
    .expect("load server tls");

    let mut join_set = JoinSet::new();
    join_set.spawn(async move {
        let worker = Worker::from_session_builder(DefaultSessionBuilder);
        let incoming = TcpListenerStream::new(listener);
        let _ = Server::builder()
            .tls_config(server_tls)
            .expect("server tls_config")
            .add_service(worker.into_worker_server())
            .serve_with_incoming(incoming)
            .await;
    });
    // Yield so the worker is accepting before the coordinator dials.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    (url, join_set)
}

/// Server-auth-only TLS: coordinator verifies worker cert against
/// the CA, but the worker accepts any client. Mirrors the simplest
/// production posture (workers in a trusted zone, encrypted on the
/// wire).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_sql_transform_runs_over_server_auth_tls() {
    let dir = TempDir::new().expect("tempdir");
    let pems = materialize_pems(&dir);
    let (peer_url, mut workers) = spawn_tls_worker(&pems, false).await;

    let cfg = DistributedConfig {
        peers: vec![peer_url],
        tls: Some(DistributedTlsConfig {
            ca_cert_pem_path: pems.ca_pem.to_str().unwrap().to_string(),
            client_identity: None,
            // Peer URL uses 127.0.0.1; cert SAN includes both
            // `localhost` and `127.0.0.1`, so we don't strictly
            // need an override — leaving `None` exercises the
            // host-from-URL default path.
            domain_name_override: None,
        }),
    };
    let backend = Arc::new(DistributedBackend::open(cfg).expect("backend"));

    // Register a small source on the coordinator's session so the
    // distributed plan has data to fan to the peer.
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from((1..=4).collect::<Vec<_>>()))],
    )
    .unwrap();
    let mem = MemTable::try_new(schema, vec![vec![batch.clone()]]).unwrap();
    let ctx = backend.session_context().await.clone();
    ctx.register_table("source", Arc::new(mem))
        .expect("register");

    let xform = DistributedSqlTransform::new("SELECT SUM(n) AS total FROM source", backend);
    let result = xform
        .transform(batch, &BatchContext::default())
        .await
        .expect("transform over TLS");

    assert_eq!(result.len(), 1);
    let arr = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64");
    assert_eq!(arr.value(0), 10, "SUM(1..4) = 10 over TLS");

    workers.abort_all();
}

/// Mutual TLS: worker also requires the coordinator's client cert
/// to chain back to the CA. This is the production-grade posture
/// for clusters reachable from outside the trusted zone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_sql_transform_runs_over_mtls() {
    let dir = TempDir::new().expect("tempdir");
    let pems = materialize_pems(&dir);
    let (peer_url, mut workers) = spawn_tls_worker(&pems, true).await;

    let cfg = DistributedConfig {
        peers: vec![peer_url],
        tls: Some(DistributedTlsConfig {
            ca_cert_pem_path: pems.ca_pem.to_str().unwrap().to_string(),
            client_identity: Some(DistributedClientIdentityConfig {
                cert_pem_path: pems.client_cert_pem.to_str().unwrap().to_string(),
                key_pem_path: pems.client_key_pem.to_str().unwrap().to_string(),
            }),
            domain_name_override: None,
        }),
    };
    let backend = Arc::new(DistributedBackend::open(cfg).expect("backend"));

    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from((1..=5).collect::<Vec<_>>()))],
    )
    .unwrap();
    let mem = MemTable::try_new(schema, vec![vec![batch.clone()]]).unwrap();
    let ctx = backend.session_context().await.clone();
    ctx.register_table("source", Arc::new(mem))
        .expect("register");

    let xform = DistributedSqlTransform::new("SELECT SUM(n) AS total FROM source", backend);
    let result = xform
        .transform(batch, &BatchContext::default())
        .await
        .expect("transform over mTLS");

    let arr = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64");
    assert_eq!(arr.value(0), 15, "SUM(1..5) = 15 over mTLS");

    workers.abort_all();
}
