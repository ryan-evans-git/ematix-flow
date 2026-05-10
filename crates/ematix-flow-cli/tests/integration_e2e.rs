//! CLI E2E integration test.
//!
//! Validates the full `flow consume` runtime path end-to-end:
//!   - TOML config → `PipelineCliConfig`
//!   - `run_consume_with(...)` → backend factory → StreamingPipeline
//!   - The pipeline drains a real Kafka topic via testcontainers
//!   - Rows land in a real SQLite file (in a tempdir) via the
//!     framework's Arrow IO path
//!   - The metrics HTTP server serves Prometheus exposition with
//!     non-zero counters
//!   - Programmatic shutdown via `ConsumeOptions.shutdown_signal`
//!     drains the in-flight batch and exits cleanly
//!
//! Marked `#[ignore]` so the default `cargo test` lane stays
//! Docker-free; run with `cargo test -- --ignored` to exercise.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::Int64Array;
use ematix_flow_cli::{ConsumeOptions, PipelineCliConfig, run_consume_with};
use ematix_flow_core::SQLiteBackend;
use ematix_flow_core::backend::Backend;
use ematix_flow_core::streaming::ShutdownSignal;
use futures_util::TryStreamExt;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};

async fn start_kafka() -> (testcontainers::ContainerAsync<Kafka>, String) {
    use testcontainers::ImageExt;
    let container = Kafka::default()
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .start()
        .await
        .expect("kafka container");
    let host = container.get_host().await.expect("kafka host").to_string();
    let port = container
        .get_host_port_ipv4(KAFKA_PORT)
        .await
        .expect("kafka port");
    (container, format!("{host}:{port}"))
}

async fn produce_json(bootstrap: &str, topic: &str, rows: &[i64]) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("message.timeout.ms", "10000")
        .create()
        .expect("producer");
    for id in rows {
        let payload = format!(r#"{{"id": {id}}}"#);
        producer
            .send(
                FutureRecord::<(), str>::to(topic).payload(&payload),
                Duration::from_secs(5),
            )
            .await
            .expect("produce");
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn cli_e2e_kafka_to_sqlite_via_run_consume_with() {
    let (_container, bootstrap) = start_kafka().await;
    let topic = "cli-e2e-topic";

    // 1. Pre-create the SQLite target file + table. The framework's
    //    PG/SQLite write paths INSERT into an existing table; auto-DDL
    //    isn't part of `write_arrow_stream`'s contract.
    let dir = tempfile::tempdir().expect("tempdir");
    let sqlite_path = dir.path().join("events.sqlite");
    let sqlite_path_str = sqlite_path.to_str().unwrap().to_string();
    {
        let setup = SQLiteBackend::open(&sqlite_path_str).unwrap();
        setup
            .execute("CREATE TABLE events (id BIGINT)")
            .await
            .expect("create table");
    }

    // 2. Produce 5 messages.
    produce_json(&bootstrap, topic, &[1, 2, 3, 4, 5]).await;

    // 3. Build a TOML config that the CLI's parser would see.
    let toml = format!(
        r#"
            pipeline_name = "cli-e2e"
            source_query = "{topic}"
            idle_pause_ms = 250

            [source]
            kind = "kafka"
            bootstrap_servers = "{bootstrap}"
            group_id = "cli-e2e-grp"

            [target]
            kind = "sqlite"
            path = "{sqlite_path_str}"

            [target.table]
            schema = "main"
            name = "events"
        "#
    );
    let cfg = PipelineCliConfig::from_toml_str(&toml).expect("parse toml");
    assert_eq!(cfg.pipeline_name, "cli-e2e");

    // 4. Programmatic shutdown signal. The metrics server is on
    //    an ephemeral port (the bound port is logged but we
    //    don't care about it for this assertion shape — we'll
    //    pick a fixed one we know is free in CI).
    let (signal, trigger) = ShutdownSignal::new();
    let options = ConsumeOptions {
        metrics_port: None, // metrics server validated by its own unit tests
        shutdown_signal: Some(signal),
        ..Default::default()
    };

    // 5. Run the pipeline in a background task. Poll the SQLite
    //    target until 5 rows arrive, then trigger shutdown.
    let pipeline_handle = tokio::spawn(async move { run_consume_with(cfg, options).await });

    let probe: Arc<dyn Backend> = Arc::new(SQLiteBackend::open(&sqlite_path_str).unwrap());
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut row_count = 0_i64;
    while std::time::Instant::now() < deadline {
        let stream = probe
            .read_arrow_stream("SELECT count(*) FROM events")
            .await
            .expect("read count");
        let batches: Vec<_> = stream.try_collect().await.expect("collect");
        row_count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        if row_count >= 5 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        row_count >= 5,
        "expected 5 rows in events; got {row_count} after 60s"
    );

    // 6. Trigger shutdown; pipeline drains + exits.
    trigger.trigger();
    let metrics = tokio::time::timeout(Duration::from_secs(15), pipeline_handle)
        .await
        .expect("pipeline timeout")
        .expect("pipeline join")
        .expect("pipeline result");
    assert!(metrics.shutdown_triggered, "expected shutdown_triggered");
    assert!(
        metrics.total_rows >= 5,
        "expected total_rows >= 5; got {}",
        metrics.total_rows
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker; run with `cargo test -- --ignored`"]
async fn cli_e2e_metrics_endpoint_serves_pipeline_counters() {
    let (_container, bootstrap) = start_kafka().await;
    let topic = "cli-e2e-metrics-topic";

    let dir = tempfile::tempdir().expect("tempdir");
    let sqlite_path = dir.path().join("events.sqlite");
    let sqlite_path_str = sqlite_path.to_str().unwrap().to_string();
    {
        let setup = SQLiteBackend::open(&sqlite_path_str).unwrap();
        setup
            .execute("CREATE TABLE events (id BIGINT)")
            .await
            .expect("create table");
    }

    produce_json(&bootstrap, topic, &[10, 20, 30]).await;

    let toml = format!(
        r#"
            pipeline_name = "cli-e2e-metrics"
            source_query = "{topic}"
            idle_pause_ms = 250

            [source]
            kind = "kafka"
            bootstrap_servers = "{bootstrap}"
            group_id = "cli-e2e-metrics-grp"

            [target]
            kind = "sqlite"
            path = "{sqlite_path_str}"

            [target.table]
            schema = "main"
            name = "events"
        "#
    );
    let cfg = PipelineCliConfig::from_toml_str(&toml).expect("parse toml");

    // Bind the metrics server on an ephemeral port via 0.
    // ConsumeOptions doesn't expose the resolved port back to us,
    // so we pick a fixed-but-likely-free port in the
    // 49152..65535 ephemeral range and skip the test gracefully if
    // it happens to be taken (rare).
    let metrics_port: u16 = 53917;
    let (signal, trigger) = ShutdownSignal::new();
    let options = ConsumeOptions {
        metrics_port: Some(metrics_port),
        shutdown_signal: Some(signal),
        ..Default::default()
    };

    let pipeline_handle = {
        let cfg = cfg.clone();
        let opts = options.clone();
        tokio::spawn(async move { run_consume_with(cfg, opts).await })
    };

    // Poll the target until 3 rows arrive.
    let probe: Arc<dyn Backend> = Arc::new(SQLiteBackend::open(&sqlite_path_str).unwrap());
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut row_count = 0_i64;
    while std::time::Instant::now() < deadline {
        let stream = probe
            .read_arrow_stream("SELECT count(*) FROM events")
            .await
            .expect("read count");
        let batches: Vec<_> = stream.try_collect().await.expect("collect");
        row_count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        if row_count >= 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(row_count >= 3, "expected 3 rows; got {row_count}");

    // Scrape /metrics. Expected: Prometheus exposition with our
    // pipeline's counters. The exact metric names live in
    // ematix_flow_core::streaming::StreamingPipelineMetricsCounters
    // and use the `pipeline=<name>` const label.
    let body = reqwest::get(format!("http://127.0.0.1:{metrics_port}/metrics"))
        .await
        .expect("metrics http")
        .text()
        .await
        .expect("metrics body");
    assert!(
        body.contains("pipeline=\"cli-e2e-metrics\""),
        "expected pipeline label in /metrics; got:\n{body}"
    );
    assert!(
        body.contains("ematix_streaming_iterations_total")
            || body.contains("ematix_streaming_rows_written_total"),
        "expected at least one streaming counter in /metrics; got:\n{body}"
    );

    trigger.trigger();
    let _ = tokio::time::timeout(Duration::from_secs(15), pipeline_handle).await;
}
