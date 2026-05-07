//! Verifies that every TOML example under `examples/` parses
//! through the CLI's `PipelineCliConfig::from_toml_str` validator.
//!
//! These are not container-backed tests — they validate the static
//! shape of each example (kinds, required fields, cross-validation
//! rules) so a CLI refactor that breaks an example surfaces here
//! before users hit it.

use std::path::PathBuf;

use ematix_flow_cli::PipelineCliConfig;

fn examples_dir() -> PathBuf {
    let manifest = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest)
        .join("..")
        .join("..")
        .join("examples")
}

fn parse_example(name: &str) -> PipelineCliConfig {
    let path = examples_dir().join(name);
    let toml =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    PipelineCliConfig::from_toml_str(&toml)
        .unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
}

#[test]
fn streaming_kafka_to_pg_parses() {
    let cfg = parse_example("05_streaming_kafka_to_pg.toml");
    assert_eq!(cfg.pipeline_name, "events-to-pg");
    assert_eq!(cfg.source_query, "events");
}

#[test]
fn windowed_session_parses() {
    let cfg = parse_example("07_session_window.toml");
    let w = cfg
        .transform
        .as_ref()
        .and_then(|t| t.window.as_ref())
        .unwrap();
    assert_eq!(w.gap_ms, Some(300_000));
    assert_eq!(w.max_session_duration_ms, Some(86_400_000));
    assert!(
        cfg.state_store.is_some(),
        "session pipeline must declare state_store"
    );
}

#[test]
fn stream_join_parses() {
    let cfg = parse_example("08_stream_join.toml");
    assert_eq!(cfg.sources.len(), 2);
    let j = cfg
        .transform
        .as_ref()
        .and_then(|t| t.join.as_ref())
        .unwrap();
    assert_eq!(j.left_source, "orders");
    assert_eq!(j.right_source, "payments");
    assert_eq!(j.time_window_ms, 300_000);
    assert!(
        cfg.state_store.is_some(),
        "join pipeline must declare state_store"
    );
}

/// Phase Δ PR 6: the CDC demo's pipeline.toml must parse — its
/// validity is part of the user-facing contract.
#[test]
fn cdc_debezium_pipeline_parses() {
    let cfg = parse_example("cdc-debezium/pipeline.toml");
    assert_eq!(cfg.pipeline_name, "cdc-mirror-customers");
    assert_eq!(cfg.source_query, "dbz.public.customers");
    let cdc = cfg
        .transform
        .as_ref()
        .and_then(|t| t.cdc.as_ref())
        .expect("cdc-debezium example must declare [transform.cdc]");
    assert_eq!(cdc.envelope, "debezium");
    assert_eq!(cdc.key_field.as_deref(), Some("after.id"));
}

/// Δ.X1.2: `[target.table].primary_key` is the user-facing channel
/// for declaring PKs on backends that can't surface them via
/// reflection. Locks the parse + the lowering helper that the
/// streaming runtime consumes.
#[test]
fn target_table_primary_key_threads_through() {
    let toml = r#"
pipeline_name = "cdc-mirror"
source_query  = "dbz.public.customers"
idle_pause_ms = 200

[source]
kind              = "kafka"
bootstrap_servers = "localhost:9094"

[target]
kind = "delta_local"
path = "/tmp/lake"

[target.table]
schema      = "default"
name        = "customers"
primary_key = ["id"]

[transform.cdc]
envelope  = "debezium"
key_field = "after.id"
"#;
    let cfg = ematix_flow_cli::PipelineCliConfig::from_toml_str(toml)
        .expect("delta-local + cdc + primary_key must parse");
    let pks = cfg.target_primary_keys();
    assert_eq!(
        pks,
        vec![vec!["id".to_string()]],
        "user-declared PK threads through the lowering helper"
    );
}
