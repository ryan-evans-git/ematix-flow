//! CLI for ematix-flow.
//!
//! Houses the testable surfaces of the `flow` binary: config
//! parsing, backend instantiation from config, and the pipeline-run
//! glue. The binary entry point in `main.rs` is a thin wrapper
//! that calls into [`run_consume`].
//!
//! ## Phases
//!   - **CLI.1** — Scaffolding. `flow consume <toml>` parses a
//!     config, builds source + target backends, and runs a
//!     [`StreamingPipeline`] under SIGTERM/SIGINT shutdown.
//!     Backends supported: Kafka / RabbitMQ / Pub/Sub / Kinesis
//!     as sources; Postgres / MySQL / SQLite / DuckDB / Kafka /
//!     RabbitMQ / Pub/Sub / Kinesis / Delta (local + S3) /
//!     ObjectStore (local + S3, parquet / csv / orc / jsonl) as
//!     targets.
//!   - **CLI.2** — `/metrics` HTTP endpoint exposing the pipeline's
//!     Prometheus registry. Opt in with `--metrics-port <PORT>`.
//!     Server shares the pipeline's shutdown signal so both stop
//!     together.
//!   - **CLI.3 (this commit)** — Process-level supervisor:
//!     restart-on-crash with exponential backoff. Opt in with
//!     `--restart-on-error` plus tuning flags. See
//!     [`crate::supervisor`].
//!
//! ## TOML config shape
//! ```toml
//! pipeline_name = "events-to-pg"
//! source_query = "events"
//! idle_pause_ms = 500
//!
//! [source]
//! kind = "kafka"
//! bootstrap_servers = "localhost:9092"
//! group_id = "ematix-flow"
//!
//! [target]
//! kind = "postgres"
//! url = "postgres://localhost/mydb"
//!
//! [target.table]
//! schema = "public"
//! name = "events"
//! ```

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use arrow_array::RecordBatch;
use ematix_flow_core::backend::{
    Backend, BackendError, ObjectFormat, ObjectWriteOptions, ParquetCompression, TargetTable,
    WriteMode,
};
use ematix_flow_core::pg::PgPool;
use ematix_flow_core::streaming::WatermarkConfig;
use ematix_flow_core::streaming::{
    ShutdownSignal, StreamingPipeline, StreamingPipelineConfig, StreamingPipelineMetrics,
    StreamingPipelineMetricsCounters, install_shutdown_handler,
};
use ematix_flow_core::transform::{BatchTransform, LazySqlTransform, LookupTable};
use ematix_flow_core::windowed::{
    AggKind, AggregationSpec, CountDistinctMode, LateDataPolicy, WindowConfig, WindowKind,
    WindowedAggregateTransform, WindowedMetrics,
};
use futures_util::TryStreamExt;
use std::collections::BTreeMap;

pub mod metrics_server;
pub mod supervisor;
use ematix_flow_core::kafka_backend::KafkaPayloadFormat;
use ematix_flow_core::{
    DeltaBackend, DuckDBBackend, KafkaBackend, KinesisBackend, MySQLBackend, ObjectStoreBackend,
    PostgresBackend, PubSubBackend, RabbitMQBackend, SQLiteBackend,
};
use serde::Deserialize;

/// Detect whether a `scheme://user:password@host/...` URL carries
/// a password segment. Returns `true` only when a *non-empty*
/// password is present (so `postgres://app@host/db` and
/// `postgres://localhost/db` both return `false`). Used by the
/// Π.5 inline-credentials deprecation warning.
fn url_has_password(url: &str) -> bool {
    let Some(scheme_end) = url.find("://") else {
        return false;
    };
    let rest = &url[scheme_end + 3..];
    let authority = match rest.split_once('/') {
        Some((a, _)) => a,
        None => rest,
    };
    let userinfo = match authority.split_once('@') {
        Some((u, _)) => u,
        None => return false,
    };
    match userinfo.split_once(':') {
        Some((_, password)) => !password.is_empty(),
        None => false,
    }
}

/// Π.5: walk a `SourceConfig` and append a finding string per
/// inline-credential pattern detected. Used by
/// [`PipelineCliConfig::inline_credential_findings`].
fn scan_source_for_credentials(s: &SourceConfig, findings: &mut Vec<String>) {
    match s {
        SourceConfig::Kafka {
            sasl_plain_password,
            sasl_scram_password,
            schema_registry_basic_auth_password,
            ..
        } => {
            if sasl_plain_password.is_some() || sasl_scram_password.is_some() {
                findings.push(
                    "[source] Kafka SASL password is inline — register a \
                     KafkaConnection via @ematix.connection (with \
                     `${VAR}` env-var interpolation on \
                     sasl_plain_password / sasl_scram_password) and drop \
                     it from the TOML"
                        .into(),
                );
            }
            if schema_registry_basic_auth_password.is_some() {
                findings.push(
                    "[source] Kafka schema_registry_basic_auth_password \
                     is inline — register a SchemaRegistryConnection \
                     via @ematix.connection (kind = \"schema_registry\") \
                     and reference it through KafkaConnection.schema_registry"
                        .into(),
                );
            }
        }
        SourceConfig::Rabbitmq { amqp_url } => {
            if url_has_password(amqp_url) {
                findings.push(
                    "[source] RabbitMQ amqp_url contains an inline password \
                     — register the connection (kind = \"rabbitmq\") via \
                     @ematix.connection or `~/.ematix-flow/connections.toml` \
                     and reference it from a Python pipeline driven by \
                     `flow consume --module M name`"
                        .into(),
                );
            }
        }
        SourceConfig::Pubsub { .. } => {
            // Pub/Sub uses ADC by default; the only inline option
            // is `anonymous_auth = true` for the emulator, which
            // doesn't carry a credential.
        }
        SourceConfig::Kinesis {
            access_key_id,
            secret_access_key,
            ..
        } => {
            if access_key_id.is_some() || secret_access_key.is_some() {
                findings.push(
                    "[source] Kinesis access_key_id / secret_access_key are \
                     inline credentials — register a KinesisConnection via \
                     @ematix.connection (with `${VAR}` env-var \
                     interpolation) and drop them from the TOML"
                        .into(),
                );
            }
        }
    }
}

/// Π.5: same as `scan_source_for_credentials` but for `TargetConfig`.
fn scan_target_for_credentials(t: &TargetConfig, findings: &mut Vec<String>) {
    match t {
        TargetConfig::Postgres { url, .. } => {
            if url_has_password(url) {
                findings.push(
                    "[target] Postgres URL contains an inline password — \
                     register a PostgresConnection via @ematix.connection \
                     (with `${VAR}` env-var interpolation) and reference it \
                     by name"
                        .into(),
                );
            }
        }
        TargetConfig::Mysql { url, .. } => {
            if url_has_password(url) {
                findings.push(
                    "[target] MySQL URL contains an inline password — \
                     register a MySQLConnection via @ematix.connection"
                        .into(),
                );
            }
        }
        TargetConfig::Sqlite { .. }
        | TargetConfig::Duckdb { .. }
        | TargetConfig::DeltaLocal { .. }
        | TargetConfig::ObjectStoreLocal { .. } => {
            // File-path-only backends — no credentials to flag.
        }
        TargetConfig::Kafka {
            sasl_plain_password,
            sasl_scram_password,
            schema_registry_basic_auth_password,
            ..
        } => {
            if sasl_plain_password.is_some() || sasl_scram_password.is_some() {
                findings.push(
                    "[target] Kafka SASL password is inline — register a \
                     KafkaConnection via @ematix.connection"
                        .into(),
                );
            }
            if schema_registry_basic_auth_password.is_some() {
                findings.push(
                    "[target] Kafka schema_registry_basic_auth_password \
                     is inline — register a SchemaRegistryConnection \
                     via @ematix.connection"
                        .into(),
                );
            }
        }
        TargetConfig::Rabbitmq { amqp_url, .. } => {
            if url_has_password(amqp_url) {
                findings.push(
                    "[target] RabbitMQ amqp_url contains an inline password \
                     — register a RabbitMQConnection via @ematix.connection"
                        .into(),
                );
            }
        }
        TargetConfig::Pubsub { .. } => {}
        TargetConfig::Kinesis {
            access_key_id,
            secret_access_key,
            ..
        } => {
            if access_key_id.is_some() || secret_access_key.is_some() {
                findings.push(
                    "[target] Kinesis access_key_id / secret_access_key \
                     are inline credentials — register a KinesisConnection \
                     via @ematix.connection"
                        .into(),
                );
            }
        }
        TargetConfig::DeltaS3 {
            access_key_id,
            secret_access_key,
            ..
        }
        | TargetConfig::ObjectStoreS3 {
            access_key_id,
            secret_access_key,
            ..
        } => {
            // S3 backends always require both keys — flag whenever
            // either is non-empty (the literal "" sentinel for
            // serde-default tolerance is rare in practice).
            if !access_key_id.is_empty() || !secret_access_key.is_empty() {
                findings.push(
                    "[target] S3-backed target carries inline AWS \
                     credentials — register a DeltaS3Connection / \
                     ObjectStoreS3Connection via @ematix.connection \
                     (with `${VAR}` env-var interpolation)"
                        .into(),
                );
            }
        }
    }
}

/// Strip the password segment of a `scheme://user:password@host`
/// URL for display. Returns the URL unchanged if no userinfo or
/// no password segment is present.
///
/// Used by the redacting `Debug` impls on `SourceConfig` /
/// `TargetConfig` so logging a `PipelineCliConfig` (directly or
/// transitively via tracing's `?value` field) doesn't leak
/// credentials to log aggregators.
fn redact_db_url(url: &str) -> String {
    // Find scheme separator.
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let (scheme, rest) = url.split_at(scheme_end + 3); // include "://"
    let (authority, tail) = match rest.split_once('/') {
        Some((a, t)) => (a, format!("/{t}")),
        None => (rest, String::new()),
    };
    let (userinfo, host) = match authority.split_once('@') {
        Some((u, h)) => (u, h),
        None => return url.to_string(),
    };
    let user = userinfo.split(':').next().unwrap_or(userinfo);
    if user.is_empty() {
        format!("{scheme}<redacted>@{host}{tail}")
    } else {
        format!("{scheme}{user}:<redacted>@{host}{tail}")
    }
}

/// Top-level pipeline config loaded from a TOML file.
///
/// Targets accept two surfaces:
/// 1. **`[target]`** — single-target shorthand (the v0.1 form).
/// 2. **`[[targets]]`** — array of targets (Π.4a multi-target
///    fan-out). Each entry has the same shape as the single
///    `[target]` block.
///
/// Exactly one of the two must be set. Use [`Self::targets`] to
/// read in a form-agnostic way.
#[derive(Clone, Deserialize)]
pub struct PipelineCliConfig {
    /// Used for log lines + Prometheus labels.
    pub pipeline_name: String,
    /// Argument passed to `source.read_arrow_stream`. For Kafka /
    /// Kinesis: stream/topic name. For Pub/Sub: subscription. For
    /// RabbitMQ: queue name. Required when using the single
    /// `[source]` form; ignored when using `[[sources]]` (each
    /// entry carries its own `query`).
    #[serde(default)]
    pub source_query: String,
    /// Sleep duration (ms) when the source returns an empty batch.
    /// Defaults to 500 ms.
    #[serde(default = "default_idle_pause_ms")]
    pub idle_pause_ms: u64,
    /// Optional dead-letter topic. Same Kafka-only constraint as
    /// `StreamingPipelineConfig::dead_letter_topic`.
    pub dead_letter_topic: Option<String>,
    /// Source backend (single-source form). Mutually exclusive
    /// with `sources`.
    #[serde(default)]
    pub source: Option<SourceConfig>,
    /// Π.4b-2: multi-source fan-in form. Each entry pairs a
    /// source backend with its own `query`. Mutually exclusive
    /// with `source` + top-level `source_query`.
    #[serde(default)]
    pub sources: Vec<SourceEntryConfig>,
    /// Single-target form. Mutually exclusive with `targets`.
    #[serde(default)]
    pub target: Option<TargetConfig>,
    /// Multi-target form (Π.4a). Mutually exclusive with `target`.
    /// Default = empty Vec.
    #[serde(default)]
    pub targets: Vec<TargetConfig>,
    /// Π.4b-1: optional SQL transform applied per batch between
    /// the source and target. Omit the `[transform]` block to keep
    /// the zero-overhead pass-through path.
    #[serde(default)]
    pub transform: Option<TransformConfig>,
    /// Phase 39.5a: durable per-pipeline state. Required when the
    /// transform configures a session window or (39.5b) a stateful
    /// stream-stream join. Tumbling/hopping pipelines stay
    /// in-memory and leave this `None`.
    #[serde(default)]
    pub state_store: Option<StateStoreConfig>,
    /// Π.1 advanced-knob: per-source watermark tuning. Omit to keep
    /// the Rust core's defaults (auto-enabled with `lateness_ms = 0`
    /// and `source_idleness_ms = 60_000` whenever a window or join
    /// is configured). Set to override either field independently.
    #[serde(default)]
    pub watermark: Option<WatermarkConfigToml>,
}

/// `[watermark]` TOML block.
///
/// Each field is `Option` so a partial override (e.g. only
/// `lateness_ms`) keeps the framework default for whatever the user
/// didn't set. The runner combines these with
/// `WatermarkConfig::default()` at build time.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct WatermarkConfigToml {
    #[serde(default)]
    pub lateness_ms: Option<u64>,
    #[serde(default)]
    pub source_idleness_ms: Option<u64>,
}

/// `[transform]` block.
///
/// ```toml
/// [transform]
/// sql = """
///     SELECT s.user_id, s.event_type, u.country
///     FROM source s
///     LEFT JOIN users u ON s.user_id = u.id
///     WHERE s.event_type IN ('click', 'view')
/// """
///
/// [transform.lookups.users]
/// kind = "postgres"
/// url = "postgres://localhost/mydb"
/// schema = "public"
/// table = "users"
/// ```
///
/// The SQL references the streaming source as `source` and any
/// configured lookups by their `[transform.lookups.<name>]` map
/// key. Lookups are loaded once at pipeline startup (Π.4b-3);
/// refresh-on-interval is a future phase.
#[derive(Clone, Debug, Deserialize)]
pub struct TransformConfig {
    /// SQL pre-stage. Required for non-windowed transforms; for
    /// windowed transforms the SQL stage runs *before* the window
    /// aggregator and is itself optional — set the empty string or
    /// omit when no pre-stage is wanted.
    #[serde(default)]
    pub sql: String,
    /// Π.4b-3: static lookup tables. Map keys become the table
    /// names visible to the SQL. `BTreeMap` keeps the load order
    /// deterministic — useful for log lines + tests.
    #[serde(default)]
    pub lookups: BTreeMap<String, LookupConfig>,
    /// Phase 39.4: optional windowed aggregator wrapping the SQL
    /// pre-stage. Drives the per-pipeline state machine and emits
    /// `(window_start, window_end, group_keys..., aggregates...)`
    /// rows.
    #[serde(default)]
    pub window: Option<WindowConfigToml>,
    /// Phase 39.5b: optional keyed time-windowed stream-stream
    /// join. Mutually exclusive with `[transform.window]` and with
    /// the SQL pre-stage (`sql`) — joins consume two distinct
    /// sources directly without an inner DataFusion stage.
    #[serde(default)]
    pub join: Option<JoinConfigToml>,
    /// Phase 39.5a P2.15: error policy for transform-call
    /// failures. `"fail"` (default), `"drop"`, or `"dlq"`. See
    /// `streaming::TransformErrorPolicy` for full semantics.
    #[serde(default = "default_transform_on_error")]
    pub on_error: String,

    /// Σ.A2 PR 1: source SQL dialect of the `sql` field. Translated
    /// to DataFusion's native dialect before being handed to the
    /// transform layer. Accepted values: `"datafusion"` (default,
    /// pass-through), `"spark"`, `"duckdb"`. See
    /// `ematix_flow_core::dialect` + `docs/PHASE_SIGMA_PLAN.md` Σ.A2.
    ///
    /// Today only `"datafusion"` is fully wired; `"spark"` and
    /// `"duckdb"` parse here but will panic at pipeline-build time
    /// with a clear "translator not yet implemented" pointer until
    /// PRs 2 / 5 of Σ.A2 land. Validation of the string into the
    /// typed `Dialect` enum happens at config-load via
    /// `validate_transform_dialect`, so unknown values fail fast
    /// without reaching the panic site.
    #[serde(default)]
    pub dialect: Option<String>,
}

fn default_transform_on_error() -> String {
    "fail".into()
}

/// `[transform.window]` block — config-block surface for the
/// Phase 39.4 windowed aggregator. Mirrors `windowed::WindowConfig`
/// with all knobs flattened to the TOML surface.
#[derive(Clone, Debug, Deserialize)]
pub struct WindowConfigToml {
    /// `"tumbling"`, `"hopping"`, or `"session"` (Phase 39.5a).
    pub kind: WindowKindToml,
    /// Required when `kind = "tumbling" | "hopping"`. For
    /// `"session"` it must be omitted (or 0) — sessions are
    /// gap-based.
    #[serde(default)]
    pub duration_ms: u64,
    /// Required when `kind = "hopping"`; ignored for tumbling /
    /// session.
    #[serde(default)]
    pub hop_ms: Option<u64>,
    /// Phase 39.5a: required when `kind = "session"`.
    #[serde(default)]
    pub gap_ms: Option<u64>,
    /// Phase 39.5a: required when `kind = "session"`. Hard ceiling
    /// on session duration (force-emit boundary). Must be > `gap_ms`.
    #[serde(default)]
    pub max_session_duration_ms: Option<u64>,
    /// Defaults to `"_event_ts"`.
    #[serde(default = "default_event_time_column")]
    pub event_time_column: String,
    /// Group-by columns. Empty = single-key aggregation.
    #[serde(default)]
    pub group_by: Vec<String>,
    /// Late-data policy: `"drop"` (default) or `"reopen"` (PR 2c).
    /// `"dlq"` is reserved for a future follow-on.
    #[serde(default = "default_late_data")]
    pub late_data: String,
    /// PR 2c: required when `late_data = "reopen"`. Window state is
    /// retained for this many milliseconds past `window_end`; late
    /// arrivals within the budget re-aggregate and the window
    /// re-emits with corrected aggregates. Past the budget, late
    /// arrivals drop.
    #[serde(default)]
    pub allowed_lateness_ms: Option<u64>,
    /// Per-window cap on distinct group keys. Cap-hit fails the
    /// pipeline.
    pub max_groups_per_window: usize,
    #[serde(default = "default_window_start_column")]
    pub window_start_column: String,
    #[serde(default = "default_window_end_column")]
    pub window_end_column: String,
    /// Phase 39.5a: output column name for `session_id` when
    /// `kind = "session"`. Defaults to `"session_id"`.
    #[serde(default = "default_session_id_column")]
    pub session_id_column: String,
    /// `[[transform.window.aggregations]]` blocks.
    #[serde(default, rename = "aggregations")]
    pub aggregations: Vec<AggregationToml>,
}

/// Translate the TOML-deserialized window config into the core
/// crate's `WindowConfig`. Returns config-load-time errors as
/// strings.
fn window_toml_to_core(t: &WindowConfigToml) -> Result<WindowConfig, String> {
    let kind = match t.kind {
        WindowKindToml::Tumbling => WindowKind::Tumbling,
        WindowKindToml::Hopping => WindowKind::Hopping,
        WindowKindToml::Session => WindowKind::Session,
    };
    let hop_ms = match (kind, t.hop_ms) {
        (WindowKind::Tumbling, _) => t.duration_ms,
        (WindowKind::Hopping, Some(h)) => h,
        (WindowKind::Hopping, None) => {
            return Err("[transform.window]: hop_ms is required when kind = \"hopping\"".into());
        }
        // Sessions don't use hop_ms; the core validator rejects a
        // non-zero value, so we pass 0 through.
        (WindowKind::Session, _) => 0,
    };
    let late_data = match t.late_data.as_str() {
        "drop" => LateDataPolicy::Drop,
        "reopen" => {
            let budget = t.allowed_lateness_ms.ok_or_else(|| {
                "[transform.window]: late_data = \"reopen\" requires allowed_lateness_ms"
                    .to_string()
            })?;
            LateDataPolicy::Reopen {
                allowed_lateness_ms: budget,
            }
        }
        "dlq" => LateDataPolicy::Dlq,
        other => {
            return Err(format!(
                "[transform.window]: late_data = {other:?} not supported \
                 (use \"drop\" or \"reopen\")"
            ));
        }
    };
    let aggregations: Result<Vec<AggregationSpec>, String> = t
        .aggregations
        .iter()
        .map(|a| {
            let kind = match a.agg.as_str() {
                "count" => AggKind::CountStar,
                "count_col" => AggKind::CountCol,
                "sum" => AggKind::Sum,
                "min" => AggKind::Min,
                "max" => AggKind::Max,
                "avg" => AggKind::Avg,
                "first" => AggKind::First,
                "last" => AggKind::Last,
                "count_distinct" => AggKind::CountDistinct,
                other => {
                    return Err(format!(
                        "[[transform.window.aggregations]]: agg = {other:?} not supported \
                         (use one of count / count_col / sum / min / max / avg / \
                         first / last / count_distinct)"
                    ));
                }
            };
            // PR 2b: parse mode + cap for count_distinct.
            let count_distinct_mode = if matches!(kind, AggKind::CountDistinct) {
                Some(match a.mode.as_deref().unwrap_or("approximate") {
                    "approximate" => CountDistinctMode::Approximate,
                    "exact" => CountDistinctMode::Exact,
                    other => {
                        return Err(format!(
                            "[[transform.window.aggregations]]: mode = {other:?} not supported \
                             (use \"approximate\" or \"exact\")"
                        ));
                    }
                })
            } else {
                None
            };
            Ok(AggregationSpec {
                kind,
                column: a.column.clone(),
                alias: a.alias.clone(),
                count_distinct_mode,
                max_distinct_values_per_group: a.max_distinct_values_per_group,
            })
        })
        .collect();
    Ok(WindowConfig {
        kind,
        duration_ms: t.duration_ms,
        hop_ms,
        event_time_column: t.event_time_column.clone(),
        group_by: t.group_by.clone(),
        aggregations: aggregations?,
        late_data,
        max_groups_per_window: t.max_groups_per_window,
        window_start_column: t.window_start_column.clone(),
        window_end_column: t.window_end_column.clone(),
        session_id_column: t.session_id_column.clone(),
        gap_ms: t.gap_ms,
        max_session_duration_ms: t.max_session_duration_ms,
    })
}

fn default_event_time_column() -> String {
    "_event_ts".into()
}

fn default_late_data() -> String {
    "drop".into()
}

fn default_window_start_column() -> String {
    "window_start".into()
}

fn default_window_end_column() -> String {
    "window_end".into()
}

fn default_session_id_column() -> String {
    "session_id".into()
}

fn default_state_store_schema() -> String {
    "public".into()
}

fn default_join_left_prefix() -> String {
    "left_".into()
}

fn default_join_right_prefix() -> String {
    "right_".into()
}

/// `[transform.join]` block — Phase 39.5b config surface.
///
/// ```toml
/// [transform.join]
/// kind = "stream_stream_join"
/// left_source = "orders"
/// right_source = "payments"
/// left_keys = ["order_id"]
/// right_keys = ["order_id"]
/// time_window_ms = 300000
/// event_time_column = "_event_ts"
/// late_data = "drop"
/// left_column_prefix = "left_"   # optional, default
/// right_column_prefix = "right_" # optional, default
/// ```
///
/// `left_source` / `right_source` must match the `name` field of
/// two distinct `[[sources]]` entries — the pipeline routes per-
/// source batches to the join transform via `BatchContext::source_id`.
#[derive(Clone, Debug, Deserialize)]
pub struct JoinConfigToml {
    /// Discriminator. Only `"stream_stream_join"` is supported.
    pub kind: String,
    pub left_source: String,
    pub right_source: String,
    pub left_keys: Vec<String>,
    pub right_keys: Vec<String>,
    /// Symmetric `|right.event_ts - left.event_ts| ≤ time_window_ms`.
    /// Used when `min_delta_ms` and `max_delta_ms` are both unset.
    #[serde(default)]
    pub time_window_ms: u64,
    /// Phase 39.5b P2.14: asymmetric join window — minimum
    /// `right.event_ts - left.event_ts` in milliseconds (negative
    /// for "right may arrive before left"). Must be set together
    /// with `max_delta_ms`; overrides `time_window_ms`.
    #[serde(default)]
    pub min_delta_ms: Option<i64>,
    /// Maximum `right.event_ts - left.event_ts` in milliseconds.
    #[serde(default)]
    pub max_delta_ms: Option<i64>,
    #[serde(default = "default_event_time_column")]
    pub event_time_column: String,
    /// `"drop"` (default) or `"reopen"` (Phase 39.5b P2.13).
    /// `"reopen"` requires `allowed_lateness_ms` and extends the
    /// per-side retention horizon by that many ms.
    #[serde(default = "default_late_data")]
    pub late_data: String,
    /// Phase 39.5b P2.13: required when `late_data = "reopen"`.
    /// Extends the per-side retention horizon — a left buffered
    /// row evicts at `wm > L.ts + max_delta + allowed_lateness`;
    /// a right buffered row evicts at
    /// `wm > R.ts - min_delta + allowed_lateness`.
    #[serde(default)]
    pub allowed_lateness_ms: Option<u64>,
    #[serde(default = "default_join_left_prefix")]
    pub left_column_prefix: String,
    #[serde(default = "default_join_right_prefix")]
    pub right_column_prefix: String,
}

fn join_toml_to_core(t: &JoinConfigToml) -> Result<ematix_flow_core::join::JoinConfig, String> {
    use ematix_flow_core::join::{JoinConfig, JoinKind, JoinLateDataPolicy};
    let late_data = match t.late_data.as_str() {
        "drop" => JoinLateDataPolicy::Drop,
        "reopen" => {
            let budget = t.allowed_lateness_ms.ok_or_else(|| {
                "[transform.join] late_data = \"reopen\" requires \
                 allowed_lateness_ms"
                    .to_string()
            })?;
            JoinLateDataPolicy::Reopen {
                allowed_lateness_ms: budget,
            }
        }
        other => {
            return Err(format!(
                "[transform.join] late_data = {other:?} not supported \
                 (use \"drop\" or \"reopen\")"
            ));
        }
    };
    let kind = match t.kind.as_str() {
        "stream_stream_join" => JoinKind::Inner, // back-compat: legacy alias
        "inner" => JoinKind::Inner,
        "left_outer" => JoinKind::LeftOuter,
        "right_outer" => JoinKind::RightOuter,
        "full_outer" => JoinKind::FullOuter,
        other => {
            return Err(format!(
                "[transform.join] kind = {other:?} not supported (use \"inner\", \
                 \"left_outer\", \"right_outer\", \"full_outer\", or the legacy \
                 alias \"stream_stream_join\" = inner)"
            ));
        }
    };
    Ok(JoinConfig {
        kind,
        left_source: t.left_source.clone(),
        right_source: t.right_source.clone(),
        left_keys: t.left_keys.clone(),
        right_keys: t.right_keys.clone(),
        time_window_ms: t.time_window_ms,
        min_delta_ms: t.min_delta_ms,
        max_delta_ms: t.max_delta_ms,
        event_time_column: t.event_time_column.clone(),
        late_data,
        left_column_prefix: t.left_column_prefix.clone(),
        right_column_prefix: t.right_column_prefix.clone(),
    })
}

fn default_checkpoint_interval_ms() -> u64 {
    60_000
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WindowKindToml {
    Tumbling,
    Hopping,
    /// Phase 39.5a: gap-based per-key sessions.
    Session,
}

/// `[state_store]` block (Phase 39.5a). Top-level — same level as
/// `[transform]` — because 39.5b stream-stream joins will share the
/// same backend without nesting under the windowed transform.
///
/// ```toml
/// [state_store]
/// kind = "postgres"
/// url = "postgres://localhost/ematix_state"
/// schema = "public"
/// checkpoint_interval_ms = 60000
/// ```
///
/// The discriminant is `kind`; everything else is per-variant.
/// `kind = "in_memory"` is documented for tests + as a no-op
/// production option (loud config-load warning lands in PR 3).
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum StateStoreConfig {
    Postgres {
        url: String,
        #[serde(default = "default_state_store_schema")]
        schema: String,
        #[serde(default = "default_checkpoint_interval_ms")]
        checkpoint_interval_ms: u64,
    },
    InMemory {
        #[serde(default = "default_checkpoint_interval_ms")]
        checkpoint_interval_ms: u64,
    },
}

impl StateStoreConfig {
    /// Periodic dirty-only checkpoint cadence in milliseconds.
    /// Bounds replay-on-restart for idle-but-not-empty pipelines —
    /// see `docs/PHASE_39_5_SESSIONS.md` § "Cadence".
    pub fn checkpoint_interval_ms(&self) -> u64 {
        match self {
            StateStoreConfig::Postgres {
                checkpoint_interval_ms,
                ..
            }
            | StateStoreConfig::InMemory {
                checkpoint_interval_ms,
            } => *checkpoint_interval_ms,
        }
    }

    /// Validate the block in isolation. Called from
    /// `PipelineCliConfig::from_toml_str`.
    fn validate(&self) -> Result<(), ConfigError> {
        if self.checkpoint_interval_ms() == 0 {
            return Err(ConfigError::Parse(
                "[state_store]: checkpoint_interval_ms must be > 0 — \
                 zero defeats the dirty-only periodic floor"
                    .into(),
            ));
        }
        match self {
            StateStoreConfig::Postgres { url, .. } if url.is_empty() => Err(ConfigError::Parse(
                "[state_store] kind = \"postgres\": url must be non-empty".into(),
            )),
            _ => Ok(()),
        }
    }

    /// Build the backing [`StateStore`] implementation.
    ///
    /// Postgres opens a pool eagerly + ensures the two state tables
    /// exist. InMemory is synchronous internally but the method is
    /// `async` so the call site is uniform across variants.
    ///
    /// [`StateStore`]: ematix_flow_core::state_store::StateStore
    pub async fn build(
        &self,
    ) -> Result<Arc<dyn ematix_flow_core::state_store::StateStore>, BackendError> {
        use ematix_flow_core::state_store::{InMemoryStateStore, PostgresStateStore};
        match self {
            StateStoreConfig::Postgres { url, schema, .. } => {
                let store = PostgresStateStore::connect(url, schema).await?;
                store.ensure_schema().await?;
                Ok(Arc::new(store))
            }
            StateStoreConfig::InMemory { .. } => Ok(Arc::new(InMemoryStateStore::new())),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct AggregationToml {
    /// `"count"` (= COUNT(*)), `"count_col"`, `"sum"`, `"min"`,
    /// `"max"`, `"avg"`, `"first"`, `"last"`, `"count_distinct"`
    /// (PR 2b).
    pub agg: String,
    /// Column to aggregate. Required for everything except `"count"`.
    #[serde(default)]
    pub column: Option<String>,
    /// Output column alias. Required.
    #[serde(rename = "as")]
    pub alias: String,
    /// PR 2b: `"approximate"` (default) or `"exact"`. Only meaningful
    /// when `agg = "count_distinct"`.
    #[serde(default)]
    pub mode: Option<String>,
    /// PR 2b: per-group cap on distinct values for
    /// `count_distinct mode = "exact"`. Required when mode is
    /// `"exact"`. Cap-hit fails the pipeline.
    #[serde(default)]
    pub max_distinct_values_per_group: Option<usize>,
}

/// `[transform.lookups.<name>]` entry. Each lookup is loaded
/// from a database backend via `read_arrow_stream` of a
/// `SELECT * FROM <schema>.<table>` query (or `<table>` when
/// `schema` is empty), then registered as a DataFusion
/// `MemTable` named `<name>`.
///
/// Phase 39.3: every variant carries an optional
/// `refresh_interval_ms`. When set, a tokio task re-loads the
/// lookup on the configured interval and atomically swaps the
/// registered MemTable in the transform's SessionContext. Omit
/// the field (or set to 0) to load once at startup and never
/// refresh — the historical behavior.
#[derive(Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LookupConfig {
    Postgres {
        url: String,
        #[serde(default)]
        schema: String,
        table: String,
        #[serde(default)]
        refresh_interval_ms: Option<u64>,
    },
    Mysql {
        url: String,
        #[serde(default)]
        schema: String,
        table: String,
        #[serde(default)]
        refresh_interval_ms: Option<u64>,
    },
    Sqlite {
        path: String,
        #[serde(default)]
        schema: String,
        table: String,
        #[serde(default)]
        refresh_interval_ms: Option<u64>,
    },
    Duckdb {
        path: String,
        #[serde(default)]
        schema: String,
        table: String,
        #[serde(default)]
        refresh_interval_ms: Option<u64>,
    },
}

impl std::fmt::Debug for LookupConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LookupConfig::Postgres {
                url,
                schema,
                table,
                refresh_interval_ms,
            } => f
                .debug_struct("Postgres")
                .field("url", &redact_db_url(url))
                .field("schema", schema)
                .field("table", table)
                .field("refresh_interval_ms", refresh_interval_ms)
                .finish(),
            LookupConfig::Mysql {
                url,
                schema,
                table,
                refresh_interval_ms,
            } => f
                .debug_struct("Mysql")
                .field("url", &redact_db_url(url))
                .field("schema", schema)
                .field("table", table)
                .field("refresh_interval_ms", refresh_interval_ms)
                .finish(),
            LookupConfig::Sqlite {
                path,
                schema,
                table,
                refresh_interval_ms,
            } => f
                .debug_struct("Sqlite")
                .field("path", path)
                .field("schema", schema)
                .field("table", table)
                .field("refresh_interval_ms", refresh_interval_ms)
                .finish(),
            LookupConfig::Duckdb {
                path,
                schema,
                table,
                refresh_interval_ms,
            } => f
                .debug_struct("Duckdb")
                .field("path", path)
                .field("schema", schema)
                .field("table", table)
                .field("refresh_interval_ms", refresh_interval_ms)
                .finish(),
        }
    }
}

impl LookupConfig {
    /// Qualified name used in the auto-generated `SELECT *` query.
    /// Joins schema + table when both are set; falls back to
    /// table-only when schema is empty.
    fn qualified_name(&self) -> String {
        let (schema, table) = match self {
            LookupConfig::Postgres { schema, table, .. }
            | LookupConfig::Mysql { schema, table, .. }
            | LookupConfig::Sqlite { schema, table, .. }
            | LookupConfig::Duckdb { schema, table, .. } => (schema.as_str(), table.as_str()),
        };
        if schema.is_empty() {
            table.to_string()
        } else {
            format!("{schema}.{table}")
        }
    }

    /// Open the underlying backend. Each lookup gets its own
    /// pool / handle — the pool isn't reused across lookups
    /// (lookup load is one-shot, pooling buys nothing here).
    async fn build_backend(&self) -> Result<Arc<dyn Backend>, BackendError> {
        match self {
            LookupConfig::Postgres { url, .. } => {
                let pool = PgPool::connect(url).await?;
                let b = PostgresBackend::new(Arc::new(pool), url.clone());
                Ok(Arc::new(b))
            }
            LookupConfig::Mysql { url, .. } => {
                let b = MySQLBackend::open(url)?;
                Ok(Arc::new(b))
            }
            LookupConfig::Sqlite { path, .. } => {
                let b = SQLiteBackend::open(path)?;
                Ok(Arc::new(b))
            }
            LookupConfig::Duckdb { path, .. } => {
                let b = DuckDBBackend::open(path)?;
                Ok(Arc::new(b))
            }
        }
    }

    /// Phase 39.3: refresh interval, when configured. Zero is
    /// treated the same as `None` — refresh disabled.
    pub fn refresh_interval_ms(&self) -> Option<u64> {
        let raw = match self {
            LookupConfig::Postgres {
                refresh_interval_ms,
                ..
            }
            | LookupConfig::Mysql {
                refresh_interval_ms,
                ..
            }
            | LookupConfig::Sqlite {
                refresh_interval_ms,
                ..
            }
            | LookupConfig::Duckdb {
                refresh_interval_ms,
                ..
            } => *refresh_interval_ms,
        };
        raw.filter(|n| *n > 0)
    }

    /// Single-shot load: open backend, read every row, return the
    /// resulting batches with their inferred schema. Shared by the
    /// startup-load path (`PipelineCliConfig::load_lookups`) and
    /// the Phase 39.3 background refresh task.
    pub async fn load_once(
        &self,
        name: &str,
    ) -> Result<(arrow_schema::SchemaRef, Vec<RecordBatch>), BackendError> {
        let backend = self.build_backend().await?;
        let qname = self.qualified_name();
        let query = format!("SELECT * FROM {qname}");
        let stream = backend.read_arrow_stream(&query).await?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        let schema = batches.first().map(|b| b.schema()).ok_or_else(|| {
            BackendError::Other(format!(
                "transform: lookup `{name}` returned zero batches \
                 (cannot infer schema)"
            ))
        })?;
        Ok((schema, batches))
    }
}

fn default_idle_pause_ms() -> u64 {
    500
}

/// `[[sources]]` entry: a `SourceConfig` plus its per-source
/// `query`. Each `[[sources]]` table reads as
/// `kind = "..."` + the variant's fields + `query = "..."`.
#[derive(Clone, Deserialize)]
pub struct SourceEntryConfig {
    /// Same string passed to `read_arrow_stream` for this source.
    pub query: String,
    #[serde(flatten)]
    pub source: SourceConfig,
}

impl std::fmt::Debug for SourceEntryConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceEntryConfig")
            .field("query", &self.query)
            .field("source", &self.source)
            .finish()
    }
}

/// Source backend variants. Tagged on `kind` so TOML reads
/// naturally with `[source]` + `kind = "..."`.
#[derive(Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SourceConfig {
    Kafka {
        bootstrap_servers: String,
        group_id: Option<String>,
        /// Π.1: Kafka payload encoding —
        /// `"json"` (default) | `"raw_bytes"` | `"avro"` | `"protobuf"`.
        /// Avro / Protobuf require `schema_registry_url`.
        #[serde(default)]
        payload_format: Option<String>,
        /// Π.1: Confluent-style Schema Registry URL. Required when
        /// `payload_format = "avro" | "protobuf"`. Per-pipeline today;
        /// the typed-Python facade lets users factor this through a
        /// `SchemaRegistryConnection` in the connection registry.
        #[serde(default)]
        schema_registry_url: Option<String>,
        /// Π.1 follow-up: HTTP Basic auth on Schema Registry
        /// (Confluent Cloud uses an API-key / API-secret pair). Both
        /// must be set together, or both omitted.
        #[serde(default)]
        schema_registry_basic_auth_user: Option<String>,
        #[serde(default)]
        schema_registry_basic_auth_password: Option<String>,
        /// Kafka SASL/PLAIN credentials. Mutually exclusive with
        /// SASL/SCRAM and MSK-IAM (validated at backend-build time).
        #[serde(default)]
        sasl_plain_username: Option<String>,
        #[serde(default)]
        sasl_plain_password: Option<String>,
        /// Kafka SASL/SCRAM-SHA-{256,512} credentials.
        /// `sasl_scram_mechanism` is `"sha-256"` or `"sha-512"`.
        #[serde(default)]
        sasl_scram_username: Option<String>,
        #[serde(default)]
        sasl_scram_password: Option<String>,
        #[serde(default)]
        sasl_scram_mechanism: Option<String>,
        /// AWS MSK IAM region. Triggers `OAUTHBEARER`-mechanism auth
        /// against MSK with sigv4 signing.
        #[serde(default)]
        msk_iam_region: Option<String>,
    },
    Rabbitmq {
        amqp_url: String,
    },
    Pubsub {
        project_id: String,
        endpoint: Option<String>,
        #[serde(default)]
        anonymous_auth: bool,
    },
    Kinesis {
        stream_name: String,
        region: Option<String>,
        endpoint: Option<String>,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
    },
}

/// Target backend variants. Includes both the OLTP/OLAP / Delta /
/// ObjectStore destinations (the typical streaming sink) and the
/// streaming backends themselves (for stream→stream pipelines).
#[derive(Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TargetConfig {
    Postgres {
        url: String,
        table: TableSpecConfig,
    },
    Mysql {
        url: String,
        table: TableSpecConfig,
    },
    Sqlite {
        path: String,
        table: TableSpecConfig,
    },
    Duckdb {
        path: String,
        table: TableSpecConfig,
    },
    Kafka {
        bootstrap_servers: String,
        group_id: Option<String>,
        topic: String,
        /// Phase 40.2: per-row Kafka message-key column. When set,
        /// each batch row's value in this column becomes the
        /// produced message's Kafka key. Column must be Utf8,
        /// LargeUtf8, or Binary. Default (omitted): round-robin.
        #[serde(default)]
        message_key_column: Option<String>,
        /// Π.1: payload encoding for produce path. Same set of
        /// values as the source side: `"json"` | `"raw_bytes"` |
        /// `"avro"` | `"protobuf"`.
        #[serde(default)]
        payload_format: Option<String>,
        /// Π.1: Schema Registry URL used by Avro / Protobuf
        /// produce.
        #[serde(default)]
        schema_registry_url: Option<String>,
        /// Π.1 follow-up: HTTP Basic auth on Schema Registry; same
        /// shape as the source side.
        #[serde(default)]
        schema_registry_basic_auth_user: Option<String>,
        #[serde(default)]
        schema_registry_basic_auth_password: Option<String>,
        /// Kafka auth — same shape as the source side; see
        /// [`SourceConfig::Kafka`].
        #[serde(default)]
        sasl_plain_username: Option<String>,
        #[serde(default)]
        sasl_plain_password: Option<String>,
        #[serde(default)]
        sasl_scram_username: Option<String>,
        #[serde(default)]
        sasl_scram_password: Option<String>,
        #[serde(default)]
        sasl_scram_mechanism: Option<String>,
        #[serde(default)]
        msk_iam_region: Option<String>,
    },
    Rabbitmq {
        amqp_url: String,
        queue: String,
    },
    Pubsub {
        project_id: String,
        endpoint: Option<String>,
        #[serde(default)]
        anonymous_auth: bool,
        topic: String,
    },
    Kinesis {
        stream_name: String,
        region: Option<String>,
        endpoint: Option<String>,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        partition_key_prefix: String,
    },
    /// Local-filesystem-backed Delta table.
    DeltaLocal {
        path: String,
        table: TableSpecConfig,
        /// Phase 40.1: column names used for partitioned table
        /// creation on first write. Empty (default) =
        /// unpartitioned. Pre-existing tables retain their
        /// layout; deltalake-rs validates + rejects mismatches.
        #[serde(default)]
        partition_by: Vec<String>,
    },
    /// S3-backed Delta table. Production deployments with
    /// concurrent writers should configure DynamoDB locking
    /// separately — the `s3` feature flag in deltalake-aws
    /// registers the basic store; the framework doesn't expose
    /// per-table lock provider config yet.
    DeltaS3 {
        endpoint: String,
        bucket: String,
        /// Prefix under the bucket. Empty string = bucket root.
        #[serde(default)]
        prefix: String,
        region: String,
        access_key_id: String,
        secret_access_key: String,
        table: TableSpecConfig,
        /// Phase 40.1: column names used for partitioned table
        /// creation on first write. See DeltaLocal for details.
        #[serde(default)]
        partition_by: Vec<String>,
    },
    /// Local-filesystem object store (parquet / csv / orc / jsonl).
    /// `path` is the root directory; `prefix` is the per-table
    /// prefix (mapped to `TargetTable.name` so the framework's
    /// table-prefix builder produces `<root>/<prefix>/...`).
    ObjectStoreLocal {
        path: String,
        format: ObjectFormatConfig,
        prefix: String,
        /// Π.1.4: Parquet compression codec — `"uncompressed"`,
        /// `"snappy"`, `"gzip"`, or `"zstd"`. Only consulted when
        /// `format = "parquet"`. Validated at backend-build time.
        #[serde(default)]
        parquet_compression: Option<String>,
        /// Π.1.4: CSV column delimiter (single character). Only
        /// consulted when `format = "csv"`.
        #[serde(default)]
        csv_delimiter: Option<String>,
        /// Π.1.4: emit a CSV header row. Default = `true` (omit to
        /// keep the historical behavior).
        #[serde(default)]
        csv_header: Option<bool>,
    },
    /// S3-backed object store (parquet / csv / orc / jsonl).
    ObjectStoreS3 {
        endpoint: String,
        bucket: String,
        region: String,
        access_key_id: String,
        secret_access_key: String,
        format: ObjectFormatConfig,
        prefix: String,
        #[serde(default)]
        parquet_compression: Option<String>,
        #[serde(default)]
        csv_delimiter: Option<String>,
        #[serde(default)]
        csv_header: Option<bool>,
    },
}

// ---- Redacting Debug impls (credential safety) -----------------
//
// SourceConfig / TargetConfig / PipelineCliConfig deliberately do
// NOT derive Debug. The decoded TOML carries `url`, `amqp_url`,
// `access_key_id`, and `secret_access_key` fields — printing any
// of them through tracing's `?value` field would leak the
// credential into log aggregators. The hand-written impls below
// redact every secret-bearing field while keeping the
// non-secret bits visible for debuggability.

impl std::fmt::Debug for PipelineCliConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineCliConfig")
            .field("pipeline_name", &self.pipeline_name)
            .field("source_query", &self.source_query)
            .field("idle_pause_ms", &self.idle_pause_ms)
            .field("dead_letter_topic", &self.dead_letter_topic)
            .field("source", &self.source)
            .field("sources", &self.sources)
            .field("target", &self.target)
            .field("targets", &self.targets)
            .field("transform", &self.transform)
            .finish()
    }
}

impl std::fmt::Debug for SourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceConfig::Kafka {
                bootstrap_servers,
                group_id,
                payload_format,
                schema_registry_url,
                schema_registry_basic_auth_user,
                schema_registry_basic_auth_password,
                sasl_plain_username,
                sasl_plain_password,
                sasl_scram_username,
                sasl_scram_password,
                sasl_scram_mechanism,
                msk_iam_region,
            } => f
                .debug_struct("Kafka")
                .field("bootstrap_servers", bootstrap_servers)
                .field("group_id", group_id)
                .field("payload_format", payload_format)
                .field("schema_registry_url", schema_registry_url)
                .field(
                    "schema_registry_basic_auth_user",
                    schema_registry_basic_auth_user,
                )
                .field(
                    "schema_registry_basic_auth_password",
                    &schema_registry_basic_auth_password
                        .as_ref()
                        .map(|_| "<redacted>"),
                )
                .field("sasl_plain_username", sasl_plain_username)
                .field(
                    "sasl_plain_password",
                    &sasl_plain_password.as_ref().map(|_| "<redacted>"),
                )
                .field("sasl_scram_username", sasl_scram_username)
                .field(
                    "sasl_scram_password",
                    &sasl_scram_password.as_ref().map(|_| "<redacted>"),
                )
                .field("sasl_scram_mechanism", sasl_scram_mechanism)
                .field("msk_iam_region", msk_iam_region)
                .finish(),
            SourceConfig::Rabbitmq { amqp_url } => f
                .debug_struct("Rabbitmq")
                .field("amqp_url", &redact_db_url(amqp_url))
                .finish(),
            SourceConfig::Pubsub {
                project_id,
                endpoint,
                anonymous_auth,
            } => f
                .debug_struct("Pubsub")
                .field("project_id", project_id)
                .field("endpoint", endpoint)
                .field("anonymous_auth", anonymous_auth)
                .finish(),
            SourceConfig::Kinesis {
                stream_name,
                region,
                endpoint,
                access_key_id,
                secret_access_key,
            } => f
                .debug_struct("Kinesis")
                .field("stream_name", stream_name)
                .field("region", region)
                .field("endpoint", endpoint)
                .field(
                    "access_key_id",
                    &access_key_id.as_ref().map(|_| "<redacted>"),
                )
                .field(
                    "secret_access_key",
                    &secret_access_key.as_ref().map(|_| "<redacted>"),
                )
                .finish(),
        }
    }
}

impl std::fmt::Debug for TargetConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TargetConfig::Postgres { url, table } => f
                .debug_struct("Postgres")
                .field("url", &redact_db_url(url))
                .field("table", table)
                .finish(),
            TargetConfig::Mysql { url, table } => f
                .debug_struct("Mysql")
                .field("url", &redact_db_url(url))
                .field("table", table)
                .finish(),
            TargetConfig::Sqlite { path, table } => f
                .debug_struct("Sqlite")
                .field("path", path)
                .field("table", table)
                .finish(),
            TargetConfig::Duckdb { path, table } => f
                .debug_struct("Duckdb")
                .field("path", path)
                .field("table", table)
                .finish(),
            TargetConfig::Kafka {
                bootstrap_servers,
                group_id,
                topic,
                message_key_column,
                payload_format,
                schema_registry_url,
                schema_registry_basic_auth_user,
                schema_registry_basic_auth_password,
                sasl_plain_username,
                sasl_plain_password,
                sasl_scram_username,
                sasl_scram_password,
                sasl_scram_mechanism,
                msk_iam_region,
            } => f
                .debug_struct("Kafka")
                .field("bootstrap_servers", bootstrap_servers)
                .field("group_id", group_id)
                .field("topic", topic)
                .field("message_key_column", message_key_column)
                .field("payload_format", payload_format)
                .field("schema_registry_url", schema_registry_url)
                .field(
                    "schema_registry_basic_auth_user",
                    schema_registry_basic_auth_user,
                )
                .field(
                    "schema_registry_basic_auth_password",
                    &schema_registry_basic_auth_password
                        .as_ref()
                        .map(|_| "<redacted>"),
                )
                .field("sasl_plain_username", sasl_plain_username)
                .field(
                    "sasl_plain_password",
                    &sasl_plain_password.as_ref().map(|_| "<redacted>"),
                )
                .field("sasl_scram_username", sasl_scram_username)
                .field(
                    "sasl_scram_password",
                    &sasl_scram_password.as_ref().map(|_| "<redacted>"),
                )
                .field("sasl_scram_mechanism", sasl_scram_mechanism)
                .field("msk_iam_region", msk_iam_region)
                .finish(),
            TargetConfig::Rabbitmq { amqp_url, queue } => f
                .debug_struct("Rabbitmq")
                .field("amqp_url", &redact_db_url(amqp_url))
                .field("queue", queue)
                .finish(),
            TargetConfig::Pubsub {
                project_id,
                endpoint,
                anonymous_auth,
                topic,
            } => f
                .debug_struct("Pubsub")
                .field("project_id", project_id)
                .field("endpoint", endpoint)
                .field("anonymous_auth", anonymous_auth)
                .field("topic", topic)
                .finish(),
            TargetConfig::Kinesis {
                stream_name,
                region,
                endpoint,
                access_key_id,
                secret_access_key,
                partition_key_prefix,
            } => f
                .debug_struct("Kinesis")
                .field("stream_name", stream_name)
                .field("region", region)
                .field("endpoint", endpoint)
                .field(
                    "access_key_id",
                    &access_key_id.as_ref().map(|_| "<redacted>"),
                )
                .field(
                    "secret_access_key",
                    &secret_access_key.as_ref().map(|_| "<redacted>"),
                )
                .field("partition_key_prefix", partition_key_prefix)
                .finish(),
            TargetConfig::DeltaLocal {
                path,
                table,
                partition_by,
            } => f
                .debug_struct("DeltaLocal")
                .field("path", path)
                .field("table", table)
                .field("partition_by", partition_by)
                .finish(),
            TargetConfig::DeltaS3 {
                endpoint,
                bucket,
                prefix,
                region,
                access_key_id: _,
                secret_access_key: _,
                table,
                partition_by,
            } => f
                .debug_struct("DeltaS3")
                .field("endpoint", endpoint)
                .field("bucket", bucket)
                .field("prefix", prefix)
                .field("region", region)
                .field("access_key_id", &"<redacted>")
                .field("secret_access_key", &"<redacted>")
                .field("table", table)
                .field("partition_by", partition_by)
                .finish(),
            TargetConfig::ObjectStoreLocal {
                path,
                format,
                prefix,
                parquet_compression,
                csv_delimiter,
                csv_header,
            } => f
                .debug_struct("ObjectStoreLocal")
                .field("path", path)
                .field("format", format)
                .field("prefix", prefix)
                .field("parquet_compression", parquet_compression)
                .field("csv_delimiter", csv_delimiter)
                .field("csv_header", csv_header)
                .finish(),
            TargetConfig::ObjectStoreS3 {
                endpoint,
                bucket,
                region,
                access_key_id: _,
                secret_access_key: _,
                format,
                prefix,
                parquet_compression,
                csv_delimiter,
                csv_header,
            } => f
                .debug_struct("ObjectStoreS3")
                .field("endpoint", endpoint)
                .field("bucket", bucket)
                .field("region", region)
                .field("access_key_id", &"<redacted>")
                .field("secret_access_key", &"<redacted>")
                .field("format", format)
                .field("prefix", prefix)
                .field("parquet_compression", parquet_compression)
                .field("csv_delimiter", csv_delimiter)
                .field("csv_header", csv_header)
                .finish(),
        }
    }
}

/// CLI-side mirror of [`ObjectFormat`]. Defined separately so we
/// can derive `Deserialize` without touching core (where the
/// enum has no serde derives yet).
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectFormatConfig {
    Parquet,
    Csv,
    Orc,
    /// `jsonl` (newline-delimited JSON), matching the framework's
    /// existing file-suffix convention.
    #[serde(alias = "jsonl")]
    JsonLines,
}

impl From<ObjectFormatConfig> for ObjectFormat {
    fn from(f: ObjectFormatConfig) -> Self {
        match f {
            ObjectFormatConfig::Parquet => ObjectFormat::Parquet,
            ObjectFormatConfig::Csv => ObjectFormat::Csv,
            ObjectFormatConfig::Orc => ObjectFormat::Orc,
            ObjectFormatConfig::JsonLines => ObjectFormat::JsonLines,
        }
    }
}

/// `(schema, name)` pair used by DB targets. Maps onto
/// [`TargetTable`].
#[derive(Debug, Clone, Deserialize)]
pub struct TableSpecConfig {
    #[serde(default)]
    pub schema: String,
    pub name: String,
}

impl PipelineCliConfig {
    /// Parse a TOML config from a string.
    pub fn from_toml_str(s: &str) -> Result<Self, ConfigError> {
        let cfg: Self = toml::from_str(s).map_err(|e| ConfigError::Parse(e.to_string()))?;
        cfg.validate_target_shape()?;
        cfg.validate_source_shape()?;
        if let Some(ss) = &cfg.state_store {
            ss.validate()?;
        }
        cfg.validate_state_store_session_pairing()?;
        cfg.validate_join_config()?;
        cfg.validate_transform_on_error()?;
        cfg.validate_transform_dialect()?;
        cfg.warn_on_stateful_in_memory_store();
        Ok(cfg)
    }

    /// Σ.A2 PR 1: validate `[transform] dialect` parses into a known
    /// [`ematix_flow_core::dialect::Dialect`] variant. Doesn't run the
    /// translator — that happens at pipeline-build time. Failing fast
    /// here means a typo (`dialect = "trinos"`) errors at config load
    /// rather than at first batch.
    fn validate_transform_dialect(&self) -> Result<(), ConfigError> {
        let Some(t) = &self.transform else {
            return Ok(());
        };
        let Some(s) = t.dialect.as_deref() else {
            return Ok(());
        };
        s.parse::<ematix_flow_core::dialect::Dialect>()
            .map(|_| ())
            .map_err(|e| ConfigError::Parse(format!("[transform] {e}")))
    }

    /// Phase 39.5a P2.15: validate `[transform] on_error` value.
    fn validate_transform_on_error(&self) -> Result<(), ConfigError> {
        let Some(t) = &self.transform else {
            return Ok(());
        };
        match t.on_error.as_str() {
            "fail" | "drop" | "dlq" => Ok(()),
            other => Err(ConfigError::Parse(format!(
                "[transform] on_error = {other:?} not supported \
                 (use \"fail\", \"drop\", or \"dlq\")"
            ))),
        }
    }

    /// Phase 39.5a follow-up (P1.10): emit a loud `tracing::warn!`
    /// when a stateful pipeline (session window or stream-stream
    /// join) is paired with `[state_store] kind = "in_memory"`.
    /// In-memory state is process-local; restart wipes everything,
    /// which silently breaks at-least-once guarantees and produces
    /// duplicate emits or lost windows.
    ///
    /// Tests use `kind = "in_memory"` everywhere; the warning skips
    /// when no stateful transform is configured so the test stream
    /// stays clean.
    fn warn_on_stateful_in_memory_store(&self) {
        let in_memory = matches!(self.state_store, Some(StateStoreConfig::InMemory { .. }));
        if !in_memory {
            return;
        }
        let has_session = self
            .transform
            .as_ref()
            .and_then(|t| t.window.as_ref())
            .map(|w| matches!(w.kind, WindowKindToml::Session))
            .unwrap_or(false);
        let has_join = self
            .transform
            .as_ref()
            .map(|t| t.join.is_some())
            .unwrap_or(false);
        if has_session || has_join {
            tracing::warn!(
                pipeline = %self.pipeline_name,
                "[state_store] kind = \"in_memory\" with a {} — state is \
                 process-local and will be lost on restart, breaking the \
                 at-least-once guarantee. Use kind = \"postgres\" for \
                 production deployments. (Set RUST_LOG=warn to silence \
                 if intentional.)",
                if has_session { "session window" } else { "stream-stream join" }
            );
        }
    }

    /// Phase 39.5b: cross-validation for `[transform.join]`.
    ///
    /// - `[transform.join]` and `[transform.window]` are mutually
    ///   exclusive — joins consume two sources directly; windows
    ///   work on a merged single-source stream.
    /// - `[transform.join]` requires the multi-source form
    ///   (`[[sources]]`) with at least two entries; the single
    ///   `[source]` form has nothing to route.
    /// - `[transform.join] left_source` / `right_source` must each
    ///   match the `name` field of a configured source.
    /// - `[transform.join]` requires `[state_store]` for the same
    ///   reason sessions do — buffer state must survive restart.
    /// - `[transform.join] sql` (the pre-stage) is rejected;
    ///   joins compose with downstream transforms via a separate
    ///   pipeline, not via an inner SQL.
    fn validate_join_config(&self) -> Result<(), ConfigError> {
        let Some(transform) = &self.transform else {
            return Ok(());
        };
        let Some(join) = &transform.join else {
            return Ok(());
        };
        if transform.window.is_some() {
            return Err(ConfigError::Parse(
                "[transform.join] and [transform.window] are mutually exclusive — \
                 a single transform stage runs one or the other"
                    .into(),
            ));
        }
        if !transform.sql.is_empty() {
            return Err(ConfigError::Parse(
                "[transform.join] does not accept a `sql` pre-stage — joins consume \
                 two sources directly; chain a separate transform downstream of the \
                 join's output if SQL processing is needed"
                    .into(),
            ));
        }
        if self.sources.is_empty() {
            return Err(ConfigError::Parse(
                "[transform.join] requires the multi-source form `[[sources]]` with \
                 at least two entries (left + right); the single-source `[source]` \
                 form has no routing target"
                    .into(),
            ));
        }
        // Source identity is the per-source `query` field — that's
        // what the pipeline passes as `BatchContext::source_id`.
        let queries: Vec<&str> = self.sources.iter().map(|s| s.query.as_str()).collect();
        if !queries.contains(&join.left_source.as_str()) {
            return Err(ConfigError::Parse(format!(
                "[transform.join] left_source = {:?} doesn't match any [[sources]] \
                 query (configured: {:?})",
                join.left_source, queries
            )));
        }
        if !queries.contains(&join.right_source.as_str()) {
            return Err(ConfigError::Parse(format!(
                "[transform.join] right_source = {:?} doesn't match any [[sources]] \
                 query (configured: {:?})",
                join.right_source, queries
            )));
        }
        if self.state_store.is_none() {
            return Err(ConfigError::Parse(
                "[transform.join] requires a [state_store] block — join buffers \
                 must survive restart for at-least-once semantics"
                    .into(),
            ));
        }
        // Validate every configured source supports seek_to (same
        // rule as session pipelines).
        self.validate_sources_support_seek_to()?;
        Ok(())
    }

    /// Phase 39.5a PR 3: cross-validation between
    /// `[transform.window]` and `[state_store]`.
    ///
    /// - `kind = "session"` requires a `[state_store]` block (state
    ///   persistence is mandatory for sessions because in-flight
    ///   session state can be unbounded; see
    ///   `docs/PHASE_39_5_SESSIONS.md`).
    /// - `[state_store]` is rejected on tumbling/hopping configs —
    ///   PR 3 does not retrofit those to use the state store.
    /// - `count_distinct mode = "approximate"` (HLL+) is rejected
    ///   in stateful sessions — `HyperLogLogPlus` keeps its
    ///   register state in private fields and isn't postcard-
    ///   serializable. `mode = "exact"` works (P1.9).
    fn validate_state_store_session_pairing(&self) -> Result<(), ConfigError> {
        // Only the session-with-state_store combination has hard
        // rules. `[state_store]` alone is permitted (it's a no-op
        // for non-session pipelines but harmless); non-session
        // windows simply ignore it. Tumbling/hopping pipelines with
        // `[state_store]` are rejected because PR 3 doesn't
        // retrofit state persistence to those kinds.
        let Some(window) = self.transform.as_ref().and_then(|t| t.window.as_ref()) else {
            return Ok(());
        };
        match window.kind {
            WindowKindToml::Session => {
                if self.state_store.is_none() {
                    return Err(ConfigError::Parse(
                        "[transform.window] kind = \"session\" requires a \
                         [state_store] block — session pipelines need durable per-key \
                         state to recover correctly across restarts"
                            .into(),
                    ));
                }
                for agg in &window.aggregations {
                    if agg.agg == "count_distinct" {
                        // P1.9: only the exact-mode HashSet is
                        // serializable. Approximate (HLL+) keeps
                        // its register state in private upstream
                        // fields. Default mode is "approximate";
                        // accept "exact" here.
                        let mode = agg.mode.as_deref().unwrap_or("approximate");
                        if mode != "exact" {
                            return Err(ConfigError::Parse(format!(
                                "[transform.window] aggregation `{}` (count_distinct \
                                 mode = {:?}) is not supported with `kind = \"session\"` \
                                 + [state_store] — switch to mode = \"exact\" with a \
                                 bounded max_distinct_values_per_group, or drop the \
                                 aggregation",
                                agg.alias, mode,
                            )));
                        }
                    }
                }
                // Validate seek_to support across configured sources.
                self.validate_sources_support_seek_to()?;
            }
            WindowKindToml::Tumbling | WindowKindToml::Hopping => {
                if self.state_store.is_some() {
                    return Err(ConfigError::Parse(
                        "[state_store] is currently only valid with \
                         `kind = \"session\"` — tumbling and hopping windows rebuild \
                         state from source replay on restart and don't need a state \
                         store"
                            .into(),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Phase 39.5a P1.7a: All configured sources must either
    /// support client-side `seek_to` (Kafka) or use broker-tracked
    /// offsets via the manual-ack model (Pub/Sub, RabbitMQ). All
    /// four current `SourceConfig` variants are covered; this match
    /// stays exhaustive so adding a new variant forces a compile-time
    /// decision on whether it supports stateful resume.
    fn validate_sources_support_seek_to(&self) -> Result<(), ConfigError> {
        let check = |s: &SourceConfig| -> Result<(), ConfigError> {
            match s {
                // Client-side seek (StateStore-tracked offsets).
                SourceConfig::Kafka { .. } => Ok(()),
                // P1.7b: Kinesis client-side seek via per-shard
                // sequence numbers (`AfterSequenceNumber` iterator).
                SourceConfig::Kinesis { .. } => Ok(()),
                // Broker-tracked offsets — `seek_to` is a no-op,
                // ack stream IS the offset.
                SourceConfig::Pubsub { .. } => Ok(()),
                SourceConfig::Rabbitmq { .. } => Ok(()),
            }
        };
        if let Some(s) = &self.source {
            check(s)?;
        }
        for s in &self.sources {
            check(&s.source)?;
        }
        Ok(())
    }

    /// Read + parse a TOML config from a file path.
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let s = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::Io(format!("read {}: {e}", path.display())))?;
        Self::from_toml_str(&s)
    }

    /// Form-agnostic accessor: returns the single `[target]` as a
    /// 1-element slice or the full `[[targets]]` array.
    pub fn targets(&self) -> Vec<&TargetConfig> {
        if let Some(t) = &self.target {
            return vec![t];
        }
        self.targets.iter().collect()
    }

    /// Π.5: enumerate inline-credential patterns in this config that
    /// should trigger the deprecation warning. Each entry is a
    /// human-readable pointer ("[target] Postgres URL contains an
    /// inline password — move it to the connection registry") that
    /// the binary surfaces via `tracing::warn!`. Empty list means
    /// the config has nothing to flag (typed-Python-driven shapes,
    /// passwordless local URLs, env-var-interpolated DSNs).
    ///
    /// Detection is conservative: a URL without a password segment,
    /// an `access_key_id` without a `secret_access_key`, etc. don't
    /// produce findings. This matches what users would normally
    /// pull through env-vars or a connection registry.
    pub fn inline_credential_findings(&self) -> Vec<String> {
        let mut findings: Vec<String> = Vec::new();

        // Source-side findings.
        let mut source_iter: Vec<&SourceConfig> = Vec::new();
        if let Some(s) = &self.source {
            source_iter.push(s);
        }
        for s in &self.sources {
            source_iter.push(&s.source);
        }
        for s in source_iter {
            scan_source_for_credentials(s, &mut findings);
        }

        // Target-side findings.
        for t in self.targets() {
            scan_target_for_credentials(t, &mut findings);
        }

        findings
    }

    fn validate_source_shape(&self) -> Result<(), ConfigError> {
        match (self.source.is_some(), !self.sources.is_empty()) {
            (true, true) => Err(ConfigError::Parse(
                "pipeline config sets both `source` and `sources` — pick one form".into(),
            )),
            (false, false) => Err(ConfigError::Parse(
                "pipeline config has no source: set `[source]` or `[[sources]]`".into(),
            )),
            (true, false) => {
                if self.source_query.is_empty() {
                    Err(ConfigError::Parse(
                        "pipeline config: `source_query` is required when using \
                         the single `[source]` form"
                            .into(),
                    ))
                } else {
                    Ok(())
                }
            }
            (false, true) => Ok(()),
        }
    }

    fn validate_target_shape(&self) -> Result<(), ConfigError> {
        match (self.target.is_some(), !self.targets.is_empty()) {
            (true, true) => Err(ConfigError::Parse(
                "pipeline config sets both `target` and `targets` — pick one form".into(),
            )),
            (false, false) => Err(ConfigError::Parse(
                "pipeline config has no target: set `[target]` or `[[targets]]`".into(),
            )),
            _ => Ok(()),
        }
    }

    /// Convert one source variant to a concrete `Arc<dyn Backend>`.
    /// Internal helper shared by the single- and multi-source paths.
    fn build_one_source(source: &SourceConfig) -> Result<Arc<dyn Backend>, BackendError> {
        match source {
            SourceConfig::Kafka {
                bootstrap_servers,
                group_id,
                payload_format,
                schema_registry_url,
                schema_registry_basic_auth_user,
                schema_registry_basic_auth_password,
                sasl_plain_username,
                sasl_plain_password,
                sasl_scram_username,
                sasl_scram_password,
                sasl_scram_mechanism,
                msk_iam_region,
            } => {
                let mut b = KafkaBackend::open(bootstrap_servers, group_id.as_deref())?;
                if let Some(fmt) = payload_format {
                    b = b.with_payload_format(parse_kafka_payload_format(fmt)?);
                }
                if let Some(url) = schema_registry_url {
                    b = b.with_schema_registry_url(url);
                }
                b = apply_sr_basic_auth(
                    b,
                    schema_registry_basic_auth_user.as_deref(),
                    schema_registry_basic_auth_password.as_deref(),
                )?;
                b = apply_kafka_auth(
                    b,
                    sasl_plain_username.as_deref(),
                    sasl_plain_password.as_deref(),
                    sasl_scram_username.as_deref(),
                    sasl_scram_password.as_deref(),
                    sasl_scram_mechanism.as_deref(),
                    msk_iam_region.as_deref(),
                )?;
                Ok(Arc::new(b))
            }
            SourceConfig::Rabbitmq { amqp_url } => {
                let b = RabbitMQBackend::open(amqp_url)?;
                Ok(Arc::new(b))
            }
            SourceConfig::Pubsub {
                project_id,
                endpoint,
                anonymous_auth,
            } => {
                let mut b = PubSubBackend::open(project_id)?;
                if let Some(ep) = endpoint {
                    b = b.with_endpoint(ep);
                }
                if *anonymous_auth {
                    b = b.with_anonymous_auth();
                }
                Ok(Arc::new(b))
            }
            SourceConfig::Kinesis {
                stream_name,
                region,
                endpoint,
                access_key_id,
                secret_access_key,
            } => Ok(Arc::new(build_kinesis_backend(
                stream_name,
                region.as_deref(),
                endpoint.as_deref(),
                access_key_id.as_deref(),
                secret_access_key.as_deref(),
            )?)),
        }
    }

    /// Build the first source (single-source convenience). Errors
    /// when the config uses the multi-source `[[sources]]` form
    /// with zero entries.
    pub fn build_source(&self) -> Result<Arc<dyn Backend>, BackendError> {
        let sources = self.build_sources()?;
        sources
            .into_iter()
            .next()
            .map(|(b, _)| b)
            .ok_or_else(|| BackendError::Other("no source configured".into()))
    }

    /// Π.4b-2: build every configured source. Returns a 1-element
    /// Vec for the legacy `[source]` + `source_query` form, or one
    /// `(backend, query)` per `[[sources]]` entry.
    #[allow(clippy::type_complexity)]
    pub fn build_sources(&self) -> Result<Vec<(Arc<dyn Backend>, String)>, BackendError> {
        if let Some(s) = &self.source {
            let backend = Self::build_one_source(s)?;
            return Ok(vec![(backend, self.source_query.clone())]);
        }
        let mut out: Vec<(Arc<dyn Backend>, String)> = Vec::with_capacity(self.sources.len());
        for entry in &self.sources {
            let backend = Self::build_one_source(&entry.source)?;
            out.push((backend, entry.query.clone()));
        }
        Ok(out)
    }

    /// Convert the target variant to a concrete `Arc<dyn Backend>`
    /// + the [`TargetTable`] used by the pipeline.
    ///
    /// Streaming-sink targets (Kafka / Pub/Sub / Kinesis / RabbitMQ)
    /// project their per-kind name field onto `TargetTable.name`.
    ///
    /// When the config uses `[[targets]]`, this returns the first
    /// element. Multi-target callers should use
    /// [`Self::build_targets`] (Vec-returning) instead.
    pub async fn build_target(&self) -> Result<(Arc<dyn Backend>, TargetTable), BackendError> {
        self.build_targets()
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| BackendError::Other("no target configured".into()))
    }

    /// Π.4a-2: build every configured target. Returns one
    /// (backend, target_table) pair per `[[targets]]` entry, or a
    /// single-element Vec when the legacy `[target]` form is used.
    /// Targets are constructed sequentially so a single
    /// connection-time failure short-circuits before the framework
    /// holds half-initialized handles.
    pub async fn build_targets(
        &self,
    ) -> Result<Vec<(Arc<dyn Backend>, TargetTable)>, BackendError> {
        let mut out: Vec<(Arc<dyn Backend>, TargetTable)> = Vec::new();
        for target in self.targets() {
            out.push(Self::build_one_target(target).await?);
        }
        Ok(out)
    }

    async fn build_one_target(
        target: &TargetConfig,
    ) -> Result<(Arc<dyn Backend>, TargetTable), BackendError> {
        match target {
            TargetConfig::Postgres { url, table } => {
                let pool = PgPool::connect(url).await?;
                let b = PostgresBackend::new(Arc::new(pool), url.clone());
                Ok((Arc::new(b), table.into()))
            }
            TargetConfig::Mysql { url, table } => {
                let b = MySQLBackend::open(url)?;
                Ok((Arc::new(b), table.into()))
            }
            TargetConfig::Sqlite { path, table } => {
                let b = SQLiteBackend::open(path)?;
                Ok((Arc::new(b), table.into()))
            }
            TargetConfig::Duckdb { path, table } => {
                let b = DuckDBBackend::open(path)?;
                Ok((Arc::new(b), table.into()))
            }
            TargetConfig::Kafka {
                bootstrap_servers,
                group_id,
                topic,
                message_key_column,
                payload_format,
                schema_registry_url,
                schema_registry_basic_auth_user,
                schema_registry_basic_auth_password,
                sasl_plain_username,
                sasl_plain_password,
                sasl_scram_username,
                sasl_scram_password,
                sasl_scram_mechanism,
                msk_iam_region,
            } => {
                let mut b = KafkaBackend::open(bootstrap_servers, group_id.as_deref())?;
                if let Some(col) = message_key_column {
                    b = b.with_message_key_column(col);
                }
                if let Some(fmt) = payload_format {
                    b = b.with_payload_format(parse_kafka_payload_format(fmt)?);
                }
                if let Some(url) = schema_registry_url {
                    b = b.with_schema_registry_url(url);
                }
                b = apply_sr_basic_auth(
                    b,
                    schema_registry_basic_auth_user.as_deref(),
                    schema_registry_basic_auth_password.as_deref(),
                )?;
                b = apply_kafka_auth(
                    b,
                    sasl_plain_username.as_deref(),
                    sasl_plain_password.as_deref(),
                    sasl_scram_username.as_deref(),
                    sasl_scram_password.as_deref(),
                    sasl_scram_mechanism.as_deref(),
                    msk_iam_region.as_deref(),
                )?;
                Ok((
                    Arc::new(b),
                    TargetTable {
                        schema: String::new(),
                        name: topic.clone(),
                    },
                ))
            }
            TargetConfig::Rabbitmq { amqp_url, queue } => {
                let b = RabbitMQBackend::open(amqp_url)?;
                Ok((
                    Arc::new(b),
                    TargetTable {
                        schema: String::new(),
                        name: queue.clone(),
                    },
                ))
            }
            TargetConfig::Pubsub {
                project_id,
                endpoint,
                anonymous_auth,
                topic,
            } => {
                let mut b = PubSubBackend::open(project_id)?;
                if let Some(ep) = endpoint {
                    b = b.with_endpoint(ep);
                }
                if *anonymous_auth {
                    b = b.with_anonymous_auth();
                }
                Ok((
                    Arc::new(b),
                    TargetTable {
                        schema: String::new(),
                        name: topic.clone(),
                    },
                ))
            }
            TargetConfig::Kinesis {
                stream_name,
                region,
                endpoint,
                access_key_id,
                secret_access_key,
                partition_key_prefix,
            } => Ok((
                Arc::new(build_kinesis_backend(
                    stream_name,
                    region.as_deref(),
                    endpoint.as_deref(),
                    access_key_id.as_deref(),
                    secret_access_key.as_deref(),
                )?),
                TargetTable {
                    schema: String::new(),
                    name: partition_key_prefix.clone(),
                },
            )),
            TargetConfig::DeltaLocal {
                path,
                table,
                partition_by,
            } => {
                let mut b = DeltaBackend::open_local(path)?;
                if !partition_by.is_empty() {
                    b = b.with_partition_columns(partition_by.clone());
                }
                Ok((Arc::new(b), table.into()))
            }
            TargetConfig::DeltaS3 {
                endpoint,
                bucket,
                prefix,
                region,
                access_key_id,
                secret_access_key,
                table,
                partition_by,
            } => {
                let mut b = DeltaBackend::open_s3(
                    endpoint,
                    bucket,
                    prefix,
                    region,
                    access_key_id,
                    secret_access_key,
                )?;
                if !partition_by.is_empty() {
                    b = b.with_partition_columns(partition_by.clone());
                }
                Ok((Arc::new(b), table.into()))
            }
            TargetConfig::ObjectStoreLocal {
                path,
                format,
                prefix,
                parquet_compression,
                csv_delimiter,
                csv_header,
            } => {
                let opts = build_object_write_options(
                    parquet_compression.as_deref(),
                    csv_delimiter.as_deref(),
                    *csv_header,
                )?;
                let b = ObjectStoreBackend::open_local(path, (*format).into())?
                    .with_write_options(opts);
                Ok((
                    Arc::new(b),
                    TargetTable {
                        schema: String::new(),
                        name: prefix.clone(),
                    },
                ))
            }
            TargetConfig::ObjectStoreS3 {
                endpoint,
                bucket,
                region,
                access_key_id,
                secret_access_key,
                format,
                prefix,
                parquet_compression,
                csv_delimiter,
                csv_header,
            } => {
                let opts = build_object_write_options(
                    parquet_compression.as_deref(),
                    csv_delimiter.as_deref(),
                    *csv_header,
                )?;
                let b = ObjectStoreBackend::open_s3(
                    endpoint,
                    bucket,
                    region,
                    access_key_id,
                    secret_access_key,
                    (*format).into(),
                )?
                .with_write_options(opts);
                Ok((
                    Arc::new(b),
                    TargetTable {
                        schema: String::new(),
                        name: prefix.clone(),
                    },
                ))
            }
        }
    }

    /// Build the [`StreamingPipelineConfig`] used by the runner,
    /// using pre-loaded lookups (see [`Self::load_lookups`]).
    /// Callers that don't use lookups can pass an empty Vec. Doesn't
    /// auto-wire `WindowedMetrics` into the pipeline registry — the
    /// CLI runner does that via
    /// [`Self::streaming_config_with_lookups_and_metrics`] using a
    /// pre-constructed [`StreamingPipelineMetricsCounters`].
    pub fn streaming_config_with_lookups(
        &self,
        target: TargetTable,
        lookups: Vec<LookupTable>,
    ) -> StreamingPipelineConfig {
        self.streaming_config_with_lookups_and_metrics(target, lookups, None)
    }

    /// Phase 39.4 PR 2b: like [`Self::streaming_config_with_lookups`]
    /// but accepts a borrowed registry so a windowed transform's
    /// [`WindowedMetrics`] can register into the same Prometheus
    /// registry the pipeline will own. Pass `None` for the registry
    /// to skip metrics auto-wiring (windowed transform still works
    /// correctly; per-window counters/gauges just don't surface in
    /// `/metrics`).
    pub fn streaming_config_with_lookups_and_metrics(
        &self,
        target: TargetTable,
        lookups: Vec<LookupTable>,
        metrics_registry: Option<&prometheus::Registry>,
    ) -> StreamingPipelineConfig {
        let mut cfg = StreamingPipelineConfig::new(
            self.source_query.clone(),
            target,
            self.pipeline_name.clone(),
        );
        cfg.idle_pause_ms = self.idle_pause_ms;
        cfg.mode = WriteMode::Append;
        if let Some(dlt) = &self.dead_letter_topic {
            cfg = cfg.with_dead_letter_topic(dlt.clone());
        }
        if let Some(t) = &self.transform {
            // SQL pre-stage. Empty `sql` means "no pre-stage" — only
            // valid when a window block is configured (windowed
            // transform doesn't require a SQL inner).
            let inner_sql: Option<Arc<LazySqlTransform>> = if t.sql.is_empty() {
                None
            } else {
                // Σ.A2 PR 1: translate from the source dialect into
                // DataFusion's native dialect before handing the SQL
                // to the transform layer. Default `None`/`"datafusion"`
                // is pass-through (zero-cost). `validate_transform_
                // dialect` already rejected unknown strings at config
                // load; the only failure path here is `Spark` /
                // `DuckDb`'s NotImplemented stub (PR 2 / 5 land them).
                let dialect: ematix_flow_core::dialect::Dialect = t
                    .dialect
                    .as_deref()
                    .unwrap_or("datafusion")
                    .parse()
                    .expect("dialect string already validated at config-load");
                let translated = ematix_flow_core::dialect::translate(&t.sql, dialect)
                    .expect("dialect translation failed (Σ.A2 PR 2/5 fills the gap)");
                Some(Arc::new(LazySqlTransform::new_with_lookups(
                    translated, lookups,
                )))
            };
            // Windowed / join wrapper, if configured. Otherwise the
            // SQL pre-stage IS the transform.
            let wrapped: Arc<dyn BatchTransform> = if let Some(join_toml) = &t.join {
                // Phase 39.5b: stream-stream join. No SQL pre-stage
                // (cross-validation rejects setting both); routes per
                // source via `BatchContext::source_id`.
                let join_cfg = join_toml_to_core(join_toml).expect("join config translation");
                let jt = ematix_flow_core::join::TimeWindowedJoinTransform::new(join_cfg)
                    .expect("join transform construction");
                Arc::new(jt)
            } else {
                match &t.window {
                    None => match inner_sql {
                        Some(t) => t,
                        None => {
                            // Empty SQL + no window + no join: nothing
                            // to do — skip transform attachment.
                            return cfg;
                        }
                    },
                    Some(window_toml) => {
                        let window_cfg =
                            window_toml_to_core(window_toml).expect("window config translation");
                        let mut wt = WindowedAggregateTransform::new(window_cfg, inner_sql)
                            .expect("windowed transform construction");
                        if let Some(reg) = metrics_registry {
                            let metrics = WindowedMetrics::new(reg, &self.pipeline_name)
                                .expect("WindowedMetrics registration");
                            wt = wt.with_metrics(metrics);
                        }
                        Arc::new(wt)
                    }
                }
            };
            cfg = cfg.with_transform(wrapped);
            // Phase 39.4 / 39.5b: stateful transforms (window or
            // join) need watermark machinery. Auto-enable, then let
            // an explicit `[watermark]` block (Π.1) override below.
            if t.window.is_some() || t.join.is_some() {
                cfg = cfg.with_watermark(WatermarkConfig::default());
            }
            // Phase 39.5a P2.15: transform-error policy. Validated
            // at config-load time; convert to the typed enum.
            let policy = match t.on_error.as_str() {
                "fail" => ematix_flow_core::streaming::TransformErrorPolicy::Fail,
                "drop" => ematix_flow_core::streaming::TransformErrorPolicy::Drop,
                "dlq" => ematix_flow_core::streaming::TransformErrorPolicy::Dlq,
                other => panic!("[transform] on_error = {other:?} not supported"),
            };
            cfg = cfg.with_transform_on_error(policy);
        }
        // Π.1 advanced-knob: `[watermark]` block overrides any
        // auto-enabled defaults (or enables watermarking for
        // pipelines that don't have a window/join — useful when a
        // user wants per-source watermark for downstream
        // observability without buffering aggregates).
        if let Some(wm) = &self.watermark {
            let mut wcfg = WatermarkConfig::default();
            if let Some(l) = wm.lateness_ms {
                wcfg.lateness_ms = l;
            }
            if let Some(i) = wm.source_idleness_ms {
                wcfg.source_idleness_ms = i;
            }
            cfg = cfg.with_watermark(wcfg);
        }
        cfg
    }

    /// Synchronous shorthand for [`Self::streaming_config_with_lookups`]
    /// when no lookups are configured. Tests + library callers
    /// without DB-loadable lookups stay on this path.
    pub fn streaming_config(&self, target: TargetTable) -> StreamingPipelineConfig {
        self.streaming_config_with_lookups(target, Vec::new())
    }

    /// Π.4b-3: load every configured `[transform.lookups.<name>]`
    /// from its DB backend via a `SELECT * FROM <schema>.<table>`
    /// stream, returning `LookupTable`s ready to attach to the
    /// pipeline's `LazySqlTransform`. Lookups are loaded
    /// sequentially; a single load failure short-circuits the
    /// pipeline before it touches the source.
    pub async fn load_lookups(&self) -> Result<Vec<LookupTable>, BackendError> {
        let Some(transform_cfg) = &self.transform else {
            return Ok(Vec::new());
        };
        let mut out: Vec<LookupTable> = Vec::with_capacity(transform_cfg.lookups.len());
        for (name, lookup_cfg) in &transform_cfg.lookups {
            let (schema, batches) = lookup_cfg.load_once(name).await?;
            out.push(LookupTable::new(name.clone(), schema, batches));
        }
        Ok(out)
    }
}

impl From<&TableSpecConfig> for TargetTable {
    fn from(t: &TableSpecConfig) -> Self {
        TargetTable {
            schema: t.schema.clone(),
            name: t.name.clone(),
        }
    }
}

/// Π.1.4: collapse the per-format option strings off
/// `TargetConfig::ObjectStoreLocal` / `S3` into the typed
/// `ObjectWriteOptions` the core backend expects. Validates each
/// field independently so a user's typo (e.g. `compression =
/// "lzo"`) errors with a list of supported codecs rather than
/// silently falling through to the default.
fn build_object_write_options(
    parquet_compression: Option<&str>,
    csv_delimiter: Option<&str>,
    csv_header: Option<bool>,
) -> Result<ObjectWriteOptions, BackendError> {
    let mut opts = ObjectWriteOptions::default();
    if let Some(s) = parquet_compression {
        opts.parquet_compression = Some(parse_parquet_compression(s)?);
    }
    if let Some(d) = csv_delimiter {
        let bytes = d.as_bytes();
        if bytes.len() != 1 {
            return Err(BackendError::Other(format!(
                "csv_delimiter must be a single ASCII byte, got {d:?}"
            )));
        }
        opts.csv_delimiter = Some(bytes[0]);
    }
    if let Some(h) = csv_header {
        opts.csv_header = Some(h);
    }
    Ok(opts)
}

fn parse_parquet_compression(s: &str) -> Result<ParquetCompression, BackendError> {
    match s {
        "uncompressed" => Ok(ParquetCompression::Uncompressed),
        "snappy" => Ok(ParquetCompression::Snappy),
        "gzip" => Ok(ParquetCompression::Gzip),
        "zstd" => Ok(ParquetCompression::Zstd),
        other => Err(BackendError::Other(format!(
            "unknown parquet_compression {other:?} \
             (supported: \"uncompressed\", \"snappy\", \"gzip\", \"zstd\")"
        ))),
    }
}

/// Apply the Schema Registry basic-auth fields off `SourceConfig::Kafka`
/// / `TargetConfig::Kafka` to a `KafkaBackend`. Both fields must be set
/// or both omitted — a half-set pair is a config error.
fn apply_sr_basic_auth(
    b: KafkaBackend,
    user: Option<&str>,
    password: Option<&str>,
) -> Result<KafkaBackend, BackendError> {
    match (user, password) {
        (None, None) => Ok(b),
        (Some(u), Some(p)) => Ok(b.with_schema_registry_basic_auth(u, p)),
        (Some(_), None) => Err(BackendError::Other(
            "schema_registry_basic_auth_user without \
             schema_registry_basic_auth_password"
                .into(),
        )),
        (None, Some(_)) => Err(BackendError::Other(
            "schema_registry_basic_auth_password without \
             schema_registry_basic_auth_user"
                .into(),
        )),
    }
}

/// Apply the Kafka auth fields off `SourceConfig::Kafka` /
/// `TargetConfig::Kafka` to a `KafkaBackend`. Mutually exclusive:
/// at most one of (SASL/PLAIN, SASL/SCRAM, MSK-IAM) may be
/// configured; setting more than one is a config error caught here
/// so users don't get silently downgraded auth modes from the
/// last builder call winning. PLAIN requires both username +
/// password; SCRAM requires all three (`username`, `password`,
/// `mechanism = "sha-256" | "sha-512"`).
fn apply_kafka_auth(
    mut b: KafkaBackend,
    sasl_plain_username: Option<&str>,
    sasl_plain_password: Option<&str>,
    sasl_scram_username: Option<&str>,
    sasl_scram_password: Option<&str>,
    sasl_scram_mechanism: Option<&str>,
    msk_iam_region: Option<&str>,
) -> Result<KafkaBackend, BackendError> {
    let plain_set = sasl_plain_username.is_some() || sasl_plain_password.is_some();
    let scram_set = sasl_scram_username.is_some()
        || sasl_scram_password.is_some()
        || sasl_scram_mechanism.is_some();
    let iam_set = msk_iam_region.is_some();
    let modes = [plain_set, scram_set, iam_set]
        .into_iter()
        .filter(|x| *x)
        .count();
    if modes > 1 {
        return Err(BackendError::Other(
            "Kafka auth: at most one of (SASL/PLAIN, SASL/SCRAM, MSK-IAM) \
             may be configured; got combined fields"
                .into(),
        ));
    }
    if plain_set {
        let user = sasl_plain_username.ok_or_else(|| {
            BackendError::Other("sasl_plain_password without sasl_plain_username".into())
        })?;
        let pw = sasl_plain_password.ok_or_else(|| {
            BackendError::Other("sasl_plain_username without sasl_plain_password".into())
        })?;
        b = b.with_sasl_plain(user, pw);
    } else if scram_set {
        let user = sasl_scram_username.ok_or_else(|| {
            BackendError::Other(
                "sasl_scram_password / sasl_scram_mechanism without sasl_scram_username".into(),
            )
        })?;
        let pw = sasl_scram_password.ok_or_else(|| {
            BackendError::Other("sasl_scram_username without sasl_scram_password".into())
        })?;
        let mech_str = sasl_scram_mechanism.ok_or_else(|| {
            BackendError::Other(
                "sasl_scram_username/password without sasl_scram_mechanism (\"sha-256\" or \"sha-512\")"
                    .into(),
            )
        })?;
        let mech = match mech_str {
            "sha-256" | "sha256" | "SHA-256" | "SHA256" => {
                ematix_flow_core::kafka_backend::ScramMechanism::Sha256
            }
            "sha-512" | "sha512" | "SHA-512" | "SHA512" => {
                ematix_flow_core::kafka_backend::ScramMechanism::Sha512
            }
            other => {
                return Err(BackendError::Other(format!(
                    "unknown sasl_scram_mechanism {other:?} \
                     (supported: \"sha-256\", \"sha-512\")"
                )));
            }
        };
        b = b.with_sasl_scram(mech, user, pw);
    } else if iam_set {
        let region = msk_iam_region.expect("checked iam_set");
        b = b.with_msk_iam(region);
    }
    Ok(b)
}

/// Π.1: parse the TOML `payload_format = "..."` string into the
/// core's `KafkaPayloadFormat` enum. Surface a typo-friendly error
/// so users see the supported set immediately.
fn parse_kafka_payload_format(s: &str) -> Result<KafkaPayloadFormat, BackendError> {
    match s {
        "json" => Ok(KafkaPayloadFormat::Json),
        "raw_bytes" => Ok(KafkaPayloadFormat::RawBytes),
        "avro" => Ok(KafkaPayloadFormat::Avro),
        "protobuf" => Ok(KafkaPayloadFormat::Protobuf),
        other => Err(BackendError::Other(format!(
            "unknown Kafka payload_format {other:?} \
             (supported: \"json\", \"raw_bytes\", \"avro\", \"protobuf\")"
        ))),
    }
}

fn build_kinesis_backend(
    stream_name: &str,
    region: Option<&str>,
    endpoint: Option<&str>,
    access_key_id: Option<&str>,
    secret_access_key: Option<&str>,
) -> Result<KinesisBackend, BackendError> {
    let mut b = KinesisBackend::open(stream_name)?;
    if let Some(r) = region {
        b = b.with_region(r);
    }
    if let Some(ep) = endpoint {
        b = b.with_endpoint(ep);
    }
    if let (Some(ak), Some(sk)) = (access_key_id, secret_access_key) {
        b = b.with_static_credentials(ak, sk);
    }
    Ok(b)
}

/// Errors surfaced by config parsing / file IO.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("config IO error: {0}")]
    Io(String),
    #[error("config parse error: {0}")]
    Parse(String),
}

/// Errors from the top-level `flow consume` flow.
#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error("{0}")]
    Config(#[from] ConfigError),
    #[error("backend error: {0}")]
    Backend(#[from] BackendError),
    #[error("runtime error: {0}")]
    Runtime(String),
}

/// Optional runtime knobs for [`run_consume`]. CLI flags map onto
/// this; library callers can pass `Default::default()` for the
/// pipeline-only path.
#[derive(Debug, Clone, Default)]
pub struct ConsumeOptions {
    /// When `Some`, spawn a metrics HTTP server bound to
    /// `127.0.0.1:<port>` exposing `/metrics` from the pipeline's
    /// Prometheus registry. The server shares the pipeline's
    /// shutdown signal so both stop together.
    pub metrics_port: Option<u16>,
    /// Externally-provided shutdown signal. When `None`,
    /// [`run_consume_with`] installs the default SIGTERM/SIGINT
    /// handler. Tests + library callers that want to drive
    /// shutdown programmatically (without sending real signals)
    /// pass `Some(signal)`. Use [`ShutdownSignal::new`] to create
    /// the pair and keep the trigger end alive for the duration
    /// of the run.
    pub shutdown_signal: Option<ShutdownSignal>,
}

/// Run a single pipeline to completion (until shutdown). Used by
/// the `flow consume` subcommand and by integration tests.
///
/// Convenience wrapper around [`run_consume_with`] for callers
/// that don't need the runtime knobs.
pub async fn run_consume(config: PipelineCliConfig) -> Result<StreamingPipelineMetrics, CliError> {
    run_consume_with(config, ConsumeOptions::default()).await
}

/// Run a single pipeline with explicit [`ConsumeOptions`]. Spawns
/// the metrics server (if configured) before the pipeline starts,
/// then awaits the pipeline. The server is asked to shut down
/// when the pipeline exits, regardless of how it exited.
pub async fn run_consume_with(
    config: PipelineCliConfig,
    options: ConsumeOptions,
) -> Result<StreamingPipelineMetrics, CliError> {
    let sources = config.build_sources()?;
    let targets = config.build_targets().await?;
    if sources.is_empty() {
        return Err(CliError::Runtime("no source configured".into()));
    }
    // Π.4b-3: load every configured lookup before the pipeline
    // starts. A failure here surfaces immediately instead of
    // showing up mid-stream — a single misconfigured lookup
    // would otherwise stall every batch.
    let lookups = config.load_lookups().await?;
    // streaming_config still wants a single TargetTable for back-
    // compat. Use the first target's — it's the historical shape
    // when only one target is configured, and unused on the multi-
    // target hot path (the pipeline iterates `self.targets`).
    let primary_table = targets
        .first()
        .map(|(_, t)| t.clone())
        .ok_or_else(|| CliError::Runtime("no target configured".into()))?;
    // Phase 39.4 PR 2b: pre-construct the pipeline's metrics
    // counters so a windowed transform's WindowedMetrics can
    // register into the same Prometheus registry. Pass that
    // registry through `streaming_config_with_lookups_and_metrics`,
    // then hand the same counters into `new_multi_source_with_metrics`
    // so we don't double-construct.
    let pipeline_metrics = StreamingPipelineMetricsCounters::new(&config.pipeline_name);
    let mut pipeline_cfg = config.streaming_config_with_lookups_and_metrics(
        primary_table,
        lookups,
        Some(&pipeline_metrics.registry),
    );
    // Phase 39.5a PR 3: instantiate the StateStore (if configured)
    // and attach it to the pipeline. Recovery happens below before
    // `pipeline.run()`.
    let state_store: Option<Arc<dyn ematix_flow_core::state_store::StateStore>> =
        match &config.state_store {
            Some(ss) => Some(ss.build().await?),
            None => None,
        };
    if let Some(s) = state_store.clone() {
        pipeline_cfg = pipeline_cfg.with_state_store(s);
        // Phase 39.5a P1.8: wire the periodic dirty-only checkpoint
        // ticker. The interval comes from `[state_store]
        // checkpoint_interval_ms` (default 60_000 in the CLI's
        // StateStoreConfig). Per-emit commits are still the primary
        // durability path; the ticker is a floor for idle-but-dirty
        // pipelines.
        if let Some(ss) = &config.state_store {
            pipeline_cfg = pipeline_cfg.with_checkpoint_interval_ms(ss.checkpoint_interval_ms());
        }
    }
    let pipeline = StreamingPipeline::new_multi_source_with_metrics(
        sources,
        targets,
        pipeline_cfg,
        pipeline_metrics,
    );

    // Phase 39.5a PR 3: load committed state + offsets, then apply
    // `seek_to` per source and rehydrate the windowed transform.
    if let Some(store) = &state_store {
        pipeline.load_state(store.as_ref()).await?;
    }

    // Pick a shutdown source. Tests + programmatic callers can
    // pass an external signal via ConsumeOptions; otherwise we
    // install the default SIGTERM/SIGINT handler. The signal-
    // listening task's JoinHandle (when we install) drops at the
    // end of this function — fine, the runtime tears it down.
    let (shutdown, _shutdown_handle) = match options.shutdown_signal.clone() {
        Some(signal) => (signal, None),
        None => {
            let (s, h) = install_shutdown_handler();
            (s, Some(h))
        }
    };

    // Phase 39.3: spawn one refresh task per lookup that has
    // `refresh_interval_ms` configured. Tasks share the
    // pipeline's shutdown signal so they exit cleanly when the
    // pipeline does.
    let refresh_handles = if let Some(transform) = pipeline.config.transform.clone() {
        spawn_lookup_refresh_tasks(&config, transform, shutdown.clone())
    } else {
        Vec::new()
    };

    // Optional metrics server. We give it its own ShutdownSignal
    // pair so the pipeline's exit (success OR error) can stop the
    // server before this function returns.
    let (metrics_signal, metrics_trigger) = ShutdownSignal::new();
    let metrics_handle = if let Some(port) = options.metrics_port {
        let addr: SocketAddr =
            format!("127.0.0.1:{port}")
                .parse()
                .map_err(|e: std::net::AddrParseError| {
                    CliError::Runtime(format!("metrics addr parse: {e}"))
                })?;
        let registry = Arc::new(pipeline.metrics_registry().clone());
        let (_bound, handle) = metrics_server::spawn_metrics_server(addr, registry, metrics_signal)
            .await
            .map_err(|e| CliError::Runtime(format!("metrics bind {addr}: {e}")))?;
        Some(handle)
    } else {
        // No server requested — drop the signal pair on the floor.
        drop(metrics_signal);
        None
    };

    let result = pipeline
        .run(shutdown)
        .await
        .map_err(|e| CliError::Runtime(e.to_string()));

    // Stop the metrics server (best effort).
    metrics_trigger.trigger();
    if let Some(handle) = metrics_handle {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
    }
    // Wait for refresh tasks to drain. They observe the pipeline's
    // shutdown signal and exit on the next select! poll. Bound the
    // wait so a hung loader can't stall process exit.
    for handle in refresh_handles {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), handle).await;
    }
    result
}

/// Phase 39.3: spawn one tokio task per lookup that has
/// `refresh_interval_ms` set. Each task: sleeps for the configured
/// interval, re-loads the lookup, and calls
/// `BatchTransform::refresh_lookup` to swap the registered MemTable.
/// Errors are logged via `tracing::warn` and don't terminate the
/// task — a transient DB outage shouldn't crash the pipeline.
fn spawn_lookup_refresh_tasks(
    config: &PipelineCliConfig,
    transform: Arc<dyn BatchTransform>,
    shutdown: ShutdownSignal,
) -> Vec<tokio::task::JoinHandle<()>> {
    let Some(transform_cfg) = &config.transform else {
        return Vec::new();
    };
    let mut handles = Vec::new();
    for (name, lookup_cfg) in &transform_cfg.lookups {
        let Some(interval_ms) = lookup_cfg.refresh_interval_ms() else {
            continue;
        };
        let name = name.clone();
        let lookup_cfg = lookup_cfg.clone();
        let transform = transform.clone();
        let shutdown = shutdown.clone();
        let handle = tokio::spawn(async move {
            run_lookup_refresh_loop(name, lookup_cfg, interval_ms, transform, shutdown).await;
        });
        handles.push(handle);
    }
    handles
}

async fn run_lookup_refresh_loop(
    name: String,
    cfg: LookupConfig,
    interval_ms: u64,
    transform: Arc<dyn BatchTransform>,
    shutdown: ShutdownSignal,
) {
    let interval = std::time::Duration::from_millis(interval_ms);
    loop {
        tokio::select! {
            _ = shutdown.wait() => return,
            _ = tokio::time::sleep(interval) => {}
        }
        match cfg.load_once(&name).await {
            Ok((schema, batches)) => match transform.refresh_lookup(&name, schema, batches).await {
                Ok(()) => {
                    tracing::debug!(lookup = %name, "lookup refreshed");
                }
                Err(e) => {
                    // The "not initialized" case is benign — the
                    // pipeline simply hasn't seen its first batch
                    // yet, so the LazySqlTransform's inner doesn't
                    // exist. With a quiet source + a short refresh
                    // interval this would flood the log at warn
                    // level. Demote to debug; the next interval
                    // retries.
                    let msg = e.to_string();
                    if msg.contains("not initialized") {
                        tracing::debug!(
                            lookup = %name,
                            "lookup refresh deferred (transform not yet initialized)"
                        );
                    } else {
                        tracing::warn!(
                            lookup = %name,
                            error = %e,
                            "lookup refresh swap failed"
                        );
                    }
                }
            },
            Err(e) => {
                tracing::warn!(
                    lookup = %name,
                    error = %e,
                    "lookup re-load failed"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kafka_to_pg_toml() -> &'static str {
        r#"
            pipeline_name = "events-to-pg"
            source_query = "events"
            idle_pause_ms = 250

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"
            group_id = "ematix-flow"

            [target]
            kind = "postgres"
            url = "postgres://localhost/mydb"

            [target.table]
            schema = "public"
            name = "events"
        "#
    }

    #[test]
    fn parses_kafka_to_postgres_config() {
        let cfg = PipelineCliConfig::from_toml_str(kafka_to_pg_toml()).unwrap();
        assert_eq!(cfg.pipeline_name, "events-to-pg");
        assert_eq!(cfg.source_query, "events");
        assert_eq!(cfg.idle_pause_ms, 250);
        match cfg.source.as_ref().expect("source set") {
            SourceConfig::Kafka {
                bootstrap_servers,
                group_id,
                ..
            } => {
                assert_eq!(bootstrap_servers, "localhost:9092");
                assert_eq!(group_id.as_deref(), Some("ematix-flow"));
            }
            other => panic!("expected Kafka source, got {other:?}"),
        }
        match cfg.target.as_ref().unwrap() {
            TargetConfig::Postgres { url, table } => {
                assert_eq!(url, "postgres://localhost/mydb");
                assert_eq!(table.schema, "public");
                assert_eq!(table.name, "events");
            }
            other => panic!("expected Postgres target, got {other:?}"),
        }
    }

    #[test]
    fn parses_transform_lookups_block() {
        let toml = r#"
            pipeline_name = "events-enriched"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "events_enriched"

            [transform]
            sql = "SELECT s.user_id, u.name FROM source s LEFT JOIN users u ON s.user_id = u.id"

            [transform.lookups.users]
            kind = "postgres"
            url = "postgres://localhost/mydb"
            schema = "public"
            table = "users"

            [transform.lookups.products]
            kind = "sqlite"
            path = "/tmp/products.db"
            table = "products"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let t = cfg.transform.as_ref().expect("transform set");
        assert_eq!(t.lookups.len(), 2);
        match t.lookups.get("users").expect("users lookup") {
            LookupConfig::Postgres { schema, table, .. } => {
                assert_eq!(schema, "public");
                assert_eq!(table, "users");
            }
            other => panic!("expected Postgres lookup, got {other:?}"),
        }
        match t.lookups.get("products").expect("products lookup") {
            LookupConfig::Sqlite { path, table, .. } => {
                assert_eq!(path, "/tmp/products.db");
                assert_eq!(table, "products");
            }
            other => panic!("expected Sqlite lookup, got {other:?}"),
        }
    }

    #[test]
    fn lookup_qualified_name_joins_schema_when_set() {
        let lookup = LookupConfig::Postgres {
            url: "postgres://localhost/db".into(),
            schema: "public".into(),
            table: "users".into(),
            refresh_interval_ms: None,
        };
        assert_eq!(lookup.qualified_name(), "public.users");

        let lookup = LookupConfig::Sqlite {
            path: ":memory:".into(),
            schema: "".into(),
            table: "products".into(),
            refresh_interval_ms: None,
        };
        assert_eq!(
            lookup.qualified_name(),
            "products",
            "empty schema falls back to bare table name"
        );
    }

    #[test]
    fn lookup_refresh_interval_ms_zero_is_disabled() {
        let lookup = LookupConfig::Sqlite {
            path: ":memory:".into(),
            schema: "".into(),
            table: "users".into(),
            refresh_interval_ms: Some(0),
        };
        assert!(lookup.refresh_interval_ms().is_none(), "0 ms ⇒ disabled");

        let lookup = LookupConfig::Sqlite {
            path: ":memory:".into(),
            schema: "".into(),
            table: "users".into(),
            refresh_interval_ms: Some(5_000),
        };
        assert_eq!(lookup.refresh_interval_ms(), Some(5_000));
    }

    #[test]
    fn parses_window_block() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "events_per_min"

            [transform]
            sql = "SELECT user_id, amount, _event_ts FROM source"

            [transform.window]
            kind = "tumbling"
            duration_ms = 60000
            event_time_column = "_event_ts"
            group_by = ["user_id"]
            late_data = "drop"
            max_groups_per_window = 1000

            [[transform.window.aggregations]]
            agg = "count"
            as = "n"

            [[transform.window.aggregations]]
            agg = "sum"
            column = "amount"
            as = "amount_sum"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let w = cfg.transform.as_ref().unwrap().window.as_ref().unwrap();
        assert_eq!(w.kind, WindowKindToml::Tumbling);
        assert_eq!(w.duration_ms, 60000);
        assert_eq!(w.group_by, vec!["user_id"]);
        assert_eq!(w.aggregations.len(), 2);
        assert_eq!(w.aggregations[0].agg, "count");
        assert_eq!(w.aggregations[0].alias, "n");
        assert_eq!(w.aggregations[1].column.as_deref(), Some("amount"));

        // Verify streaming_config_with_lookups attaches both the
        // windowed transform and the watermark machinery.
        let table = TargetTable {
            schema: "".into(),
            name: "events_per_min".into(),
        };
        let scfg = cfg.streaming_config_with_lookups(table, Vec::new());
        assert!(
            scfg.transform.is_some(),
            "windowed transform must be attached"
        );
        assert!(
            scfg.watermark.is_some(),
            "windowed pipelines auto-enable watermark machinery"
        );
    }

    #[test]
    fn parses_count_distinct_aggregation() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "out"

            [transform]
            sql = "SELECT user_id, page_url, _event_ts FROM source"

            [transform.window]
            kind = "tumbling"
            duration_ms = 60000
            group_by = ["user_id"]
            max_groups_per_window = 1000

            [[transform.window.aggregations]]
            agg = "count_distinct"
            column = "page_url"
            mode = "approximate"
            as = "unique_pages"

            [[transform.window.aggregations]]
            agg = "count_distinct"
            column = "page_url"
            mode = "exact"
            max_distinct_values_per_group = 10000
            as = "unique_pages_exact"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let aggs = &cfg
            .transform
            .as_ref()
            .unwrap()
            .window
            .as_ref()
            .unwrap()
            .aggregations;
        assert_eq!(aggs.len(), 2);
        assert_eq!(aggs[0].agg, "count_distinct");
        assert_eq!(aggs[0].mode.as_deref(), Some("approximate"));
        assert_eq!(aggs[1].mode.as_deref(), Some("exact"));
        assert_eq!(aggs[1].max_distinct_values_per_group, Some(10000));

        // Build the windowed transform via the CLI path. Construction
        // would fail-fast on missing cap for exact mode if either
        // aggregation were misconfigured.
        let table = TargetTable {
            schema: "".into(),
            name: "out".into(),
        };
        let scfg = cfg.streaming_config_with_lookups(table, Vec::new());
        assert!(scfg.transform.is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn windowed_metrics_register_into_pipeline_registry() {
        // Phase 39.4 PR 2b: confirm the windowed transform's metrics
        // land in the same Prometheus registry the pipeline owns
        // (= what `/metrics` exposes).
        use prometheus::{Encoder, TextEncoder};

        let toml = r#"
            pipeline_name = "metrics-p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "out"

            [transform]
            sql = "SELECT user_id, _event_ts FROM source"

            [transform.window]
            kind = "tumbling"
            duration_ms = 60000
            group_by = ["user_id"]
            max_groups_per_window = 1000

            [[transform.window.aggregations]]
            agg = "count"
            as = "n"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let table = TargetTable {
            schema: "".into(),
            name: "out".into(),
        };
        let pipeline_metrics = StreamingPipelineMetricsCounters::new("metrics-p");
        let _scfg = cfg.streaming_config_with_lookups_and_metrics(
            table,
            Vec::new(),
            Some(&pipeline_metrics.registry),
        );

        let mut buf: Vec<u8> = Vec::new();
        TextEncoder::new()
            .encode(&pipeline_metrics.registry.gather(), &mut buf)
            .unwrap();
        let body = String::from_utf8(buf).unwrap();
        assert!(
            body.contains("ematix_streaming_windows_emitted_total"),
            "windowed counter registered into pipeline registry; got:\n{body}"
        );
        assert!(
            body.contains("ematix_streaming_windows_active"),
            "windowed gauge registered into pipeline registry; got:\n{body}"
        );
    }

    #[test]
    fn parses_reopen_late_data_with_lateness_budget() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "out"

            [transform]
            sql = "SELECT user_id, _event_ts FROM source"

            [transform.window]
            kind = "tumbling"
            duration_ms = 60000
            group_by = ["user_id"]
            late_data = "reopen"
            allowed_lateness_ms = 30000
            max_groups_per_window = 1000

            [[transform.window.aggregations]]
            agg = "count"
            as = "n"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let w = cfg.transform.as_ref().unwrap().window.as_ref().unwrap();
        assert_eq!(w.late_data, "reopen");
        assert_eq!(w.allowed_lateness_ms, Some(30_000));

        // Confirm streaming_config_with_lookups builds the windowed
        // transform without panicking (validates allowed_lateness_ms
        // is plumbed through).
        let table = TargetTable {
            schema: "".into(),
            name: "out".into(),
        };
        let scfg = cfg.streaming_config_with_lookups(table, Vec::new());
        assert!(scfg.transform.is_some());
    }

    #[test]
    fn rejects_reopen_without_allowed_lateness_ms() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "out"

            [transform]
            sql = "SELECT user_id, _event_ts FROM source"

            [transform.window]
            kind = "tumbling"
            duration_ms = 60000
            group_by = ["user_id"]
            late_data = "reopen"
            max_groups_per_window = 1000

            [[transform.window.aggregations]]
            agg = "count"
            as = "n"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let table = TargetTable {
            schema: "".into(),
            name: "out".into(),
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            cfg.streaming_config_with_lookups(table, Vec::new())
        }));
        assert!(result.is_err(), "reopen requires allowed_lateness_ms");
    }

    // ----- Phase 39.5a P2.15: transform on_error policy -----

    #[test]
    fn parses_transform_on_error_drop() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "out"

            [transform]
            sql = "SELECT user_id FROM source"
            on_error = "drop"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        assert_eq!(cfg.transform.as_ref().unwrap().on_error, "drop");
    }

    #[test]
    fn rejects_unknown_transform_on_error() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "out"

            [transform]
            sql = "SELECT user_id FROM source"
            on_error = "yolo"
        "#;
        let err = PipelineCliConfig::from_toml_str(toml).unwrap_err();
        assert!(
            err.to_string().contains("on_error") && err.to_string().contains("yolo"),
            "got: {err}"
        );
    }

    #[test]
    fn transform_on_error_default_is_fail() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "out"

            [transform]
            sql = "SELECT user_id FROM source"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        assert_eq!(cfg.transform.as_ref().unwrap().on_error, "fail");
    }

    // ----- Σ.A2 PR 1: dialect selector -----

    /// `dialect = "spark"` round-trips through TOML parsing today;
    /// the actual translator is wired up in Σ.A2 PR 2. PR 1 only
    /// guarantees that the field is recognized + validated.
    #[test]
    fn transform_dialect_parses_from_toml() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "out"

            [transform]
            sql = "SELECT user_id FROM source"
            dialect = "spark"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        assert_eq!(
            cfg.transform.as_ref().unwrap().dialect.as_deref(),
            Some("spark")
        );
    }

    /// Default — `dialect` field absent — leaves `Option::None` on
    /// the typed config. The build path treats that the same as
    /// `"datafusion"` (pass-through translator).
    #[test]
    fn transform_dialect_defaults_to_none() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "out"

            [transform]
            sql = "SELECT user_id FROM source"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        assert!(cfg.transform.as_ref().unwrap().dialect.is_none());
    }

    /// Unknown dialect strings fail at config-load with a message
    /// listing the valid options. No need to wait until pipeline
    /// build for a typo to surface.
    #[test]
    fn transform_dialect_rejects_unknown_at_config_load() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "out"

            [transform]
            sql = "SELECT user_id FROM source"
            dialect = "trino"
        "#;
        let err = PipelineCliConfig::from_toml_str(toml).expect_err("trino unsupported");
        let msg = format!("{err}");
        assert!(msg.contains("trino"), "must echo bad input; got: {msg}");
        // Must list at least one valid option so the operator can fix
        // it without leaving the error.
        assert!(
            msg.contains("datafusion") || msg.contains("spark") || msg.contains("duckdb"),
            "must list valid dialects; got: {msg}"
        );
    }

    /// Pass-through dialect (`"datafusion"`) builds the streaming
    /// config without surprises. Round-trips the SQL unchanged.
    #[test]
    fn transform_dialect_datafusion_passthrough_builds_pipeline() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "out"

            [transform]
            sql = "SELECT user_id FROM source"
            dialect = "datafusion"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        // Build the streaming config — this exercises the translate()
        // call in the build path. Pass-through must not panic.
        let table = TargetTable {
            schema: "".into(),
            name: "out".into(),
        };
        let scfg = cfg.streaming_config_with_lookups(table, Vec::new());
        assert!(scfg.transform.is_some());
    }

    #[test]
    fn parses_dlq_late_data() {
        // Phase 39.5a P1.6: late_data = "dlq" is now supported.
        // Late rows past `window_end + lateness_us` are routed to
        // the configured `dead_letter_topic` instead of being
        // dropped silently. Without a `dead_letter_topic`, the
        // rows are still discarded (with a tracing warning).
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"
            dead_letter_topic = "events-late"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "out"

            [transform]
            sql = "SELECT user_id, _event_ts FROM source"

            [transform.window]
            kind = "tumbling"
            duration_ms = 60000
            group_by = ["user_id"]
            late_data = "dlq"
            max_groups_per_window = 1000

            [[transform.window.aggregations]]
            agg = "count"
            as = "n"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        assert_eq!(cfg.dead_letter_topic.as_deref(), Some("events-late"));
        let table = TargetTable {
            schema: "".into(),
            name: "out".into(),
        };
        // Construction must not panic — the windowed transform
        // accepts the Dlq policy.
        let _ = cfg.streaming_config_with_lookups(table, Vec::new());
    }

    #[test]
    fn parses_lookup_with_refresh_interval_ms() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "events"

            [transform]
            sql = "SELECT s.user_id, u.name FROM source s LEFT JOIN users u ON s.user_id = u.id"

            [transform.lookups.users]
            kind = "postgres"
            url = "postgres://localhost/mydb"
            table = "users"
            refresh_interval_ms = 30000
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let users = cfg
            .transform
            .as_ref()
            .unwrap()
            .lookups
            .get("users")
            .unwrap();
        assert_eq!(users.refresh_interval_ms(), Some(30_000));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn refresh_task_swaps_sqlite_backed_lookup_mid_pipeline() {
        // End-to-end exercise of the Phase 39.3 refresh path:
        //   1. SQLite-backed lookup loaded at startup.
        //   2. LazySqlTransform initialized via a first transform() call.
        //   3. Underlying SQLite mutated.
        //   4. spawn_lookup_refresh_tasks reloads on its interval +
        //      atomically swaps the registered MemTable.
        //   5. A subsequent transform() call sees the new lookup
        //      contents (without rebuilding the transform).
        use arrow_array::{Int32Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema, SchemaRef};
        use ematix_flow_core::SQLiteBackend;
        use ematix_flow_core::backend::Backend;
        use ematix_flow_core::streaming::ShutdownSignal;

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("users.db");
        let db_path_str = db_path.to_str().unwrap().to_string();

        let backend = SQLiteBackend::open(&db_path_str).unwrap();
        backend
            .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            .await
            .unwrap();
        backend
            .execute("INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')")
            .await
            .unwrap();
        drop(backend);

        let toml = format!(
            r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "events"

            [transform]
            sql = "SELECT s.user_id, u.name FROM source s INNER JOIN users u ON s.user_id = u.id"

            [transform.lookups.users]
            kind = "sqlite"
            path = "{db_path_str}"
            table = "users"
            refresh_interval_ms = 100
            "#
        );
        let cfg = PipelineCliConfig::from_toml_str(&toml).unwrap();

        // Initial load + transform construction.
        let lookups = cfg.load_lookups().await.expect("initial load");
        let source_schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            "user_id",
            DataType::Int32,
            false,
        )]));
        let pipeline_cfg = cfg.streaming_config_with_lookups(
            TargetTable {
                schema: "".into(),
                name: "events".into(),
            },
            lookups,
        );
        let transform = pipeline_cfg
            .transform
            .clone()
            .expect("transform configured");

        // First batch: drives the LazySqlTransform's inner build,
        // joining (1, 2, 99) → matches alice + bob → 2 rows.
        let initial_batch = RecordBatch::try_new(
            source_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 99]))],
        )
        .unwrap();
        let out = transform
            .transform(
                initial_batch.clone(),
                &ematix_flow_core::transform::BatchContext::default(),
            )
            .await
            .expect("first transform");
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2, "before refresh: 2 of 3 rows match (id 99 absent)");

        // Mutate the underlying lookup behind the running pipeline.
        let backend = SQLiteBackend::open(&db_path_str).unwrap();
        backend
            .execute("INSERT INTO users (id, name) VALUES (99, 'carol')")
            .await
            .unwrap();
        drop(backend);

        // Spawn the refresh task using the same wiring run_consume_with
        // uses. Hand it the transform from the streaming config so a
        // successful refresh swaps the live MemTable.
        let (shutdown, trigger) = ShutdownSignal::new();
        let handles = spawn_lookup_refresh_tasks(&cfg, transform.clone(), shutdown);
        assert_eq!(handles.len(), 1, "one refresh task per refreshing lookup");

        // Give the refresh loop time to fire ~3 cycles.
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        trigger.trigger();
        for h in handles {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(2), h).await;
        }

        // Post-refresh: id=99 now matches → 3 of 3 rows survive.
        let out = transform
            .transform(
                initial_batch,
                &ematix_flow_core::transform::BatchContext::default(),
            )
            .await
            .expect("post-refresh transform");
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3, "after refresh: id=99 (carol) now joins");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn load_lookups_loads_from_sqlite() {
        // Build a SQLite file with a tiny users table, then ask
        // load_lookups to pull it as Arrow batches. Drives the
        // full lookup-load path end-to-end without needing a
        // network DB.
        use ematix_flow_core::SQLiteBackend;
        use ematix_flow_core::backend::Backend;
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("users.db");
        let db_path_str = db_path.to_str().unwrap().to_string();
        let backend = SQLiteBackend::open(&db_path_str).unwrap();
        backend
            .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
            .await
            .unwrap();
        backend
            .execute("INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob')")
            .await
            .unwrap();
        drop(backend);

        let toml = format!(
            r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "events"

            [transform]
            sql = "SELECT s.user_id, u.name FROM source s LEFT JOIN users u ON s.user_id = u.id"

            [transform.lookups.users]
            kind = "sqlite"
            path = "{db_path_str}"
            table = "users"
            "#
        );
        let cfg = PipelineCliConfig::from_toml_str(&toml).unwrap();
        let lookups = cfg.load_lookups().await.expect("load_lookups");
        assert_eq!(lookups.len(), 1);
        assert_eq!(lookups[0].name, "users");
        let total: usize = lookups[0].batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2, "two users loaded into the lookup");
    }

    #[test]
    fn transform_block_omitted_yields_none() {
        let cfg = PipelineCliConfig::from_toml_str(kafka_to_pg_toml()).unwrap();
        assert!(cfg.transform.is_none(), "no [transform] block → None");
        let table = TargetTable {
            schema: "public".into(),
            name: "events".into(),
        };
        let scfg = cfg.streaming_config(table);
        assert!(
            scfg.transform.is_none(),
            "streaming_config carries None when no transform configured"
        );
    }

    #[test]
    fn transform_block_parses_and_attaches_to_streaming_config() {
        let toml = r#"
            pipeline_name = "events-clean"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "events_clean"

            [transform]
            sql = "SELECT id, name FROM source WHERE id > 10"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let t = cfg.transform.as_ref().expect("transform block present");
        assert!(t.sql.contains("WHERE id > 10"));

        let table = TargetTable {
            schema: "".into(),
            name: "events_clean".into(),
        };
        let scfg = cfg.streaming_config(table);
        assert!(
            scfg.transform.is_some(),
            "streaming_config carries the transform when configured"
        );
    }

    #[test]
    fn parses_multi_source_array_form() {
        let toml = r#"
            pipeline_name = "fan-in"

            [[sources]]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"
            group_id = "ematix-flow"
            query = "events-us"

            [[sources]]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"
            group_id = "ematix-flow"
            query = "events-eu"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "events"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        assert!(cfg.source.is_none());
        assert_eq!(cfg.sources.len(), 2);
        assert_eq!(cfg.sources[0].query, "events-us");
        assert_eq!(cfg.sources[1].query, "events-eu");
        match &cfg.sources[0].source {
            SourceConfig::Kafka {
                bootstrap_servers, ..
            } => {
                assert_eq!(bootstrap_servers, "localhost:9092");
            }
            other => panic!("expected Kafka, got {other:?}"),
        }
    }

    #[test]
    fn rejects_both_source_and_sources_set() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "q"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [[sources]]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"
            query = "q2"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "t"
        "#;
        let err = PipelineCliConfig::from_toml_str(toml).unwrap_err();
        assert!(err.to_string().contains("both `source` and `sources`"));
    }

    #[test]
    fn rejects_neither_source_nor_sources_set() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "q"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "t"
        "#;
        let err = PipelineCliConfig::from_toml_str(toml).unwrap_err();
        assert!(err.to_string().contains("no source"));
    }

    #[test]
    fn rejects_single_source_without_source_query() {
        let toml = r#"
            pipeline_name = "p"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "t"
        "#;
        let err = PipelineCliConfig::from_toml_str(toml).unwrap_err();
        assert!(
            err.to_string().contains("source_query"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn build_sources_returns_one_per_array_entry() {
        let toml = r#"
            pipeline_name = "p"

            [[sources]]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"
            query = "topic-a"

            [[sources]]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"
            query = "topic-b"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "t"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let built = cfg.build_sources().expect("build_sources");
        assert_eq!(built.len(), 2);
        assert_eq!(built[0].1, "topic-a");
        assert_eq!(built[1].1, "topic-b");
    }

    #[test]
    fn idle_pause_ms_defaults_when_omitted() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "q"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "t"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        assert_eq!(cfg.idle_pause_ms, 500);
    }

    #[test]
    fn parses_pubsub_with_anonymous_auth() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "my-sub"

            [source]
            kind = "pubsub"
            project_id = "my-project"
            endpoint = "http://localhost:8085"
            anonymous_auth = true

            [target]
            kind = "duckdb"
            path = ":memory:"

            [target.table]
            schema = "main"
            name = "events"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        match cfg.source.as_ref().expect("source set") {
            SourceConfig::Pubsub {
                project_id,
                endpoint,
                anonymous_auth,
            } => {
                assert_eq!(project_id, "my-project");
                assert_eq!(endpoint.as_deref(), Some("http://localhost:8085"));
                assert!(*anonymous_auth);
            }
            other => panic!("expected Pubsub source, got {other:?}"),
        }
    }

    #[test]
    fn parses_rabbitmq_to_kafka_stream_to_stream() {
        let toml = r#"
            pipeline_name = "rb-to-kafka"
            source_query = "in-queue"

            [source]
            kind = "rabbitmq"
            amqp_url = "amqp://localhost:5672"

            [target]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"
            topic = "out-topic"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        match (
            cfg.source.as_ref().expect("source set"),
            cfg.target.as_ref().unwrap(),
        ) {
            (
                SourceConfig::Rabbitmq { amqp_url },
                TargetConfig::Kafka {
                    bootstrap_servers,
                    topic,
                    ..
                },
            ) => {
                assert_eq!(amqp_url, "amqp://localhost:5672");
                assert_eq!(bootstrap_servers, "localhost:9092");
                assert_eq!(topic, "out-topic");
            }
            other => panic!("unexpected pair: {other:?}"),
        }
    }

    #[test]
    fn parses_kinesis_with_static_creds() {
        let toml = r#"
            pipeline_name = "kn"
            source_query = "any"

            [source]
            kind = "kinesis"
            stream_name = "events"
            region = "us-east-1"
            endpoint = "http://localhost:4566"
            access_key_id = "fake"
            secret_access_key = "fake"

            [target]
            kind = "delta_local"
            path = "/tmp/delta-table"

            [target.table]
            name = "events"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        match cfg.source.as_ref().expect("source set") {
            SourceConfig::Kinesis {
                stream_name,
                region,
                endpoint,
                access_key_id,
                secret_access_key,
            } => {
                assert_eq!(stream_name, "events");
                assert_eq!(region.as_deref(), Some("us-east-1"));
                assert_eq!(endpoint.as_deref(), Some("http://localhost:4566"));
                assert_eq!(access_key_id.as_deref(), Some("fake"));
                assert_eq!(secret_access_key.as_deref(), Some("fake"));
            }
            other => panic!("expected Kinesis source, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unknown_source_kind() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "q"

            [source]
            kind = "bogus"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "t"
        "#;
        let err = PipelineCliConfig::from_toml_str(toml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("unknown variant") || msg.contains("bogus"),
            "got: {msg}"
        );
    }

    #[test]
    fn streaming_config_passes_through_dead_letter_topic() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "q"
            dead_letter_topic = "p-dlq"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "t"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let table = TargetTable {
            schema: "".into(),
            name: "t".into(),
        };
        let sc = cfg.streaming_config(table);
        assert_eq!(sc.dead_letter_topic.as_deref(), Some("p-dlq"));
        assert_eq!(sc.idle_pause_ms, 500);
        assert_eq!(sc.pipeline_name, "p");
    }

    #[test]
    fn build_source_constructs_kafka_backend() {
        let cfg = PipelineCliConfig::from_toml_str(kafka_to_pg_toml()).unwrap();
        let _src = cfg.build_source().expect("build_source");
        // The Backend trait doesn't expose a direct dialect comparison here
        // without an active connection, but unwrapping the Arc returns the
        // KafkaBackend handle.
    }

    #[test]
    fn parses_delta_s3_target() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "topic"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "delta_s3"
            endpoint = "http://localhost:9000"
            bucket = "events"
            prefix = "raw"
            region = "us-east-1"
            access_key_id = "minioadmin"
            secret_access_key = "minioadmin"

            [target.table]
            schema = "default"
            name = "events"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        match cfg.target.as_ref().unwrap() {
            TargetConfig::DeltaS3 {
                endpoint,
                bucket,
                prefix,
                region,
                access_key_id,
                secret_access_key,
                table,
                partition_by,
            } => {
                assert_eq!(endpoint, "http://localhost:9000");
                assert_eq!(bucket, "events");
                assert_eq!(prefix, "raw");
                assert_eq!(region, "us-east-1");
                assert_eq!(access_key_id, "minioadmin");
                assert_eq!(secret_access_key, "minioadmin");
                assert_eq!(table.name, "events");
                assert!(partition_by.is_empty(), "default partition_by is empty");
            }
            other => panic!("expected DeltaS3, got {other:?}"),
        }
    }

    /// Phase 40.1: TOML `partition_by` field carries through to
    /// `TargetConfig::DeltaLocal`.
    /// Credential safety: Debug on `PipelineCliConfig` must redact
    /// every secret field. This is what `info!(?cfg, ...)` /
    /// `error!(?cfg, ...)` would render through tracing — those
    /// outputs go to log aggregators and must not include
    /// passwords, access keys, etc.
    #[test]
    fn debug_redacts_postgres_url_password() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "postgres"
            url = "postgres://alice:DO_NOT_LEAK@host/db"

            [target.table]
            name = "t"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let s = format!("{cfg:?}");
        assert!(!s.contains("DO_NOT_LEAK"), "Debug leaked password: {s}");
        assert!(s.contains("<redacted>"), "expected <redacted> marker: {s}");
        assert!(s.contains("alice"), "username should remain: {s}");
        assert!(s.contains("host"), "host should remain: {s}");
    }

    #[test]
    fn debug_redacts_rabbitmq_amqp_url_password() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "q"

            [source]
            kind = "rabbitmq"
            amqp_url = "amqp://guest:DO_NOT_LEAK@broker.local/vh"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "t"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let s = format!("{cfg:?}");
        assert!(
            !s.contains("DO_NOT_LEAK"),
            "Debug leaked AMQP password: {s}"
        );
        assert!(s.contains("guest"), "username should remain: {s}");
    }

    #[test]
    fn debug_redacts_kinesis_static_credentials() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "any"

            [source]
            kind = "kinesis"
            stream_name = "events"
            region = "us-east-1"
            access_key_id = "AKIA_DO_NOT_LEAK"
            secret_access_key = "SECRET_DO_NOT_LEAK"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "t"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let s = format!("{cfg:?}");
        assert!(!s.contains("AKIA_DO_NOT_LEAK"), "Debug leaked AK: {s}");
        assert!(!s.contains("SECRET_DO_NOT_LEAK"), "Debug leaked SK: {s}");
        assert!(
            s.contains("us-east-1"),
            "non-secret region should remain: {s}"
        );
    }

    #[test]
    fn debug_redacts_delta_s3_credentials() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "topic"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "delta_s3"
            endpoint = "http://localhost:9000"
            bucket = "events"
            region = "us-east-1"
            access_key_id = "AKIA_DELTA_DO_NOT_LEAK"
            secret_access_key = "SECRET_DELTA_DO_NOT_LEAK"

            [target.table]
            name = "events"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let s = format!("{cfg:?}");
        assert!(
            !s.contains("AKIA_DELTA_DO_NOT_LEAK"),
            "Debug leaked Delta S3 AK: {s}"
        );
        assert!(
            !s.contains("SECRET_DELTA_DO_NOT_LEAK"),
            "Debug leaked Delta S3 SK: {s}"
        );
        assert!(s.contains("events"), "bucket should remain: {s}");
    }

    #[test]
    fn redact_db_url_handles_common_shapes() {
        // No userinfo → unchanged.
        assert_eq!(
            redact_db_url("postgres://localhost/mydb"),
            "postgres://localhost/mydb"
        );
        // With password → redacted.
        assert_eq!(
            redact_db_url("postgres://alice:s3cret@host:5432/db"),
            "postgres://alice:<redacted>@host:5432/db"
        );
        // mysql:// works the same way.
        assert_eq!(
            redact_db_url("mysql://u:p@host:3306/db"),
            "mysql://u:<redacted>@host:3306/db"
        );
        // Username-only userinfo.
        assert_eq!(
            redact_db_url("postgres://justuser@host/db"),
            "postgres://justuser:<redacted>@host/db"
        );
        // Schemeless string passes through.
        assert_eq!(redact_db_url("nothing"), "nothing");
    }

    #[test]
    fn parses_delta_local_with_partition_by() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "topic"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "delta_local"
            path = "/tmp/events"
            partition_by = ["year", "month"]

            [target.table]
            schema = "default"
            name = "events"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        match cfg.target.as_ref().unwrap() {
            TargetConfig::DeltaLocal { partition_by, .. } => {
                assert_eq!(partition_by, &vec!["year".to_string(), "month".to_string()]);
            }
            other => panic!("expected DeltaLocal, got {other:?}"),
        }
    }

    /// Phase 40.1: omitted `partition_by` defaults to empty.
    #[test]
    fn delta_local_partition_by_defaults_to_empty() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "topic"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "delta_local"
            path = "/tmp/events"

            [target.table]
            name = "events"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        match cfg.target.as_ref().unwrap() {
            TargetConfig::DeltaLocal { partition_by, .. } => assert!(partition_by.is_empty()),
            other => panic!("expected DeltaLocal, got {other:?}"),
        }
    }

    /// Phase 40.2: TOML `message_key_column` field on Kafka target.
    #[test]
    fn parses_kafka_target_with_message_key_column() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "src"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"
            topic = "events"
            message_key_column = "user_id"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        match cfg.target.as_ref().unwrap() {
            TargetConfig::Kafka {
                message_key_column, ..
            } => {
                assert_eq!(message_key_column.as_deref(), Some("user_id"));
            }
            other => panic!("expected Kafka, got {other:?}"),
        }
    }

    /// Π.1: a Kafka source TOML carrying `payload_format` +
    /// `schema_registry_url` parses cleanly. Avro / Protobuf
    /// pipelines were silently dropping these fields when driven
    /// through the typed-Python facade because the streaming TOML
    /// schema didn't accept them.
    /// Kafka SASL/PLAIN through the streaming TOML — credentials in
    /// the schema, redacted in `Debug`, plumbed to `KafkaBackend`
    /// at build time.
    #[test]
    fn kafka_source_accepts_sasl_plain_credentials() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "broker:9094"
            group_id = "g"
            sasl_plain_username = "alice"
            sasl_plain_password = "s3cret"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "events"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        match cfg.source.as_ref().expect("source set") {
            SourceConfig::Kafka {
                sasl_plain_username,
                sasl_plain_password,
                ..
            } => {
                assert_eq!(sasl_plain_username.as_deref(), Some("alice"));
                assert_eq!(sasl_plain_password.as_deref(), Some("s3cret"));
            }
            other => panic!("expected Kafka source, got {other:?}"),
        }
        // Debug must not leak the password.
        let dbg = format!("{:?}", cfg.source);
        assert!(!dbg.contains("s3cret"), "Debug leaked password: {dbg}");
    }

    #[test]
    fn kafka_source_accepts_sasl_scram_with_mechanism() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "broker:9094"
            group_id = "g"
            sasl_scram_username = "bob"
            sasl_scram_password = "scram-secret"
            sasl_scram_mechanism = "sha-512"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "events"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        match cfg.source.as_ref().expect("source set") {
            SourceConfig::Kafka {
                sasl_scram_username,
                sasl_scram_password,
                sasl_scram_mechanism,
                ..
            } => {
                assert_eq!(sasl_scram_username.as_deref(), Some("bob"));
                assert_eq!(sasl_scram_password.as_deref(), Some("scram-secret"));
                assert_eq!(sasl_scram_mechanism.as_deref(), Some("sha-512"));
            }
            other => panic!("expected Kafka source, got {other:?}"),
        }
    }

    #[test]
    fn kafka_target_accepts_msk_iam_region() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "src"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "kafka"
            bootstrap_servers = "broker.kafka.us-east-1.amazonaws.com:9098"
            topic = "out"
            msk_iam_region = "us-east-1"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        match cfg.target.as_ref().unwrap() {
            TargetConfig::Kafka { msk_iam_region, .. } => {
                assert_eq!(msk_iam_region.as_deref(), Some("us-east-1"));
            }
            other => panic!("expected Kafka target, got {other:?}"),
        }
    }

    /// Mutual exclusion: at most one of (PLAIN, SCRAM, MSK-IAM) may
    /// be configured per Kafka block. The validator should reject
    /// the combo at config-load time, before the runner tries to
    /// build a `KafkaBackend`.
    #[tokio::test(flavor = "multi_thread")]
    async fn kafka_rejects_simultaneous_plain_and_scram() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "b:9094"
            sasl_plain_username = "alice"
            sasl_plain_password = "p"
            sasl_scram_username = "bob"
            sasl_scram_password = "p2"
            sasl_scram_mechanism = "sha-256"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "events"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let result = cfg.build_source();
        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("simultaneous SASL/PLAIN + SCRAM should error"),
        };
        let msg = err.to_string();
        assert!(
            msg.contains("auth") || msg.contains("SASL") || msg.contains("at most one"),
            "got: {msg}"
        );
    }

    /// Π.5: now that the SASL fields are reachable through the
    /// streaming TOML, the inline-credential detector should flag
    /// `sasl_plain_password` and `sasl_scram_password` too.
    #[test]
    fn kafka_inline_sasl_password_detected_by_pi5() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "b:9094"
            sasl_plain_username = "alice"
            sasl_plain_password = "s3cret"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "events"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let findings = cfg.inline_credential_findings();
        assert!(
            findings
                .iter()
                .any(|f| f.contains("Kafka") && f.contains("SASL")),
            "expected a Kafka/SASL finding, got: {findings:?}"
        );
    }

    /// Π.5: inline-credential detection for the deprecation warning.
    /// The detector walks every source / target variant and reports
    /// human-readable findings for any field that looks like a
    /// credential leaked into the TOML — `postgres://user:pw@...`,
    /// `sasl_plain_password`, `secret_access_key`, `amqp_url` with
    /// userinfo, etc. The CLI binary surfaces these via
    /// `tracing::warn!` so users see the migration pointer.
    #[test]
    fn inline_postgres_password_detected() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "postgres"
            url = "postgres://app:s3cret@host/db"

            [target.table]
            schema = "public"
            name = "events"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let findings = cfg.inline_credential_findings();
        assert!(
            findings
                .iter()
                .any(|f| f.contains("Postgres") && f.contains("password")),
            "expected a Postgres-password finding, got: {findings:?}"
        );
    }

    #[test]
    fn passwordless_postgres_url_does_not_trigger() {
        // `postgres://localhost/db` carries no credentials → no
        // finding. Same for `postgres://user@host/db` (user-only;
        // peer auth, IAM auth).
        for url in [
            "postgres://localhost/mydb",
            "postgres://app@localhost/mydb",
            "postgres://localhost:5432/mydb",
        ] {
            let toml = format!(
                r#"
                    pipeline_name = "p"
                    source_query = "events"

                    [source]
                    kind = "kafka"
                    bootstrap_servers = "localhost:9092"

                    [target]
                    kind = "postgres"
                    url = "{url}"

                    [target.table]
                    schema = "public"
                    name = "events"
                "#
            );
            let cfg = PipelineCliConfig::from_toml_str(&toml).unwrap();
            assert_eq!(
                cfg.inline_credential_findings(),
                Vec::<String>::new(),
                "no findings expected for url={url}"
            );
        }
    }

    #[test]
    fn inline_kafka_sasl_password_detected() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "events"
        "#;
        // Note: SourceConfig::Kafka in the CLI doesn't yet accept
        // sasl_plain_password — that's a P2 follow-up. For Π.5 we
        // only flag what's reachable through the current TOML
        // schema, so this test instead covers a different inline
        // credential: the RabbitMQ amqp_url with a password.
        let _ = toml; // not the one we exercise
        let toml_amqp = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "rabbitmq"
            amqp_url = "amqp://app:hunter2@localhost:5672/%2f"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "events"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml_amqp).unwrap();
        let findings = cfg.inline_credential_findings();
        assert!(
            findings
                .iter()
                .any(|f| f.contains("RabbitMQ") && f.contains("password")),
            "expected a RabbitMQ-password finding, got: {findings:?}"
        );
    }

    #[test]
    fn inline_kinesis_secret_access_key_detected() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "any"

            [source]
            kind = "kinesis"
            stream_name = "events"
            region = "us-east-1"
            access_key_id = "AKIA..."
            secret_access_key = "..."

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "events"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let findings = cfg.inline_credential_findings();
        assert!(
            findings.iter().any(|f| f.contains("Kinesis")),
            "expected a Kinesis credential finding, got: {findings:?}"
        );
    }

    #[test]
    fn passwordless_config_yields_no_findings() {
        // A config that pulls every credential through env-var
        // interpolation upstream (or has none at all) should produce
        // an empty findings list — the deprecation warning stays
        // silent.
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"
            group_id = "ematix-flow"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "events"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        assert_eq!(cfg.inline_credential_findings(), Vec::<String>::new());
    }

    /// Π.1.4: object-store target accepts per-format write options
    /// in TOML — `parquet_compression` for Parquet, `csv_delimiter`
    /// and `csv_header` for CSV. Each maps to a builder call on
    /// `ObjectStoreBackend::with_write_options`.
    #[test]
    fn object_store_local_accepts_parquet_compression() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "src"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "object_store_local"
            path = "/tmp/lake"
            format = "parquet"
            prefix = "events"
            parquet_compression = "zstd"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        match cfg.target.as_ref().unwrap() {
            TargetConfig::ObjectStoreLocal {
                parquet_compression,
                ..
            } => {
                assert_eq!(parquet_compression.as_deref(), Some("zstd"));
            }
            other => panic!("expected ObjectStoreLocal, got {other:?}"),
        }
    }

    #[test]
    fn object_store_local_accepts_csv_delimiter_and_header() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "src"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "object_store_local"
            path = "/tmp/lake"
            format = "csv"
            prefix = "events"
            csv_delimiter = ";"
            csv_header = false
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        match cfg.target.as_ref().unwrap() {
            TargetConfig::ObjectStoreLocal {
                csv_delimiter,
                csv_header,
                ..
            } => {
                assert_eq!(csv_delimiter.as_deref(), Some(";"));
                assert_eq!(*csv_header, Some(false));
            }
            other => panic!("expected ObjectStoreLocal, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn object_store_local_rejects_unknown_parquet_codec() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "src"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "object_store_local"
            path = "/tmp/lake"
            format = "parquet"
            prefix = "events"
            parquet_compression = "lzo"
        "#;
        // Parser accepts the string; build-time validation rejects.
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let result = cfg.build_target().await;
        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("unknown codec should fail at build time"),
        };
        let msg = err.to_string();
        assert!(
            msg.contains("lzo") || msg.contains("compression"),
            "got: {msg}"
        );
    }

    /// Π.1 advanced-knob: `[watermark]` TOML block lets users tune
    /// the per-source watermark machinery without editing Rust
    /// defaults. Omit the block → defaults apply (auto-enabled when
    /// a window or join is configured).
    #[test]
    fn watermark_block_overrides_defaults() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "events"

            [watermark]
            lateness_ms = 5000
            source_idleness_ms = 120000
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let wm = cfg.watermark.as_ref().expect("[watermark] block parsed");
        assert_eq!(wm.lateness_ms, Some(5_000));
        assert_eq!(wm.source_idleness_ms, Some(120_000));
    }

    #[test]
    fn watermark_block_partial_keeps_other_default() {
        // Only one field set → the other stays None and the runner
        // falls through to `WatermarkConfig::default()` for it.
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "events"

            [watermark]
            lateness_ms = 2500
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let wm = cfg.watermark.as_ref().unwrap();
        assert_eq!(wm.lateness_ms, Some(2_500));
        assert!(wm.source_idleness_ms.is_none());
    }

    /// Π.1 advanced-knob: `[transform] on_error` is already
    /// accepted; this guards the Python facade's emission against
    /// Rust-side parser drift.
    #[test]
    fn transform_on_error_drop_round_trips() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "events"

            [transform]
            sql = "SELECT * FROM source"
            on_error = "drop"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        assert_eq!(cfg.transform.as_ref().unwrap().on_error, "drop");
    }

    #[test]
    fn kafka_source_accepts_payload_format_and_schema_registry_url() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"
            group_id = "g"
            payload_format = "avro"
            schema_registry_url = "http://sr:8081"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "events"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        match cfg.source.as_ref().expect("source set") {
            SourceConfig::Kafka {
                payload_format,
                schema_registry_url,
                ..
            } => {
                assert_eq!(payload_format.as_deref(), Some("avro"));
                assert_eq!(schema_registry_url.as_deref(), Some("http://sr:8081"));
            }
            other => panic!("expected Kafka source, got {other:?}"),
        }
    }

    #[test]
    fn kafka_target_accepts_payload_format_and_schema_registry_url() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "src"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"
            topic = "events-out"
            payload_format = "protobuf"
            schema_registry_url = "http://sr:8081"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        match cfg.target.as_ref().unwrap() {
            TargetConfig::Kafka {
                payload_format,
                schema_registry_url,
                ..
            } => {
                assert_eq!(payload_format.as_deref(), Some("protobuf"));
                assert_eq!(schema_registry_url.as_deref(), Some("http://sr:8081"));
            }
            other => panic!("expected Kafka target, got {other:?}"),
        }
    }

    /// Phase 40.2: omitted `message_key_column` defaults to None
    /// (round-robin, pre-40.2 behavior).
    #[test]
    fn kafka_target_message_key_column_defaults_to_none() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "src"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"
            topic = "events"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        match cfg.target.as_ref().unwrap() {
            TargetConfig::Kafka {
                message_key_column, ..
            } => {
                assert!(message_key_column.is_none());
            }
            other => panic!("expected Kafka, got {other:?}"),
        }
    }

    #[test]
    fn parses_object_store_local_target_with_each_format() {
        for (format_str, want) in [
            ("parquet", ObjectFormatConfig::Parquet),
            ("csv", ObjectFormatConfig::Csv),
            ("orc", ObjectFormatConfig::Orc),
            ("json_lines", ObjectFormatConfig::JsonLines),
            ("jsonl", ObjectFormatConfig::JsonLines), // alias
        ] {
            let toml = format!(
                r#"
                    pipeline_name = "p"
                    source_query = "topic"

                    [source]
                    kind = "kafka"
                    bootstrap_servers = "localhost:9092"

                    [target]
                    kind = "object_store_local"
                    path = "/var/data"
                    format = "{format_str}"
                    prefix = "events"
                "#
            );
            let cfg = PipelineCliConfig::from_toml_str(&toml).unwrap();
            match cfg.target.as_ref().unwrap() {
                TargetConfig::ObjectStoreLocal {
                    path,
                    format,
                    prefix,
                    ..
                } => {
                    assert_eq!(path, "/var/data");
                    assert_eq!(prefix, "events");
                    let parsed: ObjectFormat = (*format).into();
                    let expected: ObjectFormat = want.into();
                    assert_eq!(parsed, expected, "format mismatch for {format_str}");
                }
                other => panic!("expected ObjectStoreLocal, got {other:?}"),
            }
        }
    }

    #[test]
    fn parses_object_store_s3_target() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "topic"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "object_store_s3"
            endpoint = "http://localhost:9000"
            bucket = "events"
            region = "us-east-1"
            access_key_id = "minioadmin"
            secret_access_key = "minioadmin"
            format = "parquet"
            prefix = "raw"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        match cfg.target.as_ref().unwrap() {
            TargetConfig::ObjectStoreS3 {
                endpoint,
                bucket,
                region,
                access_key_id,
                secret_access_key,
                format,
                prefix,
                ..
            } => {
                assert_eq!(endpoint, "http://localhost:9000");
                assert_eq!(bucket, "events");
                assert_eq!(region, "us-east-1");
                assert_eq!(access_key_id, "minioadmin");
                assert_eq!(secret_access_key, "minioadmin");
                assert_eq!(prefix, "raw");
                let parsed: ObjectFormat = (*format).into();
                assert_eq!(parsed, ObjectFormat::Parquet);
            }
            other => panic!("expected ObjectStoreS3, got {other:?}"),
        }
    }

    #[test]
    fn delta_s3_prefix_defaults_to_empty_when_omitted() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "topic"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "delta_s3"
            endpoint = "http://localhost:9000"
            bucket = "events"
            region = "us-east-1"
            access_key_id = "minioadmin"
            secret_access_key = "minioadmin"

            [target.table]
            name = "events"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        match cfg.target.as_ref().unwrap() {
            TargetConfig::DeltaS3 { prefix, .. } => assert_eq!(prefix, ""),
            other => panic!("expected DeltaS3, got {other:?}"),
        }
    }

    #[test]
    fn from_path_reads_config_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        std::fs::write(&path, kafka_to_pg_toml()).unwrap();
        let cfg = PipelineCliConfig::from_path(&path).unwrap();
        assert_eq!(cfg.pipeline_name, "events-to-pg");
    }

    // --- Π.4a-2: build_targets returns a Vec ------------------------------

    #[tokio::test(flavor = "multi_thread")]
    async fn build_targets_returns_one_for_single_target_form() {
        // Use an in-memory backend so the test doesn't need a live
        // service. The schema layer is what we're verifying here.
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "duckdb"
            path = ":memory:"
            [target.table]
            schema = ""
            name = "events"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let built = cfg
            .build_targets()
            .await
            .expect("build_targets should succeed for in-memory backend");
        assert_eq!(built.len(), 1);
        assert_eq!(built[0].1.name, "events");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn build_targets_returns_one_per_array_entry() {
        let toml = r#"
            pipeline_name = "fanout"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [[targets]]
            kind = "duckdb"
            path = ":memory:"
            [targets.table]
            schema = ""
            name = "events"

            [[targets]]
            kind = "sqlite"
            path = ":memory:"
            [targets.table]
            schema = ""
            name = "events_archive"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let built = cfg
            .build_targets()
            .await
            .expect("build_targets should succeed for in-memory backends");
        assert_eq!(built.len(), 2);
        assert_eq!(built[0].1.name, "events");
        assert_eq!(built[1].1.name, "events_archive");
    }

    // --- Π.4a: multi-target [[targets]] schema ----------------------------

    #[test]
    fn parses_multi_target_array_form() {
        let toml = r#"
            pipeline_name = "fanout"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [[targets]]
            kind = "postgres"
            url = "postgres://localhost/wh"
            [targets.table]
            schema = "public"
            name = "events"

            [[targets]]
            kind = "delta_local"
            path = "/tmp/lake"
            partition_by = ["year", "month"]
            [targets.table]
            schema = ""
            name = "events_archive"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let targets = cfg.targets();
        assert_eq!(targets.len(), 2, "expected 2 targets");
        match &targets[0] {
            TargetConfig::Postgres { url, table } => {
                assert_eq!(url, "postgres://localhost/wh");
                assert_eq!(table.schema, "public");
                assert_eq!(table.name, "events");
            }
            other => panic!("expected Postgres at [0], got {other:?}"),
        }
        match &targets[1] {
            TargetConfig::DeltaLocal {
                path,
                table,
                partition_by,
            } => {
                assert_eq!(path, "/tmp/lake");
                assert_eq!(table.name, "events_archive");
                assert_eq!(partition_by, &vec!["year".to_string(), "month".to_string()]);
            }
            other => panic!("expected DeltaLocal at [1], got {other:?}"),
        }
    }

    #[test]
    fn single_target_form_still_works_via_targets_accessor() {
        // Existing TOMLs with `[target]` keep parsing; the unified
        // `targets()` accessor returns a one-element slice.
        let cfg = PipelineCliConfig::from_toml_str(kafka_to_pg_toml()).unwrap();
        let targets = cfg.targets();
        assert_eq!(targets.len(), 1);
        match &targets[0] {
            TargetConfig::Postgres { url, .. } => {
                assert_eq!(url, "postgres://localhost/mydb");
            }
            other => panic!("expected Postgres, got {other:?}"),
        }
    }

    #[test]
    fn rejects_both_target_and_targets_set() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "postgres"
            url = "postgres://localhost/wh"
            [target.table]
            name = "t"

            [[targets]]
            kind = "postgres"
            url = "postgres://localhost/wh"
            [targets.table]
            name = "t"
        "#;
        let err = PipelineCliConfig::from_toml_str(toml)
            .expect_err("expected ConfigError when both forms present");
        let msg = err.to_string();
        assert!(
            msg.contains("target") && msg.contains("targets"),
            "error should mention both forms: {msg}"
        );
    }

    #[test]
    fn rejects_neither_target_nor_targets_set() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"
        "#;
        let err = PipelineCliConfig::from_toml_str(toml)
            .expect_err("expected ConfigError when no target form present");
        let msg = err.to_string();
        assert!(
            msg.contains("target"),
            "error should mention missing target: {msg}"
        );
    }

    // ----- Phase 39.5a PR 1 slice 1.4: [state_store] block -----

    fn minimal_pipeline_with_state_store(state_store_block: &str) -> String {
        format!(
            r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "out"

            {state_store_block}
            "#
        )
    }

    #[test]
    fn state_store_block_omitted_yields_none() {
        let toml = minimal_pipeline_with_state_store("");
        let cfg = PipelineCliConfig::from_toml_str(&toml).unwrap();
        assert!(
            cfg.state_store.is_none(),
            "no [state_store] block must parse to None — \
             39.4 tumbling/hopping pipelines stay opt-out"
        );
    }

    #[test]
    fn parses_postgres_state_store_with_defaults() {
        let block = r#"
            [state_store]
            kind = "postgres"
            url = "postgres://localhost/ematix_state"
        "#;
        let cfg =
            PipelineCliConfig::from_toml_str(&minimal_pipeline_with_state_store(block)).unwrap();
        let ss = cfg.state_store.expect("state_store must parse");
        match ss {
            StateStoreConfig::Postgres {
                url,
                schema,
                checkpoint_interval_ms,
            } => {
                assert_eq!(url, "postgres://localhost/ematix_state");
                assert_eq!(schema, "public", "schema defaults to \"public\"");
                assert_eq!(
                    checkpoint_interval_ms, 60_000,
                    "checkpoint_interval_ms defaults to 60s"
                );
            }
            other => panic!("expected Postgres, got {other:?}"),
        }
    }

    #[test]
    fn parses_postgres_state_store_with_overrides() {
        let block = r#"
            [state_store]
            kind = "postgres"
            url = "postgres://localhost/ematix_state"
            schema = "ematix"
            checkpoint_interval_ms = 30000
        "#;
        let cfg =
            PipelineCliConfig::from_toml_str(&minimal_pipeline_with_state_store(block)).unwrap();
        let ss = cfg.state_store.unwrap();
        match ss {
            StateStoreConfig::Postgres {
                schema,
                checkpoint_interval_ms,
                ..
            } => {
                assert_eq!(schema, "ematix");
                assert_eq!(checkpoint_interval_ms, 30_000);
            }
            other => panic!("expected Postgres, got {other:?}"),
        }
    }

    #[test]
    fn parses_in_memory_state_store() {
        let block = r#"
            [state_store]
            kind = "in_memory"
        "#;
        let cfg =
            PipelineCliConfig::from_toml_str(&minimal_pipeline_with_state_store(block)).unwrap();
        match cfg.state_store.unwrap() {
            StateStoreConfig::InMemory {
                checkpoint_interval_ms,
            } => {
                assert_eq!(checkpoint_interval_ms, 60_000);
            }
            other => panic!("expected InMemory, got {other:?}"),
        }
    }

    #[test]
    fn rejects_postgres_state_store_without_url() {
        let block = r#"
            [state_store]
            kind = "postgres"
        "#;
        let err = PipelineCliConfig::from_toml_str(&minimal_pipeline_with_state_store(block))
            .expect_err("missing url must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("url"),
            "error should mention missing url: {msg}"
        );
    }

    #[test]
    fn rejects_unknown_state_store_kind() {
        let block = r#"
            [state_store]
            kind = "redis"
        "#;
        let err = PipelineCliConfig::from_toml_str(&minimal_pipeline_with_state_store(block))
            .expect_err("unknown kind must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("redis") || msg.contains("kind"),
            "error should mention the bad kind: {msg}"
        );
    }

    #[test]
    fn rejects_zero_checkpoint_interval() {
        // Zero would mean "checkpoint every batch" which is the same
        // as the per-emit commit; defeating the floor. Reject as a
        // config-load error to make the operator pick a real value.
        let block = r#"
            [state_store]
            kind = "postgres"
            url = "postgres://localhost/ematix_state"
            checkpoint_interval_ms = 0
        "#;
        let err = PipelineCliConfig::from_toml_str(&minimal_pipeline_with_state_store(block))
            .expect_err("zero checkpoint must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("checkpoint_interval_ms"),
            "error should mention checkpoint_interval_ms: {msg}"
        );
    }

    // ----- Phase 39.5a PR 2 slice 2.5: session window TOML -----

    #[test]
    fn parses_session_window_block() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "out"

            [transform]
            sql = "SELECT user_id, page, _event_ts FROM source"

            [transform.window]
            kind = "session"
            gap_ms = 30000
            max_session_duration_ms = 7200000
            group_by = ["user_id"]
            late_data = "drop"
            max_groups_per_window = 1000

            [[transform.window.aggregations]]
            agg = "count"
            as = "events"

            [[transform.window.aggregations]]
            agg = "first"
            column = "page"
            as = "first_page"

            [state_store]
            kind = "in_memory"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let w = cfg.transform.as_ref().unwrap().window.as_ref().unwrap();
        assert_eq!(w.kind, WindowKindToml::Session);
        assert_eq!(w.gap_ms, Some(30_000));
        assert_eq!(w.max_session_duration_ms, Some(7_200_000));
        assert_eq!(w.group_by, vec!["user_id"]);
        assert_eq!(w.aggregations.len(), 2);

        // Translation to core config must validate against the core
        // validator (gap_ms set, max > gap, group_by non-empty).
        let core = window_toml_to_core(w).unwrap();
        let core_inst = WindowedAggregateTransform::new(core, None).unwrap();
        let schema = core_inst.output_schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        // session_id_column defaults to "session_id".
        assert!(names.contains(&"session_id"));
        assert!(names.contains(&"window_start"));
    }

    #[test]
    fn session_window_uses_default_session_id_column() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "out"

            [transform]
            sql = "SELECT user_id, _event_ts FROM source"

            [transform.window]
            kind = "session"
            gap_ms = 100
            max_session_duration_ms = 1000
            group_by = ["user_id"]
            max_groups_per_window = 100

            [[transform.window.aggregations]]
            agg = "count"
            as = "n"

            [state_store]
            kind = "in_memory"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let w = cfg.transform.as_ref().unwrap().window.as_ref().unwrap();
        assert_eq!(w.session_id_column, "session_id");
    }

    #[test]
    fn session_window_accepts_custom_session_id_column() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "out"

            [transform]
            sql = "SELECT user_id, _event_ts FROM source"

            [transform.window]
            kind = "session"
            gap_ms = 100
            max_session_duration_ms = 1000
            group_by = ["user_id"]
            max_groups_per_window = 100
            session_id_column = "sid"

            [[transform.window.aggregations]]
            agg = "count"
            as = "n"

            [state_store]
            kind = "in_memory"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let w = cfg.transform.as_ref().unwrap().window.as_ref().unwrap();
        assert_eq!(w.session_id_column, "sid");
    }

    // ----- PR 3 cross-validation: session ↔ state_store -----

    fn pipeline_with_session_and_optional_state_store(with_state_store: bool) -> String {
        let state_store_block = if with_state_store {
            r#"
            [state_store]
            kind = "in_memory"
            "#
        } else {
            ""
        };
        format!(
            r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "out"

            [transform]
            sql = "SELECT user_id, _event_ts FROM source"

            [transform.window]
            kind = "session"
            gap_ms = 100
            max_session_duration_ms = 1000
            group_by = ["user_id"]
            max_groups_per_window = 100

            [[transform.window.aggregations]]
            agg = "count"
            as = "n"

            {state_store_block}
            "#
        )
    }

    #[test]
    fn session_window_requires_state_store() {
        let toml = pipeline_with_session_and_optional_state_store(false);
        let err = PipelineCliConfig::from_toml_str(&toml)
            .expect_err("session without state_store must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("session") && msg.contains("state_store"),
            "got: {msg}"
        );
    }

    #[test]
    fn session_window_with_state_store_passes() {
        let toml = pipeline_with_session_and_optional_state_store(true);
        PipelineCliConfig::from_toml_str(&toml).unwrap();
    }

    #[test]
    fn tumbling_window_with_state_store_is_rejected() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "out"

            [transform]
            sql = "SELECT user_id, _event_ts FROM source"

            [transform.window]
            kind = "tumbling"
            duration_ms = 60000
            event_time_column = "_event_ts"
            group_by = ["user_id"]
            max_groups_per_window = 100

            [[transform.window.aggregations]]
            agg = "count"
            as = "n"

            [state_store]
            kind = "in_memory"
        "#;
        let err = PipelineCliConfig::from_toml_str(toml)
            .expect_err("tumbling + state_store rejected in PR 3");
        assert!(
            err.to_string().contains("session"),
            "error should suggest sessions: {err}"
        );
    }

    #[test]
    fn state_store_without_window_is_allowed() {
        // PR 3 design: `[state_store]` alone is a no-op (the
        // pipeline carries the handle but has no state to commit).
        // Permitted so users can stage state-store provisioning
        // before adding a session window.
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "out"

            [state_store]
            kind = "in_memory"
        "#;
        PipelineCliConfig::from_toml_str(toml).unwrap();
    }

    #[test]
    fn session_with_approximate_count_distinct_and_state_store_is_rejected() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "out"

            [transform]
            sql = "SELECT user_id, page, _event_ts FROM source"

            [transform.window]
            kind = "session"
            gap_ms = 100
            max_session_duration_ms = 1000
            group_by = ["user_id"]
            max_groups_per_window = 100

            [[transform.window.aggregations]]
            agg = "count_distinct"
            column = "page"
            as = "distinct_pages"
            mode = "approximate"

            [state_store]
            kind = "in_memory"
        "#;
        let err = PipelineCliConfig::from_toml_str(toml)
            .expect_err("approximate count_distinct in stateful session rejected");
        let msg = err.to_string();
        assert!(msg.contains("count_distinct"), "got: {msg}");
        assert!(
            msg.contains("exact"),
            "error should suggest exact mode: {msg}"
        );
    }

    #[test]
    fn session_with_exact_count_distinct_and_state_store_is_accepted() {
        // P1.9: exact-mode count_distinct is HashSet-backed and
        // postcard-serializable. Allowed in stateful sessions as
        // long as max_distinct_values_per_group caps the cardinality.
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "out"

            [transform]
            sql = "SELECT user_id, page, _event_ts FROM source"

            [transform.window]
            kind = "session"
            gap_ms = 100
            max_session_duration_ms = 1000
            group_by = ["user_id"]
            max_groups_per_window = 100

            [[transform.window.aggregations]]
            agg = "count_distinct"
            column = "page"
            as = "distinct_pages"
            mode = "exact"
            max_distinct_values_per_group = 1000

            [state_store]
            kind = "in_memory"
        "#;
        PipelineCliConfig::from_toml_str(toml).unwrap();
    }

    #[test]
    fn session_with_pubsub_source_and_state_store_is_accepted() {
        // P1.7a: Pub/Sub uses broker-tracked offsets. seek_to is a
        // no-op; the ack stream is the offset. Stateful pipelines
        // accept Pub/Sub as a source.
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "pubsub"
            project_id = "p"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "out"

            [transform]
            sql = "SELECT user_id, _event_ts FROM source"

            [transform.window]
            kind = "session"
            gap_ms = 100
            max_session_duration_ms = 1000
            group_by = ["user_id"]
            max_groups_per_window = 100

            [[transform.window.aggregations]]
            agg = "count"
            as = "n"

            [state_store]
            kind = "in_memory"
        "#;
        PipelineCliConfig::from_toml_str(toml).unwrap();
    }

    #[test]
    fn session_with_kinesis_source_and_state_store_is_accepted() {
        // P1.7b: Kinesis now implements `seek_to` (per-shard sequence
        // numbers via `AfterSequenceNumber` iterator type) and is
        // accepted as a stateful-pipeline source.
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kinesis"
            stream_name = "events"
            region = "us-east-1"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "out"

            [transform]
            sql = "SELECT user_id, _event_ts FROM source"

            [transform.window]
            kind = "session"
            gap_ms = 100
            max_session_duration_ms = 1000
            group_by = ["user_id"]
            max_groups_per_window = 100

            [[transform.window.aggregations]]
            agg = "count"
            as = "n"

            [state_store]
            kind = "in_memory"
        "#;
        PipelineCliConfig::from_toml_str(toml).unwrap();
    }

    #[test]
    fn session_window_translation_rejects_missing_gap_ms() {
        // The CLI lets the value be omitted; the core's
        // `WindowedAggregateTransform::new` is the validator that
        // catches it.
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "out"

            [transform]
            sql = "SELECT user_id, _event_ts FROM source"

            [transform.window]
            kind = "session"
            max_session_duration_ms = 1000
            group_by = ["user_id"]
            max_groups_per_window = 100

            [[transform.window.aggregations]]
            agg = "count"
            as = "n"

            [state_store]
            kind = "in_memory"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let w = cfg.transform.as_ref().unwrap().window.as_ref().unwrap();
        let core = window_toml_to_core(w).unwrap();
        let err = WindowedAggregateTransform::new(core, None).unwrap_err();
        assert!(err.to_string().contains("gap_ms is required"), "got: {err}");
    }

    // ----- Phase 39.5b PR 2: [transform.join] config -----

    fn pipeline_with_join(state_store: bool) -> String {
        let ss = if state_store {
            r#"
            [state_store]
            kind = "in_memory"
            "#
        } else {
            ""
        };
        format!(
            r#"
            pipeline_name = "p"

            [[sources]]
            query = "orders"
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [[sources]]
            query = "payments"
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "joined"

            [transform]

            [transform.join]
            kind = "stream_stream_join"
            left_source = "orders"
            right_source = "payments"
            left_keys = ["order_id"]
            right_keys = ["order_id"]
            time_window_ms = 300000

            {ss}
            "#
        )
    }

    #[test]
    fn parses_left_outer_join_kind() {
        let toml = r#"
            pipeline_name = "p"

            [[sources]]
            query = "orders"
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [[sources]]
            query = "payments"
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "joined"

            [transform]

            [transform.join]
            kind = "left_outer"
            left_source = "orders"
            right_source = "payments"
            left_keys = ["order_id"]
            right_keys = ["order_id"]
            time_window_ms = 60000

            [state_store]
            kind = "in_memory"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let j = cfg.transform.as_ref().unwrap().join.as_ref().unwrap();
        assert_eq!(j.kind, "left_outer");

        // Build path constructs the join transform without panic.
        let table = TargetTable {
            schema: "".into(),
            name: "joined".into(),
        };
        let _ = cfg.streaming_config_with_lookups(table, Vec::new());
    }

    #[test]
    fn parses_full_outer_join_kind() {
        let toml = r#"
            pipeline_name = "p"

            [[sources]]
            query = "orders"
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [[sources]]
            query = "payments"
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "joined"

            [transform]

            [transform.join]
            kind = "full_outer"
            left_source = "orders"
            right_source = "payments"
            left_keys = ["order_id"]
            right_keys = ["order_id"]
            time_window_ms = 60000

            [state_store]
            kind = "in_memory"
        "#;
        PipelineCliConfig::from_toml_str(toml).unwrap();
    }

    #[test]
    fn parses_join_with_reopen_late_data() {
        let toml = r#"
            pipeline_name = "p"

            [[sources]]
            query = "orders"
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [[sources]]
            query = "payments"
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "joined"

            [transform]

            [transform.join]
            kind = "stream_stream_join"
            left_source = "orders"
            right_source = "payments"
            left_keys = ["order_id"]
            right_keys = ["order_id"]
            time_window_ms = 60000
            late_data = "reopen"
            allowed_lateness_ms = 30000

            [state_store]
            kind = "in_memory"
        "#;
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let j = cfg.transform.as_ref().unwrap().join.as_ref().unwrap();
        assert_eq!(j.late_data, "reopen");
        assert_eq!(j.allowed_lateness_ms, Some(30000));
    }

    #[test]
    fn join_reopen_requires_allowed_lateness_ms() {
        let toml = r#"
            pipeline_name = "p"

            [[sources]]
            query = "orders"
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [[sources]]
            query = "payments"
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "joined"

            [transform]

            [transform.join]
            kind = "stream_stream_join"
            left_source = "orders"
            right_source = "payments"
            left_keys = ["order_id"]
            right_keys = ["order_id"]
            time_window_ms = 60000
            late_data = "reopen"

            [state_store]
            kind = "in_memory"
        "#;
        // CLI validation passes — the missing `allowed_lateness_ms`
        // surfaces from `join_toml_to_core` at pipeline-build time.
        let cfg = PipelineCliConfig::from_toml_str(toml).unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let table = TargetTable {
                schema: "".into(),
                name: "joined".into(),
            };
            cfg.streaming_config_with_lookups(table, Vec::new())
        }));
        assert!(
            result.is_err(),
            "missing allowed_lateness_ms should fail at build"
        );
    }

    #[test]
    fn parses_stream_stream_join_block() {
        let toml = pipeline_with_join(true);
        let cfg = PipelineCliConfig::from_toml_str(&toml).unwrap();
        let j = cfg.transform.as_ref().unwrap().join.as_ref().unwrap();
        assert_eq!(j.kind, "stream_stream_join");
        assert_eq!(j.left_source, "orders");
        assert_eq!(j.right_source, "payments");
        assert_eq!(j.left_keys, vec!["order_id"]);
        assert_eq!(j.time_window_ms, 300_000);
        // Defaults applied.
        assert_eq!(j.event_time_column, "_event_ts");
        assert_eq!(j.late_data, "drop");
        assert_eq!(j.left_column_prefix, "left_");
        assert_eq!(j.right_column_prefix, "right_");
    }

    #[test]
    fn join_requires_state_store() {
        let toml = pipeline_with_join(false);
        let err = PipelineCliConfig::from_toml_str(&toml).unwrap_err();
        assert!(err.to_string().contains("[state_store]"), "got: {err}");
    }

    #[test]
    fn join_rejects_with_window_block() {
        let toml = r#"
            pipeline_name = "p"

            [[sources]]
            query = "orders"
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [[sources]]
            query = "payments"
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "out"

            [transform]

            [transform.join]
            kind = "stream_stream_join"
            left_source = "orders"
            right_source = "payments"
            left_keys = ["k"]
            right_keys = ["k"]
            time_window_ms = 1000

            [transform.window]
            kind = "session"
            gap_ms = 100
            max_session_duration_ms = 1000
            group_by = ["k"]
            max_groups_per_window = 100

            [[transform.window.aggregations]]
            agg = "count"
            as = "n"

            [state_store]
            kind = "in_memory"
        "#;
        let err = PipelineCliConfig::from_toml_str(toml).unwrap_err();
        assert!(err.to_string().contains("mutually exclusive"), "got: {err}");
    }

    #[test]
    fn join_rejects_with_sql_pre_stage() {
        let toml = r#"
            pipeline_name = "p"

            [[sources]]
            query = "orders"
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [[sources]]
            query = "payments"
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "out"

            [transform]
            sql = "SELECT * FROM source"

            [transform.join]
            kind = "stream_stream_join"
            left_source = "orders"
            right_source = "payments"
            left_keys = ["k"]
            right_keys = ["k"]
            time_window_ms = 1000

            [state_store]
            kind = "in_memory"
        "#;
        let err = PipelineCliConfig::from_toml_str(toml).unwrap_err();
        assert!(err.to_string().contains("sql"), "got: {err}");
    }

    #[test]
    fn join_rejects_unknown_left_source() {
        let toml = r#"
            pipeline_name = "p"

            [[sources]]
            query = "orders"
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [[sources]]
            query = "payments"
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "out"

            [transform]

            [transform.join]
            kind = "stream_stream_join"
            left_source = "bogus"
            right_source = "payments"
            left_keys = ["k"]
            right_keys = ["k"]
            time_window_ms = 1000

            [state_store]
            kind = "in_memory"
        "#;
        let err = PipelineCliConfig::from_toml_str(toml).unwrap_err();
        assert!(
            err.to_string().contains("doesn't match any") || err.to_string().contains("bogus"),
            "got: {err}"
        );
    }

    #[test]
    fn join_rejects_single_source_form() {
        let toml = r#"
            pipeline_name = "p"
            source_query = "events"

            [source]
            kind = "kafka"
            bootstrap_servers = "localhost:9092"

            [target]
            kind = "sqlite"
            path = ":memory:"

            [target.table]
            name = "out"

            [transform]

            [transform.join]
            kind = "stream_stream_join"
            left_source = "left"
            right_source = "right"
            left_keys = ["k"]
            right_keys = ["k"]
            time_window_ms = 1000

            [state_store]
            kind = "in_memory"
        "#;
        let err = PipelineCliConfig::from_toml_str(toml).unwrap_err();
        assert!(
            err.to_string().contains("[[sources]]") || err.to_string().contains("multi-source"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn build_in_memory_state_store_returns_working_store() {
        use ematix_flow_core::state_store::CommitSnapshot;

        let cfg = StateStoreConfig::InMemory {
            checkpoint_interval_ms: 60_000,
        };
        let store = cfg.build().await.unwrap();
        // Round-trip a tiny commit to prove `build` returned a
        // functional store, not a stub.
        store
            .commit(
                "p",
                CommitSnapshot {
                    state_upserts: vec![(b"k".to_vec(), b"v".to_vec())],
                    state_version: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let r = store.load("p").await.unwrap();
        assert_eq!(
            r.state_by_key.get(b"k".as_slice()).map(|v| v.as_slice()),
            Some(&b"v"[..])
        );
    }
}
