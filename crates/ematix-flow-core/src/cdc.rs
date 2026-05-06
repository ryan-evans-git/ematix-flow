//! Phase Δ — CDC (change-data-capture) source mode.
//!
//! [`CdcConfig`] declares how a streaming pipeline should
//! interpret CDC envelopes from a Kafka topic and apply the
//! resulting change events to the target table. Configurable
//! via either the typed-Python `CDC(envelope="debezium")`
//! dataclass on `@ematix.streaming_pipeline` or the
//! `[transform.cdc]` TOML block — peer-equivalent paths into
//! this same struct.
//!
//! ## What ships so far
//!
//! **PR 1** — config scaffolding:
//! - [`CdcConfig`] struct + the [`EnvelopeKind`] / [`CdcOp`] /
//!   [`DeleteMode`] / [`SchemaEvolutionPolicy`] enums.
//! - Canonical field-mapping defaults for Debezium and Maxwell
//!   envelopes (so `CDC(envelope="debezium")` is a one-liner).
//! - Serde derives + round-trip tests so config travels cleanly
//!   through the same `BackendConfig` round-trip the rest of
//!   the connector trait uses.
//!
//! **PR 2** — envelope parser:
//! - [`CdcEvent`] — the post-resolution event shape (op + key +
//!   ts + before/after JSON objects) the PR 3 executor consumes.
//! - [`parse_event`] — pure function from a decoded JSON row
//!   to a [`CdcEvent`]. Handles tombstones (returns `None`),
//!   delete-side key fallback (`after.id` → `before.id`), and
//!   Maxwell's seconds-vs-milliseconds timestamp wart.
//!
//! **PR 3** — RecordBatch → events bridge + Postgres executor:
//! - [`batch_to_json_rows`] / [`events_from_batch`] — convert
//!   the Kafka source's Arrow output to per-row [`ParsedRow`]
//!   for the executor.
//! - `Backend::run_cdc` trait method (default: returns a
//!   "not yet implemented for this dialect" error). PostgresBackend
//!   overrides with a transactional per-op apply that uses
//!   `jsonb_populate_record` for type-safe JSON → row coercion.
//!
//! **PR 4** — per-PK idempotency gate:
//! - `ematix_flow.cdc_idempotency` table tracks `(pipeline,
//!   pk_json) → last_seen_ts_ms`. The CDC executor admits an
//!   event only when its `ts_ms` exceeds the stored value, via
//!   a single-round-trip `INSERT … ON CONFLICT DO UPDATE …
//!   RETURNING` gate that runs inside the same Postgres
//!   transaction as the data write — so a crash mid-batch
//!   leaves gate + target consistent.
//! - Surfaced on [`crate::backend::CdcRunResult::idempotent_skipped`]
//!   so Kafka redeliveries are visible in metrics rather than
//!   silently absorbed.
//!
//! **PR 5** — schema-evolution detection:
//! - Per-event `after`-payload keys checked against the target's
//!   declared column set. [`SchemaEvolutionPolicy::Skip`] (default)
//!   emits a single `tracing::warn!` per drift column per batch,
//!   then lets Postgres's `jsonb_populate_record` discard the
//!   unknown key. [`SchemaEvolutionPolicy::Fail`] returns an
//!   error naming the column + the policy, rolling back the
//!   whole batch transactionally.
//! - `AlterTable` policy remains deferred — bundling per-target
//!   `ALTER TABLE` plumbing into PR 5 would have doubled the
//!   surface, and Δ.X1 (Delta) doesn't use `ALTER TABLE` syntax
//!   at all.
//!
//! Still to come: end-to-end Debezium-via-testcontainers
//! example (PR 6).
//!
//! Plan: `docs/PHASE_DELTA_CDC_PLAN.md`.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Wire-format / source-system family. Each variant carries an
/// implicit canonical field mapping (see [`CdcConfig::for_envelope`])
/// — pick `Debezium` or `Maxwell` and the rest of the field paths
/// fill in from the spec; pick `Custom` and supply every field
/// path explicitly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvelopeKind {
    /// Debezium envelope (Postgres / MySQL / SQL Server / Mongo
    /// connectors). Canonical mapping:
    ///   op = "op"            (c|u|d|r)
    ///   before = "before"
    ///   after = "after"
    ///   ts = "source.ts_ms"
    ///   key = derived per-row from the message key, not the value
    Debezium,

    /// [Maxwell's daemon](https://maxwells-daemon.io) envelope.
    /// Canonical mapping:
    ///   op = "type"          (insert|update|delete)
    ///   before = "old"
    ///   after = "data"
    ///   ts = "ts"            (seconds, we coerce to ms)
    ///   key = "data.<pk>"
    Maxwell,

    /// User-defined envelope. All field paths are required —
    /// see [`CdcConfig::op_field`] / `before_field` / `after_field`
    /// / `key_field` / `ts_field` / `op_map`. The validator
    /// rejects `Custom` with any required field unset.
    Custom,
}

/// One of the four standard CDC operations. The string values
/// upstream wire formats use to denote each (`c`/`u`/`d`/`r`
/// for Debezium, `insert`/`update`/`delete` for Maxwell, etc.)
/// map to these via [`CdcConfig::op_map`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CdcOp {
    /// New row in the source table. INSERT against the target
    /// (UPSERT on PK conflict by default).
    Create,
    /// Existing row changed. UPDATE by PK; if the PK isn't in
    /// the target yet, fall back to INSERT (covers the
    /// out-of-order case where update arrives before create).
    Update,
    /// Row removed from the source. DELETE by PK, or
    /// soft-delete column flip if [`DeleteMode::Soft`] is
    /// configured.
    Delete,
    /// Snapshot read — Debezium emits `r` while replaying
    /// existing rows during the initial snapshot phase.
    /// Treated identically to [`CdcOp::Create`] (UPSERT) —
    /// the snapshot just happens to predate streaming.
    Read,
}

/// How `Delete` ops translate to target-side DDL.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum DeleteMode {
    /// Issue `DELETE FROM target WHERE pk = ...`. Target rows
    /// vanish from the table.
    #[default]
    Hard,
    /// Issue `UPDATE target SET <column> = NOW() WHERE pk = ...`
    /// instead. Preserves history; target callers filter on
    /// `WHERE <column> IS NULL` to see the live set. The named
    /// column must exist in the target schema with a
    /// nullable timestamp type.
    Soft {
        /// Column to flip on delete. Type must accept timestamp
        /// values (Postgres `TIMESTAMPTZ` recommended).
        column: String,
    },
}

/// How to react when an `after` payload contains columns the
/// target table doesn't have. Driving question: should
/// upstream schema evolution stop the pipeline, or let it ride?
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaEvolutionPolicy {
    /// Default. Warn once per unknown column (via `tracing::warn!`)
    /// and omit the column from the applied DDL. Pipeline keeps
    /// running; user picks up the new column on next deploy with
    /// an updated `@ematix.table` definition.
    #[default]
    Skip,
    /// Strict mode — raise on first unknown column. Useful for
    /// teams that gate releases on schema sync.
    Fail,
    // `AlterTable` policy deferred — see plan's "deferred"
    // section. Needs per-target ALTER plumbing that varies by
    // backend (Postgres ✓, DuckDB ✓, Delta partial,
    // object-store no).
}

/// Pipeline-level CDC config. Lives on `TransformConfig` (next
/// to `transform.window` and `transform.join`); mutually
/// exclusive with both since CDC is a different way of
/// applying input, not a transformation that windows over time.
///
/// The default values populate from [`EnvelopeKind`] — calling
/// [`CdcConfig::for_envelope(EnvelopeKind::Debezium)`] or
/// [`CdcConfig::for_envelope(EnvelopeKind::Maxwell)`] yields
/// a fully-populated config that needs no further fields.
/// `EnvelopeKind::Custom` requires every field path explicitly
/// set; [`CdcConfig::validate`] rejects custom envelopes with
/// missing required fields.
///
/// All field-path strings are dot-delimited paths into the
/// decoded message body (e.g. `"after.id"` for the Debezium
/// after-payload's `id` column, `"source.ts_ms"` for the
/// Debezium source-block timestamp). Path resolution is done
/// by the executor in PR 3.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CdcConfig {
    /// Wire-format family. See [`EnvelopeKind`].
    pub envelope: EnvelopeKind,

    /// Path to the operation discriminator in the decoded
    /// message body. Debezium: `"op"`. Maxwell: `"type"`.
    pub op_field: String,

    /// Path to the pre-image (`before` row state). `None` is
    /// allowed for envelopes that don't ship pre-images for
    /// every op (Maxwell omits `old` on inserts).
    pub before_field: Option<String>,

    /// Path to the post-image (`after` row state). For
    /// `Delete` ops, the `before` payload is used instead and
    /// this field's content is typically null; the executor
    /// reads from whichever side is non-null.
    pub after_field: String,

    /// Path used to extract the primary key from each event.
    /// Typically `"after.<pk_col>"`; for `Delete` the executor
    /// falls back to `"before.<pk_col>"`.
    pub key_field: String,

    /// Path to a per-event timestamp used for idempotency
    /// (last-seen-ts-per-pk in the StateStore). `None`
    /// disables idempotency — Kafka redelivery may double-apply.
    /// Debezium: `Some("source.ts_ms")`. Maxwell: `Some("ts")`.
    pub ts_field: Option<String>,

    /// Wire-format `op_field` value → canonical [`CdcOp`].
    /// Debezium: `c→Create, u→Update, d→Delete, r→Read`.
    /// Maxwell: `insert→Create, update→Update, delete→Delete`.
    /// Custom envelopes pass an arbitrary mapping.
    #[serde(default)]
    pub op_map: HashMap<String, CdcOp>,

    /// How `Delete` ops translate. Default [`DeleteMode::Hard`].
    #[serde(default)]
    pub delete_mode: DeleteMode,

    /// How to react when `after` payloads grow new columns.
    /// Default [`SchemaEvolutionPolicy::Skip`].
    #[serde(default)]
    pub schema_evolution: SchemaEvolutionPolicy,

    /// Tolerance for per-key out-of-order events (ms). Kafka
    /// guarantees per-partition order, and Debezium partitions
    /// by PK by default — so events for a given key arrive in
    /// order. The executor `tracing::warn!`s when an incoming
    /// per-key timestamp goes backwards by more than this
    /// budget. Default: 5 seconds.
    #[serde(default = "default_out_of_order_tolerance_ms")]
    pub out_of_order_tolerance_ms: i64,
}

fn default_out_of_order_tolerance_ms() -> i64 {
    5_000
}

impl CdcConfig {
    /// Construct a fully-populated config from a built-in
    /// envelope kind. For [`EnvelopeKind::Custom`] this returns
    /// a skeleton with empty path fields — the caller must
    /// supply every required path before [`Self::validate`]
    /// will accept the config.
    pub fn for_envelope(envelope: EnvelopeKind) -> Self {
        match envelope {
            EnvelopeKind::Debezium => Self::debezium_canonical(),
            EnvelopeKind::Maxwell => Self::maxwell_canonical(),
            EnvelopeKind::Custom => Self {
                envelope: EnvelopeKind::Custom,
                op_field: String::new(),
                before_field: None,
                after_field: String::new(),
                key_field: String::new(),
                ts_field: None,
                op_map: HashMap::new(),
                delete_mode: DeleteMode::default(),
                schema_evolution: SchemaEvolutionPolicy::default(),
                out_of_order_tolerance_ms: default_out_of_order_tolerance_ms(),
            },
        }
    }

    fn debezium_canonical() -> Self {
        let mut op_map = HashMap::with_capacity(4);
        op_map.insert("c".into(), CdcOp::Create);
        op_map.insert("u".into(), CdcOp::Update);
        op_map.insert("d".into(), CdcOp::Delete);
        op_map.insert("r".into(), CdcOp::Read);
        Self {
            envelope: EnvelopeKind::Debezium,
            op_field: "op".into(),
            before_field: Some("before".into()),
            after_field: "after".into(),
            // Default to `after.id` — the executor handles the
            // delete-side fallback to `before.id` itself. Users
            // with non-`id` PKs override via the explicit-field
            // path on the typed-Python or TOML surface.
            key_field: "after.id".into(),
            ts_field: Some("source.ts_ms".into()),
            op_map,
            delete_mode: DeleteMode::default(),
            schema_evolution: SchemaEvolutionPolicy::default(),
            out_of_order_tolerance_ms: default_out_of_order_tolerance_ms(),
        }
    }

    fn maxwell_canonical() -> Self {
        let mut op_map = HashMap::with_capacity(3);
        op_map.insert("insert".into(), CdcOp::Create);
        op_map.insert("update".into(), CdcOp::Update);
        op_map.insert("delete".into(), CdcOp::Delete);
        Self {
            envelope: EnvelopeKind::Maxwell,
            op_field: "type".into(),
            before_field: Some("old".into()),
            after_field: "data".into(),
            key_field: "data.id".into(),
            ts_field: Some("ts".into()),
            op_map,
            delete_mode: DeleteMode::default(),
            schema_evolution: SchemaEvolutionPolicy::default(),
            out_of_order_tolerance_ms: default_out_of_order_tolerance_ms(),
        }
    }

    /// Reject obviously-broken configs at config-load time.
    /// Catches the common errors:
    ///
    /// - `Custom` envelope with required field paths empty.
    /// - `op_map` empty (executor wouldn't know how to dispatch).
    /// - `Soft` delete mode with empty column name.
    pub fn validate(&self) -> Result<(), String> {
        if self.envelope == EnvelopeKind::Custom {
            if self.op_field.is_empty() {
                return Err("[transform.cdc] envelope = \"custom\" requires op_field".into());
            }
            if self.after_field.is_empty() {
                return Err("[transform.cdc] envelope = \"custom\" requires after_field".into());
            }
            if self.key_field.is_empty() {
                return Err("[transform.cdc] envelope = \"custom\" requires key_field".into());
            }
        }
        if self.op_map.is_empty() {
            return Err(
                "[transform.cdc] op_map cannot be empty — executor needs at least \
                 one wire-format op to dispatch on"
                    .into(),
            );
        }
        if let DeleteMode::Soft { column } = &self.delete_mode
            && column.is_empty()
        {
            return Err(
                "[transform.cdc] delete_mode = \"soft\" requires soft_delete_column \
                 (a non-empty target column name)"
                    .into(),
            );
        }
        Ok(())
    }
}

impl Default for CdcConfig {
    /// Defaults to canonical Debezium — the most common case.
    /// Other envelopes use [`Self::for_envelope`].
    fn default() -> Self {
        Self::debezium_canonical()
    }
}

// =================================================================
// Phase Δ PR 2 — envelope parser
// =================================================================
//
// Operates on `serde_json::Value` rows so it's independent of the
// upstream wire format (Avro / JSON / Protobuf all decode to JSON-
// equivalent structs once SR-aware decoding has happened in the
// Kafka source). The PR 3 executor reads RecordBatches off the
// streaming source, converts each row to `serde_json::Map<String,
// Value>` via `arrow_json`, then feeds the row here.

use serde_json::{Map, Value};

/// One parsed CDC event, post envelope-resolution. The PR 3
/// executor consumes these — groups by op, dispatches to the
/// per-op strategy executor, and atomically commits the
/// resulting target writes alongside the StateStore offset.
#[derive(Debug, Clone, PartialEq)]
pub struct CdcEvent {
    /// Canonicalised operation. The wire-format op string was
    /// looked up in [`CdcConfig::op_map`] to produce this.
    pub op: CdcOp,
    /// Primary-key value extracted from the configured `key_field`.
    /// Stored as JSON so composite keys (objects) and scalar keys
    /// share a representation. The executor uses this verbatim as
    /// the StateStore lookup key + the target-side WHERE clause
    /// value.
    pub key: Value,
    /// Per-event timestamp from `ts_field`, in milliseconds. `None`
    /// when the config doesn't have a `ts_field` set, OR when the
    /// resolved value isn't a number; the executor falls back to
    /// processing time for idempotency in that case (with a
    /// `tracing::warn!`).
    pub ts_ms: Option<i64>,
    /// Post-image (the `after` payload). For `Delete` ops with no
    /// `after` payload (Debezium delete records have null `after`)
    /// this is `None` and the executor reads from `before` instead.
    pub after: Option<Map<String, Value>>,
    /// Pre-image (the `before` payload). `None` for inserts /
    /// snapshot reads where the source row didn't exist before.
    pub before: Option<Map<String, Value>>,
}

/// Errors specific to envelope parsing. Distinct from
/// [`crate::backend::BackendError`] so the executor can choose to
/// handle parse errors differently from backend errors (e.g.
/// route to DLQ instead of failing the pipeline).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CdcParseError {
    /// The configured `op_field` path didn't resolve to a string,
    /// or resolved to a string not present in `op_map`.
    #[error("CDC envelope: op_field {path:?} → {value:?} not in op_map")]
    UnknownOp { path: String, value: String },

    /// A path required to extract data didn't resolve (e.g.
    /// `key_field = "after.id"` but the row has no `after` column,
    /// or the `after` struct has no `id` field). Different from
    /// "resolved-but-null" — that's allowed for optional paths.
    #[error("CDC envelope: required path {path:?} not present in row")]
    MissingPath { path: String },

    /// A path resolved but the value's type is wrong for the
    /// expected use (e.g. `op_field` resolved to a number).
    #[error("CDC envelope: path {path:?} resolved to wrong type ({reason})")]
    WrongType { path: String, reason: String },
}

/// Parse one decoded row into a [`CdcEvent`].
///
/// Returns:
/// - `Ok(Some(event))` — successful parse, executor applies it.
/// - `Ok(None)` — tombstone (Debezium emits a record with a null
///   value after each delete). Executor skips.
/// - `Err(...)` — malformed envelope (unknown op, missing
///   required field). Executor's `transform_on_error` policy
///   decides: fail the pipeline / drop the row / route to DLQ.
///
/// Path semantics: dot-delimited dot-paths like `"after.id"` walk
/// nested JSON objects. A leaf value of `null` is *not* a missing
/// path — `Value::Null` is preserved (the executor uses it for
/// nullable column INSERTs). Missing keys mid-path raise
/// [`CdcParseError::MissingPath`].
pub fn parse_event(
    row: &Map<String, Value>,
    cfg: &CdcConfig,
) -> Result<Option<CdcEvent>, CdcParseError> {
    // Tombstone detection: a row that's empty (post-delete null
    // payload from Debezium, decoded as an empty map by the
    // upstream JSON layer) skips. We don't trust just `op` being
    // missing because some Avro decoders emit `op = ""` on
    // tombstones — empty-row check is the most reliable signal.
    if row.is_empty() {
        return Ok(None);
    }

    let row_value = Value::Object(row.clone());

    // Extract op. The op_field path *must* resolve to a string.
    let op_value =
        resolve_path(&row_value, &cfg.op_field).ok_or_else(|| CdcParseError::MissingPath {
            path: cfg.op_field.clone(),
        })?;
    let op_str = op_value.as_str().ok_or_else(|| CdcParseError::WrongType {
        path: cfg.op_field.clone(),
        reason: format!("expected string, got {}", json_type_name(op_value)),
    })?;
    let op = cfg
        .op_map
        .get(op_str)
        .copied()
        .ok_or_else(|| CdcParseError::UnknownOp {
            path: cfg.op_field.clone(),
            value: op_str.to_string(),
        })?;

    // Extract after. Required path, but Delete ops legitimately
    // have null/missing after — try both before/after for the key
    // extraction below.
    let after = extract_object_at(&row_value, &cfg.after_field);

    // Extract before — optional even if `before_field` is set,
    // because inserts have null before.
    let before = cfg
        .before_field
        .as_deref()
        .and_then(|p| extract_object_at(&row_value, p));

    // Extract key. For deletes with null `after`, fall back to
    // the same path on `before` — the canonical Debezium form
    // ships PK in both pre- and post-image. The fallback only
    // kicks in when the original key_field path resolves to
    // null/missing AND we have a before image.
    let key = match resolve_path(&row_value, &cfg.key_field) {
        Some(v) if !v.is_null() => v.clone(),
        _ => {
            // Try the equivalent before-side path. Debezium's
            // canonical key_field is `"after.id"` — for deletes
            // we want `"before.id"`. Substitute the path's first
            // segment if before_field is configured.
            if let (Some(before_root), Some((_, rest))) =
                (cfg.before_field.as_deref(), cfg.key_field.split_once('.'))
            {
                let fallback_path = format!("{before_root}.{rest}");
                resolve_path(&row_value, &fallback_path)
                    .filter(|v| !v.is_null())
                    .cloned()
                    .ok_or_else(|| CdcParseError::MissingPath {
                        path: cfg.key_field.clone(),
                    })?
            } else {
                return Err(CdcParseError::MissingPath {
                    path: cfg.key_field.clone(),
                });
            }
        }
    };

    // Extract ts. Optional. Best-effort numeric coercion: integer
    // ms (Debezium), integer seconds (Maxwell — auto-coerced to ms
    // by multiplying), or string-encoded number. Anything we can't
    // coerce to i64 yields `None` so the executor falls back to
    // processing time.
    let ts_ms = cfg.ts_field.as_deref().and_then(|p| {
        let v = resolve_path(&row_value, p)?;
        match v {
            Value::Number(n) => {
                let ms = n.as_i64()?;
                // Maxwell ships seconds; Debezium ms. Heuristic:
                // values < 10^11 (≈ 5138 AD in seconds) are likely
                // seconds, scale up. Values ≥ 10^11 stay ms.
                if ms < 100_000_000_000 {
                    Some(ms.checked_mul(1_000)?)
                } else {
                    Some(ms)
                }
            }
            Value::String(s) => s.parse().ok(),
            _ => None,
        }
    });

    Ok(Some(CdcEvent {
        op,
        key,
        ts_ms,
        after,
        before,
    }))
}

/// Walk a dot-delimited path through a `serde_json::Value`.
/// Returns `None` if any segment is missing or a non-object is
/// indexed; returns `Some(&Value::Null)` if a segment resolves to
/// an explicit JSON null. The caller distinguishes the two — for
/// required paths a missing segment is an error, but a
/// resolved-null is a legitimate value.
fn resolve_path<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = root;
    for segment in path.split('.') {
        if segment.is_empty() {
            return None;
        }
        match current {
            Value::Object(map) => match map.get(segment) {
                Some(v) => current = v,
                None => return None,
            },
            // Indexing into a non-object (or null) — path doesn't
            // resolve. The executor reports this as a parse error
            // for required fields.
            _ => return None,
        }
    }
    Some(current)
}

/// Resolve a path expected to point at a JSON object (struct).
/// Returns `None` if the path is missing OR resolves to null OR
/// resolves to a non-object value. This is the right shape for
/// `before` / `after` extraction since both should be either a
/// struct or absent.
fn extract_object_at(root: &Value, path: &str) -> Option<Map<String, Value>> {
    match resolve_path(root, path)? {
        Value::Object(map) => Some(map.clone()),
        _ => None,
    }
}

fn json_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

// =================================================================
// Phase Δ PR 3 — RecordBatch → JSON rows bridge + event lifter
// =================================================================

use crate::backend::BackendError;
use arrow_array::RecordBatch;
use std::io::Cursor;

/// Convert a `RecordBatch` to a `Vec<Map<String, Value>>` —
/// one JSON object per row. Bridges the Kafka source's Arrow
/// output to the parser's JSON input.
///
/// Implementation: serializes the batch to JSON-Lines via
/// `arrow_json::LineDelimitedWriter`, then re-parses each line
/// as a `serde_json::Value`. The double-pass costs one extra
/// allocation per row but keeps the parser independent of Arrow's
/// type system — adding a new wire format (Avro-with-logical-
/// types, Protobuf-with-extensions) doesn't require changing the
/// parser.
///
/// Throughput: at typical CDC volumes (1-10 K events/s) the JSON
/// roundtrip is well under the per-batch budget; target-side
/// commits dominate. If a profile shows this as a hot path, the
/// fix is a direct Arrow → `serde_json::Value` walker that skips
/// the intermediate UTF-8 — but premature optimisation.
pub fn batch_to_json_rows(
    batch: &RecordBatch,
) -> Result<Vec<serde_json::Map<String, Value>>, BackendError> {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = arrow_json::LineDelimitedWriter::new(&mut buf);
        writer
            .write(batch)
            .map_err(|e| BackendError::Other(format!("cdc batch → json: {e}")))?;
        writer
            .finish()
            .map_err(|e| BackendError::Other(format!("cdc batch → json finish: {e}")))?;
    }

    let mut rows = Vec::with_capacity(batch.num_rows());
    let cursor = Cursor::new(&buf);
    for line_result in std::io::BufRead::lines(std::io::BufReader::new(cursor)) {
        let line =
            line_result.map_err(|e| BackendError::Other(format!("cdc batch → json read: {e}")))?;
        if line.is_empty() {
            // Trailing newline from LineDelimitedWriter.
            continue;
        }
        let value: Value = serde_json::from_str(&line)
            .map_err(|e| BackendError::Other(format!("cdc batch → json parse: {e}")))?;
        match value {
            Value::Object(map) => rows.push(map),
            other => {
                return Err(BackendError::Other(format!(
                    "cdc batch → json: expected each row to serialize as a JSON \
                     object, got {}",
                    json_type_name(&other)
                )));
            }
        }
    }
    Ok(rows)
}

/// One row's parse outcome. Lets the executor distinguish events
/// (apply), tombstones (skip silently), and parse errors (skip
/// + count + log).
#[derive(Debug)]
pub enum ParsedRow {
    /// A real change event the executor applies to the target.
    Event(CdcEvent),
    /// Tombstone or empty-row pattern — caller skips.
    Tombstone,
    /// Envelope parse failed. The executor counts these as
    /// `skipped` and logs a warning. Held for the executor to
    /// surface in metrics or DLQ-routing.
    ParseError(CdcParseError),
}

/// Convert a RecordBatch directly into per-row parse outcomes,
/// chaining [`batch_to_json_rows`] + [`parse_event`].
pub fn events_from_batch(
    batch: &RecordBatch,
    cfg: &CdcConfig,
) -> Result<Vec<ParsedRow>, BackendError> {
    let rows = batch_to_json_rows(batch)?;
    Ok(rows
        .into_iter()
        .map(|row| match parse_event(&row, cfg) {
            Ok(Some(event)) => ParsedRow::Event(event),
            Ok(None) => ParsedRow::Tombstone,
            Err(e) => ParsedRow::ParseError(e),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debezium_canonical_has_full_field_mapping() {
        let cfg = CdcConfig::for_envelope(EnvelopeKind::Debezium);
        assert_eq!(cfg.op_field, "op");
        assert_eq!(cfg.before_field.as_deref(), Some("before"));
        assert_eq!(cfg.after_field, "after");
        assert_eq!(cfg.ts_field.as_deref(), Some("source.ts_ms"));
        assert_eq!(cfg.op_map.get("c"), Some(&CdcOp::Create));
        assert_eq!(cfg.op_map.get("u"), Some(&CdcOp::Update));
        assert_eq!(cfg.op_map.get("d"), Some(&CdcOp::Delete));
        assert_eq!(cfg.op_map.get("r"), Some(&CdcOp::Read));
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn maxwell_canonical_has_full_field_mapping() {
        let cfg = CdcConfig::for_envelope(EnvelopeKind::Maxwell);
        assert_eq!(cfg.op_field, "type");
        assert_eq!(cfg.after_field, "data");
        assert_eq!(cfg.before_field.as_deref(), Some("old"));
        assert_eq!(cfg.op_map.get("insert"), Some(&CdcOp::Create));
        assert_eq!(cfg.op_map.get("update"), Some(&CdcOp::Update));
        assert_eq!(cfg.op_map.get("delete"), Some(&CdcOp::Delete));
        // Maxwell doesn't ship snapshot ops; `r` shouldn't appear.
        assert_eq!(cfg.op_map.get("r"), None);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn custom_envelope_skeleton_fails_validate_until_filled_in() {
        let mut cfg = CdcConfig::for_envelope(EnvelopeKind::Custom);
        // Empty out of the box → validation rejects.
        assert!(cfg.validate().is_err());

        // Fill in the required paths + at least one op_map entry.
        cfg.op_field = "action".into();
        cfg.after_field = "new_state".into();
        cfg.key_field = "new_state.id".into();
        cfg.op_map.insert("INSERT".into(), CdcOp::Create);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn cdc_config_round_trips_through_serde_json() {
        let original = CdcConfig::for_envelope(EnvelopeKind::Debezium);
        let json = serde_json::to_string(&original).expect("serialize");
        let recovered: CdcConfig = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(original, recovered);
    }

    #[test]
    fn delete_mode_soft_round_trips() {
        let original = CdcConfig {
            delete_mode: DeleteMode::Soft {
                column: "deleted_at".into(),
            },
            ..CdcConfig::default()
        };
        let json = serde_json::to_string(&original).unwrap();
        // Internally tagged enum — kind discriminator + fields
        // sit at the same level for soft delete.
        assert!(json.contains(r#""kind":"soft""#));
        assert!(json.contains(r#""column":"deleted_at""#));
        let recovered: CdcConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(original, recovered);
    }

    #[test]
    fn delete_mode_soft_with_empty_column_rejected() {
        let cfg = CdcConfig {
            delete_mode: DeleteMode::Soft {
                column: String::new(),
            },
            ..CdcConfig::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(err.contains("soft_delete_column"), "got: {err}");
    }

    #[test]
    fn empty_op_map_rejected() {
        let mut cfg = CdcConfig::default();
        cfg.op_map.clear();
        let err = cfg.validate().unwrap_err();
        assert!(err.contains("op_map"), "got: {err}");
    }

    #[test]
    fn schema_evolution_round_trips_all_variants() {
        for policy in [SchemaEvolutionPolicy::Skip, SchemaEvolutionPolicy::Fail] {
            let cfg = CdcConfig {
                schema_evolution: policy,
                ..CdcConfig::default()
            };
            let json = serde_json::to_string(&cfg).unwrap();
            let recovered: CdcConfig = serde_json::from_str(&json).unwrap();
            assert_eq!(cfg.schema_evolution, recovered.schema_evolution);
        }
    }

    #[test]
    fn envelope_kind_serializes_snake_case() {
        let json = serde_json::to_string(&EnvelopeKind::Debezium).unwrap();
        assert_eq!(json, r#""debezium""#);
        let json = serde_json::to_string(&EnvelopeKind::Maxwell).unwrap();
        assert_eq!(json, r#""maxwell""#);
        let json = serde_json::to_string(&EnvelopeKind::Custom).unwrap();
        assert_eq!(json, r#""custom""#);
    }

    #[test]
    fn out_of_order_tolerance_default_5s() {
        let cfg = CdcConfig::default();
        assert_eq!(cfg.out_of_order_tolerance_ms, 5_000);
    }

    // -------------------------------------------------------------
    // PR 2 — envelope-parser tests.
    //
    // Hand-crafted JSON values representative of real Debezium /
    // Maxwell payloads. The PR 3 executor will exercise the
    // Arrow-row → JSON conversion separately; these tests pin the
    // envelope-resolution logic in isolation.
    // -------------------------------------------------------------

    use serde_json::json;

    fn debezium_insert_row() -> Map<String, Value> {
        // Canonical Debezium INSERT for a Postgres source.
        json!({
            "before": null,
            "after": {"id": 42, "email": "alice@example.com", "name": "Alice"},
            "source": {"ts_ms": 1_700_000_000_000_i64, "db": "shop", "table": "customers"},
            "op": "c",
            "ts_ms": 1_700_000_000_001_i64,
        })
        .as_object()
        .unwrap()
        .clone()
    }

    fn debezium_update_row() -> Map<String, Value> {
        json!({
            "before": {"id": 42, "email": "alice@example.com", "name": "Alice"},
            "after": {"id": 42, "email": "alice@example.com", "name": "Alice Smith"},
            "source": {"ts_ms": 1_700_000_005_000_i64, "db": "shop", "table": "customers"},
            "op": "u",
            "ts_ms": 1_700_000_005_001_i64,
        })
        .as_object()
        .unwrap()
        .clone()
    }

    fn debezium_delete_row() -> Map<String, Value> {
        // Debezium delete: `after` is null, `before` carries the
        // old row (including the PK).
        json!({
            "before": {"id": 42, "email": "alice@example.com", "name": "Alice Smith"},
            "after": null,
            "source": {"ts_ms": 1_700_000_010_000_i64, "db": "shop", "table": "customers"},
            "op": "d",
            "ts_ms": 1_700_000_010_001_i64,
        })
        .as_object()
        .unwrap()
        .clone()
    }

    fn debezium_snapshot_row() -> Map<String, Value> {
        // Debezium emits `r` while replaying the initial snapshot.
        json!({
            "before": null,
            "after": {"id": 1, "email": "first@example.com", "name": "First"},
            "source": {"ts_ms": 1_700_000_000_000_i64, "snapshot": "true"},
            "op": "r",
            "ts_ms": 1_700_000_000_000_i64,
        })
        .as_object()
        .unwrap()
        .clone()
    }

    fn debezium_cfg_with_pk(pk: &str) -> CdcConfig {
        let mut cfg = CdcConfig::for_envelope(EnvelopeKind::Debezium);
        cfg.key_field = format!("after.{pk}");
        cfg
    }

    #[test]
    fn parse_debezium_insert() {
        let cfg = debezium_cfg_with_pk("id");
        let row = debezium_insert_row();
        let event = parse_event(&row, &cfg).unwrap().expect("non-tombstone");
        assert_eq!(event.op, CdcOp::Create);
        assert_eq!(event.key, json!(42));
        assert_eq!(event.ts_ms, Some(1_700_000_000_000));
        let after = event.after.expect("after present");
        assert_eq!(after.get("name"), Some(&json!("Alice")));
        assert!(event.before.is_none(), "INSERT has no before image");
    }

    #[test]
    fn parse_debezium_update() {
        let cfg = debezium_cfg_with_pk("id");
        let row = debezium_update_row();
        let event = parse_event(&row, &cfg).unwrap().expect("non-tombstone");
        assert_eq!(event.op, CdcOp::Update);
        assert_eq!(event.key, json!(42));
        let before = event.before.expect("before present on update");
        assert_eq!(before.get("name"), Some(&json!("Alice")));
        let after = event.after.expect("after present on update");
        assert_eq!(after.get("name"), Some(&json!("Alice Smith")));
    }

    #[test]
    fn parse_debezium_delete_falls_back_to_before_for_key() {
        let cfg = debezium_cfg_with_pk("id");
        let row = debezium_delete_row();
        let event = parse_event(&row, &cfg).unwrap().expect("non-tombstone");
        assert_eq!(event.op, CdcOp::Delete);
        // key_field = "after.id" but `after` is null on delete →
        // parser falls back to `before.id`. Lock the contract.
        assert_eq!(event.key, json!(42));
        assert!(event.after.is_none(), "DELETE has null after");
        let before = event.before.expect("before present on delete");
        assert_eq!(before.get("id"), Some(&json!(42)));
    }

    #[test]
    fn parse_debezium_snapshot_read_routes_to_read_op() {
        let cfg = debezium_cfg_with_pk("id");
        let row = debezium_snapshot_row();
        let event = parse_event(&row, &cfg).unwrap().expect("non-tombstone");
        assert_eq!(event.op, CdcOp::Read, "snapshot row maps to Read");
        assert_eq!(event.key, json!(1));
    }

    #[test]
    fn parse_tombstone_returns_none() {
        // Tombstone after delete — empty payload after upstream
        // null-payload decode. Parser returns None; executor skips.
        let cfg = debezium_cfg_with_pk("id");
        let row = Map::new();
        let result = parse_event(&row, &cfg).unwrap();
        assert!(result.is_none(), "tombstone → Ok(None)");
    }

    #[test]
    fn parse_unknown_op_string_errors() {
        let cfg = debezium_cfg_with_pk("id");
        let mut row = debezium_insert_row();
        row.insert("op".into(), json!("WAT"));
        let err = parse_event(&row, &cfg).unwrap_err();
        match err {
            CdcParseError::UnknownOp { value, .. } => assert_eq!(value, "WAT"),
            other => panic!("expected UnknownOp, got {other:?}"),
        }
    }

    #[test]
    fn parse_op_with_wrong_type_errors() {
        let cfg = debezium_cfg_with_pk("id");
        let mut row = debezium_insert_row();
        row.insert("op".into(), json!(42)); // op as number, not string
        let err = parse_event(&row, &cfg).unwrap_err();
        match err {
            CdcParseError::WrongType { path, reason } => {
                assert_eq!(path, "op");
                assert!(reason.contains("number"), "reason: {reason}");
            }
            other => panic!("expected WrongType, got {other:?}"),
        }
    }

    #[test]
    fn parse_missing_op_field_errors() {
        let cfg = debezium_cfg_with_pk("id");
        let mut row = debezium_insert_row();
        row.remove("op");
        let err = parse_event(&row, &cfg).unwrap_err();
        match err {
            CdcParseError::MissingPath { path } => assert_eq!(path, "op"),
            other => panic!("expected MissingPath, got {other:?}"),
        }
    }

    #[test]
    fn parse_maxwell_envelope() {
        // Maxwell INSERT shape:
        //   { type: "insert", database: "shop", table: "customers",
        //     ts: 1700000000, data: {id:1, name:"x"} }
        // Note: ts in seconds, not ms — parser auto-scales.
        let cfg = CdcConfig::for_envelope(EnvelopeKind::Maxwell);
        let row = json!({
            "type": "insert",
            "database": "shop",
            "table": "customers",
            "ts": 1_700_000_000_i64,
            "data": {"id": 7, "name": "Maxine"},
        })
        .as_object()
        .unwrap()
        .clone();

        let event = parse_event(&row, &cfg).unwrap().expect("non-tombstone");
        assert_eq!(event.op, CdcOp::Create);
        assert_eq!(event.key, json!(7));
        // Maxwell ships seconds; parser scales to ms.
        assert_eq!(event.ts_ms, Some(1_700_000_000_000));
        let after = event.after.expect("data present");
        assert_eq!(after.get("name"), Some(&json!("Maxine")));
    }

    #[test]
    fn parse_maxwell_update_with_old_image() {
        let cfg = CdcConfig::for_envelope(EnvelopeKind::Maxwell);
        let row = json!({
            "type": "update",
            "database": "shop",
            "table": "customers",
            "ts": 1_700_000_005_i64,
            "data": {"id": 7, "name": "Maxine Smith"},
            "old":  {"name": "Maxine"},
        })
        .as_object()
        .unwrap()
        .clone();

        let event = parse_event(&row, &cfg).unwrap().expect("non-tombstone");
        assert_eq!(event.op, CdcOp::Update);
        assert_eq!(event.before.unwrap().get("name"), Some(&json!("Maxine")));
        assert_eq!(
            event.after.unwrap().get("name"),
            Some(&json!("Maxine Smith"))
        );
    }

    #[test]
    fn parse_custom_envelope_with_explicit_paths() {
        // Pretend we have an in-house CDC format that ships
        // changes as `{action: "INSERT", new_state: {...}, ...}`.
        let mut cfg = CdcConfig::for_envelope(EnvelopeKind::Custom);
        cfg.op_field = "action".into();
        cfg.before_field = Some("old_state".into());
        cfg.after_field = "new_state".into();
        cfg.key_field = "new_state.id".into();
        cfg.ts_field = Some("changed_at_ms".into());
        cfg.op_map.insert("INSERT".into(), CdcOp::Create);
        cfg.op_map.insert("UPDATE".into(), CdcOp::Update);
        cfg.op_map.insert("DELETE".into(), CdcOp::Delete);
        cfg.validate().unwrap();

        let row = json!({
            "action": "INSERT",
            "old_state": null,
            "new_state": {"id": "u-001", "value": 1},
            "changed_at_ms": 1_700_000_000_999_i64,
        })
        .as_object()
        .unwrap()
        .clone();

        let event = parse_event(&row, &cfg).unwrap().expect("non-tombstone");
        assert_eq!(event.op, CdcOp::Create);
        assert_eq!(event.key, json!("u-001"));
        assert_eq!(event.ts_ms, Some(1_700_000_000_999));
    }

    #[test]
    fn parse_path_resolution_handles_deep_nesting() {
        // Some custom envelopes nest the op deeper. The dot-path
        // resolver should handle arbitrary depth.
        let mut cfg = CdcConfig::for_envelope(EnvelopeKind::Custom);
        cfg.op_field = "envelope.metadata.op".into();
        cfg.after_field = "payload.data".into();
        cfg.key_field = "payload.data.id".into();
        cfg.before_field = None;
        cfg.ts_field = None;
        cfg.op_map.insert("ins".into(), CdcOp::Create);
        cfg.validate().unwrap();

        let row = json!({
            "envelope": {"metadata": {"op": "ins"}},
            "payload": {"data": {"id": 99, "name": "deep"}},
        })
        .as_object()
        .unwrap()
        .clone();

        let event = parse_event(&row, &cfg).unwrap().expect("non-tombstone");
        assert_eq!(event.op, CdcOp::Create);
        assert_eq!(event.key, json!(99));
        assert!(event.ts_ms.is_none(), "no ts_field configured → None");
    }

    #[test]
    fn parse_ts_string_encoded_number_accepted() {
        let mut cfg = CdcConfig::for_envelope(EnvelopeKind::Debezium);
        cfg.key_field = "after.id".into();
        let mut row = debezium_insert_row();
        // Some Avro decoders emit ts_ms as a string. Parser
        // tolerates that.
        row.get_mut("source")
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("ts_ms".into(), json!("1700000000000"));

        let event = parse_event(&row, &cfg).unwrap().expect("non-tombstone");
        assert_eq!(event.ts_ms, Some(1_700_000_000_000));
    }

    #[test]
    fn parse_ts_unparseable_value_yields_none() {
        // Parser is lenient about ts_field — unparseable values
        // yield None; the executor falls back to processing time.
        let mut cfg = CdcConfig::for_envelope(EnvelopeKind::Debezium);
        cfg.key_field = "after.id".into();
        let mut row = debezium_insert_row();
        row.get_mut("source")
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("ts_ms".into(), json!("not a number"));

        let event = parse_event(&row, &cfg).unwrap().expect("non-tombstone");
        assert!(event.ts_ms.is_none());
    }

    // -------------------------------------------------------------
    // PR 3 — RecordBatch → JSON rows + events_from_batch tests.
    //
    // Build small RecordBatches by encoding hand-crafted JSON
    // through arrow-json's reader (the inverse of what
    // batch_to_json_rows does on the way back out). Round-trip
    // exercises the Arrow ↔ JSON conversion + the parser
    // chained together — the same code path the PR 3 Postgres
    // executor uses on every Kafka batch.
    // -------------------------------------------------------------

    use arrow_array::RecordBatch;
    use std::io::Cursor;
    use std::sync::Arc;

    fn batch_from_json_objects(rows: &[Map<String, Value>]) -> RecordBatch {
        // Serialize each row to JSON-lines, then re-decode through
        // arrow-json's schema-inferring reader. This is the
        // canonical "round-trip JSON through Arrow" idiom that
        // mirrors how the Kafka source decodes JSON payloads.
        let mut buf = Vec::new();
        for row in rows {
            buf.extend(serde_json::to_string(row).unwrap().bytes());
            buf.push(b'\n');
        }

        // Infer schema then read.
        let mut sniff = Cursor::new(&buf);
        let (schema, _) = arrow_json::reader::infer_json_schema_from_seekable(&mut sniff, None)
            .expect("infer schema");
        let cursor = Cursor::new(&buf);
        arrow_json::ReaderBuilder::new(Arc::new(schema))
            .build(cursor)
            .expect("build reader")
            .next()
            .expect("at least one batch")
            .expect("decode ok")
    }

    #[test]
    fn batch_to_json_rows_round_trips_single_row() {
        let original = vec![debezium_insert_row()];
        let batch = batch_from_json_objects(&original);
        let recovered = batch_to_json_rows(&batch).expect("convert");
        assert_eq!(recovered.len(), 1);
        // The arrow-json round-trip preserves the structure but
        // may shuffle field ordering inside structs — compare by
        // deep equivalence rather than literal byte equality.
        let expected_after = original[0].get("after").unwrap();
        let recovered_after = Value::Object(recovered[0].clone())
            .get("after")
            .cloned()
            .unwrap();
        assert_eq!(*expected_after, recovered_after);
    }

    #[test]
    fn batch_to_json_rows_round_trips_multi_row() {
        let original = vec![
            debezium_insert_row(),
            debezium_update_row(),
            debezium_snapshot_row(),
        ];
        let batch = batch_from_json_objects(&original);
        let recovered = batch_to_json_rows(&batch).expect("convert");
        assert_eq!(recovered.len(), 3);
        // Ops survive the round-trip — that's the load-bearing
        // assertion for PR 3's executor (the parser keys off it).
        assert_eq!(recovered[0].get("op"), Some(&json!("c")));
        assert_eq!(recovered[1].get("op"), Some(&json!("u")));
        assert_eq!(recovered[2].get("op"), Some(&json!("r")));
    }

    #[test]
    fn events_from_batch_chains_decode_and_parse() {
        // Three Debezium events in one batch: insert + update +
        // snapshot read. The full PR 3 executor's input shape.
        let cfg = debezium_cfg_with_pk("id");
        let original = vec![
            debezium_insert_row(),
            debezium_update_row(),
            debezium_snapshot_row(),
        ];
        let batch = batch_from_json_objects(&original);
        let parsed = events_from_batch(&batch, &cfg).expect("convert + parse");
        assert_eq!(parsed.len(), 3);
        for row in &parsed {
            assert!(
                matches!(row, ParsedRow::Event(_)),
                "every Debezium-shaped row should parse to an Event, got {row:?}"
            );
        }
        let ops: Vec<CdcOp> = parsed
            .iter()
            .map(|r| match r {
                ParsedRow::Event(e) => e.op,
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(ops, vec![CdcOp::Create, CdcOp::Update, CdcOp::Read]);
    }

    #[test]
    fn events_from_batch_surfaces_parse_errors_per_row() {
        // Mix a valid INSERT with a row whose `op` is unknown.
        // The executor needs to see both parse outcomes from one
        // call so it can apply the valid event + count the bad
        // one as `skipped`.
        let cfg = debezium_cfg_with_pk("id");
        let mut bad = debezium_insert_row();
        bad.insert("op".into(), json!("WAT"));
        let original = vec![debezium_insert_row(), bad];
        let batch = batch_from_json_objects(&original);
        let parsed = events_from_batch(&batch, &cfg).expect("convert");
        assert_eq!(parsed.len(), 2);
        assert!(matches!(parsed[0], ParsedRow::Event(_)));
        assert!(
            matches!(
                &parsed[1],
                ParsedRow::ParseError(CdcParseError::UnknownOp { .. })
            ),
            "second row should surface UnknownOp; got {:?}",
            parsed[1]
        );
    }
}
