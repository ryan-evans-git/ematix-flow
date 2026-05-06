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
//! ## What ships in PR 1 (this commit): scaffolding only
//!
//! - [`CdcConfig`] struct + the [`EnvelopeKind`] / [`CdcOp`] /
//!   [`DeleteMode`] / [`SchemaEvolutionPolicy`] enums.
//! - Canonical field-mapping defaults for Debezium and Maxwell
//!   envelopes (so `CDC(envelope="debezium")` is a one-liner).
//! - Serde derives + round-trip tests so config travels cleanly
//!   through the same `BackendConfig` round-trip the rest of
//!   the connector trait uses.
//!
//! Execution (per-op dispatch via [`Backend::run_cdc`],
//! envelope parsing, idempotency via the StateStore, schema
//! evolution detection) lands in PRs 2–5 per the plan.
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
}
