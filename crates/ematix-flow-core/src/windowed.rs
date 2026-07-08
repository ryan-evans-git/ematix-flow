//! Phase 39.4: tumbling + hopping window aggregations.
//!
//! [`WindowedAggregateTransform`] is a stateful [`BatchTransform`]
//! that buffers incoming rows into per-window per-group accumulators
//! and emits aggregated `RecordBatch`es when the pipeline's
//! watermark crosses a window's end.
//!
//! # Composition
//!
//! The transform optionally wraps an inner [`LazySqlTransform`] for
//! the SQL pre-stage (filter / project / cast / lookup-join — all
//! the Phase 39.1–39.3 machinery). The pipeline holds a single
//! `Arc<dyn BatchTransform>`; the windowed transform's constructor
//! takes the SQL pre-stage as an inner.
//!
//! ```text
//!     RecordBatch (with `_event_ts`) ── inner SQL ──▶ filtered batch
//!                                                          │
//!                                                          ▼
//!                                                  per-row windowing
//!                                                  + accumulator update
//!                                                          │
//!                            ctx.global_wm ≥ window_end ──▶ emit
//! ```
//!
//! # State
//!
//! State is keyed by `(window_start_micros, group_key)` and lives
//! behind a `tokio::sync::Mutex` so refresh-or-emit work between
//! batches doesn't race with the pipeline's serial dispatch.
//!
//! # Scope (PR 2)
//!
//! - Window kinds: tumbling, hopping.
//! - Aggregators: count, sum, min, max, avg, first, last.
//! - Late-data policy: `"drop"` only.
//! - Memory cap fail-loud (`max_groups_per_window`).
//!
//! Deferred to PR 2b: `count_distinct` (HLL+ approximate + exact),
//! `late_data = "reopen"` (state retention + re-emit), `late_data = "dlq"`.

use std::collections::{HashMap, HashSet};
use std::hash::RandomState;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::TimestampMicrosecondType;
use arrow_array::{
    Array, ArrayRef, Float64Array, Int64Array, RecordBatch, TimestampMicrosecondArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use hyperloglogplus::{HyperLogLog, HyperLogLogPlus};
use prometheus::{IntCounter, IntCounterVec, IntGauge, Opts, Registry};
use tokio::sync::Mutex;

use crate::backend::BackendError;
use crate::transform::{BatchContext, BatchTransform, LazySqlTransform};

// =====================================================================
// Configuration types
// =====================================================================

/// Window kind.
///
/// - `Tumbling` — non-overlapping fixed-duration windows; `duration_ms`
///   sets the size, `hop_ms` is normalized to `duration_ms`.
/// - `Hopping` — overlapping fixed-duration windows that step by
///   `hop_ms ≤ duration_ms`.
/// - `Session` (Phase 39.5a) — gap-based; per-key sessions span a
///   maximal run of rows with consecutive event-time gap ≤ `gap_ms`.
///   Required: `gap_ms`, `max_session_duration_ms`, non-empty
///   `group_by`. `duration_ms` and `hop_ms` are unused for sessions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowKind {
    Tumbling,
    Hopping,
    /// Phase 39.5a: gap-based per-key sessions. See module docs and
    /// `docs/PHASE_39_5_SESSIONS.md` for semantics.
    Session,
}

/// Aggregation kind. Each variant except `CountDistinct` is
/// fixed-width per group. `CountDistinct` is variable-state by
/// design — see [`CountDistinctMode`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggKind {
    /// `COUNT(*)` — counts all rows including those with NULL in the
    /// referenced column.
    CountStar,
    /// `COUNT(col)` — skips NULL values.
    CountCol,
    Sum,
    Min,
    Max,
    Avg,
    /// First non-null value, ordered by `_event_ts` (smallest wins).
    First,
    /// Last non-null value, ordered by `_event_ts` (largest wins).
    Last,
    /// PR 2b: distinct-count over a column. Driven by
    /// [`AggregationSpec::count_distinct_mode`].
    CountDistinct,
}

/// PR 2b: how to count distinct values inside `count_distinct`.
///
/// `Approximate` uses HyperLogLog+ at precision 14 — ~16 KB sketch
/// per group, ~0.81% standard error. Recommended default for
/// high-cardinality columns where exact counts aren't needed.
///
/// `Exact` keeps a `HashSet` per group; capped by
/// [`AggregationSpec::max_distinct_values_per_group`] (a fail-loud
/// cap, mirroring `max_groups_per_window`). Memory grows linearly
/// with cardinality — only safe when cardinality is bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CountDistinctMode {
    Approximate,
    Exact,
}

/// Default HLL+ precision parameter. `p = 14` gives 2^14 = 16384
/// registers, ~16 KB per sketch, ~0.81% standard error. Matches
/// the Druid / Snowflake default. Configurable per aggregation in
/// future via `AggregationSpec::hll_precision`; fixed at 14 in
/// PR 2b.
const HLL_PRECISION: u8 = 14;

/// One aggregation entry from `[[transform.window.aggregations]]`.
#[derive(Debug, Clone)]
pub struct AggregationSpec {
    pub kind: AggKind,
    /// Column name to aggregate. `None` only for `COUNT(*)`.
    pub column: Option<String>,
    /// Required output column alias.
    pub alias: String,
    /// PR 2b: only meaningful when `kind = CountDistinct`. Defaults
    /// to `Approximate` when unset.
    pub count_distinct_mode: Option<CountDistinctMode>,
    /// PR 2b: per-group cap on distinct values for
    /// `CountDistinct { Exact }`. Required when mode = `Exact`;
    /// ignored otherwise. Cap-hit fails the pipeline.
    pub max_distinct_values_per_group: Option<usize>,
}

impl AggregationSpec {
    /// Convenience constructor for the common case (count / sum /
    /// min / max / avg / first / last). Use the struct-literal form
    /// directly when configuring `count_distinct`.
    pub fn new(kind: AggKind, column: Option<String>, alias: impl Into<String>) -> Self {
        Self {
            kind,
            column,
            alias: alias.into(),
            count_distinct_mode: None,
            max_distinct_values_per_group: None,
        }
    }
}

/// Late-data handling.
///
/// `Drop` (default): late rows are silently discarded; counter tracks
/// drops by policy label.
///
/// `Reopen` (PR 2c): the window's state is held past `window_end` for
/// `allowed_lateness_ms`. Late arrivals within that budget
/// re-aggregate; the window re-emits with corrected aggregates on the
/// next watermark advance. Past the budget, late arrivals drop. This
/// requires update-style targets (DB MERGE, Delta MERGE) to absorb
/// the multiple emits — append-only sinks see N rows for the same
/// `(window_start, group_key)` pair.
///
/// `Dlq` for late rows is deferred — it requires a separate write
/// path (the existing target-write-failure DLQ can't be reused since
/// late rows haven't failed any write).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LateDataPolicy {
    #[default]
    Drop,
    Reopen {
        allowed_lateness_ms: u64,
    },
    /// Phase 39.5a P1.6: late rows are routed to the pipeline's
    /// configured `dead_letter_topic` instead of being silently
    /// dropped. The transform stages each past-budget row into an
    /// internal buffer; the pipeline drains it after every
    /// `transform()` / `on_idle_tick()` call and forwards via the
    /// existing app-level DLQ path (Kafka-only — same constraint
    /// as the per-batch-write-failure DLQ).
    ///
    /// **Information-loss caveat:** rows arrive at the transform
    /// post-SQL-prestage, so the DLQ receives the projected
    /// columns, not the original Kafka payload. Users who need
    /// raw bytes should rely on the per-batch-write-failure DLQ
    /// path on a non-windowed pipeline shape.
    Dlq,
}

impl LateDataPolicy {
    /// Microseconds of state retention past `window_end`. Drop +
    /// Dlq = 0 (no retention; past-budget rows leave the pipeline
    /// immediately, either dropped or routed to DLQ); Reopen =
    /// the configured budget.
    fn lateness_micros(self) -> i64 {
        match self {
            Self::Drop | Self::Dlq => 0,
            Self::Reopen {
                allowed_lateness_ms,
            } => (allowed_lateness_ms as i64).saturating_mul(1_000),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Drop => "drop",
            Self::Reopen { .. } => "reopen",
            Self::Dlq => "dlq",
        }
    }
}

/// Configuration for a [`WindowedAggregateTransform`].
#[derive(Debug, Clone)]
pub struct WindowConfig {
    pub kind: WindowKind,
    /// Required when `kind = Tumbling | Hopping`; ignored for
    /// `Session` (set to 0 — sessions are gap-based, not fixed-width).
    pub duration_ms: u64,
    /// Required when `kind = Hopping`; must be ≤ `duration_ms` and
    /// strictly positive. For `Tumbling` this field is ignored (set
    /// to `duration_ms` by the constructor). Ignored for `Session`.
    pub hop_ms: u64,
    /// Phase 39.5a: required when `kind = Session`. The per-key gap
    /// budget — rows within `gap_ms` of the prior row's event-time
    /// extend the current session; rows past it open a new one.
    /// `None` for tumbling/hopping.
    pub gap_ms: Option<u64>,
    /// Phase 39.5a: required when `kind = Session`. Hard ceiling on
    /// session duration; the cap is enforced even under
    /// `LateDataPolicy::Reopen`. Force-emit fires before processing
    /// the boundary-crossing row. Must be > `gap_ms`. `None` for
    /// tumbling/hopping.
    pub max_session_duration_ms: Option<u64>,
    /// Column carrying the per-row event timestamp. Defaults to
    /// `"_event_ts"` if the user doesn't override.
    pub event_time_column: String,
    /// Group-by columns. May be empty (single-key aggregation) for
    /// `Tumbling` / `Hopping`. Required non-empty for `Session` —
    /// global sessions span the entire stream and never close.
    pub group_by: Vec<String>,
    pub aggregations: Vec<AggregationSpec>,
    pub late_data: LateDataPolicy,
    /// Per-window cap on the number of distinct group keys. Cap-hit
    /// fails the pipeline. For `Session`, this caps the number of
    /// concurrently-active group keys (each contributes one or more
    /// session blocks).
    pub max_groups_per_window: usize,
    /// Output column name for `window_start`. Defaults to
    /// `"window_start"`.
    pub window_start_column: String,
    /// Output column name for `window_end`. Defaults to `"window_end"`.
    pub window_end_column: String,
    /// Phase 39.5a: output column name for `session_id` (only emitted
    /// when `kind = Session`). Defaults to `"session_id"`.
    pub session_id_column: String,
}

impl WindowConfig {
    /// Validate user-supplied invariants and normalize fields. Called
    /// at construction time; errors are config-load-time errors.
    fn validate(&mut self) -> Result<(), BackendError> {
        match self.kind {
            WindowKind::Tumbling | WindowKind::Hopping => {
                if self.duration_ms == 0 {
                    return Err(BackendError::Other(
                        "window: duration_ms must be > 0".into(),
                    ));
                }
                if self.gap_ms.is_some() {
                    return Err(BackendError::Other(
                        "window: gap_ms is only valid when kind = \"session\"".into(),
                    ));
                }
                if self.max_session_duration_ms.is_some() {
                    return Err(BackendError::Other(
                        "window: max_session_duration_ms is only valid when kind = \"session\""
                            .into(),
                    ));
                }
            }
            WindowKind::Session => {
                // Sessions are gap-based — duration_ms / hop_ms must
                // be left unset. Reject explicit non-zero values so
                // users can't accidentally configure both forms.
                if self.duration_ms != 0 {
                    return Err(BackendError::Other(
                        "window: duration_ms must be 0 (or unset) when kind = \"session\" — \
                         session windows are gap-based, not fixed-width"
                            .into(),
                    ));
                }
                if self.hop_ms != 0 {
                    return Err(BackendError::Other(
                        "window: hop_ms must be 0 (or unset) when kind = \"session\"".into(),
                    ));
                }
            }
        }
        match self.kind {
            WindowKind::Tumbling => {
                // Normalize hop_ms = duration_ms for tumbling so the
                // window-id math is uniform.
                self.hop_ms = self.duration_ms;
            }
            WindowKind::Hopping => {
                if self.hop_ms == 0 {
                    return Err(BackendError::Other(
                        "window: hop_ms must be > 0 for hopping windows".into(),
                    ));
                }
                if self.hop_ms > self.duration_ms {
                    return Err(BackendError::Other(format!(
                        "window: hop_ms ({}) must be <= duration_ms ({}) for hopping windows",
                        self.hop_ms, self.duration_ms
                    )));
                }
            }
            WindowKind::Session => {
                let gap = self.gap_ms.ok_or_else(|| {
                    BackendError::Other("window: gap_ms is required when kind = \"session\"".into())
                })?;
                if gap == 0 {
                    return Err(BackendError::Other("window: gap_ms must be > 0".into()));
                }
                let max_dur = self.max_session_duration_ms.ok_or_else(|| {
                    BackendError::Other(
                        "window: max_session_duration_ms is required when kind = \"session\" — \
                         it bounds session-state memory and prevents pathological long sessions"
                            .into(),
                    )
                })?;
                if max_dur <= gap {
                    return Err(BackendError::Other(format!(
                        "window: max_session_duration_ms ({max_dur}) must be > gap_ms ({gap})"
                    )));
                }
                if self.group_by.is_empty() {
                    return Err(BackendError::Other(
                        "window: group_by must be non-empty when kind = \"session\" — \
                         a global session never closes and creates a hot-spot key"
                            .into(),
                    ));
                }
            }
        }
        if self.aggregations.is_empty() {
            return Err(BackendError::Other(
                "window: at least one aggregation is required".into(),
            ));
        }
        // Alias uniqueness + non-collision with group_by names + non-
        // collision with the canonical window column names.
        let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
        seen.insert(self.window_start_column.as_str());
        if !seen.insert(self.window_end_column.as_str()) {
            return Err(BackendError::Other(format!(
                "window: window_start_column and window_end_column collide ({})",
                self.window_end_column
            )));
        }
        // Session adds a `session_id` column too; keep it unique.
        if matches!(self.kind, WindowKind::Session) && !seen.insert(self.session_id_column.as_str())
        {
            return Err(BackendError::Other(format!(
                "window: session_id_column `{}` collides with another output column",
                self.session_id_column
            )));
        }
        for g in &self.group_by {
            if !seen.insert(g.as_str()) {
                return Err(BackendError::Other(format!(
                    "window: group_by column `{g}` collides with another output column"
                )));
            }
        }
        for a in &self.aggregations {
            if !seen.insert(a.alias.as_str()) {
                return Err(BackendError::Other(format!(
                    "window: aggregation alias `{}` collides with another output column",
                    a.alias
                )));
            }
            if matches!(a.kind, AggKind::CountStar) && a.column.is_some() {
                // Allow but ignore the column; document.
            }
            if !matches!(a.kind, AggKind::CountStar) && a.column.is_none() {
                return Err(BackendError::Other(format!(
                    "window: aggregation `{}` requires a `column` field",
                    a.alias
                )));
            }
        }
        if self.max_groups_per_window == 0 {
            return Err(BackendError::Other(
                "window: max_groups_per_window must be > 0".into(),
            ));
        }
        Ok(())
    }
}

// =====================================================================
// Group keys
// =====================================================================

/// A composite group key built from the row's `group_by` column
/// values. Equality + Hash power the per-window state HashMap.
///
/// Stores the values as a `Vec<KeyValue>` rather than serializing
/// because that keeps the comparison cheap and avoids a hash
/// dependency.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroupKey(pub(crate) Vec<KeyValue>);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum KeyValue {
    Null,
    Int64(i64),
    UInt64(u64),
    Float64Bits(u64),
    Utf8(String),
    TsMicros(i64),
}

impl GroupKey {
    pub(crate) fn from_row(arrays: &[ArrayRef], row: usize) -> Result<Self, BackendError> {
        let mut out: Vec<KeyValue> = Vec::with_capacity(arrays.len());
        for arr in arrays {
            out.push(extract_key(arr, row)?);
        }
        Ok(GroupKey(out))
    }

    /// Phase 39.5a PR 3: borrow the inner values for serialization
    /// helpers (see [`crate::session_blob`]).
    pub fn values(&self) -> &[KeyValue] {
        &self.0
    }

    /// Phase 39.5a PR 3: build a `GroupKey` from a serialized
    /// representation. Used by recovery on pipeline startup.
    pub fn from_values(values: Vec<KeyValue>) -> Self {
        GroupKey(values)
    }
}

fn extract_key(arr: &ArrayRef, row: usize) -> Result<KeyValue, BackendError> {
    if arr.is_null(row) {
        return Ok(KeyValue::Null);
    }
    match arr.data_type() {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            // Cast through Int64 for any signed integer.
            let v = match arr.data_type() {
                DataType::Int8 => arr
                    .as_primitive::<arrow_array::types::Int8Type>()
                    .value(row) as i64,
                DataType::Int16 => arr
                    .as_primitive::<arrow_array::types::Int16Type>()
                    .value(row) as i64,
                DataType::Int32 => arr
                    .as_primitive::<arrow_array::types::Int32Type>()
                    .value(row) as i64,
                DataType::Int64 => arr
                    .as_primitive::<arrow_array::types::Int64Type>()
                    .value(row),
                _ => unreachable!(),
            };
            Ok(KeyValue::Int64(v))
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            let v = match arr.data_type() {
                DataType::UInt8 => arr
                    .as_primitive::<arrow_array::types::UInt8Type>()
                    .value(row) as u64,
                DataType::UInt16 => arr
                    .as_primitive::<arrow_array::types::UInt16Type>()
                    .value(row) as u64,
                DataType::UInt32 => arr
                    .as_primitive::<arrow_array::types::UInt32Type>()
                    .value(row) as u64,
                DataType::UInt64 => arr
                    .as_primitive::<arrow_array::types::UInt64Type>()
                    .value(row),
                _ => unreachable!(),
            };
            Ok(KeyValue::UInt64(v))
        }
        DataType::Float32 | DataType::Float64 => {
            let bits = match arr.data_type() {
                DataType::Float32 => (arr
                    .as_primitive::<arrow_array::types::Float32Type>()
                    .value(row) as f64)
                    .to_bits(),
                DataType::Float64 => arr
                    .as_primitive::<arrow_array::types::Float64Type>()
                    .value(row)
                    .to_bits(),
                _ => unreachable!(),
            };
            Ok(KeyValue::Float64Bits(bits))
        }
        DataType::Utf8 => {
            let s = arr.as_string::<i32>().value(row);
            Ok(KeyValue::Utf8(s.to_string()))
        }
        DataType::LargeUtf8 => {
            let s = arr.as_string::<i64>().value(row);
            Ok(KeyValue::Utf8(s.to_string()))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let v = arr.as_primitive::<TimestampMicrosecondType>().value(row);
            Ok(KeyValue::TsMicros(v))
        }
        other => Err(BackendError::Other(format!(
            "window: group_by column has unsupported type {other:?}"
        ))),
    }
}

// =====================================================================
// Accumulators
// =====================================================================

/// Per-aggregator running state. Enum dispatch keeps the hot loop
/// branch-predictor-friendly and avoids `Box<dyn Trait>` allocations
/// per group.
#[derive(Debug)]
pub enum AccState {
    CountStar(i64),
    CountCol(i64),
    SumI64 {
        sum: i128,
        any: bool,
    },
    SumF64 {
        sum: f64,
        any: bool,
    },
    MinI64(Option<i64>),
    MinF64(Option<f64>),
    MaxI64(Option<i64>),
    MaxF64(Option<f64>),
    AvgI64 {
        sum: i128,
        count: i64,
    },
    AvgF64 {
        sum: f64,
        count: i64,
    },
    FirstI64 {
        ts: Option<i64>,
        value: Option<i64>,
    },
    FirstF64 {
        ts: Option<i64>,
        value: Option<f64>,
    },
    FirstUtf8 {
        ts: Option<i64>,
        value: Option<String>,
    },
    LastI64 {
        ts: Option<i64>,
        value: Option<i64>,
    },
    LastF64 {
        ts: Option<i64>,
        value: Option<f64>,
    },
    LastUtf8 {
        ts: Option<i64>,
        value: Option<String>,
    },
    /// PR 2b: HyperLogLog+ approximate distinct counter on a
    /// numeric column. Numeric values (integer / float) are stored
    /// as their u64 bit pattern so a single HLL handles every
    /// numeric input type.
    CountDistinctHllNumeric(HyperLogLogPlus<u64, RandomState>),
    /// PR 2b: HLL+ approximate distinct counter on a Utf8 column.
    CountDistinctHllUtf8(HyperLogLogPlus<String, RandomState>),
    /// PR 2b: exact distinct counter on a numeric column. Capped
    /// by `AggregationSpec::max_distinct_values_per_group`.
    CountDistinctExactNumeric {
        set: HashSet<u64>,
        cap: usize,
    },
    /// PR 2b: exact distinct counter on a Utf8 column.
    CountDistinctExactUtf8 {
        set: HashSet<String>,
        cap: usize,
    },
}

/// Output type for an aggregation, derived from the input column's
/// data type.
fn agg_output_type(kind: AggKind, input_type: Option<&DataType>) -> Result<DataType, BackendError> {
    match kind {
        AggKind::CountStar | AggKind::CountCol => Ok(DataType::Int64),
        AggKind::Sum => match input_type {
            Some(DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64) => {
                Ok(DataType::Int64)
            }
            Some(DataType::Float32 | DataType::Float64) => Ok(DataType::Float64),
            Some(other) => Err(BackendError::Other(format!(
                "window: SUM on unsupported column type {other:?}"
            ))),
            None => Err(BackendError::Other("window: SUM requires a column".into())),
        },
        AggKind::Min | AggKind::Max | AggKind::First | AggKind::Last => {
            // First/Last/Min/Max preserve the input type for numeric
            // types or coerce strings to Utf8.
            match input_type {
                Some(DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64) => {
                    Ok(DataType::Int64)
                }
                Some(DataType::Float32 | DataType::Float64) => Ok(DataType::Float64),
                Some(DataType::Utf8 | DataType::LargeUtf8) => Ok(DataType::Utf8),
                Some(other) => Err(BackendError::Other(format!(
                    "window: {kind:?} on unsupported column type {other:?}"
                ))),
                None => Err(BackendError::Other(format!(
                    "window: {kind:?} requires a column"
                ))),
            }
        }
        AggKind::Avg => match input_type {
            Some(
                DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64,
            ) => Ok(DataType::Float64),
            Some(other) => Err(BackendError::Other(format!(
                "window: AVG on unsupported column type {other:?}"
            ))),
            None => Err(BackendError::Other("window: AVG requires a column".into())),
        },
        AggKind::CountDistinct => Ok(DataType::UInt64),
    }
}

/// Initial state for an aggregator, parameterized by the input
/// column's data type (when applicable). For `CountDistinct`, the
/// `spec` carries the mode + cap.
fn new_acc_state(
    spec: &AggregationSpec,
    input_type: Option<&DataType>,
) -> Result<AccState, BackendError> {
    let kind = spec.kind;
    match kind {
        AggKind::CountStar => Ok(AccState::CountStar(0)),
        AggKind::CountCol => Ok(AccState::CountCol(0)),
        AggKind::Sum => match input_type {
            Some(DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64) => {
                Ok(AccState::SumI64 { sum: 0, any: false })
            }
            Some(DataType::Float32 | DataType::Float64) => Ok(AccState::SumF64 {
                sum: 0.0,
                any: false,
            }),
            _ => Err(BackendError::Other(format!(
                "window: SUM on unsupported column type {input_type:?}"
            ))),
        },
        AggKind::Min => match input_type {
            Some(DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64) => {
                Ok(AccState::MinI64(None))
            }
            Some(DataType::Float32 | DataType::Float64) => Ok(AccState::MinF64(None)),
            _ => Err(BackendError::Other(format!(
                "window: MIN on unsupported column type {input_type:?}"
            ))),
        },
        AggKind::Max => match input_type {
            Some(DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64) => {
                Ok(AccState::MaxI64(None))
            }
            Some(DataType::Float32 | DataType::Float64) => Ok(AccState::MaxF64(None)),
            _ => Err(BackendError::Other(format!(
                "window: MAX on unsupported column type {input_type:?}"
            ))),
        },
        AggKind::Avg => match input_type {
            Some(DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64) => {
                Ok(AccState::AvgI64 { sum: 0, count: 0 })
            }
            Some(DataType::Float32 | DataType::Float64) => {
                Ok(AccState::AvgF64 { sum: 0.0, count: 0 })
            }
            _ => Err(BackendError::Other(format!(
                "window: AVG on unsupported column type {input_type:?}"
            ))),
        },
        AggKind::First => match input_type {
            Some(DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64) => {
                Ok(AccState::FirstI64 {
                    ts: None,
                    value: None,
                })
            }
            Some(DataType::Float32 | DataType::Float64) => Ok(AccState::FirstF64 {
                ts: None,
                value: None,
            }),
            Some(DataType::Utf8 | DataType::LargeUtf8) => Ok(AccState::FirstUtf8 {
                ts: None,
                value: None,
            }),
            _ => Err(BackendError::Other(format!(
                "window: FIRST on unsupported column type {input_type:?}"
            ))),
        },
        AggKind::Last => match input_type {
            Some(DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64) => {
                Ok(AccState::LastI64 {
                    ts: None,
                    value: None,
                })
            }
            Some(DataType::Float32 | DataType::Float64) => Ok(AccState::LastF64 {
                ts: None,
                value: None,
            }),
            Some(DataType::Utf8 | DataType::LargeUtf8) => Ok(AccState::LastUtf8 {
                ts: None,
                value: None,
            }),
            _ => Err(BackendError::Other(format!(
                "window: LAST on unsupported column type {input_type:?}"
            ))),
        },
        AggKind::CountDistinct => {
            let mode = spec
                .count_distinct_mode
                .unwrap_or(CountDistinctMode::Approximate);
            let is_utf8 = matches!(input_type, Some(DataType::Utf8 | DataType::LargeUtf8));
            match (mode, is_utf8) {
                (CountDistinctMode::Approximate, false) => Ok(AccState::CountDistinctHllNumeric(
                    HyperLogLogPlus::new(HLL_PRECISION, RandomState::new())
                        .map_err(|e| BackendError::Other(format!("HLL+ init: {e:?}")))?,
                )),
                (CountDistinctMode::Approximate, true) => Ok(AccState::CountDistinctHllUtf8(
                    HyperLogLogPlus::new(HLL_PRECISION, RandomState::new())
                        .map_err(|e| BackendError::Other(format!("HLL+ init: {e:?}")))?,
                )),
                (CountDistinctMode::Exact, _) => {
                    let cap = spec.max_distinct_values_per_group.ok_or_else(|| {
                        BackendError::Other(
                            "window: count_distinct mode=exact requires \
                             max_distinct_values_per_group"
                                .into(),
                        )
                    })?;
                    if is_utf8 {
                        Ok(AccState::CountDistinctExactUtf8 {
                            set: HashSet::new(),
                            cap,
                        })
                    } else {
                        Ok(AccState::CountDistinctExactNumeric {
                            set: HashSet::new(),
                            cap,
                        })
                    }
                }
            }
        }
    }
}

/// Apply one row's contribution to an accumulator. `event_ts` is
/// only consulted by First / Last; everything else ignores it.
fn update_acc(
    state: &mut AccState,
    column: Option<&ArrayRef>,
    row: usize,
    event_ts: i64,
) -> Result<(), BackendError> {
    match state {
        AccState::CountStar(c) => {
            *c += 1;
            Ok(())
        }
        AccState::CountCol(c) => {
            if let Some(col) = column {
                if !col.is_null(row) {
                    *c += 1;
                }
            }
            Ok(())
        }
        AccState::SumI64 { sum, any } => {
            if let Some(col) = column {
                if !col.is_null(row) {
                    let v = primitive_as_i64(col, row)?;
                    *sum += v as i128;
                    *any = true;
                }
            }
            Ok(())
        }
        AccState::SumF64 { sum, any } => {
            if let Some(col) = column {
                if !col.is_null(row) {
                    let v = primitive_as_f64(col, row)?;
                    *sum += v;
                    *any = true;
                }
            }
            Ok(())
        }
        AccState::MinI64(slot) => {
            if let Some(col) = column {
                if !col.is_null(row) {
                    let v = primitive_as_i64(col, row)?;
                    *slot = Some(slot.map_or(v, |m| m.min(v)));
                }
            }
            Ok(())
        }
        AccState::MinF64(slot) => {
            if let Some(col) = column {
                if !col.is_null(row) {
                    let v = primitive_as_f64(col, row)?;
                    *slot = Some(slot.map_or(v, |m| m.min(v)));
                }
            }
            Ok(())
        }
        AccState::MaxI64(slot) => {
            if let Some(col) = column {
                if !col.is_null(row) {
                    let v = primitive_as_i64(col, row)?;
                    *slot = Some(slot.map_or(v, |m| m.max(v)));
                }
            }
            Ok(())
        }
        AccState::MaxF64(slot) => {
            if let Some(col) = column {
                if !col.is_null(row) {
                    let v = primitive_as_f64(col, row)?;
                    *slot = Some(slot.map_or(v, |m| m.max(v)));
                }
            }
            Ok(())
        }
        AccState::AvgI64 { sum, count } => {
            if let Some(col) = column {
                if !col.is_null(row) {
                    let v = primitive_as_i64(col, row)?;
                    *sum += v as i128;
                    *count += 1;
                }
            }
            Ok(())
        }
        AccState::AvgF64 { sum, count } => {
            if let Some(col) = column {
                if !col.is_null(row) {
                    let v = primitive_as_f64(col, row)?;
                    *sum += v;
                    *count += 1;
                }
            }
            Ok(())
        }
        AccState::FirstI64 { ts, value } => {
            update_first(column, row, event_ts, ts, value, primitive_as_i64)
        }
        AccState::FirstF64 { ts, value } => {
            update_first(column, row, event_ts, ts, value, primitive_as_f64)
        }
        AccState::FirstUtf8 { ts, value } => {
            update_first(column, row, event_ts, ts, value, utf8_as_string)
        }
        AccState::LastI64 { ts, value } => {
            update_last(column, row, event_ts, ts, value, primitive_as_i64)
        }
        AccState::LastF64 { ts, value } => {
            update_last(column, row, event_ts, ts, value, primitive_as_f64)
        }
        AccState::LastUtf8 { ts, value } => {
            update_last(column, row, event_ts, ts, value, utf8_as_string)
        }
        AccState::CountDistinctHllNumeric(hll) => {
            if let Some(col) = column {
                if !col.is_null(row) {
                    let bits = numeric_to_u64_bits(col, row)?;
                    hll.insert(&bits);
                }
            }
            Ok(())
        }
        AccState::CountDistinctHllUtf8(hll) => {
            if let Some(col) = column {
                if !col.is_null(row) {
                    let s = utf8_as_string(col, row)?;
                    hll.insert(&s);
                }
            }
            Ok(())
        }
        AccState::CountDistinctExactNumeric { set, cap } => {
            if let Some(col) = column {
                if !col.is_null(row) {
                    let bits = numeric_to_u64_bits(col, row)?;
                    if !set.contains(&bits) {
                        if set.len() >= *cap {
                            return Err(BackendError::Other(format!(
                                "window: count_distinct exact cap hit: \
                                 max_distinct_values_per_group={cap} reached"
                            )));
                        }
                        set.insert(bits);
                    }
                }
            }
            Ok(())
        }
        AccState::CountDistinctExactUtf8 { set, cap } => {
            if let Some(col) = column {
                if !col.is_null(row) {
                    let s = utf8_as_string(col, row)?;
                    if !set.contains(&s) {
                        if set.len() >= *cap {
                            return Err(BackendError::Other(format!(
                                "window: count_distinct exact cap hit: \
                                 max_distinct_values_per_group={cap} reached"
                            )));
                        }
                        set.insert(s);
                    }
                }
            }
            Ok(())
        }
    }
}

/// Phase 39.5a (PR 2): merge `other` into `self`. Used by
/// session-window out-of-order merging: when two sessions for the
/// same group key are joined by a late row, their accumulator
/// states are combined into one.
///
/// Returns an error if the two states are different variants —
/// programming error, never expected at runtime since both
/// originate from the same `aggregations` spec.
impl AccState {
    fn combine(&mut self, other: AccState) -> Result<(), BackendError> {
        match (self, other) {
            (AccState::CountStar(a), AccState::CountStar(b)) => {
                *a += b;
                Ok(())
            }
            (AccState::CountCol(a), AccState::CountCol(b)) => {
                *a += b;
                Ok(())
            }
            (AccState::SumI64 { sum: s, any: a }, AccState::SumI64 { sum: s2, any: a2 }) => {
                *s += s2;
                *a |= a2;
                Ok(())
            }
            (AccState::SumF64 { sum: s, any: a }, AccState::SumF64 { sum: s2, any: a2 }) => {
                *s += s2;
                *a |= a2;
                Ok(())
            }
            (AccState::MinI64(slot), AccState::MinI64(o)) => {
                *slot = match (*slot, o) {
                    (None, x) | (x, None) => x,
                    (Some(a), Some(b)) => Some(a.min(b)),
                };
                Ok(())
            }
            (AccState::MinF64(slot), AccState::MinF64(o)) => {
                *slot = match (*slot, o) {
                    (None, x) | (x, None) => x,
                    (Some(a), Some(b)) => Some(a.min(b)),
                };
                Ok(())
            }
            (AccState::MaxI64(slot), AccState::MaxI64(o)) => {
                *slot = match (*slot, o) {
                    (None, x) | (x, None) => x,
                    (Some(a), Some(b)) => Some(a.max(b)),
                };
                Ok(())
            }
            (AccState::MaxF64(slot), AccState::MaxF64(o)) => {
                *slot = match (*slot, o) {
                    (None, x) | (x, None) => x,
                    (Some(a), Some(b)) => Some(a.max(b)),
                };
                Ok(())
            }
            (AccState::AvgI64 { sum: s, count: c }, AccState::AvgI64 { sum: s2, count: c2 }) => {
                *s += s2;
                *c += c2;
                Ok(())
            }
            (AccState::AvgF64 { sum: s, count: c }, AccState::AvgF64 { sum: s2, count: c2 }) => {
                *s += s2;
                *c += c2;
                Ok(())
            }
            (
                AccState::FirstI64 { ts, value },
                AccState::FirstI64 {
                    ts: ts2,
                    value: value2,
                },
            ) => {
                combine_first(ts, value, ts2, value2);
                Ok(())
            }
            (
                AccState::FirstF64 { ts, value },
                AccState::FirstF64 {
                    ts: ts2,
                    value: value2,
                },
            ) => {
                combine_first(ts, value, ts2, value2);
                Ok(())
            }
            (
                AccState::FirstUtf8 { ts, value },
                AccState::FirstUtf8 {
                    ts: ts2,
                    value: value2,
                },
            ) => {
                combine_first(ts, value, ts2, value2);
                Ok(())
            }
            (
                AccState::LastI64 { ts, value },
                AccState::LastI64 {
                    ts: ts2,
                    value: value2,
                },
            ) => {
                combine_last(ts, value, ts2, value2);
                Ok(())
            }
            (
                AccState::LastF64 { ts, value },
                AccState::LastF64 {
                    ts: ts2,
                    value: value2,
                },
            ) => {
                combine_last(ts, value, ts2, value2);
                Ok(())
            }
            (
                AccState::LastUtf8 { ts, value },
                AccState::LastUtf8 {
                    ts: ts2,
                    value: value2,
                },
            ) => {
                combine_last(ts, value, ts2, value2);
                Ok(())
            }
            (
                AccState::CountDistinctHllNumeric(hll_a),
                AccState::CountDistinctHllNumeric(hll_b),
            ) => hll_a
                .merge(&hll_b)
                .map_err(|e| BackendError::Other(format!("HLL+ merge: {e:?}"))),
            (AccState::CountDistinctHllUtf8(hll_a), AccState::CountDistinctHllUtf8(hll_b)) => hll_a
                .merge(&hll_b)
                .map_err(|e| BackendError::Other(format!("HLL+ merge: {e:?}"))),
            (
                AccState::CountDistinctExactNumeric { set: s, cap },
                AccState::CountDistinctExactNumeric { set: s2, .. },
            ) => combine_exact_set(s, *cap, s2),
            (
                AccState::CountDistinctExactUtf8 { set: s, cap },
                AccState::CountDistinctExactUtf8 { set: s2, .. },
            ) => combine_exact_set(s, *cap, s2),
            (a, b) => Err(BackendError::Other(format!(
                "window: combine on mismatched accumulator variants: \
                 self = {a:?}, other = {b:?}"
            ))),
        }
    }
}

/// Combine two `First*` accumulators: keep the smaller-ts entry.
/// `ts = None` represents "no value yet".
fn combine_first<T>(
    ts: &mut Option<i64>,
    value: &mut Option<T>,
    other_ts: Option<i64>,
    other_value: Option<T>,
) {
    match (*ts, other_ts) {
        (_, None) => { /* keep self */ }
        (None, Some(_)) => {
            *ts = other_ts;
            *value = other_value;
        }
        (Some(a), Some(b)) if b < a => {
            *ts = other_ts;
            *value = other_value;
        }
        _ => {}
    }
}

/// Combine two `Last*` accumulators: keep the larger-ts entry.
fn combine_last<T>(
    ts: &mut Option<i64>,
    value: &mut Option<T>,
    other_ts: Option<i64>,
    other_value: Option<T>,
) {
    match (*ts, other_ts) {
        (_, None) => {}
        (None, Some(_)) => {
            *ts = other_ts;
            *value = other_value;
        }
        (Some(a), Some(b)) if b > a => {
            *ts = other_ts;
            *value = other_value;
        }
        _ => {}
    }
}

/// Combine exact-distinct sets, enforcing `cap`. Drains `other`
/// into `self`; if the union would exceed `cap`, returns the
/// same fail-loud error as the per-row insert path.
fn combine_exact_set<T: std::hash::Hash + Eq>(
    set: &mut HashSet<T>,
    cap: usize,
    other: HashSet<T>,
) -> Result<(), BackendError> {
    for v in other {
        if !set.contains(&v) {
            if set.len() >= cap {
                return Err(BackendError::Other(format!(
                    "window: count_distinct exact cap hit during merge: \
                     max_distinct_values_per_group={cap} reached"
                )));
            }
            set.insert(v);
        }
    }
    Ok(())
}

/// Convert any supported numeric Arrow value to a stable u64 bit
/// pattern. Sign-extending to i64 first preserves distinct-ness for
/// negative integers; floats use IEEE-754 `to_bits`. NaN values map
/// to the canonical NaN bit pattern via raw `to_bits` — different
/// NaN payloads count as distinct, which matches the user's
/// "two distinct NaN-encoded inputs are different" intuition (and is
/// rare enough to not worry about).
fn numeric_to_u64_bits(arr: &ArrayRef, row: usize) -> Result<u64, BackendError> {
    use arrow_array::types::*;
    Ok(match arr.data_type() {
        DataType::Int8 => arr.as_primitive::<Int8Type>().value(row) as i64 as u64,
        DataType::Int16 => arr.as_primitive::<Int16Type>().value(row) as i64 as u64,
        DataType::Int32 => arr.as_primitive::<Int32Type>().value(row) as i64 as u64,
        DataType::Int64 => arr.as_primitive::<Int64Type>().value(row) as u64,
        DataType::UInt8 => arr.as_primitive::<UInt8Type>().value(row) as u64,
        DataType::UInt16 => arr.as_primitive::<UInt16Type>().value(row) as u64,
        DataType::UInt32 => arr.as_primitive::<UInt32Type>().value(row) as u64,
        DataType::UInt64 => arr.as_primitive::<UInt64Type>().value(row),
        DataType::Float32 => (arr.as_primitive::<Float32Type>().value(row) as f64).to_bits(),
        DataType::Float64 => arr.as_primitive::<Float64Type>().value(row).to_bits(),
        other => {
            return Err(BackendError::Other(format!(
                "window: count_distinct numeric mode requires numeric column, got {other:?}"
            )));
        }
    })
}

fn update_first<T>(
    column: Option<&ArrayRef>,
    row: usize,
    event_ts: i64,
    ts_slot: &mut Option<i64>,
    value_slot: &mut Option<T>,
    extract: fn(&ArrayRef, usize) -> Result<T, BackendError>,
) -> Result<(), BackendError> {
    if let Some(col) = column {
        if col.is_null(row) {
            return Ok(());
        }
        match *ts_slot {
            None => {
                *ts_slot = Some(event_ts);
                *value_slot = Some(extract(col, row)?);
            }
            Some(prev) if event_ts < prev => {
                *ts_slot = Some(event_ts);
                *value_slot = Some(extract(col, row)?);
            }
            _ => {}
        }
    }
    Ok(())
}

fn update_last<T>(
    column: Option<&ArrayRef>,
    row: usize,
    event_ts: i64,
    ts_slot: &mut Option<i64>,
    value_slot: &mut Option<T>,
    extract: fn(&ArrayRef, usize) -> Result<T, BackendError>,
) -> Result<(), BackendError> {
    if let Some(col) = column {
        if col.is_null(row) {
            return Ok(());
        }
        match *ts_slot {
            None => {
                *ts_slot = Some(event_ts);
                *value_slot = Some(extract(col, row)?);
            }
            Some(prev) if event_ts > prev => {
                *ts_slot = Some(event_ts);
                *value_slot = Some(extract(col, row)?);
            }
            _ => {}
        }
    }
    Ok(())
}

fn primitive_as_i64(arr: &ArrayRef, row: usize) -> Result<i64, BackendError> {
    use arrow_array::types::*;
    Ok(match arr.data_type() {
        DataType::Int8 => arr.as_primitive::<Int8Type>().value(row) as i64,
        DataType::Int16 => arr.as_primitive::<Int16Type>().value(row) as i64,
        DataType::Int32 => arr.as_primitive::<Int32Type>().value(row) as i64,
        DataType::Int64 => arr.as_primitive::<Int64Type>().value(row),
        other => {
            return Err(BackendError::Other(format!(
                "window: expected integer column, got {other:?}"
            )));
        }
    })
}

fn primitive_as_f64(arr: &ArrayRef, row: usize) -> Result<f64, BackendError> {
    use arrow_array::types::*;
    Ok(match arr.data_type() {
        DataType::Float32 => arr.as_primitive::<Float32Type>().value(row) as f64,
        DataType::Float64 => arr.as_primitive::<Float64Type>().value(row),
        DataType::Int8 => arr.as_primitive::<Int8Type>().value(row) as f64,
        DataType::Int16 => arr.as_primitive::<Int16Type>().value(row) as f64,
        DataType::Int32 => arr.as_primitive::<Int32Type>().value(row) as f64,
        DataType::Int64 => arr.as_primitive::<Int64Type>().value(row) as f64,
        other => {
            return Err(BackendError::Other(format!(
                "window: expected numeric column, got {other:?}"
            )));
        }
    })
}

fn utf8_as_string(arr: &ArrayRef, row: usize) -> Result<String, BackendError> {
    Ok(match arr.data_type() {
        DataType::Utf8 => arr.as_string::<i32>().value(row).to_string(),
        DataType::LargeUtf8 => arr.as_string::<i64>().value(row).to_string(),
        other => {
            return Err(BackendError::Other(format!(
                "window: expected string column, got {other:?}"
            )));
        }
    })
}

// =====================================================================
// WindowedAggregateTransform
// =====================================================================

/// Per-window per-group accumulator set, indexed by `(window_start, group_key)`.
type StateMap = HashMap<(i64, GroupKey), Vec<AccState>>;

/// Per-window metadata held alongside the accumulator state.
///
/// `n_groups`: count of distinct keys; checked against
/// `max_groups_per_window` on every insert.
///
/// `emitted`: whether this window has produced at least one emit
/// already. With `LateDataPolicy::Drop` always coincides with
/// state-drop (one-shot emit). With `Reopen` flips true on first
/// emit and stays true through the lateness budget.
///
/// `dirty`: set whenever new rows hit a window after its first emit.
/// Drives re-emit decisions in `emit_ready`. Reset on each emit.
#[derive(Debug, Default)]
struct WindowMeta {
    n_groups: usize,
    emitted: bool,
    dirty: bool,
}

#[derive(Debug)]
struct WindowState {
    /// Tumbling/Hopping: accumulators per (window_start, group_key).
    by_group: StateMap,
    /// Tumbling/Hopping: per-window metadata.
    meta: HashMap<i64, WindowMeta>,
    /// Phase 39.5a (PR 2): Session windows. Per group_key, a list
    /// of currently-tracked sessions. Usually 1 entry; >1 only
    /// transiently under `Reopen` retention before a late row
    /// triggers a merge or the watermark drops a stale session.
    sessions: HashMap<GroupKey, Vec<SessionState>>,
    /// Phase 39.5a (PR 3): group keys whose `sessions` entry has
    /// changed since the last `take_state_commit` drain.
    /// Drives the dirty-only commit cadence — the pipeline only
    /// re-encodes + re-writes blobs for keys that actually moved.
    dirty_keys: HashSet<GroupKey>,
    /// Phase 39.5a (PR 3): group keys whose `sessions` entry has
    /// been fully removed since the last drain. Pipeline emits a
    /// delete row per key. Mutually exclusive with `dirty_keys` —
    /// `take_state_commit` resolves any overlap by treating
    /// the key as "evicted then re-created" → upsert (since the
    /// new state takes precedence).
    evicted_keys: HashSet<GroupKey>,
    /// Phase 39.5a P1.6: late rows captured under
    /// `LateDataPolicy::Dlq`. Drained by `take_dlq_rows()` after
    /// every `transform()` / `on_idle_tick()` call; the pipeline
    /// forwards them to the configured `dead_letter_topic`.
    dlq_pending: Vec<RecordBatch>,
}

impl WindowState {
    fn new() -> Self {
        Self {
            by_group: HashMap::new(),
            meta: HashMap::new(),
            sessions: HashMap::new(),
            dirty_keys: HashSet::new(),
            evicted_keys: HashSet::new(),
            dlq_pending: Vec::new(),
        }
    }
}

/// Phase 39.5a (PR 2): one open or recently-emitted session for a
/// single group key.
///
/// Lifetime:
/// 1. Created on first row for `(group_key, ~event_ts)`.
/// 2. Extended in place as rows within `gap_ms` of `last_event_ts`
///    arrive (advancing `last_event_ts`).
/// 3. May merge with a sibling session if a late row falls within
///    `gap_ms` of both (Reopen only).
/// 4. Force-emit fires when `event_ts - start_ts ≥
///    max_session_duration_ms` — the existing session is finalized
///    and a fresh one opens for the boundary-crossing row.
/// 5. Watermark-driven emit fires when `global_wm > last_event_ts +
///    gap_ms + allowed_lateness_ms`. State retained under `Reopen`
///    until that same deadline; evicted under `Drop` on emit.
#[derive(Debug)]
pub struct SessionState {
    /// Microseconds — earliest event_ts seen in this session.
    pub(crate) start_ts: i64,
    /// Microseconds — latest event_ts seen in this session. Drives
    /// the gap check + emit deadline.
    pub(crate) last_event_ts: i64,
    /// Per-aggregator accumulators, in spec order. Combined under
    /// merge via [`AccState::combine`].
    pub(crate) accs: Vec<AccState>,
    /// True after the session has produced ≥1 emit. Drives re-emit
    /// decisions under `Reopen`.
    pub(crate) emitted: bool,
    /// Set when a new row updates the session after a prior emit.
    /// Reset on each emit. Drives re-emit on next watermark advance.
    pub(crate) dirty: bool,
    /// Set when ingest detected this session would force-emit on the
    /// duration cap (a new row arrived that would push the span past
    /// `max_session_duration_ms`). The next `emit_ready` call emits
    /// and evicts this session unconditionally — even under `Reopen`,
    /// per the design doc's hard-ceiling rule.
    pub(crate) force_emit_pending: bool,
}

impl SessionState {
    fn new(event_ts: i64, accs: Vec<AccState>) -> Self {
        Self {
            start_ts: event_ts,
            last_event_ts: event_ts,
            accs,
            emitted: false,
            dirty: false,
            force_emit_pending: false,
        }
    }

    /// Phase 39.5a PR 3: rebuild a `SessionState` from a recovered
    /// blob. `force_emit_pending` is always reset to `false` —
    /// force-emit is a transient ingest-time signal that doesn't
    /// belong on disk.
    pub fn from_blob(
        start_ts: i64,
        last_event_ts: i64,
        emitted: bool,
        dirty: bool,
        accs: Vec<AccState>,
    ) -> Self {
        Self {
            start_ts,
            last_event_ts,
            accs,
            emitted,
            dirty,
            force_emit_pending: false,
        }
    }

    /// Watermark threshold at which this session is ready to emit.
    /// Under `Drop`, lateness=0 and this collapses to `last+gap`.
    fn emit_threshold(&self, gap_us: i64) -> i64 {
        self.last_event_ts + gap_us
    }

    /// Watermark threshold at which retained session state is freed.
    /// Under `Drop`, identical to `emit_threshold` (state evicts on
    /// emit). Under `Reopen`, deferred by `lateness_us` — late rows
    /// arriving before this deadline merge into the session and
    /// trigger a re-emit.
    fn drop_threshold(&self, gap_us: i64, lateness_us: i64) -> i64 {
        self.last_event_ts + gap_us + lateness_us
    }
}

/// Phase 39.4: stateful tumbling/hopping windowed aggregate.
pub struct WindowedAggregateTransform {
    config: WindowConfig,
    inner: Option<Arc<LazySqlTransform>>,
    /// Output schema, computed at construction. Pre-computing it
    /// avoids per-batch allocation and lets the pipeline validate
    /// against the target before the first batch arrives.
    output_schema: SchemaRef,
    /// Cached input schema — captured on first batch (after any
    /// inner SQL transform), used to resolve column indices.
    input_schema: tokio::sync::OnceCell<SchemaRef>,
    state: Mutex<WindowState>,
    /// Optional Prometheus counters. `None` for free-standing tests
    /// that don't wire a registry.
    metrics: Option<WindowedMetrics>,
}

impl std::fmt::Debug for WindowedAggregateTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WindowedAggregateTransform")
            .field("config", &self.config)
            .field("has_inner", &self.inner.is_some())
            .finish()
    }
}

/// Prometheus counters specific to a windowed pipeline. Construct
/// via [`WindowedMetrics::new`] which registers the metrics into the
/// pipeline's existing Registry, then attach to the windowed
/// transform via [`WindowedAggregateTransform::with_metrics`].
#[derive(Debug, Clone)]
pub struct WindowedMetrics {
    /// Total windows emitted, incremented per emit.
    pub windows_emitted_total: IntCounter,
    /// Late rows dropped (or routed to DLQ / re-aggregated, in
    /// future). Labeled by `policy = "drop" | "dlq" | "reopen"`.
    pub late_rows_dropped_total: IntCounterVec,
    /// Currently active windows (open + waiting for watermark).
    pub windows_active: IntGauge,
    /// Sum of distinct group keys across all active windows.
    pub state_groups_total: IntGauge,
}

impl WindowedMetrics {
    /// Register windowed-pipeline metrics into the supplied
    /// Prometheus registry. The pipeline's
    /// [`crate::streaming::StreamingPipelineMetricsCounters::registry`]
    /// is the right thing to pass in production; tests can pass a
    /// fresh `Registry::new()`.
    pub fn new(registry: &Registry, pipeline_name: &str) -> Result<Self, BackendError> {
        let mk_counter = |name: &str, help: &str| -> Result<IntCounter, BackendError> {
            let c =
                IntCounter::with_opts(Opts::new(name, help).const_label("pipeline", pipeline_name))
                    .map_err(|e| BackendError::Other(format!("metrics: {e}")))?;
            registry
                .register(Box::new(c.clone()))
                .map_err(|e| BackendError::Other(format!("metrics register: {e}")))?;
            Ok(c)
        };
        let mk_gauge = |name: &str, help: &str| -> Result<IntGauge, BackendError> {
            let g =
                IntGauge::with_opts(Opts::new(name, help).const_label("pipeline", pipeline_name))
                    .map_err(|e| BackendError::Other(format!("metrics: {e}")))?;
            registry
                .register(Box::new(g.clone()))
                .map_err(|e| BackendError::Other(format!("metrics register: {e}")))?;
            Ok(g)
        };

        let late_rows = IntCounterVec::new(
            Opts::new(
                "ematix_streaming_late_rows_dropped_total",
                "Late rows discarded by the windowed transform, labeled by policy.",
            )
            .const_label("pipeline", pipeline_name),
            &["policy"],
        )
        .map_err(|e| BackendError::Other(format!("metrics: {e}")))?;
        registry
            .register(Box::new(late_rows.clone()))
            .map_err(|e| BackendError::Other(format!("metrics register: {e}")))?;

        Ok(Self {
            windows_emitted_total: mk_counter(
                "ematix_streaming_windows_emitted_total",
                "Total windows emitted by the windowed transform.",
            )?,
            late_rows_dropped_total: late_rows,
            windows_active: mk_gauge(
                "ematix_streaming_windows_active",
                "Number of windows currently open in the windowed transform's state.",
            )?,
            state_groups_total: mk_gauge(
                "ematix_streaming_state_groups_total",
                "Sum of distinct group keys across all active windows.",
            )?,
        })
    }
}

impl WindowedAggregateTransform {
    /// Construct a windowed transform, validating the configuration
    /// at the same time. Returns config-load-time errors as
    /// `BackendError::Other`.
    pub fn new(
        mut config: WindowConfig,
        inner: Option<Arc<LazySqlTransform>>,
    ) -> Result<Self, BackendError> {
        config.validate()?;
        // Output schema is computed lazily at construction time
        // using only the parts of input we know upfront — group_by
        // columns and aggregations. The actual input column types
        // are resolved on the first batch (inner SQL may transform
        // them). Until then, treat numeric agg outputs as their
        // declared output type assuming common cases (Int64 / Float64).
        // PR 2 punts schema-from-types-of-actual-columns to first
        // batch — we build a placeholder schema here that gets
        // replaced once we see real data.
        //
        // To keep things simple in PR 2: fail fast if the user hasn't
        // declared enough info — but for windowed transforms we can
        // assume Int64 columns by default. For full correctness the
        // pipeline validates the output schema against the target
        // after the first emit.
        let output_schema = build_placeholder_output_schema(&config)?;
        Ok(Self {
            config,
            inner,
            output_schema,
            input_schema: tokio::sync::OnceCell::new(),
            state: Mutex::new(WindowState::new()),
            metrics: None,
        })
    }

    /// Attach Prometheus counters. Optional — pipelines without a
    /// registry can run without metrics.
    pub fn with_metrics(mut self, metrics: WindowedMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Phase 39.5a PR 3: drain the set of group keys whose session
    /// state has changed (or been evicted) since the last call, and
    /// return per-key encoded blobs ready to hand to
    /// [`crate::state_store::StateStore::commit`].
    ///
    /// Returns `(upserts, deletes)` where:
    /// - `upserts` is `Vec<(encoded_group_key, postcard_session_blob)>`.
    /// - `deletes` is `Vec<encoded_group_key>` — keys whose entire
    ///   session list was evicted since the last drain.
    ///
    /// Calling on a non-Session config returns empty vectors —
    /// tumbling/hopping pipelines don't participate in session
    /// state persistence (they rebuild from source replay on
    /// restart, same as 39.4).
    ///
    /// Failure surface: encoding a `count_distinct` aggregator
    /// returns an error. Pipelines that mix `count_distinct` with
    /// `[state_store]` should be rejected at config-load (PR 3
    /// CLI cross-validation) — this is the defense in depth.
    pub async fn take_state_commit(
        &self,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, Vec<Vec<u8>>), BackendError> {
        if !matches!(self.config.kind, WindowKind::Session) {
            return Ok((Vec::new(), Vec::new()));
        }
        let mut state = self.state.lock().await;
        let dirty: Vec<GroupKey> = state.dirty_keys.drain().collect();
        let evicted: Vec<GroupKey> = state.evicted_keys.drain().collect();

        let mut upserts: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(dirty.len());
        for key in dirty {
            let sessions: &[SessionState] = state
                .sessions
                .get(&key)
                .map(|v| v.as_slice())
                .unwrap_or(&[]);
            // If a key was marked dirty but somehow has no sessions
            // by drain time (shouldn't normally happen — eviction
            // moves it to evicted_keys), emit a delete instead.
            if sessions.is_empty() {
                upserts.push((crate::session_blob::encode_group_key(&key)?, Vec::new()));
                continue;
            }
            let blob = crate::session_blob::encode_sessions(sessions)?;
            let key_bytes = crate::session_blob::encode_group_key(&key)?;
            upserts.push((key_bytes, blob));
        }

        let mut deletes: Vec<Vec<u8>> = Vec::with_capacity(evicted.len());
        for key in evicted {
            deletes.push(crate::session_blob::encode_group_key(&key)?);
        }

        Ok((upserts, deletes))
    }

    /// Phase 39.5a PR 3: install recovered session state for the
    /// configured group keys. Called once at pipeline startup with
    /// the `state_by_key` map from
    /// [`crate::state_store::StateStore::load`].
    ///
    /// No-op for non-Session configs. Sessions present in the
    /// recovered map are inserted as-is; the in-memory map starts
    /// empty so callers don't need to worry about merging with
    /// pre-existing state.
    ///
    /// `force_emit_pending` is reset to `false` in the recovered
    /// state — the cap-crossing condition that set it must be
    /// re-detected from the next batch (and the pre-cap session
    /// already emitted before the original commit anyway).
    pub async fn recover_state(
        &self,
        state_by_key: &std::collections::HashMap<Vec<u8>, Vec<u8>>,
    ) -> Result<(), BackendError> {
        if !matches!(self.config.kind, WindowKind::Session) {
            return Ok(());
        }
        let mut state = self.state.lock().await;
        for (key_bytes, blob) in state_by_key {
            let key = crate::session_blob::decode_group_key(key_bytes)?;
            let sessions = crate::session_blob::decode_sessions(blob)?;
            if !sessions.is_empty() {
                state.sessions.insert(key, sessions);
            }
        }
        // Recovery wipes any stale dirty markers — the in-memory
        // state now matches the store, by definition.
        state.dirty_keys.clear();
        state.evicted_keys.clear();
        Ok(())
    }

    fn group_by_indices(&self, schema: &Schema) -> Result<Vec<usize>, BackendError> {
        self.config
            .group_by
            .iter()
            .map(|c| {
                schema.index_of(c.as_str()).map_err(|_| {
                    BackendError::Other(format!(
                        "window: group_by column `{c}` not found in input schema"
                    ))
                })
            })
            .collect()
    }

    fn agg_indices(&self, schema: &Schema) -> Result<Vec<Option<usize>>, BackendError> {
        self.config
            .aggregations
            .iter()
            .map(|a| match &a.column {
                None => Ok(None),
                Some(c) => Ok(Some(schema.index_of(c.as_str()).map_err(|_| {
                    BackendError::Other(format!(
                        "window: aggregation column `{c}` not found in input schema"
                    ))
                })?)),
            })
            .collect()
    }

    fn event_ts_index(&self, schema: &Schema) -> Result<usize, BackendError> {
        schema
            .index_of(self.config.event_time_column.as_str())
            .map_err(|_| {
                BackendError::Other(format!(
                    "window: event_time_column `{}` not found in input schema",
                    self.config.event_time_column
                ))
            })
    }

    /// Compute the list of `window_start` values a row at `event_ts`
    /// belongs to. For tumbling, exactly one. For hopping, up to
    /// `ceil(duration_ms / hop_ms)` values.
    fn window_starts_for(&self, event_ts_micros: i64) -> Vec<i64> {
        let duration_us = (self.config.duration_ms as i64).saturating_mul(1_000);
        let hop_us = (self.config.hop_ms as i64).saturating_mul(1_000);
        // First window containing event_ts: largest hop multiple <= event_ts.
        let mut s = (event_ts_micros / hop_us) * hop_us;
        // Step backwards while s + duration_us > event_ts (and s + duration_us
        // remains a valid window-end > event_ts).
        let mut out: Vec<i64> = Vec::new();
        while s + duration_us > event_ts_micros && s <= event_ts_micros {
            out.push(s);
            if s == i64::MIN {
                break;
            }
            s -= hop_us;
        }
        out.reverse();
        out
    }

    /// Ingest one batch into per-window state. `ctx_wm` is the
    /// pipeline's current watermark; used to classify each row's
    /// arrival relative to its window's emit boundary (see
    /// `LateDataPolicy`).
    async fn ingest(&self, batch: RecordBatch, ctx_wm: Option<i64>) -> Result<(), BackendError> {
        // Capture input schema on first batch.
        let schema = self
            .input_schema
            .get_or_init(|| async { batch.schema() })
            .await
            .clone();

        let group_idx = self.group_by_indices(&schema)?;
        let agg_idx = self.agg_indices(&schema)?;
        let ts_idx = self.event_ts_index(&schema)?;

        // Validate event_ts column is Timestamp(Microsecond, _).
        let ts_array = batch.column(ts_idx);
        if !matches!(
            ts_array.data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, _)
        ) {
            return Err(BackendError::Other(format!(
                "window: event_time_column `{}` must be Timestamp(Microsecond, _), got {:?}",
                self.config.event_time_column,
                ts_array.data_type()
            )));
        }
        let ts_array = ts_array.as_primitive::<TimestampMicrosecondType>();

        // Pre-collect group_by + agg column references.
        let group_arrays: Vec<ArrayRef> =
            group_idx.iter().map(|&i| batch.column(i).clone()).collect();
        let agg_arrays: Vec<Option<ArrayRef>> = agg_idx
            .iter()
            .map(|i| i.map(|i| batch.column(i).clone()))
            .collect();

        // Phase 39.5a (PR 2): session windows take a dedicated path.
        if matches!(self.config.kind, WindowKind::Session) {
            // Clone the timestamp column so the session path doesn't
            // borrow from `batch` (the rest of the path uses owned
            // ArrayRef clones too).
            let ts_owned: TimestampMicrosecondArray = ts_array.clone();
            return self
                .ingest_session(&batch, ctx_wm, &group_arrays, &agg_arrays, &ts_owned)
                .await;
        }

        let mut state = self.state.lock().await;
        let max_groups = self.config.max_groups_per_window;
        let lateness_us = self.config.late_data.lateness_micros();
        let policy_label = self.config.late_data.label();
        // Pipeline's current watermark — needed to decide whether
        // an arriving row is "late" (event_ts within an already-emitted
        // window). `None` means we haven't yet seen any non-idle source
        // batch; treat as "everything is on time."
        let global_wm = ctx_wm;

        for row in 0..batch.num_rows() {
            if ts_array.is_null(row) {
                // NULL event_ts: drop the row (can't window it).
                continue;
            }
            let event_ts = ts_array.value(row);
            let starts = self.window_starts_for(event_ts);
            for window_start in starts {
                let duration_us = (self.config.duration_ms as i64).saturating_mul(1_000);
                let window_end = window_start + duration_us;
                // Late-data classification:
                //   - "open"           : window_end > global_wm (still receiving on-time data)
                //   - "in lateness budget" : window_end <= global_wm < window_end + lateness_us
                //   - "past budget"    : window_end + lateness_us <= global_wm
                if let Some(wm) = global_wm {
                    if wm >= window_end + lateness_us {
                        // Past budget: drop, or stash for DLQ. The
                        // counter is incremented either way —
                        // operators see late-row volume regardless
                        // of whether the row was discarded.
                        if let Some(m) = &self.metrics {
                            m.late_rows_dropped_total
                                .with_label_values(&[policy_label])
                                .inc();
                        }
                        if matches!(self.config.late_data, LateDataPolicy::Dlq) {
                            // Slice the input to just this row and
                            // append to the per-transform DLQ
                            // buffer. The pipeline drains it after
                            // this `transform()` call returns.
                            state.dlq_pending.push(batch.slice(row, 1));
                        }
                        continue;
                    }
                    if wm >= window_end {
                        // Within lateness budget. With Drop policy this
                        // is identical to past-budget (lateness_us=0);
                        // with Reopen, the row re-aggregates and we'll
                        // re-emit on next watermark advance.
                        match self.config.late_data {
                            LateDataPolicy::Drop => {
                                if let Some(m) = &self.metrics {
                                    m.late_rows_dropped_total.with_label_values(&["drop"]).inc();
                                }
                                continue;
                            }
                            LateDataPolicy::Reopen { .. } => {
                                // Fall through to ingest; mark dirty after.
                            }
                            LateDataPolicy::Dlq => {
                                // Unreachable: Dlq's lateness_us = 0
                                // collapses past-budget and
                                // within-budget into the same
                                // boundary, so the past-budget arm
                                // above already handled it.
                                unreachable!("Dlq policy — past-budget arm should have fired");
                            }
                        }
                    }
                }

                let group_key = GroupKey::from_row(&group_arrays, row)?;
                let key = (window_start, group_key);

                // Cap check + insert.
                if !state.by_group.contains_key(&key) {
                    let meta = state.meta.entry(window_start).or_default();
                    if meta.n_groups >= max_groups {
                        return Err(BackendError::Other(format!(
                            "window state cap hit: max_groups_per_window={} reached on window [{}us, {}us)",
                            max_groups, window_start, window_end
                        )));
                    }
                    meta.n_groups += 1;
                    let accs: Result<Vec<AccState>, BackendError> = self
                        .config
                        .aggregations
                        .iter()
                        .zip(agg_arrays.iter())
                        .map(|(spec, arr)| new_acc_state(spec, arr.as_ref().map(|a| a.data_type())))
                        .collect();
                    state.by_group.insert(key.clone(), accs?);
                }

                // Update each accumulator.
                let accs = state
                    .by_group
                    .get_mut(&key)
                    .expect("just inserted or pre-existing");
                for (acc, arr) in accs.iter_mut().zip(agg_arrays.iter()) {
                    update_acc(acc, arr.as_ref(), row, event_ts)?;
                }

                // Mark the window dirty if it has already emitted at
                // least once — drives re-emit on the next emit_ready
                // call (only meaningful with Reopen policy).
                let meta = state
                    .meta
                    .get_mut(&window_start)
                    .expect("meta entry just created or pre-existing");
                if meta.emitted {
                    meta.dirty = true;
                }
            }
        }
        // Refresh gauges from current state.
        if let Some(m) = &self.metrics {
            m.windows_active.set(state.meta.len() as i64);
            let total_groups: usize = state.meta.values().map(|w| w.n_groups).sum();
            m.state_groups_total.set(total_groups as i64);
        }
        Ok(())
    }

    /// Phase 39.5a (PR 2): session-window ingest path.
    ///
    /// Per row:
    /// 1. **Late-data check.** Drop the row if
    ///    `wm > event_ts + gap_us + lateness_us` — no session that
    ///    would contain this row can still be open.
    /// 2. **Find candidate sessions for `group_key`.** A session is
    ///    a candidate iff `event_ts` is within `gap_us` of either
    ///    its start or its last_event_ts (or inside the session's
    ///    span).
    /// 3. **Cap-test each candidate.** If absorbing the row would
    ///    push the merged span past `max_session_duration_us`, mark
    ///    that candidate `force_emit_pending` and exclude it — the
    ///    cap is a hard ceiling regardless of policy.
    /// 4. **Apply.**
    ///    - 0 candidates: open a new session for the row.
    ///    - 1 candidate: extend in place; mark `dirty` if already
    ///      emitted.
    ///    - 2+ candidates (Reopen only): merge them into the lowest
    ///      index, absorb the row, mark `dirty` if any participant
    ///      had emitted.
    ///
    /// `max_groups_per_window` caps the number of distinct group
    /// keys with at least one open session — checked before opening
    /// a session for a previously-unseen key.
    async fn ingest_session(
        &self,
        batch: &RecordBatch,
        ctx_wm: Option<i64>,
        group_arrays: &[ArrayRef],
        agg_arrays: &[Option<ArrayRef>],
        ts_array: &TimestampMicrosecondArray,
    ) -> Result<(), BackendError> {
        let n_rows = batch.num_rows();
        let gap_us = (self
            .config
            .gap_ms
            .expect("validate() ensures Some for Session")) as i64
            * 1_000;
        let max_dur_us = (self
            .config
            .max_session_duration_ms
            .expect("validate() ensures Some for Session")) as i64
            * 1_000;
        let lateness_us = self.config.late_data.lateness_micros();
        let policy_label = self.config.late_data.label();
        let max_groups = self.config.max_groups_per_window;

        let mut state = self.state.lock().await;

        for row in 0..n_rows {
            if ts_array.is_null(row) {
                continue;
            }
            let event_ts = ts_array.value(row);
            let group_key = GroupKey::from_row(group_arrays, row)?;

            // Late-data check: a row past `event_ts + gap + lateness`
            // can never form an open session because every candidate
            // session containing it has already exceeded its drop
            // threshold.
            if let Some(wm) = ctx_wm {
                if wm > event_ts + gap_us + lateness_us {
                    if let Some(m) = &self.metrics {
                        m.late_rows_dropped_total
                            .with_label_values(&[policy_label])
                            .inc();
                    }
                    if matches!(self.config.late_data, LateDataPolicy::Dlq) {
                        // Slice the input to just this row and
                        // append to the per-transform DLQ buffer.
                        // The pipeline drains via `take_dlq_rows()`
                        // after this `transform()` call returns.
                        state.dlq_pending.push(batch.slice(row, 1));
                    }
                    continue;
                }
            }

            // Cap on distinct group keys. Only triggers when this is
            // a new key with no existing sessions.
            let new_key = !state
                .sessions
                .get(&group_key)
                .map(|v| !v.is_empty())
                .unwrap_or(false);
            if new_key && state.sessions.len() >= max_groups {
                // The key isn't yet in the map (either absent or
                // present-but-empty); inserting it would push
                // distinct-key count past the cap.
                if !state.sessions.contains_key(&group_key) {
                    return Err(BackendError::Other(format!(
                        "window state cap hit: max_groups_per_window={max_groups} reached \
                         (sessions: distinct group keys)"
                    )));
                }
            }

            let sessions = state.sessions.entry(group_key.clone()).or_default();

            // Find candidate sessions for this row.
            let mut matching: Vec<usize> = Vec::new();
            for (idx, s) in sessions.iter_mut().enumerate() {
                if s.force_emit_pending {
                    // Already pending force-emit — don't grow it.
                    continue;
                }
                let gap_ok =
                    event_ts >= s.start_ts - gap_us && event_ts <= s.last_event_ts + gap_us;
                if !gap_ok {
                    continue;
                }
                // Cap test on the prospective merged span.
                let new_start = s.start_ts.min(event_ts);
                let new_last = s.last_event_ts.max(event_ts);
                if new_last - new_start > max_dur_us {
                    s.force_emit_pending = true;
                    continue;
                }
                matching.push(idx);
            }

            // Apply.
            match matching.len() {
                0 => {
                    let accs: Result<Vec<AccState>, BackendError> = self
                        .config
                        .aggregations
                        .iter()
                        .zip(agg_arrays.iter())
                        .map(|(spec, arr)| new_acc_state(spec, arr.as_ref().map(|a| a.data_type())))
                        .collect();
                    let mut new_session = SessionState::new(event_ts, accs?);
                    for (acc, arr) in new_session.accs.iter_mut().zip(agg_arrays.iter()) {
                        update_acc(acc, arr.as_ref(), row, event_ts)?;
                    }
                    sessions.push(new_session);
                }
                1 => {
                    let idx = matching[0];
                    let s = &mut sessions[idx];
                    for (acc, arr) in s.accs.iter_mut().zip(agg_arrays.iter()) {
                        update_acc(acc, arr.as_ref(), row, event_ts)?;
                    }
                    if event_ts < s.start_ts {
                        s.start_ts = event_ts;
                    }
                    if event_ts > s.last_event_ts {
                        s.last_event_ts = event_ts;
                    }
                    if s.emitted {
                        s.dirty = true;
                    }
                }
                _ => {
                    // Multiple matches — merge under Reopen. Absorb
                    // every matching session into the lowest-indexed
                    // one (sort first so we drain in reverse safely).
                    let mut sorted = matching.clone();
                    sorted.sort();
                    let base_idx = sorted[0];
                    // Drain the others in reverse-index order so
                    // earlier indices stay valid.
                    let mut absorbed: Vec<SessionState> = Vec::new();
                    for &idx in sorted.iter().skip(1).rev() {
                        absorbed.push(sessions.remove(idx));
                    }
                    let base = &mut sessions[base_idx];
                    for other in absorbed {
                        base.start_ts = base.start_ts.min(other.start_ts);
                        base.last_event_ts = base.last_event_ts.max(other.last_event_ts);
                        for (a, b) in base.accs.iter_mut().zip(other.accs) {
                            a.combine(b)?;
                        }
                        if other.emitted {
                            base.emitted = true;
                            base.dirty = true;
                        }
                        if other.dirty {
                            base.dirty = true;
                        }
                    }
                    // Now absorb the row.
                    for (acc, arr) in base.accs.iter_mut().zip(agg_arrays.iter()) {
                        update_acc(acc, arr.as_ref(), row, event_ts)?;
                    }
                    if event_ts < base.start_ts {
                        base.start_ts = event_ts;
                    }
                    if event_ts > base.last_event_ts {
                        base.last_event_ts = event_ts;
                    }
                    if base.emitted {
                        base.dirty = true;
                    }
                }
            }

            // PR 3: every per-row mutation marks the key dirty for
            // the next state-store commit. The pre-existing
            // entry-or-default above guarantees the key is in the
            // sessions map, so dirty + evicted are mutually
            // consistent (we don't ingest into an evicted key
            // without re-creating it).
            state.evicted_keys.remove(&group_key);
            state.dirty_keys.insert(group_key);
        }

        // Refresh gauges. For sessions, `windows_active` = open
        // sessions across all keys; `state_groups_total` = distinct
        // active group keys.
        if let Some(m) = &self.metrics {
            let total_sessions: usize = state.sessions.values().map(|v| v.len()).sum();
            m.windows_active.set(total_sessions as i64);
            m.state_groups_total.set(state.sessions.len() as i64);
        }
        Ok(())
    }

    /// Emit windows whose `window_end ≤ global_wm` and drop those
    /// past the lateness budget.
    ///
    /// Two phases:
    /// 1. **Emit**: any window with `window_end ≤ global_wm` whose
    ///    state is fresh (not yet emitted) or dirty (re-aggregated
    ///    after first emit) produces an output batch. With
    ///    `LateDataPolicy::Drop`, every ready window is fresh; with
    ///    `Reopen`, dirty windows re-emit until the lateness budget
    ///    expires.
    /// 2. **Drop**: any window with
    ///    `window_end + lateness_us ≤ global_wm` AND already emitted
    ///    has its state freed. With Drop, lateness_us=0, so emit and
    ///    drop happen together. With Reopen, drop is delayed by the
    ///    budget.
    ///
    /// Critically: emit reads accumulator state via `&mut` without
    /// removing it, so Reopen pipelines retain state across multiple
    /// re-emits. State is dropped only in phase 2.
    async fn emit_ready(&self, global_wm: i64) -> Result<Vec<RecordBatch>, BackendError> {
        // Phase 39.5a (PR 2): session-window emit takes a dedicated path.
        if matches!(self.config.kind, WindowKind::Session) {
            return self.emit_ready_session(global_wm).await;
        }
        let duration_us = (self.config.duration_ms as i64).saturating_mul(1_000);
        let lateness_us = self.config.late_data.lateness_micros();
        let mut state = self.state.lock().await;

        // Phase 1: gather windows ready to emit (fresh or dirty).
        let to_emit: Vec<i64> = state
            .meta
            .iter()
            .filter_map(|(s, m)| {
                let window_end = s + duration_us;
                if window_end > global_wm {
                    return None;
                }
                if !m.emitted || m.dirty {
                    Some(*s)
                } else {
                    None
                }
            })
            .collect();

        let input_schema = self.input_schema.get().cloned();
        let mut out: Vec<RecordBatch> = Vec::with_capacity(to_emit.len());
        for window_start in &to_emit {
            let window_start = *window_start;
            let window_end = window_start + duration_us;

            // Read out the keys so we can iterate via get_mut without
            // borrowing the HashMap immutably.
            let keys_for_window: Vec<GroupKey> = state
                .by_group
                .keys()
                .filter(|(s, _)| *s == window_start)
                .map(|(_, k)| k.clone())
                .collect();

            // Finalize each (group, accs) into Owned values without
            // moving state out of `by_group`. AccState (HLL+) isn't
            // Clone, so we walk via `get_mut` and call HLL+'s
            // `count()` which takes `&mut self`.
            let mut finalized: Vec<(GroupKey, Vec<FinalizedValue>)> =
                Vec::with_capacity(keys_for_window.len());
            for key in &keys_for_window {
                let accs = state
                    .by_group
                    .get_mut(&(window_start, key.clone()))
                    .expect("present");
                let mut row_finalized: Vec<FinalizedValue> = Vec::with_capacity(accs.len());
                for (acc, spec) in accs.iter_mut().zip(self.config.aggregations.iter()) {
                    row_finalized.push(finalize_acc(acc, spec));
                }
                finalized.push((key.clone(), row_finalized));
            }

            // Build the emit batch from the finalized snapshots.
            let batch = build_emit_batch(
                &self.config,
                input_schema.as_ref(),
                window_start,
                window_end,
                finalized,
            )?;
            out.push(batch);

            // Mark emitted + reset dirty.
            let meta = state
                .meta
                .get_mut(&window_start)
                .expect("meta entry exists for emitted window");
            meta.emitted = true;
            meta.dirty = false;

            if let Some(m) = &self.metrics {
                m.windows_emitted_total.inc();
            }
        }

        // Phase 2: drop state for windows past the lateness budget.
        // Both Drop and Reopen go through this — for Drop,
        // lateness_us=0, so dropping happens immediately after the
        // first emit (same iteration). For Reopen, drop is delayed.
        let to_drop: Vec<i64> = state
            .meta
            .iter()
            .filter(|(s, m)| {
                let close_at = *s + duration_us + lateness_us;
                m.emitted && close_at <= global_wm
            })
            .map(|(s, _)| *s)
            .collect();
        for window_start in to_drop {
            let keys: Vec<(i64, GroupKey)> = state
                .by_group
                .keys()
                .filter(|(s, _)| *s == window_start)
                .cloned()
                .collect();
            for k in keys {
                state.by_group.remove(&k);
            }
            state.meta.remove(&window_start);
        }

        if let Some(m) = &self.metrics {
            m.windows_active.set(state.meta.len() as i64);
            let total_groups: usize = state.meta.values().map(|w| w.n_groups).sum();
            m.state_groups_total.set(total_groups as i64);
        }
        Ok(out)
    }

    /// Phase 39.5a (PR 2): emit + retain/evict pass for session
    /// windows.
    ///
    /// For each session:
    /// - **Emit** (record an output row) when:
    ///   - `force_emit_pending = true` (duration cap was hit during
    ///     ingest); OR
    ///   - `wm > emit_threshold` (= `last_event_ts + gap_us`) and
    ///     either the session hasn't emitted yet, or has been
    ///     dirtied by a late merge (Reopen only).
    /// - **Evict** (remove from state) when:
    ///   - `force_emit_pending` (force-emit always evicts, even
    ///     under Reopen — the cap is a hard ceiling); OR
    ///   - `wm > drop_threshold` (= `last + gap + lateness`).
    ///
    /// Under `Drop` policy, `lateness_us = 0` collapses
    /// `emit_threshold == drop_threshold`, so Drop-mode sessions
    /// always emit-and-evict in the same pass. Under `Reopen` the
    /// two thresholds are distinct, so a session can emit, retain
    /// state through the lateness budget, absorb late rows, then
    /// re-emit before final eviction.
    ///
    /// Multiple sessions ready in the same call coalesce into one
    /// output `RecordBatch` with one row per session.
    async fn emit_ready_session(&self, global_wm: i64) -> Result<Vec<RecordBatch>, BackendError> {
        let gap_us = (self
            .config
            .gap_ms
            .expect("validate() ensures Some for Session")) as i64
            * 1_000;
        let lateness_us = self.config.late_data.lateness_micros();
        let mut state = self.state.lock().await;

        // Walk every key; partition each session into emit / evict.
        // Emits collected in a single Vec for batch construction.
        let mut emits: Vec<SessionEmit> = Vec::new();
        // PR 3: collect dirty/evicted updates locally to avoid
        // double-borrow of `state` (`state.sessions` vs.
        // `state.dirty_keys` / `state.evicted_keys`); apply at the
        // end of the function.
        let mut newly_dirty: Vec<GroupKey> = Vec::new();
        let keys: Vec<GroupKey> = state.sessions.keys().cloned().collect();
        for key in keys {
            let sessions = match state.sessions.get_mut(&key) {
                Some(v) => v,
                None => continue,
            };
            let mut idx = 0;
            let mut key_changed = false;
            while idx < sessions.len() {
                // Compute thresholds + decisions for the current
                // session, then act.
                let s = &sessions[idx];
                let force_emit = s.force_emit_pending;
                let emit_threshold = s.emit_threshold(gap_us);
                let drop_threshold = s.drop_threshold(gap_us, lateness_us);
                let should_emit =
                    force_emit || (global_wm > emit_threshold && (!s.emitted || s.dirty));
                let should_evict = force_emit || global_wm > drop_threshold;

                if should_emit {
                    // Snapshot accumulators in place — finalize_acc
                    // takes `&mut` (HLL+ caches its count) but doesn't
                    // consume.
                    let s_mut = &mut sessions[idx];
                    let mut row_finalized: Vec<FinalizedValue> =
                        Vec::with_capacity(s_mut.accs.len());
                    for (acc, spec) in s_mut.accs.iter_mut().zip(self.config.aggregations.iter()) {
                        row_finalized.push(finalize_acc(acc, spec));
                    }
                    emits.push(SessionEmit {
                        group_key: key.clone(),
                        start_ts: s_mut.start_ts,
                        last_event_ts: s_mut.last_event_ts,
                        finalized: row_finalized,
                    });
                    s_mut.emitted = true;
                    s_mut.dirty = false;
                    key_changed = true;
                    if let Some(m) = &self.metrics {
                        m.windows_emitted_total.inc();
                    }
                }

                if should_evict {
                    sessions.remove(idx);
                    key_changed = true;
                    // Don't advance idx — vector shifted left.
                } else {
                    idx += 1;
                }
            }
            if key_changed {
                newly_dirty.push(key);
            }
        }

        // Drop empty per-key Vecs so distinct-key counts stay
        // accurate. Track which keys went fully empty so the next
        // commit emits a delete row for them.
        let mut newly_evicted: Vec<GroupKey> = Vec::new();
        state.sessions.retain(|k, v| {
            if v.is_empty() {
                newly_evicted.push(k.clone());
                false
            } else {
                true
            }
        });
        for k in newly_dirty {
            // Skip if the key is fully evicted — the eviction row
            // wins (delete) over an upsert.
            if !newly_evicted.contains(&k) {
                state.dirty_keys.insert(k);
            }
        }
        for k in newly_evicted {
            state.dirty_keys.remove(&k);
            state.evicted_keys.insert(k);
        }

        let input_schema = self.input_schema.get().cloned();
        let out: Vec<RecordBatch> = if emits.is_empty() {
            Vec::new()
        } else {
            vec![build_session_emit_batch(
                &self.config,
                input_schema.as_ref(),
                gap_us,
                emits,
            )?]
        };

        if let Some(m) = &self.metrics {
            let total_sessions: usize = state.sessions.values().map(|v| v.len()).sum();
            m.windows_active.set(total_sessions as i64);
            m.state_groups_total.set(state.sessions.len() as i64);
        }
        Ok(out)
    }
}

/// One emitted-session row, captured during `emit_ready_session`
/// before batch construction.
struct SessionEmit {
    group_key: GroupKey,
    start_ts: i64,
    last_event_ts: i64,
    finalized: Vec<FinalizedValue>,
}

/// Finalized output value of one accumulator. Used during emit so
/// `build_emit_batch` doesn't need to consume `AccState` (which
/// matters for `LateDataPolicy::Reopen` — state must survive emit).
#[derive(Debug, Clone)]
enum FinalizedValue {
    Int64(Option<i64>),
    Float64(Option<f64>),
    UInt64(Option<u64>),
    Utf8(Option<String>),
}

/// Snapshot one accumulator's value for emit. Mutates the
/// accumulator (HLL+'s `count()` takes `&mut self` for cache update)
/// but doesn't consume it.
fn finalize_acc(acc: &mut AccState, _spec: &AggregationSpec) -> FinalizedValue {
    match acc {
        AccState::CountStar(v) | AccState::CountCol(v) => FinalizedValue::Int64(Some(*v)),
        AccState::SumI64 { sum, any } => {
            if *any {
                FinalizedValue::Int64(Some(*sum as i64))
            } else {
                FinalizedValue::Int64(None)
            }
        }
        AccState::SumF64 { sum, any } => {
            if *any {
                FinalizedValue::Float64(Some(*sum))
            } else {
                FinalizedValue::Float64(None)
            }
        }
        AccState::MinI64(slot) | AccState::MaxI64(slot) => FinalizedValue::Int64(*slot),
        AccState::MinF64(slot) | AccState::MaxF64(slot) => FinalizedValue::Float64(*slot),
        AccState::AvgI64 { sum, count } => {
            if *count > 0 {
                FinalizedValue::Float64(Some(*sum as f64 / *count as f64))
            } else {
                FinalizedValue::Float64(None)
            }
        }
        AccState::AvgF64 { sum, count } => {
            if *count > 0 {
                FinalizedValue::Float64(Some(*sum / *count as f64))
            } else {
                FinalizedValue::Float64(None)
            }
        }
        AccState::FirstI64 { value, .. } | AccState::LastI64 { value, .. } => {
            FinalizedValue::Int64(*value)
        }
        AccState::FirstF64 { value, .. } | AccState::LastF64 { value, .. } => {
            FinalizedValue::Float64(*value)
        }
        AccState::FirstUtf8 { value, .. } | AccState::LastUtf8 { value, .. } => {
            FinalizedValue::Utf8(value.clone())
        }
        AccState::CountDistinctHllNumeric(hll) => {
            FinalizedValue::UInt64(Some(hll.count().round() as u64))
        }
        AccState::CountDistinctHllUtf8(hll) => {
            FinalizedValue::UInt64(Some(hll.count().round() as u64))
        }
        AccState::CountDistinctExactNumeric { set, .. } => {
            FinalizedValue::UInt64(Some(set.len() as u64))
        }
        AccState::CountDistinctExactUtf8 { set, .. } => {
            FinalizedValue::UInt64(Some(set.len() as u64))
        }
    }
}

#[async_trait]
impl BatchTransform for WindowedAggregateTransform {
    fn input_schema(&self) -> SchemaRef {
        // Best-effort: return the captured input schema if known,
        // else the configured group_by + agg output schema as a
        // placeholder.
        self.input_schema
            .get()
            .cloned()
            .unwrap_or_else(|| self.output_schema.clone())
    }

    fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    async fn transform(
        &self,
        input: RecordBatch,
        ctx: &BatchContext,
    ) -> Result<Vec<RecordBatch>, BackendError> {
        // 1. Run inner SQL pre-stage if present.
        let pre_batches: Vec<RecordBatch> = if let Some(inner) = &self.inner {
            inner.transform(input, ctx).await?
        } else {
            vec![input]
        };

        // 2. Ingest each batch into windowed state.
        for b in pre_batches {
            self.ingest(b, ctx.global_wm).await?;
        }

        // 3. Emit any windows whose end <= global_wm.
        match ctx.global_wm {
            Some(wm) => self.emit_ready(wm).await,
            None => Ok(Vec::new()),
        }
    }

    async fn on_idle_tick(&self, ctx: &BatchContext) -> Result<Vec<RecordBatch>, BackendError> {
        match ctx.global_wm {
            Some(wm) => self.emit_ready(wm).await,
            None => Ok(Vec::new()),
        }
    }

    /// Phase 39.5a P1.8 fix: trait-method override that delegates
    /// to the inherent `take_state_commit`. Without this override,
    /// callers holding `&dyn BatchTransform` (the streaming pipeline
    /// orchestrator + the periodic checkpoint ticker) hit the empty
    /// default impl and silently see no dirty state. The inherent
    /// method stays for direct-typed call sites in tests.
    async fn take_state_commit(
        &self,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, Vec<Vec<u8>>), BackendError> {
        WindowedAggregateTransform::take_state_commit(self).await
    }

    async fn recover_state(
        &self,
        state_by_key: &std::collections::HashMap<Vec<u8>, Vec<u8>>,
    ) -> Result<(), BackendError> {
        WindowedAggregateTransform::recover_state(self, state_by_key).await
    }

    /// Phase 39.5a P1.6: drain the per-transform DLQ buffer that
    /// `ingest()` / `ingest_session()` populate with single-row
    /// slices of past-budget rows under `LateDataPolicy::Dlq`.
    async fn take_dlq_rows(&self) -> Result<Vec<RecordBatch>, BackendError> {
        let mut state = self.state.lock().await;
        Ok(std::mem::take(&mut state.dlq_pending))
    }
}

// =====================================================================
// Output-batch builder
// =====================================================================

fn build_placeholder_output_schema(config: &WindowConfig) -> Result<SchemaRef, BackendError> {
    // Without input schema info, assume numeric columns are Int64
    // and string columns Utf8 — conservative defaults that match
    // most pipelines. The actual schema-from-real-types replacement
    // happens at first emit.
    let mut fields: Vec<Field> = Vec::new();
    fields.push(Field::new(
        config.window_start_column.as_str(),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        false,
    ));
    fields.push(Field::new(
        config.window_end_column.as_str(),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        false,
    ));
    if matches!(config.kind, WindowKind::Session) {
        // Phase 39.5a: session_id ordering matches the real emit
        // batch — right after window_start / window_end, before
        // group_by columns.
        fields.push(Field::new(
            config.session_id_column.as_str(),
            DataType::UInt64,
            false,
        ));
    }
    for g in &config.group_by {
        // Placeholder Utf8; resolved on first batch.
        fields.push(Field::new(g.as_str(), DataType::Utf8, true));
    }
    for spec in &config.aggregations {
        let ty = match spec.kind {
            AggKind::CountStar | AggKind::CountCol => DataType::Int64,
            AggKind::Avg => DataType::Float64,
            AggKind::CountDistinct => DataType::UInt64,
            AggKind::Sum | AggKind::Min | AggKind::Max | AggKind::First | AggKind::Last => {
                // Placeholder Int64; the real emit batch overrides.
                DataType::Int64
            }
        };
        fields.push(Field::new(spec.alias.as_str(), ty, true));
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn build_emit_batch(
    config: &WindowConfig,
    input_schema: Option<&SchemaRef>,
    window_start: i64,
    window_end: i64,
    entries: Vec<(GroupKey, Vec<FinalizedValue>)>,
) -> Result<RecordBatch, BackendError> {
    let n = entries.len();

    // Resolve group_by + agg column types from the input schema so
    // emit batches carry the right types.
    let input = input_schema
        .ok_or_else(|| BackendError::Other("window: emit before any batch ingested".into()))?;
    let group_idx: Vec<usize> = config
        .group_by
        .iter()
        .map(|c| input.index_of(c.as_str()))
        .collect::<Result<_, _>>()
        .map_err(|e| BackendError::Other(format!("window: emit schema lookup: {e}")))?;
    let agg_idx: Vec<Option<usize>> = config
        .aggregations
        .iter()
        .map(|a| match &a.column {
            None => Ok::<Option<usize>, BackendError>(None),
            Some(c) => Ok(Some(input.index_of(c.as_str()).map_err(|e| {
                BackendError::Other(format!("window: emit schema lookup: {e}"))
            })?)),
        })
        .collect::<Result<_, _>>()?;

    // window_start / window_end columns.
    let window_starts_array =
        TimestampMicrosecondArray::from(vec![window_start; n]).with_timezone("UTC");
    let window_ends_array =
        TimestampMicrosecondArray::from(vec![window_end; n]).with_timezone("UTC");

    // group_by columns: build per-column arrays from each entry's GroupKey.
    let mut fields: Vec<Field> =
        Vec::with_capacity(2 + config.group_by.len() + config.aggregations.len());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(fields.capacity());
    fields.push(Field::new(
        config.window_start_column.as_str(),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        false,
    ));
    fields.push(Field::new(
        config.window_end_column.as_str(),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        false,
    ));
    columns.push(Arc::new(window_starts_array));
    columns.push(Arc::new(window_ends_array));

    for (g_pos, &g_idx) in group_idx.iter().enumerate() {
        let g_field = input.field(g_idx);
        let arr = build_group_column(&entries, g_pos, g_field.data_type())?;
        fields.push(Field::new(
            g_field.name(),
            g_field.data_type().clone(),
            true,
        ));
        columns.push(arr);
    }

    for (a_pos, spec) in config.aggregations.iter().enumerate() {
        let input_type = agg_idx[a_pos].map(|i| input.field(i).data_type());
        let out_type = agg_output_type(spec.kind, input_type)?;
        let arr = build_agg_column(&entries, a_pos, &out_type)?;
        fields.push(Field::new(spec.alias.as_str(), out_type, true));
        columns.push(arr);
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns)
        .map_err(|e| BackendError::Other(format!("window: emit batch: {e}")))
}

/// Phase 39.5a: build the per-row emit batch for a session pass.
/// Differs from [`build_emit_batch`] in that `window_start` /
/// `window_end` are per-row (not constant for the batch) and a
/// `session_id` column is included.
fn build_session_emit_batch(
    config: &WindowConfig,
    input_schema: Option<&SchemaRef>,
    gap_us: i64,
    emits: Vec<SessionEmit>,
) -> Result<RecordBatch, BackendError> {
    let n = emits.len();
    let input = input_schema.ok_or_else(|| {
        BackendError::Other("window: session emit before any batch ingested".into())
    })?;

    let group_idx: Vec<usize> = config
        .group_by
        .iter()
        .map(|c| input.index_of(c.as_str()))
        .collect::<Result<_, _>>()
        .map_err(|e| BackendError::Other(format!("window: session emit schema lookup: {e}")))?;
    let agg_idx: Vec<Option<usize>> = config
        .aggregations
        .iter()
        .map(|a| match &a.column {
            None => Ok::<Option<usize>, BackendError>(None),
            Some(c) => Ok(Some(input.index_of(c.as_str()).map_err(|e| {
                BackendError::Other(format!("window: session emit schema lookup: {e}"))
            })?)),
        })
        .collect::<Result<_, _>>()?;

    let window_starts: Vec<i64> = emits.iter().map(|e| e.start_ts).collect();
    // window_end for a session is the close boundary — the first
    // wallclock at which no further row can be assigned to this
    // session given its current `last_event_ts`.
    let window_ends: Vec<i64> = emits.iter().map(|e| e.last_event_ts + gap_us).collect();
    let session_ids: Vec<u64> = emits
        .iter()
        .map(|e| compute_session_id(&e.group_key, e.start_ts))
        .collect();

    let window_starts_array = TimestampMicrosecondArray::from(window_starts).with_timezone("UTC");
    let window_ends_array = TimestampMicrosecondArray::from(window_ends).with_timezone("UTC");
    let session_ids_array = UInt64Array::from(session_ids);

    let mut fields: Vec<Field> =
        Vec::with_capacity(3 + config.group_by.len() + config.aggregations.len());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(fields.capacity());
    fields.push(Field::new(
        config.window_start_column.as_str(),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        false,
    ));
    fields.push(Field::new(
        config.window_end_column.as_str(),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        false,
    ));
    fields.push(Field::new(
        config.session_id_column.as_str(),
        DataType::UInt64,
        false,
    ));
    columns.push(Arc::new(window_starts_array));
    columns.push(Arc::new(window_ends_array));
    columns.push(Arc::new(session_ids_array));

    // Adapt to the same shape `build_group_column` /
    // `build_agg_column` already understand: `(GroupKey, Vec<FinalizedValue>)`.
    let entries: Vec<(GroupKey, Vec<FinalizedValue>)> = emits
        .into_iter()
        .map(|e| (e.group_key, e.finalized))
        .collect();

    for (g_pos, &g_idx) in group_idx.iter().enumerate() {
        let g_field = input.field(g_idx);
        let arr = build_group_column(&entries, g_pos, g_field.data_type())?;
        fields.push(Field::new(
            g_field.name(),
            g_field.data_type().clone(),
            true,
        ));
        columns.push(arr);
    }

    for (a_pos, spec) in config.aggregations.iter().enumerate() {
        let input_type = agg_idx[a_pos].map(|i| input.field(i).data_type());
        let out_type = agg_output_type(spec.kind, input_type)?;
        let arr = build_agg_column(&entries, a_pos, &out_type)?;
        fields.push(Field::new(spec.alias.as_str(), out_type, true));
        columns.push(arr);
    }

    let _ = n;
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns)
        .map_err(|e| BackendError::Other(format!("window: session emit batch: {e}")))
}

/// Phase 39.5a: deterministic per-session identifier.
/// `hash(group_key, start_ts)` as `u64`. Under `Reopen`, a session's
/// `start_ts` may shift backward (a late row earlier than the prior
/// start becomes the new start), so `session_id` shifts too. The
/// design doc accepts this — idempotent target writes use
/// `(group_keys..., session_id)` as the dedup key, same contract as
/// 39.4 windows.
fn compute_session_id(key: &GroupKey, start_ts: i64) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    key.hash(&mut h);
    start_ts.hash(&mut h);
    h.finish()
}

fn build_group_column(
    entries: &[(GroupKey, Vec<FinalizedValue>)],
    pos: usize,
    out_type: &DataType,
) -> Result<ArrayRef, BackendError> {
    use arrow_array::builder::*;
    match out_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let mut b = Int64Array::builder(entries.len());
            for (k, _) in entries {
                match &k.0[pos] {
                    KeyValue::Null => b.append_null(),
                    KeyValue::Int64(v) => b.append_value(*v),
                    KeyValue::UInt64(v) => b.append_value(*v as i64),
                    other => {
                        return Err(BackendError::Other(format!(
                            "window: group_by value type mismatch: expected integer, got {other:?}"
                        )));
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::with_capacity(entries.len(), entries.len() * 16);
            for (k, _) in entries {
                match &k.0[pos] {
                    KeyValue::Null => b.append_null(),
                    KeyValue::Utf8(s) => b.append_value(s.as_str()),
                    other => {
                        return Err(BackendError::Other(format!(
                            "window: group_by value type mismatch: expected utf8, got {other:?}"
                        )));
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let mut b = Float64Array::builder(entries.len());
            for (k, _) in entries {
                match &k.0[pos] {
                    KeyValue::Null => b.append_null(),
                    KeyValue::Float64Bits(bits) => b.append_value(f64::from_bits(*bits)),
                    other => {
                        return Err(BackendError::Other(format!(
                            "window: group_by value type mismatch: expected float, got {other:?}"
                        )));
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let mut b = TimestampMicrosecondArray::builder(entries.len());
            for (k, _) in entries {
                match &k.0[pos] {
                    KeyValue::Null => b.append_null(),
                    KeyValue::TsMicros(v) => b.append_value(*v),
                    other => {
                        return Err(BackendError::Other(format!(
                            "window: group_by value type mismatch: expected timestamp, got {other:?}"
                        )));
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        other => Err(BackendError::Other(format!(
            "window: group_by output type {other:?} not yet supported"
        ))),
    }
}

fn build_agg_column(
    entries: &[(GroupKey, Vec<FinalizedValue>)],
    pos: usize,
    out_type: &DataType,
) -> Result<ArrayRef, BackendError> {
    use arrow_array::builder::*;
    match out_type {
        DataType::Int64 => {
            let mut b = Int64Array::builder(entries.len());
            for (_, vals) in entries.iter() {
                match &vals[pos] {
                    FinalizedValue::Int64(Some(v)) => b.append_value(*v),
                    FinalizedValue::Int64(None) => b.append_null(),
                    other => {
                        return Err(BackendError::Other(format!(
                            "window: finalized value {other:?} not compatible with Int64 output"
                        )));
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let mut b = Float64Array::builder(entries.len());
            for (_, vals) in entries.iter() {
                match &vals[pos] {
                    FinalizedValue::Float64(Some(v)) => b.append_value(*v),
                    FinalizedValue::Float64(None) => b.append_null(),
                    other => {
                        return Err(BackendError::Other(format!(
                            "window: finalized value {other:?} not compatible with Float64 output"
                        )));
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::with_capacity(entries.len(), entries.len() * 16);
            for (_, vals) in entries.iter() {
                match &vals[pos] {
                    FinalizedValue::Utf8(Some(v)) => b.append_value(v),
                    FinalizedValue::Utf8(None) => b.append_null(),
                    other => {
                        return Err(BackendError::Other(format!(
                            "window: finalized value {other:?} not compatible with Utf8 output"
                        )));
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::UInt64 => {
            let mut b = UInt64Array::builder(entries.len());
            for (_, vals) in entries.iter() {
                match &vals[pos] {
                    FinalizedValue::UInt64(Some(v)) => b.append_value(*v),
                    FinalizedValue::UInt64(None) => b.append_null(),
                    other => {
                        return Err(BackendError::Other(format!(
                            "window: finalized value {other:?} not compatible with UInt64 output"
                        )));
                    }
                }
            }
            Ok(Arc::new(b.finish()))
        }
        other => Err(BackendError::Other(format!(
            "window: aggregation output type {other:?} not yet supported"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};

    fn schema_event() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Int64, false),
            Field::new("amount", DataType::Int64, true),
            Field::new(
                "_event_ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
        ]))
    }

    fn batch_event(ids: Vec<i64>, amounts: Vec<Option<i64>>, ts: Vec<i64>) -> RecordBatch {
        let schema = schema_event();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(amounts)),
                Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC")),
            ],
        )
        .unwrap()
    }

    fn basic_config() -> WindowConfig {
        WindowConfig {
            kind: WindowKind::Tumbling,
            duration_ms: 60_000, // 1-minute windows
            hop_ms: 60_000,
            event_time_column: "_event_ts".into(),
            group_by: vec!["user_id".into()],
            aggregations: vec![
                AggregationSpec::new(AggKind::CountStar, None, "n"),
                AggregationSpec::new(AggKind::Sum, Some("amount".into()), "amount_sum"),
            ],
            late_data: LateDataPolicy::Drop,
            max_groups_per_window: 100,
            window_start_column: "window_start".into(),
            window_end_column: "window_end".into(),
            session_id_column: "session_id".into(),
            gap_ms: None,
            max_session_duration_ms: None,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn tumbling_window_emits_when_watermark_crosses_end() {
        let cfg = basic_config();
        let t = WindowedAggregateTransform::new(cfg, None).unwrap();

        // Two rows in the same 1-minute window [0, 60_000_000).
        let b = batch_event(
            vec![1, 1, 2],
            vec![Some(10), Some(20), Some(100)],
            vec![1_000_000, 2_000_000, 3_000_000],
        );
        // global_wm before window end → no emit yet.
        let ctx = BatchContext {
            global_wm: Some(30_000_000),
            source_id: None,
        };
        let out = t.transform(b, &ctx).await.expect("first transform");
        assert!(out.is_empty(), "watermark < window_end → no emit");

        // Now advance watermark past window_end (60_000_000).
        let empty = batch_event(vec![], vec![], vec![]);
        let ctx = BatchContext {
            global_wm: Some(60_000_000),
            source_id: None,
        };
        let out = t.transform(empty, &ctx).await.expect("emit transform");
        assert_eq!(out.len(), 1, "one window emitted");
        let b = &out[0];
        assert_eq!(b.num_rows(), 2, "two distinct user_ids");
        // Verify schema columns.
        let schema = b.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec!["window_start", "window_end", "user_id", "n", "amount_sum"]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn idle_tick_emits_after_watermark_advances() {
        let cfg = basic_config();
        let t = WindowedAggregateTransform::new(cfg, None).unwrap();

        // Ingest one batch; watermark below window end.
        let b = batch_event(vec![1], vec![Some(10)], vec![1_000_000]);
        let _ = t
            .transform(
                b,
                &BatchContext {
                    global_wm: Some(1_500_000),
                    source_id: None,
                },
            )
            .await
            .unwrap();

        // Now an idle tick with watermark crossing the window end.
        let out = t
            .on_idle_tick(&BatchContext {
                global_wm: Some(60_000_000),
                source_id: None,
            })
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn late_data_dropped_after_window_emit() {
        let cfg = basic_config();
        let t = WindowedAggregateTransform::new(cfg, None).unwrap();

        // Phase 1: ingest row at ts=1_000_000 with wm < window_end.
        let b1 = batch_event(vec![1], vec![Some(10)], vec![1_000_000]);
        let _ = t
            .transform(
                b1,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: None,
                },
            )
            .await
            .unwrap();

        // Phase 2: advance wm past window_end → window [0, 60s) emits.
        let empty = batch_event(vec![], vec![], vec![]);
        let out = t
            .transform(
                empty.clone(),
                &BatchContext {
                    global_wm: Some(60_000_000),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(out.len(), 1, "first emit on watermark advance");

        // Phase 3: a row arrives for the already-emitted window
        // (event_ts < window_end + lateness_us = 60s + 0).
        let late = batch_event(vec![1], vec![Some(99)], vec![500_000]);
        let out = t
            .transform(
                late,
                &BatchContext {
                    global_wm: Some(60_000_000),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        assert!(out.is_empty(), "late row dropped → no new emit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn hopping_window_fans_row_out_to_overlapping_windows() {
        let cfg = WindowConfig {
            kind: WindowKind::Hopping,
            duration_ms: 60_000,
            hop_ms: 30_000,
            event_time_column: "_event_ts".into(),
            group_by: vec!["user_id".into()],
            aggregations: vec![AggregationSpec::new(AggKind::CountStar, None, "n")],
            late_data: LateDataPolicy::Drop,
            max_groups_per_window: 100,
            window_start_column: "window_start".into(),
            window_end_column: "window_end".into(),
            session_id_column: "session_id".into(),
            gap_ms: None,
            max_session_duration_ms: None,
        };
        let t = WindowedAggregateTransform::new(cfg, None).unwrap();

        // Row at ts=45_000_000us (45s). Belongs to windows starting at:
        //   - 0ms (window [0, 60s))
        //   - 30s (window [30s, 90s))
        // Both contain ts=45s.
        let b = batch_event(vec![1], vec![Some(0)], vec![45_000_000]);
        let _ = t
            .transform(
                b,
                &BatchContext {
                    global_wm: Some(50_000_000),
                    source_id: None,
                },
            )
            .await
            .unwrap();

        // Advance wm past 60s — first window emits.
        let empty = batch_event(vec![], vec![], vec![]);
        let out = t
            .transform(
                empty.clone(),
                &BatchContext {
                    global_wm: Some(60_000_000),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(out.len(), 1, "first hopping window emits");

        // Advance wm past 90s — second window emits.
        let out = t
            .transform(
                empty,
                &BatchContext {
                    global_wm: Some(90_000_000),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(out.len(), 1, "second hopping window emits");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cap_hit_fails_loud() {
        let mut cfg = basic_config();
        cfg.max_groups_per_window = 1;
        let t = WindowedAggregateTransform::new(cfg, None).unwrap();

        // Two distinct user_ids in the same window — cap=1 trips on
        // the second.
        let b = batch_event(vec![1, 2], vec![Some(10), Some(20)], vec![100, 200]);
        let err = t
            .transform(
                b,
                &BatchContext {
                    global_wm: None,
                    source_id: None,
                },
            )
            .await
            .expect_err("cap hit");
        assert!(err.to_string().contains("max_groups_per_window=1"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn count_sum_avg_first_last_correct() {
        let cfg = WindowConfig {
            kind: WindowKind::Tumbling,
            duration_ms: 60_000,
            hop_ms: 60_000,
            event_time_column: "_event_ts".into(),
            group_by: vec!["user_id".into()],
            aggregations: vec![
                AggregationSpec::new(AggKind::CountStar, None, "n"),
                AggregationSpec::new(AggKind::Sum, Some("amount".into()), "s"),
                AggregationSpec::new(AggKind::Min, Some("amount".into()), "mn"),
                AggregationSpec::new(AggKind::Max, Some("amount".into()), "mx"),
                AggregationSpec::new(AggKind::Avg, Some("amount".into()), "av"),
                AggregationSpec::new(AggKind::First, Some("amount".into()), "fst"),
                AggregationSpec::new(AggKind::Last, Some("amount".into()), "lst"),
            ],
            late_data: LateDataPolicy::Drop,
            max_groups_per_window: 100,
            window_start_column: "window_start".into(),
            window_end_column: "window_end".into(),
            session_id_column: "session_id".into(),
            gap_ms: None,
            max_session_duration_ms: None,
        };
        let t = WindowedAggregateTransform::new(cfg, None).unwrap();

        // user_id=1 with amounts [10, 20, 30] at ts [10us, 20us, 30us].
        let b = batch_event(
            vec![1, 1, 1],
            vec![Some(10), Some(20), Some(30)],
            vec![10, 20, 30],
        );
        // Ingest with wm < window_end so rows aren't past-budget.
        let _ = t
            .transform(
                b,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        // Now advance wm past window_end via idle tick → emit.
        let out = t
            .on_idle_tick(&BatchContext {
                global_wm: Some(60_000_000),
                source_id: None,
            })
            .await
            .unwrap();
        assert_eq!(out.len(), 1, "idle tick emits the [0, 60s) window");
        // Drop reference so we can rebuild for the second-pass check below.
        let cfg = WindowConfig {
            kind: WindowKind::Tumbling,
            duration_ms: 60_000,
            hop_ms: 60_000,
            event_time_column: "_event_ts".into(),
            group_by: vec!["user_id".into()],
            aggregations: vec![
                AggregationSpec::new(AggKind::CountStar, None, "n"),
                AggregationSpec::new(AggKind::Sum, Some("amount".into()), "s"),
                AggregationSpec::new(AggKind::Min, Some("amount".into()), "mn"),
                AggregationSpec::new(AggKind::Max, Some("amount".into()), "mx"),
                AggregationSpec::new(AggKind::Avg, Some("amount".into()), "av"),
                AggregationSpec::new(AggKind::First, Some("amount".into()), "fst"),
                AggregationSpec::new(AggKind::Last, Some("amount".into()), "lst"),
            ],
            late_data: LateDataPolicy::Drop,
            max_groups_per_window: 100,
            window_start_column: "window_start".into(),
            window_end_column: "window_end".into(),
            session_id_column: "session_id".into(),
            gap_ms: None,
            max_session_duration_ms: None,
        };
        let t = WindowedAggregateTransform::new(cfg, None).unwrap();
        let b = batch_event(
            vec![1, 1, 1],
            vec![Some(10), Some(20), Some(30)],
            vec![10, 20, 30],
        );
        // Ingest with wm=0 (rows on time), then emit by advancing wm.
        let _ = t
            .transform(
                b,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        let empty = batch_event(vec![], vec![], vec![]);
        let out = t
            .transform(
                empty,
                &BatchContext {
                    global_wm: Some(60_000_000),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        let b = &out[0];
        assert_eq!(b.num_rows(), 1);
        // Verify per-aggregator values.
        let n = b
            .column_by_name("n")
            .unwrap()
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0);
        assert_eq!(n, 3);
        let s = b
            .column_by_name("s")
            .unwrap()
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0);
        assert_eq!(s, 60);
        let mn = b
            .column_by_name("mn")
            .unwrap()
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0);
        assert_eq!(mn, 10);
        let mx = b
            .column_by_name("mx")
            .unwrap()
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0);
        assert_eq!(mx, 30);
        let av = b
            .column_by_name("av")
            .unwrap()
            .as_primitive::<arrow_array::types::Float64Type>()
            .value(0);
        assert!((av - 20.0).abs() < 1e-9);
        let fst = b
            .column_by_name("fst")
            .unwrap()
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0);
        assert_eq!(fst, 10, "first by event_ts (ts=10us)");
        let lst = b
            .column_by_name("lst")
            .unwrap()
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0);
        assert_eq!(lst, 30, "last by event_ts (ts=30us)");
    }

    #[test]
    fn config_validates_hopping_constraints() {
        let mut cfg = basic_config();
        cfg.kind = WindowKind::Hopping;
        cfg.hop_ms = 90_000; // > duration_ms
        let err = WindowedAggregateTransform::new(cfg, None).expect_err("hop > dur");
        assert!(err.to_string().contains("hop_ms"));

        let mut cfg = basic_config();
        cfg.kind = WindowKind::Hopping;
        cfg.hop_ms = 0;
        let err = WindowedAggregateTransform::new(cfg, None).expect_err("hop = 0");
        assert!(err.to_string().contains("hop_ms"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn metrics_track_emits_late_drops_and_active_state() {
        let registry = Registry::new();
        let metrics = WindowedMetrics::new(&registry, "p").unwrap();

        let cfg = basic_config();
        let t = WindowedAggregateTransform::new(cfg, None)
            .unwrap()
            .with_metrics(metrics.clone());

        // Ingest two rows in window [0, 60s) — windows_active=1.
        let b = batch_event(vec![1, 2], vec![Some(10), Some(20)], vec![100, 200]);
        let _ = t
            .transform(
                b,
                &BatchContext {
                    global_wm: Some(30_000_000),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(metrics.windows_active.get(), 1);
        assert_eq!(metrics.state_groups_total.get(), 2);
        assert_eq!(metrics.windows_emitted_total.get(), 0);

        // Emit by advancing watermark; windows_active drops to 0.
        let empty = batch_event(vec![], vec![], vec![]);
        let _ = t
            .transform(
                empty,
                &BatchContext {
                    global_wm: Some(60_000_000),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(metrics.windows_active.get(), 0);
        assert_eq!(metrics.windows_emitted_total.get(), 1);

        // Now feed a late row — late_rows_dropped_total{policy=drop}++.
        let late = batch_event(vec![1], vec![Some(99)], vec![100]);
        let _ = t
            .transform(
                late,
                &BatchContext {
                    global_wm: Some(60_000_000),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            metrics
                .late_rows_dropped_total
                .with_label_values(&["drop"])
                .get(),
            1
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reopen_re_emits_window_after_late_arrival() {
        // Reopen budget = 30 s. Sequence:
        //   Phase 1: ingest user_id=1 with sum=10 in window [0, 60s). wm=0.
        //   Phase 2: advance wm to 65s. Window emits sum=10. State retained.
        //   Phase 3: late row arrives for window [0, 60s) with sum=99. wm=65s.
        //   Phase 4: advance wm to 70s (still < 60s + 30s budget). Re-emit
        //            with corrected sum = 10+99 = 109.
        //   Phase 5: advance wm past 90s (window_end + lateness). State drops.
        let cfg = WindowConfig {
            kind: WindowKind::Tumbling,
            duration_ms: 60_000,
            hop_ms: 60_000,
            event_time_column: "_event_ts".into(),
            group_by: vec!["user_id".into()],
            aggregations: vec![
                AggregationSpec::new(AggKind::CountStar, None, "n"),
                AggregationSpec::new(AggKind::Sum, Some("amount".into()), "amount_sum"),
            ],
            late_data: LateDataPolicy::Reopen {
                allowed_lateness_ms: 30_000,
            },
            max_groups_per_window: 100,
            window_start_column: "window_start".into(),
            window_end_column: "window_end".into(),
            session_id_column: "session_id".into(),
            gap_ms: None,
            max_session_duration_ms: None,
        };
        let t = WindowedAggregateTransform::new(cfg, None).unwrap();

        // Phase 1: on-time row.
        let _ = t
            .transform(
                batch_event(vec![1], vec![Some(10)], vec![1_000_000]),
                &BatchContext {
                    global_wm: Some(0),
                    source_id: None,
                },
            )
            .await
            .unwrap();

        // Phase 2: first emit at wm=65s.
        let empty = batch_event(vec![], vec![], vec![]);
        let out = t
            .transform(
                empty.clone(),
                &BatchContext {
                    global_wm: Some(65_000_000),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(out.len(), 1, "first emit");
        let sum = out[0]
            .column_by_name("amount_sum")
            .unwrap()
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0);
        assert_eq!(sum, 10);

        // Phase 3: late row arrives. wm=65s, window_end + budget = 90s.
        // Within budget → row re-aggregates AND triggers immediate
        // re-emit (transform() runs ingest THEN emit_ready in the
        // same call; dirty gets set by ingest and consumed by
        // emit_ready). No need to wait for next tick.
        let out = t
            .transform(
                batch_event(vec![1], vec![Some(99)], vec![5_000_000]),
                &BatchContext {
                    global_wm: Some(65_000_000),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(out.len(), 1, "late arrival triggers re-emit");
        let sum = out[0]
            .column_by_name("amount_sum")
            .unwrap()
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0);
        assert_eq!(sum, 10 + 99, "re-emit shows corrected sum");

        // Phase 4: subsequent wm advance with no further late rows;
        // dirty was reset by Phase 3's emit, so no re-emit.
        let out = t
            .transform(
                empty.clone(),
                &BatchContext {
                    global_wm: Some(70_000_000),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        assert!(out.is_empty(), "dirty reset → no re-emit on next tick");

        // Phase 5: wm past lateness budget → state drops, late row drops.
        let out = t
            .transform(
                batch_event(vec![1], vec![Some(123)], vec![3_000_000]),
                &BatchContext {
                    global_wm: Some(95_000_000),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        assert!(out.is_empty(), "past-budget late row produces no emit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reopen_emits_only_once_when_no_dirty_flag() {
        // Reopen budget=30s. After first emit, no late rows. The window
        // should NOT re-emit on subsequent watermark advances (still
        // within budget) because dirty=false.
        let cfg = WindowConfig {
            kind: WindowKind::Tumbling,
            duration_ms: 60_000,
            hop_ms: 60_000,
            event_time_column: "_event_ts".into(),
            group_by: vec!["user_id".into()],
            aggregations: vec![AggregationSpec::new(AggKind::CountStar, None, "n")],
            late_data: LateDataPolicy::Reopen {
                allowed_lateness_ms: 30_000,
            },
            max_groups_per_window: 100,
            window_start_column: "window_start".into(),
            window_end_column: "window_end".into(),
            session_id_column: "session_id".into(),
            gap_ms: None,
            max_session_duration_ms: None,
        };
        let t = WindowedAggregateTransform::new(cfg, None).unwrap();

        let _ = t
            .transform(
                batch_event(vec![1], vec![Some(10)], vec![1_000_000]),
                &BatchContext {
                    global_wm: Some(0),
                    source_id: None,
                },
            )
            .await
            .unwrap();

        // First emit at wm=65s.
        let empty = batch_event(vec![], vec![], vec![]);
        let out = t
            .transform(
                empty.clone(),
                &BatchContext {
                    global_wm: Some(65_000_000),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(out.len(), 1);

        // Subsequent wm advance, still within budget — no late rows
        // since first emit, no re-emit expected.
        let out = t
            .transform(
                empty.clone(),
                &BatchContext {
                    global_wm: Some(75_000_000),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        assert!(out.is_empty(), "no dirty flag → no re-emit");

        // wm past budget → state drops. No emit, just cleanup.
        let out = t
            .transform(
                empty,
                &BatchContext {
                    global_wm: Some(95_000_000),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        assert!(out.is_empty(), "drop phase doesn't emit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn count_distinct_approximate_returns_within_error_bound() {
        // 10 batches of 100 rows each; each batch has user_id 0..99
        // (so 100 distinct user_ids total). HLL+ at p=14 has ~0.81%
        // standard error; 100 distinct values should land in
        // [98, 102] easily.
        let cfg = WindowConfig {
            kind: WindowKind::Tumbling,
            duration_ms: 60_000,
            hop_ms: 60_000,
            event_time_column: "_event_ts".into(),
            group_by: vec![],
            aggregations: vec![AggregationSpec {
                kind: AggKind::CountDistinct,
                column: Some("user_id".into()),
                alias: "unique_users".into(),
                count_distinct_mode: Some(CountDistinctMode::Approximate),
                max_distinct_values_per_group: None,
            }],
            late_data: LateDataPolicy::Drop,
            max_groups_per_window: 100,
            window_start_column: "window_start".into(),
            window_end_column: "window_end".into(),
            session_id_column: "session_id".into(),
            gap_ms: None,
            max_session_duration_ms: None,
        };
        let t = WindowedAggregateTransform::new(cfg, None).unwrap();

        for batch_idx in 0..10 {
            let ids: Vec<i64> = (0..100).collect();
            let amounts: Vec<Option<i64>> = (0..100).map(Some).collect();
            let ts: Vec<i64> = (0..100).map(|i| batch_idx * 1000 + i).collect();
            let b = batch_event(ids, amounts, ts);
            let _ = t
                .transform(
                    b,
                    &BatchContext {
                        global_wm: Some(0),
                        source_id: None,
                    },
                )
                .await
                .unwrap();
        }
        // Emit by advancing watermark.
        let empty = batch_event(vec![], vec![], vec![]);
        let out = t
            .transform(
                empty,
                &BatchContext {
                    global_wm: Some(60_000_000),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        let b = &out[0];
        assert_eq!(b.num_rows(), 1);
        let n = b
            .column_by_name("unique_users")
            .unwrap()
            .as_primitive::<arrow_array::types::UInt64Type>()
            .value(0);
        // 100 ground truth, ±2 for 0.81% error budget
        assert!(
            (98..=102).contains(&n),
            "HLL+ approximate count {} outside [98, 102]",
            n
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn count_distinct_exact_returns_exactly_correct() {
        let cfg = WindowConfig {
            kind: WindowKind::Tumbling,
            duration_ms: 60_000,
            hop_ms: 60_000,
            event_time_column: "_event_ts".into(),
            group_by: vec![],
            aggregations: vec![AggregationSpec {
                kind: AggKind::CountDistinct,
                column: Some("user_id".into()),
                alias: "unique_users".into(),
                count_distinct_mode: Some(CountDistinctMode::Exact),
                max_distinct_values_per_group: Some(1000),
            }],
            late_data: LateDataPolicy::Drop,
            max_groups_per_window: 100,
            window_start_column: "window_start".into(),
            window_end_column: "window_end".into(),
            session_id_column: "session_id".into(),
            gap_ms: None,
            max_session_duration_ms: None,
        };
        let t = WindowedAggregateTransform::new(cfg, None).unwrap();

        // 5 distinct user_ids, repeated. Ingest with wm=0, emit by advancing.
        let ids: Vec<i64> = vec![1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 1, 1];
        let amounts: Vec<Option<i64>> = ids.iter().map(|_| Some(0)).collect();
        let ts: Vec<i64> = (0..ids.len() as i64).collect();
        let b = batch_event(ids, amounts, ts);
        let _ = t
            .transform(
                b,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        let empty = batch_event(vec![], vec![], vec![]);
        let out = t
            .transform(
                empty,
                &BatchContext {
                    global_wm: Some(60_000_000),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        let n = out[0]
            .column_by_name("unique_users")
            .unwrap()
            .as_primitive::<arrow_array::types::UInt64Type>()
            .value(0);
        assert_eq!(n, 5, "exact count_distinct must be exact");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn count_distinct_exact_cap_hit_fails_loud() {
        let cfg = WindowConfig {
            kind: WindowKind::Tumbling,
            duration_ms: 60_000,
            hop_ms: 60_000,
            event_time_column: "_event_ts".into(),
            group_by: vec![],
            aggregations: vec![AggregationSpec {
                kind: AggKind::CountDistinct,
                column: Some("user_id".into()),
                alias: "unique_users".into(),
                count_distinct_mode: Some(CountDistinctMode::Exact),
                max_distinct_values_per_group: Some(2),
            }],
            late_data: LateDataPolicy::Drop,
            max_groups_per_window: 100,
            window_start_column: "window_start".into(),
            window_end_column: "window_end".into(),
            session_id_column: "session_id".into(),
            gap_ms: None,
            max_session_duration_ms: None,
        };
        let t = WindowedAggregateTransform::new(cfg, None).unwrap();

        let ids: Vec<i64> = vec![1, 2, 3]; // 3rd value trips cap=2
        let amounts: Vec<Option<i64>> = vec![Some(0), Some(0), Some(0)];
        let ts: Vec<i64> = vec![1, 2, 3];
        let b = batch_event(ids, amounts, ts);
        let err = t
            .transform(
                b,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: None,
                },
            )
            .await
            .expect_err("cap hit");
        assert!(err.to_string().contains("max_distinct_values_per_group=2"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn count_distinct_exact_requires_cap() {
        let cfg = WindowConfig {
            kind: WindowKind::Tumbling,
            duration_ms: 60_000,
            hop_ms: 60_000,
            event_time_column: "_event_ts".into(),
            group_by: vec![],
            aggregations: vec![AggregationSpec {
                kind: AggKind::CountDistinct,
                column: Some("user_id".into()),
                alias: "unique_users".into(),
                count_distinct_mode: Some(CountDistinctMode::Exact),
                max_distinct_values_per_group: None, // missing → fail-loud
            }],
            late_data: LateDataPolicy::Drop,
            max_groups_per_window: 100,
            window_start_column: "window_start".into(),
            window_end_column: "window_end".into(),
            session_id_column: "session_id".into(),
            gap_ms: None,
            max_session_duration_ms: None,
        };
        let t = WindowedAggregateTransform::new(cfg, None).unwrap();
        let b = batch_event(vec![1], vec![Some(0)], vec![1]);
        let err = t
            .transform(
                b,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: None,
                },
            )
            .await
            .expect_err("missing cap");
        assert!(err.to_string().contains("max_distinct_values_per_group"));
    }

    #[test]
    fn config_validates_alias_collisions() {
        let mut cfg = basic_config();
        cfg.aggregations[0].alias = "user_id".into();
        let err = WindowedAggregateTransform::new(cfg, None).expect_err("alias clash");
        assert!(err.to_string().contains("collides"));
    }

    // Suppress unused warnings on helper used only by future tests.
    #[allow(dead_code)]
    fn schema_with_str() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new(
                "_event_ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
        ]))
    }

    #[allow(dead_code)]
    fn _force_use_str(_b: &StringArray) {}

    // =================================================================
    // Phase 39.5a (PR 2) — session windows
    // =================================================================

    fn session_config_basic() -> WindowConfig {
        WindowConfig {
            kind: WindowKind::Session,
            duration_ms: 0,
            hop_ms: 0,
            gap_ms: Some(30_000),
            max_session_duration_ms: Some(7_200_000),
            event_time_column: "_event_ts".into(),
            group_by: vec!["user_id".into()],
            aggregations: vec![AggregationSpec::new(AggKind::CountStar, None, "n")],
            late_data: LateDataPolicy::Drop,
            max_groups_per_window: 100,
            window_start_column: "window_start".into(),
            window_end_column: "window_end".into(),
            session_id_column: "session_id".into(),
        }
    }

    #[test]
    fn session_validates_with_minimal_config() {
        let mut cfg = session_config_basic();
        cfg.validate().expect("session config must validate");
    }

    #[test]
    fn session_rejects_missing_gap_ms() {
        let mut cfg = session_config_basic();
        cfg.gap_ms = None;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("gap_ms is required"));
    }

    #[test]
    fn session_rejects_zero_gap_ms() {
        let mut cfg = session_config_basic();
        cfg.gap_ms = Some(0);
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("gap_ms must be > 0"));
    }

    #[test]
    fn session_rejects_missing_max_duration() {
        let mut cfg = session_config_basic();
        cfg.max_session_duration_ms = None;
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string()
                .contains("max_session_duration_ms is required"),
            "got: {err}"
        );
    }

    #[test]
    fn session_rejects_max_duration_le_gap() {
        let mut cfg = session_config_basic();
        cfg.gap_ms = Some(30_000);
        cfg.max_session_duration_ms = Some(30_000);
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("must be > gap_ms"));
    }

    #[test]
    fn session_rejects_empty_group_by() {
        let mut cfg = session_config_basic();
        cfg.group_by = Vec::new();
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string().contains("group_by must be non-empty"),
            "got: {err}"
        );
    }

    #[test]
    fn session_rejects_nonzero_duration_ms() {
        let mut cfg = session_config_basic();
        cfg.duration_ms = 60_000;
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string().contains("duration_ms must be 0"),
            "got: {err}"
        );
    }

    #[test]
    fn session_rejects_nonzero_hop_ms() {
        let mut cfg = session_config_basic();
        cfg.hop_ms = 1_000;
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("hop_ms must be 0"), "got: {err}");
    }

    #[test]
    fn tumbling_rejects_gap_ms() {
        let mut cfg = basic_config();
        cfg.gap_ms = Some(1_000);
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string().contains("gap_ms is only valid"),
            "got: {err}"
        );
    }

    #[test]
    fn tumbling_rejects_max_session_duration_ms() {
        let mut cfg = basic_config();
        cfg.max_session_duration_ms = Some(1_000);
        let err = cfg.validate().unwrap_err();
        assert!(
            err.to_string()
                .contains("max_session_duration_ms is only valid"),
            "got: {err}"
        );
    }

    #[test]
    fn session_rejects_session_id_collision_with_window_start() {
        let mut cfg = session_config_basic();
        cfg.session_id_column = "window_start".into();
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("session_id_column"), "got: {err}");
    }

    // ----- AccState::combine() — slice 2.2 -----

    #[test]
    fn combine_count_star_adds() {
        let mut a = AccState::CountStar(3);
        a.combine(AccState::CountStar(7)).unwrap();
        match a {
            AccState::CountStar(v) => assert_eq!(v, 10),
            _ => unreachable!(),
        }
    }

    #[test]
    fn combine_sum_i64_propagates_any() {
        let mut a = AccState::SumI64 { sum: 0, any: false };
        let b = AccState::SumI64 { sum: 5, any: true };
        a.combine(b).unwrap();
        match a {
            AccState::SumI64 { sum, any } => {
                assert_eq!(sum, 5);
                assert!(any);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn combine_min_handles_none() {
        let mut a = AccState::MinI64(None);
        a.combine(AccState::MinI64(Some(7))).unwrap();
        match a {
            AccState::MinI64(s) => assert_eq!(s, Some(7)),
            _ => unreachable!(),
        }
        let mut b = AccState::MinI64(Some(3));
        b.combine(AccState::MinI64(Some(5))).unwrap();
        match b {
            AccState::MinI64(s) => assert_eq!(s, Some(3)),
            _ => unreachable!(),
        }
    }

    #[test]
    fn combine_avg_componentwise() {
        let mut a = AccState::AvgI64 { sum: 10, count: 2 };
        a.combine(AccState::AvgI64 { sum: 30, count: 3 }).unwrap();
        match a {
            AccState::AvgI64 { sum, count } => {
                assert_eq!(sum, 40);
                assert_eq!(count, 5);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn combine_first_picks_smaller_ts() {
        let mut a = AccState::FirstI64 {
            ts: Some(20),
            value: Some(200),
        };
        let b = AccState::FirstI64 {
            ts: Some(10),
            value: Some(100),
        };
        a.combine(b).unwrap();
        match a {
            AccState::FirstI64 { ts, value } => {
                assert_eq!(ts, Some(10));
                assert_eq!(value, Some(100));
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn combine_last_picks_larger_ts() {
        let mut a = AccState::LastI64 {
            ts: Some(10),
            value: Some(100),
        };
        let b = AccState::LastI64 {
            ts: Some(20),
            value: Some(200),
        };
        a.combine(b).unwrap();
        match a {
            AccState::LastI64 { ts, value } => {
                assert_eq!(ts, Some(20));
                assert_eq!(value, Some(200));
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn combine_first_with_empty_other_keeps_self() {
        let mut a = AccState::FirstI64 {
            ts: Some(10),
            value: Some(100),
        };
        a.combine(AccState::FirstI64 {
            ts: None,
            value: None,
        })
        .unwrap();
        match a {
            AccState::FirstI64 { ts, value } => {
                assert_eq!(ts, Some(10));
                assert_eq!(value, Some(100));
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn combine_hll_union_grows_count() {
        // Two sketches with disjoint inputs should combine into a
        // sketch whose count ≈ sum of inputs.
        let mut a = AccState::CountDistinctHllNumeric(
            HyperLogLogPlus::new(HLL_PRECISION, RandomState::new()).unwrap(),
        );
        let mut b = AccState::CountDistinctHllNumeric(
            HyperLogLogPlus::new(HLL_PRECISION, RandomState::new()).unwrap(),
        );
        if let AccState::CountDistinctHllNumeric(hll) = &mut a {
            for i in 0..50_u64 {
                hll.insert(&i);
            }
        }
        if let AccState::CountDistinctHllNumeric(hll) = &mut b {
            for i in 50..100_u64 {
                hll.insert(&i);
            }
        }
        a.combine(b).unwrap();
        let count = match &mut a {
            AccState::CountDistinctHllNumeric(hll) => hll.count().round() as u64,
            _ => unreachable!(),
        };
        // HLL+ accuracy: ~0.81% std error; allow a generous bound.
        assert!(
            (90..=110).contains(&count),
            "merged count {count} should be ~100"
        );
    }

    #[test]
    fn combine_exact_set_unions() {
        let mut a = AccState::CountDistinctExactNumeric {
            set: [1_u64, 2, 3].into_iter().collect(),
            cap: 100,
        };
        let b = AccState::CountDistinctExactNumeric {
            set: [3_u64, 4, 5].into_iter().collect(),
            cap: 100,
        };
        a.combine(b).unwrap();
        match &a {
            AccState::CountDistinctExactNumeric { set, .. } => {
                assert_eq!(set.len(), 5);
                for i in 1..=5 {
                    assert!(set.contains(&i));
                }
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn combine_exact_set_cap_failure() {
        let mut a = AccState::CountDistinctExactNumeric {
            set: [1_u64, 2, 3].into_iter().collect(),
            cap: 4,
        };
        let b = AccState::CountDistinctExactNumeric {
            set: [4_u64, 5].into_iter().collect(),
            cap: 4,
        };
        let err = a.combine(b).unwrap_err();
        assert!(err.to_string().contains("cap hit"), "got: {err}");
    }

    #[test]
    fn combine_mismatched_variants_errors() {
        let mut a = AccState::CountStar(1);
        let b = AccState::SumI64 { sum: 5, any: true };
        let err = a.combine(b).unwrap_err();
        assert!(err.to_string().contains("mismatched accumulator"));
    }

    // ----- Session ingest + emit — slices 2.3 + 2.4 -----

    /// Session config tweaked for tighter timings — gap=10us,
    /// max=100us, all in microseconds. Tests use the `_event_ts`
    /// column in microseconds throughout, so the gap_ms/etc fields
    /// must remain in milliseconds; we set gap_ms=1 (=1000us) so
    /// rows at 100us spacings open a new session.
    fn session_config_for_test() -> WindowConfig {
        WindowConfig {
            kind: WindowKind::Session,
            duration_ms: 0,
            hop_ms: 0,
            gap_ms: Some(1),                    // 1ms = 1000us gap
            max_session_duration_ms: Some(100), // 100ms cap
            event_time_column: "_event_ts".into(),
            group_by: vec!["user_id".into()],
            aggregations: vec![
                AggregationSpec::new(AggKind::CountStar, None, "n"),
                AggregationSpec::new(AggKind::Sum, Some("amount".into()), "s"),
            ],
            late_data: LateDataPolicy::Drop,
            max_groups_per_window: 100,
            window_start_column: "window_start".into(),
            window_end_column: "window_end".into(),
            session_id_column: "session_id".into(),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn session_emits_when_watermark_crosses_close_deadline() {
        // gap_ms=1 → 1000us. Three rows at ts=10/20/30 us (all
        // within 1000us of each other → one session).
        let cfg = session_config_for_test();
        let t = WindowedAggregateTransform::new(cfg, None).unwrap();

        let b = batch_event(
            vec![1, 1, 1],
            vec![Some(10), Some(20), Some(30)],
            vec![10, 20, 30],
        );
        // wm before close_deadline (last=30 + gap=1000 = 1030).
        let out = t
            .transform(
                b,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        assert!(out.is_empty(), "wm < close_deadline → no emit");

        // Advance wm past close_deadline.
        let empty = batch_event(vec![], vec![], vec![]);
        let out = t
            .transform(
                empty,
                &BatchContext {
                    global_wm: Some(2000),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(out.len(), 1, "session emits once watermark crosses");
        let b = &out[0];
        assert_eq!(b.num_rows(), 1, "one session for one user");
        let schema = b.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec![
                "window_start",
                "window_end",
                "session_id",
                "user_id",
                "n",
                "s"
            ]
        );
        // Verify aggregator values.
        let n = b
            .column_by_name("n")
            .unwrap()
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0);
        assert_eq!(n, 3);
        let s = b
            .column_by_name("s")
            .unwrap()
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0);
        assert_eq!(s, 60);
        // window_start = first event ts; window_end = last + gap.
        let ws = b
            .column_by_name("window_start")
            .unwrap()
            .as_primitive::<TimestampMicrosecondType>()
            .value(0);
        assert_eq!(ws, 10);
        let we = b
            .column_by_name("window_end")
            .unwrap()
            .as_primitive::<TimestampMicrosecondType>()
            .value(0);
        assert_eq!(we, 30 + 1000);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn session_gap_splits_into_two_sessions() {
        // Two clusters of rows: [10,20] and [10000,10010].
        // Gap between clusters: 9980us > 1000us → two sessions.
        let cfg = session_config_for_test();
        let t = WindowedAggregateTransform::new(cfg, None).unwrap();
        let b = batch_event(
            vec![1, 1, 1, 1],
            vec![Some(1), Some(2), Some(3), Some(4)],
            vec![10, 20, 10_000, 10_010],
        );
        let _ = t
            .transform(
                b,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        // Advance wm past second session's close (10010 + 1000 = 11010).
        let out = t
            .on_idle_tick(&BatchContext {
                global_wm: Some(20_000),
                source_id: None,
            })
            .await
            .unwrap();
        // Two sessions emitted in one batch.
        assert_eq!(out.len(), 1);
        let b = &out[0];
        assert_eq!(b.num_rows(), 2);
        let mut starts: Vec<i64> = (0..2)
            .map(|i| {
                b.column_by_name("window_start")
                    .unwrap()
                    .as_primitive::<TimestampMicrosecondType>()
                    .value(i)
            })
            .collect();
        starts.sort();
        assert_eq!(starts, vec![10, 10_000]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn session_per_key_isolation() {
        // Two users, each with their own session.
        let cfg = session_config_for_test();
        let t = WindowedAggregateTransform::new(cfg, None).unwrap();
        let b = batch_event(
            vec![1, 2, 1, 2],
            vec![Some(10), Some(100), Some(20), Some(200)],
            vec![5, 5, 15, 15],
        );
        let _ = t
            .transform(
                b,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        let out = t
            .on_idle_tick(&BatchContext {
                global_wm: Some(5_000),
                source_id: None,
            })
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        let b = &out[0];
        assert_eq!(b.num_rows(), 2, "one session per user");

        // Build (user_id -> sum) and verify isolation.
        let user_id = b
            .column_by_name("user_id")
            .unwrap()
            .as_primitive::<arrow_array::types::Int64Type>();
        let s = b
            .column_by_name("s")
            .unwrap()
            .as_primitive::<arrow_array::types::Int64Type>();
        let mut pairs: Vec<(i64, i64)> = (0..b.num_rows())
            .map(|i| (user_id.value(i), s.value(i)))
            .collect();
        pairs.sort();
        assert_eq!(pairs, vec![(1, 30), (2, 300)]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn session_force_emit_on_duration_cap() {
        // gap_ms=10 → 10_000us gap. max_session_duration_ms=100 →
        // 100_000us cap. Rows form a chain inside the gap, but the
        // last row pushes the session span past the cap.
        let cfg = WindowConfig {
            gap_ms: Some(10),
            max_session_duration_ms: Some(100),
            ..session_config_for_test()
        };
        let t = WindowedAggregateTransform::new(cfg, None).unwrap();

        // Rows at 0, 9_000, 18_000, …, 99_000us all within gap
        // (≤ 10_000us steps), all within cap (max−min ≤ 100_000us).
        // The 105_000us row is also within gap of 99_000 but pushes
        // span to 105_000 > 100_000 → force-emit prior session.
        let mut ts: Vec<i64> = (0..=11).map(|i| i * 9_000).collect();
        ts.push(105_000);
        let n_rows = ts.len();
        let ids: Vec<i64> = vec![1; n_rows];
        let amounts: Vec<Option<i64>> = (0..n_rows).map(|_| Some(1)).collect();
        let b = batch_event(ids, amounts, ts);
        let out = t
            .transform(
                b,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        // Force-emit fires regardless of wm.
        assert_eq!(out.len(), 1, "force-emit produces a batch");
        let force_batch = &out[0];
        assert_eq!(force_batch.num_rows(), 1);
        let n = force_batch
            .column_by_name("n")
            .unwrap()
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0);
        assert_eq!(n, 12, "first 12 in-cap rows form the force-emitted session");

        // The boundary-crossing row started a new session. Drain it.
        let empty = batch_event(vec![], vec![], vec![]);
        let out = t
            .transform(
                empty,
                &BatchContext {
                    global_wm: Some(200_000_000),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        let b = &out[0];
        assert_eq!(b.num_rows(), 1);
        let n = b
            .column_by_name("n")
            .unwrap()
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0);
        assert_eq!(n, 1, "boundary-crossing row alone in new session");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn session_session_id_changes_when_start_shifts_under_reopen() {
        // Reopen with allowed_lateness_ms covering several seconds.
        let cfg = WindowConfig {
            gap_ms: Some(10),                      // 10ms gap
            max_session_duration_ms: Some(10_000), // 10s cap
            late_data: LateDataPolicy::Reopen {
                allowed_lateness_ms: 1_000,
            },
            ..session_config_for_test()
        };
        let t = WindowedAggregateTransform::new(cfg, None).unwrap();

        // Initial row at ts=20_000us → session [20_000, 20_000].
        let b1 = batch_event(vec![1], vec![Some(1)], vec![20_000]);
        let _ = t
            .transform(
                b1,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: None,
                },
            )
            .await
            .unwrap();

        // Late row at ts=5_000us. Within 10ms (10_000us) of session
        // start (20_000us)? 20_000 - 5_000 = 15_000 > 10_000us.
        // So this row would NOT extend backward. Let me fix: use
        // ts=15_000us → 20_000 - 15_000 = 5_000 ≤ 10_000 → extends
        // session backward to start_ts=15_000.
        let b2 = batch_event(vec![1], vec![Some(2)], vec![15_000]);
        let _ = t
            .transform(
                b2,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: None,
                },
            )
            .await
            .unwrap();

        // Capture session_id by emitting. Advance wm past the
        // session close + lateness.
        let out = t
            .on_idle_tick(&BatchContext {
                global_wm: Some(2_000_000),
                source_id: None,
            })
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        let b = &out[0];
        assert_eq!(b.num_rows(), 1);
        let ws = b
            .column_by_name("window_start")
            .unwrap()
            .as_primitive::<TimestampMicrosecondType>()
            .value(0);
        assert_eq!(
            ws, 15_000,
            "session_start shifted backward to absorb the late row"
        );
        let n = b
            .column_by_name("n")
            .unwrap()
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0);
        assert_eq!(n, 2);
    }

    /// Phase 39.5a P1.6: under `LateDataPolicy::Dlq`, past-budget
    /// rows stash into the transform's DLQ buffer instead of being
    /// dropped. `take_dlq_rows()` drains the buffer.
    #[tokio::test(flavor = "multi_thread")]
    async fn session_late_row_routes_to_dlq_under_dlq_policy() {
        use crate::transform::BatchTransform;
        let mut cfg = session_config_for_test();
        cfg.late_data = LateDataPolicy::Dlq;
        let t = WindowedAggregateTransform::new(cfg, None).unwrap();

        // Admit one row.
        let b1 = batch_event(vec![1], vec![Some(1)], vec![10]);
        let _ = t
            .transform(
                b1,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: None,
                },
            )
            .await
            .unwrap();

        // Late row past budget: wm=2000, gap=1000us, ts=5 →
        // 2000 > 5+1000 → past-budget. Under Dlq, captured to
        // the buffer.
        let b2 = batch_event(vec![1], vec![Some(99)], vec![5]);
        let _ = t
            .transform(
                b2,
                &BatchContext {
                    global_wm: Some(2_000),
                    source_id: None,
                },
            )
            .await
            .unwrap();

        // Drain through the trait method so we hit production
        // dispatch (the same path the pipeline uses).
        let dlq = (&t as &dyn BatchTransform).take_dlq_rows().await.unwrap();
        assert_eq!(dlq.len(), 1, "one late row stashed to DLQ");
        assert_eq!(dlq[0].num_rows(), 1);
        // The slice carries the original column values.
        let amount = dlq[0]
            .column_by_name("amount")
            .unwrap()
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0);
        assert_eq!(amount, 99);

        // Buffer is drained; second take returns empty.
        let dlq2 = (&t as &dyn BatchTransform).take_dlq_rows().await.unwrap();
        assert!(dlq2.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn session_late_row_drops_under_drop_policy() {
        // Drop policy: rows past `wm > event_ts + gap` are dropped
        // at ingest.
        let cfg = session_config_for_test();
        let t = WindowedAggregateTransform::new(cfg, None).unwrap();

        // First batch: row at ts=10, wm=0 → admitted → S=[10,10].
        let b1 = batch_event(vec![1], vec![Some(1)], vec![10]);
        let _ = t
            .transform(
                b1,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: None,
                },
            )
            .await
            .unwrap();

        // Second batch: late row at ts=5 (well behind wm). With
        // gap=1000us and lateness=0, the row's "max possible"
        // close_deadline is 1005us; wm=2_000 > 1005 → drop.
        // The same wm=2000 also advances past S's emit_threshold
        // (10+1000=1010), so S emits with the original count of 1.
        let b2 = batch_event(vec![1], vec![Some(99)], vec![5]);
        let out = t
            .transform(
                b2,
                &BatchContext {
                    global_wm: Some(2_000),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            out.len(),
            1,
            "session emits at wm=2000 > emit_threshold=1010"
        );
        let b = &out[0];
        let n = b
            .column_by_name("n")
            .unwrap()
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0);
        assert_eq!(
            n, 1,
            "late row dropped under Drop policy; only the original row counts"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn session_merge_two_sessions_under_reopen() {
        // gap_ms=15 → 15_000us gap. cap=1s. Reopen lateness=1s.
        // S1 at ts=100, S2 at ts=20_000 — split because
        // 20_000 - 100 = 19_900 > 15_000.
        // Late row at ts=10_500: dist to S1.last=100 is 10_400
        // (≤ 15_000 in gap of S1); dist to S2.start=20_000 is
        // 9_500 (≤ 15_000 in gap of S2). Bridges → merge.
        let cfg = WindowConfig {
            gap_ms: Some(15),
            max_session_duration_ms: Some(1_000),
            late_data: LateDataPolicy::Reopen {
                allowed_lateness_ms: 1_000,
            },
            ..session_config_for_test()
        };
        let t = WindowedAggregateTransform::new(cfg, None).unwrap();

        let b1 = batch_event(vec![1], vec![Some(1)], vec![100]);
        let _ = t
            .transform(
                b1,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: None,
                },
            )
            .await
            .unwrap();

        let b2 = batch_event(vec![1], vec![Some(10)], vec![20_000]);
        let _ = t
            .transform(
                b2,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: None,
                },
            )
            .await
            .unwrap();

        let b3 = batch_event(vec![1], vec![Some(100)], vec![10_500]);
        let _ = t
            .transform(
                b3,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: None,
                },
            )
            .await
            .unwrap();

        let out = t
            .on_idle_tick(&BatchContext {
                global_wm: Some(50_000_000),
                source_id: None,
            })
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        let b = &out[0];
        assert_eq!(b.num_rows(), 1, "two sessions merged into one");
        let n = b
            .column_by_name("n")
            .unwrap()
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0);
        assert_eq!(n, 3);
        let s = b
            .column_by_name("s")
            .unwrap()
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0);
        assert_eq!(s, 111);
        let ws = b
            .column_by_name("window_start")
            .unwrap()
            .as_primitive::<TimestampMicrosecondType>()
            .value(0);
        assert_eq!(ws, 100);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn session_id_stable_within_unchanged_session() {
        let cfg = session_config_for_test();
        let t = WindowedAggregateTransform::new(cfg, None).unwrap();
        let b = batch_event(vec![1, 1], vec![Some(1), Some(2)], vec![10, 20]);
        let _ = t
            .transform(
                b,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        let out = t
            .on_idle_tick(&BatchContext {
                global_wm: Some(5_000),
                source_id: None,
            })
            .await
            .unwrap();
        let b = &out[0];
        let sid = b
            .column_by_name("session_id")
            .unwrap()
            .as_primitive::<arrow_array::types::UInt64Type>()
            .value(0);
        // Recompute via direct hash: same key + start_ts should equal sid.
        let key = GroupKey(vec![KeyValue::Int64(1)]);
        assert_eq!(sid, compute_session_id(&key, 10));
    }

    // ----- PR 3: state-store commit drain + recovery -----

    #[tokio::test(flavor = "multi_thread")]
    async fn take_state_commit_returns_dirty_keys() {
        let cfg = session_config_for_test();
        let t = WindowedAggregateTransform::new(cfg, None).unwrap();

        let b = batch_event(vec![1, 2], vec![Some(10), Some(20)], vec![5, 5]);
        let _ = t
            .transform(
                b,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: None,
                },
            )
            .await
            .unwrap();

        let (upserts, deletes) = t.take_state_commit().await.unwrap();
        assert_eq!(upserts.len(), 2, "two keys touched");
        assert!(deletes.is_empty(), "no evictions yet");

        // Bytes should round-trip through the blob codec.
        for (key_bytes, blob_bytes) in &upserts {
            let key = crate::session_blob::decode_group_key(key_bytes).unwrap();
            let sessions = crate::session_blob::decode_sessions(blob_bytes).unwrap();
            assert_eq!(sessions.len(), 1);
            assert_eq!(sessions[0].start_ts, 5);
            assert_eq!(sessions[0].last_event_ts, 5);
            // GroupKey carries one Int64 value.
            assert_eq!(key.values().len(), 1);
        }

        // Subsequent drain returns empty (dirty cleared).
        let (u2, d2) = t.take_state_commit().await.unwrap();
        assert!(u2.is_empty() && d2.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn take_state_commit_marks_evictions_after_emit() {
        let cfg = session_config_for_test();
        let t = WindowedAggregateTransform::new(cfg, None).unwrap();
        let b = batch_event(vec![1], vec![Some(10)], vec![10]);
        let _ = t
            .transform(
                b,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: None,
                },
            )
            .await
            .unwrap();

        // Drain initial dirty.
        let (_u, _d) = t.take_state_commit().await.unwrap();

        // Advance wm past close → emit + evict (Drop policy).
        let _ = t
            .on_idle_tick(&BatchContext {
                global_wm: Some(10_000),
                source_id: None,
            })
            .await
            .unwrap();

        let (upserts, deletes) = t.take_state_commit().await.unwrap();
        assert!(upserts.is_empty(), "no live sessions after eviction");
        assert_eq!(deletes.len(), 1, "evicted key shows up as delete");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recover_state_round_trips() {
        // Pipeline 1: ingest 2 keys with active sessions.
        let cfg = session_config_for_test();
        let t1 = WindowedAggregateTransform::new(cfg, None).unwrap();
        let b = batch_event(vec![1, 2], vec![Some(7), Some(13)], vec![10, 20]);
        let _ = t1
            .transform(
                b,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        let (upserts, _) = t1.take_state_commit().await.unwrap();

        // Pipeline 2: fresh transform; recover the upserts. Use the
        // same config so aggregator layout matches.
        let cfg2 = session_config_for_test();
        let t2 = WindowedAggregateTransform::new(cfg2, None).unwrap();
        let map: std::collections::HashMap<Vec<u8>, Vec<u8>> = upserts.into_iter().collect();
        t2.recover_state(&map).await.unwrap();

        // Production flow: after recovery, the next batch from the
        // source populates the input schema. We emulate that with a
        // trivial in-gap row that joins one of the existing
        // sessions, then advance the watermark.
        let nudge = batch_event(vec![1], vec![Some(0)], vec![11]);
        let _ = t2
            .transform(
                nudge,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        let out = t2
            .on_idle_tick(&BatchContext {
                global_wm: Some(50_000),
                source_id: None,
            })
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        let b = &out[0];
        assert_eq!(b.num_rows(), 2, "two recovered sessions emit");
        let user_id = b
            .column_by_name("user_id")
            .unwrap()
            .as_primitive::<arrow_array::types::Int64Type>();
        let s = b
            .column_by_name("s")
            .unwrap()
            .as_primitive::<arrow_array::types::Int64Type>();
        let mut pairs: Vec<(i64, i64)> = (0..b.num_rows())
            .map(|i| (user_id.value(i), s.value(i)))
            .collect();
        pairs.sort();
        // user_id=1 had sum=7 from recovery + 0 from the nudge → 7.
        // user_id=2 had sum=13 from recovery, untouched.
        assert_eq!(pairs, vec![(1, 7), (2, 13)]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn take_state_commit_is_noop_for_tumbling() {
        let cfg = basic_config();
        let t = WindowedAggregateTransform::new(cfg, None).unwrap();
        let (upserts, deletes) = t.take_state_commit().await.unwrap();
        assert!(upserts.is_empty());
        assert!(deletes.is_empty());
    }

    // ----- DLQ Phase 3: rewind hooks (is_stateful / clear_state) -----

    /// Every window kind accumulates cross-batch state, so the
    /// rewind orchestration must demand `confirm_state_reset` for
    /// any windowed pipeline.
    #[tokio::test(flavor = "multi_thread")]
    async fn windowed_transform_reports_stateful() {
        use crate::transform::BatchTransform;
        let tumbling = WindowedAggregateTransform::new(basic_config(), None).unwrap();
        assert!(
            BatchTransform::is_stateful(&tumbling),
            "tumbling windows hold open accumulators across batches"
        );
        let session = WindowedAggregateTransform::new(session_config_for_test(), None).unwrap();
        assert!(BatchTransform::is_stateful(&session));
    }

    /// `clear_state` wipes session state entirely: no pending
    /// commit rows, and nothing emits once the watermark passes.
    #[tokio::test(flavor = "multi_thread")]
    async fn clear_state_discards_sessions_and_dirty_keys() {
        use crate::transform::BatchTransform;
        let t = WindowedAggregateTransform::new(session_config_for_test(), None).unwrap();
        let b = batch_event(vec![1, 2], vec![Some(10), Some(20)], vec![5, 5]);
        let _ = t
            .transform(
                b,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: None,
                },
            )
            .await
            .unwrap();

        BatchTransform::clear_state(&t).await.unwrap();

        let (upserts, deletes) = t.take_state_commit().await.unwrap();
        assert!(
            upserts.is_empty() && deletes.is_empty(),
            "clear_state wipes dirty/evicted tracking"
        );
        let out = t
            .on_idle_tick(&BatchContext {
                global_wm: Some(50_000),
                source_id: None,
            })
            .await
            .unwrap();
        assert!(out.is_empty(), "cleared sessions never emit");
    }

    /// `clear_state` also drops open tumbling/hopping windows — a
    /// rewound pipeline re-consumes their rows, so leftover
    /// accumulators would double-count.
    #[tokio::test(flavor = "multi_thread")]
    async fn clear_state_discards_open_tumbling_windows() {
        use crate::transform::BatchTransform;
        let t = WindowedAggregateTransform::new(basic_config(), None).unwrap();
        let b = batch_event(vec![1], vec![Some(10)], vec![1_000_000]);
        let out = t
            .transform(
                b,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        assert!(out.is_empty(), "window still open");

        BatchTransform::clear_state(&t).await.unwrap();

        // Watermark passes the window end — nothing to emit.
        let empty = batch_event(vec![], vec![], vec![]);
        let out = t
            .transform(
                empty,
                &BatchContext {
                    global_wm: Some(60_000_000),
                    source_id: None,
                },
            )
            .await
            .unwrap();
        let rows: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 0, "cleared windows never emit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn session_max_groups_cap_fails_loud() {
        let mut cfg = session_config_for_test();
        cfg.max_groups_per_window = 1;
        let t = WindowedAggregateTransform::new(cfg, None).unwrap();
        // Two distinct keys → second insert hits cap.
        let b = batch_event(vec![1, 2], vec![Some(1), Some(2)], vec![10, 10]);
        let err = t
            .transform(
                b,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: None,
                },
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("max_groups_per_window=1"),
            "got: {err}"
        );
    }
}
