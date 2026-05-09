//! Phase 39.5b: keyed time-windowed stream-stream join.
//!
//! [`TimeWindowedJoinTransform`] joins two `[[sources]]` on a
//! configured key set, emitting one output row per (left, right)
//! pair whose event-times fall within `time_window_ms`.
//!
//! See `docs/PHASE_39_5B_JOINS.md` for the full design.
//!
//! # Routing
//!
//! Each input batch arrives via [`crate::transform::BatchTransform::transform`]
//! with a [`crate::transform::BatchContext::source_id`] field set to
//! the source's `query`. The transform routes to its left buffer
//! when `source_id == config.left_source` and right buffer when
//! `source_id == config.right_source`. Batches with any other
//! `source_id` (or `None`) are rejected — config-load already
//! rejects mismatches, so this is a defensive check.
//!
//! # Match algebra
//!
//! On each row `R` arriving on side `S`:
//! 1. Compute the join key `K` from `R[S_keys]`.
//! 2. Look up the **opposite** side's buffer for `K`.
//! 3. For each entry `E` in that buffer with
//!    `|R.event_ts - E.event_ts| ≤ time_window_ms`, emit one
//!    output row combining `R`'s columns with `E`'s columns.
//! 4. After emit, append `(R.event_ts, R)` to `S`'s buffer.
//!
//! # Eviction
//!
//! On every `transform` / `on_idle_tick` call, before processing
//! the new batch, evict any buffer entry past
//! `wm > entry.event_ts + time_window_ms + allowed_lateness_ms`.
//! Evictions are per-row, not per-key — a partially-stale buffer
//! keeps its fresh entries.
//!
//! # PR 1 scope (in-memory only)
//!
//! - Inner join only.
//! - `Drop` late-data policy only.
//! - State is held in memory; lost on restart. State-store
//!   integration lands in PR 2.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::TimestampMicrosecondType;
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::backend::BackendError;
use crate::transform::{BatchContext, BatchTransform};
use crate::windowed::{GroupKey, KeyValue};

/// Join kind.
///
/// - `Inner` (default): emit one row per (L, R) match within the
///   time window. Unmatched rows are silently dropped at eviction.
/// - `LeftOuter` (Phase 39.5b P2.12): every left row emits at
///   least once. If a left row evicts without ever matching, emit
///   `(left_data, NULLs)`. Right-side columns become nullable in
///   the output schema.
/// - `RightOuter`: symmetric — every right row emits at least
///   once.
/// - `FullOuter`: union of LeftOuter + RightOuter — both sides'
///   unmatched rows fire NULL-padded emits at eviction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

impl JoinKind {
    /// Does an unmatched left row (evicted without ever matching)
    /// emit a NULL-padded output row under this kind?
    fn emits_orphan_left(self) -> bool {
        matches!(self, Self::LeftOuter | Self::FullOuter)
    }

    /// Does an unmatched right row emit?
    fn emits_orphan_right(self) -> bool {
        matches!(self, Self::RightOuter | Self::FullOuter)
    }
}

/// Late-data policy for stream-stream join.
///
/// - `Drop` (default): rows past their per-side retention horizon
///   are dropped at ingest. Buffered rows are evicted when
///   `wm > entry.event_ts + horizon`.
/// - `Reopen` (Phase 39.5b P2.13): extends the retention horizon
///   on both sides by `allowed_lateness_ms`. Late rows arriving
///   within the budget are still admitted and can match opposite-
///   side buffered rows that are similarly retained.
///
/// Each (left, right) pair still emits exactly once under either
/// policy — match detection runs at ingest, against the opposite
/// side's *current* buffer. Reopen just gives both sides longer
/// to find each other. The "duplicate emit" risk that exists in
/// this layer is actually the state-store-recovery + Kafka-
/// redelivery case, present under any policy: idempotent targets
/// absorb at-least-once duplicates via the join's downstream
/// dedup key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinLateDataPolicy {
    Drop,
    Reopen { allowed_lateness_ms: u64 },
}

impl JoinLateDataPolicy {
    fn lateness_micros(self) -> i64 {
        match self {
            Self::Drop => 0,
            Self::Reopen {
                allowed_lateness_ms,
            } => (allowed_lateness_ms as i64).saturating_mul(1_000),
        }
    }
}

/// Configuration for a [`TimeWindowedJoinTransform`].
#[derive(Debug, Clone)]
pub struct JoinConfig {
    pub kind: JoinKind,
    /// `source_id` of the left source — usually the source's
    /// `query` field. Match-routed at ingest time.
    pub left_source: String,
    pub right_source: String,
    /// Equi-join keys. `left_keys[i]` joins against `right_keys[i]`.
    /// Must be non-empty and same length on both sides.
    pub left_keys: Vec<String>,
    pub right_keys: Vec<String>,
    /// Symmetric join window: `|left.event_ts - right.event_ts| ≤
    /// time_window_ms` when `min_delta_ms` / `max_delta_ms` are
    /// `None`. Ignored if either asymmetric bound is set.
    pub time_window_ms: u64,
    /// Phase 39.5b P2.14: asymmetric join window — minimum
    /// `right.event_ts - left.event_ts` in milliseconds. Negative
    /// for "right may arrive before left." When set, overrides
    /// the symmetric `time_window_ms`. Both `min_delta_ms` and
    /// `max_delta_ms` must be set together.
    pub min_delta_ms: Option<i64>,
    /// Maximum `right.event_ts - left.event_ts` in milliseconds.
    /// Positive for "right may arrive after left."
    pub max_delta_ms: Option<i64>,
    /// Column carrying the per-row event timestamp on **both**
    /// sides. Defaults to `"_event_ts"`.
    pub event_time_column: String,
    pub late_data: JoinLateDataPolicy,
    /// Output column prefixes. Default `"left_"` / `"right_"` —
    /// every input column on each side gets prefixed in the
    /// output schema. Eliminates collision risk between two
    /// sources that share a column name.
    pub left_column_prefix: String,
    pub right_column_prefix: String,
}

impl JoinConfig {
    pub fn validate(&self) -> Result<(), BackendError> {
        if self.left_source.is_empty() || self.right_source.is_empty() {
            return Err(BackendError::Other(
                "join: left_source and right_source must be non-empty".into(),
            ));
        }
        if self.left_source == self.right_source {
            return Err(BackendError::Other(
                "join: left_source and right_source must differ".into(),
            ));
        }
        if self.left_keys.is_empty() || self.right_keys.is_empty() {
            return Err(BackendError::Other(
                "join: left_keys and right_keys must be non-empty".into(),
            ));
        }
        if self.left_keys.len() != self.right_keys.len() {
            return Err(BackendError::Other(format!(
                "join: left_keys ({}) and right_keys ({}) must have equal length",
                self.left_keys.len(),
                self.right_keys.len()
            )));
        }
        match (self.min_delta_ms, self.max_delta_ms) {
            (None, None) => {
                if self.time_window_ms == 0 {
                    return Err(BackendError::Other(
                        "join: time_window_ms must be > 0".into(),
                    ));
                }
            }
            (Some(mn), Some(mx)) => {
                if mn >= mx {
                    return Err(BackendError::Other(format!(
                        "join: min_delta_ms ({mn}) must be < max_delta_ms ({mx})"
                    )));
                }
            }
            _ => {
                return Err(BackendError::Other(
                    "join: min_delta_ms and max_delta_ms must be set together (or \
                     leave both unset to use symmetric time_window_ms)"
                        .into(),
                ));
            }
        }
        if self.left_column_prefix == self.right_column_prefix {
            return Err(BackendError::Other(
                "join: left_column_prefix and right_column_prefix must differ".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    Left,
    Right,
}

/// One buffered row, retained until the watermark passes its
/// retention deadline.
///
/// `values` is held behind an `Arc` so a row that matches N
/// opposite-side rows on emit pays N cheap pointer-clones instead
/// of N deep `Vec<ScalarValue>` clones — the steady-state hot
/// path documented in P3 #22 (criterion bench
/// `join_steady_state_1000x1000_match_rate`). Wire format is
/// unchanged: with serde's `rc` feature, `Arc<T>` round-trips as
/// `T`, so the postcard buffer blob is byte-identical to the pre-Arc
/// shape.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct BufferedRow {
    event_ts: i64,
    values: Arc<Vec<ScalarValue>>,
    /// Phase 39.5b P2.12: `true` once this row has matched any
    /// opposite-side row. Drives outer-join orphan-emit logic at
    /// eviction time. Default `false`; flipped to `true` on first
    /// match.
    #[serde(default)]
    matched: bool,
}

/// Owned scalar — minimal subset matching the windowed module's
/// `KeyValue`, plus boolean. Keeps the value carrier independent
/// from Arrow's Array indexing model.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum ScalarValue {
    Null,
    Int64(i64),
    UInt64(u64),
    Float64Bits(u64),
    Utf8(String),
    TsMicros(i64),
    Bool(bool),
}

/// Phase 39.5b PR 2: postcard wire format for a per-(side, key)
/// buffer. Mirrors the in-memory `Vec<BufferedRow>` directly —
/// `BufferedRow` already serializes via derived serde. Wrapped
/// in a struct so future shape changes go through a single
/// versioned envelope.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct BufferBlob {
    rows: Vec<BufferedRow>,
}

/// Side prefix on the encoded `StateKey` so left and right buffers
/// for the same group key don't collide in the state store.
const STATE_KEY_LEFT: u8 = b'L';
const STATE_KEY_RIGHT: u8 = b'R';

fn encode_state_key(side: Side, key: &GroupKey) -> Result<Vec<u8>, BackendError> {
    let prefix = match side {
        Side::Left => STATE_KEY_LEFT,
        Side::Right => STATE_KEY_RIGHT,
    };
    let mut out = Vec::with_capacity(2);
    out.push(prefix);
    out.push(0); // separator
    let key_bytes = crate::session_blob::encode_group_key(key)?;
    out.extend_from_slice(&key_bytes);
    Ok(out)
}

fn decode_state_key(bytes: &[u8]) -> Result<(Side, GroupKey), BackendError> {
    if bytes.len() < 2 {
        return Err(BackendError::Other(
            "join state key: too short to carry side prefix".into(),
        ));
    }
    let side = match bytes[0] {
        STATE_KEY_LEFT => Side::Left,
        STATE_KEY_RIGHT => Side::Right,
        other => {
            return Err(BackendError::Other(format!(
                "join state key: unknown side prefix {other:#x}"
            )));
        }
    };
    if bytes[1] != 0 {
        return Err(BackendError::Other(
            "join state key: missing 0-byte separator after side prefix".into(),
        ));
    }
    let key = crate::session_blob::decode_group_key(&bytes[2..])?;
    Ok((side, key))
}

fn encode_buffer(rows: &[BufferedRow]) -> Result<Vec<u8>, BackendError> {
    let blob = BufferBlob {
        rows: rows.to_vec(),
    };
    postcard::to_allocvec(&blob)
        .map_err(|e| BackendError::Other(format!("join buffer encode: {e}")))
}

fn decode_buffer(bytes: &[u8]) -> Result<Vec<BufferedRow>, BackendError> {
    let blob: BufferBlob = postcard::from_bytes(bytes)
        .map_err(|e| BackendError::Other(format!("join buffer decode: {e}")))?;
    Ok(blob.rows)
}

#[derive(Debug, Default)]
struct JoinState {
    left: HashMap<GroupKey, Vec<BufferedRow>>,
    right: HashMap<GroupKey, Vec<BufferedRow>>,
    /// Group keys whose left/right buffer changed since the last
    /// `take_state_commit` drain. Drives dirty-only commits.
    dirty_left_keys: HashSet<GroupKey>,
    dirty_right_keys: HashSet<GroupKey>,
    /// Group keys whose left/right buffer was fully evicted since
    /// the last drain — emit a delete row.
    evicted_left_keys: HashSet<GroupKey>,
    evicted_right_keys: HashSet<GroupKey>,
}

pub struct TimeWindowedJoinTransform {
    config: JoinConfig,
    /// Captured on first batch from each side.
    left_schema: tokio::sync::OnceCell<SchemaRef>,
    right_schema: tokio::sync::OnceCell<SchemaRef>,
    /// Output schema — built lazily from the first batch on each
    /// side once both have been seen.
    output_schema: Mutex<Option<SchemaRef>>,
    state: Mutex<JoinState>,
}

impl std::fmt::Debug for TimeWindowedJoinTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimeWindowedJoinTransform")
            .field("config", &self.config)
            .finish()
    }
}

impl TimeWindowedJoinTransform {
    pub fn new(config: JoinConfig) -> Result<Self, BackendError> {
        config.validate()?;
        Ok(Self {
            config,
            left_schema: tokio::sync::OnceCell::new(),
            right_schema: tokio::sync::OnceCell::new(),
            output_schema: Mutex::new(None),
            state: Mutex::new(JoinState::default()),
        })
    }

    fn key_indices(&self, schema: &Schema, names: &[String]) -> Result<Vec<usize>, BackendError> {
        names
            .iter()
            .map(|n| {
                schema.index_of(n.as_str()).map_err(|_| {
                    BackendError::Other(format!("join: key column `{n}` not found in input schema"))
                })
            })
            .collect()
    }

    fn ts_index(&self, schema: &Schema) -> Result<usize, BackendError> {
        schema
            .index_of(self.config.event_time_column.as_str())
            .map_err(|_| {
                BackendError::Other(format!(
                    "join: event_time_column `{}` not found in input schema",
                    self.config.event_time_column
                ))
            })
    }

    /// Phase 39.5b P2.14: resolve the active `(min_us, max_us)`
    /// signed window where `r - l` must lie. Symmetric configs
    /// expand `time_window_ms` to `[-W, +W]`; explicit
    /// asymmetric bounds override.
    fn delta_range_us(&self) -> (i64, i64) {
        match (self.config.min_delta_ms, self.config.max_delta_ms) {
            (Some(mn), Some(mx)) => (mn.saturating_mul(1_000), mx.saturating_mul(1_000)),
            _ => {
                let w = (self.config.time_window_ms as i64).saturating_mul(1_000);
                (-w, w)
            }
        }
    }

    /// Per-side retention horizon. A left row at ts L is no-longer-
    /// matchable when wm > L + max_us + lateness_us; a right row at
    /// ts R when wm > R - min_us + lateness_us.
    fn retention_horizons_us(&self) -> (i64, i64) {
        let (min_us, max_us) = self.delta_range_us();
        let lateness_us = self.config.late_data.lateness_micros();
        (max_us + lateness_us, -min_us + lateness_us)
    }

    /// Evict buffer entries past their retention deadline. Per-row.
    /// Keys whose buffer becomes empty get pushed onto the
    /// per-side eviction sets so the next commit emits a delete.
    ///
    /// Phase 39.5b P2.12: returns the list of orphan emits — rows
    /// that evict without ever having matched, under outer-join
    /// kinds. Inner joins return an empty Vec.
    fn evict_state(&self, state: &mut JoinState, wm: Option<i64>) -> Vec<EmittedRow> {
        let mut orphans: Vec<EmittedRow> = Vec::new();
        let Some(wm) = wm else { return orphans };
        let (left_horizon, right_horizon) = self.retention_horizons_us();
        let kind = self.config.kind;

        let mut left_evicted: Vec<GroupKey> = Vec::new();
        state.left.retain(|k, rows| {
            let before = rows.len();
            rows.retain(|r| {
                let still_alive = wm <= r.event_ts + left_horizon;
                if !still_alive && !r.matched && kind.emits_orphan_left() {
                    // Outer-join orphan: emit `(left_data, NULLs)`.
                    // `Arc::clone` — no deep scalar copy.
                    orphans.push(EmittedRow {
                        left: Some(Arc::clone(&r.values)),
                        right: None,
                    });
                }
                still_alive
            });
            let kept = !rows.is_empty();
            if !kept && before > 0 {
                left_evicted.push(k.clone());
            } else if rows.len() != before {
                // Partial eviction — buffer mutated but not empty.
                state.dirty_left_keys.insert(k.clone());
            }
            kept
        });
        for k in left_evicted {
            state.dirty_left_keys.remove(&k);
            state.evicted_left_keys.insert(k);
        }

        let mut right_evicted: Vec<GroupKey> = Vec::new();
        state.right.retain(|k, rows| {
            let before = rows.len();
            rows.retain(|r| {
                let still_alive = wm <= r.event_ts + right_horizon;
                if !still_alive && !r.matched && kind.emits_orphan_right() {
                    orphans.push(EmittedRow {
                        left: None,
                        right: Some(Arc::clone(&r.values)),
                    });
                }
                still_alive
            });
            let kept = !rows.is_empty();
            if !kept && before > 0 {
                right_evicted.push(k.clone());
            } else if rows.len() != before {
                state.dirty_right_keys.insert(k.clone());
            }
            kept
        });
        for k in right_evicted {
            state.dirty_right_keys.remove(&k);
            state.evicted_right_keys.insert(k);
        }
        orphans
    }

    async fn ingest_side(
        &self,
        side: Side,
        batch: RecordBatch,
        wm: Option<i64>,
    ) -> Result<Vec<RecordBatch>, BackendError> {
        // Capture per-side schema on first batch.
        let schema = match side {
            Side::Left => {
                self.left_schema
                    .get_or_init(|| async { batch.schema() })
                    .await
            }
            Side::Right => {
                self.right_schema
                    .get_or_init(|| async { batch.schema() })
                    .await
            }
        }
        .clone();

        let key_idx = self.key_indices(
            &schema,
            match side {
                Side::Left => &self.config.left_keys,
                Side::Right => &self.config.right_keys,
            },
        )?;
        let ts_idx = self.ts_index(&schema)?;

        let ts_array = batch.column(ts_idx);
        if !matches!(
            ts_array.data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, _)
        ) {
            return Err(BackendError::Other(format!(
                "join: event_time_column `{}` must be Timestamp(Microsecond, _), got {:?}",
                self.config.event_time_column,
                ts_array.data_type()
            )));
        }
        let ts_array = ts_array.as_primitive::<TimestampMicrosecondType>();

        let key_arrays: Vec<ArrayRef> = key_idx.iter().map(|&i| batch.column(i).clone()).collect();
        let all_arrays: Vec<ArrayRef> = (0..batch.num_columns())
            .map(|i| batch.column(i).clone())
            .collect();

        let (min_us, max_us) = self.delta_range_us();
        let (left_horizon, right_horizon) = self.retention_horizons_us();

        let mut state = self.state.lock().await;
        // Evict before processing — opposite-side rows past their
        // deadline can't match this batch's rows anyway. Under
        // outer-join kinds, eviction may surface orphan emits;
        // include them in this batch's output.
        let mut emitted_rows: Vec<EmittedRow> = self.evict_state(&mut state, wm);

        for row in 0..batch.num_rows() {
            if ts_array.is_null(row) {
                continue;
            }
            let event_ts = ts_array.value(row);

            // Late-data check: per-side retention horizon. A left
            // row is droppable when wm > L + max + lateness; a
            // right row when wm > R - min + lateness.
            if let Some(wm) = wm {
                let horizon = match side {
                    Side::Left => left_horizon,
                    Side::Right => right_horizon,
                };
                if wm > event_ts + horizon {
                    continue;
                }
            }

            // Build the join key from the side's key columns.
            let key = GroupKey::from_row(&key_arrays, row)?;

            // Snapshot the row's values for buffering + emit. Wrap
            // in Arc so subsequent match emits and the buffer entry
            // share one heap allocation.
            let values: Arc<Vec<ScalarValue>> = Arc::new(
                (0..all_arrays.len())
                    .map(|c| extract_scalar(&all_arrays[c], row))
                    .collect::<Result<Vec<_>, _>>()?,
            );

            // Find matches against the OPPOSITE buffer.
            // P2.12: track whether THIS arriving row matched any
            // opposite-side row, so it gets buffered with
            // `matched = true` (no orphan emit at eviction).
            let mut my_matched = false;
            let opposite = match side {
                Side::Left => state.right.get_mut(&key),
                Side::Right => state.left.get_mut(&key),
            };
            if let Some(matches) = opposite {
                for opp_row in matches.iter_mut() {
                    // Signed delta: r - l. The arriving side's
                    // identity is `event_ts`; the buffered side
                    // is `opp_row.event_ts`.
                    let dt = match side {
                        Side::Left => opp_row.event_ts - event_ts,
                        Side::Right => event_ts - opp_row.event_ts,
                    };
                    if min_us <= dt && dt <= max_us {
                        // Order: left + right (regardless of which
                        // side arrived first). Both clones are
                        // cheap `Arc::clone`s — no deep copy.
                        let (left_vals, right_vals) = match side {
                            Side::Left => (Arc::clone(&values), Arc::clone(&opp_row.values)),
                            Side::Right => (Arc::clone(&opp_row.values), Arc::clone(&values)),
                        };
                        emitted_rows.push(EmittedRow {
                            left: Some(left_vals),
                            right: Some(right_vals),
                        });
                        // P2.12: mark the opposite-side row matched
                        // so it doesn't fire an orphan-emit when it
                        // evicts.
                        opp_row.matched = true;
                        my_matched = true;
                    }
                }
            }

            // Append `(R)` to its own side's buffer. `matched` is
            // set per the matching pass above — outer-join orphan
            // emit at eviction depends on it.
            let own_buf = match side {
                Side::Left => &mut state.left,
                Side::Right => &mut state.right,
            };
            own_buf.entry(key.clone()).or_default().push(BufferedRow {
                event_ts,
                values,
                matched: my_matched,
            });
            // Track dirty for PR 2's state-store commit path.
            match side {
                Side::Left => state.dirty_left_keys.insert(key),
                Side::Right => state.dirty_right_keys.insert(key),
            };
        }

        if emitted_rows.is_empty() {
            return Ok(Vec::new());
        }

        // Build the output batch. Resolve / cache the output schema
        // on first emit using both side schemas.
        let mut out_schema = self.output_schema.lock().await;
        if out_schema.is_none() {
            let l = self.left_schema.get();
            let r = self.right_schema.get();
            if let (Some(ls), Some(rs)) = (l, r) {
                *out_schema = Some(build_output_schema(
                    ls,
                    rs,
                    &self.config.left_column_prefix,
                    &self.config.right_column_prefix,
                )?);
            } else {
                return Err(BackendError::Other(
                    "join: emit before both sides have produced a batch".into(),
                ));
            }
        }
        let schema = out_schema.as_ref().unwrap().clone();
        let left_schema = self.left_schema.get().unwrap().clone();
        let right_schema = self.right_schema.get().unwrap().clone();
        drop(out_schema);
        drop(state);

        let batch = build_emit_batch(&schema, &left_schema, &right_schema, &emitted_rows)?;
        Ok(vec![batch])
    }
}

#[async_trait]
impl BatchTransform for TimeWindowedJoinTransform {
    fn input_schema(&self) -> SchemaRef {
        // Joins consume two distinct schemas; the abstract
        // "input_schema" exposed via the trait is the left side
        // when known, else an empty placeholder. The pipeline
        // doesn't validate against this for join transforms.
        self.left_schema
            .get()
            .cloned()
            .unwrap_or_else(|| Arc::new(Schema::empty()))
    }

    fn output_schema(&self) -> SchemaRef {
        // Best-effort: cached output once both sides seen, else
        // an empty placeholder. The target validates after first
        // emit.
        match self.output_schema.try_lock() {
            Ok(g) => g
                .as_ref()
                .cloned()
                .unwrap_or_else(|| Arc::new(Schema::empty())),
            Err(_) => Arc::new(Schema::empty()),
        }
    }

    async fn transform(
        &self,
        input: RecordBatch,
        ctx: &BatchContext,
    ) -> Result<Vec<RecordBatch>, BackendError> {
        let source_id = ctx.source_id.as_deref().ok_or_else(|| {
            BackendError::Other(
                "join transform requires `BatchContext::source_id` — pipeline must \
                 dispatch per-source batches"
                    .into(),
            )
        })?;
        let side = if source_id == self.config.left_source {
            Side::Left
        } else if source_id == self.config.right_source {
            Side::Right
        } else {
            return Err(BackendError::Other(format!(
                "join: batch from source `{source_id}` doesn't match \
                 left=`{}` or right=`{}`",
                self.config.left_source, self.config.right_source
            )));
        };
        self.ingest_side(side, input, ctx.global_wm).await
    }

    async fn on_idle_tick(&self, ctx: &BatchContext) -> Result<Vec<RecordBatch>, BackendError> {
        // Idle ticks advance the eviction watermark. Under outer-
        // join kinds, eviction surfaces orphan emits (left/right
        // rows that never matched); package them as a batch.
        // Inner-join idle ticks return empty.
        let mut state = self.state.lock().await;
        let orphans = self.evict_state(&mut state, ctx.global_wm);
        if orphans.is_empty() {
            return Ok(Vec::new());
        }
        let mut out_schema = self.output_schema.lock().await;
        if out_schema.is_none() {
            let l = self.left_schema.get();
            let r = self.right_schema.get();
            if let (Some(ls), Some(rs)) = (l, r) {
                *out_schema = Some(build_output_schema(
                    ls,
                    rs,
                    &self.config.left_column_prefix,
                    &self.config.right_column_prefix,
                )?);
            } else {
                // Both sides haven't produced a batch yet; can't
                // build the schema. Drop the orphans silently —
                // this only happens on a brand-new pipeline that
                // hasn't seen any input on at least one side.
                return Ok(Vec::new());
            }
        }
        let schema = out_schema.as_ref().unwrap().clone();
        let left_schema = self.left_schema.get().unwrap().clone();
        let right_schema = self.right_schema.get().unwrap().clone();
        drop(out_schema);
        drop(state);
        let batch = build_emit_batch(&schema, &left_schema, &right_schema, &orphans)?;
        Ok(vec![batch])
    }

    /// Phase 39.5b PR 2: drain dirty left/right buffers + evicted
    /// keys into encoded `(state_key, blob)` rows for the state
    /// store. Side identity is folded into the encoded key via
    /// the `L\0` / `R\0` prefix.
    async fn take_state_commit(
        &self,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, Vec<Vec<u8>>), BackendError> {
        let mut state = self.state.lock().await;
        let dirty_left: Vec<GroupKey> = state.dirty_left_keys.drain().collect();
        let dirty_right: Vec<GroupKey> = state.dirty_right_keys.drain().collect();
        let evicted_left: Vec<GroupKey> = state.evicted_left_keys.drain().collect();
        let evicted_right: Vec<GroupKey> = state.evicted_right_keys.drain().collect();

        let mut upserts: Vec<(Vec<u8>, Vec<u8>)> =
            Vec::with_capacity(dirty_left.len() + dirty_right.len());
        for key in dirty_left {
            let rows: &[BufferedRow] = state.left.get(&key).map(|v| v.as_slice()).unwrap_or(&[]);
            if rows.is_empty() {
                // Race between dirty + eviction — emit as delete.
                upserts.push((encode_state_key(Side::Left, &key)?, Vec::new()));
                continue;
            }
            upserts.push((encode_state_key(Side::Left, &key)?, encode_buffer(rows)?));
        }
        for key in dirty_right {
            let rows: &[BufferedRow] = state.right.get(&key).map(|v| v.as_slice()).unwrap_or(&[]);
            if rows.is_empty() {
                upserts.push((encode_state_key(Side::Right, &key)?, Vec::new()));
                continue;
            }
            upserts.push((encode_state_key(Side::Right, &key)?, encode_buffer(rows)?));
        }

        let mut deletes: Vec<Vec<u8>> =
            Vec::with_capacity(evicted_left.len() + evicted_right.len());
        for key in evicted_left {
            deletes.push(encode_state_key(Side::Left, &key)?);
        }
        for key in evicted_right {
            deletes.push(encode_state_key(Side::Right, &key)?);
        }

        Ok((upserts, deletes))
    }

    /// Phase 39.5b PR 2: install recovered left/right buffers
    /// from an opaque-bytes `state_by_key` map. Decodes each
    /// entry's side prefix, places rows into the matching buffer.
    /// No-op when no entries match the join's key shape.
    async fn recover_state(
        &self,
        state_by_key: &std::collections::HashMap<Vec<u8>, Vec<u8>>,
    ) -> Result<(), BackendError> {
        let mut state = self.state.lock().await;
        for (key_bytes, blob) in state_by_key {
            let (side, key) = decode_state_key(key_bytes)?;
            let rows = decode_buffer(blob)?;
            if rows.is_empty() {
                continue;
            }
            match side {
                Side::Left => {
                    state.left.insert(key, rows);
                }
                Side::Right => {
                    state.right.insert(key, rows);
                }
            }
        }
        // Recovery wipes any stale dirty markers — the in-memory
        // state matches the store after this call.
        state.dirty_left_keys.clear();
        state.dirty_right_keys.clear();
        state.evicted_left_keys.clear();
        state.evicted_right_keys.clear();
        Ok(())
    }
}

// ---------------------------------------------------------------------
// Output batch builder
// ---------------------------------------------------------------------

/// One row of join output. Inner-join emits always have both
/// sides populated. Outer-join orphan emits leave the unmatched
/// side as `None`, which `build_emit_batch` materializes as a
/// row of NULLs for that side's columns.
///
/// Sides are `Arc<Vec<ScalarValue>>` references back to the
/// `BufferedRow` they came from — a cheap `Arc::clone` per
/// emitted row instead of a deep `Vec<ScalarValue>` clone (P3 #22).
#[derive(Debug, Clone)]
struct EmittedRow {
    left: Option<Arc<Vec<ScalarValue>>>,
    right: Option<Arc<Vec<ScalarValue>>>,
}

fn build_output_schema(
    left: &SchemaRef,
    right: &SchemaRef,
    left_prefix: &str,
    right_prefix: &str,
) -> Result<SchemaRef, BackendError> {
    let mut fields: Vec<Field> = Vec::with_capacity(left.fields().len() + right.fields().len());
    for f in left.fields() {
        fields.push(Field::new(
            format!("{left_prefix}{}", f.name()),
            f.data_type().clone(),
            true,
        ));
    }
    for f in right.fields() {
        fields.push(Field::new(
            format!("{right_prefix}{}", f.name()),
            f.data_type().clone(),
            true,
        ));
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn build_emit_batch(
    out_schema: &SchemaRef,
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
    rows: &[EmittedRow],
) -> Result<RecordBatch, BackendError> {
    let n = rows.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(out_schema.fields().len());
    // Phase 39.5b P2.12: Outer-join orphan rows have None for one
    // side; materialize as `ScalarValue::Null` for that side's
    // columns. The output schema already marks every field
    // nullable, so this Just Works.
    let null = ScalarValue::Null;
    for (col_idx, f) in left_schema.fields().iter().enumerate() {
        let scalars: Vec<&ScalarValue> = rows
            .iter()
            .map(|r| match &r.left {
                Some(vals) => &vals[col_idx],
                None => &null,
            })
            .collect();
        columns.push(scalars_to_array(&scalars, f.data_type(), n)?);
    }
    for (col_idx, f) in right_schema.fields().iter().enumerate() {
        let scalars: Vec<&ScalarValue> = rows
            .iter()
            .map(|r| match &r.right {
                Some(vals) => &vals[col_idx],
                None => &null,
            })
            .collect();
        columns.push(scalars_to_array(&scalars, f.data_type(), n)?);
    }
    RecordBatch::try_new(out_schema.clone(), columns)
        .map_err(|e| BackendError::Other(format!("join: emit batch: {e}")))
}

fn extract_scalar(arr: &ArrayRef, row: usize) -> Result<ScalarValue, BackendError> {
    if arr.is_null(row) {
        return Ok(ScalarValue::Null);
    }
    use arrow_array::types::*;
    Ok(match arr.data_type() {
        DataType::Int8 => ScalarValue::Int64(arr.as_primitive::<Int8Type>().value(row) as i64),
        DataType::Int16 => ScalarValue::Int64(arr.as_primitive::<Int16Type>().value(row) as i64),
        DataType::Int32 => ScalarValue::Int64(arr.as_primitive::<Int32Type>().value(row) as i64),
        DataType::Int64 => ScalarValue::Int64(arr.as_primitive::<Int64Type>().value(row)),
        DataType::UInt8 => ScalarValue::UInt64(arr.as_primitive::<UInt8Type>().value(row) as u64),
        DataType::UInt16 => ScalarValue::UInt64(arr.as_primitive::<UInt16Type>().value(row) as u64),
        DataType::UInt32 => ScalarValue::UInt64(arr.as_primitive::<UInt32Type>().value(row) as u64),
        DataType::UInt64 => ScalarValue::UInt64(arr.as_primitive::<UInt64Type>().value(row)),
        DataType::Float32 => ScalarValue::Float64Bits(
            (arr.as_primitive::<Float32Type>().value(row) as f64).to_bits(),
        ),
        DataType::Float64 => {
            ScalarValue::Float64Bits(arr.as_primitive::<Float64Type>().value(row).to_bits())
        }
        DataType::Utf8 => ScalarValue::Utf8(arr.as_string::<i32>().value(row).to_string()),
        DataType::LargeUtf8 => ScalarValue::Utf8(arr.as_string::<i64>().value(row).to_string()),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            ScalarValue::TsMicros(arr.as_primitive::<TimestampMicrosecondType>().value(row))
        }
        DataType::Boolean => ScalarValue::Bool(arr.as_boolean().value(row)),
        other => {
            return Err(BackendError::Other(format!(
                "join: unsupported column type {other:?} (extend `extract_scalar` to add it)"
            )));
        }
    })
}

fn scalars_to_array(
    scalars: &[&ScalarValue],
    out_type: &DataType,
    _n: usize,
) -> Result<ArrayRef, BackendError> {
    use arrow_array::builder::*;
    use arrow_array::*;
    match out_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let mut b = Int64Array::builder(scalars.len());
            for s in scalars {
                match s {
                    ScalarValue::Null => b.append_null(),
                    ScalarValue::Int64(v) => b.append_value(*v),
                    ScalarValue::UInt64(v) => b.append_value(*v as i64),
                    other => return Err(mismatch("Int64", other)),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            let mut b = UInt64Array::builder(scalars.len());
            for s in scalars {
                match s {
                    ScalarValue::Null => b.append_null(),
                    ScalarValue::UInt64(v) => b.append_value(*v),
                    ScalarValue::Int64(v) if *v >= 0 => b.append_value(*v as u64),
                    other => return Err(mismatch("UInt64", other)),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float32 | DataType::Float64 => {
            let mut b = Float64Array::builder(scalars.len());
            for s in scalars {
                match s {
                    ScalarValue::Null => b.append_null(),
                    ScalarValue::Float64Bits(bits) => b.append_value(f64::from_bits(*bits)),
                    other => return Err(mismatch("Float64", other)),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::new();
            for s in scalars {
                match s {
                    ScalarValue::Null => b.append_null(),
                    ScalarValue::Utf8(v) => b.append_value(v.as_str()),
                    other => return Err(mismatch("Utf8", other)),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::LargeUtf8 => {
            let mut b = LargeStringBuilder::new();
            for s in scalars {
                match s {
                    ScalarValue::Null => b.append_null(),
                    ScalarValue::Utf8(v) => b.append_value(v.as_str()),
                    other => return Err(mismatch("LargeUtf8", other)),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let mut b = TimestampMicrosecondArray::builder(scalars.len());
            for s in scalars {
                match s {
                    ScalarValue::Null => b.append_null(),
                    ScalarValue::TsMicros(v) => b.append_value(*v),
                    other => return Err(mismatch("Timestamp", other)),
                }
            }
            let arr = b.finish();
            Ok(Arc::new(match tz {
                Some(t) => arr.with_timezone(t.as_ref()),
                None => arr,
            }))
        }
        DataType::Boolean => {
            let mut b = BooleanBuilder::new();
            for s in scalars {
                match s {
                    ScalarValue::Null => b.append_null(),
                    ScalarValue::Bool(v) => b.append_value(*v),
                    other => return Err(mismatch("Boolean", other)),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        other => Err(BackendError::Other(format!(
            "join: output type {other:?} not supported"
        ))),
    }
}

fn mismatch(expected: &str, actual: &ScalarValue) -> BackendError {
    BackendError::Other(format!(
        "join: scalar type mismatch (expected {expected}, got {actual:?})"
    ))
}

// Re-use windowed's GroupKey::from_row API for buffer keying. The
// type already supports the broad set of key value types we need.
// `KeyValue` is brought into the use list so the `extract_key`
// reference compiles.
#[allow(dead_code)]
fn _kv_anchor(_v: KeyValue) {}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray, TimestampMicrosecondArray};

    fn schema_left() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("amount", DataType::Int64, true),
            Field::new(
                "_event_ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
        ]))
    }

    fn schema_right() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("status", DataType::Utf8, true),
            Field::new(
                "_event_ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
        ]))
    }

    fn left_batch(orders: Vec<i64>, amounts: Vec<Option<i64>>, ts: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            schema_left(),
            vec![
                Arc::new(Int64Array::from(orders)),
                Arc::new(Int64Array::from(amounts)),
                Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC")),
            ],
        )
        .unwrap()
    }

    fn right_batch(orders: Vec<i64>, statuses: Vec<Option<&str>>, ts: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            schema_right(),
            vec![
                Arc::new(Int64Array::from(orders)),
                Arc::new(StringArray::from(statuses)),
                Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC")),
            ],
        )
        .unwrap()
    }

    fn cfg() -> JoinConfig {
        JoinConfig {
            kind: JoinKind::Inner,
            left_source: "orders".into(),
            right_source: "payments".into(),
            left_keys: vec!["order_id".into()],
            right_keys: vec!["order_id".into()],
            time_window_ms: 5,
            min_delta_ms: None,
            max_delta_ms: None,
            event_time_column: "_event_ts".into(),
            late_data: JoinLateDataPolicy::Drop,
            left_column_prefix: "left_".into(),
            right_column_prefix: "right_".into(),
        }
    }

    #[test]
    fn validate_rejects_empty_keys() {
        let mut c = cfg();
        c.left_keys = Vec::new();
        assert!(c.validate().unwrap_err().to_string().contains("non-empty"));
    }

    #[test]
    fn validate_rejects_unequal_key_lengths() {
        let mut c = cfg();
        c.right_keys = vec!["a".into(), "b".into()];
        assert!(
            c.validate()
                .unwrap_err()
                .to_string()
                .contains("equal length")
        );
    }

    // ----- P2.12 outer-join tests -----

    /// LEFT OUTER: left rows that never match emit a NULL-padded
    /// row at eviction. Inner-join (default) drops them silently.
    #[tokio::test(flavor = "multi_thread")]
    async fn left_outer_emits_orphan_left_at_eviction() {
        let mut c = cfg();
        c.kind = JoinKind::LeftOuter;
        let t = TimeWindowedJoinTransform::new(c).unwrap();

        // Left at ts=1000 never matches any right row.
        let lb = left_batch(vec![1], vec![Some(50)], vec![1_000]);
        let _ = t
            .transform(
                lb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("orders".into()),
                },
            )
            .await
            .unwrap();
        // Drive a right batch (no match by key) to populate
        // right_schema; the left buffer doesn't evict yet.
        let rb = right_batch(vec![999], vec![Some("x")], vec![999_000_000]);
        let _ = t
            .transform(
                rb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("payments".into()),
                },
            )
            .await
            .unwrap();
        // Idle tick advances wm past the left horizon
        // (1000 + 5_000 = 6_000us). With wm=20_000 the left row
        // evicts; under LEFT OUTER it emits NULL-padded.
        let out = t
            .on_idle_tick(&BatchContext {
                global_wm: Some(20_000),
                source_id: None,
            })
            .await
            .unwrap();
        assert_eq!(out.len(), 1, "orphan left row should emit at eviction");
        let b = &out[0];
        assert_eq!(b.num_rows(), 1);
        // left_order_id is populated (50 = the original).
        let left_amount = b.column_by_name("left_amount").unwrap();
        assert!(!left_amount.is_null(0), "left side carries data");
        // right_status is NULL (no match).
        let right_status = b.column_by_name("right_status").unwrap();
        assert!(right_status.is_null(0), "right side null on orphan left");
    }

    /// RIGHT OUTER: right rows that never match emit at eviction.
    #[tokio::test(flavor = "multi_thread")]
    async fn right_outer_emits_orphan_right_at_eviction() {
        let mut c = cfg();
        c.kind = JoinKind::RightOuter;
        let t = TimeWindowedJoinTransform::new(c).unwrap();

        // Schema-warm-up the left side via a batch with a key that
        // never matches the upcoming right row.
        let lb = left_batch(vec![999], vec![Some(0)], vec![999_000_000]);
        let _ = t
            .transform(
                lb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("orders".into()),
                },
            )
            .await
            .unwrap();
        let rb = right_batch(vec![1], vec![Some("paid")], vec![1_000]);
        let _ = t
            .transform(
                rb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("payments".into()),
                },
            )
            .await
            .unwrap();
        let out = t
            .on_idle_tick(&BatchContext {
                global_wm: Some(20_000),
                source_id: None,
            })
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        let b = &out[0];
        // Find the orphan right row (the never-matched key=999 left
        // could also evict, but LEFT_OUTER isn't set so it drops).
        // Under RIGHT_OUTER only the right orphan emits.
        let mut right_orphans = 0;
        let mut left_orphans = 0;
        let left_amount = b.column_by_name("left_amount").unwrap();
        let right_status = b.column_by_name("right_status").unwrap();
        for i in 0..b.num_rows() {
            if !left_amount.is_null(i) && right_status.is_null(i) {
                left_orphans += 1;
            } else if left_amount.is_null(i) && !right_status.is_null(i) {
                right_orphans += 1;
            }
        }
        assert_eq!(left_orphans, 0, "RIGHT_OUTER drops left orphans");
        assert_eq!(right_orphans, 1, "RIGHT_OUTER emits right orphan");
    }

    /// FULL OUTER: both sides emit orphans at eviction.
    #[tokio::test(flavor = "multi_thread")]
    async fn full_outer_emits_both_orphans() {
        let mut c = cfg();
        c.kind = JoinKind::FullOuter;
        let t = TimeWindowedJoinTransform::new(c).unwrap();

        let lb = left_batch(vec![1], vec![Some(50)], vec![1_000]);
        let _ = t
            .transform(
                lb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("orders".into()),
                },
            )
            .await
            .unwrap();
        let rb = right_batch(vec![999], vec![Some("x")], vec![1_000]);
        let _ = t
            .transform(
                rb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("payments".into()),
                },
            )
            .await
            .unwrap();
        let out = t
            .on_idle_tick(&BatchContext {
                global_wm: Some(20_000),
                source_id: None,
            })
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        let b = &out[0];
        assert_eq!(b.num_rows(), 2, "FULL_OUTER emits both orphans");
    }

    /// Matched rows under LEFT OUTER do NOT also emit as orphans
    /// at eviction — `matched: bool` flips on first match.
    #[tokio::test(flavor = "multi_thread")]
    async fn left_outer_matched_row_does_not_re_emit_at_eviction() {
        let mut c = cfg();
        c.kind = JoinKind::LeftOuter;
        let t = TimeWindowedJoinTransform::new(c).unwrap();

        let lb = left_batch(vec![1], vec![Some(50)], vec![1_000]);
        let _ = t
            .transform(
                lb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("orders".into()),
                },
            )
            .await
            .unwrap();
        // Right matches (1ms apart, in symmetric 5ms window).
        let rb = right_batch(vec![1], vec![Some("paid")], vec![2_000]);
        let out = t
            .transform(
                rb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("payments".into()),
                },
            )
            .await
            .unwrap();
        assert_eq!(out.len(), 1, "match emits");

        // Idle tick after horizon — left buffer evicts. Should NOT
        // emit an orphan because the row has matched=true.
        let out = t
            .on_idle_tick(&BatchContext {
                global_wm: Some(20_000),
                source_id: None,
            })
            .await
            .unwrap();
        assert!(out.is_empty(), "matched left does not orphan-emit");
    }

    // ----- P2.13 Reopen tests -----

    /// Under Drop: a late right row past its retention horizon is
    /// rejected at ingest. Under Reopen: with sufficient
    /// `allowed_lateness_ms` it's admitted and matches the still-
    /// retained left buffer.
    #[tokio::test(flavor = "multi_thread")]
    async fn reopen_admits_late_row_within_budget() {
        let mut c = cfg();
        c.late_data = JoinLateDataPolicy::Reopen {
            allowed_lateness_ms: 100, // 100ms = 100_000us
        };
        let t = TimeWindowedJoinTransform::new(c).unwrap();

        // Left at ts=1_000us. With time_window=5ms, normal right
        // horizon is R - (-5ms) = R + 5ms, plus lateness 100ms =
        // 105ms total.
        let lb = left_batch(vec![1], vec![Some(50)], vec![1_000]);
        let _ = t
            .transform(
                lb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("orders".into()),
                },
            )
            .await
            .unwrap();

        // Right at ts=2_000us, but wm has advanced to 50_000us.
        // Under Drop: wm > 2_000 + 5_000 → 50_000 > 7_000 → drop.
        // Under Reopen: wm > 2_000 + 5_000 + 100_000 → 50_000 vs
        // 107_000 → admit.
        let rb = right_batch(vec![1], vec![Some("paid")], vec![2_000]);
        let out = t
            .transform(
                rb,
                &BatchContext {
                    global_wm: Some(50_000),
                    source_id: Some("payments".into()),
                },
            )
            .await
            .unwrap();
        assert_eq!(out.len(), 1, "Reopen retains buffer + admits late row");
    }

    /// Reopen with lateness=0 collapses to Drop semantics — late
    /// rows past the original horizon still drop.
    #[tokio::test(flavor = "multi_thread")]
    async fn reopen_with_zero_lateness_matches_drop() {
        let mut c = cfg();
        c.late_data = JoinLateDataPolicy::Reopen {
            allowed_lateness_ms: 0,
        };
        let t = TimeWindowedJoinTransform::new(c).unwrap();
        let lb = left_batch(vec![1], vec![Some(50)], vec![1_000]);
        let _ = t
            .transform(
                lb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("orders".into()),
                },
            )
            .await
            .unwrap();
        // wm past horizon: 50_000 > 2_000+5_000=7_000.
        let rb = right_batch(vec![1], vec![Some("paid")], vec![2_000]);
        let out = t
            .transform(
                rb,
                &BatchContext {
                    global_wm: Some(50_000),
                    source_id: Some("payments".into()),
                },
            )
            .await
            .unwrap();
        assert!(out.is_empty(), "lateness=0 → Drop-equivalent");
    }

    // ----- P2.14 asymmetric window tests -----

    #[test]
    fn validate_rejects_only_one_asymmetric_bound() {
        let mut c = cfg();
        c.min_delta_ms = Some(-100);
        // max_delta_ms left None
        let err = c.validate().unwrap_err();
        assert!(err.to_string().contains("set together"), "got: {err}");
    }

    #[test]
    fn validate_rejects_inverted_asymmetric_bounds() {
        let mut c = cfg();
        c.min_delta_ms = Some(100);
        c.max_delta_ms = Some(-100);
        let err = c.validate().unwrap_err();
        assert!(err.to_string().contains("must be <"), "got: {err}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn asymmetric_window_admits_only_right_after_left() {
        // Constraint: right must arrive 1–10ms AFTER left (in event-
        // time). dt = r - l ∈ [1, 10] ms = [1000, 10000] us.
        let mut c = cfg();
        c.time_window_ms = 0; // ignored under asymmetric
        c.min_delta_ms = Some(1);
        c.max_delta_ms = Some(10);
        let t = TimeWindowedJoinTransform::new(c).unwrap();

        // Left at ts=1000, right at ts=2000. dt = 1000us = 1ms (=min). Match.
        let lb = left_batch(vec![1], vec![Some(50)], vec![1_000]);
        let _ = t
            .transform(
                lb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("orders".into()),
                },
            )
            .await
            .unwrap();
        let rb = right_batch(vec![1], vec![Some("paid")], vec![2_000]);
        let out = t
            .transform(
                rb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("payments".into()),
                },
            )
            .await
            .unwrap();
        assert_eq!(out.len(), 1, "right 1ms after left → match");

        // New attempt: right BEFORE left (negative dt). Should NOT match.
        let lb = left_batch(vec![2], vec![Some(50)], vec![10_000]);
        let _ = t
            .transform(
                lb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("orders".into()),
                },
            )
            .await
            .unwrap();
        let rb = right_batch(vec![2], vec![Some("paid")], vec![5_000]);
        let out = t
            .transform(
                rb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("payments".into()),
                },
            )
            .await
            .unwrap();
        assert!(
            out.is_empty(),
            "right BEFORE left rejected by asymmetric window"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn asymmetric_window_max_boundary_inclusive() {
        let mut c = cfg();
        c.time_window_ms = 0;
        c.min_delta_ms = Some(0);
        c.max_delta_ms = Some(5);
        let t = TimeWindowedJoinTransform::new(c).unwrap();

        // dt = 5ms exactly. At max bound. Match.
        let lb = left_batch(vec![1], vec![Some(0)], vec![1_000]);
        let _ = t
            .transform(
                lb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("orders".into()),
                },
            )
            .await
            .unwrap();
        let rb = right_batch(vec![1], vec![Some("paid")], vec![6_000]);
        let out = t
            .transform(
                rb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("payments".into()),
                },
            )
            .await
            .unwrap();
        assert_eq!(out.len(), 1);

        // dt = 5001us (> 5ms). Out of range. No match.
        let lb = left_batch(vec![2], vec![Some(0)], vec![1_000]);
        let _ = t
            .transform(
                lb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("orders".into()),
                },
            )
            .await
            .unwrap();
        let rb = right_batch(vec![2], vec![Some("paid")], vec![6_001]);
        let out = t
            .transform(
                rb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("payments".into()),
                },
            )
            .await
            .unwrap();
        assert!(out.is_empty(), "dt=5.001ms exceeds 5ms max");
    }

    #[test]
    fn validate_rejects_zero_time_window() {
        let mut c = cfg();
        c.time_window_ms = 0;
        assert!(
            c.validate()
                .unwrap_err()
                .to_string()
                .contains("time_window_ms")
        );
    }

    #[test]
    fn validate_rejects_same_source_id_on_both_sides() {
        let mut c = cfg();
        c.right_source = "orders".into();
        assert!(c.validate().unwrap_err().to_string().contains("differ"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn left_then_right_within_window_emits_one_row() {
        let t = TimeWindowedJoinTransform::new(cfg()).unwrap();
        // Left arrives first.
        let lb = left_batch(vec![1], vec![Some(100)], vec![1_000]);
        let out = t
            .transform(
                lb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("orders".into()),
                },
            )
            .await
            .unwrap();
        assert!(out.is_empty(), "no right side yet → no match");

        // Right arrives within window.
        let rb = right_batch(vec![1], vec![Some("paid")], vec![3_000]);
        let out = t
            .transform(
                rb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("payments".into()),
                },
            )
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        let b = &out[0];
        assert_eq!(b.num_rows(), 1);
        let schema = b.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(names.contains(&"left_order_id"));
        assert!(names.contains(&"right_status"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn right_then_left_emits_same() {
        let t = TimeWindowedJoinTransform::new(cfg()).unwrap();
        let rb = right_batch(vec![7], vec![Some("paid")], vec![1_000]);
        let _ = t
            .transform(
                rb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("payments".into()),
                },
            )
            .await
            .unwrap();
        let lb = left_batch(vec![7], vec![Some(50)], vec![3_000]);
        let out = t
            .transform(
                lb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("orders".into()),
                },
            )
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        // Output is left-then-right regardless of arrival order.
        let order_id = out[0]
            .column_by_name("left_order_id")
            .unwrap()
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0);
        assert_eq!(order_id, 7);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn beyond_time_window_no_emit() {
        let t = TimeWindowedJoinTransform::new(cfg()).unwrap();
        // 5ms window = 5_000us; events 100_000us apart.
        let lb = left_batch(vec![1], vec![Some(100)], vec![0]);
        let _ = t
            .transform(
                lb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("orders".into()),
                },
            )
            .await
            .unwrap();
        let rb = right_batch(vec![1], vec![Some("paid")], vec![100_000]);
        let out = t
            .transform(
                rb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("payments".into()),
                },
            )
            .await
            .unwrap();
        assert!(out.is_empty(), "100ms apart > 5ms window → no match");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn key_mismatch_no_emit() {
        let t = TimeWindowedJoinTransform::new(cfg()).unwrap();
        let lb = left_batch(vec![1], vec![Some(100)], vec![1_000]);
        let _ = t
            .transform(
                lb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("orders".into()),
                },
            )
            .await
            .unwrap();
        let rb = right_batch(vec![2], vec![Some("paid")], vec![1_000]);
        let out = t
            .transform(
                rb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("payments".into()),
                },
            )
            .await
            .unwrap();
        assert!(out.is_empty(), "different order_id → no match");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn one_to_many_join_emits_one_per_match() {
        let t = TimeWindowedJoinTransform::new(cfg()).unwrap();
        // One left row. Three right rows for the same key, all in window.
        let lb = left_batch(vec![1], vec![Some(100)], vec![1_000]);
        let _ = t
            .transform(
                lb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("orders".into()),
                },
            )
            .await
            .unwrap();
        let rb = right_batch(
            vec![1, 1, 1],
            vec![Some("paid"), Some("refunded"), Some("disputed")],
            vec![2_000, 3_000, 4_000],
        );
        let out = t
            .transform(
                rb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("payments".into()),
                },
            )
            .await
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn watermark_evicts_stale_buffer() {
        let t = TimeWindowedJoinTransform::new(cfg()).unwrap();
        let lb = left_batch(vec![1], vec![Some(100)], vec![1_000]);
        let _ = t
            .transform(
                lb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("orders".into()),
                },
            )
            .await
            .unwrap();
        // Right arrives much later — wm advanced past the left row's
        // retention deadline (1_000 + 5_000 = 6_000us). wm=10_000.
        let rb = right_batch(vec![1], vec![Some("paid")], vec![20_000]);
        let out = t
            .transform(
                rb,
                &BatchContext {
                    global_wm: Some(10_000),
                    source_id: Some("payments".into()),
                },
            )
            .await
            .unwrap();
        // Left was evicted before the right batch processed; no match.
        assert!(out.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rejects_unknown_source_id() {
        let t = TimeWindowedJoinTransform::new(cfg()).unwrap();
        let lb = left_batch(vec![1], vec![Some(100)], vec![1_000]);
        let err = t
            .transform(
                lb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("bogus".into()),
                },
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("doesn't match"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rejects_missing_source_id() {
        let t = TimeWindowedJoinTransform::new(cfg()).unwrap();
        let lb = left_batch(vec![1], vec![Some(100)], vec![1_000]);
        let err = t
            .transform(
                lb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: None,
                },
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("source_id"));
    }

    // ----- PR 2: state commit + recovery -----

    #[tokio::test(flavor = "multi_thread")]
    async fn take_state_commit_returns_dirty_keys_per_side() {
        let t = TimeWindowedJoinTransform::new(cfg()).unwrap();
        let lb = left_batch(vec![1, 2], vec![Some(10), Some(20)], vec![1_000, 1_000]);
        let _ = t
            .transform(
                lb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("orders".into()),
                },
            )
            .await
            .unwrap();
        let rb = right_batch(vec![1], vec![Some("paid")], vec![2_000]);
        let _ = t
            .transform(
                rb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("payments".into()),
                },
            )
            .await
            .unwrap();

        let (upserts, deletes) = t.take_state_commit().await.unwrap();
        // 2 left + 1 right = 3 upserts.
        assert_eq!(upserts.len(), 3);
        assert!(deletes.is_empty());

        // Verify side prefixes round-trip and bytes decode.
        let mut left_keys = 0;
        let mut right_keys = 0;
        for (key_bytes, blob_bytes) in &upserts {
            let (side, _key) = decode_state_key(key_bytes).unwrap();
            match side {
                Side::Left => left_keys += 1,
                Side::Right => right_keys += 1,
            }
            let rows = decode_buffer(blob_bytes).unwrap();
            assert!(!rows.is_empty());
        }
        assert_eq!(left_keys, 2);
        assert_eq!(right_keys, 1);

        // Subsequent drain returns nothing — sets are cleared.
        let (u2, d2) = t.take_state_commit().await.unwrap();
        assert!(u2.is_empty() && d2.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn watermark_eviction_marks_keys_for_delete() {
        let t = TimeWindowedJoinTransform::new(cfg()).unwrap();
        let lb = left_batch(vec![1], vec![Some(10)], vec![1_000]);
        let _ = t
            .transform(
                lb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("orders".into()),
                },
            )
            .await
            .unwrap();
        // Drain initial dirty.
        let (_u, _d) = t.take_state_commit().await.unwrap();

        // Idle tick advances wm past retention deadline (1_000 + 5_000us).
        let _ = t
            .on_idle_tick(&BatchContext {
                global_wm: Some(20_000),
                source_id: None,
            })
            .await
            .unwrap();

        let (upserts, deletes) = t.take_state_commit().await.unwrap();
        assert!(upserts.is_empty());
        assert_eq!(deletes.len(), 1, "evicted key emits a delete");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recover_state_restores_buffers() {
        // Pipeline 1: ingest a left row, capture upserts.
        let t1 = TimeWindowedJoinTransform::new(cfg()).unwrap();
        let lb = left_batch(vec![42], vec![Some(99)], vec![5_000]);
        let _ = t1
            .transform(
                lb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("orders".into()),
                },
            )
            .await
            .unwrap();
        let (upserts, _) = t1.take_state_commit().await.unwrap();

        // Pipeline 2: recover. After restart, each source replays
        // from its committed offset; the test simulates that by
        // feeding one schema-nudge batch on each side. The
        // recovered left buffer contains the real row from t1.
        let t2 = TimeWindowedJoinTransform::new(cfg()).unwrap();
        let recovered: std::collections::HashMap<Vec<u8>, Vec<u8>> = upserts.into_iter().collect();
        t2.recover_state(&recovered).await.unwrap();

        // Schema-nudge: one left row far out of window so it
        // doesn't match anything but populates left_schema.
        let nudge = left_batch(vec![999], vec![Some(0)], vec![1_000_000_000]);
        let _ = t2
            .transform(
                nudge,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("orders".into()),
                },
            )
            .await
            .unwrap();

        let rb = right_batch(vec![42], vec![Some("paid")], vec![6_000]);
        let out = t2
            .transform(
                rb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("payments".into()),
                },
            )
            .await
            .unwrap();
        assert_eq!(out.len(), 1, "right joined against recovered left");
        assert_eq!(out[0].num_rows(), 1);
        let amount = out[0]
            .column_by_name("left_amount")
            .unwrap()
            .as_primitive::<arrow_array::types::Int64Type>()
            .value(0);
        assert_eq!(amount, 99);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn buffer_blob_postcard_roundtrip() {
        let rows = vec![
            BufferedRow {
                event_ts: 1234,
                values: Arc::new(vec![
                    ScalarValue::Int64(7),
                    ScalarValue::Utf8("hello".into()),
                    ScalarValue::Null,
                ]),
                matched: false,
            },
            BufferedRow {
                event_ts: 5678,
                values: Arc::new(vec![ScalarValue::Float64Bits(1.5_f64.to_bits())]),
                matched: true,
            },
        ];
        let bytes = encode_buffer(&rows).unwrap();
        let back = decode_buffer(&bytes).unwrap();
        assert_eq!(back.len(), 2);
        assert_eq!(back[0].event_ts, 1234);
        match &back[0].values[1] {
            ScalarValue::Utf8(s) => assert_eq!(s, "hello"),
            _ => panic!(),
        }
    }

    #[test]
    fn state_key_side_roundtrip() {
        let key = GroupKey::from_values(vec![KeyValue::Int64(1)]);
        for side in [Side::Left, Side::Right] {
            let bytes = encode_state_key(side, &key).unwrap();
            let (back_side, back_key) = decode_state_key(&bytes).unwrap();
            assert_eq!(back_side, side);
            assert_eq!(back_key, key);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn late_row_drops_under_drop_policy() {
        let t = TimeWindowedJoinTransform::new(cfg()).unwrap();
        let lb = left_batch(vec![1], vec![Some(100)], vec![1_000]);
        let _ = t
            .transform(
                lb,
                &BatchContext {
                    global_wm: Some(0),
                    source_id: Some("orders".into()),
                },
            )
            .await
            .unwrap();
        // Late: wm=20_000, right at ts=2_000. Late budget for the
        // right row is 2_000 + 5_000 = 7_000; wm > budget → drop.
        let rb = right_batch(vec![1], vec![Some("paid")], vec![2_000]);
        let out = t
            .transform(
                rb,
                &BatchContext {
                    global_wm: Some(20_000),
                    source_id: Some("payments".into()),
                },
            )
            .await
            .unwrap();
        assert!(out.is_empty(), "right row past lateness budget dropped");
    }
}
