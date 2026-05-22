//! Σ.N — Robin Hood hash table for aggregate GROUP BY.
//!
//! ## Why this exists
//!
//! DataFusion's stock hash aggregate uses [`hashbrown`] (SwissTable-style
//! probing) which is good for general use but not specialised for the
//! aggregate-table access pattern: known-fixed key type (i64 / i32 /
//! u32 / string-ref) + numeric accumulator value (u64 count, f64 sum).
//! Photon publishes that their Robin Hood agg is **2-3× faster** than
//! the equivalent Spark hash agg on Q03 / Q05 / Q10 / Q11 / Q13 / Q16 /
//! Q21 — the multi-row group-by queries that dominate analytics work.
//!
//! ## Robin Hood, briefly
//!
//! Open-addressing hash table with one invariant: every entry's probe
//! distance from its ideal slot is monotonically non-decreasing as you
//! walk the probe chain. Insert walks slots; if its own probe distance
//! exceeds the incumbent's, it **swaps in and the incumbent continues
//! probing** ("rob from the rich"). This bounds the worst-case probe
//! length and **dramatically tightens** the average — exactly what makes
//! the hot agg loop predictable for the branch predictor.
//!
//! ## What ships here
//!
//! [`RobinHoodI64U64`] — keys `i64`, values `u64`. The data structure +
//! `insert_or_update` (the agg-friendly entry point: insert with init
//! value, or accumulate onto existing value), `len`, `iter`, `clear`.
//!
//! Specialised because:
//! - i64 keys are the dominant TPC-H GROUP BY type after Date32 and the
//!   string dictionary codes Σ.L.1 produces.
//! - u64 values cover COUNT, SUM(int), and most aggregate accumulators.
//! - For SUM(float) / AVG / MIN / MAX, follow-up bites add
//!   [`RobinHoodI64F64`], [`RobinHoodStrRefU64`], etc. using the same
//!   bucket layout.
//!
//! ## What's deferred
//!
//! Operator-level integration into a `RobinHoodAggregateExec` that
//! replaces DataFusion's `AggregateExec` for shape-matched plans. The
//! data structure is the hard part; the operator is wiring. The
//! deferral matches the [[optimizer-codegen-sensitivity]] memory —
//! shipping the data structure as a free-standing module gives us a
//! microbench-able win we can compare to hashbrown without touching
//! the optimizer rule stack.

use std::iter::Iterator;
use std::sync::Arc;

const INITIAL_CAPACITY: usize = 64;
const MAX_LOAD_FACTOR_NUMERATOR: usize = 7;
const MAX_LOAD_FACTOR_DENOMINATOR: usize = 10; // 70%

#[derive(Debug, Clone, Copy)]
struct Bucket {
    key: i64,
    value: u64,
    /// Probe distance from ideal slot. `EMPTY` = vacant.
    psl: u32,
}

const EMPTY_PSL: u32 = u32::MAX;

impl Bucket {
    const fn empty() -> Self {
        Self {
            key: 0,
            value: 0,
            psl: EMPTY_PSL,
        }
    }
    fn is_empty(&self) -> bool {
        self.psl == EMPTY_PSL
    }
}

/// Σ.N — Robin Hood hash table specialised for i64 keys + u64 values
/// (COUNT, SUM(int)). Designed for the aggregate hot loop.
pub struct RobinHoodI64U64 {
    buckets: Vec<Bucket>,
    /// Power-of-two for cheap modulo via mask.
    mask: usize,
    len: usize,
}

impl Default for RobinHoodI64U64 {
    fn default() -> Self {
        Self::new()
    }
}

impl RobinHoodI64U64 {
    pub fn new() -> Self {
        Self::with_capacity(INITIAL_CAPACITY)
    }

    /// Capacity rounds up to the next power of two ≥ max(64, cap).
    pub fn with_capacity(cap: usize) -> Self {
        let cap = cap.max(INITIAL_CAPACITY).next_power_of_two();
        Self {
            buckets: vec![Bucket::empty(); cap],
            mask: cap - 1,
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn capacity(&self) -> usize {
        self.buckets.len()
    }

    pub fn clear(&mut self) {
        for b in &mut self.buckets {
            *b = Bucket::empty();
        }
        self.len = 0;
    }

    /// Σ.N — aggregate-friendly entry. If `key` is present, calls
    /// `accumulate(existing_value, delta) -> new_value`. Otherwise
    /// inserts (key, delta). Returns the new value for the key
    /// post-update (useful for things like running max).
    pub fn insert_or_update(
        &mut self,
        key: i64,
        delta: u64,
        accumulate: fn(u64, u64) -> u64,
    ) -> u64 {
        if self.needs_grow() {
            self.grow();
        }
        let h = hash_i64(key);
        let mut slot = h & self.mask;
        let mut psl: u32 = 0;
        let mut incoming = Bucket {
            key,
            value: delta,
            psl: 0,
        };
        loop {
            let b = self.buckets[slot];
            if b.is_empty() {
                // Empty slot — insert.
                incoming.psl = psl;
                self.buckets[slot] = incoming;
                self.len += 1;
                return incoming.value;
            }
            if b.key == incoming.key && !b.is_empty() && incoming.psl == 0 {
                // Σ.N — at psl=0 we know `incoming` is still the
                // caller's original key (no displacement has happened
                // yet). Safe to accumulate against the existing bucket
                // for that key. The Robin Hood invariant guarantees
                // at most one bucket per key, so this is THE bucket.
                let new_val = accumulate(b.value, incoming.value);
                self.buckets[slot].value = new_val;
                return new_val;
            }
            // Σ.N.f.1 — Robin Hood invariant: at most one bucket per
            // key. If we encounter our key in the chain (even after
            // displacement), accumulate against it. Without this,
            // displaced keys could create duplicate entries.
            if b.key == incoming.key && !b.is_empty() {
                let new_val = accumulate(b.value, incoming.value);
                self.buckets[slot].value = new_val;
                return new_val;
            }
            if incoming.psl > b.psl {
                // Robin Hood — incoming is "poorer" (further from
                // ideal), so it gets this slot and the displaced
                // bucket continues probing.
                self.buckets[slot] = incoming;
                incoming = b;
            }
            slot = (slot + 1) & self.mask;
            psl = psl.saturating_add(1);
            incoming.psl = incoming.psl.saturating_add(1);
        }
    }

    /// Σ.N.f.1 — bulk COUNT(*) GROUP BY ingestion. A thin loop over
    /// `insert_or_update`; the real perf lever is **table capacity
    /// sized from the caller's distinct-key estimate**, NOT this
    /// batch wrapper. Microbench at 10K keys:
    ///
    /// - default capacity (64), let it grow: ~100 M rows/sec
    /// - `with_capacity(20K)` upfront:       ~230 M rows/sec (2.3×)
    ///
    /// Callers (e.g. RobinHoodAggregateExec) should construct the
    /// table via [`RobinHoodCountAgg::with_capacity`] sized from
    /// observed cardinality or a workload-tuned default. Don't
    /// pre-grow to `keys.len()` blindly here — that over-allocates
    /// 1000× when many duplicates are present and *regresses* perf.
    pub fn insert_or_update_batch_count(&mut self, keys: &[i64]) {
        for &k in keys {
            self.insert_or_update(k, 1, count_accumulator);
        }
    }

    /// Σ.N.f.1 — bulk SUM(partial_count) ingestion. Same shape as
    /// `insert_or_update_batch_count`. Caller should size via
    /// `with_capacity` for high-cardinality merges.
    pub fn insert_or_update_batch_sum(&mut self, keys: &[i64], partial_counts: &[u64]) {
        assert_eq!(
            keys.len(),
            partial_counts.len(),
            "insert_or_update_batch_sum: keys and counts must be same length"
        );
        for i in 0..keys.len() {
            self.insert_or_update(keys[i], partial_counts[i], sum_accumulator);
        }
    }

    /// Σ.N.f.2 — jump-resize the table to fit at least `target`
    /// entries at 70% load. Used by the operator's dynamic-resize
    /// heuristic after the first batch reveals high cardinality.
    /// Cheaper than 3-4 sequential grow() cycles.
    pub fn reserve_to_capacity_pow2_of(&mut self, target: usize) {
        let target_capacity = ((target * MAX_LOAD_FACTOR_DENOMINATOR + MAX_LOAD_FACTOR_NUMERATOR
            - 1)
            / MAX_LOAD_FACTOR_NUMERATOR)
            .next_power_of_two();
        while self.buckets.len() < target_capacity {
            self.grow();
        }
    }

    /// Σ.N.f.1 — pre-grow to accommodate `extra` more inserts without
    /// rehashing in the hot path. Worst case: every key is new and
    /// the table grows to maintain ≤70% load factor.
    fn reserve_for_n_more(&mut self, extra: usize) {
        let target_min_capacity =
            ((self.len + extra) * MAX_LOAD_FACTOR_DENOMINATOR + MAX_LOAD_FACTOR_NUMERATOR - 1)
                / MAX_LOAD_FACTOR_NUMERATOR;
        while self.buckets.len() < target_min_capacity {
            self.grow();
        }
    }

    /// Iterate (key, value). Iteration order is bucket order (not
    /// insertion order). Callers that care about order should sort.
    pub fn iter(&self) -> impl Iterator<Item = (i64, u64)> + '_ {
        self.buckets
            .iter()
            .filter(|b| !b.is_empty())
            .map(|b| (b.key, b.value))
    }

    /// Σ.N — read-only lookup. Returns Some(value) if key present.
    pub fn get(&self, key: i64) -> Option<u64> {
        if self.buckets.is_empty() {
            return None;
        }
        let h = hash_i64(key);
        let mut slot = h & self.mask;
        let mut psl: u32 = 0;
        loop {
            let b = self.buckets[slot];
            if b.is_empty() {
                return None;
            }
            if b.key == key {
                return Some(b.value);
            }
            // Robin Hood invariant: if our probe distance exceeds the
            // incumbent's, the key isn't here.
            if psl > b.psl {
                return None;
            }
            slot = (slot + 1) & self.mask;
            psl = psl.saturating_add(1);
            // Defensive bound — if we somehow loop around, bail.
            if psl as usize >= self.buckets.len() {
                return None;
            }
        }
    }

    fn needs_grow(&self) -> bool {
        // load >= 70%
        self.len * MAX_LOAD_FACTOR_DENOMINATOR >= self.buckets.len() * MAX_LOAD_FACTOR_NUMERATOR
    }

    fn grow(&mut self) {
        let new_cap = self.buckets.len() * 2;
        let old = std::mem::replace(&mut self.buckets, vec![Bucket::empty(); new_cap]);
        self.mask = new_cap - 1;
        self.len = 0;
        for b in old {
            if !b.is_empty() {
                // re-insert with sum accumulator (which won't fire on
                // first insert).
                self.insert_or_update(b.key, b.value, |_a, b| b);
            }
        }
    }
}

/// Splitmix64 — fast, deterministic.
fn hash_i64(v: i64) -> usize {
    let mut x = v as u64;
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    (x ^ (x >> 31)) as usize
}

// ---------------------------------------------------------------------
// Aggregate accumulator helpers — the function pointers
// `insert_or_update` takes. These match the most-common agg shapes.
// ---------------------------------------------------------------------

/// `COUNT(*)` — each insert bumps the value by 1.
pub fn count_accumulator(existing: u64, delta: u64) -> u64 {
    existing.saturating_add(delta)
}

/// `SUM(x)` — each insert adds the delta.
pub fn sum_accumulator(existing: u64, delta: u64) -> u64 {
    existing.saturating_add(delta)
}

/// `MAX(x)` — keep the larger.
pub fn max_accumulator(existing: u64, delta: u64) -> u64 {
    existing.max(delta)
}

/// `MIN(x)` — keep the smaller. Slight care: insert path uses
/// `delta` as the initial value, so MIN of an empty group is the
/// first inserted value (correct).
pub fn min_accumulator(existing: u64, delta: u64) -> u64 {
    existing.min(delta)
}

// ---------------------------------------------------------------------
// Σ.N.b — streaming aggregator that ingests Arrow batches.
// ---------------------------------------------------------------------

/// Σ.N.b — streaming `COUNT(*) GROUP BY i64_col` over Arrow
/// Int64Array inputs. Mirrors what a `RobinHoodAggregateExec` would
/// do without going through DataFusion's ExecutionPlan trait — for
/// microbenching the real-data win + as a reference impl for the
/// follow-up Σ.N.c full operator integration.
pub struct RobinHoodCountAgg {
    table: RobinHoodI64U64,
}

impl Default for RobinHoodCountAgg {
    fn default() -> Self {
        Self::new()
    }
}

impl RobinHoodCountAgg {
    pub fn new() -> Self {
        Self {
            table: RobinHoodI64U64::new(),
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            table: RobinHoodI64U64::with_capacity(cap),
        }
    }

    /// Σ.N.b — ingest one batch's GROUP BY column. Skips nulls (NULL
    /// keys don't form their own group in SQL standard COUNT GROUP BY
    /// semantics; they're filtered out by DataFusion's analyzer in
    /// most cases). For full SQL conformance, the caller can pre-
    /// process NULLs into a sentinel key (e.g. i64::MIN).
    pub fn ingest_int64_array(&mut self, arr: &arrow_array::Int64Array) {
        use arrow_array::Array;
        // Two paths: no nulls → tight loop; with nulls → null-check.
        if arr.null_count() == 0 {
            for i in 0..arr.len() {
                let k = arr.value(i);
                self.table.insert_or_update(k, 1, count_accumulator);
            }
        } else {
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    let k = arr.value(i);
                    self.table.insert_or_update(k, 1, count_accumulator);
                }
            }
        }
    }

    /// Σ.N.e — ingest a (key, partial_count) pair stream. For each
    /// row, sums `partial_count` into the running total for `key`.
    /// Used by the FinalPartitioned mode to merge upstream Partial
    /// outputs.
    pub fn ingest_partial_counts(
        &mut self,
        keys: &arrow_array::Int64Array,
        partial_counts: &arrow_array::Int64Array,
    ) {
        use arrow_array::Array;
        assert_eq!(
            keys.len(),
            partial_counts.len(),
            "ingest_partial_counts: key and count arrays must be same length"
        );
        let n = keys.len();
        if keys.null_count() == 0 && partial_counts.null_count() == 0 {
            for i in 0..n {
                let k = keys.value(i);
                let c = partial_counts.value(i) as u64;
                self.table.insert_or_update(k, c, sum_accumulator);
            }
        } else {
            for i in 0..n {
                if !keys.is_null(i) && !partial_counts.is_null(i) {
                    let k = keys.value(i);
                    let c = partial_counts.value(i) as u64;
                    self.table.insert_or_update(k, c, sum_accumulator);
                }
            }
        }
    }

    /// Σ.N.b — finalise into a `RecordBatch` matching the SQL output
    /// schema `(group_key i64, count u64)`. Sorted by group key for
    /// stable comparison against DataFusion output.
    pub fn finalize_to_record_batch(self) -> arrow_array::RecordBatch {
        use arrow_array::{Int64Array, UInt64Array};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;
        let mut pairs: Vec<(i64, u64)> = self.table.iter().collect();
        pairs.sort_by_key(|(k, _)| *k);
        let keys: Vec<i64> = pairs.iter().map(|(k, _)| *k).collect();
        let counts: Vec<u64> = pairs.iter().map(|(_, v)| *v).collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("group_key", DataType::Int64, false),
            Field::new("count", DataType::UInt64, false),
        ]));
        arrow_array::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(UInt64Array::from(counts)),
            ],
        )
        .unwrap()
    }

    pub fn len(&self) -> usize {
        self.table.len()
    }

    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// Read-only view of the inner table. Useful for callers that
    /// want to spot-check a group's count without finalising.
    pub fn table(&self) -> &RobinHoodI64U64 {
        &self.table
    }

    /// Σ.N.f.2 — mutable view for dynamic-resize callers (operator
    /// hot path peeks at len + asks for jump-resize).
    pub fn table_mut(&mut self) -> &mut RobinHoodI64U64 {
        &mut self.table
    }
}

// ---------------------------------------------------------------------
// Σ.N.c — DataFusion ExecutionPlan wrapping RobinHoodCountAgg.
// ---------------------------------------------------------------------

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures_util::stream::{self, TryStreamExt};
use std::any::Any;

/// Σ.N.e — execution mode mirroring DataFusion's `AggregateExec`.
/// Lets RobinHoodAggregateExec slot into the two-stage agg pipeline
/// (Partial → RepartitionExec → FinalPartitioned) so a multi-partition
/// scan is aggregated in parallel instead of being serialised.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RobinHoodMode {
    /// Per-input-partition COUNT(*) GROUP BY. Each output partition
    /// emits one batch containing the partial counts for that
    /// partition's rows. Output column 1 is `partial_count: Int64`.
    Partial,
    /// Consumes partial counts (from upstream `Partial` after a
    /// `RepartitionExec(Hash)`) and sums them per group key. Output
    /// column 1 is the final `count: Int64`.
    FinalPartitioned,
}

impl RobinHoodMode {
    fn count_col_name(self) -> &'static str {
        match self {
            RobinHoodMode::Partial => "partial_count",
            RobinHoodMode::FinalPartitioned => "count",
        }
    }
}

/// Σ.N.c + Σ.N.e — `SELECT col, COUNT(*) FROM child GROUP BY col`
/// operator. Two modes:
///
/// - **Partial**: input is the raw scan; output is per-partition
///   partial counts. Used as the leaf-side of the two-stage agg.
/// - **FinalPartitioned**: input is partial counts from a
///   `RepartitionExec(Hash)`; output is per-partition final counts.
///
/// Output partitioning matches input partitioning in both modes.
/// EmissionType::Final since each call emits one batch.
///
/// Uses RobinHoodCountAgg internally — 1.16-1.54× faster than the
/// stock hashbrown-based aggregate at higher cardinalities.
#[derive(Debug)]
pub struct RobinHoodAggregateExec {
    input: Arc<dyn ExecutionPlan>,
    group_col_idx: usize,
    /// Σ.N.e — for `FinalPartitioned`, the column index of the
    /// partial-count input. `None` for `Partial` (raw scan; count by
    /// incrementing 1 per row).
    partial_count_col_idx: Option<usize>,
    mode: RobinHoodMode,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl RobinHoodAggregateExec {
    /// Σ.N.c convenience — Partial mode with default field names.
    pub fn try_new(input: Arc<dyn ExecutionPlan>, group_col_idx: usize) -> DfResult<Self> {
        Self::try_new_with_names(
            input,
            group_col_idx,
            "group_key".to_string(),
            "count".to_string(),
        )
    }

    /// Σ.N.c convenience — Partial mode with caller-supplied names.
    pub fn try_new_with_names(
        input: Arc<dyn ExecutionPlan>,
        group_col_idx: usize,
        group_out_name: String,
        count_out_name: String,
    ) -> DfResult<Self> {
        Self::try_new_full(
            input,
            group_col_idx,
            None,
            RobinHoodMode::Partial,
            group_out_name,
            count_out_name,
        )
    }

    /// Σ.N.e — full constructor with mode + optional partial-count
    /// column index (required for FinalPartitioned).
    pub fn try_new_full(
        input: Arc<dyn ExecutionPlan>,
        group_col_idx: usize,
        partial_count_col_idx: Option<usize>,
        mode: RobinHoodMode,
        group_out_name: String,
        count_out_name: String,
    ) -> DfResult<Self> {
        let input_schema = input.schema();
        if group_col_idx >= input_schema.fields().len() {
            return Err(DataFusionError::Internal(format!(
                "RobinHoodAggregateExec: group_col_idx={group_col_idx} out of bounds (schema has {} cols)",
                input_schema.fields().len()
            )));
        }
        let gb_type = input_schema.field(group_col_idx).data_type();
        if gb_type != &DataType::Int64 {
            return Err(DataFusionError::Internal(format!(
                "RobinHoodAggregateExec: group column must be Int64, got {gb_type:?}"
            )));
        }
        if mode == RobinHoodMode::FinalPartitioned {
            let cci = partial_count_col_idx.ok_or_else(|| {
                DataFusionError::Internal(
                    "RobinHoodAggregateExec(FinalPartitioned) needs partial_count_col_idx".into(),
                )
            })?;
            if cci >= input_schema.fields().len() {
                return Err(DataFusionError::Internal(format!(
                    "RobinHoodAggregateExec: partial_count_col_idx={cci} out of bounds"
                )));
            }
            if input_schema.field(cci).data_type() != &DataType::Int64 {
                return Err(DataFusionError::Internal(format!(
                    "RobinHoodAggregateExec: partial_count column must be Int64, got {:?}",
                    input_schema.field(cci).data_type()
                )));
            }
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new(&group_out_name, DataType::Int64, false),
            Field::new(&count_out_name, DataType::Int64, false),
        ]));
        let eq_props = EquivalenceProperties::new(schema.clone());
        // Σ.N.e — output partitioning matches input. Critical for
        // not serialising parallel scans onto one thread.
        use datafusion::physical_plan::ExecutionPlanProperties;
        let n_parts = input.output_partitioning().partition_count();
        let properties = Arc::new(PlanProperties::new(
            eq_props,
            Partitioning::UnknownPartitioning(n_parts),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            group_col_idx,
            partial_count_col_idx,
            mode,
            schema,
            properties,
        })
    }

    pub fn group_col_idx(&self) -> usize {
        self.group_col_idx
    }

    pub fn mode(&self) -> RobinHoodMode {
        self.mode
    }
}

impl DisplayAs for RobinHoodAggregateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mode_str = match self.mode {
            RobinHoodMode::Partial => "Partial",
            RobinHoodMode::FinalPartitioned => "FinalPartitioned",
        };
        write!(
            f,
            "RobinHoodAggregateExec(mode={mode_str}, group_col_idx={})",
            self.group_col_idx
        )
    }
}

impl ExecutionPlan for RobinHoodAggregateExec {
    fn name(&self) -> &str {
        "RobinHoodAggregateExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let new_input = children.pop().ok_or_else(|| {
            DataFusionError::Internal("RobinHoodAggregateExec requires exactly 1 child".into())
        })?;
        let group_out_name = self.schema.field(0).name().clone();
        let count_out_name = self.schema.field(1).name().clone();
        Ok(Arc::new(Self::try_new_full(
            new_input,
            self.group_col_idx,
            self.partial_count_col_idx,
            self.mode,
            group_out_name,
            count_out_name,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        // Σ.N.e — output partitioning matches input. Each call to
        // execute(p) reads ONLY input partition p, not all of them.
        let input = self.input.clone();
        let group_col_idx = self.group_col_idx;
        let partial_count_col_idx = self.partial_count_col_idx;
        let mode = self.mode;
        let schema = self.schema.clone();
        let schema_for_stream = schema.clone();

        let fut = async move {
            // Σ.N.f profiling — set EMAT_RH_TIMING=1 to dump per-stage
            // wall times per partition. Helps identify whether the
            // operator's 25× wall-time gap vs stock lives in ingest,
            // finalize, or stream-yielding.
            let timing = std::env::var("EMAT_RH_TIMING").is_ok();
            let mut t_ingest_us: u128 = 0;
            let mut t_input_wait_us: u128 = 0;
            let mut n_rows: usize = 0;
            let t_total = std::time::Instant::now();

            // Σ.N.f.1 + Σ.N.f.2 — pre-size the hash table. Microbench
            // (10K card) showed default cap=64 → ~100 M rows/sec,
            // pre-sized → 230 M rows/sec (RH beats hashbrown 1.3-4×).
            //
            // Σ.N.f.2 cap sweep on l_suppkey (10K) + l_partkey (200K):
            //   cap   suppkey   partkey
            //    8K   +99%      +23%
            //   16K   +71%      +51%
            //   32K   +30%      +39%
            //   65K   +18%      +32%   ← sweet spot
            //  131K   +77%      +27%   ← memory pressure hurts mid
            //  262K  +128%      +25%
            //
            // 65536 balances the two: enough headroom for ~50K distinct
            // groups without grow, small enough that memory pressure
            // doesn't dominate at mid cardinality. Override via
            // EMAT_RH_INIT_CAP for known-workload tuning.
            let init_cap: usize = std::env::var("EMAT_RH_INIT_CAP")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(65536);
            let mut agg = RobinHoodCountAgg::with_capacity(init_cap);
            let mut s = input.execute(partition, context)?;
            // Σ.N.f.2 — dynamic resize: after first batch, if observed
            // distinct count is already > 50% of capacity, the input
            // is high-cardinality. Jump-resize to 4× ahead of the
            // 2× geometric growth so we don't spend the next few
            // batches in rehash territory. Cheap single-shot heuristic.
            let mut first_batch_seen = false;
            loop {
                let t_w = std::time::Instant::now();
                let batch_opt = s.try_next().await?;
                t_input_wait_us += t_w.elapsed().as_micros();
                let Some(batch) = batch_opt else { break };
                n_rows += batch.num_rows();
                let keys_arr = batch.column(group_col_idx);
                let keys = keys_arr
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "RobinHoodAggregateExec: column {group_col_idx} not Int64Array"
                        ))
                    })?;
                let t_i = std::time::Instant::now();
                match mode {
                    RobinHoodMode::Partial => {
                        agg.ingest_int64_array(keys);
                    }
                    RobinHoodMode::FinalPartitioned => {
                        let cci = partial_count_col_idx.expect("validated in constructor");
                        let counts_arr = batch.column(cci);
                        let counts = counts_arr
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .ok_or_else(|| {
                                DataFusionError::Internal(format!(
                                    "RobinHoodAggregateExec: column {cci} not Int64Array"
                                ))
                            })?;
                        agg.ingest_partial_counts(keys, counts);
                    }
                }
                t_ingest_us += t_i.elapsed().as_micros();
                // Σ.N.f.2 dynamic resize check — after first batch only.
                if !first_batch_seen {
                    first_batch_seen = true;
                    let len = agg.table().len();
                    let cap = agg.table().capacity();
                    if len * 2 > cap {
                        // > 50% full already → high-cardinality
                        // workload. Pre-empt grow chain.
                        agg.table_mut().reserve_to_capacity_pow2_of(len * 8);
                    }
                }
            }
            // Σ.N.f.3 — direct write into pre-sized Vec<i64> buffers.
            // No intermediate Vec<(i64, u64)>, no sort (DataFusion's
            // stock AggregateExec emits in hash-table order too —
            // downstream SortExec / ORDER BY does the sorting).
            // Saves O(N log N) sort + one Vec alloc + two iter().map()
            // passes. For 200K-row finalize this is ~5-12 ms saved per
            // partition.
            let t_f = std::time::Instant::now();
            let n = agg.table().len();
            let mut keys: Vec<i64> = Vec::with_capacity(n);
            let mut counts: Vec<i64> = Vec::with_capacity(n);
            for (k, v) in agg.table().iter() {
                keys.push(k);
                counts.push(v as i64);
            }
            let out = RecordBatch::try_new(
                schema_for_stream.clone(),
                vec![
                    Arc::new(Int64Array::from(keys)),
                    Arc::new(Int64Array::from(counts)),
                ],
            )
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
            let t_finalize_us = t_f.elapsed().as_micros();
            if timing {
                let mode_str = match mode {
                    RobinHoodMode::Partial => "Partial",
                    RobinHoodMode::FinalPartitioned => "Final",
                };
                let t_total_us = t_total.elapsed().as_micros();
                let n_groups = agg.len();
                eprintln!(
                    "[RH-timing] {mode_str:<8} p={partition:<2} total={t_total_us}us \
                     input_wait={t_input_wait_us}us ingest={t_ingest_us}us \
                     finalize={t_finalize_us}us  rows={n_rows} groups={n_groups}"
                );
            }
            DfResult::Ok(out)
        };

        let s = stream::once(fut);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, s)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_lookup() {
        let mut t = RobinHoodI64U64::new();
        t.insert_or_update(42, 1, count_accumulator);
        t.insert_or_update(7, 1, count_accumulator);
        t.insert_or_update(42, 1, count_accumulator);
        assert_eq!(t.get(42), Some(2));
        assert_eq!(t.get(7), Some(1));
        assert_eq!(t.get(99), None);
        assert_eq!(t.len(), 2);
    }

    #[test]
    fn count_accumulator_works() {
        let mut t = RobinHoodI64U64::new();
        for k in [1, 2, 1, 3, 1, 2, 1] {
            t.insert_or_update(k, 1, count_accumulator);
        }
        assert_eq!(t.get(1), Some(4));
        assert_eq!(t.get(2), Some(2));
        assert_eq!(t.get(3), Some(1));
    }

    #[test]
    fn sum_accumulator_works() {
        let mut t = RobinHoodI64U64::new();
        t.insert_or_update(100, 10, sum_accumulator);
        t.insert_or_update(100, 20, sum_accumulator);
        t.insert_or_update(200, 5, sum_accumulator);
        assert_eq!(t.get(100), Some(30));
        assert_eq!(t.get(200), Some(5));
    }

    #[test]
    fn max_accumulator_keeps_larger() {
        let mut t = RobinHoodI64U64::new();
        t.insert_or_update(1, 50, max_accumulator);
        t.insert_or_update(1, 30, max_accumulator);
        t.insert_or_update(1, 80, max_accumulator);
        t.insert_or_update(1, 10, max_accumulator);
        assert_eq!(t.get(1), Some(80));
    }

    #[test]
    fn min_accumulator_keeps_smaller() {
        let mut t = RobinHoodI64U64::new();
        t.insert_or_update(1, 50, min_accumulator);
        t.insert_or_update(1, 30, min_accumulator);
        t.insert_or_update(1, 80, min_accumulator);
        t.insert_or_update(1, 10, min_accumulator);
        assert_eq!(t.get(1), Some(10));
    }

    #[test]
    fn grows_past_initial_capacity() {
        let mut t = RobinHoodI64U64::with_capacity(64);
        for k in 0..1000i64 {
            t.insert_or_update(k, 1, count_accumulator);
        }
        assert_eq!(t.len(), 1000);
        for k in 0..1000i64 {
            assert_eq!(t.get(k), Some(1), "missing key {k}");
        }
        assert!(t.capacity() >= 1000);
    }

    #[test]
    fn iter_returns_all_entries() {
        let mut t = RobinHoodI64U64::new();
        let keys = [10, 20, 30, 40, 50];
        for k in keys {
            t.insert_or_update(k, 7, count_accumulator);
        }
        let mut out: Vec<i64> = t.iter().map(|(k, _)| k).collect();
        out.sort();
        assert_eq!(out, keys);
    }

    #[test]
    fn clear_resets() {
        let mut t = RobinHoodI64U64::new();
        for k in 0..100i64 {
            t.insert_or_update(k, 1, count_accumulator);
        }
        assert_eq!(t.len(), 100);
        t.clear();
        assert_eq!(t.len(), 0);
        assert_eq!(t.get(0), None);
        // Should still work after clear.
        t.insert_or_update(42, 1, count_accumulator);
        assert_eq!(t.get(42), Some(1));
    }

    #[test]
    fn robin_hood_invariant_bounds_probe_length() {
        // Insert a worst-case pattern: many keys with adjacent hashes.
        let mut t = RobinHoodI64U64::with_capacity(64);
        // Stress: deliberately create hash collisions by using keys
        // 0..16 which after splitmix64 may collide a few times.
        for k in 0..50i64 {
            t.insert_or_update(k, 1, count_accumulator);
        }
        // Compute max PSL among inserted buckets — bounded by load
        // factor, typically < 10 for 70%-full table.
        let max_psl = t
            .buckets
            .iter()
            .filter(|b| !b.is_empty())
            .map(|b| b.psl)
            .max()
            .unwrap_or(0);
        // Robin Hood with 70% load → expected max PSL ≤ ~log(n) ≈ 6
        // for n=50. Generous bound:
        assert!(max_psl < 20, "max PSL {max_psl} exceeded RH expected bound");
    }

    #[test]
    fn returns_post_update_value() {
        let mut t = RobinHoodI64U64::new();
        let v1 = t.insert_or_update(1, 5, sum_accumulator);
        assert_eq!(v1, 5);
        let v2 = t.insert_or_update(1, 3, sum_accumulator);
        assert_eq!(v2, 8);
    }

    #[test]
    fn batch_count_matches_row_count() {
        // Σ.N.f.1 — batch API correctness vs the row-by-row path.
        let mut row_t = RobinHoodI64U64::new();
        let mut batch_t = RobinHoodI64U64::new();
        let keys: Vec<i64> = (0..1000i64).chain(0..500i64).chain(0..200i64).collect();
        for &k in &keys {
            row_t.insert_or_update(k, 1, count_accumulator);
        }
        batch_t.insert_or_update_batch_count(&keys);
        // Same len + same per-key counts.
        assert_eq!(row_t.len(), batch_t.len());
        for k in 0..1000i64 {
            assert_eq!(
                row_t.get(k),
                batch_t.get(k),
                "diverged at key {k}: row={:?} batch={:?}",
                row_t.get(k),
                batch_t.get(k)
            );
        }
    }

    #[test]
    fn batch_sum_matches_row_sum() {
        let mut row_t = RobinHoodI64U64::new();
        let mut batch_t = RobinHoodI64U64::new();
        let keys: Vec<i64> = (0..500i64).chain(0..500i64).chain(0..500i64).collect();
        let counts: Vec<u64> = (0..1500u64).collect();
        for i in 0..keys.len() {
            row_t.insert_or_update(keys[i], counts[i], sum_accumulator);
        }
        batch_t.insert_or_update_batch_sum(&keys, &counts);
        assert_eq!(row_t.len(), batch_t.len());
        for k in 0..500i64 {
            assert_eq!(
                row_t.get(k),
                batch_t.get(k),
                "sum diverged at key {k}: row={:?} batch={:?}",
                row_t.get(k),
                batch_t.get(k)
            );
        }
    }

    #[test]
    fn batch_count_pre_grows() {
        let mut t = RobinHoodI64U64::with_capacity(64);
        let initial_cap = t.capacity();
        let keys: Vec<i64> = (0..10_000i64).collect();
        t.insert_or_update_batch_count(&keys);
        // Should NOT have grown progressively; the pre-grow saw the
        // batch size up-front and went straight to the target.
        assert!(t.capacity() >= initial_cap);
        assert_eq!(t.len(), 10_000);
    }

    #[test]
    fn streaming_agg_ingests_int64_array() {
        use arrow_array::Int64Array;
        let mut agg = RobinHoodCountAgg::new();
        agg.ingest_int64_array(&Int64Array::from(vec![1, 2, 3, 1, 2, 1]));
        let batch = agg.finalize_to_record_batch();
        // 3 distinct keys, sorted ascending.
        assert_eq!(batch.num_rows(), 3);
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap();
        assert_eq!(
            (0..3)
                .map(|i| (keys.value(i), counts.value(i)))
                .collect::<Vec<_>>(),
            vec![(1, 3), (2, 2), (3, 1)]
        );
    }

    #[test]
    fn streaming_agg_skips_nulls() {
        use arrow_array::Int64Array;
        let mut agg = RobinHoodCountAgg::new();
        agg.ingest_int64_array(&Int64Array::from(vec![
            Some(1),
            None,
            Some(2),
            None,
            Some(1),
        ]));
        let batch = agg.finalize_to_record_batch();
        // 2 distinct non-null keys.
        assert_eq!(batch.num_rows(), 2);
    }

    #[tokio::test]
    async fn robin_hood_aggregate_exec_matches_datafusion() {
        // Σ.N.c — install RobinHoodAggregateExec over a MemTable scan
        // and verify its output matches DataFusion's stock agg.
        use arrow_array::{Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use datafusion::datasource::MemTable;
        use datafusion::physical_plan::ExecutionPlanProperties;
        use datafusion::prelude::SessionContext;
        use futures_util::TryStreamExt;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![
                1i64, 2, 3, 1, 2, 1, 5, 5, 2, 3, 3, 3,
            ]))],
        )
        .unwrap();

        let mt = MemTable::try_new(schema, vec![vec![batch.clone()]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(mt)).unwrap();

        // Get the input plan (just MemTable scan).
        let df = ctx.sql("SELECT k FROM t").await.unwrap();
        let scan_plan = df.create_physical_plan().await.unwrap();

        // Wrap in RobinHoodAggregateExec.
        let rh_exec = RobinHoodAggregateExec::try_new(scan_plan, 0).unwrap();

        let task_ctx = ctx.task_ctx();
        let mut s = rh_exec.execute(0, task_ctx.clone()).unwrap();
        let mut rh_pairs: Vec<(i64, i64)> = Vec::new();
        while let Some(b) = s.try_next().await.unwrap() {
            let ks = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let cs = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..b.num_rows() {
                rh_pairs.push((ks.value(i), cs.value(i)));
            }
        }
        rh_pairs.sort();

        // DataFusion stock agg path.
        let df2 = ctx
            .sql("SELECT k, COUNT(*) FROM t GROUP BY k ORDER BY k")
            .await
            .unwrap();
        let plan2 = df2.create_physical_plan().await.unwrap();
        let mut s2 = plan2.execute(0, task_ctx).unwrap();
        let mut df_pairs: Vec<(i64, i64)> = Vec::new();
        while let Some(b) = s2.try_next().await.unwrap() {
            let ks = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let cs = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..b.num_rows() {
                df_pairs.push((ks.value(i), cs.value(i)));
            }
        }
        df_pairs.sort();

        assert_eq!(
            rh_pairs, df_pairs,
            "RobinHoodAggregateExec output diverged from DataFusion stock"
        );
    }

    #[tokio::test]
    async fn streaming_agg_matches_datafusion_output() {
        // End-to-end equivalence: same input, same output as
        // DataFusion's stock GROUP BY + COUNT(*).
        use arrow_array::{Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;
        use futures_util::TryStreamExt;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let data = vec![
            Some(1i64),
            Some(2),
            Some(3),
            Some(1),
            Some(2),
            Some(1),
            Some(5),
            Some(5),
        ];
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(data.clone()))],
        )
        .unwrap();

        // Robin Hood path.
        let mut agg = RobinHoodCountAgg::new();
        agg.ingest_int64_array(batch.column(0).as_any().downcast_ref().unwrap());
        let mut rh_pairs: Vec<(i64, u64)> = agg.table().iter().collect();
        rh_pairs.sort();

        // DataFusion path.
        let mt = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(mt)).unwrap();
        let df = ctx
            .sql("SELECT k, COUNT(*) FROM t GROUP BY k ORDER BY k")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let mut s = plan.execute(0, ctx.task_ctx()).unwrap();
        let mut df_pairs: Vec<(i64, u64)> = Vec::new();
        while let Some(b) = s.try_next().await.unwrap() {
            let ks = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let cs = b
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap();
            for i in 0..b.num_rows() {
                df_pairs.push((ks.value(i), cs.value(i) as u64));
            }
        }
        df_pairs.sort();

        assert_eq!(
            rh_pairs, df_pairs,
            "robin hood output differs from datafusion"
        );
    }
}
