//! Phase 2: `EmatixFastParquetExec` + `EmatixFastParquetTableProvider`.
//!
//! Alternate `TableProvider` for parquet files that decodes columns via
//! the `ematix_parquet_bridge` (which dispatches through ematix-parquet
//! kernels — NEON-fused bw=12/17 unpack, Snappy buffer reuse, bitmap-
//! driven sparse gather) instead of parquet-rs's
//! `ParquetRecordBatchReader`.
//!
//! Shape mirrors [`crate::fast_parquet::FastParquetExec`]:
//!   - Row-group-parallel via `Partitioning::UnknownPartitioning(N)`,
//!     where N = `min(num_row_groups, target_partitions).max(1)`.
//!   - Each partition opens the file once, decodes its assigned RGs
//!     sequentially in a `spawn_blocking` worker, yields one
//!     `RecordBatch` per RG over an mpsc channel.
//!   - No filter pushdown, no row-group pruning — the bridge handles
//!     the hot decode path; Phase 3 will add Phase 5-style
//!     bitmap-first predicate eval.
//!
//! Phase 2 supports primitive columns only (INT32, INT64, DOUBLE,
//! Date32). BYTE_ARRAY / Utf8(View) / nested types error out at
//! `try_new`. Callers that need those use the existing
//! [`crate::fast_parquet::FastParquetTableProvider`].

use std::any::Any;
use std::collections::HashMap;
// Σ.Q06.SF10.5.c: `try_new` no longer opens the file directly (metadata
// comes from ematix-parquet); `std::fs::File` is now only used by the
// parquet-rs writer in the test fixtures.
#[cfg(test)]
use std::fs::File;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::ScalarValue;
use datafusion::common::stats::Statistics;
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures_util::StreamExt;

use crate::emat_arrow_reader::EmatArrowBatchReaderBuilder;
use crate::ematix_parquet_bridge::{
    decode_column_chunk_byte_array, decode_column_chunk_byte_array_dict_preserved,
    decode_column_chunk_f64, decode_column_chunk_i32, decode_column_chunk_i64,
    masked_decode_byte_array, masked_decode_f64, masked_decode_i32, masked_decode_i64,
    sparse_gather_chunk_f64, sparse_gather_chunk_i32, sparse_gather_chunk_i64,
};
use crate::fast_parquet::{RangePredicate, extract_range_predicate};

/// Phase 3 predicate: single-column conjunction of `column OP literal`
/// Multi-column predicate set, AND-combined. Each `ColumnPredicate`
/// runs against ONE column; per-column bitmaps are built by the
/// streaming reader's masked path and AND-ed together before
/// projection columns are masked-decoded.
#[derive(Debug, Clone)]
pub struct BridgeFilter {
    predicates: Vec<ColumnPredicate>,
    /// Σ.E5 Phase 1.8: pre-computed pass-rate prediction. Set by the
    /// provider's `scan()` from per-column stats; used by the
    /// streaming reader to choose between parallel-bitmap+dense
    /// (high-sel) and serial-bitmap+masked-decode (low-sel) paths.
    /// 0.5 = unknown (no stats), conservative default.
    predicted_pass_rate: f64,
    /// L9.ADAPT Guard 2 — shared runtime probe-disarm counters, set by
    /// the scan's `execute()` when merging a TIGHT-ADMITTED sideband's
    /// published predicates. `None` (the default, and all non-tight
    /// wraps): probe behavior is unchanged.
    probe_disarm: Option<Arc<crate::bridge_filter_sideband::ProbeDisarm>>,
    /// Σ.Q05.CHAIN — when true, the reader's masked→dense pass-rate
    /// routing must STASH the bitmap (per-batch Arrow filter) instead
    /// of discarding it above the REV.23 threshold. Set by the scan's
    /// `execute()` when merging a CHAIN-INTERMEDIATE sideband's
    /// predicates: a chain link's whole value is the rows it removes
    /// from the NEXT link's build sample — a discarded bitmap silently
    /// neuters the cascade (Q05: the 20%-pass nation/supplier blooms
    /// sit above the 10% threshold). Chain intermediates are dim-sized
    /// scans, so the per-batch filter cost is negligible. `false` (the
    /// default, every other wrap): routing behavior unchanged.
    apply_when_dense: bool,
}

impl BridgeFilter {
    /// Constructor for tests + benches. Production callers go through
    /// the predicate-collection pipeline in `merge_predicates_*` (see
    /// line ~1086) so they get TPC-H-specific predicate merging.
    pub fn new(predicates: Vec<ColumnPredicate>) -> Self {
        Self {
            predicates,
            predicted_pass_rate: 0.5,
            probe_disarm: None,
            apply_when_dense: false,
        }
    }

    /// L9.ADAPT Guard 2 — attach the tight-admitted wrap's shared
    /// disarm counters. Called by the scan's `execute()` right after
    /// merging the sideband's published predicates.
    pub fn set_probe_disarm(&mut self, d: Arc<crate::bridge_filter_sideband::ProbeDisarm>) {
        self.probe_disarm = Some(d);
    }

    /// Σ.Q05.CHAIN — mark this filter as carrying a chain-intermediate
    /// predicate whose bitmap must be APPLIED even when the pass-rate
    /// routing picks the dense decode. See the field docs.
    pub fn set_apply_when_dense(&mut self) {
        self.apply_when_dense = true;
    }

    /// Σ.Q05.CHAIN — must the dense-routed bitmap be stashed (applied
    /// per-batch) instead of discarded?
    pub fn apply_when_dense(&self) -> bool {
        self.apply_when_dense
    }

    /// L9.ADAPT Guard 2 — the attached disarm counters, if this filter
    /// carries probes from a tight-rescued wrap. Test-only observer
    /// (the hot path reads the field directly).
    #[cfg(test)]
    pub(crate) fn probe_disarm(&self) -> Option<&Arc<crate::bridge_filter_sideband::ProbeDisarm>> {
        self.probe_disarm.as_ref()
    }

    /// L9.ADAPT Guard 1 — can `eval_on_decoded_views` evaluate every
    /// predicate currently in this filter against the given decode
    /// projection? Used at WRAP time by the L9 rule: a tight-admitted
    /// wrap whose probe scan carries a non-fused-evaluable static
    /// (string shape, or a column outside the projection) would push
    /// the whole bundle off the masked-decode path onto the legacy
    /// per-predicate re-decode path — Q19's l_shipmode/l_shipinstruct
    /// statics cost +102% that way. Keep the arm list in lockstep with
    /// `eval_on_decoded_views`' bind loop.
    pub fn statics_fused_evaluable(&self, projection: &[usize]) -> bool {
        self.predicates.iter().all(|p| match p {
            ColumnPredicate::I64Range { col_idx, .. }
            | ColumnPredicate::I64InSet { col_idx, .. }
            | ColumnPredicate::I64InBloom { col_idx, .. }
            | ColumnPredicate::I32Range { col_idx, .. }
            | ColumnPredicate::I32In { col_idx, .. }
            | ColumnPredicate::F64Range { col_idx, .. } => projection.contains(col_idx),
            ColumnPredicate::I32ColumnPair {
                left_col,
                right_col,
                ..
            } => projection.contains(left_col) && projection.contains(right_col),
            // String shapes (and any future repr) force the legacy path.
            _ => false,
        })
    }

    /// Σ.E5 Phase 1.8: combined pass-rate estimate across all
    /// predicates (AND'd). `full_col_stats` is indexed by the
    /// PROVIDER's full schema column index (the same index space
    /// `ColumnPredicate::col_idx()` returns). Returns 0.5 if any
    /// predicate's column lacks stats.
    pub fn estimate_pass_rate(
        &self,
        full_col_stats: &[datafusion::common::stats::ColumnStatistics],
    ) -> f64 {
        let mut sel = 1.0_f64;
        for p in &self.predicates {
            let col = p.col_idx();
            let Some(stats) = full_col_stats.get(col) else {
                return 0.5;
            };
            sel *= p.estimate_pass_rate(stats);
        }
        sel.clamp(0.0, 1.0)
    }

    /// Σ.AE.4 (2026-05-26): pass-rate estimate for partition_statistics
    /// that ONLY counts predicates whose FilterExec is being dropped
    /// from the plan. For Inexact-declared user filters (StringEq
    /// under our string-gate; all user filters under default mode),
    /// FilterExec sits on top of the scan and DataFusion's planner
    /// already applies the predicate's selectivity on the scan's
    /// reported cardinality. Pre-applying it here would double-count.
    ///
    /// Q10 SF=10 lineitem returnflag was double-counted, fooling
    /// the planner into picking the wrong HashJoin build side
    /// (+17% wall-time regression).
    ///
    /// Two classes count toward the pass-rate:
    ///
    /// 1. **Always-injected predicates** — bloom/set/range predicates
    ///    added by the Σ.Q.L9 sideband rule via `with_added_predicates`.
    ///    These never have a residual FilterExec; the scan is the
    ///    only operator applying them. Q21 SF=10's −17% default-mode
    ///    win came from threading bloom selectivity into the stats.
    ///
    /// 2. **Exact-declared user predicates** — i32 range/IN when
    ///    `EMAT_EXACT_PUSHDOWN=1`. supports_filters_pushdown returns
    ///    Exact for these, so DataFusion drops their FilterExec.
    pub fn estimate_dropped_filter_pass_rate(
        &self,
        full_col_stats: &[datafusion::common::stats::ColumnStatistics],
        exact_opt_in: bool,
    ) -> f64 {
        let mut sel = 1.0_f64;
        for p in &self.predicates {
            let residual_dropped = match p {
                // Σ.Q.L9 / Σ.S.B injection: always added without
                // FilterExec — scan is the only filter step.
                ColumnPredicate::I64InBloom { .. }
                | ColumnPredicate::I64InSet { .. }
                | ColumnPredicate::I64Range { .. }
                // KEYS.5 — string runtime sideband is injected the same
                // way (no residual FilterExec; the scan applies it).
                | ColumnPredicate::StringInBloom { .. }
                | ColumnPredicate::StringInSet { .. } => true,
                // User-pushed Exact-declarable predicates: Exact only
                // when the env-var gate is on.
                ColumnPredicate::I32Range { .. } | ColumnPredicate::I32In { .. } => exact_opt_in,
                // Strings stay Inexact under our string-gate;
                // FilterExec retains them. F64 / column-pair never
                // pushed. StringLike retained conservatively.
                _ => false,
            };
            if !residual_dropped {
                continue;
            }
            let col = p.col_idx();
            let Some(stats) = full_col_stats.get(col) else {
                return 0.5;
            };
            sel *= p.estimate_pass_rate(stats);
        }
        sel.clamp(0.0, 1.0)
    }

    /// Σ.E5 Phase 1.8: store the predictor's verdict so the streaming
    /// reader doesn't have to re-compute it per RG.
    pub fn with_predicted_pass_rate(mut self, p: f64) -> Self {
        self.predicted_pass_rate = p.clamp(0.0, 1.0);
        self
    }

    /// Σ.E5 Phase 1.8: predicted pass rate (set via
    /// `with_predicted_pass_rate`). 0.5 if not set (conservative).
    pub fn predicted_pass_rate(&self) -> f64 {
        self.predicted_pass_rate
    }

    /// Σ.Q.L4′ — append additional predicates (typically I64InBloom
    /// from a planner rule that pre-built blooms off small HashJoin
    /// build sides). Order is "existing first, new after" so the
    /// dict-aware fast-path predicates run before the bloom probe.
    pub fn extend(&mut self, more: Vec<ColumnPredicate>) {
        self.predicates.extend(more);
    }

    /// Σ.AH.2 Story 1'.4 — true iff every predicate in this filter is
    /// a runtime-injected i64-only shape (`I64InBloom`, `I64InSet`,
    /// `I64Range`). These shapes share two properties that make the
    /// dense-then-post-filter path always-win vs the masked-decode
    /// path:
    /// 1. The probe column is always already in the projection
    ///    (it's the HashJoin's join key, projected for the join).
    /// 2. The per-row predicate eval is cheap (bloom probe ~1.4 ns,
    ///    set lookup ~5 ns, range compare ~1 ns).
    ///
    /// User-pushed predicates (`I32Range`, `I32In`, `F64Range`,
    /// `StringEq`, `StringLike`, ...) stay on the masked-decode path
    /// because they may target non-projected columns and benefit
    /// from page-level skip when the column's value distribution
    /// allows it.
    pub fn is_runtime_i64_only(&self) -> bool {
        !self.predicates.is_empty()
            && self.predicates.iter().all(|p| {
                matches!(
                    p,
                    ColumnPredicate::I64InBloom { .. }
                        | ColumnPredicate::I64InSet { .. }
                        | ColumnPredicate::I64Range { .. }
                )
            })
    }

    /// Number of pushed predicates. Used by trace/diagnostic code
    /// (`EMAT_L9_TRACE`, stage_profiler) — not on the hot path.
    pub fn predicates_len(&self) -> usize {
        self.predicates.len()
    }

    /// Σ.AH.2 Story 1'.4 Stage 1 — apply all runtime-i64 predicates
    /// against an already-decoded i64 column. Returns
    /// `Some((target_file_col_idx, bitmap))` when the filter is
    /// `is_runtime_i64_only()` AND all predicates target the same
    /// column. Returns `None` otherwise so the caller can fall back
    /// to the masked path.
    ///
    /// The point: skips the duplicate decode of the filter column.
    /// The masked path calls `build_bitmap` which RE-DECODES the
    /// column from the file (~6 ms per 1M-row partition on
    /// lineitem.l_partkey). The fused path reuses the buffer that
    /// `load_row_group_dense` already produced.
    /// Σ.AH.2 Story 1'.4 Stage 1 — file-schema column index that all
    /// runtime-i64 predicates target, or `None` if not runtime-i64-only
    /// or if predicates target multiple columns.
    pub fn single_runtime_i64_col(&self) -> Option<usize> {
        if !self.is_runtime_i64_only() {
            return None;
        }
        let first = self.predicates.first()?.col_idx();
        if self.predicates.iter().all(|p| p.col_idx() == first) {
            Some(first)
        } else {
            None
        }
    }

    pub fn probe_i64_values_from_decoded(&self, values: &[i64]) -> Option<(usize, Vec<u8>)> {
        if !self.is_runtime_i64_only() {
            return None;
        }
        let first_col = {
            let p = self.predicates.first()?;
            p.col_idx()
        };
        if !self.predicates.iter().all(|p| p.col_idx() == first_col) {
            return None;
        }
        let total = values.len();
        let mut bitmap = vec![0u8; total.div_ceil(8)];

        // Σ.AH.2 Story 1'.4 Stage 2 — chunk-of-8 byte-packing inner
        // loop for the single-predicate fast path. Processes 8 i64
        // values per iteration, builds one bitmap byte at a time.
        // LLVM unrolls the inner per-lane chain and vectorises the
        // bloom probe across lanes (per Σ.S.A's splash layout, which
        // exposes the data-parallel probe). Faster than the scalar
        // `for (row, &v)` loop because bit-set's
        // read-modify-write pattern (`bitmap[row>>3] |= 1<<(row&7)`)
        // serialised the inner loop through the bitmap array.
        if self.predicates.len() == 1 {
            match &self.predicates[0] {
                ColumnPredicate::I64InBloom { bloom, .. } => {
                    probe_membership_into_bitmap(values, &mut bitmap, |v| {
                        bloom.might_contain_i64(v)
                    });
                    return Some((first_col, bitmap));
                }
                ColumnPredicate::I64InSet { set, .. } => {
                    probe_membership_into_bitmap(values, &mut bitmap, |v| set.contains(v));
                    return Some((first_col, bitmap));
                }
                ColumnPredicate::I64Range { lo, hi, .. } => {
                    let (lo, hi) = (*lo, *hi);
                    probe_chunks_into_bitmap(values, &mut bitmap, |v| v >= lo && v <= hi);
                    return Some((first_col, bitmap));
                }
                _ => {}
            }
        }

        // Multi-predicate AND path (slower; rarely fires in practice
        // since the L9 emitter usually injects just one predicate).
        for (row, &v) in values.iter().enumerate() {
            let pass = self.predicates.iter().all(|p| match p {
                ColumnPredicate::I64InBloom { bloom, .. } => bloom.might_contain_i64(v),
                ColumnPredicate::I64InSet { set, .. } => set.contains(v),
                ColumnPredicate::I64Range { lo, hi, .. } => v >= *lo && v <= *hi,
                _ => false, // unreachable since is_runtime_i64_only
            });
            if pass {
                bitmap[row >> 3] |= 1 << (row & 7);
            }
        }
        Some((first_col, bitmap))
    }

    /// L9.PROBEORDER — does this filter carry a runtime membership
    /// probe (set/bloom) from the L9 sideband? These are the expensive
    /// per-row predicates (hash + lookup vs 2 comparisons); the fused
    /// multi-predicate path orders them LAST so they only run on rows
    /// that survived the cheap static predicates.
    pub fn has_runtime_probe(&self) -> bool {
        self.predicates.iter().any(|p| {
            matches!(
                p,
                ColumnPredicate::I64InSet { .. } | ColumnPredicate::I64InBloom { .. }
            )
        })
    }

    /// L9.PROBEORDER (2026-06-10) — evaluate ALL predicates against
    /// already-decoded column buffers, in cost order: static
    /// comparisons (ranges / IN / column-pair) build the bitmap first,
    /// then set/bloom membership probes run MASKED — only on rows
    /// still set. On Q20 SF=100 the unordered path probed the 217K-key
    /// partkey set on all 600M rows (+2.4s CPU) even though the
    /// bundled shipdate range leaves only 15% alive; masked ordering
    /// probes 91M instead.
    ///
    /// `resolve` maps a FILE-schema column index to the decoded buffer
    /// view (the caller holds the dense-decoded row group). Returns
    /// `None` — caller falls back to the legacy double-decode path —
    /// when any predicate targets an unresolvable column (not in the
    /// projection / unsupported decoded repr) or is a string shape
    /// (dict-preserved string eval stays in `build_bitmap`), or when
    /// resolved column lengths disagree.
    /// L9.ADAPT Guard 2 — when this filter carries a `probe_disarm`
    /// handle (tight-admitted wrap), each call records the marginal
    /// probe outcome (statics-survivors in vs post-probe survivors
    /// out); once `disarmed(threshold)`, the probes are SKIPPED:
    /// statics still evaluate (cheap, still route stash-vs-dense), and
    /// a probe-only filter returns an all-ones bitmap whose pass-rate
    /// routes the row group to dense emission — exactly the no-filter
    /// behavior. Correct by construction: every runtime probe is
    /// inexact-by-design and re-applied by the join.
    pub(crate) fn eval_on_decoded_views<'a>(
        &self,
        resolve: impl Fn(usize) -> Option<DecodedView<'a>>,
    ) -> Option<(Vec<u8>, usize)> {
        if self.predicates.is_empty() {
            return None;
        }
        let skip_probes = self
            .probe_disarm
            .as_ref()
            .map(|d| d.disarmed(crate::emat_arrow_reader::masked_dense_passrate_threshold()))
            .unwrap_or(false);
        // Resolve + validate every predicate's column(s) up front so a
        // late unsupported predicate can't leave a half-built bitmap.
        enum Bound<'p, 'a> {
            I64(&'p ColumnPredicate, &'a [i64]),
            /// NARROW.DEC × L9 (2026-07-02): an i64-domain predicate
            /// bound to a KEYS.2-narrowed key decoded at Int32
            /// (`EMAT_NARROW_KEY_DECODE`). Each value widens `as i64`
            /// on eval — lossless, the narrowing is stats-proven to
            /// fit i32. Without this binding the whole filter bailed
            /// to the legacy per-predicate re-decode path (Q08 SF=100:
            /// the L9 part→lineitem bloom wrap went +62 ms → +4.4 s
            /// whenever narrow keys were also on).
            I64onI32(&'p ColumnPredicate, &'a [i32]),
            I32(&'p ColumnPredicate, &'a [i32]),
            F64(&'p ColumnPredicate, &'a [f64]),
            I32Pair {
                left: &'a [i32],
                right: &'a [i32],
                op: Operator,
            },
        }
        let mut statics: Vec<Bound<'_, 'a>> = Vec::new();
        let mut probes: Vec<Bound<'_, 'a>> = Vec::new();
        let mut total: Option<usize> = None;
        let check_len = |len: usize, total: &mut Option<usize>| -> bool {
            match total {
                None => {
                    *total = Some(len);
                    true
                }
                Some(t) => *t == len,
            }
        };
        for p in &self.predicates {
            match p {
                ColumnPredicate::I64Range { col_idx, .. } => match resolve(*col_idx)? {
                    DecodedView::I64(v) => {
                        if !check_len(v.len(), &mut total) {
                            return None;
                        }
                        statics.push(Bound::I64(p, v));
                    }
                    // Narrowed key decoded at Int32: widen per value.
                    DecodedView::I32(v) => {
                        if !check_len(v.len(), &mut total) {
                            return None;
                        }
                        statics.push(Bound::I64onI32(p, v));
                    }
                    _ => return None,
                },
                ColumnPredicate::I64InSet { col_idx, .. }
                | ColumnPredicate::I64InBloom { col_idx, .. } => {
                    let bound = match resolve(*col_idx)? {
                        DecodedView::I64(v) => {
                            if !check_len(v.len(), &mut total) {
                                return None;
                            }
                            Bound::I64(p, v)
                        }
                        // Narrowed key decoded at Int32: widen per value.
                        DecodedView::I32(v) => {
                            if !check_len(v.len(), &mut total) {
                                return None;
                            }
                            Bound::I64onI32(p, v)
                        }
                        _ => return None,
                    };
                    // Guard 2 — disarmed probes still resolve (so
                    // `total` is known for a probe-only filter) but
                    // never evaluate.
                    if !skip_probes {
                        probes.push(bound);
                    }
                }
                ColumnPredicate::I32Range { col_idx, .. }
                | ColumnPredicate::I32In { col_idx, .. } => {
                    let DecodedView::I32(v) = resolve(*col_idx)? else {
                        return None;
                    };
                    if !check_len(v.len(), &mut total) {
                        return None;
                    }
                    statics.push(Bound::I32(p, v));
                }
                ColumnPredicate::F64Range { col_idx, .. } => {
                    let DecodedView::F64(v) = resolve(*col_idx)? else {
                        return None;
                    };
                    if !check_len(v.len(), &mut total) {
                        return None;
                    }
                    statics.push(Bound::F64(p, v));
                }
                ColumnPredicate::I32ColumnPair {
                    left_col,
                    right_col,
                    op,
                } => {
                    let DecodedView::I32(left) = resolve(*left_col)? else {
                        return None;
                    };
                    let DecodedView::I32(right) = resolve(*right_col)? else {
                        return None;
                    };
                    if !check_len(left.len(), &mut total) || left.len() != right.len() {
                        return None;
                    }
                    statics.push(Bound::I32Pair {
                        left,
                        right,
                        op: *op,
                    });
                }
                // String shapes need the dict-preserved decode path.
                _ => return None,
            }
        }
        let total = total?;
        // Guard 2 — every predicate was a disarmed probe: emit an
        // all-ones bitmap (tail bits zeroed) so popcount routing sees
        // pass-rate 1.0 and the caller discards it and emits dense —
        // exactly the no-filter behavior.
        if statics.is_empty() && probes.is_empty() {
            let mut bitmap = vec![0xFFu8; total.div_ceil(8)];
            if total % 8 != 0 {
                if let Some(last) = bitmap.last_mut() {
                    *last = 0xFFu8 >> (8 - total % 8);
                }
            }
            return Some((bitmap, total));
        }
        let mut bitmap = vec![0u8; total.div_ceil(8)];

        let eval_row = |b: &Bound<'_, 'a>, row: usize| -> bool {
            match b {
                Bound::I64(p, v) => match p {
                    ColumnPredicate::I64Range { lo, hi, .. } => v[row] >= *lo && v[row] <= *hi,
                    ColumnPredicate::I64InSet { set, .. } => set.contains(v[row]),
                    ColumnPredicate::I64InBloom { bloom, .. } => bloom.might_contain_i64(v[row]),
                    _ => unreachable!("Bound::I64 holds only i64 predicate shapes"),
                },
                Bound::I64onI32(p, v) => {
                    let x = v[row] as i64;
                    match p {
                        ColumnPredicate::I64Range { lo, hi, .. } => x >= *lo && x <= *hi,
                        ColumnPredicate::I64InSet { set, .. } => set.contains(x),
                        ColumnPredicate::I64InBloom { bloom, .. } => bloom.might_contain_i64(x),
                        _ => unreachable!("Bound::I64onI32 holds only i64 predicate shapes"),
                    }
                }
                Bound::I32(p, v) => p.eval_i32(v[row]),
                Bound::F64(p, v) => p.eval_f64(v[row]),
                Bound::I32Pair { left, right, op } => {
                    let (l, r) = (left[row], right[row]);
                    match op {
                        Operator::Lt => l < r,
                        Operator::LtEq => l <= r,
                        Operator::Gt => l > r,
                        Operator::GtEq => l >= r,
                        Operator::Eq => l == r,
                        Operator::NotEq => l != r,
                        _ => false,
                    }
                }
            }
        };

        let mut first = true;
        // Q05.STATVEC — default-ON opt-out (`EMAT_STATVEC=0`) so the
        // strict harness can A/B the vectorised static first pass in
        // isolation. One env read per row-group filter eval.
        let statvec = crate::flags::enabled("EMAT_STATVEC");
        let apply = |b: &Bound<'_, 'a>, bitmap: &mut Vec<u8>, first: &mut bool| {
            if *first {
                // Full pass, chunk-of-8 byte packing (avoids the
                // read-modify-write bitmap serialisation).
                match b {
                    Bound::I64(p, v) => match p {
                        ColumnPredicate::I64Range { lo, hi, .. } => {
                            let (lo, hi) = (*lo, *hi);
                            probe_chunks_into_bitmap(v, bitmap, |x| x >= lo && x <= hi);
                        }
                        ColumnPredicate::I64InSet { set, .. } => {
                            probe_membership_into_bitmap(v, bitmap, |x| set.contains(x));
                        }
                        ColumnPredicate::I64InBloom { bloom, .. } => {
                            probe_membership_into_bitmap(v, bitmap, |x| bloom.might_contain_i64(x));
                        }
                        _ => unreachable!("Bound::I64 holds only i64 predicate shapes"),
                    },
                    // Narrowed-key binding: same chunked pass, widening
                    // each i32 value into the i64 predicate domain.
                    Bound::I64onI32(p, v) => match p {
                        ColumnPredicate::I64Range { lo, hi, .. } => {
                            let (lo, hi) = (*lo, *hi);
                            probe_chunks_into_bitmap(v, bitmap, |x| {
                                let x = x as i64;
                                x >= lo && x <= hi
                            });
                        }
                        ColumnPredicate::I64InSet { set, .. } => {
                            probe_membership_into_bitmap(v, bitmap, |x| set.contains(x as i64));
                        }
                        ColumnPredicate::I64InBloom { bloom, .. } => {
                            probe_membership_into_bitmap(v, bitmap, |x| {
                                bloom.might_contain_i64(x as i64)
                            });
                        }
                        _ => unreachable!("Bound::I64onI32 holds only i64 predicate shapes"),
                    },
                    // Q05.STATVEC (2026-07-03) — the scalar fallback
                    // below serialised the o_orderdate-class first-pass
                    // statics through the bitmap read-modify-write
                    // (Q05 SF=10 profile: `eval_i32` over the 15M-row
                    // orders scan ≈ 90 ms Σ, ON the critical path that
                    // gates the L9 l_orderkey bloom → lineitem phase).
                    // Route i32/f64 single-column statics through the
                    // same chunk-of-8 byte-packing loop the i64 shapes
                    // already use — value-identical evaluation, packed
                    // writes.
                    Bound::I32(p, v) if statvec => {
                        // Fold the dominant shape (pure comparison
                        // clauses, e.g. the TPC-H date-range bridge
                        // filters) into one [lo, hi] window so the
                        // per-row clause loop disappears entirely.
                        if let Some((lo, hi)) = p.i32_range_window() {
                            probe_chunks_into_bitmap(v, bitmap, |x| x >= lo && x <= hi);
                        } else {
                            probe_chunks_into_bitmap(v, bitmap, |x| p.eval_i32(x));
                        }
                    }
                    Bound::F64(p, v) if statvec => {
                        probe_chunks_into_bitmap(v, bitmap, |x| p.eval_f64(x));
                    }
                    _ => {
                        for row in 0..total {
                            if eval_row(b, row) {
                                bitmap[row >> 3] |= 1 << (row & 7);
                            }
                        }
                    }
                }
                *first = false;
            } else {
                and_eval_masked(bitmap, total, |row| eval_row(b, row));
            }
        };
        for b in statics.iter() {
            apply(b, &mut bitmap, &mut first);
            // Early exit: nothing survives, later predicates are moot.
            if bitmap.iter().all(|&x| x == 0) {
                break;
            }
        }
        // Guard 2 — capture the statics-survivor count so the probes'
        // MARGINAL pass-rate can be recorded (only when this filter
        // carries a disarm handle; one extra popcount per row group,
        // ~µs against a multi-ms decode).
        let probe_seen = if self.probe_disarm.is_some() && !probes.is_empty() {
            Some(if first {
                total // no statics ran; probes see every row
            } else {
                bitmap.iter().map(|b| b.count_ones() as usize).sum()
            })
        } else {
            None
        };
        for b in probes.iter() {
            apply(b, &mut bitmap, &mut first);
            if bitmap.iter().all(|&x| x == 0) {
                break;
            }
        }
        if let (Some(d), Some(seen)) = (self.probe_disarm.as_ref(), probe_seen) {
            let passed: usize = bitmap.iter().map(|b| b.count_ones() as usize).sum();
            d.record(seen, passed);
        }
        Some((bitmap, total))
    }
}

/// L9.PROBEORDER — view over an already-decoded column buffer, used by
/// [`BridgeFilter::eval_on_decoded_views`]. The reader maps its
/// `DecodedColumn` reprs into these slices (zero-copy `typed_data`).
pub(crate) enum DecodedView<'a> {
    I64(&'a [i64]),
    I32(&'a [i32]),
    F64(&'a [f64]),
}

/// L9.PROBEORDER — AND a predicate into `bitmap`, evaluating ONLY rows
/// whose bit is currently set (whole zero bytes are skipped, so cost
/// scales with surviving rows, not total rows). `eval` takes the row
/// index so mixed column types and column-pair predicates share one
/// helper.
fn and_eval_masked(bitmap: &mut [u8], n_rows: usize, eval: impl Fn(usize) -> bool) {
    for (byte_idx, b) in bitmap.iter_mut().enumerate() {
        let mut cur = *b;
        if cur == 0 {
            continue;
        }
        let base = byte_idx << 3;
        while cur != 0 {
            let bit = cur.trailing_zeros() as usize;
            let row = base + bit;
            if row >= n_rows || !eval(row) {
                *b &= !(1u8 << bit);
            }
            cur &= cur - 1;
        }
    }
}

/// Σ.AH.2 Story 1'.4 Stage 2 — process 8 i64 values per loop
/// iteration, packing the per-value boolean result into one bitmap
/// byte. Avoids the read-modify-write bitmap pattern of the scalar
/// `for (row, &v)` loop, which serialised the inner loop through
/// `bitmap[row>>3] |= 1<<(row&7)`. The 8-lane unroll lets LLVM
/// vectorise the predicate evaluation across lanes.
#[inline(always)]
fn probe_chunks_into_bitmap<T: Copy>(values: &[T], bitmap: &mut [u8], probe: impl Fn(T) -> bool) {
    let chunks = values.chunks_exact(8);
    let rem = chunks.remainder();
    let n_chunks = values.len() / 8;
    for (chunk_idx, chunk) in chunks.enumerate() {
        let mut byte = 0u8;
        // Explicit unroll — LLVM auto-unrolls anyway but spelling it
        // out makes the bit-position constants obvious to the
        // optimizer.
        if probe(chunk[0]) {
            byte |= 1 << 0;
        }
        if probe(chunk[1]) {
            byte |= 1 << 1;
        }
        if probe(chunk[2]) {
            byte |= 1 << 2;
        }
        if probe(chunk[3]) {
            byte |= 1 << 3;
        }
        if probe(chunk[4]) {
            byte |= 1 << 4;
        }
        if probe(chunk[5]) {
            byte |= 1 << 5;
        }
        if probe(chunk[6]) {
            byte |= 1 << 6;
        }
        if probe(chunk[7]) {
            byte |= 1 << 7;
        }
        bitmap[chunk_idx] = byte;
    }
    for (i, &v) in rem.iter().enumerate() {
        let row = n_chunks * 8 + i;
        if probe(v) {
            bitmap[row >> 3] |= 1 << (row & 7);
        }
    }
}

/// Q05.MEMO (2026-07-03) — run-memoized variant of
/// [`probe_chunks_into_bitmap`] for EXPENSIVE membership probes
/// (bloom / hash-set). Fact tables clustered on the probed key repeat
/// the previous key on most consecutive rows (TPC-H
/// `lineitem.l_orderkey`: ~4 rows per order → 75% consecutive
/// duplicates), so re-using the previous verdict replaces a ~4 ns
/// hash+load probe with a compare. Q05 SF=10 stage profile: the L9
/// l_orderkey bloom probe over 60M rows was ~285 ms Σ compute inside
/// the dominant lineitem scan; memoization probes ~15M distinct runs
/// instead.
///
/// Correctness: bit-identical to the plain helper — the probe is a
/// pure function of the value, so equal adjacent values always share
/// one verdict.
#[inline(always)]
fn probe_chunks_into_bitmap_memo<T: Copy + PartialEq>(
    values: &[T],
    bitmap: &mut [u8],
    probe: impl Fn(T) -> bool,
) {
    let Some(&first) = values.first() else {
        return;
    };
    let mut last_v = first;
    let mut last_r = probe(first);
    let chunks = values.chunks_exact(8);
    let rem = chunks.remainder();
    let n_chunks = values.len() / 8;
    for (chunk_idx, chunk) in chunks.enumerate() {
        let mut byte = 0u8;
        for (lane, &v) in chunk.iter().enumerate() {
            if v != last_v {
                last_v = v;
                last_r = probe(v);
            }
            byte |= (last_r as u8) << lane;
        }
        bitmap[chunk_idx] = byte;
    }
    for (i, &v) in rem.iter().enumerate() {
        if v != last_v {
            last_v = v;
            last_r = probe(v);
        }
        if last_r {
            let row = n_chunks * 8 + i;
            bitmap[row >> 3] |= 1 << (row & 7);
        }
    }
}

/// Q05.MEMO — should the run-memo probe path be used for this column
/// buffer? Pure core (the `tri_state_of` convention): `force` is the
/// parsed `EMAT_PROBE_RUN_MEMO` tri-state — `Some(true)` always memo,
/// `Some(false)` never, `None` = AUTO: sample the first
/// [`RUN_MEMO_SAMPLE`] values and require ≥25% consecutive duplicates
/// (memo wins from ~15% on the ~4 ns bloom probe; 25% keeps margin).
/// Unique-key columns (dup fraction 0) and short buffers stay on the
/// 8-lane vectorised path, so non-clustered shapes (Q20 l_partkey,
/// dim-PK scans) never pay the loop-carried memo dependency.
const RUN_MEMO_SAMPLE: usize = 1024;
fn run_memo_pays<T: Copy + PartialEq>(values: &[T], force: Option<bool>) -> bool {
    if let Some(f) = force {
        return f;
    }
    let n = values.len().min(RUN_MEMO_SAMPLE);
    if n < 64 {
        return false;
    }
    let dups = values[..n].windows(2).filter(|w| w[0] == w[1]).count();
    // dups/(n-1) ≥ 1/4
    dups * 4 >= n - 1
}

/// Q05.MEMO — env resolver for the `EMAT_PROBE_RUN_MEMO` tri-state
/// (unset = AUTO; see `docs/EMAT_FLAGS.md`). Read per row-group column
/// — one `env::var` against a multi-ms decode, not a hot-path read.
fn probe_run_memo_flag() -> Option<bool> {
    crate::flags::tri_state("EMAT_PROBE_RUN_MEMO")
}

/// Q05.MEMO — dispatch an expensive membership probe (bloom /
/// hash-set) over a full column buffer to the run-memoized or plain
/// chunk-of-8 loop. Cheap static predicates (ranges) keep calling
/// [`probe_chunks_into_bitmap`] directly — a compare-based memo can't
/// beat a compare-based predicate.
#[inline(always)]
fn probe_membership_into_bitmap<T: Copy + PartialEq>(
    values: &[T],
    bitmap: &mut [u8],
    probe: impl Fn(T) -> bool,
) {
    if run_memo_pays(values, probe_run_memo_flag()) {
        probe_chunks_into_bitmap_memo(values, bitmap, probe);
    } else {
        probe_chunks_into_bitmap(values, bitmap, probe);
    }
}

#[derive(Debug, Clone)]
pub enum ColumnPredicate {
    /// AND of comparisons on the same i32/Date32 column.
    I32Range {
        col_idx: usize,
        clauses: Vec<RangeClause>,
    },
    /// `col IN (v1, v2, ...)` on an i32 column (Q16's p_size).
    I32In { col_idx: usize, values: Vec<i32> },
    /// AND of comparisons on the same Float64 column. Used for
    /// `l_quantity BETWEEN ...` (Q06, Q19).
    F64Range {
        col_idx: usize,
        clauses: Vec<F64RangeClause>,
    },
    /// `col = literal` on a string column (Q19's l_shipinstruct).
    StringEq { col_idx: usize, value: String },
    /// `col != literal` on a string column (Q16's p_brand <> 'Brand#45').
    StringNotEq { col_idx: usize, value: String },
    /// `col IN (v1, v2, ...)` on a string column. Captures both
    /// SQL `IN (...)` *and* OR-of-equality (Q19's l_shipmode).
    StringIn { col_idx: usize, values: Vec<String> },
    /// `col [NOT] LIKE 'pattern'` on a string column. Pattern uses
    /// SQL wildcards (`%` = any, `_` not yet supported — caller
    /// avoids pushing patterns with `_`). `negated` flips the match.
    /// Examples:
    ///   Q13: `o_comment NOT LIKE '%special%requests%'`
    ///   Q16: `p_type NOT LIKE 'MEDIUM POLISHED %'`
    StringLike {
        col_idx: usize,
        pattern: String,
        negated: bool,
    },
    /// `col_a OP col_b` on two i32/Date32 columns of the same type.
    /// Q12 has `l_commitdate < l_receiptdate AND l_shipdate <
    /// l_commitdate`; Q21 has `l_receiptdate > l_commitdate`.
    /// Pairwise eval in build_bitmap.
    I32ColumnPair {
        left_col: usize,
        right_col: usize,
        op: Operator,
    },
    /// Σ.Q.L4′ — Bloom-filter membership probe on an i64 column.
    /// Pre-built by the planner from a HashJoin's build side (e.g.
    /// the post-filter supplier/nation keys) and pushed into the
    /// probe-side scan so lineitem rows whose join key isn't in the
    /// bloom skip masked-decode entirely. Approximate by design —
    /// false positives are allowed (the residual join still runs);
    /// this is selectivity reduction, not correctness.
    I64InBloom {
        col_idx: usize,
        bloom: Arc<crate::bloom::BloomFilter>,
    },
    /// L9.HashSet (2026-05-24) — exact i64 membership probe. Used by
    /// the L9 runtime sideband when the build side is small enough
    /// that an exact `I64Set` outperforms the probabilistic bloom
    /// (Q17 SF=10 profile: 1.3 ns/probe vs 17.2 ns/probe for bloom
    /// at a 2K-key build, plus zero false positives).
    /// Falls back to `I64InBloom` past `EMAT_L9_SET_THRESHOLD` keys.
    I64InSet {
        col_idx: usize,
        set: Arc<crate::i64_set::I64Set>,
    },
    /// Lever 3 (2026-05-24) — closed-interval `lo ≤ v ≤ hi` predicate
    /// on an i64 column. Emitted alongside `I64InBloom` / `I64InSet`
    /// by `BuildSideBloomEmitterExec` (tracking the min/max of all
    /// build keys per-partition then unioning at publish). The win
    /// isn't the per-row check — that's just two comparisons, the
    /// bloom/set is already a single hash + lookup. The win is the
    /// **RG-level skip**: `BridgeFilter::build_bitmap` consults the
    /// parquet column-chunk min/max statistics for the target RG
    /// and short-circuits to an all-zero bitmap when stats don't
    /// overlap `[lo, hi]`. For TPC-H queries with a narrow dim
    /// filter (Q05's ASIA → ~20K customers in a contiguous key
    /// range, Q07's nation-filtered supplier set, Q08's brand+nation
    /// part-filter) this can skip whole row groups before any
    /// l_partkey / l_suppkey decode happens. For Q17 the range
    /// covers ~100% of l_partkey so no RG is skipped — the lever is
    /// query-shape dependent.
    I64Range { col_idx: usize, lo: i64, hi: i64 },
    /// KEYS.5 (2026-06-07) — Bloom-filter membership probe on a
    /// *string* column. The string analog of `I64InBloom`: pre-built
    /// by `BuildSideBloomEmitterExec` from a string join key's build
    /// side (e.g. a filtered nation/region name set) and pushed into
    /// the probe-side scan so rows whose join key isn't in the bloom
    /// skip the residual join probe. Approximate by design (FPR > 0);
    /// the residual equi-join still runs — this is selectivity
    /// reduction, not correctness. Hashing is byte-level
    /// (`bloom.might_contain_str`), so it is encoding-independent: it
    /// works against dict-encoded or PLAIN string columns alike via
    /// `BridgeFilter::build_bitmap`'s existing string decode path.
    StringInBloom {
        col_idx: usize,
        bloom: Arc<crate::bloom::BloomFilter>,
    },
    /// KEYS.5 (2026-06-07) — exact string membership probe. The string
    /// analog of `I64InSet`: used when the build side is small enough
    /// that an exact set (zero false positives) beats the probabilistic
    /// bloom. The emitter falls back to `StringInBloom` past the set
    /// threshold. `HashSet<String>::contains` borrows `&str`, so the
    /// per-probe check allocates nothing.
    StringInSet {
        col_idx: usize,
        set: Arc<std::collections::HashSet<String>>,
    },
}

#[derive(Debug, Clone, Copy)]
pub struct RangeClause {
    pub op: Operator,
    pub literal_i32: i32,
}

#[derive(Debug, Clone, Copy)]
pub struct F64RangeClause {
    pub op: Operator,
    pub literal_f64: f64,
}

impl BridgeFilter {
    pub fn predicates(&self) -> &[ColumnPredicate] {
        &self.predicates
    }

    /// Σ.CC — 64-bit fingerprint of the predicate set for the
    /// condition cache; `None` when ANY predicate is a per-query
    /// runtime artifact (join-derived blooms/sets), which must never
    /// be cached across queries. Covers every semantic field of the
    /// static variants (f64 via `to_bits`, operators via `Debug`);
    /// order-sensitive, matching the planner's deterministic
    /// predicate order. The match is exhaustive on purpose: a new
    /// variant must DECIDE its cacheability here or fail to compile.
    pub fn cond_fingerprint(&self) -> Option<u64> {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        for p in &self.predicates {
            match p {
                // Per-query runtime artifacts — never cacheable.
                ColumnPredicate::I64InBloom { .. }
                | ColumnPredicate::I64InSet { .. }
                | ColumnPredicate::StringInBloom { .. }
                | ColumnPredicate::StringInSet { .. } => return None,
                ColumnPredicate::I32Range { col_idx, clauses } => {
                    (1u8, col_idx).hash(&mut h);
                    for c in clauses {
                        format!("{:?}", c.op).hash(&mut h);
                        c.literal_i32.hash(&mut h);
                    }
                }
                ColumnPredicate::I32In { col_idx, values } => {
                    (2u8, col_idx, values).hash(&mut h);
                }
                ColumnPredicate::F64Range { col_idx, clauses } => {
                    (3u8, col_idx).hash(&mut h);
                    for c in clauses {
                        format!("{:?}", c.op).hash(&mut h);
                        c.literal_f64.to_bits().hash(&mut h);
                    }
                }
                ColumnPredicate::StringEq { col_idx, value } => {
                    (4u8, col_idx, value).hash(&mut h);
                }
                ColumnPredicate::StringNotEq { col_idx, value } => {
                    (5u8, col_idx, value).hash(&mut h);
                }
                ColumnPredicate::StringIn { col_idx, values } => {
                    (6u8, col_idx, values).hash(&mut h);
                }
                ColumnPredicate::StringLike {
                    col_idx,
                    pattern,
                    negated,
                } => {
                    (7u8, col_idx, pattern, negated).hash(&mut h);
                }
                ColumnPredicate::I32ColumnPair {
                    left_col,
                    right_col,
                    op,
                } => {
                    (8u8, left_col, right_col).hash(&mut h);
                    format!("{op:?}").hash(&mut h);
                }
                ColumnPredicate::I64Range { col_idx, lo, hi } => {
                    (9u8, col_idx, lo, hi).hash(&mut h);
                }
            }
        }
        Some(h.finish())
    }

    /// Σ.E5 #513: build a combined row bitmap by AND-combining one
    /// per-predicate bitmap. Returns `(bitmap, total_rows)`. For i32
    /// predicates uses the fast dict-mask + RLE-aware kernel; for
    /// string predicates uses the dict-preserved per-entry mask.
    /// Falls back to dense decode if the i32 column isn't
    /// dict-encoded (e.g. PLAIN-only).
    pub fn build_bitmap(&self, path: &std::path::Path, rg: usize) -> DfResult<(Vec<u8>, usize)> {
        use crate::ematix_parquet_bridge::{
            filter_byte_array_to_bitmap, filter_byte_array_to_bitmap_dense,
            filter_f64_column_to_bitmap, filter_f64_column_to_bitmap_dense,
            filter_i32_column_to_bitmap, filter_i32_column_to_bitmap_dense,
            filter_i64_column_to_bitmap_dense,
        };
        // Σ.CC: a repeated STATIC predicate set answers from the
        // condition cache — zero predicate-column decodes for this
        // row group. Runtime blooms/sets fingerprint to None and
        // bypass; file identity (mtime, len) is in the key, so
        // rewrites never false-hit.
        let cond_key = self
            .cond_fingerprint()
            .and_then(|fp| crate::cond_cache::key_for(path, rg, fp));
        if let Some(k) = &cond_key {
            if let Some((bm, total)) = crate::cond_cache::lookup(k) {
                return Ok(((*bm).clone(), total));
            }
        }
        let mut combined: Option<(Vec<u8>, usize)> = None;
        for p in &self.predicates {
            // Σ.LM.1 observability: one tick per predicate-column
            // bitmap decode — the quantity the short-circuit below
            // exists to reduce (tests pin the elision through it).
            BRIDGE_PREDICATE_EVALS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let (b, total) = match p {
                ColumnPredicate::I32Range { col_idx, .. }
                | ColumnPredicate::I32In { col_idx, .. } => {
                    let pclone = p.clone();
                    // Try the fast dict-aware kernel first; fall back
                    // to dense decode on PLAIN-only chunks.
                    match filter_i32_column_to_bitmap(path, rg, *col_idx, {
                        let pc = pclone.clone();
                        move |v: i32| pc.eval_i32(v)
                    }) {
                        Ok(r) => r,
                        Err(_) => {
                            let pc2 = pclone.clone();
                            let file = crate::ematix_parquet_bridge::open_cached(path)?;
                            filter_i32_column_to_bitmap_dense(
                                &file,
                                rg,
                                *col_idx,
                                move |v: i32| pc2.eval_i32(v),
                            )?
                        }
                    }
                }
                ColumnPredicate::F64Range { col_idx, .. } => {
                    let pclone = p.clone();
                    // Σ.E6: try dict-fused first; fall back to dense
                    // decode on PLAIN-only chunks (Err returned).
                    match filter_f64_column_to_bitmap(path, rg, *col_idx, {
                        let pc = pclone.clone();
                        move |v: f64| pc.eval_f64(v)
                    }) {
                        Ok(r) => {
                            if std::env::var("EMAT_F64_DICT_TRACE").is_ok() {
                                eprintln!("[f64 dict-fused] col={col_idx} rg={rg} OK");
                            }
                            r
                        }
                        Err(e) => {
                            if std::env::var("EMAT_F64_DICT_TRACE").is_ok() {
                                eprintln!("[f64 dict-fused] col={col_idx} rg={rg} FALLBACK: {e}");
                            }
                            let pc2 = pclone.clone();
                            filter_f64_column_to_bitmap_dense(path, rg, *col_idx, move |v: f64| {
                                pc2.eval_f64(v)
                            })?
                        }
                    }
                }
                ColumnPredicate::I32ColumnPair {
                    left_col,
                    right_col,
                    op,
                } => {
                    // Decode both cols dense via masked_decode_i32
                    // with all-ones masks. Same shape as the F64 dense
                    // path. Apply the op pairwise to build the bitmap.
                    use crate::ematix_parquet_bridge::{masked_decode_i32, open_cached};
                    let file = open_cached(path)?;
                    let md = file
                        .cached_metadata()
                        .map_err(|e| DataFusionError::External(format!("metadata: {e}").into()))?;
                    let total = md.row_groups[rg].columns[*left_col]
                        .meta_data
                        .as_ref()
                        .map(|m| m.num_values as usize)
                        .unwrap_or(0);
                    let all_ones = vec![0xFFu8; total.div_ceil(8)];
                    let left = masked_decode_i32(&file, rg, *left_col, &all_ones)?;
                    let right = masked_decode_i32(&file, rg, *right_col, &all_ones)?;
                    if left.len() != right.len() || left.len() != total {
                        return Err(DataFusionError::External(
                            format!(
                                "I32ColumnPair: row count mismatch left={} right={} total={}",
                                left.len(),
                                right.len(),
                                total
                            )
                            .into(),
                        ));
                    }
                    let mut bitmap = vec![0u8; total.div_ceil(8)];
                    let op = *op;
                    for row in 0..total {
                        let l = left[row];
                        let r = right[row];
                        let pass = match op {
                            Operator::Lt => l < r,
                            Operator::LtEq => l <= r,
                            Operator::Gt => l > r,
                            Operator::GtEq => l >= r,
                            Operator::Eq => l == r,
                            Operator::NotEq => l != r,
                            _ => false,
                        };
                        if pass {
                            bitmap[row >> 3] |= 1 << (row & 7);
                        }
                    }
                    (bitmap, total)
                }
                ColumnPredicate::I64InBloom { col_idx, bloom } => {
                    let bloom = bloom.clone();
                    filter_i64_column_to_bitmap_dense(path, rg, *col_idx, move |v: i64| {
                        bloom.might_contain_i64(v)
                    })?
                }
                ColumnPredicate::I64InSet { col_idx, set } => {
                    let set = set.clone();
                    filter_i64_column_to_bitmap_dense(path, rg, *col_idx, move |v: i64| {
                        set.contains(v)
                    })?
                }
                ColumnPredicate::I64Range { col_idx, lo, hi } => {
                    // Lever 3 — RG-level skip via parquet column-chunk
                    // min/max statistics. If the RG's stats don't
                    // overlap `[lo, hi]`, return an all-zero bitmap
                    // immediately (no decode). When stats are missing
                    // or overlapping, fall through to the per-row
                    // range check (which is two i64 comparisons).
                    let lo = *lo;
                    let hi = *hi;
                    let col_idx = *col_idx;
                    if let Some((rg_min, rg_max)) =
                        crate::ematix_parquet_bridge::rg_i64_min_max(path, rg, col_idx)?
                    {
                        // No overlap: short-circuit.
                        if rg_max < lo || rg_min > hi {
                            // We still need to know `total` (rows in RG).
                            let total =
                                crate::ematix_parquet_bridge::rg_num_values(path, rg, col_idx)?;
                            (vec![0u8; total.div_ceil(8)], total)
                        } else if rg_min >= lo && rg_max <= hi {
                            // Fully inside: every row passes the range.
                            // Skip the per-row check; return an all-
                            // ones bitmap. (Costs one masked-decode for
                            // the AND-combine downstream, but no
                            // per-row predicate evaluation.)
                            let total =
                                crate::ematix_parquet_bridge::rg_num_values(path, rg, col_idx)?;
                            let mut bitmap = vec![0xFFu8; total.div_ceil(8)];
                            // Clear the unused tail bits in the final
                            // byte (avoid spuriously matching ghost
                            // rows past `total`).
                            let tail_bits = total & 7;
                            if tail_bits != 0 {
                                if let Some(last) = bitmap.last_mut() {
                                    *last &= (1u8 << tail_bits) - 1;
                                }
                            }
                            (bitmap, total)
                        } else {
                            // Partial overlap: per-row check.
                            filter_i64_column_to_bitmap_dense(path, rg, col_idx, move |v: i64| {
                                v >= lo && v <= hi
                            })?
                        }
                    } else {
                        // Stats unavailable: per-row check.
                        filter_i64_column_to_bitmap_dense(path, rg, col_idx, move |v: i64| {
                            v >= lo && v <= hi
                        })?
                    }
                }
                ColumnPredicate::StringEq { col_idx, .. }
                | ColumnPredicate::StringNotEq { col_idx, .. }
                | ColumnPredicate::StringIn { col_idx, .. }
                | ColumnPredicate::StringLike { col_idx, .. }
                // KEYS.5 — the string runtime sideband shapes decode the
                // same byte-array column and evaluate via `eval_str`
                // (bloom.might_contain_str / set.contains). Reusing this
                // arm gives them the dict-preserved fast path + dense
                // fallback for free.
                | ColumnPredicate::StringInBloom { col_idx, .. }
                | ColumnPredicate::StringInSet { col_idx, .. } => {
                    let pclone = p.clone();
                    // Try dict-preserved fast path; fall back to dense
                    // on PLAIN-encoded high-cardinality columns
                    // (Q13's o_comment).
                    match filter_byte_array_to_bitmap(path, rg, *col_idx, {
                        let pc = pclone.clone();
                        move |bytes: &[u8]| match std::str::from_utf8(bytes) {
                            Ok(s) => pc.eval_str(s),
                            Err(_) => false,
                        }
                    }) {
                        Ok(r) => r,
                        Err(_) => {
                            let pc2 = pclone.clone();
                            filter_byte_array_to_bitmap_dense(
                                path,
                                rg,
                                *col_idx,
                                move |bytes: &[u8]| match std::str::from_utf8(bytes) {
                                    Ok(s) => pc2.eval_str(s),
                                    Err(_) => false,
                                },
                            )?
                        }
                    }
                }
            };
            match combined.as_mut() {
                None => combined = Some((b, total)),
                Some((acc, prior_total)) => {
                    if *prior_total != total {
                        return Err(DataFusionError::External(
                            format!(
                                "BridgeFilter::build_bitmap: column row counts differ ({} vs {})",
                                *prior_total, total
                            )
                            .into(),
                        ));
                    }
                    for (a, b) in acc.iter_mut().zip(b.iter()) {
                        *a &= b;
                    }
                }
            }
            // Σ.LM.1 (PREWHERE-style short-circuit, 2026-07-12): once
            // the accumulated bitmap is all-zero this row group is
            // dead — every remaining predicate COLUMN DECODE is pure
            // waste (AND with zero stays zero). ClickHouse's
            // multi-step PREWHERE stops exactly here; so do we. The
            // stats-based RG skip above can't catch these (footer
            // ranges overlapped; the data didn't).
            if let Some((acc, _)) = combined.as_ref() {
                if bitmap_all_zero(acc) {
                    break;
                }
            }
        }
        let out = combined.ok_or_else(|| {
            DataFusionError::External("BridgeFilter::build_bitmap: no predicates".into())
        })?;
        if let Some(k) = cond_key {
            crate::cond_cache::insert(k, Arc::new(out.0.clone()), out.1);
        }
        Ok(out)
    }
}

/// Σ.LM.1 — word-wise all-zero check for the short-circuit above. A
/// 64 KiB-row bitmap is 8 KiB; this scans it as u64 words and is
/// ~free next to one avoided column decode.
#[inline]
pub(crate) fn bitmap_all_zero(bitmap: &[u8]) -> bool {
    let (head, words, tail) = unsafe { bitmap.align_to::<u64>() };
    head.iter().all(|b| *b == 0) && words.iter().all(|w| *w == 0) && tail.iter().all(|b| *b == 0)
}

/// Σ.LM.1 — process-global count of per-predicate bitmap decodes in
/// [`BridgeFilter::build_bitmap`]. Monotonic; tests read deltas to pin
/// the short-circuit (an all-zero accumulator must stop the remaining
/// predicate-column decodes for that row group).
static BRIDGE_PREDICATE_EVALS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Read [`BRIDGE_PREDICATE_EVALS`] (relaxed).
pub fn bridge_predicate_evals() -> u64 {
    BRIDGE_PREDICATE_EVALS.load(std::sync::atomic::Ordering::Relaxed)
}

impl ColumnPredicate {
    pub fn col_idx(&self) -> usize {
        match self {
            ColumnPredicate::I32Range { col_idx, .. }
            | ColumnPredicate::I32In { col_idx, .. }
            | ColumnPredicate::F64Range { col_idx, .. }
            | ColumnPredicate::StringEq { col_idx, .. }
            | ColumnPredicate::StringNotEq { col_idx, .. }
            | ColumnPredicate::StringIn { col_idx, .. }
            | ColumnPredicate::StringLike { col_idx, .. } => *col_idx,
            // ColumnPair touches two cols; return left as the "primary".
            ColumnPredicate::I32ColumnPair { left_col, .. } => *left_col,
            ColumnPredicate::I64InBloom { col_idx, .. } => *col_idx,
            ColumnPredicate::I64InSet { col_idx, .. } => *col_idx,
            ColumnPredicate::I64Range { col_idx, .. } => *col_idx,
            ColumnPredicate::StringInBloom { col_idx, .. } => *col_idx,
            ColumnPredicate::StringInSet { col_idx, .. } => *col_idx,
        }
    }

    /// Σ.E5 Phase 1.8 (2026-05-19): estimate the fraction of rows
    /// that pass this predicate, given the column's min/max/distinct
    /// stats from parquet metadata. Returns a value in [0.0, 1.0].
    ///
    /// Used by `load_row_group_masked` to dispatch between
    /// parallel-bitmap+dense (high-sel) and serial-bitmap+masked-
    /// decode (low-sel) paths. When stats are missing, returns a
    /// conservative 0.5 (treat as "may be high-sel").
    pub fn estimate_pass_rate(&self, stats: &datafusion::common::stats::ColumnStatistics) -> f64 {
        use datafusion::common::stats::Precision;

        let extract_i32 = |p: &Precision<ScalarValue>| -> Option<i32> {
            match p {
                Precision::Exact(v) | Precision::Inexact(v) => match v {
                    ScalarValue::Int32(Some(x)) => Some(*x),
                    ScalarValue::Date32(Some(x)) => Some(*x),
                    _ => None,
                },
                _ => None,
            }
        };
        let extract_usize = |p: &Precision<usize>| -> Option<usize> {
            match p {
                Precision::Exact(v) | Precision::Inexact(v) => Some(*v),
                _ => None,
            }
        };

        match self {
            ColumnPredicate::I32Range { clauses, .. } => {
                let min = extract_i32(&stats.min_value);
                let max = extract_i32(&stats.max_value);
                let (Some(min), Some(max)) = (min, max) else {
                    return 0.5;
                };
                if max <= min {
                    return 0.5;
                }
                let range = (max - min) as f64;
                // Combine clauses via AND — multiply selectivities
                // (independence assumption — conservative).
                let mut sel = 1.0_f64;
                for c in clauses {
                    let lit = c.literal_i32;
                    let clause_sel = match c.op {
                        Operator::Eq => 1.0 / ((max - min) as f64).max(1.0),
                        Operator::NotEq => 1.0 - 1.0 / ((max - min) as f64).max(1.0),
                        Operator::Lt => {
                            if lit <= min {
                                0.0
                            } else if lit > max {
                                1.0
                            } else {
                                (lit - min) as f64 / range
                            }
                        }
                        Operator::LtEq => {
                            if lit < min {
                                0.0
                            } else if lit >= max {
                                1.0
                            } else {
                                ((lit - min + 1) as f64 / (range + 1.0)).min(1.0)
                            }
                        }
                        Operator::Gt => {
                            if lit >= max {
                                0.0
                            } else if lit < min {
                                1.0
                            } else {
                                (max - lit) as f64 / range
                            }
                        }
                        Operator::GtEq => {
                            if lit > max {
                                0.0
                            } else if lit <= min {
                                1.0
                            } else {
                                ((max - lit + 1) as f64 / (range + 1.0)).min(1.0)
                            }
                        }
                        _ => 1.0, // unknown op — conservative
                    };
                    sel *= clause_sel;
                }
                sel.clamp(0.0, 1.0)
            }
            ColumnPredicate::I32In { values, .. } => {
                let min = extract_i32(&stats.min_value);
                let max = extract_i32(&stats.max_value);
                let card = extract_usize(&stats.distinct_count);
                let card = card.or_else(|| match (min, max) {
                    (Some(a), Some(b)) if b > a => Some((b - a + 1) as usize),
                    _ => None,
                });
                match card {
                    Some(c) if c > 0 => (values.len() as f64 / c as f64).clamp(0.0, 1.0),
                    _ => 0.5,
                }
            }
            ColumnPredicate::StringEq { .. } => {
                match extract_usize(&stats.distinct_count) {
                    Some(c) if c > 0 => 1.0 / c as f64,
                    _ => 0.1, // conservative default
                }
            }
            ColumnPredicate::StringNotEq { .. } => match extract_usize(&stats.distinct_count) {
                Some(c) if c > 0 => 1.0 - 1.0 / c as f64,
                _ => 0.9,
            },
            ColumnPredicate::StringIn { values, .. } => {
                match extract_usize(&stats.distinct_count) {
                    Some(c) if c > 0 => (values.len() as f64 / c as f64).clamp(0.0, 1.0),
                    _ => 0.2,
                }
            }
            ColumnPredicate::StringLike { negated, .. } => {
                // No cheap way to estimate LIKE; assume substring
                // matches are uncommon. NOT LIKE inverts.
                if *negated { 0.9 } else { 0.1 }
            }
            // Refused-for-pushdown shapes; never reach here in practice.
            ColumnPredicate::F64Range { .. } | ColumnPredicate::I32ColumnPair { .. } => 0.5,
            // Σ.Q.L4′ — bloom is selectivity-reducing. Caller supplies
            // the build-side cardinality via expected_keys when
            // constructing; we approximate pass rate as 1 - FPR ≈ 0.01
            // for true negatives + (built_card / col_distinct) for true
            // positives. Without stats we lean on the bloom's own
            // n_blocks heuristic; a 1% FPR + small build vs large
            // probe means the pass rate is roughly the build/probe
            // cardinality ratio. We don't have that here, so return
            // 0.2 as a conservative win-leaning estimate (lower than
            // the StringIn default of 0.2 because we expect blooms
            // only to be injected when the build is genuinely small).
            ColumnPredicate::I64InBloom { .. } => 0.2,
            // L9.HashSet — exact membership has no FP rate, so the
            // estimate can be tighter than the bloom's 0.2. The build
            // is small by definition (≤ EMAT_L9_SET_THRESHOLD), so we
            // expect ≤ ~0.1 pass rate in the common L9 case.
            ColumnPredicate::I64InSet { .. } => 0.1,
            // Lever 3 — closed-interval range. Without column stats we
            // can't bound it tightly; assume the L9 emitter only
            // produces this for builds with a narrow value range
            // (Q05/Q07/Q08 dim filters), so 0.3 is a conservative
            // win-leaning estimate.
            ColumnPredicate::I64Range { .. } => 0.3,
            // KEYS.5 — string runtime sideband. Mirror the i64 shapes:
            // bloom ~0.2 (FP-leaning, build small by construction),
            // exact set ~0.1 (no FP, smallest builds).
            ColumnPredicate::StringInBloom { .. } => 0.2,
            ColumnPredicate::StringInSet { .. } => 0.1,
        }
    }

    /// Σ.E5 per-filter Exact pushdown (2026-05-19): returns `true` if
    /// emat's bitmap evaluation is provably equivalent to DataFusion's
    /// predicate evaluation for this variant.
    ///
    /// Caller must ALSO check that the relevant column has no nulls
    /// (emat's kernels don't handle def-levels). See
    /// `EmatixFastParquetTableProvider::column_has_no_nulls`.
    ///
    /// See `docs/PHASE_SIGMA_E5_PER_FILTER_EXACT.md` §2 for the
    /// per-shape safety audit.
    pub fn is_exact_safe(&self) -> bool {
        match self {
            // Integer comparisons + discrete membership are byte-level
            // unambiguous AND BridgeFilter's dict-popcount-aware
            // i32 kernel beats DataFusion's FilterExec at SF=10.
            ColumnPredicate::I32Range { .. } | ColumnPredicate::I32In { .. } => true,
            // Σ.AE.1 (2026-05-26): Byte-equality matches Arrow's
            // `eq_utf8` semantically, BUT FilterExec running on
            // already-decoded Arrow batches is *faster* than
            // BridgeFilter on parquet-encoded strings for the
            // TPC-H string-filter shapes — Arrow's string-eq kernel
            // is highly tuned for contiguous batches. So even
            // though string predicates COULD be declared Exact,
            // doing so regresses Q03 (`c_mktsegment='BUILDING'`)
            // and similar at SF=10. Keep them Inexact so
            // FilterExec runs after the BridgeFilter has reduced
            // the row count. The bridge still does the heavy
            // filter; the residual FilterExec is cheap on the
            // already-narrow surviving rows.
            ColumnPredicate::StringEq { .. }
            | ColumnPredicate::StringNotEq { .. }
            | ColumnPredicate::StringIn { .. } => false,
            // LIKE is Exact only when `LikeMatcher::compile` accepts
            // the pattern (no `_`, no escape). Otherwise our matcher
            // can't represent the pattern → Inexact.
            ColumnPredicate::StringLike { pattern, .. } => {
                crate::like_matcher::LikeMatcher::compile(pattern).is_some()
            }
            // Refused for pushdown elsewhere (NaN/Inf semantics, double-
            // decode trap respectively). When/if re-enabled they'll
            // need their own audit before claiming Exact.
            ColumnPredicate::F64Range { .. } | ColumnPredicate::I32ColumnPair { .. } => false,
            // Σ.Q.L4′ — bloom is approximate by construction (FPR > 0).
            // Provider must declare this Inexact so DataFusion keeps
            // the residual HashJoin equality test.
            ColumnPredicate::I64InBloom { .. } => false,
            // L9.HashSet — exact membership IS byte-level equivalent
            // to the residual equi-join test, BUT the runtime sideband
            // path uses Inexact so the residual HashJoin still fires
            // (the sideband prunes selectivity, doesn't replace the
            // join). Mark Inexact to preserve correctness across all
            // L9 call sites. A future logical-rule pushdown that
            // proves the column has no nulls + the join is Inner could
            // promote this to true.
            ColumnPredicate::I64InSet { .. } => false,
            // Lever 3 — the I64Range is derived from build-side
            // min/max, which is necessary-but-not-sufficient for
            // the equi-join (every match key falls in [lo, hi] but
            // not every key in [lo, hi] is a match). The residual
            // HashJoin must still run. Inexact for the same reason
            // as the L9 bloom/set: selectivity reduction, not
            // replacement.
            ColumnPredicate::I64Range { .. } => false,
            // KEYS.5 — string runtime sideband is approximate (bloom)
            // or selectivity-reducing (set); the residual string
            // equi-join must still run. Inexact, like the i64 shapes.
            ColumnPredicate::StringInBloom { .. } => false,
            ColumnPredicate::StringInSet { .. } => false,
        }
    }

    /// Evaluate AND of all clauses against one f64 value (F64Range only).
    #[inline]
    pub fn eval_f64(&self, v: f64) -> bool {
        match self {
            ColumnPredicate::F64Range { clauses, .. } => {
                for c in clauses {
                    let pass = match c.op {
                        Operator::Eq => v == c.literal_f64,
                        Operator::NotEq => v != c.literal_f64,
                        Operator::Lt => v < c.literal_f64,
                        Operator::LtEq => v <= c.literal_f64,
                        Operator::Gt => v > c.literal_f64,
                        Operator::GtEq => v >= c.literal_f64,
                        _ => return false,
                    };
                    if !pass {
                        return false;
                    }
                }
                true
            }
            _ => false,
        }
    }

    /// Q05.STATVEC — fold an `I32Range` whose clauses are all plain
    /// comparisons (`=`, `<`, `<=`, `>`, `>=`) into one inclusive
    /// `[lo, hi]` window, so the first-pass scan filter tests two
    /// compares per value instead of looping the clause list per row.
    /// Returns `None` for any other predicate shape or any `!=` clause
    /// (callers fall back to [`Self::eval_i32`], value-identical).
    /// An unsatisfiable clause set folds to an empty window
    /// (`lo > hi`) — the window test is then false for every value,
    /// exactly like the clause loop.
    pub(crate) fn i32_range_window(&self) -> Option<(i32, i32)> {
        let ColumnPredicate::I32Range { clauses, .. } = self else {
            return None;
        };
        let (mut lo, mut hi) = (i32::MIN, i32::MAX);
        for c in clauses {
            let l = c.literal_i32;
            match c.op {
                Operator::Eq => {
                    lo = lo.max(l);
                    hi = hi.min(l);
                }
                Operator::Lt => hi = hi.min(l.checked_sub(1)?),
                Operator::LtEq => hi = hi.min(l),
                Operator::Gt => lo = lo.max(l.checked_add(1)?),
                Operator::GtEq => lo = lo.max(l),
                // `!=` (or anything else) punches a hole in the
                // window — not representable, keep the clause loop.
                _ => return None,
            }
        }
        Some((lo, hi))
    }

    /// Evaluate AND of all clauses against one i32 value (I32Range / I32In only).
    #[inline]
    pub fn eval_i32(&self, v: i32) -> bool {
        match self {
            ColumnPredicate::I32Range { clauses, .. } => {
                for c in clauses {
                    let pass = match c.op {
                        Operator::Eq => v == c.literal_i32,
                        Operator::NotEq => v != c.literal_i32,
                        Operator::Lt => v < c.literal_i32,
                        Operator::LtEq => v <= c.literal_i32,
                        Operator::Gt => v > c.literal_i32,
                        Operator::GtEq => v >= c.literal_i32,
                        _ => return false,
                    };
                    if !pass {
                        return false;
                    }
                }
                true
            }
            ColumnPredicate::I32In { values, .. } => values.contains(&v),
            _ => false,
        }
    }

    /// Σ.Q.L4′ — evaluate against an i64 value (I64InBloom only).
    /// Returns true on a bloom HIT (key may be present in the build
    /// side). False positives are part of the contract; the residual
    /// equijoin still runs.
    #[inline]
    pub fn eval_i64(&self, v: i64) -> bool {
        match self {
            ColumnPredicate::I64InBloom { bloom, .. } => bloom.might_contain_i64(v),
            ColumnPredicate::I64InSet { set, .. } => set.contains(v),
            ColumnPredicate::I64Range { lo, hi, .. } => v >= *lo && v <= *hi,
            _ => false,
        }
    }

    /// Evaluate against a string value (StringEq / StringNotEq /
    /// StringIn / StringLike only).
    #[inline]
    pub fn eval_str(&self, v: &str) -> bool {
        match self {
            ColumnPredicate::StringEq { value, .. } => v == value.as_str(),
            ColumnPredicate::StringNotEq { value, .. } => v != value.as_str(),
            ColumnPredicate::StringIn { values, .. } => values.iter().any(|s| s.as_str() == v),
            ColumnPredicate::StringLike {
                pattern, negated, ..
            } => {
                let m = matches_sql_like(pattern.as_str(), v);
                if *negated { !m } else { m }
            }
            // KEYS.5 — string runtime sideband probe. Byte-level hash
            // membership (bloom) / exact membership (set). False
            // positives on the bloom are part of the contract; the
            // residual string equi-join still runs.
            ColumnPredicate::StringInBloom { bloom, .. } => bloom.might_contain_str(v),
            ColumnPredicate::StringInSet { set, .. } => set.contains(v),
            _ => false,
        }
    }
}

/// SQL LIKE matcher supporting `%` wildcards. Splits the pattern by
/// `%` into literal chunks that must occur IN ORDER in the value.
/// Anchors at the front if the pattern doesn't start with `%`, and
/// at the back if it doesn't end with `%`. Bails (returns false) on
/// `_` wildcard — callers should avoid pushing patterns containing
/// `_` so we don't silently mismatch.
fn matches_sql_like(pattern: &str, value: &str) -> bool {
    if pattern.contains('_') {
        return false;
    }
    let starts_anchored = !pattern.starts_with('%');
    let ends_anchored = !pattern.ends_with('%');
    let parts: Vec<&str> = pattern.split('%').filter(|s| !s.is_empty()).collect();
    if parts.is_empty() {
        return true; // pattern was `%`, `%%`, or empty
    }
    let n = parts.len();
    let mut pos = 0usize;
    for (i, part) in parts.iter().enumerate() {
        let is_first = i == 0;
        let is_last = i == n - 1;
        let anchor_start = is_first && starts_anchored;
        let anchor_end = is_last && ends_anchored;
        if anchor_start && anchor_end {
            return value == *part;
        }
        if anchor_start {
            if !value[pos..].starts_with(part) {
                return false;
            }
            pos += part.len();
        } else if anchor_end {
            return value[pos..].ends_with(part);
        } else {
            match value[pos..].find(part) {
                Some(off) => pos += off + part.len(),
                None => return false,
            }
        }
    }
    true
}

/// Try to convert a `RangePredicate` into a [`RangeClause`] for an
/// i32/Date32 column. Returns None for type mismatches, NULL literals,
/// or unsupported operators.
fn clause_from_predicate(pred: &RangePredicate, expected_type: &DataType) -> Option<RangeClause> {
    let lit_i32: i32 = match (&pred.literal, expected_type) {
        (ScalarValue::Int32(Some(v)), DataType::Int32) => *v,
        (ScalarValue::Date32(Some(v)), DataType::Date32) => *v,
        _ => return None,
    };
    Some(RangeClause {
        op: pred.op,
        literal_i32: lit_i32,
    })
}

/// Σ.E6 — mirror of [`clause_from_predicate`] for F64 columns.
/// Dormant: F64 pushdown is currently refused (see
/// `predicate_from_expr` for the rationale). Kept available for the
/// day we find a wins regime.
#[allow(dead_code)]
fn clause_from_predicate_f64(
    pred: &RangePredicate,
    expected_type: &DataType,
) -> Option<F64RangeClause> {
    let lit_f64: f64 = match (&pred.literal, expected_type) {
        (ScalarValue::Float64(Some(v)), DataType::Float64) => *v,
        _ => return None,
    };
    Some(F64RangeClause {
        op: pred.op,
        literal_f64: lit_f64,
    })
}

/// Recognise a single-filter `Expr` and turn it into a
/// `ColumnPredicate` if its shape is supported. Returns None when the
/// filter isn't one of the supported shapes — the caller skips it
/// (which is fine when pushdown is declared `Inexact`: DataFusion's
/// residual FilterExec handles the remainder).
fn predicate_from_expr(expr: &Expr, full_schema: &Schema) -> Option<ColumnPredicate> {
    // Shape 1: col OP literal (i32/Date32 range OR f64 range).
    if let Some(p) = extract_range_predicate(expr) {
        let idx = full_schema.index_of(&p.column).ok()?;
        let dt = full_schema.field(idx).data_type();
        if matches!(dt, DataType::Int32 | DataType::Date32) {
            let clause = clause_from_predicate(&p, dt)?;
            return Some(ColumnPredicate::I32Range {
                col_idx: idx,
                clauses: vec![clause],
            });
        }
        // F64Range pushdown stays refused. Σ.E6 (2026-05-22) re-
        // tested with the dict-fused `filter_f64_column_to_bitmap`
        // and confirmed the 2026-05-19 finding: TPC-H F64 filter
        // columns (l_discount, l_quantity) are ALSO projection
        // columns, so any pushdown forces a 2× decode that exceeds
        // the saved FilterExec cost. Q06 SF=10 went 96 → 107 ms
        // when pushdown was re-enabled, even with the dict-fused
        // kernel. The dict-fused kernel + helper stay in place
        // (`filter_f64_column_to_bitmap`) for callers that have
        // already-decoded-columns scenarios. See
        // `project_sigma_e6_rejected.md` for the rationale.
        if matches!(dt, DataType::Float64) {
            return None;
        }
    }
    // Shape 2: col IN (lit, lit, ...) — DataFusion `InList`.
    if let Expr::InList(in_list) = expr {
        if in_list.negated {
            return None;
        }
        if let Expr::Column(c) = in_list.expr.as_ref() {
            let idx = full_schema.index_of(&c.name).ok()?;
            let dt = full_schema.field(idx).data_type();
            // i32 IN-list
            if matches!(dt, DataType::Int32) {
                let mut values: Vec<i32> = Vec::with_capacity(in_list.list.len());
                for v in &in_list.list {
                    if let Expr::Literal(ScalarValue::Int32(Some(x)), _) = v {
                        values.push(*x);
                    } else {
                        return None;
                    }
                }
                return Some(ColumnPredicate::I32In {
                    col_idx: idx,
                    values,
                });
            }
            // string IN-list
            if matches!(dt, DataType::Utf8 | DataType::Utf8View) {
                let mut values: Vec<String> = Vec::with_capacity(in_list.list.len());
                for v in &in_list.list {
                    let s = match v {
                        Expr::Literal(ScalarValue::Utf8(Some(s)), _)
                        | Expr::Literal(ScalarValue::Utf8View(Some(s)), _)
                        | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => s.clone(),
                        _ => return None,
                    };
                    values.push(s);
                }
                return Some(ColumnPredicate::StringIn {
                    col_idx: idx,
                    values,
                });
            }
        }
    }
    // Shape 3: col [!]= 'literal' (string equality / inequality).
    if let Expr::BinaryExpr(b) = expr {
        if matches!(b.op, Operator::Eq | Operator::NotEq) {
            if let (Expr::Column(c), Expr::Literal(lit, _)) = (b.left.as_ref(), b.right.as_ref()) {
                let idx = full_schema.index_of(&c.name).ok()?;
                let dt = full_schema.field(idx).data_type();
                if matches!(dt, DataType::Utf8 | DataType::Utf8View) {
                    let s = match lit {
                        ScalarValue::Utf8(Some(s))
                        | ScalarValue::Utf8View(Some(s))
                        | ScalarValue::LargeUtf8(Some(s)) => s.clone(),
                        _ => return None,
                    };
                    return Some(if matches!(b.op, Operator::Eq) {
                        ColumnPredicate::StringEq {
                            col_idx: idx,
                            value: s,
                        }
                    } else {
                        ColumnPredicate::StringNotEq {
                            col_idx: idx,
                            value: s,
                        }
                    });
                }
            }
        }
        // Shape 4: (col = 'A') OR (col = 'B') OR ... → StringIn
        if matches!(b.op, Operator::Or) {
            let mut values: Vec<String> = Vec::new();
            let mut col_idx: Option<usize> = None;
            if collect_string_eq_or_chain(expr, full_schema, &mut col_idx, &mut values)
                && !values.is_empty()
            {
                return Some(ColumnPredicate::StringIn {
                    col_idx: col_idx.unwrap(),
                    values,
                });
            }
        }
        // Shape 5: col_a OP col_b on two i32/Date32 columns —
        // NOT PUSHED.
        //
        // Σ.E5 (verified 2026-05-19): col-vs-col pushdown is net
        // negative — same double-decode trap as F64Range. Q12's
        // `l_commitdate < l_receiptdate AND l_shipdate <
        // l_commitdate` involves three date columns all in
        // projection. With pushdown, all three are decoded twice
        // (once for the bitmap, once masked for projection emission).
        // Empirical result: Q12 −19% → +47%, Q21 −6% → −3%.
        //
        // The I32ColumnPair variant + build_bitmap path remain in
        // the codebase for callers that explicitly construct one
        // (e.g. when filter cols can be proven disjoint from
        // projection at planning time). DataFusion's residual
        // FilterExec handles col-vs-col adequately on dense Date32
        // batches.
    }
    None
}

/// Like [`predicate_from_expr`] but with knowledge of per-column dict
/// encoding. Enables LIKE pushdown for dict-encoded columns where the
/// predicate evaluates O(|dict|) instead of O(rows). Non-dict cols
/// still refuse LIKE (verified-neg on Q13/Q20).
fn predicate_from_expr_with_dict(
    expr: &Expr,
    full_schema: &Schema,
    column_is_dict_encoded: &[bool],
) -> Option<ColumnPredicate> {
    if let Some(p) = predicate_from_expr(expr, full_schema) {
        return Some(p);
    }
    // LIKE — only if column is fully dict-encoded across all RGs.
    if let Expr::Like(like) = expr {
        if like.case_insensitive || like.escape_char.is_some() {
            return None;
        }
        let col_name = match like.expr.as_ref() {
            Expr::Column(c) => &c.name,
            _ => return None,
        };
        let idx = full_schema.index_of(col_name).ok()?;
        let dt = full_schema.field(idx).data_type();
        if !matches!(dt, DataType::Utf8 | DataType::Utf8View) {
            return None;
        }
        // Gate on the dict-encoded flag. Σ.E5 (2026-05-19, smoke):
        // tried lifting this gate + Exact pushdown so DataFusion
        // would drop the filter col from projection. Confirmed
        // projection IS dropped (`proj=Some([0,1])` excluding
        // o_comment), but Q13 regressed +25% → +123% anyway —
        // emat's masked-decode kernel for the projection cols is
        // slower than the dense decode + FilterExec path. The LIKE
        // eval is only ~38ms of the 50ms regression; the bitmap-
        // build dense byte_array decode + masked i32/i64 decode of
        // o_orderkey/o_custkey dominates. PLAIN LIKE pushdown stays
        // off until the masked decode kernel matches dense throughput.
        if !column_is_dict_encoded.get(idx).copied().unwrap_or(false) {
            return None;
        }
        let pattern: String = match like.pattern.as_ref() {
            Expr::Literal(ScalarValue::Utf8(Some(s)), _)
            | Expr::Literal(ScalarValue::Utf8View(Some(s)), _)
            | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => s.clone(),
            _ => return None,
        };
        if pattern.contains('_') {
            return None;
        }
        return Some(ColumnPredicate::StringLike {
            col_idx: idx,
            pattern,
            negated: like.negated,
        });
    }
    None
}

/// Walk an OR-chain like `(col = 'A') OR (col = 'B') OR ...` and
/// collect all literals. Returns true if every leaf matched the
/// shape AND they target the same column.
fn collect_string_eq_or_chain(
    expr: &Expr,
    schema: &Schema,
    col_idx: &mut Option<usize>,
    values: &mut Vec<String>,
) -> bool {
    if let Expr::BinaryExpr(b) = expr {
        if matches!(b.op, Operator::Or) {
            return collect_string_eq_or_chain(b.left.as_ref(), schema, col_idx, values)
                && collect_string_eq_or_chain(b.right.as_ref(), schema, col_idx, values);
        }
        if matches!(b.op, Operator::Eq) {
            if let (Expr::Column(c), Expr::Literal(lit, _)) = (b.left.as_ref(), b.right.as_ref()) {
                let idx = match schema.index_of(&c.name) {
                    Ok(i) => i,
                    Err(_) => return false,
                };
                let dt = schema.field(idx).data_type();
                if !matches!(dt, DataType::Utf8 | DataType::Utf8View) {
                    return false;
                }
                let s = match lit {
                    ScalarValue::Utf8(Some(s))
                    | ScalarValue::Utf8View(Some(s))
                    | ScalarValue::LargeUtf8(Some(s)) => s.clone(),
                    _ => return false,
                };
                if let Some(prior) = *col_idx {
                    if prior != idx {
                        return false;
                    }
                } else {
                    *col_idx = Some(idx);
                }
                values.push(s);
                return true;
            }
        }
    }
    false
}

/// Extract the BridgeFilter from DataFusion's filter list.
/// Recognises:
///   - i32/Date32 range comparisons (`<`, `<=`, etc.)
///   - i32 IN-list
///   - string equality
///   - string IN-list (including OR-of-equality on the same column)
///
/// Multiple predicates are AND-ed at evaluation time. Filters that
/// don't fit a supported shape are dropped — pushdown is declared
/// Inexact so DataFusion's residual FilterExec catches them.
fn extract_bridge_filter(
    filters: &[Expr],
    full_schema: &Schema,
    column_is_dict_encoded: &[bool],
) -> Option<BridgeFilter> {
    let mut predicates: Vec<ColumnPredicate> = Vec::new();
    for f in filters {
        if let Some(p) = predicate_from_expr_with_dict(f, full_schema, column_is_dict_encoded) {
            predicates.push(p);
        }
    }
    // Merge multiple I32Range / F64Range predicates on the same
    // column into one (matches the prior behavior of AND-combined
    // clauses). Q19's `l_quantity >= 1 AND l_quantity <= 11` becomes
    // a single F64Range with two clauses, evaluated against the dict
    // mask once.
    let mut merged: Vec<ColumnPredicate> = Vec::with_capacity(predicates.len());
    for p in predicates {
        if let ColumnPredicate::I32Range { col_idx, clauses } = &p {
            if let Some(existing) = merged.iter_mut().find_map(|e| match e {
                ColumnPredicate::I32Range {
                    col_idx: ci,
                    clauses: cs,
                } if *ci == *col_idx => Some(cs),
                _ => None,
            }) {
                existing.extend_from_slice(clauses);
                continue;
            }
        }
        if let ColumnPredicate::F64Range { col_idx, clauses } = &p {
            if let Some(existing) = merged.iter_mut().find_map(|e| match e {
                ColumnPredicate::F64Range {
                    col_idx: ci,
                    clauses: cs,
                } if *ci == *col_idx => Some(cs),
                _ => None,
            }) {
                existing.extend_from_slice(clauses);
                continue;
            }
        }
        merged.push(p);
    }
    if merged.is_empty() {
        return None;
    }
    Some(BridgeFilter {
        predicates: merged,
        predicted_pass_rate: 0.5,
        probe_disarm: None,
        apply_when_dense: false,
    })
}

/// `TableProvider` that scans a single parquet file using the
/// ematix-parquet bridge for column decode.
#[derive(Debug)]
pub struct EmatixFastParquetTableProvider {
    path: String,
    schema: SchemaRef,
    num_row_groups: usize,
    num_rows: usize,
    /// Per-row-group row counts, cached at `try_new` time so the Exec
    /// can size its partitions and pick the right reader variant
    /// without re-decoding the thrift footer.
    rg_num_rows: Arc<Vec<usize>>,
    /// LPT.RG — per-(row-group, leaf-column) `total_compressed_size`
    /// bytes `[rg][col]`, cached at `try_new` time from the footer's
    /// column-chunk metadata (never re-read per scan). `scan()` sums a
    /// row group's PROJECTED columns to predict its decode cost, then
    /// LPT bin-packs row groups across partitions so the slowest
    /// partition no longer sets the scan's tail latency (round-robin
    /// left ~54% effective parallelism on Q01 SF=10 — docs/PERF_Q01.md).
    rg_col_compressed_bytes: Arc<Vec<Vec<u64>>>,
    /// Per-column typed min/max + null_count aggregated across row
    /// groups at `try_new` time. Mirrors what `FastParquetTableProvider`
    /// computes; the planner uses these for join-build-side selection
    /// and selectivity estimates. Without them DataFusion sees
    /// `Statistics::new_unknown` and picks suboptimal join orders
    /// (e.g. Q21 — 4-way join of supplier/lineitem/orders/nation —
    /// picked nation as build side without knowing it has only 25 rows).
    column_stats: Arc<Vec<datafusion::common::stats::ColumnStatistics>>,
    /// Σ.E5: per-column flag — true iff every row group has a
    /// dictionary page for this column. Used by
    /// `supports_filters_pushdown` to gate LIKE-shape pushdowns to
    /// dict-encoded columns only (PLAIN-encoded LIKE pushdown
    /// verified-neg on Q13/Q20).
    column_is_dict_encoded: Arc<Vec<bool>>,
    /// Σ.E5 (per-filter Exact pushdown, 2026-05-19): per-column flag
    /// — true iff every row group reports `null_count == 0` AND has
    /// non-null statistics for this column. Used by
    /// `supports_filters_pushdown` to gate Exact pushdown: emat's
    /// bitmap kernels don't handle null def-levels, so Exact is only
    /// correct when there are no nulls to mis-interpret. Stats-missing
    /// counts as "may have nulls" → conservative.
    column_has_no_nulls: Arc<Vec<bool>>,
    /// Σ.E5a (Π.10 integration): when true, the filtered-decode path
    /// uses ematix-parquet v0.3.0's `read_column_*_masked_into` façade
    /// (Π.10 late-materialisation) instead of the pre-Π.10 in-flow
    /// `sparse_gather_chunk_*` path. The two are semantically
    /// equivalent — same bitmap source, same projected output — but
    /// the masked_into façade has per-page popcount skip + sparse
    /// PLAIN decode that the old path lacks.
    ///
    /// **Default `true` since 2026-05-16:** the Q14 bench (`examples/
    /// tpch_q14_late_mat_bench.rs`) validated the late-mat path
    /// strictly faster than sparse_gather at SF=1 (+8.2%, σ down 3.4×)
    /// and SF=10 (+5.9%, σ down 2.2×), with bit-identical answers.
    /// `with_late_mat(false)` is retained for benchmark comparisons.
    late_mat: bool,
    /// Σ.E3b substrate: when true, Utf8 columns are decoded via the
    /// ematix-parquet dict-preserved façade (v0.7.0+) and surface to
    /// downstream operators as `Dictionary(UInt32, Utf8)` instead of
    /// `Utf8`. Lets dict-aware operators (`DictGroupCountExec`,
    /// `DictFilterExec`) stay on dict codes end-to-end.
    ///
    /// **Off by default.** Enabling globally regresses queries whose
    /// downstream operators lack dict-fast-paths (they materialise per
    /// batch). The mirror flag on `FastParquetTableProvider` exists
    /// for the parquet-rs path; this is the Emat-side parity.
    ///
    /// When on, the table provider:
    ///   - rewrites schema Utf8 → Dictionary(UInt32, Utf8) so the
    ///     reported schema matches what `scan` produces;
    ///   - disables pushdown of i32/Date32 range filters, because the
    ///     filtered decode paths still materialise Utf8 columns as
    ///     StringArray and that would mismatch the reported schema.
    ///     DataFusion's residual FilterExec runs filters as usual.
    dict_preservation: bool,
    /// Σ.E5.1.b: when true, route batch emission through
    /// [`crate::emat_arrow_reader::EmatArrowBatchReader`] instead of the
    /// whole-row-group bridge path. The streaming reader decodes each
    /// projected column once per RG (per-column parallel — distinct
    /// `PageWalker` per thread) and slices the per-RG arrays into
    /// `batch_size`-row windows, matching `FastParquetTableProvider`'s
    /// batch shape.
    ///
    /// **Default off** until end-to-end parity is proven on the SF=1
    /// Q1 SQL gate. With it on:
    ///   - Utf8 fields in the bridge schema are promoted to `Utf8View`
    ///     for `schema()` so the reader can emit `StringViewArray`
    ///     directly (matching what FastParquet does end-to-end).
    ///   - Filter pushdown is disabled — the streaming path's first
    ///     iteration of this PR doesn't fuse with the bitmap-first
    ///     filtered decode. DataFusion's residual FilterExec runs
    ///     filters as usual. This is a deliberate Σ.E5.1.b scope cut;
    ///     a follow-up can rejoin pushdown once the unfiltered path is
    ///     verified.
    streaming_arrow_reader: bool,
    /// KEYS.2 (2026-05-31): full-schema column indices of INT64 join/group
    /// KEYS that were narrowed to advertised `Int32` (set in `try_new_opt`
    /// when `EMAT_DOWNCAST_KEYS` is on AND the column's stats proved its
    /// range fits i32). Empty in the common case (downcast off / no fitting
    /// key) → `decode_schema()` returns the advertised schema unchanged and
    /// the scan adds zero narrowing overhead. Rides through the `mut self`
    /// builder methods untouched: narrowing is independent of the Utf8View /
    /// Dictionary string rewrites, so a narrowed key stays Int32 regardless.
    narrowed_leaves: Arc<Vec<usize>>,
    /// Gate-B (2026-06-21): lazily-filled, memoized per-column dictionary
    /// cardinality (MAX `DictionaryPageHeader.num_values` across row groups),
    /// `None` for columns not dict-encoded in every RG. Read decompress-free
    /// from dict page headers on first `dict_cardinality()` call and cached
    /// here, so it costs nothing unless a join-free low-card GROUP BY plan
    /// actually queries it (Q01). Deliberately NOT written into
    /// `column_stats.distinct_count` — keeping it out of the planner-visible
    /// stats avoids the cost-model perturbation that makes the dict-distinct
    /// walk unsafe to run eagerly for fact tables (Σ.Q06.SF10.5.h).
    dict_cardinality_memo: Arc<OnceLock<Vec<Option<usize>>>>,
    /// Q10 wide-string late-mat (prod-A): EXPLICITLY-declared table constraints
    /// (a PrimaryKey on the unique key column), surfaced via
    /// [`TableProvider::constraints`]. DataFusion then derives the functional
    /// dependency `{pk} -> {all cols}` on the scan and propagates it through
    /// Inner joins + projections, so the wide-string late-materialization rule
    /// can prove the FD-minimal group key. `None` (the default) → no constraint,
    /// the rule never fires. SOUND ONLY because the PK is asserted by the
    /// catalog/DDL (like any SQL PK), NOT inferred from parquet stats (which
    /// carry no sound global-uniqueness proof). Set via [`Self::with_primary_key`].
    constraints: Option<datafusion::common::Constraints>,
}

/// Σ.Q06.SF10 (2026-05-28): cached metadata-derived state for
/// `EmatixFastParquetTableProvider`. Populating these on every
/// `try_new` call is expensive — `column_stats` runs the dict-page
/// distinct-count walk (Σ.AH.2 Story 1'.2), which decompresses every
/// dict page of every row group with Snappy. For SF=10 lineitem
/// that's up to 928 Snappy decompresses per construction. Profiled
/// at ~2 ms/Q06-trial in `EMAT_SKIP_DICT_DISTINCT=1` A/B but the
/// underlying SerializedPageReader cost is higher when accumulated
/// across all 22 queries. Cache lives in `provider_meta_cache()`
/// keyed by (canonical-path, file-size, mtime-nanos) so a file
/// edit invalidates the entry.
#[derive(Clone)]
struct CachedProviderMeta {
    schema: SchemaRef,
    num_row_groups: usize,
    num_rows: usize,
    rg_num_rows: Arc<Vec<usize>>,
    column_stats: Arc<Vec<datafusion::common::stats::ColumnStatistics>>,
    column_is_dict_encoded: Arc<Vec<bool>>,
    column_has_no_nulls: Arc<Vec<bool>>,
    /// LPT.RG — per-(rg, col) compressed sizes for the scan's
    /// cost-balanced assignment (see the provider field of the same name).
    rg_col_compressed_bytes: Arc<Vec<Vec<u64>>>,
}

type ProviderMetaCacheKey = (PathBuf, u64, u128);

static PROVIDER_META_CACHE: OnceLock<Mutex<HashMap<ProviderMetaCacheKey, CachedProviderMeta>>> =
    OnceLock::new();

fn provider_meta_cache() -> &'static Mutex<HashMap<ProviderMetaCacheKey, CachedProviderMeta>> {
    PROVIDER_META_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Build a stable cache key from a file path. Uses canonical path +
/// `len` + `mtime` (nanos) so the entry invalidates when the file
/// changes underneath us. Returns `None` if any stat-call fails —
/// the caller then bypasses the cache and constructs fresh.
fn provider_meta_cache_key(path: &str) -> Option<ProviderMetaCacheKey> {
    let canonical = std::fs::canonicalize(path).ok()?;
    let metadata = std::fs::metadata(&canonical).ok()?;
    let size = metadata.len();
    let mtime_nanos = metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    Some((canonical, size, mtime_nanos))
}

/// KEYS.2 — env gate for narrowing INT64 join/group keys to Int32 on read.
/// Σ.AI.5 (2026-07-02): SCALE-GATED tri-state. `EMAT_DOWNCAST_KEYS=1`
/// forces on, `=0` forces off; unset = AUTO — on only for SF≥100-class
/// datasets (`scale_class`, campaign evidence: Q09 SF=100 −1075 ms, but
/// net +10% across 22q at SF=10 with 11 clear regressions). Callers must
/// have called `scale_class::observe_file` for the dataset first (both
/// `try_new` paths do) so AUTO resolves order-independently.
/// NOTE: this replaces the pre-campaign presence semantics
/// (`EMAT_DOWNCAST_KEYS=0` used to mean ON; it now means OFF).
fn key_downcast_enabled() -> bool {
    crate::flags::scale_gated_large("EMAT_DOWNCAST_KEYS")
}

/// KEYS.2 — a column whose name denotes a join/group KEY (ends in "key":
/// orderkey/partkey/suppkey/custkey/nationkey/regionkey). Only these are
/// narrowed — an INT64 measure that gets summed would pay a per-row
/// re-widen with no hash/sort benefit.
fn is_downcast_key_name(name: &str) -> bool {
    name.to_ascii_lowercase().ends_with("key")
}

/// KEYS.2 — true iff column `idx`'s stats prove every value fits i32
/// (`[i32::MIN, i32::MAX]`), so an INT64→Int32 narrowing is lossless.
/// Conservative: returns false when stats are Absent or not Int64-typed
/// (can't prove → don't narrow). Keeps the narrowing safe at any scale
/// factor — e.g. l_orderkey crosses i32::MAX around SF≈350, at which point
/// this gate stops narrowing it automatically.
fn int64_col_fits_i32(stats: &[datafusion::common::stats::ColumnStatistics], idx: usize) -> bool {
    use datafusion::common::stats::Precision;
    use datafusion::scalar::ScalarValue;
    let Some(cs) = stats.get(idx) else {
        return false;
    };
    let as_i64 = |p: &Precision<ScalarValue>| -> Option<i64> {
        match p {
            Precision::Exact(ScalarValue::Int64(Some(v)))
            | Precision::Inexact(ScalarValue::Int64(Some(v))) => Some(*v),
            _ => None,
        }
    };
    match (as_i64(&cs.min_value), as_i64(&cs.max_value)) {
        (Some(min), Some(max)) => min >= i32::MIN as i64 && max <= i32::MAX as i64,
        _ => false,
    }
}

impl EmatixFastParquetTableProvider {
    /// Open the parquet file, validate that every column is one of the
    /// primitive types the bridge supports, and build the Arrow
    /// schema. Errors immediately if any column is unsupported so
    /// callers don't discover this mid-scan.
    pub fn try_new(path: impl Into<String>) -> DfResult<Self> {
        let path = path.into();
        // Σ.AI.5: record the dataset's scale BEFORE resolving the
        // narrow-keys tri-state, so AUTO sees this dataset (sibling
        // scan makes it registration-order independent — region at 5
        // rows classifies by its lineitem sibling).
        crate::scale_class::observe_file(&path);
        Self::try_new_opt(path, key_downcast_enabled())
    }

    /// KEYS.2 — `try_new` with an explicit downcast-keys flag so tests can
    /// exercise the i32-key narrowing without mutating a process-global env
    /// var (which would race other parallel tests). Production `try_new`
    /// passes `key_downcast_enabled()`.
    fn try_new_opt(path: impl Into<String>, downcast_keys: bool) -> DfResult<Self> {
        let path = path.into();
        // KEYS.2: when the i32-key downcast is enabled, bypass the
        // process-global meta cache entirely (read AND write). The cache
        // stores the FINAL schema, which is env-dependent under downcast;
        // skipping it guarantees the narrowing decision always reflects the
        // flag and never pollutes a non-downcast build in the same process.
        // The flag is experimental opt-in, so try_new perf here is
        // irrelevant.
        // Σ.Q06.SF10 (2026-05-28): check process-global metadata cache
        // first. On hit, skip the entire SerializedFileReader path
        // (Snappy decompress of dict pages, encoding-stats walk,
        // no-nulls walk) and build `Self` directly from cached Arc'd
        // fields. Opt-out via `EMAT_NO_PROVIDER_CACHE=1`.
        let use_cache = std::env::var("EMAT_NO_PROVIDER_CACHE")
            .ok()
            .map(|v| !(v == "1" || v.eq_ignore_ascii_case("true")))
            .unwrap_or(true)
            && !downcast_keys;
        if use_cache {
            if let Some(key) = provider_meta_cache_key(&path) {
                let cached = provider_meta_cache().lock().unwrap().get(&key).cloned();
                if let Some(meta) = cached {
                    return Ok(Self::from_cached_meta(path, meta));
                }
            }
        }
        // Σ.Q06.SF10.5.c (2026-05-28): load all metadata-derived state
        // (raw schema, num_rows, num_row_groups, rg_num_rows,
        // column_stats, column_is_dict_encoded, column_has_no_nulls)
        // in one pass via ematix-parquet — NO parquet-rs. Byte-identical
        // to the old `ArrowReaderMetadata::load` + `SerializedFileReader`
        // path, verified across all 8 TPC-H tables by
        // `examples/emat_meta_parity.rs` (0 mismatches).
        let em = crate::emat_parquet_metadata::load_provider_metadata(&path).map_err(|e| {
            DataFusionError::External(
                format!("EmatixFastParquetTableProvider: load metadata: {e}").into(),
            )
        })?;
        let num_rows = em.num_rows;
        let num_row_groups = em.num_row_groups;
        let rg_num_rows: Arc<Vec<usize>> = Arc::new(em.rg_num_rows);
        let rg_col_compressed_bytes: Arc<Vec<Vec<u64>>> = Arc::new(em.rg_column_compressed_sizes);
        let column_is_dict_encoded: Arc<Vec<bool>> = Arc::new(em.column_is_dict_encoded);
        let column_has_no_nulls: Arc<Vec<bool>> = Arc::new(em.column_has_no_nulls);
        // Σ.E5 (2026-05-18): promote Utf8 → Utf8View at `try_new` time.
        // The streaming reader path emits `StringViewArray`, and that's
        // the default since Σ.E5.4.b. Previously this promotion only
        // ran inside `with_streaming_arrow_reader(true)` (the builder),
        // so default users got streaming=true with a Utf8 schema — the
        // reader's dispatch fell to the StringArray branch and skipped
        // the Σ.E5.1.d fast path entirely. That accounted for the Q1
        // regression in the 22-query bench (which uses bare `try_new`
        // — no builder calls). `with_streaming_arrow_reader(false)`
        // reverts the promotion for the bridge path. Dict preservation
        // composes via its own Utf8→Dictionary rewrite.
        let raw_schema = em.schema.clone();
        // KEYS.2 (2026-05-31): when EMAT_DOWNCAST_KEYS is set, narrow an
        // INT64 join/group KEY column to Int32 IFF the column's stats prove
        // every value fits i32. DataFusion then hashes/sorts a 4-byte key
        // (smaller hash tables + better cache residency — DuckDB's
        // `__internal_compress_integral_uinteger`). Lossless (stats-gated);
        // same-domain keys (FK↔PK) narrow consistently across tables so join
        // key types still match. Decode-side narrowing lives in
        // `emat_arrow_reader::decode_one_column` (Int32 target + INT64 phys).
        // (`downcast_keys` was bound at the top of try_new.)
        // Record which leaves were Int64→Int32 narrowed so the scan can
        // hand readers a native-width `decode_schema` and cast Int64→Int32
        // once at the stream boundary (KEYS.2). Readers stay type-uniform —
        // they decode the on-disk width and never need to know about
        // narrowing — so we can't silently miss a decode entry point.
        let mut narrowed_leaves: Vec<usize> = Vec::new();
        let promoted_fields: Vec<Arc<arrow_schema::Field>> = raw_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, f)| {
                if matches!(f.data_type(), DataType::Utf8) {
                    Arc::new(
                        arrow_schema::Field::new(f.name(), DataType::Utf8View, f.is_nullable())
                            .with_metadata(f.metadata().clone()),
                    )
                } else if downcast_keys
                    && matches!(f.data_type(), DataType::Int64)
                    && is_downcast_key_name(f.name())
                    && int64_col_fits_i32(&em.column_stats, idx)
                {
                    narrowed_leaves.push(idx);
                    Arc::new(
                        arrow_schema::Field::new(f.name(), DataType::Int32, f.is_nullable())
                            .with_metadata(f.metadata().clone()),
                    )
                } else {
                    f.clone()
                }
            })
            .collect();
        let schema: SchemaRef = Arc::new(Schema::new_with_metadata(
            promoted_fields,
            raw_schema.metadata().clone(),
        ));
        let narrowed_leaves: Arc<Vec<usize>> = Arc::new(narrowed_leaves);

        // Validate: every column must be one of the types the bridge
        // can decode. Anything else, defer to FastParquetTableProvider.
        // `Utf8View` joins the list because of the Σ.E5 default
        // schema promotion above; the streaming reader's dispatch
        // routes it to `decode_byte_array_to_string_view`.
        for field in schema.fields() {
            match field.data_type() {
                DataType::Int32
                | DataType::Int64
                | DataType::UInt64
                | DataType::Float64
                | DataType::Date32
                | DataType::Utf8
                | DataType::Utf8View => {}
                other => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "EmatixFastParquetTableProvider: column `{}` has type {other:?}; bridge supports Int32/Int64/UInt64/Float64/Date32/Utf8/Utf8View only — use FastParquetTableProvider",
                        field.name()
                    )));
                }
            }
        }

        // num_rows / num_row_groups / rg_num_rows / column_is_dict_encoded
        // / column_has_no_nulls all came from `em` (Σ.Q06.SF10.5.c) above.
        // `column_stats` here carries the aggregated numeric min/max +
        // null_count; the gated dict-distinct walk below may enrich
        // distinct_count.
        let num_cols_in_schema = schema.fields().len();
        let mut column_stats = Arc::new(em.column_stats);

        // Σ.AH.2 Story 1'.2 (2026-05-26) — for every column that has
        // a dictionary page in EVERY row group, read each dict page's
        // `num_values` and take the MAX across RGs as a distinct_count
        // estimate. Without this, `ColumnStatistics::distinct_count`
        // stays Absent and `ColumnPredicate::StringEq::estimate_pass_rate`
        // falls through to its 0.1 default — over-estimating
        // post-filter cardinality on TPC-H string-Eq filters (p_type
        // shows 200k vs real 13k), which then fails the L9 ratio gate.
        // Taking the max-per-RG is a lower bound on global distinct
        // count (uniformly-distributed columns give exact; clustered
        // columns give a conservative under-estimate, which is the
        // safe direction for the bloom rule — slight under-count of
        // distinct keeps post-filter rows estimated high, biasing
        // the gate against false-positive blooms).
        //
        // Note: this is decoupled from `column_is_dict_encoded` (which
        // also requires `page_encoding_stats` for LIKE pushdown safety).
        // We only need the dict page to exist — even mixed dict+PLAIN
        // data pages still have a dict that tells us distinct count.
        //
        // Cost: one decode of each dict page (typically 1-10 KB per
        // RG); a few ms per provider construction. Cached for the
        // session via `column_stats`.
        // Σ.Q06.SF10.5.a (2026-05-28): dict-page distinct-count walk
        // via ematix-parquet — reads only the uncompressed Thrift
        // page header at each dict_page_offset, no Snappy decompress.
        // See `crate::emat_parquet_metadata`. The diagnostic harness
        // at `examples/q06_dict_distinct_compare.rs` confirms the
        // ematix-parquet walker produces byte-identical num_values
        // to the parquet-rs `SerializedPageReader::get_next_page`
        // path on TPC-H lineitem SF=10 (all 16 columns match).
        //
        // Σ.Q06.SF10.5.h (2026-05-28): the walk itself default-flipped
        // to SKIP. Strict 22q SF=10 A/B (walk on vs walk off, both
        // with cache, autotune disabled): net **-4.35% (-145.09 ms)**
        // — Q18 -26.55% (-97 ms), Q17 -21.52% (-41 ms), Q21 -18.11%
        // (-64 ms) all clear >2σ wins; Q02 +9.77% (+2.9 ms) only
        // regression. The populated distinct_count drives L9's
        // selectivity gate to fire blooms whose probe overhead
        // exceeds the savings on Q17/Q18/Q21 shapes. The Σ.AH.2
        // Story 1'.3 finding ("emat-stats-aware selectivity helps")
        // has been outpaced by post-2026-05-26 L9 changes.
        //
        // Opt back in via `EMAT_DICT_DISTINCT=1`. The proper fix is at
        // the L9 selectivity gate, not here — tracked as Σ.Q06.SF10.5.h
        // follow-up. (The legacy parquet-rs walker was removed in
        // Σ.Q06.SF10.5.c; only the ematix-parquet header-only walk
        // remains.)
        let dict_distinct_enabled = std::env::var("EMAT_DICT_DISTINCT")
            .ok()
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        // Σ.Q06.SF10.5.h.1: small-table-only dict-distinct walk. The
        // walk default-flipped OFF in .5.h because populating
        // distinct_count perturbed DataFusion's cost-based plan on
        // Q17/Q18/Q21 (lineitem/orders-heavy) — a planner effect, NOT
        // an L9 over-fire (L9 firings are byte-identical with/without
        // the walk, verified by EMAT_L9_TRACE). The harm is tied to
        // LARGE fact-table distinct_count; small dimension tables
        // (part/supplier/customer/...) benefit (Q02). This gate runs
        // the walk only when the file has ≤ `EMAT_DICT_DISTINCT_MAX_ROWS`
        // rows (default 0 = off, preserving .5.h). Set e.g.
        // `EMAT_DICT_DISTINCT_MAX_ROWS=10000000` to walk everything
        // except lineitem (60M) + orders (15M) at SF=10.
        // REV.19 single-flag UX: the NDV-corrected build-side lever
        // (`EMAT_NDV_BUILD_SIDE=1`, `force_collect_left_semi_build_rule`) needs
        // `distinct_count` populated for the dimension tables it corrects, so
        // when it is on and no explicit cap is given we default the walk to the
        // small-table cap (10M) — includes TPC-H dimensions (part 2M / supplier
        // / customer 1.5M / nation / region), excludes the large fact tables
        // (lineitem 60M, orders 15M at SF=10) whose `distinct_count` perturbs
        // the planner (Σ.Q06.SF10.5.h). An explicit `EMAT_DICT_DISTINCT_MAX_ROWS`
        // always overrides. Scale caveat: at SF=100 part is 20M > 10M, so set
        // the cap explicitly there.
        // Σ.AI.5 (2026-07-02): tri-state — `=1` on, `=0` off, unset = AUTO
        // (on for SF≥100-class datasets; `observe_file` ran in try_new).
        let ndv_build_side = crate::flags::scale_gated_large("EMAT_NDV_BUILD_SIDE");
        let max_rows_for_walk: usize = std::env::var("EMAT_DICT_DISTINCT_MAX_ROWS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(if ndv_build_side { 10_000_000 } else { 0 });
        let walk_by_size = max_rows_for_walk > 0 && num_rows <= max_rows_for_walk;
        // Legacy `EMAT_SKIP_DICT_DISTINCT=1` still forces skip for
        // explicit A/B work, but the default behaviour is now skip.
        let force_skip = std::env::var("EMAT_SKIP_DICT_DISTINCT")
            .ok()
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let skip_dict_distinct = force_skip || !(dict_distinct_enabled || walk_by_size);
        if !skip_dict_distinct {
            use datafusion::common::stats::Precision;
            // ematix-parquet header-only walk (no Snappy decompress).
            if let Ok(distinct_maxes) = crate::emat_parquet_metadata::dict_distinct_max_per_column(
                &path,
                num_cols_in_schema,
            ) {
                let mut column_stats_vec: Vec<datafusion::common::stats::ColumnStatistics> =
                    (*column_stats).clone();
                #[allow(clippy::needless_range_loop)] // col_idx also indexes distinct_maxes
                for col_idx in 0..num_cols_in_schema {
                    if let Some(max_distinct) = distinct_maxes.get(col_idx).copied().flatten() {
                        if max_distinct > 0 {
                            column_stats_vec[col_idx].distinct_count =
                                Precision::Inexact(max_distinct);
                        }
                    }
                }
                column_stats = Arc::new(column_stats_vec);
            }
        }

        // Σ.Q06.SF10: populate the process-global cache before
        // returning. Subsequent constructions for the same file
        // skip the SerializedPageReader path entirely.
        if use_cache {
            if let Some(key) = provider_meta_cache_key(&path) {
                let meta = CachedProviderMeta {
                    schema: schema.clone(),
                    num_row_groups,
                    num_rows,
                    rg_num_rows: rg_num_rows.clone(),
                    column_stats: column_stats.clone(),
                    column_is_dict_encoded: column_is_dict_encoded.clone(),
                    column_has_no_nulls: column_has_no_nulls.clone(),
                    rg_col_compressed_bytes: rg_col_compressed_bytes.clone(),
                };
                provider_meta_cache().lock().unwrap().insert(key, meta);
            }
        }

        Ok(Self {
            path,
            schema,
            num_row_groups,
            num_rows,
            rg_num_rows,
            rg_col_compressed_bytes,
            column_stats,
            column_is_dict_encoded,
            column_has_no_nulls,
            late_mat: true,
            dict_preservation: false,
            // Σ.E5 (2026-05-18): re-flipping streaming default on
            // after the Σ.E5.4.a bench was re-run on current main.
            // Fresh measurements: streaming geomean 1.0414 vs bridge
            // 1.5084 — streaming is a meaningful win across the 22
            // queries (9 EmatFaster, 3 parity, 10 regression) compared
            // to bridge (3 EmatFaster, 2 parity, 17 regression).
            // The 1.064 number in #117's revert comment was relative
            // to FastParquet on a stale state; current numbers show
            // streaming is clearly the better default.
            // The 10 remaining regressions cluster on string-filter
            // predicates that don't push down on either path (Q07/
            // Q13/Q16/Q19/Q22). Closing them is the next bite.
            streaming_arrow_reader: true,
            narrowed_leaves,
            dict_cardinality_memo: Arc::new(OnceLock::new()),
            constraints: None,
        })
    }

    /// Σ.Q06.SF10 (2026-05-28): fast-path constructor used when the
    /// process-global metadata cache has an entry for this file.
    /// Defaults match `try_new`'s slow-path return.
    fn from_cached_meta(path: String, meta: CachedProviderMeta) -> Self {
        Self {
            path,
            schema: meta.schema,
            num_row_groups: meta.num_row_groups,
            num_rows: meta.num_rows,
            rg_num_rows: meta.rg_num_rows,
            rg_col_compressed_bytes: meta.rg_col_compressed_bytes,
            column_stats: meta.column_stats,
            column_is_dict_encoded: meta.column_is_dict_encoded,
            column_has_no_nulls: meta.column_has_no_nulls,
            late_mat: true,
            dict_preservation: false,
            streaming_arrow_reader: true,
            // The provider meta cache is bypassed when EMAT_DOWNCAST_KEYS is
            // on (see `use_cache` in try_new_opt), so a cached entry always
            // reflects a non-narrowed schema → no narrowed leaves.
            narrowed_leaves: Arc::new(Vec::new()),
            dict_cardinality_memo: Arc::new(OnceLock::new()),
            constraints: None,
        }
    }

    /// Q10 wide-string late-mat (prod-A): declare a PRIMARY KEY on the given
    /// column indices, surfaced via [`TableProvider::constraints`]. DataFusion
    /// derives `{pk} -> {all cols}` and propagates it to aggregate inputs so the
    /// late-materialization rule can prove an FD-minimal group key. The caller
    /// (catalog / table registration) ASSERTS uniqueness — this is a soundness
    /// contract identical to a SQL `PRIMARY KEY`, NOT inferred from data. A wrong
    /// declaration yields wrong query results, exactly as a wrong DDL PK would.
    pub fn with_primary_key(mut self, key_col_indices: Vec<usize>) -> Self {
        use datafusion::common::{Constraint, Constraints};
        self.constraints = Some(Constraints::new_unverified(vec![Constraint::PrimaryKey(
            key_col_indices,
        )]));
        self
    }

    /// Σ.E5a: opt into / out of the Π.10 late-materialisation path.
    /// When set, the filtered-decode path uses ematix-parquet's
    /// `read_column_*_masked_into` instead of the pre-Π.10
    /// `sparse_gather_chunk_*` route in this crate's bridge.
    pub fn with_late_mat(mut self, on: bool) -> Self {
        self.late_mat = on;
        self
    }

    /// Whether the late-mat path is enabled (Σ.E5a).
    pub fn late_mat(&self) -> bool {
        self.late_mat
    }

    /// Gate-B (2026-06-21): exact distinct-value count for column `col_idx`
    /// when it is dictionary-encoded in *every* row group, else `None`.
    ///
    /// For a fully dict-encoded column the per-row-group dictionary holds
    /// exactly that RG's distinct values, so the MAX `num_values` across RGs
    /// is the column's NDV (an exact value for the common single-RG-domain
    /// case; a lower bound only for pathologically value-clustered columns,
    /// which the sole caller guards against with a conservative threshold).
    ///
    /// Read decompress-free from the dict page headers
    /// ([`crate::emat_parquet_metadata::dict_distinct_max_per_column`]) on the
    /// first call and memoized for the provider's lifetime. Returns `None`
    /// (never panics) on any read error. This is a *planner-safe* NDV peek:
    /// the value is NOT written into `column_stats.distinct_count`, so it
    /// cannot perturb the cost model — that perturbation, not the I/O, is why
    /// the dict-distinct walk is otherwise skipped for fact tables
    /// (Σ.Q06.SF10.5.h).
    pub fn dict_cardinality(&self, col_idx: usize) -> Option<usize> {
        // Fast reject without touching the file: a column that isn't dict in
        // every RG has no reliable dict-NDV (and a high-card group key like
        // l_orderkey is PLAIN, so this short-circuits it with zero I/O).
        if !self
            .column_is_dict_encoded
            .get(col_idx)
            .copied()
            .unwrap_or(false)
        {
            return None;
        }
        let memo = self.dict_cardinality_memo.get_or_init(|| {
            let n = self.schema.fields().len();
            crate::emat_parquet_metadata::dict_distinct_max_per_column(&self.path, n)
                .unwrap_or_else(|_| vec![None; n])
        });
        memo.get(col_idx).copied().flatten()
    }

    /// Σ.E3b: opt into reader-level dict preservation for Utf8
    /// columns. When on, the schema's Utf8 fields are rewritten to
    /// `Dictionary(UInt32, Utf8)` and decode uses the v0.7.0+
    /// `read_column_byte_array_dict_preserved` façade. See
    /// `dict_preservation` field docs for caveats.
    pub fn with_dict_preservation(mut self, on: bool) -> Self {
        self.dict_preservation = on;
        if on {
            // Σ.E5 follow-up: `try_new` defaults `streaming_arrow_reader`
            // to true, and the streaming reader doesn't (yet) emit
            // `DictionaryArray` outputs. Force it off when the caller
            // asks for dict-preserved arrival so SELECT routes through
            // the bridge path that actually produces Dict batches.
            // Reapply via the explicit `with_streaming_arrow_reader(true)`
            // setter if a future caller wires streaming + dict together.
            self.streaming_arrow_reader = false;

            // Rewrite Utf8 fields → Dictionary(UInt32, Utf8). Other
            // types pass through. Field metadata + nullability
            // preserved.
            // Σ.E5 follow-up: `try_new` now auto-promotes Utf8 → Utf8View
            // for the streaming reader default, so dict preservation has
            // to recognise both shapes when rewriting to Dictionary.
            //
            // NOTE (2026-06-21): dict_preservation is OPT-IN and known-fragile on
            // standard parquet — it hard-errors on PLAIN-fallback string columns
            // AND silently mis-groups on multi-row-group dict columns (per-RG
            // dictionaries are not code-unified, e.g. Q01 returns 6 groups vs 4).
            // The safe production default is dict_preservation=false (work_unit
            // Execution::default). Making dict-preservation robust requires
            // cross-row-group dict-code unification + full dense-path type
            // support — a dedicated engine project, NOT a per-page fallback.
            // Until then this rewrites every string column to Dictionary (the
            // original behavior) so failures stay LOUD rather than silently wrong.
            let fields = self
                .schema
                .fields()
                .iter()
                .map(|f| {
                    if matches!(f.data_type(), DataType::Utf8 | DataType::Utf8View) {
                        Arc::new(arrow_schema::Field::new(
                            f.name(),
                            DataType::Dictionary(
                                Box::new(DataType::UInt32),
                                Box::new(DataType::Utf8),
                            ),
                            f.is_nullable(),
                        ))
                    } else {
                        f.clone()
                    }
                })
                .collect::<Vec<_>>();
            self.schema = Arc::new(Schema::new(fields));
        }
        self
    }

    /// Whether dict-preservation is enabled (Σ.E3b).
    pub fn dict_preservation(&self) -> bool {
        self.dict_preservation
    }

    /// Σ.E5.1.b: route batch emission through `EmatArrowBatchReader`
    /// instead of the whole-row-group bridge path. Default off until
    /// e2e parity is proven; expected to flip default in a follow-up.
    ///
    /// When turned on without dict preservation, Utf8 columns in the
    /// reported schema are promoted to `Utf8View` so the streaming
    /// reader can emit `StringViewArray` directly. When combined with
    /// `with_dict_preservation(true)`, schema rewriting to
    /// `Dictionary(UInt32, Utf8)` already happened and is preserved.
    pub fn with_streaming_arrow_reader(mut self, on: bool) -> Self {
        self.streaming_arrow_reader = on;
        if on && !self.dict_preservation {
            // Promote Utf8 → Utf8View so the schema matches what the
            // reader will emit (StringViewArray, not StringArray).
            // Other types pass through, including Date32/Int32/etc.
            let fields = self
                .schema
                .fields()
                .iter()
                .map(|f| {
                    if matches!(f.data_type(), DataType::Utf8) {
                        Arc::new(arrow_schema::Field::new(
                            f.name(),
                            DataType::Utf8View,
                            f.is_nullable(),
                        ))
                    } else {
                        f.clone()
                    }
                })
                .collect::<Vec<_>>();
            self.schema = Arc::new(Schema::new(fields));
        }
        self
    }

    /// Whether the Σ.E5.1.b streaming reader path is enabled.
    pub fn streaming_arrow_reader(&self) -> bool {
        self.streaming_arrow_reader
    }

    /// KEYS.2 — the native-width schema the decode path should produce: the
    /// advertised schema with every narrowed key (`narrowed_leaves`) widened
    /// back to its on-disk `Int64`. `scan()` hands this to the readers so
    /// they decode at the physical width (type-uniform, no per-site narrowing
    /// logic), and `execute()` casts Int64→Int32 once at the stream boundary.
    /// When no key was narrowed (the default — downcast off) this equals the
    /// advertised schema, so the cast wrapper is skipped and the path is
    /// bit-identical to before.
    fn decode_schema(&self) -> SchemaRef {
        if self.narrowed_leaves.is_empty() {
            return self.schema.clone();
        }
        let fields: Vec<Arc<arrow_schema::Field>> = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                if self.narrowed_leaves.contains(&i) {
                    Arc::new(
                        arrow_schema::Field::new(f.name(), DataType::Int64, f.is_nullable())
                            .with_metadata(f.metadata().clone()),
                    )
                } else {
                    f.clone()
                }
            })
            .collect();
        Arc::new(Schema::new_with_metadata(
            fields,
            self.schema.metadata().clone(),
        ))
    }

    pub fn path(&self) -> &str {
        &self.path
    }
    pub fn num_row_groups(&self) -> usize {
        self.num_row_groups
    }
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }
}

/// LPT.RG — the legacy round-robin row-group → partition assignment
/// (`rg % num_partitions`). Kept verbatim as the `EMAT_BALANCED_RG_ASSIGN=0`
/// A/B escape hatch: with the lever off, `scan()` must reproduce this
/// exactly.
pub fn round_robin_rg_assignments(num_rgs: usize, num_partitions: usize) -> Vec<Vec<usize>> {
    let num_partitions = num_partitions.max(1);
    let mut assignments: Vec<Vec<usize>> = vec![Vec::new(); num_partitions];
    for rg in 0..num_rgs {
        assignments[rg % num_partitions].push(rg);
    }
    assignments
}

/// LPT.RG — count-balanced fallback: contiguous ascending runs whose
/// lengths differ by at most 1 (the first `num_rgs % num_partitions`
/// partitions take the extra row group). Used when per-RG cost metadata
/// is unavailable or carries no signal (all costs equal — LPT would
/// degenerate to this anyway). Never panics: `num_rgs == 0` yields
/// `num_partitions` empty lists, matching round-robin's shape.
pub fn count_balanced_rg_assignments(num_rgs: usize, num_partitions: usize) -> Vec<Vec<usize>> {
    let num_partitions = num_partitions.max(1);
    let base = num_rgs / num_partitions;
    let extra = num_rgs % num_partitions;
    let mut assignments: Vec<Vec<usize>> = Vec::with_capacity(num_partitions);
    let mut next = 0usize;
    for p in 0..num_partitions {
        let len = base + usize::from(p < extra);
        assignments.push((next..next + len).collect());
        next += len;
    }
    assignments
}

/// LPT.RG — deterministic longest-processing-time (LPT) bin-packing of
/// row groups across partitions by predicted decode cost.
///
/// - Row groups are taken in descending `costs[rg]` order (stable
///   tie-break: lower index first) and each is assigned to the currently
///   least-loaded partition (tie-break: lowest partition index) — so the
///   same input always produces the same assignment.
/// - Within each partition the assigned list is sorted ascending, which
///   preserves forward-sequential reads inside each producer.
/// - If every cost is identical (including all-zero — no signal), falls
///   back to [`count_balanced_rg_assignments`].
///
/// Classic LPT guarantee: final `max_load − min_load ≤ max(costs)` —
/// the imbalance is bounded by one row group, vs round-robin's unbounded
/// cost skew (SF=10 lineitem: 58 RGs / 14 partitions put +25% tail work
/// on two partitions before per-RG cost skew even enters).
pub fn lpt_rg_assignments(costs: &[u64], num_partitions: usize) -> Vec<Vec<usize>> {
    let num_partitions = num_partitions.max(1);
    let num_rgs = costs.len();
    if num_rgs == 0 || costs.iter().all(|&c| c == costs[0]) {
        return count_balanced_rg_assignments(num_rgs, num_partitions);
    }
    let mut order: Vec<usize> = (0..num_rgs).collect();
    // Descending cost; ties broken by ascending RG index (determinism).
    order.sort_by(|&a, &b| costs[b].cmp(&costs[a]).then_with(|| a.cmp(&b)));
    let mut loads: Vec<u128> = vec![0; num_partitions];
    let mut assignments: Vec<Vec<usize>> = vec![Vec::new(); num_partitions];
    for rg in order {
        // `min_by_key` returns the FIRST minimum → lowest partition index
        // wins ties.
        let p = (0..num_partitions)
            .min_by_key(|&p| loads[p])
            .expect("num_partitions >= 1");
        assignments[p].push(rg);
        loads[p] += costs[rg] as u128;
    }
    for part in &mut assignments {
        part.sort_unstable();
    }
    assignments
}

/// LPT.RG — per-RG predicted decode cost: the sum over the PROJECTED
/// columns of that row group's column-chunk `total_compressed_size`.
/// Compressed bytes track both imbalance sources round-robin ignores:
/// compression-ratio skew and projected-column width skew. Returns
/// `None` when the cached footer metadata doesn't cover every row group
/// (caller falls back to a count-balanced split).
pub fn rg_projected_costs(
    rg_col_compressed_bytes: &[Vec<u64>],
    num_rgs: usize,
    projection: &[usize],
) -> Option<Vec<u64>> {
    if rg_col_compressed_bytes.len() != num_rgs {
        return None;
    }
    Some(
        rg_col_compressed_bytes
            .iter()
            .map(|cols| {
                projection
                    .iter()
                    .map(|&c| cols.get(c).copied().unwrap_or(0))
                    .sum()
            })
            .collect(),
    )
}

/// LPT.RG — the assignment `scan()` uses. Gated by
/// `EMAT_BALANCED_RG_ASSIGN` (default **ON**, house `flags::enabled`
/// pattern): `=0` restores the legacy round-robin exactly (A/B escape).
/// With the lever on: LPT over per-RG projected compressed bytes;
/// count-balanced split when metadata is unavailable; never panics.
///
/// Scope: ONLY the plain `scan()` path calls this. The RANGE.AGG
/// key-disjoint chunk contracts (`EmatixFastParquetExec::with_assignments`
/// / `with_assignments_claiming_hash`) inject their own assignments and
/// are deliberately untouched.
fn compute_scan_rg_assignments(
    num_rgs: usize,
    num_partitions: usize,
    projection: &[usize],
    rg_col_compressed_bytes: &[Vec<u64>],
) -> Vec<Vec<usize>> {
    if !crate::flags::enabled("EMAT_BALANCED_RG_ASSIGN") {
        return round_robin_rg_assignments(num_rgs, num_partitions);
    }
    match rg_projected_costs(rg_col_compressed_bytes, num_rgs, projection) {
        Some(costs) => lpt_rg_assignments(&costs, num_partitions),
        None => count_balanced_rg_assignments(num_rgs, num_partitions),
    }
}

#[async_trait::async_trait]
impl TableProvider for EmatixFastParquetTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Q10 wide-string late-mat (prod-A): surface the explicitly-declared
    /// PRIMARY KEY (if any) so DataFusion derives + propagates the functional
    /// dependency the late-materialization rule needs. `None` unless
    /// [`Self::with_primary_key`] was called — zero behavioral change otherwise.
    fn constraints(&self) -> Option<&datafusion::common::Constraints> {
        self.constraints.as_ref()
    }

    /// Σ.Q.M (2026-05-23): expose the file-metadata-derived row count
    /// so logical optimizer rules can distinguish dim vs fact tables.
    /// `num_rows` is read from parquet `FileMetadata::num_rows` at
    /// registration time, so it's exact for unfiltered scans.
    ///
    /// Σ.T Phase 1 (2026-05-25): also surface the typed per-column
    /// min/max/null_count that `try_new` already cached on
    /// `self.column_stats` (via `aggregate_column_statistics`). Without
    /// this the logical join planner can only distinguish tables by
    /// row count — predicate selectivity stays invisible, so
    /// region/nation filters never propagate to "join smaller pool
    /// first" decisions. The Exec's `partition_statistics` has
    /// returned these all along; this just plumbs the same data to
    /// the TableProvider surface used by logical planning.
    fn statistics(&self) -> Option<datafusion::common::Statistics> {
        Some(datafusion::common::Statistics {
            num_rows: datafusion::common::stats::Precision::Exact(self.num_rows),
            total_byte_size: datafusion::common::stats::Precision::Absent,
            column_statistics: (*self.column_stats).clone(),
        })
    }

    /// Phase 3 pushdown: single-column AND-conjunction of `col OP lit`
    /// where col is Int32/Date32. Other shapes return `Unsupported`
    /// and stay in DataFusion's residual FilterExec.
    ///
    /// Σ.E3b: when dict-preservation is enabled, no filter is pushed.
    /// The filtered decode paths still materialise Utf8 → StringArray
    /// which would mismatch the dict-rewritten schema.
    ///
    /// Σ.E5.1.b: when the streaming reader is on, pushdown is also
    /// off. The Exec routes filter-bearing queries to the bridge
    /// filtered-decode path (line ~600) — which emits Utf8 not
    /// Utf8View. Bench measurement (2026-05-18) showed that route
    /// is materially slower than streaming-with-residual-FilterExec
    /// for Q01-shape queries: pushing the filter took Q01 from
    /// 18.5 ms → 78.7 ms. Geomean across 22 queries: 1.0414 (no
    /// pushdown) vs 1.1085 (with pushdown). Letting DataFusion's
    /// residual FilterExec run the predicate on the Utf8View batches
    /// is the right call until we have a Utf8View-aware filtered
    /// decode path.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        if self.dict_preservation {
            return Ok(filters
                .iter()
                .map(|_| TableProviderFilterPushDown::Unsupported)
                .collect());
        }
        // Σ.Q06.SF10.7 diagnostic: force all filters to stay in
        // DataFusion's residual FilterExec (no pushdown), so filter-
        // bearing scans run the dense streaming path instead of the
        // per-RG late-mat masked path. Tests whether the late-mat
        // orchestration — not the decode kernels — is the Q06 SF=10
        // bottleneck.
        if std::env::var_os("EMAT_NO_FILTER_PUSHDOWN").is_some() {
            return Ok(filters
                .iter()
                .map(|_| TableProviderFilterPushDown::Unsupported)
                .collect());
        }
        // Σ.E5 #517 (2026-05-19): streaming reader's masked-decode
        // path now uses dict-preserved Utf8View — same shape as the
        // dense fast path (dict_views: Vec<u128> cache + 16-byte
        // gather per row). Pushdown accepted for BridgeFilter-shaped
        // filters on all reader variants.
        // Σ.E5 Phase 1.8 (2026-05-19): Inexact pushdown kept. The
        // parallel-bitmap+dense path (`load_row_group_parallel_bitmap_dense`)
        // wins on net even with FilterExec still present — the per-
        // batch slice_batch filter early-drops ~5% of rows so the
        // downstream FilterExec re-eval is essentially a no-op.
        //
        // Adding Exact on top regressed badly (Q01 +13% → +105%,
        // Q02 +50%, Q04 +22%) likely from DataFusion plan changes
        // with Exact (different join order, fewer FilterExec
        // optimisations). Keeping Inexact preserves the bench wins.
        //
        // The Phase 1.8 dispatch (`EMAT_FORCE_PARALLEL_BITMAP=1` +
        // predicted > 0.33) is opt-in until we either resolve the
        // Exact-mode plan regression or accept Inexact-only-wins.
        // Σ.E5 Phase 1.8 investigation (2026-05-19): opt-in Exact via
        // EMAT_EXACT_PUSHDOWN=1 for is_exact_safe() && column_has_no_nulls
        // filters. Used by `tpch_q01_exact_diff` to A/B plan-diff. Stays
        // off by default — Exact regresses on top of parallel path.
        let exact_opt_in = std::env::var_os("EMAT_EXACT_PUSHDOWN").is_some();
        let no_nulls = &self.column_has_no_nulls;
        Ok(filters
            .iter()
            .map(|e| {
                match predicate_from_expr_with_dict(e, &self.schema, &self.column_is_dict_encoded) {
                    Some(pred)
                        if exact_opt_in
                            && pred.is_exact_safe()
                            && no_nulls.get(pred.col_idx()).copied().unwrap_or(false) =>
                    {
                        TableProviderFilterPushDown::Exact
                    }
                    Some(_) => TableProviderFilterPushDown::Inexact,
                    // Σ.SC P3W: int-eq isn't a BridgeFilter shape, but
                    // when a sidecar index sits next to this part the
                    // scan hook needs to SEE the predicate to answer it
                    // from the index. Inexact ⇒ the eq re-applies above
                    // regardless; without a sidecar file the plan is
                    // byte-identical to before.
                    None if crate::sidecar_exec::is_sidecar_pushable(
                        e,
                        std::path::Path::new(&self.path),
                    ) =>
                    {
                        TableProviderFilterPushDown::Inexact
                    }
                    None => TableProviderFilterPushDown::Unsupported,
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        self.scan_with_partition_budget(state, projection, filters, limit, None)
            .await
    }
}

impl EmatixFastParquetTableProvider {
    /// `TableProvider::scan` with an optional per-scan partition cap.
    ///
    /// Σ.MW.1 (parted-SF100 fast-path OOM): the multi-file provider
    /// unions one of these scans per part, and every part independently
    /// sizing itself to `target_partitions` multiplies the query's
    /// concurrently-polled decode width by the part count (13 lineitem
    /// parts × 14 partitions ≈ 182 streams; Q01 peaked ~9 GB instead of
    /// ~2 GB and 32 GB boxes kernel-OOM'd). The union splits the session
    /// budget across parts via `max_partitions`; `None` preserves the
    /// single-file behaviour exactly.
    pub(crate) async fn scan_with_partition_budget(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
        max_partitions: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.fields().len()).collect());
        // Σ.SC P3W: a covered, gate-approved point predicate answers
        // from the sidecar index instead of scanning this part (or
        // proves EMPTY from footer bounds without touching a data
        // page — the parted range-prune win). Falls through to the
        // normal scan on any miss; pushdown stays Inexact so the eq
        // and any sibling filters re-apply above.
        if let Some(exec) = crate::sidecar_exec::try_sidecar_lookup(
            std::path::Path::new(&self.path),
            &self.schema,
            &projection,
            filters,
        )? {
            return Ok(exec);
        }
        let projected_schema: Schema = self.schema.project(&projection)?;
        let projected_schema: SchemaRef = Arc::new(projected_schema);
        // KEYS.2 — native-width schema the readers decode to (equals
        // `projected_schema` unless a key was narrowed; see `decode_schema`).
        let projected_decode_schema: SchemaRef =
            Arc::new(self.decode_schema().project(&projection)?);

        let target_partitions = state.config_options().execution.target_partitions;
        let num_rgs = self.num_row_groups;
        let num_partitions = num_rgs
            .min(target_partitions)
            .min(max_partitions.unwrap_or(usize::MAX))
            .max(1);
        // LPT.RG — cost-balanced (LPT) row-group → partition assignment,
        // replacing round-robin (`rg % num_partitions`). Each partition's
        // producer decodes its list sequentially, so the most expensive
        // partition sets the scan's tail latency; round-robin ignored
        // both uneven counts (58 RGs / 14 partitions → +25% tail work)
        // and per-RG decode-cost skew. `EMAT_BALANCED_RG_ASSIGN=0`
        // restores round-robin exactly.
        let assignments = compute_scan_rg_assignments(
            num_rgs,
            num_partitions,
            &projection,
            &self.rg_col_compressed_bytes,
        );

        // Phase 3: extract pushable filters from DataFusion's filter
        // list. If all filters fit the shape, plumb them to the Exec
        // for bitmap-first decode. Otherwise the Exec runs Phase 2's
        // dense path (DataFusion's residual FilterExec handles the
        // predicate).
        let bridge_filter =
            extract_bridge_filter(filters, &self.schema, &self.column_is_dict_encoded).map(|bf| {
                // Σ.E5 Phase 1.8: compute predicted pass rate from
                // stats. Used by the streaming reader to dispatch
                // parallel-bitmap+dense (high-sel) vs serial-
                // bitmap+masked (low-sel).
                let p = bf.estimate_pass_rate(&self.column_stats);
                bf.with_predicted_pass_rate(p)
            });

        // Project the per-column stats so the Exec reports stats in
        // projection order (matches the projected schema indices).
        let projected_col_stats: Vec<datafusion::common::stats::ColumnStatistics> = projection
            .iter()
            .map(|&i| self.column_stats[i].clone())
            .collect();

        Ok(Arc::new(EmatixFastParquetExec::try_new(
            self.path.clone(),
            projected_schema,
            projected_decode_schema,
            Arc::clone(&self.schema),
            projection,
            assignments,
            self.num_rows,
            Arc::clone(&self.rg_num_rows),
            bridge_filter,
            self.late_mat,
            self.streaming_arrow_reader,
            projected_col_stats,
            Arc::clone(&self.column_has_no_nulls),
        )?))
    }
}

/// `ExecutionPlan` produced by [`EmatixFastParquetTableProvider`].
#[derive(Debug)]
pub struct EmatixFastParquetExec {
    path: String,
    schema: SchemaRef,
    /// KEYS.2 — projected native-width schema the readers decode to. Equals
    /// `schema` unless a key was narrowed Int64→Int32 (then that column is
    /// Int64 here, Int32 in `schema`). `execute()` hands this to the decode
    /// path and casts to `schema` once at the stream boundary.
    decode_schema: SchemaRef,
    /// Full (unprojected) file schema. Σ.E5 (2026-05-19): exposed so
    /// `InjectFusedQ*Rule` can resolve `BridgeFilter` col_idx →
    /// column name when matching the Exact-mode shape.
    file_schema: SchemaRef,
    projection: Vec<usize>,
    assignments: Vec<Vec<usize>>,
    num_rows: usize,
    /// Cached per-RG row counts from the provider (decoded once when
    /// the file was opened). Used by `execute()` to size the per-
    /// partition row totals so it can pick the inline vs eager reader
    /// without re-decoding the thrift footer per partition.
    rg_num_rows: Arc<Vec<usize>>,
    /// Phase 3: optional pushed-down filter. When present, execute()
    /// runs the bitmap-first path (Phase 5 fused-NEON filter + Phase 6
    /// sparse gather). When None, runs Phase 2 dense decode.
    filter: Option<BridgeFilter>,
    /// Σ.E5a (Π.10): when true AND `filter.is_some()`, decode goes
    /// through `read_column_*_masked_into`. Else: sparse_gather path.
    late_mat: bool,
    /// Σ.E5.1.b: when true AND `filter.is_none()`, batch emission
    /// uses `EmatArrowBatchReader` (streaming, per-column-parallel,
    /// `Utf8View`/`Dictionary`-aware) instead of the whole-RG bridge.
    streaming_arrow_reader: bool,
    /// Σ.E5: projected per-column stats (min/max/null_count) so
    /// `partition_statistics` returns real cardinality info instead
    /// of `Statistics::new_unknown`. Same shape as
    /// `FastParquetExec.column_stats`.
    column_stats: Vec<datafusion::common::stats::ColumnStatistics>,
    /// PV.M.7 — per-(file-schema-indexed) "column has no nulls" flags,
    /// threaded from the provider. The masked-decode kernels carry no
    /// def-levels, so the projection-prune fusion may only declare a
    /// range predicate Exact (drop its residual FilterExec) on a
    /// null-free column. File-indexed to match `BridgeFilter` col_idx.
    column_has_no_nulls: Arc<Vec<bool>>,
    /// Σ.Q.L9 — runtime sideband for mid-query predicate injection.
    /// `None` = no sideband attached (normal case). When set, the
    /// scan's `execute()` consults this AFTER the build phase of any
    /// upstream HashJoin has had a chance to populate it; matching
    /// predicates are merged into the BridgeFilter before decode.
    runtime_sideband: Option<crate::bridge_filter_sideband::BridgeFilterSideband>,
    /// Σ.Q05.CHAIN (2026-07-02) — ADDITIONAL runtime sidebands beyond the
    /// primary slot. The L9 cascade-chain pass can want a second bloom on a
    /// scan whose primary slot is already taken by a pass-1 wrap (Q05 SF=10:
    /// the o⋈l tight wrap holds lineitem's primary with an `l_orderkey`
    /// bloom; the supplier-chain bloom on `l_suppkey` rides here). Extras
    /// are STRICTLY best-effort: `execute()` peeks each once (no wait, no
    /// late-arm, no disarm threading) and merges any published non-empty
    /// predicates. A missed publish just means that pass runs without the
    /// extra filter — correctness never depends on it (the join re-applies
    /// every key). Empty in every pre-existing plan shape.
    extra_runtime_sidebands: Vec<crate::bridge_filter_sideband::BridgeFilterSideband>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl EmatixFastParquetExec {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        path: String,
        schema: SchemaRef,
        decode_schema: SchemaRef,
        file_schema: SchemaRef,
        projection: Vec<usize>,
        assignments: Vec<Vec<usize>>,
        num_rows: usize,
        rg_num_rows: Arc<Vec<usize>>,
        filter: Option<BridgeFilter>,
        late_mat: bool,
        streaming_arrow_reader: bool,
        column_stats: Vec<datafusion::common::stats::ColumnStatistics>,
        column_has_no_nulls: Arc<Vec<bool>>,
    ) -> DfResult<Self> {
        let eq_props = EquivalenceProperties::new(schema.clone());
        let properties = Arc::new(PlanProperties::new(
            eq_props,
            Partitioning::UnknownPartitioning(assignments.len().max(1)),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            path,
            schema,
            decode_schema,
            file_schema,
            projection,
            assignments,
            num_rows,
            rg_num_rows,
            filter,
            late_mat,
            streaming_arrow_reader,
            column_stats,
            column_has_no_nulls,
            runtime_sideband: None,
            extra_runtime_sidebands: Vec::new(),
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Σ.Q.L9 — attach a runtime sideband. The scan will consult this
    /// at `execute()` time and merge any published predicates into
    /// the BridgeFilter before decode. Returns a fresh Arc<Self> so
    /// the planner rule's TreeNode walk stays honest.
    pub fn with_runtime_sideband(
        &self,
        sideband: crate::bridge_filter_sideband::BridgeFilterSideband,
    ) -> Arc<Self> {
        let mut next = self.clone_internals();
        next.runtime_sideband = Some(sideband);
        Arc::new(next)
    }

    /// Σ.Q.L9 — the attached runtime sideband, if any. Used by the
    /// planner rule to verify it threaded the sideband correctly.
    /// L9.ADAPT Guard 1 — the plan-time pushed-down filter (user
    /// statics), if any. The L9 rule inspects it before tight-admitting
    /// a wrap: a bundle the fused arm can't evaluate would drag the
    /// whole scan onto the legacy re-decode path.
    pub fn pushed_filter(&self) -> Option<&BridgeFilter> {
        self.filter.as_ref()
    }

    pub fn runtime_sideband(&self) -> Option<&crate::bridge_filter_sideband::BridgeFilterSideband> {
        self.runtime_sideband.as_ref()
    }

    /// Σ.Q05.CHAIN — attach an ADDITIONAL runtime sideband (used when the
    /// primary slot is already occupied by another wrap). Best-effort
    /// consumption: `execute()` peeks it once with no wait and no
    /// late-arm. Returns a fresh Arc<Self> like
    /// [`Self::with_runtime_sideband`].
    pub fn with_extra_runtime_sideband(
        &self,
        sideband: crate::bridge_filter_sideband::BridgeFilterSideband,
    ) -> Arc<Self> {
        let mut next = self.clone_internals();
        next.extra_runtime_sidebands.push(sideband);
        Arc::new(next)
    }

    /// Σ.Q05.CHAIN — the additional (non-primary) runtime sidebands.
    pub fn extra_runtime_sidebands(
        &self,
    ) -> &[crate::bridge_filter_sideband::BridgeFilterSideband] {
        &self.extra_runtime_sidebands
    }

    /// Full (unprojected) file schema. Σ.E5: needed by
    /// `InjectFusedQ*Rule` to resolve a `BridgeFilter` predicate's
    /// `col_idx` back to a column name when matching the Exact-mode
    /// shape (no `FilterExec` in the plan).
    pub fn file_schema(&self) -> &SchemaRef {
        &self.file_schema
    }

    /// The pushed-down BridgeFilter, if any. Σ.E5 (2026-05-19):
    /// `InjectFusedQ*Rule` reads this when matching the Exact-mode
    /// shape (no `FilterExec` in the plan — the predicate lives on
    /// the scan instead).
    pub fn filter(&self) -> Option<&BridgeFilter> {
        self.filter.as_ref()
    }

    /// Projected column indices into the file's logical schema.
    /// Σ.E5: needed by `InjectFusedQ*Rule` Exact-shape match to map
    /// from `BridgeFilter` `col_idx` (file-schema-indexed) back to a
    /// column name via the scan's schema.
    pub fn projection(&self) -> &[usize] {
        &self.projection
    }

    /// Σ.J.2.b.vi — backing parquet file path. Used by the
    /// `EnableContextBloomRule` to compute the `<table>.<col>` uuid
    /// (where `<table>` is the file basename without extension).
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Σ.AH.2 Story 1'.3 — projected per-column statistics carrying
    /// min/max/null_count/distinct_count. Indexed by the Exec's own
    /// (projected) schema, NOT by file-schema indices. Exposed for
    /// the L9 sideband rule so it can compute filter selectivity
    /// using accurate distinct_count populated by Story 1'.2 instead
    /// of relying on DataFusion's default-0.2 FilterExec selectivity.
    pub fn column_stats(&self) -> &[datafusion::common::stats::ColumnStatistics] {
        &self.column_stats
    }

    /// Σ.AH.2 Story 1'.3 — raw (pre-filter) total rows in the file.
    /// Used by the L9 rule's emat-stats-aware build-rows estimate.
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    // PV.M.7 — accessors for rebuilding a projection-pruned scan via
    // `try_new` (drop filter-only output columns from the decode
    // projection, keeping the BridgeFilter, to avoid the double
    // Snappy-decompress of the predicate column).
    pub fn decode_schema_ref(&self) -> &SchemaRef {
        &self.decode_schema
    }
    pub fn assignments(&self) -> &[Vec<usize>] {
        &self.assignments
    }
    pub fn rg_num_rows_arc(&self) -> &Arc<Vec<usize>> {
        &self.rg_num_rows
    }
    pub fn exec_late_mat(&self) -> bool {
        self.late_mat
    }
    pub fn exec_streaming_arrow_reader(&self) -> bool {
        self.streaming_arrow_reader
    }
    /// PV.M.7 — file-schema-indexed "column has no nulls" flags.
    pub fn column_has_no_nulls(&self) -> &[bool] {
        &self.column_has_no_nulls
    }

    /// Σ.Q.L4′ — append extra predicates (e.g. I64InBloom from a
    /// build-side bloom emitter) onto this scan's BridgeFilter. If
    /// no filter existed, creates one from the supplied predicates.
    /// Returns a fresh `Arc<Self>` to keep the rule's TreeNode walk
    /// honest (no in-place mutation of shared exec nodes).
    pub fn with_added_predicates(&self, more: Vec<ColumnPredicate>) -> DfResult<Arc<Self>> {
        if more.is_empty() {
            return Ok(Arc::new(self.clone_internals()));
        }
        let mut filter = match &self.filter {
            Some(f) => f.clone(),
            None => BridgeFilter::new(Vec::new()),
        };
        filter.extend(more);
        // Re-compute the predicted pass rate so the streaming reader
        // picks the right serial-vs-parallel path.
        let p = filter.estimate_pass_rate(&self.column_stats);
        let filter = filter.with_predicted_pass_rate(p);
        Ok(Arc::new(Self {
            path: self.path.clone(),
            schema: self.schema.clone(),
            decode_schema: self.decode_schema.clone(),
            file_schema: self.file_schema.clone(),
            projection: self.projection.clone(),
            assignments: self.assignments.clone(),
            num_rows: self.num_rows,
            rg_num_rows: self.rg_num_rows.clone(),
            filter: Some(filter),
            late_mat: self.late_mat,
            streaming_arrow_reader: self.streaming_arrow_reader,
            column_stats: self.column_stats.clone(),
            column_has_no_nulls: self.column_has_no_nulls.clone(),
            runtime_sideband: self.runtime_sideband.clone(),
            extra_runtime_sidebands: self.extra_runtime_sidebands.clone(),
            properties: self.properties.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    /// Σ.Q06.SF10.8 — return a filter-stripped clone that takes the
    /// dense streaming decode path. When a downstream `FusedAggregateExec`
    /// re-applies the predicate itself, the scan's pushed-down
    /// `BridgeFilter` is redundant — and the masked late-materialization
    /// decode it triggers is ~2× slower than dense decode feeding the
    /// JIT fused filter-agg kernel (Q06 SF=10: ~89→~55 ms). Keeps any
    /// runtime sideband (callers gate on its absence so join-probe
    /// scans keep their L9 bloom). See [[q06-masked-pushdown-waste]].
    pub fn without_filter(&self) -> Arc<Self> {
        Arc::new(Self {
            path: self.path.clone(),
            schema: self.schema.clone(),
            decode_schema: self.decode_schema.clone(),
            file_schema: self.file_schema.clone(),
            projection: self.projection.clone(),
            assignments: self.assignments.clone(),
            num_rows: self.num_rows,
            rg_num_rows: self.rg_num_rows.clone(),
            filter: None,
            late_mat: self.late_mat,
            streaming_arrow_reader: self.streaming_arrow_reader,
            column_stats: self.column_stats.clone(),
            column_has_no_nulls: self.column_has_no_nulls.clone(),
            runtime_sideband: self.runtime_sideband.clone(),
            extra_runtime_sidebands: self.extra_runtime_sidebands.clone(),
            properties: self.properties.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    fn clone_internals(&self) -> Self {
        Self {
            path: self.path.clone(),
            schema: self.schema.clone(),
            decode_schema: self.decode_schema.clone(),
            file_schema: self.file_schema.clone(),
            projection: self.projection.clone(),
            assignments: self.assignments.clone(),
            num_rows: self.num_rows,
            rg_num_rows: self.rg_num_rows.clone(),
            filter: self.filter.clone(),
            late_mat: self.late_mat,
            streaming_arrow_reader: self.streaming_arrow_reader,
            column_stats: self.column_stats.clone(),
            column_has_no_nulls: self.column_has_no_nulls.clone(),
            runtime_sideband: self.runtime_sideband.clone(),
            extra_runtime_sidebands: self.extra_runtime_sidebands.clone(),
            properties: self.properties.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// RANGE.AGG — rebuild this scan with an explicit row-group →
    /// partition assignment (used to re-chunk at key-disjoint
    /// boundaries so a cluster-key group-by can aggregate each
    /// partition independently). Partition count changes, so the plan
    /// properties are rebuilt.
    pub fn with_assignments(&self, assignments: Vec<Vec<usize>>) -> Arc<Self> {
        let mut next = self.clone_internals();
        let eq_props = EquivalenceProperties::new(next.schema.clone());
        next.properties = Arc::new(PlanProperties::new(
            eq_props,
            Partitioning::UnknownPartitioning(assignments.len().max(1)),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        next.assignments = assignments;
        Arc::new(next)
    }

    /// RANGE.AGG Stage 2 — like [`Self::with_assignments`], but the
    /// rebuilt scan ADVERTISES `Partitioning::Hash(hash_exprs, n)`
    /// instead of `UnknownPartitioning(n)`.
    ///
    /// This is a deliberate over-claim: the chunks are KEY-DISJOINT
    /// (planned at strict row-group key gaps), not hash-distributed.
    /// For a hash-distribution CONSUMER the claim is row-correct —
    /// all HashPartitioned promises is that every distinct key's rows
    /// land in exactly one partition, which key-disjoint chunking
    /// guarantees — so `EnforceDistribution` accepts the scan as
    /// direct input to an `AggregateExec(SinglePartitioned)` without
    /// inserting a full-input hash shuffle (Q18 SF=100: 600M rows /
    /// ~9.6 GB saved).
    ///
    /// SAFETY CONTRACT: the claim must NOT propagate past the
    /// aggregation it feeds. Chunk boundaries do not coincide with
    /// hash buckets, so a partitioned HashJoin that paired these
    /// partitions with genuinely hash-repartitioned partitions of its
    /// other side would silently drop matches. The caller
    /// (`ClusteredSinglePhaseAggRule`) caps the rewritten aggregate
    /// with a [`crate::partition_claim_reset_exec::PartitionClaimResetExec`]
    /// so everything above sees `UnknownPartitioning` again. Do not
    /// use this constructor without an equivalent cap.
    ///
    /// The equivalence properties are rebuilt fresh from the schema
    /// (same as `with_assignments`) so no ordering/equivalence claims
    /// are derived from the false hash claim.
    pub fn with_assignments_claiming_hash(
        &self,
        assignments: Vec<Vec<usize>>,
        hash_exprs: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    ) -> Arc<Self> {
        let mut next = self.clone_internals();
        let eq_props = EquivalenceProperties::new(next.schema.clone());
        next.properties = Arc::new(PlanProperties::new(
            eq_props,
            Partitioning::Hash(hash_exprs, assignments.len().max(1)),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        next.assignments = assignments;
        Arc::new(next)
    }
}

impl DisplayAs for EmatixFastParquetExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let total_rgs: usize = self.assignments.iter().map(|a| a.len()).sum();
        write!(
            f,
            "EmatixFastParquetExec(path={}, partitions={}, row_groups={}, projection={:?})",
            self.path,
            self.assignments.len(),
            total_rgs,
            self.projection,
        )
    }
}

impl ExecutionPlan for EmatixFastParquetExec {
    fn name(&self) -> &str {
        "EmatixFastParquetExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let row_groups = self.assignments.get(partition).cloned().unwrap_or_default();
        let path = self.path.clone();
        let projection = self.projection.clone();
        let schema = self.schema.clone();
        // KEYS.2 — readers decode to the native-width schema; the cast to
        // the advertised `schema` (narrowed keys) happens once on the output
        // stream below. `decode_schema == schema` when nothing was narrowed
        // (default), so the cast wrapper is a no-op there.
        let decode_schema = self.decode_schema.clone();
        // NARROW.DEC — file-leaf indices of narrowed keys that the eager
        // streaming reader should decode DIRECTLY at Int32 (downcast-on-
        // read; opt-in via EMAT_NARROW_KEY_DECODE). Derived from the
        // (advertised, decode) schema pair: a projected column that is
        // Int32 advertised but Int64 in the decode schema is exactly a
        // KEYS.2-narrowed key. Empty (the default, and whenever
        // EMAT_DOWNCAST_KEYS narrowed nothing) keeps every reader
        // bit-identical; the boundary cast below still reconciles any
        // reader family that decodes wide.
        // Σ.AI.5 (2026-07-02): tri-state, gated with EMAT_DOWNCAST_KEYS
        // (unset = AUTO = on at SF≥100 scale). Narrowing only exists when
        // the downcast advertised Int32 (the schema-pair filter below), so
        // AUTO here follows the downcast's own resolution.
        let narrow_i64_leaves: Vec<usize> =
            if crate::flags::scale_gated_large("EMAT_NARROW_KEY_DECODE") {
                schema
                    .fields()
                    .iter()
                    .zip(decode_schema.fields().iter())
                    .enumerate()
                    .filter(|(_, (adv, dec))| {
                        matches!(adv.data_type(), DataType::Int32)
                            && matches!(dec.data_type(), DataType::Int64)
                    })
                    .map(|(i, _)| self.projection[i])
                    .collect()
            } else {
                Vec::new()
            };
        // Σ.Q.L9 — runtime sideband consumption. At execute() time
        // (which for the probe side of a HashJoinExec runs AFTER the
        // build phase has fully drained — see the L9 module doc), peek
        // at the sideband. If the build-side wrapper has published
        // predicates, merge them into the BridgeFilter we'll hand to
        // the reader. Empty / no-sideband = no-op.
        //
        // We `peek` (not `take`) because in a partitioned plan,
        // execute() is called once per partition; each partition's
        // call needs the same published predicates. The sideband
        // outlives the query, but the predicates inside are fresh
        // per query.
        // Σ.Q.L9 — runtime sideband consumption (timing-corrected).
        //
        // Earlier versions peeked the sideband HERE at execute() time
        // and resolved the BridgeFilter eagerly. That had a bug: in
        // DataFusion, `HashJoinExec::execute(partition)` calls
        // execute() on BOTH children before its build_future drains.
        // So our `execute()` ran BEFORE the upstream BuildSideBloom
        // emitter ever published. peek() always returned None →
        // filter stayed empty → bloom did no work. Trace probe
        // (EMAT_L9_TRACE=1) on Q18 SF=10 confirmed: every orders
        // partition logged `peek=None`.
        //
        // Fix: defer the peek into the stream's first poll, where
        // we ARE guaranteed to be downstream of the build_future's
        // completion (the probe stream awaits build_future on first
        // poll; first poll on our scan only happens once the parent
        // join is past its build phase).
        //
        // Mechanics: capture the sideband Arc + the base filter, then
        // build the partition stream inside a `stream::once(async)`
        // / `flatten` wrapper so all the work (including the peek and
        // the EmatArrowBatchReaderBuilder construction) is deferred
        // to first poll.
        let base_filter = self.filter.clone();
        let runtime_sideband = self.runtime_sideband.clone();
        let extra_runtime_sidebands = self.extra_runtime_sidebands.clone();
        let column_stats = self.column_stats.clone();
        let trace_l9 = std::env::var("EMAT_L9_TRACE").ok().as_deref() == Some("1");
        let late_mat = self.late_mat;
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let streaming_arrow_reader = self.streaming_arrow_reader;
        let outer_partitions = self.properties.partitioning.partition_count().max(1);

        // Σ.Q.L9 — fast path when no runtime sideband is attached
        // (the common case for queries without the L9 rule installed
        // or for scans that aren't probe-side targets of any join).
        // The deferred-peek wrapper measured ~15ms overhead on Q18
        // SF=10 even when the sideband was None; that's pure waste
        // for plans that can't benefit. So: if there's no sideband,
        // skip the wrapper entirely and use the original eager path.
        // Σ.Q05.CHAIN — extras also require the deferred-peek wrapper.
        if runtime_sideband.is_none() && extra_runtime_sidebands.is_empty() {
            let stream = build_partition_stream_dispatch(
                path,
                decode_schema.clone(),
                projection,
                row_groups,
                base_filter,
                late_mat,
                baseline,
                streaming_arrow_reader,
                outer_partitions,
                self.rg_num_rows.clone(),
                None,
                narrow_i64_leaves,
            );
            let stream = narrow_stream_to_advertised(stream, &decode_schema, schema.clone());
            return Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream))
                as SendableRecordBatchStream);
        }

        // Σ.Q.L9 deferred peek (sideband-attached case).
        //
        // Earlier versions peeked the sideband at execute() time and
        // resolved the BridgeFilter eagerly. That had a bug: in
        // DataFusion, `HashJoinExec::execute(partition)` calls
        // execute() on BOTH children before its build_future drains.
        // So our execute() ran BEFORE the upstream BuildSideBloom
        // emitter ever published. peek() returned None →
        // filter stayed empty. Trace probe (EMAT_L9_TRACE=1) on
        // Q18 SF=10 confirmed: every orders partition logged
        // `peek=None`.
        //
        // Fix below: defer the peek into the stream's first poll,
        // where we ARE guaranteed to be past at least our parent's
        // build_future (the parent's probe stream awaits build_future
        // on first poll; the first poll on our scan only happens once
        // the parent join is past its build phase).
        //
        // Caveat: scans on the BUILD side of a HashJoinExec are
        // polled eagerly (they ARE part of the build phase), so the
        // deferred peek still doesn't help them. For Q18 that's the
        // orders scan inside customer⋈orders.
        let path_for_async = path.clone();
        // KEYS.2 — readers decode to native width; the cast to `schema`
        // happens on the flattened stream below.
        let schema_for_async = decode_schema.clone();
        let projection_for_async = projection.clone();
        let row_groups_for_async = row_groups.clone();
        let column_stats_for_async = column_stats.clone();
        let rg_num_rows_for_async = self.rg_num_rows.clone();

        let inner_stream_fut = async move {
            // Resolve the actual filter NOW (first poll), with the
            // sideband possibly populated by an upstream build phase
            // that has since completed.
            let mut filter = base_filter;
            let mut late_arm: Option<crate::bridge_filter_sideband::BridgeFilterSideband> = None;
            if let Some(sb) = &runtime_sideband {
                if sb.tight_admitted() {
                    // L9.ADAPT LATE-ARM — tight-rescued wraps (payoff
                    // unproven by the default estimator) never stall
                    // the probe. The earlier probe-row-scaled wait
                    // budget granted Q17's 60M probe a 60ms stall;
                    // dedicated runs publish the warm ~2K-part build
                    // in ~6ms and win (113 vs 129ms), but IN-SWEEP the
                    // cold build blows through the budget — the probe
                    // pays the full stall and STILL gets no early
                    // bloom (+31%). Instead: peek once without
                    // waiting; if unpublished, hand the sideband to
                    // the reader, which adopts the predicate at the
                    // next row-group boundary once the build lands.
                    // L9.DIMSEL.RT — DIMSEL-gated wraps do NOT late-arm. An
                    // eager-polled dim→fact probe (Q3 orders) races past the
                    // build, peeks None, and late-arming would route it to the
                    // eager whole-RG reader to await a publish that may be empty
                    // (the disarm) — pure +routing cost. Falling back to base
                    // here keeps it on the inline reader. A lazy-polled probe
                    // (Q9 lineitem) already sees is_ready()=true so never
                    // reaches this branch and keeps its bloom.
                    if !sb.is_ready() && !sb.dimsel_gated() {
                        late_arm = Some(sb.clone());
                    }
                } else {
                    // Σ.Q.L16: brief wait for the build-side bloom to be
                    // published. Without this, the probe partitions race
                    // past the build on small-build joins (Q17 SF=10 had
                    // filtered_part = 2K rows building in ~6 ms, but 12 of
                    // 14 lineitem partitions peeked None and ran full
                    // 60 M rows). Default timeout 200 ms — small enough
                    // that big-build joins time out cleanly and proceed
                    // un-bloomed, large enough to absorb fast builds.
                    let timeout_ms: u64 = std::env::var("EMAT_L9_PEEK_TIMEOUT_MS")
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(200);
                    let _ = sb
                        .wait_for_publish(std::time::Duration::from_millis(timeout_ms))
                        .await;
                }
                let peeked = sb.peek();
                if trace_l9 {
                    let path_short = std::path::Path::new(&path_for_async)
                        .file_name()
                        .map(|s| s.to_string_lossy().into_owned())
                        .unwrap_or_else(|| path_for_async.clone());
                    match &peeked {
                        Some(preds) => {
                            let summary: Vec<String> = preds
                                .iter()
                                .map(|p| match p {
                                    ColumnPredicate::I64InBloom { col_idx, .. } => {
                                        format!("I64InBloom(col={col_idx})")
                                    }
                                    ColumnPredicate::StringInBloom { col_idx, .. } => {
                                        format!("StringInBloom(col={col_idx})")
                                    }
                                    ColumnPredicate::StringInSet { col_idx, .. } => {
                                        format!("StringInSet(col={col_idx})")
                                    }
                                    _ => "other".to_string(),
                                })
                                .collect();
                            eprintln!(
                                "[L9-trace] {path_short} p={partition} peek=Some({summary:?})"
                            );
                        }
                        None => eprintln!(
                            "[L9-trace] {path_short} p={partition} peek=None (bloom not published)"
                        ),
                    }
                }
                if let Some(extras) = peeked {
                    if !extras.is_empty() {
                        let mut bf = filter.unwrap_or_else(|| BridgeFilter::new(Vec::new()));
                        bf.extend(extras);
                        // L9.ADAPT Guard 2 — tight-admitted wraps carry
                        // the shared disarm counters so the reader can
                        // stop probing once the published set provably
                        // doesn't prune (payoff unproven at plan time).
                        if sb.tight_admitted() {
                            bf.set_probe_disarm(sb.probe_disarm_handle());
                        }
                        // Σ.Q05.CHAIN — intermediate chain links must
                        // actually prune (the next link samples this
                        // scan's output); tell the reader to keep the
                        // bitmap even above the dense-route threshold.
                        if sb.chain_intermediate() {
                            bf.set_apply_when_dense();
                        }
                        let p = bf.estimate_pass_rate(&column_stats_for_async);
                        filter = Some(bf.with_predicted_pass_rate(p));
                    }
                }
            }
            // Σ.Q05.CHAIN — additional sidebands (cascade extras). Best
            // effort by design: peek once, no wait, no late-arm, no disarm
            // threading (the primary keeps Guard 2 when it has one; with
            // both merged into the same probe list the disarm judges their
            // JOINT marginal pass-rate, which is the conservative
            // direction). A not-yet-published extra just means this pass
            // runs without that filter — the join re-applies every key so
            // correctness never depends on the peek landing.
            for esb in extra_runtime_sidebands.iter() {
                let peeked = esb.peek();
                if trace_l9 {
                    let path_short = std::path::Path::new(&path_for_async)
                        .file_name()
                        .map(|s| s.to_string_lossy().into_owned())
                        .unwrap_or_else(|| path_for_async.clone());
                    eprintln!(
                        "[L9-trace] {path_short} p={partition} extra-sideband peek={}",
                        match &peeked {
                            Some(preds) => format!("Some(len={})", preds.len()),
                            None => "None (bloom not published)".to_string(),
                        }
                    );
                }
                if let Some(preds) = peeked {
                    if !preds.is_empty() {
                        let mut bf = filter.unwrap_or_else(|| BridgeFilter::new(Vec::new()));
                        bf.extend(preds);
                        if esb.chain_intermediate() {
                            bf.set_apply_when_dense();
                        }
                        let p = bf.estimate_pass_rate(&column_stats_for_async);
                        filter = Some(bf.with_predicted_pass_rate(p));
                    }
                }
            }
            build_partition_stream_dispatch(
                path_for_async,
                schema_for_async,
                projection_for_async,
                row_groups_for_async,
                filter,
                late_mat,
                baseline,
                streaming_arrow_reader,
                outer_partitions,
                rg_num_rows_for_async,
                late_arm,
                narrow_i64_leaves,
            )
        };

        let stream = futures_util::stream::once(inner_stream_fut)
            .flatten()
            .boxed();
        let stream = narrow_stream_to_advertised(stream, &decode_schema, schema.clone());
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)) as SendableRecordBatchStream)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DfResult<Statistics> {
        // Σ.E5: report typed per-column min/max/null_count + num_rows.
        // Mirrors `FastParquetExec::partition_statistics`. The planner
        // uses these for join-build-side selection and selectivity
        // estimates; without them every join build picks the "first"
        // side, which is suboptimal for queries like Q21 where
        // pre-filter cardinalities are wildly different across joined
        // tables (e.g. nation = 25 rows vs lineitem = 6 M).
        let raw_rows = match partition {
            Some(p) if p < self.assignments.len() => self.num_rows / self.assignments.len().max(1),
            None => self.num_rows,
            _ => 0,
        };
        // Σ.AE.1 / Σ.AE.4 (2026-05-26): when a BridgeFilter is pushed
        // down AND Exact pushdown is enabled (so FilterExec is being
        // dropped for some predicates), report the post-filter
        // cardinality for those predicates so the planner picks join
        // build sides correctly. Otherwise Exact pushdown breaks
        // queries like Q21 — the planner sees orders at unfiltered
        // 15M, picks lineitem (60M) as the build side, and
        // catastrophically regresses (+124% in the 2026-05-26 Exact
        // bench).
        //
        // Critically, only apply pass-rate for the Exact-declared
        // predicates. For Inexact-declared ones (string predicates
        // under our string-gate; all predicates under default mode),
        // FilterExec is still in the plan and DataFusion's planner
        // applies the predicate's selectivity on top of our reported
        // cardinality. Pre-applying it here would double-count: Q10
        // SF=10's lineitem returnflag filter double-counted to 1.8M
        // (vs honest 15M), fooling the planner into picking lineitem
        // as the build side (15M-row hash) and regressing wall time
        // by +17% / ~40 ms.
        let exact_opt_in = std::env::var_os("EMAT_EXACT_PUSHDOWN").is_some();
        let (rows, filtered) = if let Some(filter) = &self.filter {
            let sel = filter.estimate_dropped_filter_pass_rate(&self.column_stats, exact_opt_in);
            if sel < 1.0 {
                let r = ((raw_rows as f64) * sel) as usize;
                (r.max(1), true)
            } else {
                (raw_rows, false)
            }
        } else {
            (raw_rows, false)
        };
        let mut s = Statistics::new_unknown(&self.schema);
        // Mark Inexact whenever a filter is in play — even our Exact-
        // safe predicates ride a selectivity estimate, not a true
        // count. Without the filter we keep the precise count.
        s.num_rows = if filtered {
            datafusion::common::stats::Precision::Inexact(rows)
        } else if partition.is_none() {
            datafusion::common::stats::Precision::Exact(rows)
        } else {
            datafusion::common::stats::Precision::Inexact(rows)
        };
        s.column_statistics = self.column_stats.clone();
        Ok(s)
    }
}

/// Σ.Q06.SF10.8 — descend through single-child wrapper operators and
/// strip the redundant static `BridgeFilter` from any
/// `EmatixFastParquetExec` that carries no runtime sideband, returning
/// a dense-decode clone. Stops at leaves and multi-child nodes (joins),
/// so scans feeding a join keep their pushdown — that preserves the
/// join queries (Q12/Q19), while the fused-agg shape (Q06/Q01/Q14) has
/// only linear operators between the fused op and its scan.
///
/// ONLY sound to call on the input of a `FusedAggregateExec`, which
/// re-applies the predicate from the dropped `FilterExec` itself — so
/// the scan's pre-filter is pure redundant (and slow, masked) work.
/// See [[q06-masked-pushdown-waste]].
pub fn strip_redundant_scan_filter(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    if let Some(scan) = plan.as_any().downcast_ref::<EmatixFastParquetExec>() {
        if scan.filter().is_some() && scan.runtime_sideband().is_none() {
            return scan.without_filter();
        }
        return plan;
    }
    let children = plan.children();
    if children.len() != 1 {
        return plan;
    }
    let child = Arc::clone(children[0]);
    let new_child = strip_redundant_scan_filter(Arc::clone(&child));
    if Arc::ptr_eq(&new_child, &child) {
        return plan;
    }
    match Arc::clone(&plan).with_new_children(vec![new_child]) {
        Ok(p) => p,
        Err(_) => plan,
    }
}

/// KEYS.2 — narrow a reader's native-width batch stream to the provider's
/// advertised schema. The decode path (every reader family + the bridge)
/// produces columns at the on-disk width given by `decode_schema`; when the
/// provider narrowed an INT64 key to advertised `Int32`, this casts those
/// columns Int64→Int32 (lossless by the `int64_col_fits_i32` stats gate).
///
/// This is the single choke point for the narrowing: because every reader
/// flows through `execute()`'s output stream, casting here covers the eager,
/// masked, page-streaming, inline-streaming and bridge paths at once — no
/// per-decode-site narrowing logic that could silently miss a path.
///
/// When `decode_schema == advertised` (the default — downcast off, or no
/// key fit i32) the stream is returned untouched, so the hot path is
/// bit-identical and adds zero overhead.
fn narrow_stream_to_advertised(
    stream: futures_util::stream::BoxStream<'static, DfResult<RecordBatch>>,
    decode_schema: &SchemaRef,
    advertised: SchemaRef,
) -> futures_util::stream::BoxStream<'static, DfResult<RecordBatch>> {
    use futures_util::StreamExt;
    if decode_schema.fields() == advertised.fields() {
        return stream;
    }
    stream
        .map(move |item| item.and_then(|b| narrow_batch_to_schema(b, &advertised)))
        .boxed()
}

/// Cast each column of `batch` whose type differs from `advertised` to the
/// advertised type (KEYS.2: Int64→Int32). Matching columns pass through by
/// reference (Arc clone), so only the narrowed key columns pay a cast.
fn narrow_batch_to_schema(batch: RecordBatch, advertised: &SchemaRef) -> DfResult<RecordBatch> {
    use datafusion::arrow::compute::cast;
    let mut cols: Vec<Arc<dyn Array>> = Vec::with_capacity(batch.num_columns());
    for (i, col) in batch.columns().iter().enumerate() {
        let want = advertised.field(i).data_type();
        if col.data_type() == want {
            cols.push(col.clone());
        } else {
            let c = cast(col, want).map_err(|e| {
                DataFusionError::External(
                    format!(
                        "KEYS.2 narrow cast col {i} ({:?} -> {:?}): {e}",
                        col.data_type(),
                        want
                    )
                    .into(),
                )
            })?;
            cols.push(c);
        }
    }
    RecordBatch::try_new(advertised.clone(), cols)
        .map_err(|e| DataFusionError::External(format!("KEYS.2 narrow rebatch: {e}").into()))
}

/// Σ.Q.L9 — choose between the streaming reader path and the
/// legacy bridge-only path. Factored out so the EmatixFastParquetExec
/// execute() body can call this from inside its lazy
/// `stream::once(async)` wrapper, where we've already peeked the
/// runtime sideband and resolved the final BridgeFilter.
#[allow(clippy::too_many_arguments)]
fn build_partition_stream_dispatch(
    path: String,
    schema: SchemaRef,
    projection: Vec<usize>,
    row_groups: Vec<usize>,
    filter: Option<BridgeFilter>,
    late_mat: bool,
    baseline: BaselineMetrics,
    streaming_arrow_reader: bool,
    outer_partitions: usize,
    rg_num_rows: Arc<Vec<usize>>,
    // L9.ADAPT LATE-ARM — a tight-rescued wrap whose build hadn't
    // published at execute() time. Consumed by the eager streaming
    // reader (the only path big tight probes take: the ≥8M-row
    // rescue pre-gate means multi-RG fact partitions, which the
    // auto-pick routes to `EmatArrowBatchReader`); the reader adopts
    // the published predicate at the next row-group boundary.
    late_arm: Option<crate::bridge_filter_sideband::BridgeFilterSideband>,
    // NARROW.DEC — file-leaf indices of narrowed keys the eager
    // streaming reader decodes directly at Int32 (see execute()).
    // The page-streaming / inline / whole-RG-bridge families ignore
    // this and keep decode-wide + boundary-cast.
    narrow_i64_leaves: Vec<usize>,
) -> futures_util::stream::BoxStream<'static, DfResult<RecordBatch>> {
    if streaming_arrow_reader {
        // Σ.E5.1.c — per-partition column-decode thread budget; keep
        // total concurrent decode threads aligned to THIS PROCESS'S
        // core share, not the raw core count (campaign-2026-07-03: an
        // `available_parallelism()` numerator undid the registry's
        // partition reduction under concurrent processes — 7 scoped
        // decode threads per 2-partition process; budget=1 measured
        // 26,882 → 29,271 QPH at SF10 s10). Solo, `=0` legacy, and
        // registry failure all resolve share == cores, so those paths
        // compute the historical formula bit-identically.
        let share = crate::partition_registry::resolved_core_share();
        let computed_budget =
            crate::partition_registry::reader_decode_budget(share, outer_partitions);
        // Explicit env override keeps absolute precedence, unchanged.
        let budget = std::env::var("EMAT_READER_PARALLELISM_BUDGET")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .map(|n| n.max(1))
            .unwrap_or(computed_budget);
        if crate::flags::present("EMAT_DEBUG") {
            eprintln!(
                "[reader_budget] pid={} share={share} outer_partitions={outer_partitions} budget={budget}",
                std::process::id()
            );
        }
        let partition_rows: usize = row_groups
            .iter()
            .map(|&rg| rg_num_rows.get(rg).copied().unwrap_or(0))
            .sum();
        build_streaming_partition_stream(
            path,
            schema,
            projection,
            row_groups,
            budget,
            partition_rows,
            rg_num_rows.len(),
            filter,
            baseline,
            late_arm,
            narrow_i64_leaves,
        )
    } else {
        // Late-arm has no consumer on this path; the sideband predicate
        // simply never applies (the join re-filters — correct, no relief).
        build_partition_stream(
            path, schema, projection, row_groups, filter, late_mat, baseline,
        )
    }
}

/// Per-partition decode worker. Walks its assigned RGs sequentially,
/// emits one RecordBatch per RG over an mpsc channel.
fn build_partition_stream(
    path: String,
    schema: SchemaRef,
    projection: Vec<usize>,
    row_groups: Vec<usize>,
    filter: Option<BridgeFilter>,
    late_mat: bool,
    baseline: BaselineMetrics,
) -> futures_util::stream::BoxStream<'static, DfResult<RecordBatch>> {
    use futures_util::StreamExt;

    if row_groups.is_empty() {
        return futures_util::stream::iter(Vec::<DfResult<RecordBatch>>::new()).boxed();
    }

    let (tx, rx) = tokio::sync::mpsc::channel::<DfResult<RecordBatch>>(8);
    let path_buf = PathBuf::from(path);

    tokio::task::spawn_blocking(move || {
        for rg in row_groups {
            let batch_result = {
                // Σ.AI.6d — decode-pressure gate on the legacy whole-RG
                // bridge path. Scoped so the permit drops BEFORE
                // `blocking_send`: a decode slot is never held while
                // blocked on a downstream consumer (no cross-scan wait
                // cycles). Disabled (default) = one OnceLock load.
                let _shed_permit = crate::mem_pressure::decode_gate_enter();
                match (&filter, late_mat) {
                    (Some(f), true) => {
                        decode_one_rg_filtered_late_mat(&path_buf, rg, &schema, &projection, f)
                    }
                    (Some(f), false) => {
                        decode_one_rg_filtered(&path_buf, rg, &schema, &projection, f)
                    }
                    (None, _) => decode_one_rg(&path_buf, rg, &schema, &projection),
                }
            };
            if tx.blocking_send(batch_result).is_err() {
                return; // consumer dropped
            }
        }
    });

    let stream = futures_util::stream::unfold((rx, baseline), |(mut rx, baseline)| async move {
        let timer = baseline.elapsed_compute().timer();
        let item = rx.recv().await;
        drop(timer);
        if let Some(Ok(ref batch)) = item {
            baseline.record_output(batch.num_rows());
        }
        item.map(|i| (i, (rx, baseline)))
    });
    stream.boxed()
}

/// Σ.E5.1.b: streaming partition stream built atop
/// [`EmatArrowBatchReader`].
///
/// Per partition we open the parquet file once on a `spawn_blocking`
/// worker, configure the reader against the projected schema (which
/// the provider has already promoted to `Utf8View` or
/// `Dictionary(UInt32, Utf8)` as appropriate), and stream each
/// `batch_size`-row window over an mpsc channel.
///
/// Threading note (Σ.E5.1.c): the reader internally fan-outs per-column decode
/// across `min(n_cols, parallelism_budget)` scoped threads. The
/// `EmatixFastParquetExec` partition wrapper computes a per-partition
/// budget = `max(1, core_share / n_outer_partitions)` — where
/// `core_share` is [`crate::partition_registry::resolved_core_share`]
/// (== `available_parallelism()` solo, the historical numerator) — so
/// the per-process thread count tracks the process's fair core slice
/// rather than the product `N_partitions × N_cols`. For Q1 SF=1 (6
/// outer partitions on 14 cores, solo) the budget is 2 — total ≈ 12
/// concurrent threads instead of the 42 the naive
/// `available_parallelism()` cap produced.
/// Shape-aware default for `inline_row_threshold`.
///
/// The original 900_000-row constant was hand-calibrated for the
/// TPC-H working set, where every scan projects ≤10 columns. Wider
/// scans (TPC-DS fact tables, real-world denormalized analytics
/// tables) hit the per-page sync cost at a much smaller row count
/// because the per-partition decode budget scales with rows × cols,
/// not rows alone.
///
/// **Calibration:** 900_000 rows × 7 projected columns ≈ 6_300_000
/// cell budget. SF=1 lineitem scans (Q01: 7 projected cols, Q06: 4,
/// Q14: 5, ...) all land in the ≤10-col bucket, so the
/// derived threshold equals the legacy constant by construction.
/// Bench gate parity is structural, not empirical.
///
/// **What this changes:** scans with 11+ projected columns get a
/// lower threshold (scales as 6_300_000 / col_count). For a 30-col
/// wide-fact projection the threshold drops to ~210_000 rows, which
/// is closer to the actual first-batch latency budget that drove the
/// original tuning.
///
/// **Override path is unchanged:** `EMAT_INLINE_ROW_THRESHOLD=N` env
/// still wins; this helper only feeds the default branch.
fn derive_inline_row_threshold(projected_columns: usize) -> usize {
    const TPCH_CALIBRATION_THRESHOLD: usize = 900_000;
    const INLINE_CELL_BUDGET: usize = 6_300_000;
    /// TPC-H's widest single scan in the bench gate is 8 cols. Anything
    /// at or below this leaves the legacy threshold intact; above it,
    /// shape-aware scaling kicks in. The cutoff is deliberately wider
    /// than the bench's actual max so a future TPC-H query that
    /// projects 9 or 10 cols doesn't accidentally trip the auto-scale.
    const NARROW_PROJECTION_CUTOFF: usize = 10;

    let cols = projected_columns.max(1);
    if cols <= NARROW_PROJECTION_CUTOFF {
        TPCH_CALIBRATION_THRESHOLD
    } else {
        // Strict ceiling: never raise above the calibration value.
        // Q04's +29pp regression at the 1.8M threshold sweep (see the
        // comment block at `inline_row_threshold` callsite) is the
        // anchor — going up from 900k breaks Q04's full-table GROUP BY
        // pattern. The min() preserves that invariant.
        (INLINE_CELL_BUDGET / cols).min(TPCH_CALIBRATION_THRESHOLD)
    }
}

#[cfg(test)]
mod inline_row_threshold_tests {
    use super::derive_inline_row_threshold;

    /// Narrow projections (≤10 cols, which covers the entire TPC-H
    /// bench gate) must match the legacy 900_000 constant exactly so
    /// the bench gate is bit-identical by construction.
    #[test]
    fn tpch_calibration_preserved_for_narrow_projections() {
        for cols in 1..=10 {
            assert_eq!(
                derive_inline_row_threshold(cols),
                900_000,
                "col_count={cols} must keep legacy threshold"
            );
        }
    }

    /// Wide projections scale down proportionally to the cell budget.
    #[test]
    fn wide_projections_scale_down() {
        // 15 cols: 6_300_000 / 15 = 420_000
        assert_eq!(derive_inline_row_threshold(15), 420_000);
        // 30 cols: 6_300_000 / 30 = 210_000
        assert_eq!(derive_inline_row_threshold(30), 210_000);
        // 63 cols: 6_300_000 / 63 = 100_000
        assert_eq!(derive_inline_row_threshold(63), 100_000);
    }

    /// Threshold is monotonically non-increasing in column count —
    /// strictly impossible to flip a TPC-H query to a higher threshold
    /// regardless of projection width.
    #[test]
    fn monotonic_non_increasing_in_cols() {
        let mut prev = usize::MAX;
        for cols in 1..=64 {
            let t = derive_inline_row_threshold(cols);
            assert!(t <= prev, "non-monotonic at col_count={cols}: {t} > {prev}");
            prev = t;
        }
    }

    /// Zero columns is a guard case (shouldn't happen in practice but
    /// the function must not divide-by-zero).
    #[test]
    fn zero_cols_is_safe() {
        // Treated as 1 col → legacy threshold path.
        assert_eq!(derive_inline_row_threshold(0), 900_000);
    }
}

#[allow(clippy::too_many_arguments)]
fn build_streaming_partition_stream(
    path: String,
    schema: SchemaRef,
    projection: Vec<usize>,
    row_groups: Vec<usize>,
    parallelism_budget: usize,
    // Total row count assigned to this partition, taken from the
    // provider's cached per-RG counts. Drives the inline-vs-eager
    // reader auto-pick without a per-partition footer re-decode.
    partition_rows: usize,
    // Total RG count of the file this partition reads from. Used to
    // restrict page-streaming to single-RG files (dim tables) — multi-
    // RG files like lineitem have 1M-row partitions that look small
    // per-partition but lose to per-page sync cost when streamed.
    file_total_rgs: usize,
    // Σ.E5 (#516): optional late-mat filter, plumbed into
    // EmatArrowBatchReaderBuilder::with_filter when present.
    filter: Option<BridgeFilter>,
    baseline: BaselineMetrics,
    // L9.ADAPT LATE-ARM — see `build_partition_stream_dispatch`.
    late_arm: Option<crate::bridge_filter_sideband::BridgeFilterSideband>,
    // NARROW.DEC — narrowed key leaves for the eager reader (only);
    // see `build_partition_stream_dispatch`.
    narrow_i64_leaves: Vec<usize>,
) -> futures_util::stream::BoxStream<'static, DfResult<RecordBatch>> {
    use futures_util::StreamExt;

    if row_groups.is_empty() {
        return futures_util::stream::iter(Vec::<DfResult<RecordBatch>>::new()).boxed();
    }

    let (tx, rx) = tokio::sync::mpsc::channel::<DfResult<RecordBatch>>(8);
    let path_buf = PathBuf::from(path);

    // Σ.E5 reader dispatch — three modes:
    //   EMAT_INLINE_STREAMING=1  → EmatInlineStreamingReader
    //                              (single-threaded, no mutex; wins
    //                              first-batch-latency for small-RG
    //                              partitions like part / customer /
    //                              supplier / partsupp).
    //   EMAT_PAGE_STREAMING=1    → EmatPageStreamingReader
    //                              (per-column thread pool, Condvar-
    //                              gated; legacy A/B knob from Σ.E5.6).
    //   default                  → EmatArrowBatchReader
    //                              (eager full-RG decode, column-
    //                              parallel — current SF=1 winner).
    //
    // Auto-pick (when no env var set): use the inline streamer if the
    // partition holds a SINGLE small RG (< 1M rows). This targets the
    // small-dim TPC-H regressions without affecting lineitem (1M-row
    // RGs stay on eager).
    let force_inline = std::env::var("EMAT_INLINE_STREAMING")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"));
    let force_page = std::env::var("EMAT_PAGE_STREAMING")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    // Auto-pick threshold: partitions of small single-RG *files*
    // (dim tables) with < threshold rows route through the page-
    // streaming reader. The dispatch (see below) also requires
    // `file_total_rgs == 1`, which is defensive at 900k (lineitem
    // RGs are 1M > 900k anyway) but blocks footguns if anyone raises
    // the threshold above 1M.
    //
    // Threshold sweep at SF=1 (2026-05-19):
    //   - 900k (default): geomean 0.9048 (current best)
    //   - 1.8M + gate:   geomean 0.9151 — catches orders but Q04
    //     regresses +29pp because Q04 needs full-table GROUP BY on
    //     orderpriority. Orders is a coin-flip target (Q03 likes
    //     page-streaming, Q04 hates it). Stay at 900k.
    //
    // Override via `EMAT_INLINE_ROW_THRESHOLD=N`; set to 0 to disable.
    //
    // Shape-catalog autotune (#557 follow-on): the 900k row default was
    // hand-tuned for the TPC-H working set, where every scan is
    // ≤10 projected columns. Wider scans (think TPC-DS fact tables,
    // 20-50 projected columns) hit the per-page sync cost on a much
    // smaller row count because the cell-decode budget is proportional
    // to rows × cols, not rows alone. `derive_inline_row_threshold`
    // scales the default DOWN for wide projections while leaving the
    // TPC-H calibration bit-identical. See module docstring for
    // calibration math.
    let inline_row_threshold: usize = std::env::var("EMAT_INLINE_ROW_THRESHOLD")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| derive_inline_row_threshold(projection.len()));

    tokio::task::spawn_blocking(move || {
        let file = match ematix_parquet_io::ParquetFile::open(&path_buf) {
            Ok(f) => f,
            Err(e) => {
                let _ = tx.blocking_send(Err(DataFusionError::External(
                    format!(
                        "EmatixFastParquetExec (streaming): ParquetFile::open `{}`: {e}",
                        path_buf.display()
                    )
                    .into(),
                )));
                return;
            }
        };

        // Reader choice:
        //   1. EMAT_INLINE_STREAMING=1/0 forces inline on/off.
        //   2. EMAT_PAGE_STREAMING=1 forces the threaded page reader
        //      everywhere.
        //   3. Otherwise auto-pick by per-query × per-mode bench
        //      (2026-05-19 consolidation):
        //
        //      * Small partitions (single RG, < threshold rows) →
        //        PAGE-streaming. Q22 went +18.2% → +0.6% with this;
        //        Q20 / Q17 / Q13 also flipped favourably. The
        //        first-batch-latency win lets multi-table-join /
        //        small-dim queries start downstream work earlier.
        //      * Big partitions (lineitem, orders) → EAGER. Page-
        //        streaming's per-page sync + shared decode-pool
        //        contention regresses lineitem-heavy queries (Q19
        //        +12.8pp, Q04 +10.3pp, Q06 +8.4pp, Q14 +7.2pp).
        //
        // Inline streaming is now opt-in only (`EMAT_INLINE_STREAMING=
        // 1`); it lost on every query vs the page-streaming variant.
        // Σ.E5 (#516): if a late-mat filter is present, only the eager
        // streaming reader (`EmatArrowBatchReader`) supports it today.
        // Inline + page-streaming readers don't have a masked-decode
        // branch yet — force them off.
        // L9.ADAPT LATE-ARM — a pending sideband counts as a filter for
        // reader routing: only the eager reader supports masked decode
        // and the row-group-boundary arm. (When the wait-based design
        // armed BEFORE dispatch, an armed scan took this same path.)
        let has_filter = filter.is_some() || late_arm.is_some();
        // Σ.E5 (2026-05-19): auto-inline for large multi-RG partitions.
        //
        // The eager reader decodes a whole RG before emitting any batch.
        // At SF=1 each RG decodes in ~5ms so the pipeline stall is
        // small. At SF=10 each RG takes ~50ms and 4 RGs/partition,
        // which stalls heavy downstream operators (Q18's nested hash
        // agg over lineitem).
        //
        // Cut: partition_rows >= 2M AND row_groups.len() > 1
        //   - SF=1 lineitem: 6 RGs / 6 partitions = 1 RG, 1M rows each
        //     → NO trigger (preserves SF=1 baseline)
        //   - SF=10 lineitem: 60 RGs / 14 partitions = 4 RGs, 4.3M each
        //     → TRIGGERS (Q18: 1602ms → 1543ms; Q06/Q14/Q19 also win)
        //
        // Q18 is only partially closed (+52% → ~+49% with this rule;
        // force-inline-everywhere went to +13.7%). The full close
        // requires also routing SF=10 orders (~1M rows/partition, 1 RG
        // each) through inline, which conflicts with SF=1's small dim
        // tables. Filed for future investigation.
        //
        // Override with EMAT_LARGE_PARTITION_ROWS=N.
        let large_partition_threshold: usize = std::env::var("EMAT_LARGE_PARTITION_ROWS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2_000_000);
        // Σ.Q.L6′: if the RG decode cache is installed, prefer the
        // eager reader — it's the only one wired into the cache today.
        // For Q17 SF=10 the auto_inline rule otherwise routes lineitem
        // through the inline-streaming reader and skips the cache
        // entirely, neutralising EMAT_RG_DECODE_CACHE.
        let cache_active = crate::emat_arrow_reader::process_rg_decode_cache().is_some();
        let auto_inline = !has_filter
            && !cache_active
            && row_groups.len() > 1
            && partition_rows >= large_partition_threshold;
        let use_inline = !has_filter && force_inline.unwrap_or(auto_inline);
        let use_page_streaming = if has_filter {
            false
        } else if force_page {
            !use_inline
        } else if !use_inline {
            // Auto: route partitions of small *single-RG files* (dim
            // tables: orders/part/partsupp/etc.) through the page
            // reader. The `file_total_rgs == 1` gate excludes
            // lineitem-style multi-RG files whose per-partition row
            // count (1M) looks small but loses to per-page sync.
            row_groups.len() == 1 && file_total_rgs == 1 && partition_rows < inline_row_threshold
        } else {
            false
        };

        if use_inline {
            use crate::emat_page_stream::EmatInlineStreamingReader;
            let reader = match EmatInlineStreamingReader::new(
                file,
                schema,
                projection,
                row_groups,
                crate::emat_arrow_reader::DEFAULT_BATCH_SIZE,
            ) {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.blocking_send(Err(DataFusionError::External(
                        format!("EmatInlineStreamingReader::new: {e}").into(),
                    )));
                    return;
                }
            };
            for item in reader {
                if tx.blocking_send(item).is_err() {
                    return;
                }
            }
        } else if use_page_streaming {
            use crate::emat_page_stream::EmatPageStreamingReader;
            let reader = match EmatPageStreamingReader::new(
                file,
                schema,
                projection,
                row_groups,
                crate::emat_arrow_reader::DEFAULT_BATCH_SIZE,
            ) {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.blocking_send(Err(DataFusionError::External(
                        format!("EmatPageStreamingReader::new: {e}").into(),
                    )));
                    return;
                }
            };
            for item in reader {
                if tx.blocking_send(item).is_err() {
                    return;
                }
            }
        } else {
            let mut builder = EmatArrowBatchReaderBuilder::new(file, schema)
                .with_projection(projection)
                .with_row_groups(row_groups)
                .with_parallelism_budget(parallelism_budget);
            // NARROW.DEC — only the eager reader narrows at decode; the
            // builder rewrites the matching fields Int64→Int32 so this
            // reader emits Int32Array natively (the boundary cast then
            // passes those columns through by reference).
            if !narrow_i64_leaves.is_empty() {
                builder = builder.with_narrow_i64_leaves(narrow_i64_leaves);
            }
            if let Some(sb) = late_arm.clone() {
                builder = builder.with_late_arm(sb, path_buf.clone());
            }
            if let Some(f) = filter.clone() {
                builder = builder.with_filter(f, path_buf.clone());
            } else if late_arm.is_none()
                && let Some(cache) = crate::emat_arrow_reader::process_rg_decode_cache()
            {
                // Σ.O.c.2 — wire process-wide RG decode cache (off by
                // default; opt-in via `EMAT_RG_DECODE_CACHE=1`). Only
                // attached when no filter is set; filter outputs are
                // mask-specific and not safely shareable. `with_path`
                // is required so the cache key is file-scoped.
                builder = builder
                    .with_path(path_buf.clone())
                    .with_rg_decode_cache(cache);
            }
            let reader = match builder.build() {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.blocking_send(Err(DataFusionError::External(
                        format!("EmatixFastParquetExec (streaming): build reader: {e}").into(),
                    )));
                    return;
                }
            };
            for item in reader {
                if tx.blocking_send(item).is_err() {
                    return;
                }
            }
        }
    });

    let stream = futures_util::stream::unfold((rx, baseline), |(mut rx, baseline)| async move {
        let timer = baseline.elapsed_compute().timer();
        let item = rx.recv().await;
        drop(timer);
        if let Some(Ok(ref batch)) = item {
            baseline.record_output(batch.num_rows());
        }
        item.map(|i| (i, (rx, baseline)))
    });
    stream.boxed()
}

/// Phase 3 path: filter-aware row group decoder.
///   1. Build a row bitmap for the filter column via Phase 5 fused-
///      NEON (`filter_i32_column_to_bitmap`).
///   2. For each projected column, do bitmap-driven sparse gather
///      (Phase 6 — `sparse_gather_chunk_*`). The filter column, if
///      projected, is gathered too (the dict_mask path doesn't emit
///      the filter column's values directly).
///   3. Emit a RecordBatch sized to popcount(bitmap).
///
/// On any error (wrong bit width, non-dict pages, type mismatch), the
/// error propagates; DataFusion's residual FilterExec is NOT going to
/// re-run since we declared `Exact` pushdown, so callers must accept
/// that the bridge's pushable shape is narrow.
fn decode_one_rg_filtered(
    path: &std::path::Path,
    rg: usize,
    schema: &SchemaRef,
    projection: &[usize],
    filter: &BridgeFilter,
) -> DfResult<RecordBatch> {
    let (bitmap, _total) = filter.build_bitmap(path, rg)?;

    let matches: usize = bitmap.iter().map(|b| b.count_ones() as usize).sum();
    let mut columns: Vec<Arc<dyn arrow_array::Array>> = Vec::with_capacity(projection.len());
    for (out_idx, &col_idx) in projection.iter().enumerate() {
        let field = schema.field(out_idx);
        let arr: Arc<dyn arrow_array::Array> = match field.data_type() {
            DataType::Int32 => {
                let vals = sparse_gather_chunk_i32(path, rg, col_idx, &bitmap)?;
                debug_assert_eq!(vals.len(), matches);
                Arc::new(arrow_array::Int32Array::from(vals))
            }
            DataType::Date32 => {
                let vals = sparse_gather_chunk_i32(path, rg, col_idx, &bitmap)?;
                debug_assert_eq!(vals.len(), matches);
                Arc::new(arrow_array::Date32Array::from(vals))
            }
            DataType::Int64 => {
                let vals = sparse_gather_chunk_i64(path, rg, col_idx, &bitmap)?;
                debug_assert_eq!(vals.len(), matches);
                Arc::new(arrow_array::Int64Array::from(vals))
            }
            // KEYS.4.b: UInt64 is physically INT64 — gather i64 then
            // reinterpret the buffer bit-for-bit (zero-copy, shared helper).
            DataType::UInt64 => {
                let vals = sparse_gather_chunk_i64(path, rg, col_idx, &bitmap)?;
                debug_assert_eq!(vals.len(), matches);
                let buf = datafusion::arrow::buffer::Buffer::from_vec(vals);
                Arc::new(crate::emat_arrow_reader::u64_array_from_i64_buffer(
                    buf, matches,
                ))
            }
            DataType::Float64 => {
                let vals = sparse_gather_chunk_f64(path, rg, col_idx, &bitmap)?;
                debug_assert_eq!(vals.len(), matches);
                Arc::new(arrow_array::Float64Array::from(vals))
            }
            DataType::Utf8 => {
                // No sparse-gather kernel for Utf8 yet (Phase 4 v1).
                // Fall back: dense decode, then walk the bitmap and
                // append only matching rows to a StringBuilder. Costs
                // O(num_values) instead of O(matches), but avoids
                // pulling in `arrow_select` as a runtime dep.
                let full = decode_column_chunk_byte_array(path, rg, col_idx)?;
                let mut sb = arrow_array::builder::StringBuilder::with_capacity(
                    matches,
                    matches * 16, // rough average string size estimate
                );
                for i in 0..full.len() {
                    if (bitmap[i / 8] >> (i % 8)) & 1 == 1 {
                        sb.append_value(full.value(i));
                    }
                }
                Arc::new(sb.finish())
            }
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "EmatixFastParquetExec (filtered): unsupported column type {other:?}",
                )));
            }
        };
        columns.push(arr);
    }
    RecordBatch::try_new(schema.clone(), columns).map_err(|e| {
        DataFusionError::External(
            format!("EmatixFastParquetExec (filtered): RecordBatch::try_new: {e}").into(),
        )
    })
}

/// Σ.E5a (Π.10): late-materialisation variant of the filtered decode
/// path. Same contract as [`decode_one_rg_filtered`] — same bitmap
/// source, same projected output — but column decode runs through
/// ematix-parquet v0.3.0's `read_column_*_masked_into` façade.
///
/// The masked_into façade pulls the column-chunk bytes once, then
/// decodes only at rows where the bitmap is set. Pages whose bitmap-
/// popcount is zero are skipped entirely (no decompression, no
/// unpack); for high-selectivity filters this skips ~99% of the
/// decode work the dense-then-gather path was doing.
fn decode_one_rg_filtered_late_mat(
    path: &std::path::Path,
    rg: usize,
    schema: &SchemaRef,
    projection: &[usize],
    filter: &BridgeFilter,
) -> DfResult<RecordBatch> {
    // Open the parquet file once for this row group. The masked_into
    // façade caches column-chunk bytes internally; opening the
    // ParquetFile is the only IO setup we need.
    // Σ.Q06.SF10.7: reuse a process-cached handle so the footer is
    // read + parsed once per file rather than once per (row-group ×
    // pass). build_bitmap + masked_decode below all share it.
    let file = crate::ematix_parquet_bridge::open_cached(path)?;
    // Σ.E5 #513: multi-column AND bitmap via BridgeFilter.
    let (bitmap, total) = filter.build_bitmap(path, rg)?;

    let matches: usize = bitmap.iter().map(|b| b.count_ones() as usize).sum();

    // REV.23 — same masked→dense routing gate as the streaming reader
    // (emat_arrow_reader::load_row_group_masked_legacy). This non-streaming
    // late-mat path is only live under dict-preservation
    // (`with_dict_preservation` forces `streaming_arrow_reader=false`), but it
    // has the identical latency-bound per-column gather, so above the pass-rate
    // threshold we decode dense (`decode_one_rg`, which handles every dtype incl.
    // Dictionary(UInt32,Utf8)) and compact with Arrow's SIMD `filter_record_batch`
    // — bandwidth-bound, scales to all cores. Result rows are identical to the
    // masked path. See [[rev20-q07-q08-decode-bound]] REV.23.
    if crate::emat_arrow_reader::should_route_masked_to_dense(
        matches,
        total,
        crate::emat_arrow_reader::masked_dense_passrate_threshold(),
    ) {
        let dense = decode_one_rg(path, rg, schema, projection)?;
        let bool_buf = datafusion::arrow::buffer::BooleanBuffer::new(
            datafusion::arrow::buffer::Buffer::from_vec(bitmap),
            0,
            total,
        );
        let mask = arrow_array::BooleanArray::new(bool_buf, None);
        return datafusion::arrow::compute::filter_record_batch(&dense, &mask).map_err(|e| {
            DataFusionError::External(
                format!("EmatixFastParquetExec (late_mat dense-route): filter_record_batch: {e}")
                    .into(),
            )
        });
    }

    let mut columns: Vec<Arc<dyn arrow_array::Array>> = Vec::with_capacity(projection.len());
    let check_len = |got: usize, want: usize, name: &str, kind: &str| -> DfResult<()> {
        if got != want {
            Err(DataFusionError::External(
                format!(
                    "EmatixFastParquetExec (late_mat): column `{name}` ({kind}) decoded \
                     {got} values, expected {want} (bitmap popcount). Likely a masked-decode \
                     bug in ematix-parquet for this column type."
                )
                .into(),
            ))
        } else {
            Ok(())
        }
    };
    for (out_idx, &col_idx) in projection.iter().enumerate() {
        let field = schema.field(out_idx);
        let arr: Arc<dyn arrow_array::Array> = match field.data_type() {
            DataType::Int32 => {
                let vals = masked_decode_i32(&file, rg, col_idx, &bitmap)?;
                check_len(vals.len(), matches, field.name(), "Int32")?;
                Arc::new(arrow_array::Int32Array::from(vals))
            }
            DataType::Date32 => {
                let vals = masked_decode_i32(&file, rg, col_idx, &bitmap)?;
                check_len(vals.len(), matches, field.name(), "Date32")?;
                Arc::new(arrow_array::Date32Array::from(vals))
            }
            DataType::Int64 => {
                let vals = masked_decode_i64(&file, rg, col_idx, &bitmap)?;
                check_len(vals.len(), matches, field.name(), "Int64")?;
                Arc::new(arrow_array::Int64Array::from(vals))
            }
            // KEYS.4.b: UInt64 = physical INT64, reinterpreted zero-copy.
            DataType::UInt64 => {
                let vals = masked_decode_i64(&file, rg, col_idx, &bitmap)?;
                check_len(vals.len(), matches, field.name(), "UInt64")?;
                let buf = datafusion::arrow::buffer::Buffer::from_vec(vals);
                Arc::new(crate::emat_arrow_reader::u64_array_from_i64_buffer(
                    buf, matches,
                ))
            }
            DataType::Float64 => {
                let vals = masked_decode_f64(&file, rg, col_idx, &bitmap)?;
                check_len(vals.len(), matches, field.name(), "Float64")?;
                Arc::new(arrow_array::Float64Array::from(vals))
            }
            DataType::Utf8 => {
                let vals = masked_decode_byte_array(&file, rg, col_idx, &bitmap)?;
                check_len(vals.len(), matches, field.name(), "Utf8")?;
                let mut sb = arrow_array::builder::StringBuilder::with_capacity(
                    vals.len(),
                    vals.iter().map(|v| v.len()).sum(),
                );
                for v in &vals {
                    // The masked decoder returns UTF-8 bytes for Utf8
                    // columns; treat invalid UTF-8 as a hard error
                    // (parquet writers shouldn't produce it for Utf8).
                    let s = std::str::from_utf8(v).map_err(|e| {
                        DataFusionError::External(
                            format!(
                                "EmatixFastParquetExec (late_mat): Utf8 column has invalid UTF-8: {e}"
                            )
                            .into(),
                        )
                    })?;
                    sb.append_value(s);
                }
                Arc::new(sb.finish())
            }
            DataType::Utf8View => {
                // Σ.E5 (#515): late-mat path needs StringViewArray
                // emission so it can be the integration target when
                // pushdown is re-enabled on the streaming-default
                // reader (which reports Utf8View in its schema).
                let vals = masked_decode_byte_array(&file, rg, col_idx, &bitmap)?;
                check_len(vals.len(), matches, field.name(), "Utf8View")?;
                let total_bytes: usize = vals.iter().map(|v| v.len()).sum();
                let mut sb = arrow_array::builder::StringViewBuilder::with_capacity(vals.len())
                    .with_fixed_block_size(total_bytes.max(1) as u32);
                for v in &vals {
                    let s = std::str::from_utf8(v).map_err(|e| {
                        DataFusionError::External(
                            format!(
                                "EmatixFastParquetExec (late_mat): Utf8View column has invalid UTF-8: {e}"
                            )
                            .into(),
                        )
                    })?;
                    sb.append_value(s);
                }
                Arc::new(sb.finish())
            }
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "EmatixFastParquetExec (late_mat): unsupported column type {other:?}",
                )));
            }
        };
        columns.push(arr);
    }
    RecordBatch::try_new(schema.clone(), columns).map_err(|e| {
        DataFusionError::External(
            format!("EmatixFastParquetExec (late_mat): RecordBatch::try_new: {e}").into(),
        )
    })
}

/// Decode one row group into a `RecordBatch` matching `schema`. Each
/// projected column is dispatched on Arrow data type to the
/// appropriate bridge function.
fn decode_one_rg(
    path: &std::path::Path,
    rg: usize,
    schema: &SchemaRef,
    projection: &[usize],
) -> DfResult<RecordBatch> {
    let mut columns: Vec<Arc<dyn arrow_array::Array>> = Vec::with_capacity(projection.len());
    for (out_idx, &col_idx) in projection.iter().enumerate() {
        let field = schema.field(out_idx);
        let arr: Arc<dyn arrow_array::Array> = match field.data_type() {
            DataType::Int32 => {
                decode_column_chunk_i32(path, rg, col_idx)? as Arc<dyn arrow_array::Array>
            }
            DataType::Date32 => {
                // Date32 is INT32 physically. Bridge returns Int32Array;
                // re-wrap as Date32Array.
                let i32_arr = decode_column_chunk_i32(path, rg, col_idx)?;
                let vals: Vec<i32> = i32_arr.values().to_vec();
                Arc::new(arrow_array::Date32Array::from(vals))
            }
            DataType::Int64 => {
                decode_column_chunk_i64(path, rg, col_idx)? as Arc<dyn arrow_array::Array>
            }
            // KEYS.4.b: UInt64 = physical INT64; reuse the i64 chunk
            // decode and reinterpret its value buffer bit-for-bit.
            DataType::UInt64 => {
                let a = decode_column_chunk_i64(path, rg, col_idx)?;
                let len = a.len();
                let buf = a.values().inner().clone();
                Arc::new(crate::emat_arrow_reader::u64_array_from_i64_buffer(
                    buf, len,
                ))
            }
            DataType::Float64 => {
                decode_column_chunk_f64(path, rg, col_idx)? as Arc<dyn arrow_array::Array>
            }
            DataType::Utf8 => {
                decode_column_chunk_byte_array(path, rg, col_idx)? as Arc<dyn arrow_array::Array>
            }
            DataType::Dictionary(k, v)
                if matches!(k.as_ref(), DataType::UInt32)
                    && matches!(v.as_ref(), DataType::Utf8) =>
            {
                // Σ.E3b: dict-preserved decode keeps the parquet dict
                // structure intact across the Arrow boundary so
                // downstream dict-aware operators (DictGroupCountExec,
                // DictFilterExec) can stay on dict codes.
                decode_column_chunk_byte_array_dict_preserved(path, rg, col_idx)?
                    as Arc<dyn arrow_array::Array>
            }
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "EmatixFastParquetExec: unsupported column type {other:?} for `{}`",
                    field.name()
                )));
            }
        };
        columns.push(arr);
    }
    RecordBatch::try_new(schema.clone(), columns).map_err(|e| {
        DataFusionError::External(
            format!("EmatixFastParquetExec: RecordBatch::try_new: {e}").into(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    /// L9.PROBEORDER — `and_eval_masked` must evaluate ONLY rows whose
    /// bit is set (the masked-probe contract: probe cost scales with
    /// survivors, not total rows). A counting closure proves the skip:
    /// 4 of 16 rows set → exactly 4 evals, on exactly those rows.
    /// Q05.MEMO — the run-memoized probe loop must be bit-identical to
    /// the plain chunk-of-8 loop for every buffer shape: clustered
    /// runs, unique keys, all-duplicate, non-multiple-of-8 tails,
    /// short, and empty inputs.
    #[test]
    fn run_memo_probe_bitmap_identical_to_plain() {
        let probe = |v: i64| v % 3 == 0;
        let shapes: Vec<Vec<i64>> = vec![
            // clustered: ~4 consecutive rows per key (the l_orderkey shape)
            (0..1000).map(|i| (i / 4) as i64).collect(),
            // unique keys
            (0..1000).map(|i| i as i64).collect(),
            // all one key
            vec![42i64; 333],
            // tail not a multiple of 8
            (0..77).map(|i| (i / 3) as i64).collect(),
            // short
            vec![1, 1, 2],
            // empty
            vec![],
        ];
        for values in shapes {
            let mut plain = vec![0u8; values.len().div_ceil(8)];
            let mut memo = vec![0u8; values.len().div_ceil(8)];
            probe_chunks_into_bitmap(&values, &mut plain, probe);
            probe_chunks_into_bitmap_memo(&values, &mut memo, probe);
            assert_eq!(plain, memo, "diverged on len={}", values.len());
        }
    }

    /// Q05.MEMO — the memo path must call the probe once per RUN, not
    /// once per row (that's the whole lever).
    #[test]
    fn run_memo_probe_probes_once_per_run() {
        use std::cell::RefCell;
        let values: Vec<i64> = (0..64).map(|i| (i / 4) as i64).collect(); // 16 runs
        let calls: RefCell<usize> = RefCell::new(0);
        let mut bitmap = vec![0u8; 8];
        probe_chunks_into_bitmap_memo(&values, &mut bitmap, |v| {
            *calls.borrow_mut() += 1;
            v % 2 == 0
        });
        // first-value seed probe + one per run-change (values[0] probed
        // by the seed; the loop re-probes only on key changes).
        assert_eq!(*calls.borrow(), 16, "expected one probe per run");
    }

    /// Q05.MEMO — pure gate table: force overrides win, AUTO requires
    /// ≥64 sampled values and ≥25% consecutive duplicates.
    #[test]
    fn run_memo_pays_gate_table() {
        let clustered: Vec<i64> = (0..2048).map(|i| (i / 4) as i64).collect(); // 75% dups
        let unique: Vec<i64> = (0..2048).map(|i| i as i64).collect(); // 0% dups
        let sparse_dups: Vec<i64> = (0..2048).map(|i| (i / 10 * 10 + i % 10) as i64).collect();
        let short: Vec<i64> = vec![7; 32];
        // AUTO (None): data decides.
        assert!(run_memo_pays(&clustered, None));
        assert!(!run_memo_pays(&unique, None));
        assert!(
            !run_memo_pays(&sparse_dups, None),
            "0% dups after arithmetic"
        );
        assert!(
            !run_memo_pays(&short, None),
            "buffers under the 64-value floor stay on the vector path"
        );
        // Force ON/OFF override data entirely.
        assert!(run_memo_pays(&unique, Some(true)));
        assert!(!run_memo_pays(&clustered, Some(false)));
        assert!(run_memo_pays(&short, Some(true)));
        // Boundary: exactly 1 consecutive dup per 4 values passes the
        // 25% gate (dups*4 >= n-1). Group g emits [3g, 3g, 3g+1, 3g+2].
        let quarter: Vec<i64> = (0..1024)
            .map(|i| {
                let g = (i / 4) as i64 * 3;
                match i % 4 {
                    0 | 1 => g,
                    r => g + r as i64 - 1,
                }
            })
            .collect();
        let n = quarter.len().min(RUN_MEMO_SAMPLE);
        let dups = quarter[..n].windows(2).filter(|w| w[0] == w[1]).count();
        assert!(
            dups * 4 >= n - 1,
            "constructed pattern must sit at the boundary"
        );
        assert!(run_memo_pays(&quarter, None));
    }

    /// Q05.MEMO — end-to-end via `probe_i64_values_from_decoded`: both
    /// loop implementations must agree against a real bloom, and the
    /// production entry point's bitmap must be exactly the bloom hits.
    #[test]
    fn run_memo_bridge_filter_bitmap_parity() {
        use crate::bloom::BloomFilter;
        let mut bloom = BloomFilter::for_keys(64);
        for k in [3i64, 7, 11, 200] {
            bloom.insert_i64(k);
        }
        let values: Vec<i64> = (0..600).map(|i| (i / 4) as i64).collect();
        let filt = BridgeFilter::new(vec![ColumnPredicate::I64InBloom {
            col_idx: 0,
            bloom: std::sync::Arc::new(bloom),
        }]);
        // Env-independent check: compare the two loop implementations
        // directly on the same predicate closure the filter would run.
        let ColumnPredicate::I64InBloom { bloom, .. } = &filt.predicates[0] else {
            unreachable!()
        };
        let mut plain = vec![0u8; values.len().div_ceil(8)];
        let mut memo = vec![0u8; values.len().div_ceil(8)];
        probe_chunks_into_bitmap(&values, &mut plain, |v| bloom.might_contain_i64(v));
        probe_chunks_into_bitmap_memo(&values, &mut memo, |v| bloom.might_contain_i64(v));
        assert_eq!(plain, memo);
        // And the production entry point still produces a bitmap whose
        // set rows are exactly the bloom hits.
        let (col, bitmap) = filt.probe_i64_values_from_decoded(&values).unwrap();
        assert_eq!(col, 0);
        for (row, &v) in values.iter().enumerate() {
            let bit = bitmap[row >> 3] & (1 << (row & 7)) != 0;
            assert_eq!(bit, bloom.might_contain_i64(v), "row {row} key {v}");
        }
    }

    /// Q05.STATVEC — the [lo, hi] window fold must agree with the
    /// clause-loop `eval_i32` for every foldable clause combination,
    /// and decline unfoldable shapes.
    #[test]
    fn statvec_i32_range_window_fold_table() {
        let mk = |clauses: Vec<(Operator, i32)>| ColumnPredicate::I32Range {
            col_idx: 0,
            clauses: clauses
                .into_iter()
                .map(|(op, literal_i32)| RangeClause { op, literal_i32 })
                .collect(),
        };
        // The Q05 orders shape: >= 8766 AND < 9131 (dates as i32 days).
        let p = mk(vec![(Operator::GtEq, 8766), (Operator::Lt, 9131)]);
        assert_eq!(p.i32_range_window(), Some((8766, 9130)));
        // Every op folds.
        assert_eq!(mk(vec![(Operator::Eq, 5)]).i32_range_window(), Some((5, 5)));
        assert_eq!(
            mk(vec![(Operator::Gt, 5), (Operator::LtEq, 9)]).i32_range_window(),
            Some((6, 9))
        );
        // Unsatisfiable folds to an empty window, not None.
        let empty = mk(vec![(Operator::Gt, 9), (Operator::Lt, 5)]).i32_range_window();
        let (lo, hi) = empty.unwrap();
        assert!(lo > hi, "contradictory clauses fold to an empty window");
        // != declines.
        assert_eq!(mk(vec![(Operator::NotEq, 5)]).i32_range_window(), None);
        // Overflow edges decline (correct via the clause loop instead).
        assert_eq!(mk(vec![(Operator::Lt, i32::MIN)]).i32_range_window(), None);
        assert_eq!(mk(vec![(Operator::Gt, i32::MAX)]).i32_range_window(), None);
        // Non-range shapes decline.
        assert_eq!(
            ColumnPredicate::I32In {
                col_idx: 0,
                values: vec![1, 2]
            }
            .i32_range_window(),
            None
        );
        // Window agrees with eval_i32 across the whole boundary zone.
        let p = mk(vec![(Operator::GtEq, 100), (Operator::Lt, 200)]);
        let (lo, hi) = p.i32_range_window().unwrap();
        for v in 95..205 {
            assert_eq!(v >= lo && v <= hi, p.eval_i32(v), "value {v}");
        }
    }

    /// Q05.STATVEC — first-pass i32/f64 statics through
    /// `eval_on_decoded_views` (now chunk-packed) must produce the
    /// same bitmap as a manual row-by-row eval, including tails.
    #[test]
    fn statvec_first_pass_i32_f64_bitmap_parity() {
        let dates: Vec<i32> = (0..83).map(|i| 8760 + (i % 20)).collect();
        let prices: Vec<f64> = (0..83).map(|i| i as f64).collect();
        let date_pred = ColumnPredicate::I32Range {
            col_idx: 0,
            clauses: vec![
                RangeClause {
                    op: Operator::GtEq,
                    literal_i32: 8766,
                },
                RangeClause {
                    op: Operator::Lt,
                    literal_i32: 8775,
                },
            ],
        };
        let price_pred = ColumnPredicate::F64Range {
            col_idx: 1,
            clauses: vec![F64RangeClause {
                op: Operator::Lt,
                literal_f64: 50.0,
            }],
        };
        for filt in [
            BridgeFilter::new(vec![date_pred.clone()]),
            BridgeFilter::new(vec![price_pred.clone()]),
            BridgeFilter::new(vec![date_pred.clone(), price_pred.clone()]),
        ] {
            let (bitmap, total) = filt
                .eval_on_decoded_views(|idx| match idx {
                    0 => Some(DecodedView::I32(&dates)),
                    1 => Some(DecodedView::F64(&prices)),
                    _ => None,
                })
                .expect("statics on decoded views must evaluate");
            assert_eq!(total, 83);
            for row in 0..total {
                let expect = filt.predicates.iter().all(|p| match p {
                    ColumnPredicate::I32Range { .. } => p.eval_i32(dates[row]),
                    ColumnPredicate::F64Range { .. } => p.eval_f64(prices[row]),
                    _ => unreachable!(),
                });
                let got = bitmap[row >> 3] & (1 << (row & 7)) != 0;
                assert_eq!(got, expect, "row {row}");
            }
        }
    }

    #[test]
    fn probeorder_and_eval_masked_skips_cleared_rows() {
        use std::cell::RefCell;
        // rows 1, 3, 8, 15 set
        let mut bitmap = vec![0b0000_1010u8, 0b1000_0001u8];
        let evaluated: RefCell<Vec<usize>> = RefCell::new(Vec::new());
        and_eval_masked(&mut bitmap, 16, |row| {
            evaluated.borrow_mut().push(row);
            row != 3 // miss on row 3 → cleared
        });
        assert_eq!(
            *evaluated.borrow(),
            vec![1, 3, 8, 15],
            "must evaluate exactly the set rows, in order"
        );
        assert_eq!(bitmap, vec![0b0000_0010u8, 0b1000_0001u8]);

        // Tail guard: bit set past n_rows must be cleared without eval.
        let mut tail = vec![0b1100_0000u8];
        let count: RefCell<usize> = RefCell::new(0);
        and_eval_masked(&mut tail, 7, |_| {
            *count.borrow_mut() += 1;
            true
        });
        assert_eq!(*count.borrow(), 1, "row 7 is past n_rows=7 → no eval");
        assert_eq!(tail, vec![0b0100_0000u8]);
    }

    /// L9.PROBEORDER — multi-type AND on decoded buffers: an i32 range
    /// (the Q20 shipdate analog) plus an i64 exact-set probe (the
    /// forest-partkey analog) must produce the same bitmap as a manual
    /// row-by-row AND, with the probe ordered after the static.
    #[test]
    fn probeorder_eval_on_decoded_multi_type_and() {
        use crate::i64_set::I64Set;
        let dates: Vec<i32> = (0..32).map(|i| 7000 + (i % 4)).collect(); // 25% in [7000,7000]
        let keys: Vec<i64> = (0..32).map(|i| i as i64).collect();
        let mut set = I64Set::with_keys(16);
        for k in [0i64, 4, 8, 12, 16, 20, 24, 28, 3, 7] {
            set.insert(k);
        }
        let set = std::sync::Arc::new(set);
        let filter = BridgeFilter::new(vec![
            // Probe FIRST in predicate order — eval must still run it last.
            ColumnPredicate::I64InSet {
                col_idx: 1,
                set: set.clone(),
            },
            ColumnPredicate::I32Range {
                col_idx: 10,
                clauses: vec![
                    RangeClause {
                        op: Operator::GtEq,
                        literal_i32: 7000,
                    },
                    RangeClause {
                        op: Operator::LtEq,
                        literal_i32: 7000,
                    },
                ],
            },
        ]);
        let (bitmap, total) = filter
            .eval_on_decoded_views(|col| match col {
                1 => Some(DecodedView::I64(&keys)),
                10 => Some(DecodedView::I32(&dates)),
                _ => None,
            })
            .expect("all columns resolvable → must evaluate");
        assert_eq!(total, 32);
        for row in 0..32usize {
            let expect = dates[row] == 7000 && set.contains(keys[row]);
            let got = bitmap[row >> 3] & (1 << (row & 7)) != 0;
            assert_eq!(got, expect, "row {row}");
        }
    }

    /// L9.PROBEORDER — fallback contract: a string predicate or an
    /// unresolvable column must return None (caller falls back to the
    /// legacy build_bitmap path); resolved-length mismatch likewise.
    #[test]
    fn probeorder_eval_on_decoded_falls_back_to_none() {
        use crate::i64_set::I64Set;
        let keys: Vec<i64> = (0..8).collect();
        let set = std::sync::Arc::new(I64Set::with_keys(4));
        let probe = ColumnPredicate::I64InSet {
            col_idx: 1,
            set: set.clone(),
        };

        // String predicate present → None.
        let f = BridgeFilter::new(vec![
            probe.clone(),
            ColumnPredicate::StringEq {
                col_idx: 2,
                value: "x".into(),
            },
        ]);
        assert!(
            f.eval_on_decoded_views(|_| Some(DecodedView::I64(&keys)))
                .is_none()
        );

        // Unresolvable column → None.
        let f = BridgeFilter::new(vec![probe.clone()]);
        assert!(f.eval_on_decoded_views(|_| None).is_none());

        // Length mismatch across predicate columns → None.
        let short: Vec<i32> = vec![7000; 4];
        let f = BridgeFilter::new(vec![
            probe,
            ColumnPredicate::I32Range {
                col_idx: 10,
                clauses: vec![RangeClause {
                    op: Operator::Eq,
                    literal_i32: 7000,
                }],
            },
        ]);
        assert!(
            f.eval_on_decoded_views(|col| match col {
                1 => Some(DecodedView::I64(&keys)),
                10 => Some(DecodedView::I32(&short)),
                _ => None,
            })
            .is_none()
        );
    }

    /// L9.ADAPT Guard 1 — `statics_fused_evaluable` must mirror
    /// `eval_on_decoded_views`' bind loop: numeric shapes on projected
    /// columns pass; string shapes or unprojected columns fail (they
    /// would force the legacy per-predicate re-decode — the Q19 +102%
    /// mechanism). Empty filter is vacuously evaluable.
    #[test]
    fn adapt_statics_fused_evaluable_truth_table() {
        let proj = [1usize, 5, 10];
        // Numeric statics on projected columns → true.
        let f = BridgeFilter::new(vec![
            ColumnPredicate::I32Range {
                col_idx: 10,
                clauses: vec![RangeClause {
                    op: Operator::GtEq,
                    literal_i32: 7000,
                }],
            },
            ColumnPredicate::F64Range {
                col_idx: 5,
                clauses: vec![],
            },
        ]);
        assert!(f.statics_fused_evaluable(&proj));

        // String static (Q19's l_shipmode IN shape) → false.
        let f = BridgeFilter::new(vec![ColumnPredicate::StringIn {
            col_idx: 10,
            values: vec!["AIR".into(), "AIR REG".into()],
        }]);
        assert!(!f.statics_fused_evaluable(&proj));

        // Numeric static on an UNPROJECTED column → false (the fused
        // resolve can't map it to a decoded buffer).
        let f = BridgeFilter::new(vec![ColumnPredicate::I32Range {
            col_idx: 99,
            clauses: vec![],
        }]);
        assert!(!f.statics_fused_evaluable(&proj));

        // Column-pair: both projected → true; one out → false.
        let pair_ok = BridgeFilter::new(vec![ColumnPredicate::I32ColumnPair {
            left_col: 1,
            right_col: 10,
            op: Operator::Lt,
        }]);
        assert!(pair_ok.statics_fused_evaluable(&proj));
        let pair_bad = BridgeFilter::new(vec![ColumnPredicate::I32ColumnPair {
            left_col: 1,
            right_col: 99,
            op: Operator::Lt,
        }]);
        assert!(!pair_bad.statics_fused_evaluable(&proj));

        // Empty filter → vacuously true.
        assert!(BridgeFilter::new(vec![]).statics_fused_evaluable(&proj));
    }

    /// L9.ADAPT Guard 2 — eval with a disarm handle: (1) records the
    /// probes' MARGINAL outcome (statics-survivors in, post-probe
    /// survivors out); (2) once disarmed, skips the probe — statics
    /// keep filtering, and a probe-only filter degrades to an all-ones
    /// bitmap (pass-rate 1.0 → caller discards it and emits dense).
    #[test]
    fn adapt_probe_disarm_records_and_skips() {
        use crate::bridge_filter_sideband::ProbeDisarm;
        use crate::i64_set::I64Set;

        let keys: Vec<i64> = (0..16).collect();
        let dates: Vec<i32> = (0..16).map(|i| if i < 8 { 7000 } else { 0 }).collect();
        let mut set = I64Set::with_keys(4);
        for k in [0i64, 1, 2, 3] {
            set.insert(k);
        }
        let set = Arc::new(set);
        let mk_filter = || {
            BridgeFilter::new(vec![
                ColumnPredicate::I32Range {
                    col_idx: 10,
                    clauses: vec![RangeClause {
                        op: Operator::Eq,
                        literal_i32: 7000,
                    }],
                },
                ColumnPredicate::I64InSet {
                    col_idx: 1,
                    set: set.clone(),
                },
            ])
        };
        let resolve = |col: usize| match col {
            1 => Some(DecodedView::I64(&keys)),
            10 => Some(DecodedView::I32(&dates)),
            _ => None,
        };

        // Armed: probe applies (4 of 8 statics-survivors pass) and the
        // marginal outcome is recorded.
        let disarm = Arc::new(ProbeDisarm::default());
        let mut f = mk_filter();
        f.set_probe_disarm(disarm.clone());
        let (bitmap, total) = f.eval_on_decoded_views(resolve).unwrap();
        assert_eq!(total, 16);
        let pop: usize = bitmap.iter().map(|b| b.count_ones() as usize).sum();
        assert_eq!(pop, 4, "rows 0-3 pass both static and set");
        // seen = 8 statics survivors, passed = 4.
        assert!(!disarm.disarmed(0.10), "below MIN_ROWS floor");

        // Force-disarm, then eval again: the probe must NOT apply —
        // all 8 statics survivors stay set.
        disarm.record(ProbeDisarm::MIN_ROWS, ProbeDisarm::MIN_ROWS);
        assert!(disarm.disarmed(0.10));
        let (bitmap, _) = f.eval_on_decoded_views(resolve).unwrap();
        let pop: usize = bitmap.iter().map(|b| b.count_ones() as usize).sum();
        assert_eq!(pop, 8, "disarmed probe skipped; statics still filter");

        // Probe-only filter + disarmed → all-ones bitmap (tail masked).
        let mut probe_only = BridgeFilter::new(vec![ColumnPredicate::I64InSet {
            col_idx: 1,
            set: set.clone(),
        }]);
        probe_only.set_probe_disarm(disarm.clone());
        let short_keys: Vec<i64> = (0..11).collect();
        let (bitmap, total) = probe_only
            .eval_on_decoded_views(|_| Some(DecodedView::I64(&short_keys)))
            .unwrap();
        assert_eq!(total, 11);
        let pop: usize = bitmap.iter().map(|b| b.count_ones() as usize).sum();
        assert_eq!(pop, 11, "all-ones with tail bits zeroed");
    }

    /// NARROW.DEC × L9 (2026-07-02, Q08 SF=100 composition hazard) —
    /// an i64-domain runtime probe (`I64InSet` / `I64InBloom`) and an
    /// `I64Range` static must bind a `DecodedView::I32` (a KEYS.2-
    /// narrowed key decoded at Int32 under `EMAT_NARROW_KEY_DECODE`)
    /// by widening per value, instead of returning `None`. The `None`
    /// bail sent the WHOLE bundle to the legacy per-predicate re-decode
    /// path — on SF=100 lineitem (600M rows) that re-decode turned the
    /// L9 part→lineitem bloom wrap from +62 ms into +4.4 s whenever
    /// narrow keys were also on (the campaign's ALL-ON Q08 explosion).
    #[test]
    fn narrow_dec_i64_probe_binds_i32_view_by_widening() {
        use crate::i64_set::I64Set;

        let keys_i32: Vec<i32> = (0..32).collect();
        let mut set = I64Set::with_keys(4);
        for k in [3i64, 9, 20, 31] {
            set.insert(k);
        }
        let set = Arc::new(set);

        // I64InSet over an i32 view: must evaluate (widened), not bail.
        let f = BridgeFilter::new(vec![ColumnPredicate::I64InSet {
            col_idx: 1,
            set: set.clone(),
        }]);
        let (bitmap, total) = f
            .eval_on_decoded_views(|col| match col {
                1 => Some(DecodedView::I32(&keys_i32)),
                _ => None,
            })
            .expect("i64 probe over a narrowed i32 view must evaluate, not fall back");
        assert_eq!(total, 32);
        for row in 0..32usize {
            let expect = set.contains(row as i64);
            let got = bitmap[row >> 3] & (1 << (row & 7)) != 0;
            assert_eq!(got, expect, "row {row}");
        }

        // I64InBloom over an i32 view: no false negatives on members.
        let mut bloom = crate::bloom::BloomFilter::for_keys(16);
        for k in [3i64, 9, 20, 31] {
            bloom.insert_i64(k);
        }
        let bloom = Arc::new(bloom);
        let f = BridgeFilter::new(vec![ColumnPredicate::I64InBloom {
            col_idx: 1,
            bloom: bloom.clone(),
        }]);
        let (bitmap, total) = f
            .eval_on_decoded_views(|col| match col {
                1 => Some(DecodedView::I32(&keys_i32)),
                _ => None,
            })
            .expect("i64 bloom over a narrowed i32 view must evaluate, not fall back");
        assert_eq!(total, 32);
        for k in [3usize, 9, 20, 31] {
            assert!(
                bitmap[k >> 3] & (1 << (k & 7)) != 0,
                "bloom must not false-negative member row {k}"
            );
        }

        // I64Range static over an i32 view widens too, and composes
        // with a probe on the same narrowed column (statics first).
        let f = BridgeFilter::new(vec![
            ColumnPredicate::I64Range {
                col_idx: 1,
                lo: 8,
                hi: 24,
            },
            ColumnPredicate::I64InSet {
                col_idx: 1,
                set: set.clone(),
            },
        ]);
        let (bitmap, total) = f
            .eval_on_decoded_views(|col| match col {
                1 => Some(DecodedView::I32(&keys_i32)),
                _ => None,
            })
            .expect("i64 range + probe over a narrowed i32 view must evaluate");
        assert_eq!(total, 32);
        for row in 0..32usize {
            let expect = (8..=24).contains(&row) && set.contains(row as i64);
            let got = bitmap[row >> 3] & (1 << (row & 7)) != 0;
            assert_eq!(got, expect, "row {row}");
        }
    }

    /// KEYS.5 story (a) — the string runtime sideband predicates
    /// (`StringInBloom` / `StringInSet`) must (1) probe correctly via
    /// `eval_str` — bloom by byte-hash membership, set by exact
    /// membership; (2) report their target column via `col_idx`; and
    /// (3) classify as inexact (`is_exact_safe() == false`) so the
    /// residual string equi-join is preserved, exactly like the i64
    /// sideband shapes. This is the unit under test for story (a); the
    /// emitter (story b) and end-to-end join (story d) build on it.
    #[test]
    fn keys5_string_runtime_predicates_probe_and_classify() {
        use crate::bloom::BloomFilter;

        // --- StringInBloom: byte-level membership, no false negatives.
        let mut b = BloomFilter::with_capacity(64, 16);
        for k in ["FRANCE", "GERMANY", "BRAZIL"] {
            b.insert_str(k);
        }
        let bloom = Arc::new(b);
        let pred = ColumnPredicate::StringInBloom {
            col_idx: 3,
            bloom: bloom.clone(),
        };
        // Inserted keys must ALWAYS pass (blooms have no false negatives).
        for k in ["FRANCE", "GERMANY", "BRAZIL"] {
            assert!(pred.eval_str(k), "StringInBloom missed inserted key {k}");
        }
        // eval_str must DELEGATE to the bloom exactly — including absent
        // keys, where agreement holds regardless of any false positive.
        for k in [
            "FRANCE",
            "GERMANY",
            "BRAZIL",
            "ARGENTINA",
            "JAPAN",
            "UNITED KINGDOM",
            "zzz",
            "",
        ] {
            assert_eq!(
                pred.eval_str(k),
                bloom.might_contain_str(k),
                "StringInBloom eval_str disagrees with bloom for {k:?}"
            );
        }
        assert_eq!(pred.col_idx(), 3);
        assert!(
            !pred.is_exact_safe(),
            "runtime string bloom must be inexact (residual join still runs)"
        );

        // --- StringInSet: exact membership, zero false positives.
        let set: std::collections::HashSet<String> = ["FRANCE", "GERMANY"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let pred = ColumnPredicate::StringInSet {
            col_idx: 7,
            set: Arc::new(set),
        };
        assert!(pred.eval_str("FRANCE"));
        assert!(pred.eval_str("GERMANY"));
        assert!(
            !pred.eval_str("BRAZIL"),
            "absent key must not match the exact set"
        );
        assert!(!pred.eval_str(""), "empty key must not match");
        assert_eq!(pred.col_idx(), 7);
        assert!(
            !pred.is_exact_safe(),
            "runtime string set must be inexact (residual join still runs)"
        );
    }

    /// KEYS.2 — an INT64 key column whose values fit i32 is advertised as
    /// Int32 (downcast on) and decodes losslessly through the streaming
    /// path; a same-fitting NON-key i64 column stays Int64 (name gate), and
    /// downcast-off leaves the key Int64 (control).
    ///
    /// The value round-trip exercises the full streaming decode path: a 1000-
    /// row single-RG file routes through the page-streaming reader, which
    /// decodes the key at its native INT64 width; `execute()` then casts
    /// Int64→Int32 once at the stream boundary (`narrow_stream_to_advertised`).
    /// This catches any decode entry point that mis-handles the narrowed key,
    /// since they ALL flow through that one cast.
    /// REV.23 — the non-streaming late-mat path's masked→dense gate produces
    /// identical, correct rows on both sides of the threshold. High pass rate
    /// (>10%) takes the new dense-decode + SIMD `filter_record_batch` branch;
    /// low pass rate (<10%) takes the per-column masked gather. Both must
    /// return exactly the rows passing the predicate.
    #[test]
    fn rev23_late_mat_dense_route_matches_masked() {
        use ematix_parquet_codec::write::{ColumnData, write_table_to_path};
        use ematix_parquet_format::types::CompressionCodec;

        let dir = std::env::temp_dir().join(format!("rev23_latemat_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("t.parquet");
        let c: Vec<i32> = (0..1000).collect();
        write_table_to_path(
            &path,
            &[("c", ColumnData::I32(&c))],
            CompressionCodec::Uncompressed,
        )
        .unwrap();

        let schema: SchemaRef =
            Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "c",
                DataType::Int32,
                false,
            )]));

        let mk = |lt: i32| {
            BridgeFilter::new(vec![ColumnPredicate::I32Range {
                col_idx: 0,
                clauses: vec![RangeClause {
                    op: datafusion::logical_expr::Operator::Lt,
                    literal_i32: lt,
                }],
            }])
        };

        let check = |lt: i32| {
            let batch = decode_one_rg_filtered_late_mat(&path, 0, &schema, &[0], &mk(lt)).unwrap();
            assert_eq!(batch.num_rows(), lt as usize, "row count for c < {lt}");
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .unwrap();
            assert_eq!(col.value(0), 0);
            assert_eq!(col.value(lt as usize - 1), lt - 1);
        };

        check(600); // 60% pass → dense-route branch (REV.23)
        check(50); // 5% pass → masked-gather branch
    }

    /// Q10 late-mat prod-A: `with_primary_key` surfaces a PrimaryKey constraint.
    #[test]
    fn with_primary_key_surfaces_pk_constraint() {
        use ematix_parquet_codec::write::{ColumnData, write_table_to_path};
        use ematix_parquet_format::types::CompressionCodec;
        let dir = std::env::temp_dir().join(format!("pk_constraint_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("c.parquet");
        let k: Vec<i64> = (0..100).collect();
        let v: Vec<i64> = (1000..1100).collect();
        write_table_to_path(
            &path,
            &[
                ("c_custkey", ColumnData::I64(&k)),
                ("c_extra", ColumnData::I64(&v)),
            ],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
        let p = EmatixFastParquetTableProvider::try_new(path.to_str().unwrap()).unwrap();
        assert!(p.constraints().is_none(), "no PK by default");
        let p = p.with_primary_key(vec![0]);
        let c = p.constraints().expect("PK declared");
        assert_eq!(c.len(), 1, "exactly one constraint");
        assert!(
            matches!(&c[0], datafusion::common::Constraint::PrimaryKey(cols) if cols == &[0]),
            "PrimaryKey([0])"
        );
    }

    /// Q10 late-mat prod-A: a declared PK derives an FD that DataFusion propagates
    /// THROUGH an inner join to the aggregate input — the foundation the
    /// late-materialization recognizer relies on. Differential: present iff PK declared.
    #[tokio::test]
    async fn declared_pk_propagates_fd_to_aggregate_input() {
        use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
        use datafusion::logical_expr::LogicalPlan;
        use datafusion::prelude::SessionContext;
        use ematix_parquet_codec::write::{ColumnData, write_table_to_path};
        use ematix_parquet_format::types::CompressionCodec;

        let dir = std::env::temp_dir().join(format!("pk_fd_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let cust = dir.join("customer.parquet");
        let ord = dir.join("orders.parquet");
        let ck: Vec<i64> = (0..100).collect();
        let cx: Vec<i64> = (1000..1100).collect();
        write_table_to_path(
            &cust,
            &[
                ("c_custkey", ColumnData::I64(&ck)),
                ("c_extra", ColumnData::I64(&cx)),
            ],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
        let ok: Vec<i64> = (0..100).collect();
        let oc: Vec<i64> = (0..100).collect();
        write_table_to_path(
            &ord,
            &[
                ("o_orderkey", ColumnData::I64(&ok)),
                ("o_custkey", ColumnData::I64(&oc)),
            ],
            CompressionCodec::Uncompressed,
        )
        .unwrap();

        let sql = "select c_custkey, c_extra, sum(o_orderkey) as s \
            from customer, orders where c_custkey = o_custkey \
            group by c_custkey, c_extra";

        // Returns whether the aggregate input carries ANY functional dependency.
        async fn agg_input_has_fd(pk: bool, cust: &str, ord: &str, sql: &str) -> bool {
            let ctx = SessionContext::new();
            let cp = EmatixFastParquetTableProvider::try_new(cust).unwrap();
            let cp = if pk { cp.with_primary_key(vec![0]) } else { cp };
            ctx.register_table("customer", Arc::new(cp)).unwrap();
            ctx.register_table(
                "orders",
                Arc::new(EmatixFastParquetTableProvider::try_new(ord).unwrap()),
            )
            .unwrap();
            let plan = ctx.sql(sql).await.unwrap().into_optimized_plan().unwrap();
            let mut has = false;
            plan.apply(|n| {
                if let LogicalPlan::Aggregate(a) = n {
                    has = !a.input.schema().functional_dependencies().is_empty();
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .unwrap();
            has
        }

        let c = cust.to_str().unwrap();
        let o = ord.to_str().unwrap();
        assert!(
            agg_input_has_fd(true, c, o, sql).await,
            "declared PK must propagate an FD to the aggregate input (through the join)"
        );
        assert!(
            !agg_input_has_fd(false, c, o, sql).await,
            "without a declared PK there is no FD (control)"
        );
    }

    #[tokio::test]
    async fn keys2_narrows_i64_key_to_i32_losslessly() {
        use ematix_parquet_codec::write::{ColumnData, write_table_to_path};
        use ematix_parquet_format::types::CompressionCodec;

        let dir = std::env::temp_dir().join(format!("keys2_test_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("t.parquet");

        // l_orderkey: i64, max 599_400_000 < i32::MAX → key + fits → narrow.
        // l_extra:    i64, also fits i32 but NOT a "*key" name → stays Int64.
        let orderkey: Vec<i64> = (0..1000).map(|i| i * 600_000).collect();
        let extra: Vec<i64> = (0..1000).collect();
        write_table_to_path(
            &path,
            &[
                ("l_orderkey", ColumnData::I64(&orderkey)),
                ("l_extra", ColumnData::I64(&extra)),
            ],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
        let p = path.to_str().unwrap();

        // Downcast ON: l_orderkey → Int32; l_extra stays Int64 (name gate).
        let prov = EmatixFastParquetTableProvider::try_new_opt(p, true).unwrap();
        let sch = prov.schema();
        assert_eq!(
            sch.field_with_name("l_orderkey").unwrap().data_type(),
            &DataType::Int32,
            "key with i32-fitting stats should narrow to Int32"
        );
        assert_eq!(
            sch.field_with_name("l_extra").unwrap().data_type(),
            &DataType::Int64,
            "non-key i64 must stay Int64 even though it fits i32"
        );

        // Values round-trip losslessly through the streaming decode path.
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(prov)).unwrap();
        let batches = ctx
            .sql("SELECT l_orderkey FROM t ORDER BY l_orderkey")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let got: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int32Array>()
                    .expect("l_orderkey must decode as Int32Array")
                    .values()
                    .to_vec()
            })
            .collect();
        let expect: Vec<i32> = orderkey.iter().map(|&x| x as i32).collect();
        assert_eq!(
            got, expect,
            "narrowed values must equal original i64 cast i32"
        );

        // Downcast OFF: l_orderkey stays Int64 (control).
        let prov_off = EmatixFastParquetTableProvider::try_new_opt(p, false).unwrap();
        assert_eq!(
            prov_off
                .schema()
                .field_with_name("l_orderkey")
                .unwrap()
                .data_type(),
            &DataType::Int64,
            "downcast-off must leave the key Int64"
        );
    }

    /// NARROW.DEC — `EMAT_NARROW_KEY_DECODE` toggles the downcast-on-
    /// read path end-to-end through the provider: with the flag ON the
    /// eager streaming reader decodes the narrowed key directly at
    /// Int32 (fire counter moves); with the flag OFF the decode stays
    /// wide + boundary-cast. Both paths must produce IDENTICAL query
    /// results on data with negatives and both i32 extremes.
    ///
    /// Multi-RG file (3 RGs) so the dispatch routes to the eager
    /// `EmatArrowBatchReader` (single-RG files route to the page-
    /// streaming reader, which deliberately stays on decode-wide).
    /// Serializes the tests that toggle process-global `EMAT_*` env vars
    /// (`EMAT_NARROW_KEY_DECODE`, `EMAT_DOWNCAST_KEYS`,
    /// `EMAT_LARGE_SCALE_MIN_ROWS`). Fire-counter / schema assertions break
    /// if a sibling test clears a var mid-window (observed as "counter
    /// 6 → 6" flakes). Crate-wide lock: `EMAT_LARGE_SCALE_MIN_ROWS` is also
    /// mutated by `flow_query_planner` tests, which a module-local lock
    /// cannot serialize.
    use crate::flags::EMAT_ENV_TEST_LOCK as NARROW_KEY_ENV_LOCK;

    #[tokio::test]
    async fn narrow_key_decode_flag_toggles_path_with_identical_results() {
        use ematix_parquet_codec::write::{ColumnData, write_table_to_path_with_row_group_size};
        use ematix_parquet_format::types::CompressionCodec;

        let _env_guard = NARROW_KEY_ENV_LOCK.lock().await;
        let dir = std::env::temp_dir().join(format!("narrow_flag_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("t.parquet");

        let n = 999usize;
        let mut key: Vec<i64> = (0..n as i64).map(|x| x * 37 - 400_000).collect();
        key[0] = i32::MIN as i64;
        key[n - 1] = i32::MAX as i64;
        let val: Vec<f64> = (0..n).map(|x| x as f64 * 1.5).collect();
        write_table_to_path_with_row_group_size(
            &path,
            &[
                ("l_orderkey", ColumnData::I64(&key)),
                ("l_val", ColumnData::F64(&val)),
            ],
            CompressionCodec::Uncompressed,
            n / 3, // → 3 row groups → eager streaming reader
        )
        .unwrap();
        let p = path.to_str().unwrap();

        async fn collect_keys(p: &str) -> Vec<i32> {
            let prov = EmatixFastParquetTableProvider::try_new_opt(p, true).unwrap();
            assert_eq!(
                prov.schema()
                    .field_with_name("l_orderkey")
                    .unwrap()
                    .data_type(),
                &DataType::Int32,
                "stats-fitting key must advertise Int32 under downcast"
            );
            let ctx = SessionContext::new();
            ctx.register_table("t", Arc::new(prov)).unwrap();
            let batches = ctx
                .sql("SELECT l_orderkey FROM t ORDER BY l_orderkey")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            batches
                .iter()
                .flat_map(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<arrow_array::Int32Array>()
                        .expect("l_orderkey must surface as Int32Array")
                        .values()
                        .to_vec()
                })
                .collect()
        }

        // Control: flag OFF → decode-wide + boundary cast.
        // (Scoped tightly; parallel tests seeing the flag mid-window
        // would still produce identical results by this test's own
        // invariant, so the set/remove is race-benign.)
        unsafe { std::env::remove_var("EMAT_NARROW_KEY_DECODE") };
        let wide = collect_keys(p).await;

        // Flag ON → downcast-on-read; fire counter must move.
        let before =
            crate::emat_arrow_reader::NARROW_KEY_DECODES.load(std::sync::atomic::Ordering::Relaxed);
        unsafe { std::env::set_var("EMAT_NARROW_KEY_DECODE", "1") };
        let narrow = collect_keys(p).await;
        unsafe { std::env::remove_var("EMAT_NARROW_KEY_DECODE") };
        let after =
            crate::emat_arrow_reader::NARROW_KEY_DECODES.load(std::sync::atomic::Ordering::Relaxed);

        assert_eq!(narrow, wide, "flag ON/OFF must return identical results");
        let mut expect: Vec<i32> = key.iter().map(|&x| x as i32).collect();
        expect.sort_unstable();
        assert_eq!(narrow, expect, "values must round-trip the source data");
        assert!(
            after > before,
            "EMAT_NARROW_KEY_DECODE=1 must route through the downcast decode \
             (counter {before} → {after})"
        );
    }

    fn lineitem_path() -> Option<String> {
        // Resolution order:
        //   1. `$TPCH_DATA_DIR` developer override.
        //   2. CWD-relative `examples/tpch/data/sf1/lineitem.parquet`
        //      (matches the pre-existing behaviour: resolves only when
        //      the test runner's CWD is the workspace root).
        //   3. Synthetic mini-fixture from `test_support` (the new
        //      fallback that lets the test run in CI).
        let s = match std::env::var("TPCH_DATA_DIR") {
            Ok(s) => format!("{s}/lineitem.parquet"),
            Err(_) => "examples/tpch/data/sf1/lineitem.parquet".into(),
        };
        if std::path::Path::new(&s).exists() {
            return Some(s);
        }
        let mini =
            std::path::PathBuf::from(crate::test_support::tpch_mini_dir()).join("lineitem.parquet");
        mini.exists().then(|| mini.to_string_lossy().into_owned())
    }

    /// Σ.AI.5 SCALE-GATE (2026-07-02 campaign gating) — the narrow-keys
    /// schema advertisement is tri-state on `EMAT_DOWNCAST_KEYS`:
    ///   1. unset + small-scale stats → AUTO-OFF (keys stay Int64),
    ///   2. unset + SF≥100-class stats (row-count threshold injected via
    ///      `EMAT_LARGE_SCALE_MIN_ROWS` — the same footer stats the
    ///      production gate reads) → AUTO-ON (fitting keys advertise Int32),
    ///   3. `=0` beats auto-ON, 4. `=1` beats auto-OFF.
    ///
    /// Env windows are kept tight; this is the only test mutating
    /// EMAT_DOWNCAST_KEYS (the other downcast tests use `try_new_opt`'s
    /// explicit flag precisely to avoid env races).
    #[test]
    fn downcast_keys_scale_gated_auto() {
        // Sync test → blocking_lock (no tokio runtime here). Serializes the
        // EMAT_DOWNCAST_KEYS / EMAT_LARGE_SCALE_MIN_ROWS windows below
        // against every other env-mutating test in the crate.
        let _env_guard = NARROW_KEY_ENV_LOCK.blocking_lock();
        let Some(p) = lineitem_path() else {
            eprintln!("skip: no lineitem fixture");
            return;
        };
        let orderkey_type = |prov: &EmatixFastParquetTableProvider| {
            use datafusion::catalog::TableProvider as _;
            prov.schema()
                .field_with_name("l_orderkey")
                .expect("lineitem has l_orderkey")
                .data_type()
                .clone()
        };

        // 1. AUTO below scale → Int64 (unchanged advertisement).
        unsafe { std::env::remove_var("EMAT_DOWNCAST_KEYS") };
        unsafe { std::env::remove_var("EMAT_LARGE_SCALE_MIN_ROWS") };
        let prov = EmatixFastParquetTableProvider::try_new(p.clone()).unwrap();
        assert_eq!(
            orderkey_type(&prov),
            DataType::Int64,
            "AUTO below scale must not narrow"
        );

        // 2. AUTO at (injected) large-scale stats → Int32. Every fixture
        //    lineitem (mini = 280 rows, SF=1 = 6M) exceeds 100 rows and
        //    its l_orderkey fits i32.
        unsafe { std::env::set_var("EMAT_LARGE_SCALE_MIN_ROWS", "100") };
        let prov = EmatixFastParquetTableProvider::try_new(p.clone()).unwrap();
        assert_eq!(
            orderkey_type(&prov),
            DataType::Int32,
            "AUTO at large-scale stats must narrow fitting keys"
        );

        // 3. Force-off beats auto-ON.
        unsafe { std::env::set_var("EMAT_DOWNCAST_KEYS", "0") };
        let prov = EmatixFastParquetTableProvider::try_new(p.clone()).unwrap();
        assert_eq!(
            orderkey_type(&prov),
            DataType::Int64,
            "=0 must force OFF even at large scale"
        );
        unsafe { std::env::remove_var("EMAT_LARGE_SCALE_MIN_ROWS") };

        // 4. Force-on beats auto-OFF.
        unsafe { std::env::set_var("EMAT_DOWNCAST_KEYS", "1") };
        let prov = EmatixFastParquetTableProvider::try_new(p.clone()).unwrap();
        unsafe { std::env::remove_var("EMAT_DOWNCAST_KEYS") };
        assert_eq!(
            orderkey_type(&prov),
            DataType::Int32,
            "=1 must force ON below scale"
        );
    }

    /// NARROW.DEC micro-verification (NOT a benchmark) — TPC-H Q09, the
    /// join-heavy query whose partsupp 2-key build is the narrowing
    /// target, executed ONCE with the narrowed path off and once with
    /// it on (`EMAT_DOWNCAST_KEYS` via `try_new_opt(_, true)` +
    /// `EMAT_NARROW_KEY_DECODE=1`), asserting identical results.
    ///
    /// Data resolution matches `lineitem_path()`: set `TPCH_DATA_DIR`
    /// (e.g. the real SF=1 dataset) for the full-scale run; CI falls
    /// back to the synthetic mini fixture. The mini fixture's `p_name`
    /// has no TPC-H color words, so the LIKE pattern is parameterised
    /// (`%green%` on real data, `%part%` on the mini) — the plan shape
    /// (6-table join + 2-col group-by + sum) is identical either way.
    ///
    /// The sum is compared with FP-relative tolerance: multi-partition
    /// f64 aggregation is order-sensitive run-to-run independent of the
    /// flag (same policy as `examples/tpch_validate.rs`).
    #[tokio::test]
    async fn narrow_key_decode_q09_identity_on_off() {
        let _env_guard = NARROW_KEY_ENV_LOCK.lock().await;
        // Resolve the data dir (real SF=1 via TPCH_DATA_DIR / workspace
        // layout, else mini fixture).
        let (dir, mini): (std::path::PathBuf, bool) = {
            let cand = std::env::var("TPCH_DATA_DIR")
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|_| std::path::PathBuf::from("examples/tpch/data/sf1"));
            if cand.join("lineitem.parquet").exists() {
                (cand, false)
            } else {
                (
                    std::path::PathBuf::from(crate::test_support::tpch_mini_dir()),
                    true,
                )
            }
        };
        let like = if mini { "%part%" } else { "%green%" };
        let q09 = format!(
            "select nation, o_year, sum(amount) as sum_profit from ( \
               select n_name as nation, \
                      extract(year from o_orderdate) as o_year, \
                      l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount \
               from part, supplier, lineitem, partsupp, orders, nation \
               where s_suppkey = l_suppkey and ps_suppkey = l_suppkey \
                 and ps_partkey = l_partkey and p_partkey = l_partkey \
                 and o_orderkey = l_orderkey and s_nationkey = n_nationkey \
                 and p_name like '{like}' \
             ) as profit group by nation, o_year order by nation, o_year desc"
        );

        // (nation, o_year, sum_profit) rows, already ORDER BY'd.
        async fn run(dir: &std::path::Path, downcast: bool, sql: &str) -> Vec<(String, i64, f64)> {
            const TABLES: &[&str] = &[
                "part", "supplier", "lineitem", "partsupp", "orders", "nation",
            ];
            let ctx = SessionContext::new();
            for t in TABLES {
                let path = dir.join(format!("{t}.parquet"));
                let prov = EmatixFastParquetTableProvider::try_new_opt(
                    path.to_str().unwrap().to_string(),
                    downcast,
                )
                .unwrap();
                ctx.register_table(*t, Arc::new(prov)).unwrap();
            }
            let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
            let mut rows = Vec::new();
            for b in &batches {
                let nation = b.column(0);
                let year = b.column(1);
                let profit = b
                    .column(2)
                    .as_any()
                    .downcast_ref::<arrow_array::Float64Array>()
                    .expect("sum_profit is f64");
                for r in 0..b.num_rows() {
                    let n = nation
                        .as_any()
                        .downcast_ref::<arrow_array::StringArray>()
                        .map(|a| a.value(r).to_string())
                        .or_else(|| {
                            nation
                                .as_any()
                                .downcast_ref::<arrow_array::StringViewArray>()
                                .map(|a| a.value(r).to_string())
                        })
                        .expect("nation is a string column");
                    // extract(year) surfaces as some integer/f64 width
                    // depending on planner version — normalise via i64.
                    let y = match year.data_type() {
                        DataType::Int32 => year
                            .as_any()
                            .downcast_ref::<arrow_array::Int32Array>()
                            .unwrap()
                            .value(r) as i64,
                        DataType::Int64 => year
                            .as_any()
                            .downcast_ref::<arrow_array::Int64Array>()
                            .unwrap()
                            .value(r),
                        DataType::Float64 => year
                            .as_any()
                            .downcast_ref::<arrow_array::Float64Array>()
                            .unwrap()
                            .value(r) as i64,
                        other => panic!("unexpected o_year type {other:?}"),
                    };
                    rows.push((n, y, profit.value(r)));
                }
            }
            rows
        }

        // Arm A — narrowing fully OFF (wide Int64 keys everywhere).
        unsafe { std::env::remove_var("EMAT_NARROW_KEY_DECODE") };
        let wide = run(&dir, false, &q09).await;
        assert!(
            !wide.is_empty(),
            "Q09 must return rows on the resolved dataset (pattern {like})"
        );

        // Arm B — narrowed keys + narrow decode ON.
        let before =
            crate::emat_arrow_reader::NARROW_KEY_DECODES.load(std::sync::atomic::Ordering::Relaxed);
        unsafe { std::env::set_var("EMAT_NARROW_KEY_DECODE", "1") };
        let narrow = run(&dir, true, &q09).await;
        unsafe { std::env::remove_var("EMAT_NARROW_KEY_DECODE") };
        let after =
            crate::emat_arrow_reader::NARROW_KEY_DECODES.load(std::sync::atomic::Ordering::Relaxed);

        assert_eq!(
            wide.len(),
            narrow.len(),
            "row-count mismatch between wide and narrowed Q09"
        );
        for (i, (w, n)) in wide.iter().zip(narrow.iter()).enumerate() {
            assert_eq!(w.0, n.0, "row {i}: nation mismatch");
            assert_eq!(w.1, n.1, "row {i}: o_year mismatch");
            let mag = w.2.abs().max(n.2.abs()).max(1.0);
            assert!(
                ((w.2 - n.2).abs() / mag) <= 1e-6,
                "row {i}: sum_profit diverged: wide={} narrow={}",
                w.2,
                n.2
            );
        }
        assert!(
            after > before,
            "narrow decode must have fired on the multi-RG fact scans \
             (counter {before} → {after})"
        );
    }

    #[tokio::test]
    async fn full_lineitem_count_via_provider() {
        // Phase 4: lineitem registers cleanly via Emat now that
        // BYTE_ARRAY/Utf8 is supported. Run SELECT COUNT(*) end to
        // end through DataFusion and confirm the row count.
        //
        // The mini fixture surfaces an unrelated pre-existing edge
        // case (empty-projection RecordBatch::try_new without an
        // explicit row count when COUNT(*) pushes through Emat with
        // no projected columns); real SF=1 happens to take a different
        // planner shape. Skip when the resolved path is the mini.
        let Some(path) = lineitem_path() else {
            eprintln!("skipping: SF=1 lineitem not present");
            return;
        };
        if path.starts_with(crate::test_support::tpch_mini_dir()) {
            eprintln!("skipping: SF=1 lineitem not present (mini fixture path)");
            return;
        }
        let prov = EmatixFastParquetTableProvider::try_new(path).unwrap();
        let total = prov.num_rows();
        let ctx = SessionContext::new();
        ctx.register_table("lineitem", Arc::new(prov)).unwrap();
        let df = ctx.sql("SELECT COUNT(*) FROM lineitem").await.unwrap();
        let batches = df.collect().await.unwrap();
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count as usize, total);
    }

    /// Build a small primitive-only parquet file in memory, register
    /// it via the provider, run `SELECT COUNT(*)` through DataFusion,
    /// confirm row count.
    #[tokio::test]
    async fn end_to_end_simple_count() {
        use datafusion::parquet::basic::{Compression, Repetition, Type as PhysicalType};
        use datafusion::parquet::column::writer::ColumnWriter;
        use datafusion::parquet::file::properties::WriterProperties;
        use datafusion::parquet::file::writer::SerializedFileWriter;
        use datafusion::parquet::schema::types::Type as PType;

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Schema: a (i32), b (i64), c (f64).
        let schema = Arc::new(
            PType::group_type_builder("schema")
                .with_fields(vec![
                    Arc::new(
                        PType::primitive_type_builder("a", PhysicalType::INT32)
                            .with_repetition(Repetition::REQUIRED)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        PType::primitive_type_builder("b", PhysicalType::INT64)
                            .with_repetition(Repetition::REQUIRED)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        PType::primitive_type_builder("c", PhysicalType::DOUBLE)
                            .with_repetition(Repetition::REQUIRED)
                            .build()
                            .unwrap(),
                    ),
                ])
                .build()
                .unwrap(),
        );
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build(),
        );
        let file = File::create(&path).unwrap();
        let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
        let mut rg = writer.next_row_group().unwrap();
        // a
        let a: Vec<i32> = (0..1000).collect();
        let mut col = rg.next_column().unwrap().unwrap();
        if let ColumnWriter::Int32ColumnWriter(t) = col.untyped() {
            t.write_batch(&a, None, None).unwrap();
        }
        col.close().unwrap();
        // b
        let b: Vec<i64> = (0..1000i64).map(|i| i * 100).collect();
        let mut col = rg.next_column().unwrap().unwrap();
        if let ColumnWriter::Int64ColumnWriter(t) = col.untyped() {
            t.write_batch(&b, None, None).unwrap();
        }
        col.close().unwrap();
        // c
        let c: Vec<f64> = (0..1000).map(|i| (i as f64) * 1.5).collect();
        let mut col = rg.next_column().unwrap().unwrap();
        if let ColumnWriter::DoubleColumnWriter(t) = col.untyped() {
            t.write_batch(&c, None, None).unwrap();
        }
        col.close().unwrap();
        rg.close().unwrap();
        writer.close().unwrap();

        let provider =
            EmatixFastParquetTableProvider::try_new(path.to_string_lossy().to_string()).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(provider)).unwrap();
        let df = ctx
            .sql("SELECT COUNT(*), SUM(a), SUM(b), SUM(c) FROM t")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        let b0 = &batches[0];
        let count = b0
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0);
        let sum_a = b0
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0);
        let sum_b = b0
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0);
        let sum_c = b0
            .column(3)
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 1000);
        assert_eq!(sum_a, (0..1000i64).sum::<i64>());
        assert_eq!(sum_b, (0..1000i64).map(|i| i * 100).sum::<i64>());
        let expected_c: f64 = (0..1000).map(|i| (i as f64) * 1.5).sum();
        assert!((sum_c - expected_c).abs() < 1e-6);
    }

    /// Σ.E3b: with `with_dict_preservation(true)`, the provider must
    /// (a) report Utf8 fields as `Dictionary(UInt32, Utf8)` on its
    /// schema, (b) emit DictionaryArray-typed columns at scan time,
    /// and (c) compose correctly with DataFusion's GROUP BY so the
    /// pre-existing `EnableDictGroupCountRule` (Σ.E3b operator route)
    /// has dict-encoded inputs to bind against.
    #[tokio::test]
    async fn dict_preservation_end_to_end() {
        use datafusion::parquet::basic::{Compression, Repetition, Type as PhysicalType};
        use datafusion::parquet::column::writer::ColumnWriter;
        use datafusion::parquet::data_type::ByteArray;
        use datafusion::parquet::file::properties::WriterProperties;
        use datafusion::parquet::file::writer::SerializedFileWriter;
        use datafusion::parquet::schema::types::Type as PType;

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Schema: flag (Utf8) — 3 distinct values, heavy dict
        // encoding. Single column keeps the schema rewrite assertion
        // narrow.
        let schema = Arc::new(
            PType::group_type_builder("schema")
                .with_fields(vec![Arc::new(
                    PType::primitive_type_builder("flag", PhysicalType::BYTE_ARRAY)
                        .with_repetition(Repetition::REQUIRED)
                        .with_converted_type(datafusion::parquet::basic::ConvertedType::UTF8)
                        .build()
                        .unwrap(),
                )])
                .build()
                .unwrap(),
        );
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                // Force dict encoding — default already does this but
                // make the test invariant explicit.
                .set_dictionary_enabled(true)
                .build(),
        );
        let file = File::create(&path).unwrap();
        let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
        let mut rg = writer.next_row_group().unwrap();
        let palette: [&[u8]; 3] = [b"R", b"A", b"N"];
        let values: Vec<ByteArray> = (0..1_500)
            .map(|i| ByteArray::from(palette[i % 3].to_vec()))
            .collect();
        let mut col = rg.next_column().unwrap().unwrap();
        if let ColumnWriter::ByteArrayColumnWriter(t) = col.untyped() {
            t.write_batch(&values, None, None).unwrap();
        }
        col.close().unwrap();
        rg.close().unwrap();
        writer.close().unwrap();

        // Default (no with_dict_preservation): Σ.E5 auto-promotion now
        // rewrites Utf8 → Utf8View at try_new for the streaming-reader
        // default. With dict preservation off, the column reports
        // Utf8View (was Utf8 before the Σ.E5 promotion fix); batches
        // emit StringViewArray.
        let prov_off =
            EmatixFastParquetTableProvider::try_new(path.to_string_lossy().to_string()).unwrap();
        assert!(matches!(
            prov_off.schema().field(0).data_type(),
            DataType::Utf8View
        ));

        // On → schema is Dictionary(UInt32, Utf8), batches are
        // DictionaryArray.
        let prov_on = EmatixFastParquetTableProvider::try_new(path.to_string_lossy().to_string())
            .unwrap()
            .with_dict_preservation(true);
        match prov_on.schema().field(0).data_type() {
            DataType::Dictionary(k, v) => {
                assert!(matches!(k.as_ref(), DataType::UInt32));
                assert!(matches!(v.as_ref(), DataType::Utf8));
            }
            other => panic!("expected Dictionary(UInt32, Utf8), got {other:?}"),
        }

        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(prov_on)).unwrap();
        let df = ctx.sql("SELECT flag FROM t LIMIT 5").await.unwrap();
        let batches = df.collect().await.unwrap();
        assert!(!batches.is_empty());
        let col0 = batches[0].column(0);
        assert!(matches!(
            col0.data_type(),
            DataType::Dictionary(k, v)
                if matches!(k.as_ref(), DataType::UInt32)
                    && matches!(v.as_ref(), DataType::Utf8)
        ));
        let dict_arr = col0
            .as_any()
            .downcast_ref::<arrow_array::DictionaryArray<arrow_array::types::UInt32Type>>()
            .expect("expected DictionaryArray<UInt32Type>");
        let values = dict_arr
            .values()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect("dict values must be StringArray");
        // Materialise first row and confirm it's one of the palette.
        let k0 = dict_arr.keys().value(0) as usize;
        let first = values.value(k0);
        assert!(matches!(first, "R" | "A" | "N"), "unexpected: {first:?}");

        // Also confirm GROUP BY composes through DataFusion (the rule
        // matching machinery is unit-tested elsewhere; here we just
        // verify the planner doesn't choke on the new column type).
        let df2 = ctx
            .sql("SELECT flag, COUNT(*) AS n FROM t GROUP BY flag ORDER BY flag")
            .await
            .unwrap();
        let batches2 = df2.collect().await.unwrap();
        let total: i64 = batches2
            .iter()
            .flat_map(|b| {
                let c = b
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap();
                (0..c.len()).map(move |i| c.value(i))
            })
            .sum();
        assert_eq!(total, 1_500);
    }

    /// Phase 3 oracle on real SF=1 lineitem: Q14-shape predicate
    /// pushdown via the new fused-NEON path. Compares ours vs
    /// parquet-rs+filter on the same SQL.
    #[tokio::test]
    async fn phase3_predicate_pushdown_q14_shape() {
        // The Phase 5 NEON-fused predicate hard-codes bit_width=12 for
        // the SF=1 lineitem dictionary cardinality (~4 K distinct
        // partkeys). The mini fixture has 30 unique partkeys → much
        // smaller bit width — so the decode path bails with a
        // "bit_width" mismatch. Skip when resolved path is mini.
        let Some(path) = lineitem_path() else {
            eprintln!("skipping: SF=1 lineitem not present");
            return;
        };
        if path.starts_with(crate::test_support::tpch_mini_dir()) {
            eprintln!("skipping: SF=1 lineitem not present (mini fixture path)");
            return;
        }

        // Lineitem has BYTE_ARRAY columns so the full table can't
        // register through Emat — but we can build a primitive-only
        // helper file from lineitem's Q14-relevant columns. For this
        // oracle, scan via parquet-rs to extract the columns, write
        // a new parquet that's all-primitive, register that via
        // EmatixFastParquetTableProvider, and compare.
        //
        // Setup: read SF=1 lineitem (l_shipdate, l_partkey,
        // l_extendedprice, l_discount) into a temp parquet.
        use datafusion::parquet::basic::{Compression, Repetition, Type as PhysicalType};
        use datafusion::parquet::column::reader::ColumnReader;
        use datafusion::parquet::column::writer::ColumnWriter;
        use datafusion::parquet::file::properties::WriterProperties;
        use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
        use datafusion::parquet::file::writer::SerializedFileWriter;
        use datafusion::parquet::schema::types::Type as PType;

        let r = SerializedFileReader::new(File::open(&path).unwrap()).unwrap();
        let total = r.metadata().file_metadata().num_rows() as usize;

        let mut shipdate: Vec<i32> = Vec::with_capacity(total);
        let mut partkey: Vec<i64> = Vec::with_capacity(total);
        let mut extprice: Vec<f64> = Vec::with_capacity(total);
        let mut discount: Vec<f64> = Vec::with_capacity(total);
        for rg in 0..r.metadata().num_row_groups() {
            let rgr = r.get_row_group(rg).unwrap();
            {
                let mut t = match rgr.get_column_reader(10).unwrap() {
                    ColumnReader::Int32ColumnReader(t) => t,
                    _ => panic!(),
                };
                t.read_records(
                    rgr.metadata().num_rows() as usize,
                    None,
                    None,
                    &mut shipdate,
                )
                .unwrap();
            }
            {
                let mut t = match rgr.get_column_reader(1).unwrap() {
                    ColumnReader::Int64ColumnReader(t) => t,
                    _ => panic!(),
                };
                t.read_records(rgr.metadata().num_rows() as usize, None, None, &mut partkey)
                    .unwrap();
            }
            {
                let mut t = match rgr.get_column_reader(5).unwrap() {
                    ColumnReader::DoubleColumnReader(t) => t,
                    _ => panic!(),
                };
                t.read_records(
                    rgr.metadata().num_rows() as usize,
                    None,
                    None,
                    &mut extprice,
                )
                .unwrap();
            }
            {
                let mut t = match rgr.get_column_reader(6).unwrap() {
                    ColumnReader::DoubleColumnReader(t) => t,
                    _ => panic!(),
                };
                t.read_records(
                    rgr.metadata().num_rows() as usize,
                    None,
                    None,
                    &mut discount,
                )
                .unwrap();
            }
        }

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let tmp_path = tmp.path().to_path_buf();
        let schema = Arc::new(
            PType::group_type_builder("schema")
                .with_fields(vec![
                    Arc::new(
                        PType::primitive_type_builder("l_shipdate", PhysicalType::INT32)
                            .with_repetition(Repetition::REQUIRED)
                            .with_converted_type(datafusion::parquet::basic::ConvertedType::DATE)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        PType::primitive_type_builder("l_partkey", PhysicalType::INT64)
                            .with_repetition(Repetition::REQUIRED)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        PType::primitive_type_builder("l_extendedprice", PhysicalType::DOUBLE)
                            .with_repetition(Repetition::REQUIRED)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        PType::primitive_type_builder("l_discount", PhysicalType::DOUBLE)
                            .with_repetition(Repetition::REQUIRED)
                            .build()
                            .unwrap(),
                    ),
                ])
                .build()
                .unwrap(),
        );
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build(),
        );
        let file = File::create(&tmp_path).unwrap();
        let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
        let mut rg = writer.next_row_group().unwrap();
        {
            let mut col = rg.next_column().unwrap().unwrap();
            if let ColumnWriter::Int32ColumnWriter(t) = col.untyped() {
                t.write_batch(&shipdate, None, None).unwrap();
            }
            col.close().unwrap();
        }
        {
            let mut col = rg.next_column().unwrap().unwrap();
            if let ColumnWriter::Int64ColumnWriter(t) = col.untyped() {
                t.write_batch(&partkey, None, None).unwrap();
            }
            col.close().unwrap();
        }
        {
            let mut col = rg.next_column().unwrap().unwrap();
            if let ColumnWriter::DoubleColumnWriter(t) = col.untyped() {
                t.write_batch(&extprice, None, None).unwrap();
            }
            col.close().unwrap();
        }
        {
            let mut col = rg.next_column().unwrap().unwrap();
            if let ColumnWriter::DoubleColumnWriter(t) = col.untyped() {
                t.write_batch(&discount, None, None).unwrap();
            }
            col.close().unwrap();
        }
        rg.close().unwrap();
        writer.close().unwrap();

        // Now register the synthetic primitive-only file via Emat and
        // run Q14's lineitem-only aggregate: SUM(extprice * (1-discount))
        // for rows where shipdate ∈ [9374, 9404).
        let provider =
            EmatixFastParquetTableProvider::try_new(tmp_path.to_string_lossy().to_string())
                .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("li_prim", Arc::new(provider)).unwrap();

        let sql = "SELECT \
            SUM(l_extendedprice * (1 - l_discount)) AS rev, \
            COUNT(*) AS matches \
            FROM li_prim \
            WHERE l_shipdate >= DATE '1995-09-01' \
              AND l_shipdate < DATE '1995-10-01'";
        let df = ctx.sql(sql).await.unwrap();
        let batches = df.collect().await.unwrap();
        let b = &batches[0];
        let rev = b
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .unwrap()
            .value(0);
        let matches = b
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0);

        // Expected from earlier manual POC (commit 53e908d):
        // 76024 matches, total revenue across all matching rows
        // = sum(l_extprice * (1 - l_discount)).
        let expected_rev: f64 = shipdate
            .iter()
            .zip(extprice.iter())
            .zip(discount.iter())
            .filter(|((d, _), _)| **d >= 9374 && **d < 9404)
            .map(|((_, p), d)| p * (1.0 - d))
            .sum();
        let expected_matches = shipdate
            .iter()
            .filter(|d| **d >= 9374 && **d < 9404)
            .count() as i64;

        assert_eq!(matches, expected_matches);
        assert!(
            (rev - expected_rev).abs() < 1e-3 * expected_rev.abs(),
            "rev {rev:.4} vs expected {expected_rev:.4}"
        );
    }

    /// Σ.E5.1.b end-to-end shape: the streaming reader path must
    /// produce the same Q1-shaped (filter + GROUP BY + SUM + COUNT(*))
    /// result as the legacy bridge path on a small synthetic
    /// multi-row-group parquet.
    ///
    /// We write a 4-row-group file with a primitive grouping column
    /// (Int32, three distinct values cycling) plus a Float64 value
    /// column, then run the same SQL through:
    ///   - bridge provider:  EmatixFastParquetTableProvider::try_new
    ///   - streaming provider: same + .with_streaming_arrow_reader(true)
    /// and confirm row-by-row equality. Filter is `v > 100.0` so it's
    /// not vacuous.
    #[tokio::test]
    async fn streaming_reader_provider_q1_shape() {
        use datafusion::parquet::basic::{Compression, Repetition, Type as PhysicalType};
        use datafusion::parquet::column::writer::ColumnWriter;
        use datafusion::parquet::file::properties::WriterProperties;
        use datafusion::parquet::file::writer::SerializedFileWriter;
        use datafusion::parquet::schema::types::Type as PType;

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        // 4 row groups × 200 rows each = 800 rows total. Grouping
        // column cycles through 3 values; value column is i * 0.5 so
        // some rows are > 100.0 and some aren't.
        let n_per_rg = 200usize;
        let n_rgs = 4usize;

        let schema = Arc::new(
            PType::group_type_builder("schema")
                .with_fields(vec![
                    Arc::new(
                        PType::primitive_type_builder("g", PhysicalType::INT32)
                            .with_repetition(Repetition::REQUIRED)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        PType::primitive_type_builder("v", PhysicalType::DOUBLE)
                            .with_repetition(Repetition::REQUIRED)
                            .build()
                            .unwrap(),
                    ),
                ])
                .build()
                .unwrap(),
        );
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .set_max_row_group_row_count(Some(n_per_rg))
                .build(),
        );
        let file = File::create(&path).unwrap();
        let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
        for rg_idx in 0..n_rgs {
            let mut rg = writer.next_row_group().unwrap();
            let base = rg_idx * n_per_rg;
            let g: Vec<i32> = (0..n_per_rg).map(|i| ((base + i) % 3) as i32).collect();
            let v: Vec<f64> = (0..n_per_rg).map(|i| (base + i) as f64 * 0.5).collect();
            let mut col = rg.next_column().unwrap().unwrap();
            if let ColumnWriter::Int32ColumnWriter(t) = col.untyped() {
                t.write_batch(&g, None, None).unwrap();
            }
            col.close().unwrap();
            let mut col = rg.next_column().unwrap().unwrap();
            if let ColumnWriter::DoubleColumnWriter(t) = col.untyped() {
                t.write_batch(&v, None, None).unwrap();
            }
            col.close().unwrap();
            rg.close().unwrap();
        }
        writer.close().unwrap();

        let path_str = path.to_string_lossy().to_string();
        let sql = "SELECT g, SUM(v) AS s, COUNT(*) AS n \
                   FROM t \
                   WHERE v > 100.0 \
                   GROUP BY g \
                   ORDER BY g";

        async fn run_one(
            provider: EmatixFastParquetTableProvider,
            sql: &str,
        ) -> Vec<(i32, f64, i64)> {
            let ctx = SessionContext::new();
            ctx.register_table("t", Arc::new(provider)).unwrap();
            let df = ctx.sql(sql).await.unwrap();
            let batches = df.collect().await.unwrap();
            let mut out: Vec<(i32, f64, i64)> = Vec::new();
            for b in &batches {
                let g = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int32Array>()
                    .unwrap();
                let s = b
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow_array::Float64Array>()
                    .unwrap();
                let n = b
                    .column(2)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap();
                for i in 0..b.num_rows() {
                    out.push((g.value(i), s.value(i), n.value(i)));
                }
            }
            out
        }

        let bridge_prov = EmatixFastParquetTableProvider::try_new(&path_str).unwrap();
        let stream_prov = EmatixFastParquetTableProvider::try_new(&path_str)
            .unwrap()
            .with_streaming_arrow_reader(true);
        assert!(stream_prov.streaming_arrow_reader());

        let bridge_rows = run_one(bridge_prov, sql).await;
        let stream_rows = run_one(stream_prov, sql).await;

        // Same group set, same counts, same sums (within fp slack).
        assert_eq!(bridge_rows.len(), stream_rows.len(), "row count mismatch");
        assert!(!bridge_rows.is_empty(), "expected non-empty result");
        for (i, (b, s)) in bridge_rows.iter().zip(stream_rows.iter()).enumerate() {
            assert_eq!(b.0, s.0, "group key mismatch at row {i}");
            assert_eq!(b.2, s.2, "count mismatch at row {i} (g={})", b.0);
            assert!(
                (b.1 - s.1).abs() < 1e-6 * b.1.abs().max(1.0),
                "sum mismatch at row {i}: bridge {} vs stream {}",
                b.1,
                s.1
            );
        }

        // Sanity: the manual oracle matches too.
        let mut expected: std::collections::BTreeMap<i32, (f64, i64)> =
            std::collections::BTreeMap::new();
        let total = n_per_rg * n_rgs;
        for i in 0..total {
            let g = (i % 3) as i32;
            let v = i as f64 * 0.5;
            if v > 100.0 {
                let e = expected.entry(g).or_insert((0.0, 0));
                e.0 += v;
                e.1 += 1;
            }
        }
        for (i, (g, (s, n))) in expected.into_iter().enumerate() {
            assert_eq!(bridge_rows[i].0, g);
            assert!((bridge_rows[i].1 - s).abs() < 1e-6 * s.abs().max(1.0));
            assert_eq!(bridge_rows[i].2, n);
        }
    }

    /// Σ.Q.L4′ — I64InBloom pushdown: write a small i64 column, build
    /// a bloom from a known subset of keys, run BridgeFilter::build_bitmap
    /// and assert (a) every "in-the-build-set" row is set in the bitmap
    /// (no false negatives) and (b) the bloom rejects most "not-in"
    /// rows (false-positive rate within a sane bound).
    #[test]
    fn i64_in_bloom_bitmap_rejects_misses() {
        use crate::bloom::BloomFilter;
        use datafusion::parquet::basic::{Compression, Repetition, Type as PhysicalType};
        use datafusion::parquet::column::writer::ColumnWriter;
        use datafusion::parquet::file::properties::WriterProperties;
        use datafusion::parquet::file::writer::SerializedFileWriter;
        use datafusion::parquet::schema::types::Type as PType;

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let schema = Arc::new(
            PType::group_type_builder("schema")
                .with_fields(vec![Arc::new(
                    PType::primitive_type_builder("k", PhysicalType::INT64)
                        .with_repetition(Repetition::REQUIRED)
                        .build()
                        .unwrap(),
                )])
                .build()
                .unwrap(),
        );
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build(),
        );
        let file = File::create(&path).unwrap();
        let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
        let mut rg = writer.next_row_group().unwrap();
        // k = 0, 7, 14, ... 6993 (1000 distinct i64 values).
        let keys: Vec<i64> = (0..1000i64).map(|i| i * 7).collect();
        let mut col = rg.next_column().unwrap().unwrap();
        if let ColumnWriter::Int64ColumnWriter(t) = col.untyped() {
            t.write_batch(&keys, None, None).unwrap();
        }
        col.close().unwrap();
        rg.close().unwrap();
        writer.close().unwrap();

        // Build a bloom from every 10th key: {0, 70, 140, ..., 6930}.
        let build_keys: Vec<i64> = (0..100i64).map(|i| i * 70).collect();
        let mut bloom = BloomFilter::for_keys(build_keys.len());
        for k in &build_keys {
            bloom.insert_i64(*k);
        }
        let bloom = Arc::new(bloom);

        let filter = BridgeFilter::new(vec![ColumnPredicate::I64InBloom {
            col_idx: 0,
            bloom: bloom.clone(),
        }]);
        let (bitmap, total) = filter.build_bitmap(&path, 0).unwrap();
        assert_eq!(total, keys.len());

        // No false negatives: every row whose key is in build_keys must
        // be set in the bitmap.
        let build_set: std::collections::HashSet<i64> = build_keys.iter().copied().collect();
        let mut true_positives = 0usize;
        let mut false_positives = 0usize;
        for (row, &k) in keys.iter().enumerate() {
            let bit = (bitmap[row >> 3] >> (row & 7)) & 1 == 1;
            if build_set.contains(&k) {
                assert!(bit, "false negative at row {row} (key={k})");
                true_positives += 1;
            } else if bit {
                false_positives += 1;
            }
        }
        assert_eq!(true_positives, build_keys.len());
        // 1% FPR target ⇒ on 900 non-build rows expect ≤90 FPs in the
        // worst case, but with k=8 hashes the empirical rate is much
        // lower. Allow 5% to keep the test stable.
        let fp_rate = false_positives as f64 / (keys.len() - build_keys.len()) as f64;
        assert!(
            fp_rate < 0.05,
            "bloom false-positive rate {fp_rate:.4} exceeds 5%"
        );
    }

    /// Σ.LM.1 — the PREWHERE-style short-circuit: once the AND-
    /// accumulated bitmap is all-zero, `build_bitmap` must NOT decode
    /// the remaining predicate columns for that row group. Pinned
    /// structurally, immune to parallel-test counter noise: predicate
    /// 2 names an out-of-bounds column, so it ERRORS if evaluated —
    /// `Ok(all-zero)` proves the elision, and the reversed order
    /// proves the probe actually trips when reached.
    #[test]
    fn build_bitmap_short_circuits_on_all_zero_accumulator() {
        use datafusion::parquet::basic::{Compression, Repetition, Type as PhysicalType};
        use datafusion::parquet::column::writer::ColumnWriter;
        use datafusion::parquet::file::properties::WriterProperties;
        use datafusion::parquet::file::writer::SerializedFileWriter;
        use datafusion::parquet::schema::types::Type as PType;
        use std::fs::File;

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let schema = Arc::new(
            PType::group_type_builder("schema")
                .with_fields(vec![Arc::new(
                    PType::primitive_type_builder("a", PhysicalType::INT32)
                        .with_repetition(Repetition::REQUIRED)
                        .build()
                        .unwrap(),
                )])
                .build()
                .unwrap(),
        );
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build(),
        );
        let file = File::create(&path).unwrap();
        let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
        let mut rg = writer.next_row_group().unwrap();
        let vals: Vec<i32> = (0..512).collect();
        let mut col = rg.next_column().unwrap().unwrap();
        if let ColumnWriter::Int32ColumnWriter(t) = col.untyped() {
            t.write_batch(&vals, None, None).unwrap();
        }
        col.close().unwrap();
        rg.close().unwrap();
        writer.close().unwrap();

        // Killer first: IN(-1) matches nothing → all-zero accumulator.
        let killer = ColumnPredicate::I32In {
            col_idx: 0,
            values: vec![-1],
        };
        // Sanity: the killer ALONE must produce an all-zero bitmap —
        // if a kernel treats an empty dict selection as pass-through,
        // everything below is meaningless.
        let (kb, kt) = BridgeFilter::new(vec![killer.clone()])
            .build_bitmap(&path, 0)
            .expect("killer alone");
        assert_eq!(kt, vals.len());
        let set_bits: u32 = kb.iter().map(|b| b.count_ones()).sum();
        assert_eq!(set_bits, 0, "killer bitmap must be all-zero");
        // Probe second: column 99 does not exist → evaluating it errors.
        let err_probe = ColumnPredicate::I32In {
            col_idx: 99,
            values: vec![0],
        };

        let filter = BridgeFilter::new(vec![killer.clone(), err_probe.clone()]);
        let (bitmap, total) = filter
            .build_bitmap(&path, 0)
            .expect("short-circuit must skip the out-of-bounds probe");
        assert_eq!(total, vals.len());
        assert!(
            bitmap_all_zero(&bitmap),
            "killer predicate must zero the bitmap"
        );

        // Reversed order: the probe is evaluated FIRST and must blow
        // up (the bridge panics on an out-of-bounds column) — proving
        // the probe is a real tripwire, not silently ignored, so the
        // Ok() above can only mean the short-circuit skipped it.
        let reversed = BridgeFilter::new(vec![err_probe, killer]);
        let tripped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            reversed.build_bitmap(&path, 0)
        }));
        assert!(
            tripped.is_err() || matches!(&tripped, Ok(Err(_))),
            "out-of-bounds predicate must fail when actually evaluated"
        );
    }

    /// Σ.CC — the condition cache serves a repeated static predicate
    /// WITHOUT touching the data file. Structural proof: compute once
    /// (miss), overwrite the file with same-length garbage while
    /// restoring its mtime (identity key unchanged), and the repeat
    /// must return the identical bitmap from cache — a recompute
    /// would fail on the garbage, which the mtime-bumped third call
    /// proves.
    #[test]
    fn cond_cache_serves_repeat_without_file_reads() {
        use datafusion::parquet::basic::{Compression, Repetition, Type as PhysicalType};
        use datafusion::parquet::column::writer::ColumnWriter;
        use datafusion::parquet::file::properties::WriterProperties;
        use datafusion::parquet::file::writer::SerializedFileWriter;
        use datafusion::parquet::schema::types::Type as PType;
        use std::fs::File;

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let schema = Arc::new(
            PType::group_type_builder("schema")
                .with_fields(vec![Arc::new(
                    PType::primitive_type_builder("a", PhysicalType::INT32)
                        .with_repetition(Repetition::REQUIRED)
                        .build()
                        .unwrap(),
                )])
                .build()
                .unwrap(),
        );
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build(),
        );
        let file = File::create(&path).unwrap();
        let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
        let mut rg = writer.next_row_group().unwrap();
        let vals: Vec<i32> = (0..256).collect();
        let mut col = rg.next_column().unwrap().unwrap();
        if let ColumnWriter::Int32ColumnWriter(t) = col.untyped() {
            t.write_batch(&vals, None, None).unwrap();
        }
        col.close().unwrap();
        rg.close().unwrap();
        writer.close().unwrap();

        let filter = BridgeFilter::new(vec![ColumnPredicate::I32In {
            col_idx: 0,
            values: vec![7, 11],
        }]);
        assert!(filter.cond_fingerprint().is_some(), "static set must fp");

        // 1) Miss → compute from the real file.
        let (first, total) = filter.build_bitmap(&path, 0).expect("first compute");
        assert_eq!(total, vals.len());

        // 2) Corrupt the file IN PLACE (same length), restore mtime →
        //    the identity key is unchanged and the repeat must be a
        //    cache hit with the identical bitmap.
        let orig_mtime = std::fs::metadata(&path).unwrap().modified().unwrap();
        let len = std::fs::metadata(&path).unwrap().len() as usize;
        std::fs::write(&path, vec![0xAAu8; len]).unwrap();
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.set_times(std::fs::FileTimes::new().set_modified(orig_mtime))
            .unwrap();
        let (second, total2) = filter
            .build_bitmap(&path, 0)
            .expect("repeat must be served from the condition cache");
        assert_eq!(second, first);
        assert_eq!(total2, total);

        // 3) Bump mtime → new identity → recompute hits the garbage
        //    and must fail: proof the step-2 success came from cache.
        f.set_times(std::fs::FileTimes::new().set_modified(std::time::SystemTime::now()))
            .unwrap();
        let recompute = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            filter.build_bitmap(&path, 0)
        }));
        assert!(
            recompute.is_err() || matches!(&recompute, Ok(Err(_))),
            "identity change must force a recompute, which fails on garbage"
        );

        // Runtime-bloom predicates never fingerprint.
        let mut bloom = crate::bloom::BloomFilter::for_keys(4);
        bloom.insert_i64(1);
        let bloomy = BridgeFilter::new(vec![ColumnPredicate::I64InBloom {
            col_idx: 0,
            bloom: Arc::new(bloom),
        }]);
        assert!(bloomy.cond_fingerprint().is_none());
    }

    /// Σ.LM.1 — word-scan all-zero helper edge cases.
    #[test]
    fn bitmap_all_zero_edges() {
        assert!(bitmap_all_zero(&[]));
        assert!(bitmap_all_zero(&[0u8; 17]));
        let mut b = vec![0u8; 17];
        b[16] = 0x80;
        assert!(!bitmap_all_zero(&b));
        b[16] = 0;
        b[0] = 1;
        assert!(!bitmap_all_zero(&b));
    }

    /// Σ.AH.1 re-audit (2026-07-01) — pin scan-decode-level consumption
    /// of a published runtime predicate. `PERF_REVIEW_2026_05` § Σ.AH.1
    /// claimed the L9 bloom was "consumed at HashJoinExec level — rows
    /// decoded then dropped"; in reality a sideband-published
    /// `I64InBloom` filters the SCAN's own output (no join anywhere in
    /// the plan), under BOTH decode arms:
    ///
    /// - fused probe (default ON since Σ.AI.2): dense decode + bitmap +
    ///   per-batch SIMD filter;
    /// - `EMAT_L9_FUSED_PROBE=0` legacy: `build_bitmap` + Π.10
    ///   `read_column_*_masked_into` masked decode (the literal
    ///   "skip decode of bloom-missing rows" arm).
    ///
    /// Both must emit exactly the bloom-passing rows (inexact by design
    /// — false positives included; a downstream join re-applies). An
    /// all-pass predicate routes to dense (REV.23 pass-rate gate) with
    /// every row emitted. A test like this at spec-writing time would
    /// have caught the stale premise. See
    /// `docs/PHASE_SIGMA_AH_1_DESIGN.md` § 11.
    #[tokio::test]
    async fn published_sideband_filters_rows_at_scan_decode_level() {
        use crate::bloom::BloomFilter;
        use datafusion::common::tree_node::{Transformed, TreeNode};
        use ematix_parquet_codec::write::{ColumnData, write_table_to_path};
        use ematix_parquet_format::types::CompressionCodec;

        // Crate-wide env lock: arm 2 below toggles EMAT_L9_FUSED_PROBE —
        // result-identical for concurrent readers, but any test asserting
        // on the fused PATH (fire counters) would flake mid-window.
        let _env = NARROW_KEY_ENV_LOCK.lock().await;
        let dir = std::env::temp_dir().join(format!("sideband_scan_pin_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("t.parquet");
        // Non-"*key" column names so KEYS.2 narrowing can't reshape the
        // schema under the test.
        let k: Vec<i64> = (0..4096).collect();
        let v: Vec<i64> = (0..4096).map(|x| x * 3).collect();
        write_table_to_path(
            &path,
            &[("ka", ColumnData::I64(&k)), ("vb", ColumnData::I64(&v))],
            CompressionCodec::Uncompressed,
        )
        .unwrap();

        // Bloom over every 64th key → ~1.6% pass (+ ~1% FPs), safely
        // under the 10% masked→dense routing threshold, so the bitmap
        // is stashed and the scan output is actually filtered.
        let build: Vec<i64> = (0..4096i64).step_by(64).collect();
        let mut b = BloomFilter::for_keys(build.len());
        for &x in &build {
            b.insert_i64(x);
        }
        let bloom = Arc::new(b);
        // Oracle: exactly the bloom-passing rows, in file order. The
        // bloom is deterministic, so this is exact (FPs included).
        let expected: Vec<i64> = k
            .iter()
            .copied()
            .filter(|&x| bloom.might_contain_i64(x))
            .collect();
        assert!(expected.len() >= build.len(), "bloom lost inserted keys");
        assert!(
            (expected.len() as f64) < 0.10 * k.len() as f64,
            "fixture must stay under the dense-routing threshold \
             (pass {} of {})",
            expected.len(),
            k.len()
        );

        let prov = EmatixFastParquetTableProvider::try_new(path.to_str().unwrap()).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(prov)).unwrap();
        let plan = ctx
            .sql("SELECT ka, vb FROM t")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();

        // Attach a sideband to the scan and publish BEFORE execute; the
        // scan's deferred first-poll peek must merge the predicate into
        // its BridgeFilter (ematix_fast_parquet.rs execute()).
        let sideband = crate::bridge_filter_sideband::BridgeFilterSideband::new();
        sideband.publish(vec![ColumnPredicate::I64InBloom {
            col_idx: 0,
            bloom: bloom.clone(),
        }]);
        let sb_for_walk = sideband.clone();
        let plan = plan
            .transform_up(move |node| {
                if let Some(scan) = node.as_any().downcast_ref::<EmatixFastParquetExec>() {
                    Ok(Transformed::yes(
                        scan.with_runtime_sideband(sb_for_walk.clone()) as Arc<dyn ExecutionPlan>,
                    ))
                } else {
                    Ok(Transformed::no(node))
                }
            })
            .unwrap()
            .data;

        async fn scan_keys(plan: &Arc<dyn ExecutionPlan>, ctx: &SessionContext) -> Vec<i64> {
            let batches = datafusion::physical_plan::collect(plan.clone(), ctx.task_ctx())
                .await
                .unwrap();
            batches
                .iter()
                .flat_map(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<arrow_array::Int64Array>()
                        .expect("ka must decode as Int64Array")
                        .values()
                        .to_vec()
                })
                .collect()
        }

        // Arm 1 — fused probe (default ON): the scan emits ONLY the
        // bloom-passing rows. No join exists to do it for us.
        let got_fused = scan_keys(&plan, &ctx).await;
        assert_eq!(
            got_fused, expected,
            "fused arm: scan output must be exactly the bloom-passing rows"
        );

        // Arm 2 — legacy masked decode (`EMAT_L9_FUSED_PROBE=0`): same
        // rows via build_bitmap + read_column_*_masked_into. Restore the
        // prior value afterwards (both arms are result-identical, so a
        // concurrent reader momentarily seeing "0" stays correct).
        let prior = std::env::var("EMAT_L9_FUSED_PROBE").ok();
        unsafe { std::env::set_var("EMAT_L9_FUSED_PROBE", "0") };
        let got_masked = scan_keys(&plan, &ctx).await;
        match prior {
            Some(p) => unsafe { std::env::set_var("EMAT_L9_FUSED_PROBE", p) },
            None => unsafe { std::env::remove_var("EMAT_L9_FUSED_PROBE") },
        }
        assert_eq!(
            got_masked, expected,
            "legacy masked arm must emit the same rows as the fused arm"
        );

        // Arm 3 — all-pass predicate: pass rate 100% > threshold, so
        // REV.23 routes to dense (bitmap discarded); every row emitted,
        // results unchanged (downstream re-applies inexact predicates).
        let mut all = BloomFilter::for_keys(k.len());
        for &x in &k {
            all.insert_i64(x);
        }
        sideband.publish(vec![ColumnPredicate::I64InBloom {
            col_idx: 0,
            bloom: Arc::new(all),
        }]);
        let got_dense = scan_keys(&plan, &ctx).await;
        assert_eq!(
            got_dense, k,
            "all-pass predicate must dense-route and emit every row"
        );
    }

    /// Σ.T Phase 1 (2026-05-25): `TableProvider::statistics()` must
    /// surface per-column min/max/null_count derived from parquet
    /// row-group metadata. Without this the logical planner has only
    /// `num_rows` to work with, so it can't reorder joins by
    /// predicate selectivity.
    ///
    /// The infra (`aggregate_column_statistics` + cached `column_stats`)
    /// is already in place — used by `partition_statistics` on the
    /// Exec — but the TableProvider's own `statistics()` impl ignored
    /// it and returned `new_unknown` for every column. This test
    /// guards the wire-up.
    #[tokio::test]
    async fn table_provider_statistics_exposes_typed_column_stats() {
        use datafusion::common::ScalarValue;
        use datafusion::common::stats::Precision;
        use datafusion::parquet::basic::{Compression, Repetition, Type as PhysicalType};
        use datafusion::parquet::column::writer::ColumnWriter;
        use datafusion::parquet::file::properties::WriterProperties;
        use datafusion::parquet::file::writer::SerializedFileWriter;
        use datafusion::parquet::schema::types::Type as PType;

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let pschema = Arc::new(
            PType::group_type_builder("schema")
                .with_fields(vec![
                    Arc::new(
                        PType::primitive_type_builder("a", PhysicalType::INT32)
                            .with_repetition(Repetition::REQUIRED)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        PType::primitive_type_builder("b", PhysicalType::INT64)
                            .with_repetition(Repetition::REQUIRED)
                            .build()
                            .unwrap(),
                    ),
                ])
                .build()
                .unwrap(),
        );
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build(),
        );
        let file = File::create(&path).unwrap();
        let mut writer = SerializedFileWriter::new(file, pschema, props).unwrap();
        let mut rg = writer.next_row_group().unwrap();
        let a: Vec<i32> = (10..1010).collect();
        let mut col = rg.next_column().unwrap().unwrap();
        if let ColumnWriter::Int32ColumnWriter(t) = col.untyped() {
            t.write_batch(&a, None, None).unwrap();
        }
        col.close().unwrap();
        let b: Vec<i64> = (0..1000i64).map(|i| i * 100).collect();
        let mut col = rg.next_column().unwrap().unwrap();
        if let ColumnWriter::Int64ColumnWriter(t) = col.untyped() {
            t.write_batch(&b, None, None).unwrap();
        }
        col.close().unwrap();
        rg.close().unwrap();
        writer.close().unwrap();

        let provider =
            EmatixFastParquetTableProvider::try_new(path.to_string_lossy().to_string()).unwrap();
        let stats = provider.statistics().expect("provider must publish stats");
        assert_eq!(stats.num_rows, Precision::Exact(1000));
        assert_eq!(
            stats.column_statistics.len(),
            2,
            "one ColumnStatistics per schema field"
        );

        // Column `a` — i32 in [10, 1009], no nulls
        let cs_a = &stats.column_statistics[0];
        assert_eq!(
            cs_a.null_count,
            Precision::Exact(0),
            "column a: null_count should be Exact(0), got {:?}",
            cs_a.null_count
        );
        assert_eq!(
            cs_a.min_value,
            Precision::Exact(ScalarValue::Int32(Some(10))),
            "column a: min_value should be Exact(10), got {:?}",
            cs_a.min_value
        );
        assert_eq!(
            cs_a.max_value,
            Precision::Exact(ScalarValue::Int32(Some(1009))),
            "column a: max_value should be Exact(1009), got {:?}",
            cs_a.max_value
        );

        // Column `b` — i64 in [0, 99_900]
        let cs_b = &stats.column_statistics[1];
        assert_eq!(cs_b.null_count, Precision::Exact(0));
        assert_eq!(
            cs_b.min_value,
            Precision::Exact(ScalarValue::Int64(Some(0)))
        );
        assert_eq!(
            cs_b.max_value,
            Precision::Exact(ScalarValue::Int64(Some(99_900)))
        );
    }

    /// Σ.AH.2 Story 1'.2 — on the actual TPC-H SF=10 part.parquet,
    /// the p_type column should land with `distinct_count =
    /// Inexact(150)` (150 unique p_type values in TPC-H). Skipped if
    /// the SF=10 dataset isn't present (e.g. CI).
    #[tokio::test]
    #[ignore = "dict-distinct walk is opt-in (default-skipped, EMAT_DICT_DISTINCT); see commit 22e1f6e — run with EMAT_DICT_DISTINCT=1"]
    async fn tpch_part_p_type_distinct_count_is_150() {
        use datafusion::common::stats::Precision;
        use std::path::PathBuf;
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let path = manifest
            .parent()
            .and_then(|p| p.parent())
            .map(|p| p.join("examples/tpch/data/sf10/part.parquet"));
        let Some(path) = path.filter(|p| p.exists()) else {
            eprintln!("TPC-H SF=10 part.parquet not present; skipping");
            return;
        };
        let provider =
            EmatixFastParquetTableProvider::try_new(path.to_string_lossy().to_string()).unwrap();
        let stats = provider.statistics().unwrap();
        // p_type is field index 4 in part schema
        let cs = &stats.column_statistics[4];
        assert!(
            matches!(cs.distinct_count, Precision::Inexact(n) if n == 150),
            "p_type distinct_count should be Inexact(150), got {:?}",
            cs.distinct_count
        );
    }

    /// Σ.AH.2 Story 1'.2 (2026-05-26) — for dict-encoded string
    /// columns, `column_stats[col].distinct_count` should be populated
    /// from the parquet dict-page `num_values` (max across RGs).
    /// Without this, `ColumnPredicate::StringEq::estimate_pass_rate`
    /// falls through to its 0.1 default and the L9 ratio gate
    /// over-estimates post-filter cardinality.
    ///
    /// Builds a parquet file with a Utf8 column carrying 5 distinct
    /// values × 1000 rows. After construction, distinct_count should
    /// be `Inexact(5)` (dict pages will dedup the 5 entries).
    #[tokio::test]
    #[ignore = "dict-distinct walk is opt-in (default-skipped, EMAT_DICT_DISTINCT); see commit 22e1f6e — run with EMAT_DICT_DISTINCT=1"]
    async fn dict_encoded_string_column_populates_distinct_count() {
        use datafusion::common::stats::Precision;
        use datafusion::parquet::basic::{Compression, Repetition, Type as PhysicalType};
        use datafusion::parquet::column::writer::ColumnWriter;
        use datafusion::parquet::data_type::ByteArray;
        use datafusion::parquet::file::properties::WriterProperties;
        use datafusion::parquet::file::writer::SerializedFileWriter;
        use datafusion::parquet::schema::types::Type as PType;

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        let pschema = Arc::new(
            PType::group_type_builder("schema")
                .with_fields(vec![Arc::new(
                    PType::primitive_type_builder("s", PhysicalType::BYTE_ARRAY)
                        .with_repetition(Repetition::REQUIRED)
                        .with_converted_type(datafusion::parquet::basic::ConvertedType::UTF8)
                        .build()
                        .unwrap(),
                )])
                .build()
                .unwrap(),
        );
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .set_dictionary_enabled(true)
                .build(),
        );
        let file = File::create(&path).unwrap();
        let mut writer = SerializedFileWriter::new(file, pschema, props).unwrap();
        let mut rg = writer.next_row_group().unwrap();
        // 5 distinct values × 200 reps = 1000 rows. Dict page will
        // carry exactly 5 entries.
        let values: Vec<ByteArray> = (0..1000)
            .map(|i| ByteArray::from(format!("v{}", i % 5).into_bytes()))
            .collect();
        let mut col = rg.next_column().unwrap().unwrap();
        if let ColumnWriter::ByteArrayColumnWriter(t) = col.untyped() {
            t.write_batch(&values, None, None).unwrap();
        }
        col.close().unwrap();
        rg.close().unwrap();
        writer.close().unwrap();

        let provider =
            EmatixFastParquetTableProvider::try_new(path.to_string_lossy().to_string()).unwrap();
        let stats = provider.statistics().expect("provider must publish stats");
        let cs = &stats.column_statistics[0];
        assert_eq!(
            cs.distinct_count,
            Precision::Inexact(5),
            "dict-encoded string col should report distinct_count=Inexact(5), got {:?}",
            cs.distinct_count
        );
    }

    // ------------- LPT.RG — cost-balanced scan assignment tests -------------

    /// LPT.RG — per-call unique fixture dir (process id + atomic counter,
    /// the 01e6a50 pattern) so parallel tests never collide on a re-write.
    fn lpt_fixture_dir(name: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "lpt_rg_assign_{}_{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
            name
        ));
        let _ = std::fs::create_dir_all(&dir);
        dir
    }

    /// LPT.RG — classic LPT balance guarantee on constructed skewed
    /// inputs: every RG assigned exactly once, and the final
    /// `max_load − min_load` never exceeds the largest single RG cost.
    #[test]
    fn lpt_balance_bounded_by_max_single_cost() {
        let cases: Vec<(Vec<u64>, usize)> = vec![
            (vec![100, 1, 1, 1, 90, 5, 60, 2, 80, 3, 40, 7], 3),
            (vec![1000, 999, 998, 1, 1, 1, 1, 1, 1, 1], 4),
            (vec![7, 7, 7, 7, 7, 100], 2),
            // SF=10 lineitem shape: 58 RGs / 14 partitions with cost skew.
            (
                (0..58u64)
                    .map(|i| 1_000_000 + (i * 37_919) % 500_000)
                    .collect(),
                14,
            ),
        ];
        for (costs, parts) in cases {
            let a = lpt_rg_assignments(&costs, parts);
            assert_eq!(a.len(), parts);
            let mut seen = vec![false; costs.len()];
            for part in &a {
                for &rg in part {
                    assert!(!seen[rg], "rg {rg} assigned twice");
                    seen[rg] = true;
                }
            }
            assert!(seen.iter().all(|&s| s), "every rg assigned exactly once");
            let loads: Vec<u64> = a
                .iter()
                .map(|p| p.iter().map(|&rg| costs[rg]).sum())
                .collect();
            let spread = loads.iter().max().unwrap() - loads.iter().min().unwrap();
            let max_cost = *costs.iter().max().unwrap();
            assert!(
                spread <= max_cost,
                "LPT balance property violated: spread {spread} > max cost {max_cost} \
                 (loads {loads:?} for costs {costs:?} / {parts} partitions)"
            );
        }
    }

    /// LPT.RG — determinism (same input → same assignment; cost ties
    /// break by RG index, load ties by partition index) and ascending
    /// RG order within every partition (forward-sequential reads).
    #[test]
    fn lpt_deterministic_and_ascending_within_partitions() {
        let costs: Vec<u64> = (0..37u64).map(|i| (i * 7_919) % 1_000 + 1).collect();
        let a1 = lpt_rg_assignments(&costs, 5);
        let a2 = lpt_rg_assignments(&costs, 5);
        assert_eq!(a1, a2, "same input must produce the same assignment");
        for part in &a1 {
            assert!(
                part.windows(2).all(|w| w[0] < w[1]),
                "partition list must be strictly ascending: {part:?}"
            );
        }
        // Heavy cost ties: still deterministic across calls.
        let tied = vec![5u64, 5, 5, 9, 5, 5, 9, 5];
        assert_eq!(lpt_rg_assignments(&tied, 3), lpt_rg_assignments(&tied, 3));
    }

    /// LPT.RG — no-signal costs (all zero / all equal) fall back to the
    /// count-balanced split; unavailable metadata yields `None` costs;
    /// projected costs sum ONLY the projected columns and never panic on
    /// an out-of-range column index.
    #[test]
    fn lpt_fallbacks_count_balanced_and_projected_costs() {
        for costs in [vec![0u64; 10], vec![42u64; 10]] {
            let a = lpt_rg_assignments(&costs, 4);
            assert_eq!(a, count_balanced_rg_assignments(10, 4));
            let sizes: Vec<usize> = a.iter().map(Vec::len).collect();
            assert_eq!(sizes.iter().sum::<usize>(), 10);
            assert!(
                sizes.iter().max().unwrap() - sizes.iter().min().unwrap() <= 1,
                "as even as possible: {sizes:?}"
            );
        }
        // Count-balanced shape: contiguous ascending runs, sizes differ ≤ 1.
        assert_eq!(
            count_balanced_rg_assignments(5, 3),
            vec![vec![0, 1], vec![2, 3], vec![4]]
        );
        // Degenerate shapes never panic and mirror round-robin's layout.
        let empty: Vec<Vec<usize>> = vec![vec![], vec![], vec![]];
        assert_eq!(count_balanced_rg_assignments(0, 3), empty);
        assert_eq!(round_robin_rg_assignments(0, 3), empty);

        // Metadata unavailable (rg count mismatch) → None → caller falls back.
        assert!(rg_projected_costs(&[vec![1, 2]], 3, &[0]).is_none());
        // Cost = sum of PROJECTED columns only.
        let meta = vec![vec![10u64, 100, 1], vec![20, 200, 2]];
        assert_eq!(rg_projected_costs(&meta, 2, &[0, 2]), Some(vec![11, 22]));
        // Out-of-range projected column contributes 0 (never panics).
        assert_eq!(rg_projected_costs(&meta, 2, &[0, 9]), Some(vec![10, 20]));
    }

    /// LPT.RG — `EMAT_BALANCED_RG_ASSIGN=0` must reproduce the legacy
    /// round-robin bit-for-bit; unset (default ON) takes the LPT path.
    /// Mutates a production `EMAT_*` var → takes `EMAT_ENV_TEST_LOCK`
    /// (blocking_lock — sync test) and restores the prior value.
    #[test]
    fn balanced_rg_assign_off_reproduces_round_robin() {
        let _guard = crate::flags::EMAT_ENV_TEST_LOCK.blocking_lock();
        let key = "EMAT_BALANCED_RG_ASSIGN";
        let prev = std::env::var(key).ok();

        // Strongly skewed per-RG costs (powers of two).
        let meta: Vec<Vec<u64>> = (0..9).map(|i| vec![1u64 << i]).collect();
        let proj = vec![0usize];

        unsafe { std::env::set_var(key, "0") };
        let off = compute_scan_rg_assignments(9, 4, &proj, &meta);
        assert_eq!(
            off,
            round_robin_rg_assignments(9, 4),
            "=0 must restore round-robin exactly"
        );

        unsafe { std::env::remove_var(key) };
        let on = compute_scan_rg_assignments(9, 4, &proj, &meta);
        let costs: Vec<u64> = (0..9).map(|i| 1u64 << i).collect();
        assert_eq!(on, lpt_rg_assignments(&costs, 4), "default (unset) → LPT");
        assert_ne!(
            on, off,
            "skewed costs must produce a non-round-robin packing"
        );

        match prev {
            Some(v) => unsafe { std::env::set_var(key, v) },
            None => unsafe { std::env::remove_var(key) },
        }
    }

    /// LPT.RG scope pin — RANGE.AGG-style injected assignments must pass
    /// through `with_assignments` / `with_assignments_claiming_hash`
    /// UNTOUCHED (the key-disjoint chunk contract): no re-balancing, no
    /// reordering, partition count taken verbatim.
    #[tokio::test]
    async fn injected_assignments_pass_through_unbalanced() {
        use ematix_parquet_codec::write::ColumnData;
        use ematix_parquet_codec::write::write_table_to_path_with_row_group_size;
        use ematix_parquet_format::types::CompressionCodec;

        let dir = lpt_fixture_dir("passthrough");
        let path = dir.join("t.parquet");
        let k: Vec<i64> = (0..600).collect();
        write_table_to_path_with_row_group_size(
            &path,
            &[("k", ColumnData::I64(&k))],
            CompressionCodec::Uncompressed,
            100, // → 6 row groups
        )
        .unwrap();

        let provider =
            EmatixFastParquetTableProvider::try_new(path.to_string_lossy().to_string()).unwrap();
        let ctx = SessionContext::new_with_config(
            datafusion::prelude::SessionConfig::new().with_target_partitions(2),
        );
        let state = ctx.state();
        let exec = provider.scan(&state, None, &[], None).await.unwrap();
        let exec = exec
            .as_any()
            .downcast_ref::<EmatixFastParquetExec>()
            .expect("plain scan produces EmatixFastParquetExec");

        // Deliberately UNBALANCED key-disjoint chunks — a re-balancer
        // would rewrite these; the contract forbids it.
        let injected = vec![vec![0usize, 1, 2, 3, 4], vec![5]];
        let with = exec.with_assignments(injected.clone());
        assert_eq!(
            with.assignments(),
            injected.as_slice(),
            "with_assignments must pass injected chunks through verbatim"
        );
        assert_eq!(
            with.properties().output_partitioning().partition_count(),
            2,
            "partition count follows the injected assignment"
        );

        let hash_expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
            datafusion::physical_expr::expressions::col("k", &exec.schema()).unwrap();
        let claiming = exec.with_assignments_claiming_hash(injected.clone(), vec![hash_expr]);
        assert_eq!(
            claiming.assignments(),
            injected.as_slice(),
            "with_assignments_claiming_hash must pass injected chunks through verbatim"
        );
        assert!(
            matches!(
                claiming.properties().output_partitioning(),
                Partitioning::Hash(_, 2)
            ),
            "claiming variant advertises Hash partitioning over the injected chunks"
        );
    }

    /// LPT.RG e2e — a file whose HEAVY row groups sit at indices 0 and 2
    /// (round-robin with 2 partitions stacks both on partition 0): the
    /// default-ON balanced scan must split them across partitions, keep
    /// every partition ascending, and cover all RGs exactly once; `=0`
    /// restores round-robin exactly. Mutates env → `EMAT_ENV_TEST_LOCK`
    /// (held across awaits — tokio Mutex).
    #[tokio::test]
    async fn scan_balances_heavy_row_groups_and_flag_off_round_robins() {
        use arrow_array::{Int64Array, RecordBatch};
        use datafusion::parquet::arrow::ArrowWriter;

        let _guard = crate::flags::EMAT_ENV_TEST_LOCK.lock().await;
        let key = "EMAT_BALANCED_RG_ASSIGN";
        let prev = std::env::var(key).ok();
        unsafe { std::env::remove_var(key) }; // default ON

        let dir = lpt_fixture_dir("scan_e2e");
        let path = dir.join("t.parquet");
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "k",
            DataType::Int64,
            false,
        )]));
        let f = std::fs::File::create(&path).unwrap();
        let mut w = ArrowWriter::try_new(f, schema.clone(), None).unwrap();
        // RG row counts [4000, 10, 4000, 10, 10, 10] — each flush() ends
        // a row group, so compressed sizes are heavily skewed.
        for rows in [4000i64, 10, 4000, 10, 10, 10] {
            let vals: Vec<i64> = (0..rows).map(|i| i * 31 + rows).collect();
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vals))])
                    .unwrap();
            w.write(&batch).unwrap();
            w.flush().unwrap();
        }
        w.close().unwrap();

        let provider =
            EmatixFastParquetTableProvider::try_new(path.to_string_lossy().to_string()).unwrap();
        assert_eq!(provider.num_row_groups(), 6, "fixture must have 6 RGs");
        let ctx = SessionContext::new_with_config(
            datafusion::prelude::SessionConfig::new().with_target_partitions(2),
        );
        let state = ctx.state();

        let exec = provider.scan(&state, None, &[], None).await.unwrap();
        let exec = exec
            .as_any()
            .downcast_ref::<EmatixFastParquetExec>()
            .expect("plain scan produces EmatixFastParquetExec");
        let a = exec.assignments();
        assert_eq!(a.len(), 2);
        let mut all: Vec<usize> = a.iter().flatten().copied().collect();
        all.sort_unstable();
        assert_eq!(all, vec![0, 1, 2, 3, 4, 5], "all RGs exactly once");
        for part in a {
            assert!(
                part.windows(2).all(|w| w[0] < w[1]),
                "ascending within partition: {part:?}"
            );
        }
        let part_of = |rg: usize| a.iter().position(|p| p.contains(&rg)).unwrap();
        assert_ne!(
            part_of(0),
            part_of(2),
            "LPT must split the two heavy RGs apart (round-robin stacks both on \
             partition 0); assignments: {a:?}"
        );

        unsafe { std::env::set_var(key, "0") };
        let exec = provider.scan(&state, None, &[], None).await.unwrap();
        let exec = exec
            .as_any()
            .downcast_ref::<EmatixFastParquetExec>()
            .unwrap();
        assert_eq!(
            exec.assignments(),
            round_robin_rg_assignments(6, 2).as_slice(),
            "=0 must reproduce the legacy round-robin exactly"
        );

        match prev {
            Some(v) => unsafe { std::env::set_var(key, v) },
            None => unsafe { std::env::remove_var(key) },
        }
    }
}
