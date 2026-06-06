//! PV.M.8 Stage-2 — `CombineAggExec`: fused single-`i64`-key `SUM(f64)`
//! aggregation that replaces DataFusion's
//! `AggregateExec(Partial) → hash-RepartitionExec → AggregateExec(FinalPartitioned)`
//! chain with per-partition inline open-addressing tables
//! (`ematix_flow_push::I64SumF64`) + a direct parallel combine — **no shuffle**.
//!
//! ## Why (the measured lever — PV-morsel Q15 Phase-0)
//!
//! The `q15_morsel_agg_spike` + EXPLAIN ANALYZE found that on Q15's
//! `GROUP BY l_suppkey` (2.27M rows → 100K groups) the actual aggregation
//! is ~4.5ms, but DataFusion spends ~13ms wall / ~58ms CPU — almost all in
//! `time_calculating_group_ids`, done TWICE (Partial + Final) across a
//! 1.12M-row hash-`RepartitionExec`, via a `GroupValues`→idx→accumulator
//! indirection ~10× slower per row than an inline open-addressing table.
//!
//! This operator does the group-keying ONCE, inline, per input partition
//! (riding on the scan/decode threads — the per-batch ingest is ~0.3ms /
//! partition, hidden under the ~52ms decode), then directly combines the
//! per-partition tables in parallel. No partial/final split, no 1.12M-row
//! shuffle, no pipeline barrier beyond the unavoidable final combine.
//!
//! ## Shape contract (set by the rule, no TPC-H hardcoding)
//!
//! - exactly ONE group key, evaluating to non-null `Int64`;
//! - exactly ONE aggregate: `SUM` over an expression evaluating to non-null
//!   `Float64`;
//! - output schema `[group_key, sum]` (DataFusion's group-then-aggregate
//!   column order).
//! The rule gates on these + non-null columns and falls back to the stock
//! DataFusion agg otherwise (same discipline as PV.M.7).

use std::any::Any;
use std::sync::Arc;

use arrow_array::{Array, Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, SchemaRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::enforce_distribution::EnforceDistribution;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use ematix_flow_push::I64SumF64;
use futures_util::TryFutureExt;
use futures_util::stream;
use futures_util::stream::TryStreamExt;

/// Output batch row count (downstream re-batches anyway; keep it conventional).
const OUT_BATCH_ROWS: usize = 8192;

/// Diagnostic counter: number of times `CombineAggExec::execute(0)` ran its
/// combine (one per query that actually hit the operator). Lets the A/B
/// harness confirm the operator fired vs. fell through.
pub static COMBINE_AGG_FIRES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
/// Diagnostic (ns, opt-in via reading): wall of the build phase (drain all
/// partitions + local shard-aggregate — decode-bound) vs the combine phase
/// (parallel shard merge). Split tells intrinsic-barrier from fixable combine.
pub static COMBINE_AGG_BUILD_NANOS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub static COMBINE_AGG_COMBINE_NANOS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

pub struct CombineAggExec {
    input: Arc<dyn ExecutionPlan>,
    /// Single GROUP BY key expression — must evaluate to `Int64`.
    group_expr: Arc<dyn PhysicalExpr>,
    /// Single SUM input expression — must evaluate to `Float64`.
    value_expr: Arc<dyn PhysicalExpr>,
    /// Output schema `[group_key, sum]`.
    schema: SchemaRef,
    /// Per-partition table pre-size hint (≈ expected distinct groups).
    group_card_hint: usize,
    properties: Arc<PlanProperties>,
}

impl CombineAggExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        group_expr: Arc<dyn PhysicalExpr>,
        value_expr: Arc<dyn PhysicalExpr>,
        schema: SchemaRef,
        group_card_hint: usize,
    ) -> Self {
        let eq = EquivalenceProperties::new(schema.clone());
        let properties = Arc::new(PlanProperties::new(
            eq,
            // The combine is a barrier: one combined output partition.
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            input,
            group_expr,
            value_expr,
            schema,
            group_card_hint,
            properties,
        }
    }
}

impl std::fmt::Debug for CombineAggExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CombineAggExec(group+sum, hint={})", self.group_card_hint)
    }
}

impl DisplayAs for CombineAggExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "CombineAggExec: i64-key SUM(f64), card_hint={}",
            self.group_card_hint
        )
    }
}

impl ExecutionPlan for CombineAggExec {
    fn name(&self) -> &str {
        "CombineAggExec"
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
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "CombineAggExec expects 1 child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(CombineAggExec::new(
            children[0].clone(),
            self.group_expr.clone(),
            self.value_expr.clone(),
            self.schema.clone(),
            self.group_card_hint,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "CombineAggExec: partition={partition}, only 0 is valid (UnknownPartitioning(1))"
            )));
        }
        let n_parts = self.input.output_partitioning().partition_count();
        // Claim every input partition stream SYNCHRONOUSLY at execute time
        // (before awaiting) — if the input is a RepartitionExec, delaying
        // execute(p) trips its "partition not used yet" invariant. Same
        // discipline as EmatPushPipelineExec / SharedSubtreeExec.
        let mut streams = Vec::with_capacity(n_parts);
        for p in 0..n_parts {
            streams.push(self.input.execute(p, ctx.clone())?);
        }
        let group_expr = self.group_expr.clone();
        let value_expr = self.value_expr.clone();
        let hint = self.group_card_hint;
        let out_schema = self.schema.clone();
        let stream_schema = self.schema.clone();

        // `r` shard columns (one per input partition). Pre-sharding at BUILD
        // (route each row by high hash bits) is what makes the combine cheap:
        // each combine thread then reads only small DENSE shard tables, never
        // re-scanning one big ~60%-empty 256K-slot table r× (that scan-waste
        // was the +10% loss in the first cut — q15_morsel_agg_spike PARTITIONED
        // arm is the validated design).
        let r = n_parts.max(1);
        let shard_hint = hint / r + 1;
        let fut = async move {
            let t0 = std::time::Instant::now();
            // Phase A — per-partition aggregate into `r` shard tables,
            // concurrently. The ingest rides on the thread that decodes that
            // partition (the agg work is ~0.3ms/partition under the ~50ms scan).
            let can_spawn = tokio::runtime::Handle::try_current().is_ok();
            let mut built: Vec<Vec<I64SumF64>> = Vec::with_capacity(n_parts);
            if can_spawn && n_parts > 1 {
                let mut handles = Vec::with_capacity(n_parts);
                for mut s in streams {
                    let g = group_expr.clone();
                    let v = value_expr.clone();
                    handles.push(tokio::spawn(async move {
                        let mut shards: Vec<I64SumF64> =
                            (0..r).map(|_| I64SumF64::with_capacity(shard_hint)).collect();
                        while let Some(batch) = s.try_next().await? {
                            ingest_batch_sharded(&mut shards, r, &g, &v, &batch)?;
                        }
                        Ok::<_, DataFusionError>(shards)
                    }));
                }
                for h in handles {
                    let shards = h.await.map_err(|e| {
                        DataFusionError::Execution(format!("CombineAggExec drain failed: {e}"))
                    })??;
                    built.push(shards);
                }
            } else {
                for mut s in streams {
                    let mut shards: Vec<I64SumF64> =
                        (0..r).map(|_| I64SumF64::with_capacity(shard_hint)).collect();
                    while let Some(batch) = s.try_next().await? {
                        ingest_batch_sharded(&mut shards, r, &group_expr, &value_expr, &batch)?;
                    }
                    built.push(shards);
                }
            }

            // Phase B — combine shard `s` across all partitions in parallel.
            // Disjoint shards (same routing everywhere) ⇒ lock-free; each thread
            // touches only its small tables. Off the async workers (CPU-bound).
            COMBINE_AGG_FIRES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            COMBINE_AGG_BUILD_NANOS
                .fetch_add(t0.elapsed().as_nanos() as u64, std::sync::atomic::Ordering::Relaxed);
            let t1 = std::time::Instant::now();
            let (keys, sums) = if can_spawn {
                tokio::task::spawn_blocking(move || combine_sharded(built, r, shard_hint))
                    .await
                    .map_err(|e| {
                        DataFusionError::Execution(format!("CombineAggExec combine failed: {e}"))
                    })?
            } else {
                combine_sharded(built, r, shard_hint)
            };
            COMBINE_AGG_COMBINE_NANOS
                .fetch_add(t1.elapsed().as_nanos() as u64, std::sync::atomic::Ordering::Relaxed);

            let batches = to_batches(&out_schema, keys, sums)?;
            Ok::<_, DataFusionError>(stream::iter(batches.into_iter().map(Ok::<_, DataFusionError>)))
        };

        let s = fut.try_flatten_stream();
        Ok(Box::pin(RecordBatchStreamAdapter::new(stream_schema, s)))
    }
}

/// Routing hash that assigns a key to one of `r` shards — independent of the
/// table's internal probe hash (different multiplier so shard-bits ≠ probe-bits).
/// MUST be identical at build and combine so a key lands in the same shard
/// everywhere (→ disjoint, lock-free merge).
#[inline(always)]
fn route(key: i64, r: usize) -> usize {
    ((key as u64).wrapping_mul(0xD6E8_FEB8_6659_FD93) as usize) % r
}

/// Evaluate the group + value exprs on `batch` and accumulate each row into its
/// routed shard table. Fast path when both columns are null-free (the gated
/// case); otherwise skip null rows (SUM semantics; rule gates on non-null).
fn ingest_batch_sharded(
    shards: &mut [I64SumF64],
    r: usize,
    group_expr: &Arc<dyn PhysicalExpr>,
    value_expr: &Arc<dyn PhysicalExpr>,
    batch: &RecordBatch,
) -> DfResult<()> {
    let n = batch.num_rows();
    if n == 0 {
        return Ok(());
    }
    let g = group_expr.evaluate(batch)?.into_array(n)?;
    let v = value_expr.evaluate(batch)?.into_array(n)?;
    let gi = g
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Internal("CombineAggExec: group key not Int64".into()))?;
    let vf = v
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| DataFusionError::Internal("CombineAggExec: sum input not Float64".into()))?;

    if gi.null_count() == 0 && vf.null_count() == 0 {
        let ks = gi.values();
        let vs = vf.values();
        for i in 0..n {
            let k = ks[i];
            shards[route(k, r)].add(k, vs[i]);
        }
    } else {
        for i in 0..n {
            if gi.is_valid(i) && vf.is_valid(i) {
                let k = gi.value(i);
                shards[route(k, r)].add(k, vf.value(i));
            }
        }
    }
    Ok(())
}

/// Direct combine of the per-partition shard tables — NO shuffle. `built[p]` is
/// partition `p`'s `r` shards; shard `s` (across all partitions) holds exactly
/// the keys with `route(k)==s`, so the `r` scoped threads merge disjoint key
/// sets in parallel with no contention, each touching only small dense tables.
/// Per-key sums accumulate in partition order (0..n) → deterministic.
fn combine_sharded(built: Vec<Vec<I64SumF64>>, r: usize, shard_hint: usize) -> (Vec<i64>, Vec<f64>) {
    if built.is_empty() {
        return (Vec::new(), Vec::new());
    }
    if r == 1 {
        let mut out = I64SumF64::with_capacity(shard_hint);
        for p in &built {
            out.combine(&p[0]);
        }
        let (mut ks, mut vs) = (Vec::new(), Vec::new());
        out.drain_into(&mut ks, &mut vs);
        return (ks, vs);
    }
    let built_ref = &built;
    let shards: Vec<(Vec<i64>, Vec<f64>)> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..r)
            .map(|s| {
                scope.spawn(move || {
                    let mut out = I64SumF64::with_capacity(shard_hint);
                    for p in built_ref.iter() {
                        out.combine(&p[s]);
                    }
                    let (mut ks, mut vs) = (Vec::new(), Vec::new());
                    out.drain_into(&mut ks, &mut vs);
                    (ks, vs)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let total: usize = shards.iter().map(|(k, _)| k.len()).sum();
    let mut keys = Vec::with_capacity(total);
    let mut sums = Vec::with_capacity(total);
    for (ks, vs) in shards {
        keys.extend(ks);
        sums.extend(vs);
    }
    (keys, sums)
}

/// Pack the combined groups into `[group_key, sum]` RecordBatches.
fn to_batches(schema: &SchemaRef, keys: Vec<i64>, sums: Vec<f64>) -> DfResult<Vec<RecordBatch>> {
    debug_assert_eq!(keys.len(), sums.len());
    if keys.is_empty() {
        return Ok(vec![RecordBatch::new_empty(schema.clone())]);
    }
    let mut out = Vec::with_capacity(keys.len().div_ceil(OUT_BATCH_ROWS));
    let mut lo = 0;
    while lo < keys.len() {
        let hi = (lo + OUT_BATCH_ROWS).min(keys.len());
        let k = Int64Array::from(keys[lo..hi].to_vec());
        let s = Float64Array::from(sums[lo..hi].to_vec());
        out.push(RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(k), Arc::new(s)],
        )?);
        lo = hi;
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Stage-3 — the swap rule. Opt-in (`EMAT_COMBINE_AGG=1`); default OFF. The 22q
// + tpch_validate gate (Stage 4) decides default-on. Kept opt-in like the
// radix-sum rule (optimizer-codegen-sensitivity).
// ---------------------------------------------------------------------------

/// `EMAT_COMBINE_AGG=1` enables the `Partial→Repartition→Final` → `CombineAggExec`
/// swap for a single-`i64`-key `SUM(f64)` GROUP BY.
pub fn combine_agg_enabled() -> bool {
    matches!(
        std::env::var("EMAT_COMBINE_AGG").as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE")
    )
}

/// Per-partition table pre-size hint (`EMAT_COMBINE_AGG_HINT`, default 131072 —
/// ≥ Q15's 100K groups so no growth; over-allocates for low-card but those
/// rarely match the gate). Tuned to a stats-derived value before default-on.
fn group_card_hint() -> usize {
    std::env::var("EMAT_COMBINE_AGG_HINT")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1usize << 17)
}

/// Swaps the two-phase `SUM(f64) GROUP BY i64` exchange for [`CombineAggExec`].
#[derive(Debug, Default)]
pub struct EnableCombineAggRule;

impl PhysicalOptimizerRule for EnableCombineAggRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if !combine_agg_enabled() {
            return Ok(plan);
        }
        let hint = group_card_hint();
        let rewritten = plan.transform_down(|node| {
            let Some(final_agg) = node.as_any().downcast_ref::<AggregateExec>() else {
                return Ok(Transformed::no(node));
            };
            if !matches!(
                final_agg.mode(),
                AggregateMode::Final | AggregateMode::FinalPartitioned
            ) || !final_is_single_group_single_sum(final_agg)
            {
                return Ok(Transformed::no(node));
            }
            let Some((scan_input, group_expr, value_expr)) =
                find_partial_sum_expr(final_agg.input())
            else {
                return Ok(Transformed::no(node));
            };
            let in_schema = scan_input.schema();
            // Type contract: i64 key, f64 value (the kernel's domain).
            if group_expr.data_type(&in_schema).ok() != Some(DataType::Int64)
                || value_expr.data_type(&in_schema).ok() != Some(DataType::Float64)
            {
                return Ok(Transformed::no(node));
            }
            // NOTE (Phase-1 opt-in): the kernel drops null-key rows (no null
            // group). Safe for FK keys (l_suppkey); before default-on, gate on
            // a provably-non-null key (PV.M.7 column_has_no_nulls) + add a null
            // group. tpch_validate is the correctness gate meanwhile.
            let op = CombineAggExec::new(
                scan_input,
                group_expr,
                value_expr,
                final_agg.schema(),
                hint,
            );
            Ok(Transformed::yes(Arc::new(op) as Arc<dyn ExecutionPlan>))
        })?;
        if !rewritten.transformed {
            return Ok(rewritten.data);
        }
        // CombineAggExec emits 1 partition; re-derive the downstream's required
        // distribution (Σ.BS — same repair the dedupe / radix rules do).
        EnforceDistribution::new().optimize(rewritten.data, config)
    }

    fn name(&self) -> &str {
        "ematix_flow_enable_combine_agg"
    }
    fn schema_check(&self) -> bool {
        true
    }
}

/// Final agg: exactly one group key + exactly one non-distinct `SUM`.
fn final_is_single_group_single_sum(agg: &AggregateExec) -> bool {
    let gb = agg.group_expr();
    if !gb.is_single() || gb.expr().len() != 1 {
        return false;
    }
    let aggs = agg.aggr_expr();
    aggs.len() == 1
        && aggs[0].fun().name().eq_ignore_ascii_case("sum")
        && !aggs[0].is_distinct()
}

/// Walk the single-child chain (through Repartition/Coalesce) to a `Partial`
/// agg with one group key + one non-distinct `SUM(<expr>)`. Returns its input
/// and the group/value EXPRESSIONS — unlike the radix matcher, the SUM input
/// may be a computed expr (Q15's `l_extendedprice*(1-l_discount)`).
fn find_partial_sum_expr(
    plan: &Arc<dyn ExecutionPlan>,
) -> Option<(Arc<dyn ExecutionPlan>, Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> {
    let mut cur = plan.clone();
    loop {
        if let Some(agg) = cur.as_any().downcast_ref::<AggregateExec>() {
            if !matches!(agg.mode(), AggregateMode::Partial) {
                return None;
            }
            let gb = agg.group_expr();
            if !gb.is_single() || gb.expr().len() != 1 {
                return None;
            }
            let group_expr = gb.expr()[0].0.clone();
            let aggs = agg.aggr_expr();
            if aggs.len() != 1 {
                return None;
            }
            let a = &aggs[0];
            if !a.fun().name().eq_ignore_ascii_case("sum") || a.is_distinct() {
                return None;
            }
            let exprs = a.expressions();
            if exprs.len() != 1 {
                return None;
            }
            return Some((agg.input().clone(), group_expr, exprs[0].clone()));
        }
        let children = cur.children();
        if children.len() != 1 {
            return None;
        }
        cur = children[0].clone();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build one partition's `r` shard tables from (keys, vals), routing each
    /// row exactly as the operator's build phase does.
    fn shard_partition(keys: &[i64], vals: &[f64], r: usize, hint: usize) -> Vec<I64SumF64> {
        let mut shards: Vec<I64SumF64> = (0..r).map(|_| I64SumF64::with_capacity(hint)).collect();
        for (&k, &v) in keys.iter().zip(vals) {
            shards[route(k, r)].add(k, v);
        }
        shards
    }

    #[test]
    fn combine_sharded_merges_and_is_complete() {
        let r = 4;
        let p0 = shard_partition(&[1, 2, 3], &[1.0, 2.0, 3.0], r, 8);
        let p1 = shard_partition(&[2, 3, 4], &[20.0, 30.0, 40.0], r, 8);
        let (keys, sums) = combine_sharded(vec![p0, p1], r, 8);
        let m: std::collections::HashMap<i64, f64> = keys.into_iter().zip(sums).collect();
        assert_eq!(m.len(), 4);
        assert_eq!(m[&1], 1.0);
        assert_eq!(m[&2], 22.0);
        assert_eq!(m[&3], 33.0);
        assert_eq!(m[&4], 40.0);
    }

    #[test]
    fn combine_sharded_single_partition_passthrough() {
        let r = 1;
        let p0 = shard_partition(&[5, 5, 9], &[1.0, 2.0, 3.0], r, 8);
        let (keys, sums) = combine_sharded(vec![p0], r, 8);
        let m: std::collections::HashMap<i64, f64> = keys.into_iter().zip(sums).collect();
        assert_eq!(m[&5], 3.0);
        assert_eq!(m[&9], 3.0);
    }

    #[test]
    fn combine_sharded_many_partitions_high_card() {
        // 14 partitions each touching all 3000 keys → exercises the sharded
        // parallel merge: each key routed to one shard everywhere, summed across
        // all 14 partitions.
        let r = 14usize;
        let ngroups = 3000i64;
        let keys: Vec<i64> = (0..ngroups).collect();
        let shint = ngroups as usize / r + 1;
        let mut built = Vec::new();
        for p in 0..r {
            let vals: Vec<f64> = (0..ngroups).map(|_| (p as f64) + 1.0).collect();
            built.push(shard_partition(&keys, &vals, r, shint));
        }
        let (keys_out, sums) = combine_sharded(built, r, shint);
        assert_eq!(keys_out.len(), ngroups as usize);
        let m: std::collections::HashMap<i64, f64> = keys_out.into_iter().zip(sums).collect();
        let expect: f64 = (1..=r).map(|p| p as f64).sum(); // 1+2+…+14 = 105
        for k in 0..ngroups {
            assert_eq!(m[&k], expect, "key {k}");
        }
    }
}
