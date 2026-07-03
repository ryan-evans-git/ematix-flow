//! RANGE.AGG (2026-06-10) — single-phase aggregation over a
//! cluster-key group-by, replacing DataFusion's
//! `AggregateExec(Partial) → hash-RepartitionExec →
//! AggregateExec(FinalPartitioned)` triple with ONE
//! `AggregateExec(SinglePartitioned)` over a scan re-chunked at
//! provably key-disjoint row-group boundaries.
//!
//! ## Why
//!
//! TPC-H Q18 SF=100: `SUM(l_quantity) GROUP BY l_orderkey` over 600M
//! lineitem rows / 150M groups costs 18.7s (Partial) + 0.5s (2.2 GB
//! hash shuffle) + 24.8s (Final) = 43.5s CPU — 3× DuckDB's
//! single-phase 14.2s. The two-phase split exists to merge groups that
//! span partitions, but lineitem is PHYSICALLY CLUSTERED on
//! `l_orderkey` (dbgen writes it sorted), so a group spans partitions
//! only at the 13 partition boundaries. The parquet row-group min/max
//! statistics PROVE the clustering at plan time — and if we place the
//! partition split points only at row-group boundaries with a STRICT
//! key gap (`max(rg_i) < min(rg_{i+1})`), no group spans any partition
//! at all. Every partition then aggregates its own key range to
//! completion independently: no shuffle, no merge phase, and ~14×
//! smaller hash tables (better cache behavior than even a
//! single-phase global agg).
//!
//! General, not TPC-H-specific: any group-by whose key is the file's
//! physical sort/cluster key (time-series by day, logs by session,
//! event streams by id) qualifies; the rule verifies clustering from
//! row-group statistics and silently declines otherwise.
//!
//! ## Shape gate
//!
//! ```text
//! AggregateExec(FinalPartitioned, gby=[k])
//!   RepartitionExec(Hash([k]))
//!     AggregateExec(Partial, gby=[k])
//!       [pass-throughs: CoalesceBatches / Cooperative]
//!         EmatixFastParquetExec        // k maps to a file column with
//!                                      // monotone non-overlapping
//!                                      // row-group [min,max] ranges
//! ```
//!
//! Bails (keeps the stock plan) when: the chain has anything else in
//! it (a RoundRobin repartition destroys clustering; filters are FINE
//! — they only drop rows, never move keys across partitions — but are
//! currently excluded for a smaller first surface), the key column's
//! RG ranges overlap or lack stats, or fewer than 2 strict-gap split
//! candidates exist.
//!
//! ## Stage 2 (2026-07-02) — shuffle-free sandwich
//!
//! Stage 1 rewrote the agg but the re-chunked scan advertised
//! `UnknownPartitioning`, so the rule's own `EnforceDistribution`
//! re-run put a hash `RepartitionExec` back between scan and agg —
//! re-shuffling the full input (Q18 SF=100: 600M rows / ~9.6 GB) to
//! satisfy a distribution requirement the key-disjoint chunking
//! already satisfies row-wise. Stage 2 closes that:
//!
//! ```text
//! PartitionClaimResetExec              // caps the claim: Unknown(n)
//!   AggregateExec(SinglePartitioned)   // requirement satisfied ✓
//!     EmatixFastParquetExec            // CLAIMS Hash([key], n)
//! ```
//!
//! The scan claims `Partitioning::Hash([group_key], n)` — row-correct
//! for the aggregation (every group's rows are in exactly one
//! partition) but false about hash-bucket placement, so the claim is
//! capped immediately above the agg by
//! [`crate::partition_claim_reset_exec::PartitionClaimResetExec`].
//! Any DOWNSTREAM hash requirement (e.g. a partitioned join consuming
//! the agg output) is then re-satisfied by `EnforceDistribution`
//! repartitioning the agg's OUTPUT (~10k rows post-HAVING on Q18)
//! instead of its input.
//!
//! Re-runs `EnforceDistribution` after the rewrite (the Σ.BS repair
//! pattern) so any downstream hash-distribution requirement is
//! re-satisfied.
//!
//! Default ON; opt out with `EMAT_RANGE_AGG=0`.

use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::enforce_distribution::EnforceDistribution;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::repartition::RepartitionExec;

use crate::ematix_fast_parquet::EmatixFastParquetExec;

/// `true` unless `EMAT_RANGE_AGG=0`/`false` (default ON, opt-out).
fn enabled() -> bool {
    crate::flags::enabled("EMAT_RANGE_AGG")
}

/// Plan key-disjoint partition chunks from per-row-group `[min, max]`
/// key ranges. Returns `None` when the file is not (provably)
/// clustered on the key, or when there are not enough strict-gap
/// boundaries to form at least 2 partitions.
///
/// - `ranges[j]` = the key column's `(min, max)` for row group `j`
///   (file order); `None` = stats missing for that RG → decline.
/// - `rg_rows[j]` = row count of RG `j`, used to balance chunk sizes.
/// - `target_parts` = desired partition count (split points are placed
///   at the strict-gap boundary nearest each ideal row offset).
///
/// A boundary between RG `j` and `j+1` is a valid split point iff
/// `max(rg_j) < min(rg_{j+1})` (STRICT) — equality means one key's
/// rows span the boundary and a split there would break that group
/// into two partitions.
pub(crate) fn plan_disjoint_chunks(
    ranges: &[Option<(i64, i64)>],
    rg_rows: &[usize],
    target_parts: usize,
) -> Option<Vec<Vec<usize>>> {
    let n = ranges.len();
    if n < 2 || rg_rows.len() != n || target_parts < 2 {
        return None;
    }
    // 1. All stats present + each RG internally sane.
    let mut rs = Vec::with_capacity(n);
    for r in ranges {
        let (lo, hi) = (*r)?;
        if lo > hi {
            return None;
        }
        rs.push((lo, hi));
    }
    // 2. Monotone non-overlapping in file order (equality at the seam
    //    allowed — that's a key spanning the boundary, legal for
    //    clustering but not a valid split point).
    for j in 0..n - 1 {
        if rs[j].1 > rs[j + 1].0 {
            return None;
        }
    }
    // 3. Strict-gap split candidates (boundary AFTER rg j).
    let candidates: Vec<usize> = (0..n - 1).filter(|&j| rs[j].1 < rs[j + 1].0).collect();
    if candidates.is_empty() {
        return None;
    }
    // 4. Pick ≤ target_parts-1 splits, each the candidate nearest its
    //    ideal cumulative-row offset; dedupe and order.
    let total_rows: usize = rg_rows.iter().sum();
    let mut cum = Vec::with_capacity(n);
    let mut acc = 0usize;
    for &r in rg_rows {
        acc += r;
        cum.push(acc); // cum[j] = rows through END of rg j
    }
    let mut splits: Vec<usize> = Vec::new();
    for p in 1..target_parts {
        let ideal = total_rows * p / target_parts;
        let best = candidates
            .iter()
            .copied()
            .min_by_key(|&j| cum[j].abs_diff(ideal))?;
        splits.push(best);
    }
    splits.sort_unstable();
    splits.dedup();
    if splits.is_empty() {
        return None;
    }
    // 5. Materialize chunks: [0..=s0], [s0+1..=s1], ..., [..n-1].
    let mut chunks: Vec<Vec<usize>> = Vec::with_capacity(splits.len() + 1);
    let mut start = 0usize;
    for &s in &splits {
        chunks.push((start..=s).collect());
        start = s + 1;
    }
    chunks.push((start..n).collect());
    debug_assert!(chunks.iter().all(|c| !c.is_empty()));
    // 6. Skew gate: with few strict-gap candidates the achieved
    //    chunking can carry a straggler (Q18 SF=10: 58 RGs → ~14
    //    candidates → chunks of 2..7 RGs; the 1.7× straggler measured
    //    +18% wall vs the stock two-phase plan). The single-phase
    //    rewrite only pays when the parallel sections stay balanced —
    //    decline when the largest chunk exceeds the ideal share by
    //    more than `EMAT_RANGE_AGG_MAX_SKEW` (default 1.25×). Scale-
    //    invariant: dense candidate sets (SF=100: 573 RGs) land within
    //    ±2 RGs of ideal and pass; sparse ones decline naturally.
    let max_skew: f64 = std::env::var("EMAT_RANGE_AGG_MAX_SKEW")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v: &f64| v.is_finite() && *v >= 1.0)
        .unwrap_or(1.25);
    let ideal = total_rows as f64 / chunks.len() as f64;
    let max_chunk_rows = chunks
        .iter()
        .map(|c| c.iter().map(|&j| rg_rows[j]).sum::<usize>())
        .max()
        .unwrap_or(0);
    if max_chunk_rows as f64 > ideal * max_skew {
        return None;
    }
    Some(chunks)
}

/// Walk through batching pass-throughs (they neither reorder rows
/// across partitions nor change partitioning) to the scan.
#[allow(deprecated)] // CoalesceBatchesExec still appears in DF 53 plans
fn unwrap_to_emat_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<&EmatixFastParquetExec> {
    let any = plan.as_any();
    if let Some(scan) = any.downcast_ref::<EmatixFastParquetExec>() {
        return Some(scan);
    }
    if any.is::<datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec>() {
        return unwrap_to_emat_scan(plan.children().first()?);
    }
    // CooperativeExec wraps leaf scans in DF 53 (yield points).
    if plan.name() == "CooperativeExec" {
        return unwrap_to_emat_scan(plan.children().first()?);
    }
    None
}

/// See module docs.
#[derive(Debug, Default)]
pub struct ClusteredSinglePhaseAggRule;

impl PhysicalOptimizerRule for ClusteredSinglePhaseAggRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !enabled() {
            return Ok(plan);
        }
        let trace = std::env::var_os("EMAT_RANGE_AGG_TRACE").is_some();
        let rewritten = plan.transform_up(|node| {
            // ---- match FinalPartitioned ← Repartition(Hash) ← Partial ← scan
            let Some(final_agg) = node.as_any().downcast_ref::<AggregateExec>() else {
                return Ok(Transformed::no(node));
            };
            if !matches!(final_agg.mode(), AggregateMode::FinalPartitioned) {
                return Ok(Transformed::no(node));
            }
            // Exactly one group column (the cluster key).
            if final_agg.group_expr().expr().len() != 1 {
                return Ok(Transformed::no(node));
            }
            let Some(repart) = final_agg.input().as_any().downcast_ref::<RepartitionExec>() else {
                return Ok(Transformed::no(node));
            };
            if !matches!(
                repart.partitioning(),
                datafusion::physical_expr::Partitioning::Hash(_, _)
            ) {
                return Ok(Transformed::no(node));
            }
            let Some(partial) = repart.input().as_any().downcast_ref::<AggregateExec>() else {
                return Ok(Transformed::no(node));
            };
            if !matches!(partial.mode(), AggregateMode::Partial) {
                return Ok(Transformed::no(node));
            }
            let Some(scan) = unwrap_to_emat_scan(partial.input()) else {
                if trace {
                    eprintln!(
                        "[range_agg] DECLINE: Partial input is not an Emat scan (got {})",
                        partial.input().name()
                    );
                }
                return Ok(Transformed::no(node));
            };
            // The scan must be a plain dense scan: no pushed filter or
            // runtime sideband (those drop rows — still key-disjoint,
            // but excluded from the first surface), and the group key
            // must resolve to a projected column.
            if scan.filter().is_some() || scan.runtime_sideband().is_some() {
                if trace {
                    eprintln!(
                        "[range_agg] DECLINE {}: scan carries a filter/sideband",
                        scan.path()
                    );
                }
                return Ok(Transformed::no(node));
            }
            // Resolve the group key to the scan's FILE column index.
            // Partial's group expr is against the scan output schema, so
            // a Column reference's index maps through scan.projection().
            let group_col = partial.group_expr().expr().first().cloned();
            let Some((gexpr, _gname)) = group_col else {
                return Ok(Transformed::no(node));
            };
            let Some(col) = gexpr
                .as_any()
                .downcast_ref::<datafusion::physical_expr::expressions::Column>()
            else {
                if trace {
                    eprintln!(
                        "[range_agg] DECLINE {}: group key is not a bare column",
                        scan.path()
                    );
                }
                return Ok(Transformed::no(node));
            };
            let Some(&file_col) = scan.projection().get(col.index()) else {
                if trace {
                    eprintln!(
                        "[range_agg] DECLINE {}: group col idx {} outside scan projection",
                        scan.path(),
                        col.index()
                    );
                }
                return Ok(Transformed::no(node));
            };
            // ---- prove clustering from RG stats + plan disjoint chunks
            let path = std::path::PathBuf::from(scan.path());
            let n_rgs: usize = scan.assignments().iter().map(|c| c.len()).sum();
            // Re-chunk over ALL row groups in file order (the existing
            // assignment is contiguous chunking; we rebuild from scratch).
            // ONE footer parse for the whole sweep — the per-RG bridge
            // calls re-parse the footer per call on files below the
            // handle cache's row-group threshold (58-RG SF=10 lineitem:
            // 116 parses = +23 ms plan time, measured on Q18).
            let Ok((mut ranges, mut rg_rows)) =
                crate::ematix_parquet_bridge::rg_i64_ranges_and_counts(&path, file_col)
            else {
                if trace {
                    eprintln!(
                        "[range_agg] DECLINE {}: RG stats sweep failed for col {file_col}",
                        scan.path()
                    );
                }
                return Ok(Transformed::no(node));
            };
            if ranges.len() < n_rgs {
                if trace {
                    eprintln!(
                        "[range_agg] DECLINE {}: footer has {} RGs < {n_rgs} assigned",
                        scan.path(),
                        ranges.len()
                    );
                }
                return Ok(Transformed::no(node));
            }
            ranges.truncate(n_rgs);
            rg_rows.truncate(n_rgs);
            let target = config.execution.target_partitions.max(2);
            let Some(chunks) = plan_disjoint_chunks(&ranges, &rg_rows, target) else {
                if trace {
                    eprintln!(
                        "[range_agg] DECLINE {}: key col {file_col} not provably clustered",
                        scan.path()
                    );
                }
                return Ok(Transformed::no(node));
            };
            if trace {
                eprintln!(
                    "[range_agg] FIRE {}: col {file_col}, {} disjoint chunks over {n_rgs} RGs",
                    scan.path(),
                    chunks.len()
                );
            }
            // ---- rebuild: re-chunked scan + SinglePartitioned agg
            //
            // Stage 2 (2026-07-02): the re-chunked scan CLAIMS
            // `Partitioning::Hash([group_key], n)`. The chunks are
            // key-disjoint, not hash-bucketed — but hash-partitioned
            // distribution only promises a consumer that every
            // distinct key's rows land in exactly one partition, which
            // the strict-gap chunking guarantees. The claim lets the
            // EnforceDistribution re-run below accept the scan as the
            // SinglePartitioned agg's input WITHOUT re-inserting the
            // full-input hash shuffle (Q18 SF=100: 600M rows /
            // ~9.6 GB re-shuffled for a requirement the chunking
            // already satisfies row-wise).
            let new_scan = scan.with_assignments_claiming_hash(chunks, vec![gexpr]);
            // SinglePartitioned: each partition aggregates its own key
            // range to completion (final values, no merge). Built from
            // the PARTIAL's expressions (they are against the scan
            // schema); its output schema equals the Final's.
            let single = AggregateExec::try_new(
                AggregateMode::SinglePartitioned,
                partial.group_expr().clone(),
                partial.aggr_expr().to_vec(),
                partial.filter_expr().to_vec(),
                new_scan,
                Arc::clone(&partial.input_schema()),
            )?;
            if single.schema() != final_agg.schema() {
                // Schema drift (alias/name mismatch) — decline rather
                // than risk a parent re-binding error.
                if trace {
                    eprintln!("[range_agg] DECLINE: SinglePartitioned schema != Final schema");
                }
                return Ok(Transformed::no(node));
            }
            // Stage 2 cap: the agg maps its input partitioning onto
            // its output, so without a cap the false hash claim would
            // leak upward — a partitioned HashJoin above could elide
            // its build-side repartition and pair hash-partitions with
            // range-chunks → WRONG RESULTS. The reset node advertises
            // UnknownPartitioning(n) (and fresh equivalences), so
            // EnforceDistribution re-satisfies any downstream hash
            // requirement by repartitioning the agg's OUTPUT — for
            // Q18 that repartition sits above the `HAVING sum > 300`
            // filter: ~10k rows instead of 600M.
            Ok(Transformed::yes(Arc::new(
                crate::partition_claim_reset_exec::PartitionClaimResetExec::new(Arc::new(single)),
            )))
        })?;

        if !rewritten.transformed {
            return Ok(rewritten.data);
        }
        // Σ.BS repair pattern: the new agg's partitioning is
        // UnknownPartitioning(chunks); re-satisfy any downstream hash
        // requirement.
        EnforceDistribution::new().optimize(rewritten.data, config)
    }

    fn name(&self) -> &str {
        "ematix_flow_clustered_single_phase_agg"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Write the shared multi-row-group fixture: 1000 rows, `k =
    /// row/5` (5 rows per key, keys 0..199), `v = 1`, k-sorted,
    /// `rg_size` rows per row group.
    ///
    /// - `rg_size = 123` → 9 RGs whose boundaries cut inside keys at
    ///   some seams (spans) and fall on strict gaps at others — the
    ///   boundary-span e2e shape.
    /// - `rg_size = 125` (divisible by 5) → 8 RGs, EVERY boundary a
    ///   strict gap — with target_partitions=4 the planner chunks to
    ///   exactly 4 partitions, which is the shuffle-free Stage 2
    ///   shape (chunk count == target; `EnforceDistribution` adds a
    ///   hash repartition on a satisfied-but-underpartitioned child
    ///   when chunks < target, mirroring SF=100's 573 RGs → 14
    ///   chunks == 14 targets).
    ///
    /// Returns the fixture dir (caller removes) and the parquet path.
    fn write_clustered_fixture(
        tag: &str,
        rg_size: usize,
    ) -> (std::path::PathBuf, std::path::PathBuf) {
        use arrow_array::{Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use datafusion::parquet::arrow::ArrowWriter;
        use datafusion::parquet::file::properties::WriterProperties;

        let n = 1000i64;
        let keys: Vec<i64> = (0..n).map(|i| i / 5).collect();
        let vals: Vec<i64> = (0..n).map(|_| 1).collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(Int64Array::from(vals)),
            ],
        )
        .unwrap();
        let dir = std::env::temp_dir().join(format!("range_agg_{tag}_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("t.parquet");
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(rg_size))
            .build();
        let f = std::fs::File::create(&path).unwrap();
        let mut w = ArrowWriter::try_new(f, schema, Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        (dir, path)
    }

    /// Session with default features + the rule under test, `t`
    /// registered over the fixture parquet via the ematix provider.
    fn ctx_over_fixture(
        path: &std::path::Path,
        config: datafusion::prelude::SessionConfig,
    ) -> datafusion::prelude::SessionContext {
        use datafusion::execution::session_state::SessionStateBuilder;
        use datafusion::prelude::SessionContext;
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(ClusteredSinglePhaseAggRule))
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table(
            "t",
            Arc::new(
                crate::ematix_fast_parquet::EmatixFastParquetTableProvider::try_new(
                    path.to_string_lossy().to_string(),
                )
                .unwrap(),
            ),
        )
        .unwrap();
        ctx
    }

    /// E2E: a multi-row-group parquet clustered on `k`, including a
    /// key that SPANS a row-group boundary (k=199 ends RG 1 and starts
    /// RG 2). The rule must fire (strict gaps exist elsewhere), place
    /// no split inside the spanning key, and produce exactly the same
    /// sums as a stock plan would — the spanning key's rows must land
    /// in ONE partition.
    #[tokio::test]
    async fn e2e_clustered_group_by_is_exact_across_boundary_spans() {
        use arrow_array::Int64Array;
        use datafusion::prelude::SessionConfig;

        let (dir, path) = write_clustered_fixture("e2e", 123);
        let ctx = ctx_over_fixture(&path, SessionConfig::new().with_target_partitions(4));

        let df = ctx
            .sql("SELECT k, sum(v) AS s FROM t GROUP BY k")
            .await
            .unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let rendered = format!("{plan:?}");
        assert!(
            rendered.contains("SinglePartitioned"),
            "rule must fire on the clustered key:\n{rendered}"
        );

        let batches = df.collect().await.unwrap();
        let mut got: Vec<(i64, i64)> = Vec::new();
        for b in &batches {
            let k = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let s = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..b.num_rows() {
                got.push((k.value(i), s.value(i)));
            }
        }
        got.sort_unstable();
        // Every key 0..199 must appear EXACTLY once with sum 5 — a
        // boundary-spanning key split across partitions would surface
        // as two rows (e.g. (24, 3) and (24, 2)).
        let expect: Vec<(i64, i64)> = (0..200).map(|k| (k, 5)).collect();
        assert_eq!(got, expect, "cluster-key sums must be exact and unsplit");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Stage 2 plan pin (a): NO RepartitionExec between the re-chunked
    /// scan and the SinglePartitioned agg — the scan's
    /// `Partitioning::Hash` claim satisfies the agg's distribution
    /// requirement, so the rule's EnforceDistribution re-run must not
    /// re-insert the full-input shuffle. The claim must also be capped
    /// by a PartitionClaimResetExec directly above the agg.
    #[tokio::test]
    async fn stage2_no_repartition_between_chunked_scan_and_agg() {
        use datafusion::prelude::SessionConfig;

        let (dir, path) = write_clustered_fixture("stage2_nosh", 125);
        let ctx = ctx_over_fixture(&path, SessionConfig::new().with_target_partitions(4));

        let df = ctx
            .sql("SELECT k, sum(v) AS s FROM t GROUP BY k")
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let rendered = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string();
        let lines: Vec<&str> = rendered.lines().collect();
        let reset_idx = lines
            .iter()
            .position(|l| l.contains("PartitionClaimResetExec"))
            .unwrap_or_else(|| panic!("reset cap missing:\n{rendered}"));
        let agg_idx = lines
            .iter()
            .position(|l| l.contains("AggregateExec") && l.contains("SinglePartitioned"))
            .unwrap_or_else(|| panic!("SinglePartitioned agg missing:\n{rendered}"));
        let scan_idx = lines
            .iter()
            .position(|l| l.contains("EmatixFastParquetExec"))
            .unwrap_or_else(|| panic!("chunked scan missing:\n{rendered}"));
        assert!(
            reset_idx < agg_idx && agg_idx < scan_idx,
            "expected reset → agg → scan sandwich:\n{rendered}"
        );
        assert!(
            lines[reset_idx..=scan_idx]
                .iter()
                .all(|l| !l.contains("RepartitionExec")),
            "the input shuffle was re-inserted inside the sandwich:\n{rendered}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Stage 2 plan pin (b) + leak catcher: when a PARTITIONED hash
    /// join consumes the rewritten agg's output, the false hash claim
    /// must NOT leak past the reset cap — EnforceDistribution must
    /// hash-repartition the agg's OUTPUT (above the reset node) and
    /// the join's other side must get its own hash repartition, and
    /// the join values must be exact. If the claim leaked, the join
    /// would pair genuinely hash-partitioned `d` partitions with
    /// range-chunked agg partitions and silently drop matches.
    #[tokio::test]
    async fn stage2_partitioned_join_repartitions_agg_output_not_input() {
        use arrow_array::{Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionConfig;

        let (dir, path) = write_clustered_fixture("stage2_join", 125);
        // Force Partitioned hash-join mode (no CollectLeft shortcut on
        // tiny inputs) so the join actually demands hash distribution
        // on both children.
        let mut config = SessionConfig::new().with_target_partitions(4);
        config
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold = 0;
        config
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold_rows = 0;
        let ctx = ctx_over_fixture(&path, config);

        // Dimension side: every key 0..199 with a name.
        let d_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let d_batch = RecordBatch::try_new(
            d_schema.clone(),
            vec![
                Arc::new(Int64Array::from((0..200).collect::<Vec<i64>>())),
                Arc::new(StringArray::from(
                    (0..200).map(|k| format!("n{k}")).collect::<Vec<String>>(),
                )),
            ],
        )
        .unwrap();
        ctx.register_table(
            "d",
            Arc::new(MemTable::try_new(d_schema, vec![vec![d_batch]]).unwrap()),
        )
        .unwrap();

        let df = ctx
            .sql(
                "SELECT a.k, a.s, d.name \
                 FROM (SELECT k, sum(v) AS s FROM t GROUP BY k) a \
                 JOIN d ON a.k = d.k",
            )
            .await
            .unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let rendered = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string();
        let lines: Vec<&str> = rendered.lines().collect();

        // The rewrite fired and is capped.
        let reset_idx = lines
            .iter()
            .position(|l| l.contains("PartitionClaimResetExec"))
            .unwrap_or_else(|| panic!("reset cap missing:\n{rendered}"));
        let join_idx = lines
            .iter()
            .position(|l| l.contains("HashJoinExec") && l.contains("mode=Partitioned"))
            .unwrap_or_else(|| panic!("expected a Partitioned hash join:\n{rendered}"));
        let scan_idx = lines
            .iter()
            .position(|l| l.contains("EmatixFastParquetExec"))
            .unwrap_or_else(|| panic!("chunked scan missing:\n{rendered}"));
        assert!(join_idx < reset_idx && reset_idx < scan_idx);

        // (b) The agg side IS repartitioned — ABOVE the reset node
        // (i.e. on the agg's ~200-row output, not its input)…
        assert!(
            lines[join_idx..reset_idx]
                .iter()
                .any(|l| l.contains("RepartitionExec") && l.contains("Hash")),
            "agg output must be hash-repartitioned above the reset cap:\n{rendered}"
        );
        // …and never between the reset cap and the chunked scan.
        assert!(
            lines[reset_idx..=scan_idx]
                .iter()
                .all(|l| !l.contains("RepartitionExec")),
            "the input shuffle was re-inserted inside the sandwich:\n{rendered}"
        );
        // Leak pin: the join's OTHER side keeps its own hash
        // repartition (two hash repartitions in the plan: one per
        // join input).
        let hash_reparts = lines
            .iter()
            .filter(|l| l.contains("RepartitionExec") && l.contains("Hash"))
            .count();
        assert!(
            hash_reparts >= 2,
            "expected a hash repartition on BOTH join inputs, found \
             {hash_reparts}:\n{rendered}"
        );

        // Value gate: every key exactly once, sum 5, correct name — a
        // partition mis-pairing would drop rows.
        let batches = df.collect().await.unwrap();
        let mut got: Vec<(i64, i64, String)> = Vec::new();
        for b in &batches {
            let k = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let s = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            let nm = b.column(2).as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..b.num_rows() {
                got.push((k.value(i), s.value(i), nm.value(i).to_string()));
            }
        }
        got.sort();
        let expect: Vec<(i64, i64, String)> = (0..200).map(|k| (k, 5, format!("n{k}"))).collect();
        assert_eq!(
            got, expect,
            "join over the capped agg must be exact — a leak of the \
             hash claim would silently drop matches"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn chunks_require_monotone_ranges() {
        // Overlapping RG ranges → not clustered → None.
        let ranges = vec![Some((0, 100)), Some((50, 150)), Some((151, 200))];
        let rows = vec![10, 10, 10];
        assert!(plan_disjoint_chunks(&ranges, &rows, 2).is_none());
        // Missing stats → None.
        let ranges = vec![Some((0, 100)), None, Some((151, 200))];
        assert!(plan_disjoint_chunks(&ranges, &rows, 2).is_none());
    }

    #[test]
    fn chunks_split_only_at_strict_gaps() {
        // Boundary 0-1 is a SPAN (max == min: key 100 in both RGs);
        // boundaries 1-2 and 2-3 are strict gaps. The split avoids the
        // span and lands at the balanced strict gap (after RG 1).
        let ranges = vec![
            Some((0, 100)),
            Some((100, 200)),
            Some((201, 300)),
            Some((301, 400)),
        ];
        let rows = vec![10, 10, 10, 10];
        let chunks = plan_disjoint_chunks(&ranges, &rows, 2).expect("strict gaps exist");
        assert_eq!(chunks, vec![vec![0, 1], vec![2, 3]]);
        // No strict gap anywhere → None.
        let ranges = vec![Some((0, 100)), Some((100, 200)), Some((200, 300))];
        let rows3 = vec![10, 10, 10];
        assert!(plan_disjoint_chunks(&ranges, &rows3, 3).is_none());
    }

    /// Skew gate: when the strict-gap candidates force a badly
    /// unbalanced chunking (Q18 SF=10: 58 RGs → ~14 candidates →
    /// chunks of 2..7 RGs, a 1.7× straggler that measured +18% wall),
    /// the planner must DECLINE rather than trade the shuffle for a
    /// straggler tail. Candidates here only allow one split at 1/6 of
    /// the rows — the bigger chunk is 5× ideal.
    #[test]
    fn chunks_decline_on_straggler_skew() {
        // 6 RGs; ONLY the boundary after rg0 is a strict gap (all
        // others span). target 2 parts → chunks [1 RG | 5 RGs].
        let ranges = vec![
            Some((0, 10)),
            Some((11, 20)), // strict gap after rg0
            Some((20, 30)), // span
            Some((30, 40)), // span
            Some((40, 50)), // span
            Some((50, 60)), // span
        ];
        let rows = vec![10usize; 6];
        assert!(
            plan_disjoint_chunks(&ranges, &rows, 2).is_none(),
            "a 5x-ideal straggler chunk must decline"
        );
    }

    #[test]
    fn chunks_balance_by_row_counts() {
        // 6 RGs, all boundaries strict, equal rows → 3 target parts
        // split at the 1/3 and 2/3 row offsets.
        let ranges: Vec<Option<(i64, i64)>> =
            (0..6).map(|i| Some((i * 100, i * 100 + 50))).collect();
        let rows = vec![10usize; 6];
        let chunks = plan_disjoint_chunks(&ranges, &rows, 3).unwrap();
        assert_eq!(chunks, vec![vec![0, 1], vec![2, 3], vec![4, 5]]);
    }
}
