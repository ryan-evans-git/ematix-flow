//! v2 S1.2 — grouping-set recognizer (planner-interception core for
//! Phase GS). See `docs/PHASE_V2_S1_GROUPING_SETS.md`.
//!
//! This slice is the **recognizer**: the pure, testable core of the
//! physical-optimizer rule that will swap DataFusion's generic multi-set
//! `AggregateExec` for the native `FusedGroupingSetAggregateExec`. It
//! reads DF53's grouping-set physical shape *exactly* as pinned in
//! §4.0 (via the `gs_plan_probe` example):
//!
//!   * the multi-set node is the **Partial** `AggregateExec` — the one
//!     whose `PhysicalGroupBy::groups().len() > 1`;
//!   * each set is a null-mask `Vec<bool>` where **`true` = the column is
//!     rolled up** (absent) in that set — so `present(i) == !mask[i]`;
//!   * the grouping id DF expects downstream puts the **leftmost** group
//!     column in the **high** bit:
//!     `id(set) = Σ_{i : mask[i]} 2^(n-1-i)`.
//!
//! The recognizer extracts that structure and applies the decline gates
//! (`EMAT_GROUPING_SETS_FUSED=0` opt-out; `> GS_MAX_SETS` cap — the CUBE
//! blow-up guard). The operator itself and the rule that installs it land
//! in the next slice; until then nothing is rewired, so this is inert on
//! the query path.

use std::sync::Arc;

use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};

/// Default cap on the number of grouping sets we take over natively — the
/// CUBE blow-up guard (`CUBE(k)` = 2^k sets). Above this the recognizer
/// declines and DataFusion's generic exec runs unchanged, rather than
/// building an unbounded number of per-set hash tables. Override with
/// `EMAT_GS_MAX_SETS`. Default 16 ⇒ ROLLUP up to 15 cols, CUBE up to 4.
pub const GS_MAX_SETS_DEFAULT: usize = 16;

/// The resolved `GS_MAX_SETS` cap (`EMAT_GS_MAX_SETS`, default
/// [`GS_MAX_SETS_DEFAULT`]).
pub fn gs_max_sets() -> usize {
    crate::flags::usize_or("EMAT_GS_MAX_SETS", GS_MAX_SETS_DEFAULT)
}

/// Whether native fused grouping-set execution is enabled. Tri-state
/// `EMAT_GROUPING_SETS_FUSED` (matches the `EMAT_SCALAR_AGG_BOOST`
/// default-ON convention): `=0` forces the DataFusion path (A/B +
/// debugging), `=1`/unset keep the fused path.
pub fn fused_grouping_sets_enabled() -> bool {
    crate::flags::tri_state("EMAT_GROUPING_SETS_FUSED").unwrap_or(true)
}

/// The grouping-set structure extracted from a multi-set `AggregateExec`,
/// enough for the operator to emit per-set rows with the correct
/// `__grouping_id` and rolled-up-column nulling.
#[derive(Debug, Clone)]
pub struct RecognizedGroupingSet {
    /// The full group-column universe `[c0, …, c_{n-1}]` (expr + output
    /// alias), in DF's declared order — `c0` is the high grouping-id bit.
    pub universe: Vec<(Arc<dyn PhysicalExpr>, String)>,
    /// One null-mask per grouping set. `mask[i] == true` ⇒ column `i` is
    /// rolled up (absent) in this set; `false` ⇒ present. Verbatim from
    /// `PhysicalGroupBy::groups()` (§4.0 polarity).
    pub set_masks: Vec<Vec<bool>>,
    /// The aggregate expressions to compute per set (shared across sets).
    pub aggs: Vec<Arc<AggregateFunctionExpr>>,
}

impl RecognizedGroupingSet {
    /// Number of group columns in the universe (`n`).
    pub fn universe_len(&self) -> usize {
        self.universe.len()
    }

    /// The `__grouping_id` integer for set `set_idx`, in DF53's exact
    /// convention (§4.0): the leftmost universe column is the high bit, and
    /// a bit is set iff that column is rolled up (`mask[i] == true`).
    /// `id = Σ_{i : mask[i]} 2^(n-1-i)`.
    pub fn grouping_id_of(&self, set_idx: usize) -> u64 {
        grouping_id_for_mask(&self.set_masks[set_idx], self.universe_len())
    }

    /// The column indices *present* (not rolled up) in set `set_idx`.
    pub fn present_cols(&self, set_idx: usize) -> Vec<usize> {
        self.set_masks[set_idx]
            .iter()
            .enumerate()
            .filter_map(|(i, rolled_up)| (!*rolled_up).then_some(i))
            .collect()
    }
}

/// Pure §4.0 grouping-id: leftmost group column (index 0) is the HIGH bit,
/// a bit is set iff that column is rolled up. `id = Σ_{i : mask[i]} 2^(n-1-i)`.
pub fn grouping_id_for_mask(mask: &[bool], n: usize) -> u64 {
    mask.iter()
        .enumerate()
        .filter(|(_, rolled_up)| **rolled_up)
        .map(|(i, _)| 1u64 << (n - 1 - i))
        .sum()
}

/// Recognize a multi-set `AggregateExec` (the **Partial** node of DF's
/// grouping-set lowering — §4.0) as a candidate for native fused
/// execution, extracting its set masks + aggregate exprs.
///
/// Returns `None` (⇒ leave DataFusion's plan in place) when:
///   * the fused path is disabled (`EMAT_GROUPING_SETS_FUSED=0`);
///   * this is not a multi-set node (`groups().len() <= 1`) — the disjoint
///     guard that keeps ordinary single-group aggregates on their own
///     fused path untouched;
///   * the set count exceeds [`gs_max_sets`] (the CUBE blow-up guard).
///
/// Correctness-first: this only *recognizes* the grouping-set structure.
/// Aggregate-shape validation (which aggregates the operator can compute)
/// and the actual operator swap are the next slice; a caller must still
/// gate on aggregate support before rewriting.
pub fn recognize_grouping_set(agg: &AggregateExec) -> Option<RecognizedGroupingSet> {
    if !fused_grouping_sets_enabled() {
        return None;
    }
    // The multi-set masks live on the input-reading phase (§4.0): `Partial`
    // when DF two-phases through a repartition, or `Single`/`SinglePartitioned`
    // when the input is already single-partition. The `Final`/`FinalPartitioned`
    // re-aggregation phases carry a single mask over `[cols…, __grouping_id]`,
    // so `masks.len() > 1` already excludes them; the explicit reject below
    // is belt-and-suspenders so we never rewrite a re-aggregation node.
    let group_by = agg.group_expr();
    let masks = group_by.groups();
    if masks.len() <= 1 {
        return None;
    }
    if matches!(
        agg.mode(),
        AggregateMode::Final | AggregateMode::FinalPartitioned
    ) {
        return None;
    }
    if masks.len() > gs_max_sets() {
        // Loud decline — "TPC-DS runs native" must not silently mean
        // "except the wide cubes" (§7 CUBE blow-up).
        if crate::flags::present("EMAT_DEBUG") {
            eprintln!(
                "[grouping_sets] declined: {} sets > GS_MAX_SETS {} — DataFusion generic exec retained",
                masks.len(),
                gs_max_sets()
            );
        }
        return None;
    }

    let universe: Vec<(Arc<dyn PhysicalExpr>, String)> = group_by
        .expr()
        .iter()
        .map(|(e, name)| (Arc::clone(e), name.clone()))
        .collect();
    let n = universe.len();
    // Every mask must cover the full universe (DF invariant); bail rather
    // than mis-index if that ever changes.
    if masks.iter().any(|m| m.len() != n) {
        return None;
    }

    Some(RecognizedGroupingSet {
        universe,
        set_masks: masks.to_vec(),
        aggs: agg.aggr_expr().to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;

    // ---- pure grouping-id math (the §4.0 convention) ----

    #[test]
    fn grouping_id_leftmost_col_is_high_bit() {
        // n = 2, universe [a, b]: a is bit 1 (value 2), b is bit 0 (value 1).
        assert_eq!(grouping_id_for_mask(&[false, false], 2), 0); // (a,b) both present
        assert_eq!(grouping_id_for_mask(&[true, false], 2), 2); // a rolled up
        assert_eq!(grouping_id_for_mask(&[false, true], 2), 1); // b rolled up
        assert_eq!(grouping_id_for_mask(&[true, true], 2), 3); // grand total
    }

    #[test]
    fn grouping_id_three_cols() {
        // n = 3, universe [a, b, c]: bits a=4, b=2, c=1.
        assert_eq!(grouping_id_for_mask(&[false, false, false], 3), 0);
        assert_eq!(grouping_id_for_mask(&[true, false, false], 3), 4);
        assert_eq!(grouping_id_for_mask(&[false, true, false], 3), 2);
        assert_eq!(grouping_id_for_mask(&[false, false, true], 3), 1);
        assert_eq!(grouping_id_for_mask(&[true, true, true], 3), 7);
    }

    // ---- recognizer against real DF53 physical plans ----

    async fn ctx() -> SessionContext {
        let ctx = crate::preset::session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("v", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("x"), Some("y")])),
                Arc::new(StringArray::from(vec![Some("p"), None])),
                Arc::new(Int64Array::from(vec![1_i64, 2])),
            ],
        )
        .unwrap();
        ctx.register_table(
            "t",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
        ctx
    }

    /// Find the first multi-set `AggregateExec` (groups().len() > 1) in the
    /// physical tree — the Partial node DF emits for grouping sets.
    fn find_multiset(plan: &dyn ExecutionPlan) -> Option<&AggregateExec> {
        // Not recursive-into-children via return of a borrowed ref across a
        // loop cleanly, so collect by walking with an explicit stack.
        let mut stack: Vec<&dyn ExecutionPlan> = vec![plan];
        while let Some(node) = stack.pop() {
            if let Some(agg) = node.as_any().downcast_ref::<AggregateExec>() {
                if agg.group_expr().groups().len() > 1 {
                    return Some(agg);
                }
            }
            for c in node.children() {
                stack.push(c.as_ref());
            }
        }
        None
    }

    async fn physical(ctx: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
        ctx.sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn recognizes_rollup_three_sets_with_correct_masks() {
        // recognize_grouping_set reads EMAT_* env; hold the lock so the
        // env-mutating decline tests can't race the read (house rule).
        let _guard = crate::flags::EMAT_ENV_TEST_LOCK.lock().await;
        let ctx = ctx().await;
        let plan = physical(&ctx, "SELECT a, b, sum(v) FROM t GROUP BY ROLLUP(a, b)").await;
        let agg = find_multiset(plan.as_ref()).expect("rollup must have a multi-set Partial agg");
        let rec = recognize_grouping_set(agg).expect("recognizer should accept ROLLUP(a,b)");

        assert_eq!(rec.universe_len(), 2);
        assert_eq!(rec.set_masks.len(), 3, "ROLLUP(a,b) = 3 sets");
        // Every set's grouping id + present cols must decode per §4.0. The
        // 3 sets are (a,b), (a), () in some order — assert the SET of ids.
        let mut ids: Vec<u64> = (0..3).map(|i| rec.grouping_id_of(i)).collect();
        ids.sort();
        assert_eq!(
            ids,
            vec![0, 1, 3],
            "ROLLUP(a,b) ids = {{(a,b)=0,(a)=1,()=3}}"
        );
    }

    #[tokio::test]
    async fn recognizes_cube_four_sets() {
        let _guard = crate::flags::EMAT_ENV_TEST_LOCK.lock().await;
        let ctx = ctx().await;
        let plan = physical(&ctx, "SELECT a, b, sum(v) FROM t GROUP BY CUBE(a, b)").await;
        let agg = find_multiset(plan.as_ref()).expect("cube must have a multi-set Partial agg");
        let rec = recognize_grouping_set(agg).expect("recognizer should accept CUBE(a,b)");

        assert_eq!(rec.set_masks.len(), 4, "CUBE(a,b) = 4 sets");
        let mut ids: Vec<u64> = (0..4).map(|i| rec.grouping_id_of(i)).collect();
        ids.sort();
        assert_eq!(ids, vec![0, 1, 2, 3], "CUBE(a,b) ids = all of {{0,1,2,3}}");
    }

    #[tokio::test]
    async fn present_cols_track_mask_polarity() {
        let _guard = crate::flags::EMAT_ENV_TEST_LOCK.lock().await;
        let ctx = ctx().await;
        let plan = physical(&ctx, "SELECT a, b, sum(v) FROM t GROUP BY CUBE(a, b)").await;
        let agg = find_multiset(plan.as_ref()).unwrap();
        let rec = recognize_grouping_set(agg).unwrap();

        // For every set: present_cols == the non-rolled-up indices, and the
        // grouping id has exactly the rolled-up bits set.
        for i in 0..rec.set_masks.len() {
            let present = rec.present_cols(i);
            let rolled: Vec<usize> = (0..rec.universe_len())
                .filter(|c| !present.contains(c))
                .collect();
            let expect_id: u64 = rolled
                .iter()
                .map(|c| 1u64 << (rec.universe_len() - 1 - c))
                .sum();
            assert_eq!(rec.grouping_id_of(i), expect_id);
        }
    }

    #[tokio::test]
    async fn declines_ordinary_single_group_by() {
        let ctx = ctx().await;
        // Plain GROUP BY a — single set; recognizer must decline so the
        // existing single-group fused path is untouched (non-regression).
        let plan = physical(&ctx, "SELECT a, sum(v) FROM t GROUP BY a").await;
        assert!(
            find_multiset(plan.as_ref()).is_none(),
            "plain GROUP BY must not present a multi-set AggregateExec"
        );
    }

    #[tokio::test]
    async fn declines_when_over_gs_max_sets() {
        let ctx = ctx().await;
        let plan = physical(&ctx, "SELECT a, b, sum(v) FROM t GROUP BY CUBE(a, b)").await;
        let agg = find_multiset(plan.as_ref()).unwrap();
        // Temporarily cap at 2 sets: CUBE(a,b)=4 > 2 → decline.
        let _guard = crate::flags::EMAT_ENV_TEST_LOCK.lock().await;
        let key = "EMAT_GS_MAX_SETS";
        let prev = std::env::var(key).ok();
        unsafe { std::env::set_var(key, "2") };
        let got = recognize_grouping_set(agg);
        match prev {
            Some(v) => unsafe { std::env::set_var(key, v) },
            None => unsafe { std::env::remove_var(key) },
        }
        assert!(got.is_none(), "4 sets > GS_MAX_SETS=2 must decline");
    }

    #[tokio::test]
    async fn declines_when_opted_out() {
        let ctx = ctx().await;
        let plan = physical(&ctx, "SELECT a, b, sum(v) FROM t GROUP BY ROLLUP(a, b)").await;
        let agg = find_multiset(plan.as_ref()).unwrap();
        let _guard = crate::flags::EMAT_ENV_TEST_LOCK.lock().await;
        let key = "EMAT_GROUPING_SETS_FUSED";
        let prev = std::env::var(key).ok();
        unsafe { std::env::set_var(key, "0") };
        let got = recognize_grouping_set(agg);
        match prev {
            Some(v) => unsafe { std::env::set_var(key, v) },
            None => unsafe { std::env::remove_var(key) },
        }
        assert!(got.is_none(), "EMAT_GROUPING_SETS_FUSED=0 must decline");
    }
}
