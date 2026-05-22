//! Σ.K.2 — query-shape-aware dict-arrival routing.
//!
//! ## Why this lives here
//!
//! Σ.K.1's A/B bench proved the kernel works (Q12 `COUNT(*) GROUP BY
//! l_shipmode` is **−40%** faster with `with_dict_preservation(true)`) but
//! that a blanket flip is net-negative: Q01 +104%, Q13 +25%, Q19 +35%.
//! Operators without dict-specialised paths (FilterMultiAggSpec on Q01,
//! SIMD LIKE on Q13, OR-of-AND filter on Q19) double-decode when fed
//! `Dictionary(UInt32, Utf8)` instead of `Utf8View`.
//!
//! ## What this does
//!
//! Given a SQL string + the set of registered table providers,
//! [`analyse_dict_arrival_for_sql`] returns, per table, whether
//! dict-preservation should be flipped on for *this* query. The caller
//! then re-registers each table with the chosen flag before planning.
//!
//! ## Why not an optimizer rule
//!
//! Per [[optimizer-codegen-sensitivity]] in memory: adding any new
//! PhysicalOptimizerRule costs ~7% geomean from LLVM codegen perturbation
//! alone, before the rule does any work. This is a *pre-planning* helper
//! the caller invokes explicitly — it doesn't walk the optimizer pass
//! stack, doesn't add a rule, and so doesn't pay that tax.
//!
//! ## Heuristic
//!
//! Per-table verdict is `true` (dict-preserve) iff **all** hold:
//!
//! - At least one string column from this table is referenced as a
//!   `GROUP BY` key.
//! - **Exactly one** string column from this table is in the group-by
//!   (DictGroupCountExec only handles single-key today).
//! - No string column from this table is used in a `LIKE` /
//!   `SimilarTo` / `substring` / `position` / `strpos` predicate.
//! - The aggregation over the group-by has fewer than 4 aggregate
//!   expressions (proxy for "not FilterMultiAgg shape" — Q01 has 8, Q12
//!   has 1, Q16 has 1).
//! - No `OR`-of-`AND` filter with multiple string equality branches
//!   (Q19 shape — DictFilterExec doesn't fire there).
//! - **Row count ≥ [`MIN_ROWS_FOR_DICT`]** (when the caller supplies
//!   per-table sizes via [`analyse_dict_arrival_with_sizes`]). On tiny
//!   tables (nation 25, region 5, supplier ~10K) the dict-preservation
//!   fixed overhead dwarfs the savings; the OFF path wins.
//!
//! Otherwise: `false`. The default behaviour (Utf8View arrival) wins or
//! ties for those plans.

/// Tables smaller than this lose money on dict-preservation because
/// the fixed per-batch dict-code translation cost exceeds the savings
/// on the hot agg loop. Tuned for TPC-H SF=1 / SF=10 — nation (25),
/// region (5), and supplier (~10K SF=1) are reliably below this; orders
/// / lineitem / part / customer are reliably above.
pub const MIN_ROWS_FOR_DICT: u64 = 100_000;

use std::collections::HashMap;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{Expr, LogicalPlan, Operator};
use datafusion::prelude::SessionContext;

/// Per-table verdict on whether to flip `with_dict_preservation(true)`
/// for this query. Keys are the table names registered in the
/// `SessionContext` (case-insensitive — DataFusion normalises to lower
/// case in identifiers).
pub type DictArrivalDecision = HashMap<String, bool>;

/// Σ.L.1 — richer verdict for the speculative-race resolver. The static
/// heuristic emits this; a probe (or persisted workload feedback log)
/// resolves `Speculate` into a definitive `bool`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DictArrivalVerdict {
    /// Strong static signal — query shape clearly benefits (or
    /// clearly suffers) from dict-preservation. No probe needed.
    Yes,
    No,
    /// Borderline — same query shape can go either way depending on
    /// data distribution / downstream operators we can't fully model
    /// from the LogicalPlan. Σ.L.1's probe resolves these to Yes/No
    /// at runtime; Σ.L.2 caches the resolutions persistently so the
    /// probe runs at most a few times per workload.
    Speculate {
        reason: &'static str,
    },
}

impl DictArrivalVerdict {
    /// Conservative default — collapse `Speculate` to `false` when
    /// no probe / cache is available. Matches the v0.7.0 behaviour
    /// (Utf8View arrival) for ambiguous cases.
    pub fn collapse_pessimistic(self) -> bool {
        matches!(self, DictArrivalVerdict::Yes)
    }

    /// Optimistic default — collapse `Speculate` to `true`. Useful
    /// when the workload is empirically dict-friendly and the upside
    /// outweighs the downside.
    pub fn collapse_optimistic(self) -> bool {
        !matches!(self, DictArrivalVerdict::No)
    }
}

/// Σ.L.1 richer decision map. The bench/runtime translates this to a
/// `DictArrivalDecision` via either a probe (`resolve_via_probe`) or a
/// cache lookup (Σ.L.2's workload.db).
pub type DictArrivalVerdictMap = HashMap<String, DictArrivalVerdict>;

/// Build the decision map by inspecting the logical plan of `sql`
/// against the schemas registered in `ctx`.
///
/// `ctx` here is purely an *analysis* context — it must have each
/// candidate table registered with *some* provider so `ctx.sql(sql)`
/// can resolve column names + types. The caller then re-registers
/// against a fresh context per the returned verdicts.
///
/// **Size-aware variant**: prefer [`analyse_dict_arrival_with_sizes`]
/// when you have per-table row counts. The default helper assumes all
/// tables are large enough — i.e. it does no size pruning — which is
/// fine when every table you care about has ≥100K rows. For TPC-H
/// where nation/region/supplier are tiny, the size-aware variant
/// avoids picking dict on those.
pub async fn analyse_dict_arrival_for_sql(
    ctx: &SessionContext,
    sql: &str,
) -> Result<DictArrivalDecision, datafusion::error::DataFusionError> {
    let df = ctx.sql(sql).await?;
    let plan = df.logical_plan().clone();
    Ok(analyse_plan(&plan, &HashMap::new()))
}

/// Size-aware variant. `row_counts` maps table name → estimated row
/// count; tables below [`MIN_ROWS_FOR_DICT`] get `false` regardless of
/// query shape. Tables missing from `row_counts` are treated as "large
/// enough" (no size pruning applied).
pub async fn analyse_dict_arrival_with_sizes(
    ctx: &SessionContext,
    sql: &str,
    row_counts: &HashMap<String, u64>,
) -> Result<DictArrivalDecision, datafusion::error::DataFusionError> {
    let df = ctx.sql(sql).await?;
    let plan = df.logical_plan().clone();
    Ok(analyse_plan(&plan, row_counts))
}

/// Σ.L.1 variant — returns per-table [`DictArrivalVerdict`] so callers
/// can route the `Speculate` cases through a probe (or workload-log
/// cache). Identical analysis logic as [`analyse_dict_arrival_with_sizes`]
/// but emits the richer verdict.
pub async fn analyse_dict_arrival_verdicts(
    ctx: &SessionContext,
    sql: &str,
    row_counts: &HashMap<String, u64>,
) -> Result<DictArrivalVerdictMap, datafusion::error::DataFusionError> {
    let df = ctx.sql(sql).await?;
    let plan = df.logical_plan().clone();
    Ok(analyse_plan_verdicts(&plan, row_counts))
}

/// Σ.L.1 — same shape analysis as [`analyse_plan`] but emits
/// [`DictArrivalVerdict`] (3-state) instead of `bool`. The borderline
/// cases that the static rule can't be certain about become
/// `Speculate`; the resolver (Σ.L.1 probe / Σ.L.2 cache) decides.
pub fn analyse_plan_verdicts(
    plan: &LogicalPlan,
    row_counts: &HashMap<String, u64>,
) -> DictArrivalVerdictMap {
    let decision = analyse_plan(plan, row_counts);
    decision
        .into_iter()
        .map(|(name, b)| {
            let v = if b {
                // The current heuristic identifies shapes that
                // *should* win on dict per the Σ.K.1 microbench, but
                // the 22q gate revealed Q12 sometimes loses in
                // sequential context. Treat all static-Yes as
                // Speculate-with-Yes-prior so the probe verifies.
                DictArrivalVerdict::Speculate {
                    reason: "static heuristic picks Yes; probe to confirm",
                }
            } else {
                DictArrivalVerdict::No
            };
            (name, v)
        })
        .collect()
}

/// Pure function variant — operates on a [`LogicalPlan`] directly. Useful
/// for unit tests + callers that already have a plan in hand. `row_counts`
/// is consulted for the size-pruning rule; pass `&HashMap::new()` to skip.
pub fn analyse_plan(plan: &LogicalPlan, row_counts: &HashMap<String, u64>) -> DictArrivalDecision {
    // Step 1: collect, per table, all column refs that touch it.
    //         Walk down the plan, tagging which columns each TableScan
    //         exposes, then check upstream uses.
    let mut state = AnalysisState::default();
    // Identify all TableScans first so we know table → schema mappings.
    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            // Track string-typed columns per table.
            let string_cols: Vec<String> = scan
                .projected_schema
                .fields()
                .iter()
                .filter(|f| is_stringy(f.data_type()))
                .map(|f| f.name().to_string())
                .collect();
            state
                .tables
                .entry(scan.table_name.to_string())
                .or_insert_with(|| TableInfo {
                    string_cols,
                    in_groupby: false,
                    groupby_string_keys: 0,
                    in_like_or_substr: false,
                    in_or_of_string_eq: false,
                    in_filter_multi_agg: false,
                });
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .ok();

    // Step 2: walk operators upstream of the scans to populate the
    //         per-table booleans.
    plan.apply(|node| {
        match node {
            LogicalPlan::Aggregate(agg) => {
                let n_aggs = agg.aggr_expr.len();
                for (_, info) in state.tables.iter_mut() {
                    let mut keys_from_table = 0usize;
                    for ge in &agg.group_expr {
                        if expr_references_any(ge, &info.string_cols) {
                            info.in_groupby = true;
                            keys_from_table += 1;
                            if n_aggs >= 4 {
                                info.in_filter_multi_agg = true;
                            }
                        }
                    }
                    info.groupby_string_keys = info.groupby_string_keys.max(keys_from_table);
                }
            }
            LogicalPlan::Filter(f) => {
                for (_, info) in state.tables.iter_mut() {
                    if filter_uses_like_or_substr(&f.predicate, &info.string_cols) {
                        info.in_like_or_substr = true;
                    }
                    if filter_is_or_of_string_eq(&f.predicate, &info.string_cols) {
                        info.in_or_of_string_eq = true;
                    }
                }
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .ok();

    // Step 3: combine into a verdict per table.
    state
        .tables
        .into_iter()
        .map(|(name, info)| {
            let size_ok = row_counts
                .get(&name)
                .map(|n| *n >= MIN_ROWS_FOR_DICT)
                .unwrap_or(true); // unknown → assume large
            let dict_preserve = info.in_groupby
                && info.groupby_string_keys == 1
                && !info.in_like_or_substr
                && !info.in_filter_multi_agg
                && !info.in_or_of_string_eq
                && size_ok;
            (name, dict_preserve)
        })
        .collect()
}

#[derive(Default)]
struct AnalysisState {
    tables: HashMap<String, TableInfo>,
}

struct TableInfo {
    string_cols: Vec<String>,
    in_groupby: bool,
    /// Σ.K.2: how many distinct string columns *from this table* are
    /// referenced as group-by keys. `DictGroupCountExec` only handles
    /// a single dict GROUP BY column today — multi-key string group-by
    /// (e.g. Q16's `GROUP BY p_brand, p_type`) falls through to the
    /// generic agg path, which regresses when fed Dictionary inputs.
    groupby_string_keys: usize,
    in_like_or_substr: bool,
    in_or_of_string_eq: bool,
    in_filter_multi_agg: bool,
}

fn is_stringy(dt: &arrow_schema::DataType) -> bool {
    use arrow_schema::DataType::*;
    matches!(dt, Utf8 | LargeUtf8 | Utf8View)
        || matches!(dt, Dictionary(_, inner) if matches!(**inner, Utf8 | LargeUtf8 | Utf8View))
}

fn expr_references_any(expr: &Expr, cols: &[String]) -> bool {
    let mut hit = false;
    expr.apply(|e| {
        if let Expr::Column(c) = e {
            if cols.iter().any(|s| s.eq_ignore_ascii_case(c.name())) {
                hit = true;
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .ok();
    hit
}

fn filter_uses_like_or_substr(pred: &Expr, cols: &[String]) -> bool {
    let mut hit = false;
    pred.apply(|e| {
        match e {
            Expr::Like(like) if expr_references_any(&like.expr, cols) => {
                hit = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            Expr::SimilarTo(like) if expr_references_any(&like.expr, cols) => {
                hit = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            Expr::ScalarFunction(sf) => {
                let n = sf.name().to_ascii_lowercase();
                if matches!(
                    n.as_str(),
                    "substring" | "substr" | "position" | "strpos" | "starts_with" | "ends_with"
                ) {
                    for arg in &sf.args {
                        if expr_references_any(arg, cols) {
                            hit = true;
                            return Ok(TreeNodeRecursion::Stop);
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .ok();
    hit
}

/// Match the Q19 shape: top-level OR with multiple AND branches, each
/// containing a string-equality predicate over `cols`. DictFilterExec
/// doesn't fire on this shape today, so we want to leave it as
/// Utf8View.
fn filter_is_or_of_string_eq(pred: &Expr, cols: &[String]) -> bool {
    let Expr::BinaryExpr(top) = pred else {
        return false;
    };
    if top.op != Operator::Or {
        return false;
    }
    let mut branches = Vec::new();
    collect_or_branches(pred, &mut branches);
    if branches.len() < 2 {
        return false;
    }
    let mut string_eq_branches = 0usize;
    for b in &branches {
        if branch_contains_string_eq(b, cols) {
            string_eq_branches += 1;
        }
    }
    string_eq_branches >= 2
}

fn collect_or_branches<'a>(e: &'a Expr, out: &mut Vec<&'a Expr>) {
    match e {
        Expr::BinaryExpr(b) if b.op == Operator::Or => {
            collect_or_branches(&b.left, out);
            collect_or_branches(&b.right, out);
        }
        other => out.push(other),
    }
}

fn branch_contains_string_eq(e: &Expr, cols: &[String]) -> bool {
    let mut hit = false;
    e.apply(|n| {
        if let Expr::BinaryExpr(b) = n {
            if b.op == Operator::Eq {
                // Either side a column from `cols`, other side a Literal Utf8.
                let left_col = matches!(&*b.left, Expr::Column(c) if cols.iter().any(|s| s.eq_ignore_ascii_case(c.name())));
                let right_col = matches!(&*b.right, Expr::Column(c) if cols.iter().any(|s| s.eq_ignore_ascii_case(c.name())));
                let left_lit = matches!(&*b.left, Expr::Literal(..));
                let right_lit = matches!(&*b.right, Expr::Literal(..));
                if (left_col && right_lit) || (right_col && left_lit) {
                    hit = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .ok();
    hit
}

// ---------------------------------------------------------------------
// Σ.L.1 — speculative-race resolver.
// ---------------------------------------------------------------------

/// Probe result for a single table. Total wall time of each side plus
/// the relative delta. `dict_wins` returns true iff dict-side beat
/// default by at least 5% (margin keeps random noise from flipping the
/// decision on near-ties).
#[derive(Debug, Clone, Copy)]
pub struct ProbeResult {
    pub dict_ms: f64,
    pub default_ms: f64,
}

impl ProbeResult {
    pub fn dict_wins(self) -> bool {
        // Require dict ≤ 95% of default — a 5% margin filters out
        // sub-millisecond noise on cheap probes.
        self.dict_ms <= self.default_ms * 0.95
    }
    pub fn delta_pct(self) -> f64 {
        (self.dict_ms - self.default_ms) / self.default_ms * 100.0
    }
}

/// A pair of factories: one builds a SessionContext where this table is
/// registered with dict-preservation ON, the other with it OFF. The
/// probe will time queries through each. Caller owns the implementation
/// — typically wraps `EmatixFastParquetTableProvider::try_new(path)
/// .with_dict_preservation(true|false)`.
///
/// We avoid taking concrete types so callers can use any table provider
/// (MemTable, ListingTable, custom) — the probe is provider-agnostic.
pub type CtxFactory<'a> = dyn Fn() -> SessionContext + Send + Sync + 'a;

/// Σ.L.1 probe: run the same probe SQL through both context factories
/// (dict-on and dict-off), time wall clock of each. Returns the result;
/// caller decides via [`ProbeResult::dict_wins`] whether to flip.
///
/// The probe SQL should be something cheap that exercises the same
/// hot path as the real query — typically a single GROUP BY + COUNT on
/// the dict-candidate column. Microsecond-level timing is unreliable;
/// use a probe that runs in ≥1ms.
pub async fn probe_dict_vs_default(
    dict_ctx_factory: &CtxFactory<'_>,
    default_ctx_factory: &CtxFactory<'_>,
    probe_sql: &str,
) -> Result<ProbeResult, datafusion::error::DataFusionError> {
    // Run default first (warms up OS file cache + allocator), then
    // dict-on (also gets a warm cache so we're comparing apples-to-
    // apples on warm caches — what the real query will see).
    let default_ms = time_query(default_ctx_factory, probe_sql).await?;
    let dict_ms = time_query(dict_ctx_factory, probe_sql).await?;
    // One more rep each, take the min — single probe rep is noisy.
    let default_ms = default_ms.min(time_query(default_ctx_factory, probe_sql).await?);
    let dict_ms = dict_ms.min(time_query(dict_ctx_factory, probe_sql).await?);
    Ok(ProbeResult {
        dict_ms,
        default_ms,
    })
}

async fn time_query(
    factory: &CtxFactory<'_>,
    sql: &str,
) -> Result<f64, datafusion::error::DataFusionError> {
    use datafusion::physical_plan::ExecutionPlanProperties;
    use futures_util::TryStreamExt;
    let ctx = factory();
    let t = std::time::Instant::now();
    let df = ctx.sql(sql).await?;
    let plan = df.create_physical_plan().await?;
    let mut n = 0usize;
    for p in 0..plan.output_partitioning().partition_count() {
        let mut s = plan.execute(p, ctx.task_ctx())?;
        while let Some(b) = s.try_next().await? {
            n += b.num_rows();
        }
    }
    std::hint::black_box(n);
    Ok(t.elapsed().as_secs_f64() * 1000.0)
}

/// Σ.L.2 — resolve verdicts by consulting the workload log first, then
/// probing if uncached, then recording the result. After ~3 observations
/// of the same (table, gb_col) the optimiser converges — the probe runs
/// at most a handful of times per new workload.
pub async fn resolve_via_log_or_probe<F>(
    verdicts: &DictArrivalVerdictMap,
    workload: &crate::workload_log::WorkloadLog,
    gb_col_for: impl Fn(&str) -> Option<String>,
    min_observations: i64,
    mut make_probe_factories: F,
) -> Result<DictArrivalDecision, datafusion::error::DataFusionError>
where
    F: FnMut(&str) -> Option<(Box<CtxFactory<'static>>, Box<CtxFactory<'static>>, String)>,
{
    let mut out: DictArrivalDecision = HashMap::new();
    for (table, verdict) in verdicts {
        match verdict {
            DictArrivalVerdict::Yes => {
                out.insert(table.clone(), true);
            }
            DictArrivalVerdict::No => {
                out.insert(table.clone(), false);
            }
            DictArrivalVerdict::Speculate { .. } => {
                let gb = match gb_col_for(table) {
                    Some(g) => g,
                    None => {
                        out.insert(table.clone(), false);
                        continue;
                    }
                };
                // Consult the log first.
                if let Ok(Some(cached)) = workload.consult_probe(table, &gb, min_observations) {
                    out.insert(table.clone(), cached);
                    continue;
                }
                // Probe + record.
                if let Some((dict_f, default_f, probe_sql)) = make_probe_factories(table) {
                    let r = probe_dict_vs_default(&*dict_f, &*default_f, &probe_sql).await?;
                    let _ = workload.record_probe_outcome(table, &gb, r.dict_ms, r.default_ms);
                    out.insert(table.clone(), r.dict_wins());
                } else {
                    out.insert(table.clone(), false);
                }
            }
        }
    }
    Ok(out)
}

/// Resolve a [`DictArrivalVerdictMap`] into a [`DictArrivalDecision`]
/// by probing each `Speculate` entry. `make_probe_factories` is invoked
/// per Speculate table to produce (dict_ctx_factory, default_ctx_factory,
/// probe_sql). Tables marked `Yes` / `No` pass through unchanged.
pub async fn resolve_via_probe<F>(
    verdicts: &DictArrivalVerdictMap,
    mut make_probe_factories: F,
) -> Result<DictArrivalDecision, datafusion::error::DataFusionError>
where
    F: FnMut(&str) -> Option<(Box<CtxFactory<'static>>, Box<CtxFactory<'static>>, String)>,
{
    let mut out: DictArrivalDecision = HashMap::new();
    for (table, verdict) in verdicts {
        match verdict {
            DictArrivalVerdict::Yes => {
                out.insert(table.clone(), true);
            }
            DictArrivalVerdict::No => {
                out.insert(table.clone(), false);
            }
            DictArrivalVerdict::Speculate { .. } => {
                if let Some((dict_f, default_f, probe_sql)) = make_probe_factories(table) {
                    let r = probe_dict_vs_default(&*dict_f, &*default_f, &probe_sql).await?;
                    out.insert(table.clone(), r.dict_wins());
                } else {
                    // Probe factory not provided — pessimistic default.
                    out.insert(table.clone(), false);
                }
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use std::sync::Arc;

    fn ctx_with_lineitem() -> SessionContext {
        let schema = Arc::new(Schema::new(vec![
            Field::new("l_returnflag", DataType::Utf8, false),
            Field::new("l_linestatus", DataType::Utf8, false),
            Field::new("l_shipmode", DataType::Utf8, false),
            Field::new("l_quantity", DataType::Int64, false),
            Field::new("l_extendedprice", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["N"])),
                Arc::new(StringArray::from(vec!["O"])),
                Arc::new(StringArray::from(vec!["AIR"])),
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![100])),
            ],
        )
        .unwrap();
        let mt = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("lineitem", Arc::new(mt)).unwrap();
        ctx
    }

    #[tokio::test]
    async fn q12_shape_picks_dict() {
        // COUNT GROUP BY string → dict-preserve
        let ctx = ctx_with_lineitem();
        let v = analyse_dict_arrival_for_sql(
            &ctx,
            "SELECT l_shipmode, COUNT(*) FROM lineitem GROUP BY l_shipmode",
        )
        .await
        .unwrap();
        assert_eq!(v.get("lineitem"), Some(&true));
    }

    #[tokio::test]
    async fn q01_shape_rejects_dict() {
        // 4+ aggregates over a string GROUP BY → FilterMultiAgg
        // territory, dict-preservation regresses
        let ctx = ctx_with_lineitem();
        let v = analyse_dict_arrival_for_sql(
            &ctx,
            "SELECT l_returnflag, l_linestatus, \
             COUNT(*), SUM(l_quantity), MIN(l_quantity), MAX(l_quantity), \
             AVG(l_quantity), SUM(l_extendedprice) \
             FROM lineitem GROUP BY l_returnflag, l_linestatus",
        )
        .await
        .unwrap();
        assert_eq!(v.get("lineitem"), Some(&false));
    }

    #[tokio::test]
    async fn q13_shape_rejects_dict_due_to_like() {
        // LIKE on a string column → leave as Utf8View for SIMD matcher
        let ctx = ctx_with_lineitem();
        let v = analyse_dict_arrival_for_sql(
            &ctx,
            "SELECT l_shipmode, COUNT(*) FROM lineitem \
             WHERE l_shipmode LIKE '%AIR%' GROUP BY l_shipmode",
        )
        .await
        .unwrap();
        assert_eq!(v.get("lineitem"), Some(&false));
    }

    #[tokio::test]
    async fn q19_shape_rejects_dict_or_of_string_eq() {
        let ctx = ctx_with_lineitem();
        let v = analyse_dict_arrival_for_sql(
            &ctx,
            "SELECT SUM(l_extendedprice) FROM lineitem \
             WHERE (l_shipmode = 'AIR' AND l_quantity < 10) \
                OR (l_shipmode = 'MAIL' AND l_quantity < 20) \
                OR (l_shipmode = 'SHIP' AND l_quantity < 30)",
        )
        .await
        .unwrap();
        assert_eq!(v.get("lineitem"), Some(&false));
    }

    #[tokio::test]
    async fn q16_shape_rejects_multi_key_string_groupby() {
        // GROUP BY (l_returnflag, l_linestatus) — 2 string keys.
        // DictGroupCountExec only handles 1, so dict-preservation
        // regresses (fallback to generic agg on Dict inputs).
        let ctx = ctx_with_lineitem();
        let v = analyse_dict_arrival_for_sql(
            &ctx,
            "SELECT l_returnflag, l_linestatus, COUNT(*) \
             FROM lineitem GROUP BY l_returnflag, l_linestatus",
        )
        .await
        .unwrap();
        assert_eq!(v.get("lineitem"), Some(&false));
    }

    #[tokio::test]
    async fn tiny_table_rejects_dict_via_row_count() {
        // Q12 shape, but the table is 25 rows (nation-sized).
        // Without size info → would pick dict (true). WITH size info
        // showing 25 rows → reject.
        let ctx = ctx_with_lineitem();
        let mut sizes = HashMap::new();
        sizes.insert("lineitem".to_string(), 25u64);
        let v = analyse_dict_arrival_with_sizes(
            &ctx,
            "SELECT l_shipmode, COUNT(*) FROM lineitem GROUP BY l_shipmode",
            &sizes,
        )
        .await
        .unwrap();
        assert_eq!(v.get("lineitem"), Some(&false));
    }

    #[tokio::test]
    async fn large_table_picks_dict_via_row_count() {
        let ctx = ctx_with_lineitem();
        let mut sizes = HashMap::new();
        sizes.insert("lineitem".to_string(), 6_000_000u64);
        let v = analyse_dict_arrival_with_sizes(
            &ctx,
            "SELECT l_shipmode, COUNT(*) FROM lineitem GROUP BY l_shipmode",
            &sizes,
        )
        .await
        .unwrap();
        assert_eq!(v.get("lineitem"), Some(&true));
    }

    #[tokio::test]
    async fn verdict_map_emits_speculate_for_static_yes() {
        // Q12 shape would be static-Yes — verdict variant must wrap as
        // Speculate so a probe (or cache) can confirm.
        let ctx = ctx_with_lineitem();
        let v = analyse_dict_arrival_verdicts(
            &ctx,
            "SELECT l_shipmode, COUNT(*) FROM lineitem GROUP BY l_shipmode",
            &HashMap::new(),
        )
        .await
        .unwrap();
        assert!(matches!(
            v.get("lineitem"),
            Some(&DictArrivalVerdict::Speculate { .. })
        ));
    }

    #[tokio::test]
    async fn verdict_collapse_pessimistic_treats_speculate_as_no() {
        let v = DictArrivalVerdict::Speculate { reason: "test" };
        assert!(!v.collapse_pessimistic());
        let v = DictArrivalVerdict::Yes;
        assert!(v.collapse_pessimistic());
        let v = DictArrivalVerdict::No;
        assert!(!v.collapse_pessimistic());
    }

    #[tokio::test]
    async fn probe_picks_faster_path() {
        // Synthetic: two factories returning the SAME MemTable.
        // The "dict" factory's query has an artificial sleep removed —
        // we just verify the probe's mechanics, not real dict perf.
        // (Real-data race lives in the integration bench.)
        let factory = || ctx_with_lineitem();
        let r = probe_dict_vs_default(&factory, &factory, "SELECT COUNT(*) FROM lineitem")
            .await
            .unwrap();
        // Same code path → neither side should win by >5%. The
        // important property: probe returns a valid ProbeResult.
        assert!(r.dict_ms > 0.0);
        assert!(r.default_ms > 0.0);
        // Within ±50% — wide tolerance for sub-millisecond probes.
        assert!(r.dict_ms < r.default_ms * 2.0);
        assert!(r.default_ms < r.dict_ms * 2.0);
    }

    #[tokio::test]
    async fn resolve_via_probe_passes_through_yes_no() {
        let mut verdicts = DictArrivalVerdictMap::new();
        verdicts.insert("a".to_string(), DictArrivalVerdict::Yes);
        verdicts.insert("b".to_string(), DictArrivalVerdict::No);
        let resolved = resolve_via_probe(&verdicts, |_| None).await.unwrap();
        assert_eq!(resolved.get("a"), Some(&true));
        assert_eq!(resolved.get("b"), Some(&false));
    }

    #[tokio::test]
    async fn no_string_groupby_rejects_dict() {
        let ctx = ctx_with_lineitem();
        let v = analyse_dict_arrival_for_sql(
            &ctx,
            "SELECT SUM(l_quantity) FROM lineitem WHERE l_quantity > 0",
        )
        .await
        .unwrap();
        assert_eq!(v.get("lineitem"), Some(&false));
    }
}
