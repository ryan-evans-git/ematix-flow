//! P3 owned logical IR: the flat bound-query form the binder produces and
//! the physical planner consumes.
//!
//! A query is a **join graph**, not a nested tree: per-table inputs
//! (scan + own filters), join edges equating column slots, and
//! group/aggregate/output expressions over a **global slot space**. This is
//! the classic select-project-join block — the form join-order decisions
//! and the Σ rewrite rules operate on; the *tree* (who builds, who probes,
//! which side is root) is a physical-planning decision made per execution,
//! not frozen at bind time.
//!
//! Two expression spaces, deliberately distinct:
//! - **Slot space** — `Expr::Column(s)` names global slot `s` =
//!   `slots[s] = (table, position in that table's scan projection)`.
//!   Table filters, group keys, and aggregate arguments live here.
//! - **Row space** — in [`OutputExpr`] only, `Expr::Column(i)` names the
//!   `i`-th value of a per-group result row `[group keys…, agg values…]`.
//!   This is how `sum(a)/sum(b)` projects over computed aggregates.

use std::path::PathBuf;

use crate::expr::Expr;
use crate::vector::LogicalType;

/// One column a table's scan decodes: resolved name, parquet leaf index,
/// type. A table's chunk column `i` is `projection[i]`.
#[derive(Clone, Debug, PartialEq)]
pub struct ScanColumn {
    pub name: String,
    pub leaf: usize,
    pub ty: LogicalType,
    /// `Some(s)`: the file stores an INT-backed `DECIMAL(p, s)` — decode
    /// scales by `10^s` into `Float64` (see [`crate::catalog::ColumnDef`]).
    pub dec_scale: Option<u8>,
    /// Declared `optional` in the file — decode reads definition levels
    /// (routes the table through the def-level-aware stock reader).
    pub nullable: bool,
}

/// A global column slot: which table, and which position in that table's
/// scan projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Slot {
    pub table: usize,
    pub col: usize,
}

/// Where a table's rows come from.
#[derive(Clone, Debug, PartialEq)]
pub enum TableSource {
    /// A parquet file on disk.
    Parquet(PathBuf),
    /// A materialized derived query — index into [`BoundQuery::derived`].
    /// `ScanColumn::leaf` is the derived query's output-column position.
    Derived(usize),
    /// A `WITH RECURSIVE` self-reference: reads the fixpoint driver's current
    /// working set (see [`BoundQuery::recursive`]). `ScanColumn::leaf` is the
    /// CTE's output-column position.
    WorkingSet,
    /// An inline literal row set — the FROM-less `SELECT` dual (one dummy
    /// row) and each `VALUES` side bind against one. `ScanColumn::leaf`
    /// indexes into a row.
    Values(std::sync::Arc<Vec<Vec<crate::expr::ScalarValue>>>),
}

/// One table in the query: its scan plus its own filters (conjuncts that
/// reference only this table's slots), pre-join.
#[derive(Clone, Debug, PartialEq)]
pub struct TableInput {
    /// Display name — the alias if one was given, else the table name.
    pub name: String,
    pub source: TableSource,
    pub projection: Vec<ScanColumn>,
    /// Slot-space predicate over this table's slots only.
    pub filter: Option<Expr>,
}

/// An equi-join edge: the two global slots equated by `a = b`. For a LEFT
/// OUTER join, `preserved` names the table whose rows survive without a
/// match (the executor roots the join tree there).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JoinEdge {
    pub a: usize,
    pub b: usize,
    pub preserved: Option<usize>,
}

/// A group key (slot space).
#[derive(Clone, Debug, PartialEq)]
pub struct GroupExpr {
    pub expr: Expr,
}

/// An aggregate call (argument in slot space).
#[derive(Clone, Debug, PartialEq)]
pub struct AggExpr {
    pub func: AggFunc,
    /// The bound argument. For `COUNT(*)` this is unused (any literal).
    pub arg: Expr,
    /// `string_agg`/`group_concat` only: the delimiter and in-aggregate ORDER
    /// BY. `None` for every other aggregate.
    pub str_agg: Option<StrAggSpec>,
}

/// The delimiter and ordering of a `string_agg` / `group_concat` aggregate.
#[derive(Clone, Debug, PartialEq)]
pub struct StrAggSpec {
    /// Separator inserted between concatenated values.
    pub delim: std::sync::Arc<str>,
    /// `(key, desc)` ordering applied within each group before concatenation;
    /// empty = arrival order.
    pub order: Vec<(Expr, bool)>,
    /// `string_agg(DISTINCT …)` — each distinct value contributes once (the
    /// first occurrence in concatenation order). The binder guarantees any
    /// ORDER BY is the aggregated value itself, so "first occurrence" is
    /// well-defined.
    pub distinct: bool,
}

/// Supported aggregate functions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggFunc {
    Sum,
    /// `COUNT(*)` counts rows (literal argument); `COUNT(expr)` counts
    /// non-NULL evaluations.
    Count,
    Min,
    Max,
    Avg,
    /// Sample standard deviation (`stddev_samp`).
    StddevSamp,
    /// Population standard deviation (`stddev_pop`).
    StddevPop,
    /// Sample variance (`var_samp` / `variance`).
    VarSamp,
    /// Population variance (`var_pop`).
    VarPop,
    /// `COUNT(DISTINCT <int expr>)`.
    CountDistinct,
    /// Continuous percentile / median. The buffered non-NULL argument values
    /// are sorted and linearly interpolated at fraction `p` (the `u64` is
    /// `f64::to_bits(p)`; `median(x)` binds `p = 0.5`, `percentile_cont(p)
    /// WITHIN GROUP (ORDER BY x)` binds the given `p`). Output is Float64;
    /// NULL over an empty/all-NULL group. Buffered — not foldable — so it
    /// carries [`AggState::buf`].
    PercentileCont(u64),
    /// `bool_and(x)` — TRUE iff every non-NULL input is truthy, else FALSE;
    /// NULL over an empty/all-NULL group. Foldable as `min(0/1)`. Output
    /// BOOLEAN (the binder wraps the reference as `slot = 1`).
    BoolAnd,
    /// `bool_or(x)` — TRUE iff any non-NULL input is truthy. Foldable as
    /// `max(0/1)`; same BOOLEAN rendering as [`AggFunc::BoolAnd`].
    BoolOr,
    /// `string_agg(x, delim [ORDER BY …])` / `group_concat` — concatenate the
    /// non-NULL string values with `delim`, in the optional in-aggregate ORDER
    /// BY (arrival order otherwise). Buffered (retains each value + its order
    /// key in [`AggState`]); the delimiter and ordering live in
    /// [`AggExpr::str_agg`]. Output Utf8; NULL over an empty group.
    StringAgg,
    /// `COUNT(<col of a LEFT-joined table>)` — counts only row occurrences
    /// where that table matched (the no-NULL engine's outer-join counting;
    /// unmatched preserved rows contribute 0).
    CountMatched(usize),
}

/// One SELECT output: an expression in **row space** (`Column(i)` = the
/// `i`-th of `[group keys…, agg values…]`) and its output column name.
#[derive(Clone, Debug, PartialEq)]
pub struct OutputExpr {
    pub expr: Expr,
    pub name: String,
}

/// One ORDER BY key: which output column, and direction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderByKey {
    /// Index into [`BoundQuery::output`].
    pub output: usize,
    pub desc: bool,
}

/// A window function.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowFunc {
    /// An aggregate evaluated over the partition (whole-partition when
    /// unordered, cumulative when ordered).
    Agg(AggFunc),
    /// `rank()` — ties share a rank, gaps follow.
    Rank,
    /// `dense_rank()` — ties share a rank, no gaps.
    DenseRank,
    /// `row_number()`.
    RowNumber,
    /// `lag(arg, offset)` — the arg `offset` rows earlier in the ordered
    /// partition (SQL NULL past the partition edge). Default offset 1.
    Lag(u32),
    /// `lead(arg, offset)` — the arg `offset` rows later.
    Lead(u32),
    /// `ntile(n)` — 1..=n bucket label, rows split as evenly as possible
    /// (the first `len % n` buckets get one extra row).
    Ntile(u32),
    /// `first_value(arg)` — arg at the partition's first ordered row (the
    /// default `UNBOUNDED PRECEDING..CURRENT ROW` frame always starts there).
    FirstValue,
    /// `last_value(arg)` — arg at the frame's last row. Under the default
    /// `..CURRENT ROW` frame that is the current row's peer group's last
    /// member; under an `UNBOUNDED FOLLOWING` (or unordered whole-partition)
    /// frame it is the partition's last ordered row.
    LastValue,
}

/// One window expression, evaluated over the block's POST-GROUPING result
/// rows (all component expressions are row space).
#[derive(Clone, Debug, PartialEq)]
pub struct WindowExpr {
    pub func: WindowFunc,
    /// Aggregate argument (unused for the rank family).
    pub arg: Expr,
    pub partition: Vec<Expr>,
    /// `(key, desc)` ordering within the partition; empty = whole
    /// partition.
    pub order: Vec<(Expr, bool)>,
    /// Explicit `ROWS UNBOUNDED PRECEDING..CURRENT ROW` (strict running);
    /// `false` = RANGE semantics (peers of the current row included).
    pub rows_frame: bool,
    /// The frame ends at `UNBOUNDED FOLLOWING` — the frame spans the whole
    /// partition regardless of ORDER BY, so aggregate windows produce the
    /// partition total on every row (not a running value) and `last_value`
    /// returns the partition's last ordered row.
    pub frame_end_unbounded: bool,
    /// `lag`/`lead` DEFAULT (3rd argument) — the value returned past the
    /// partition edge instead of SQL NULL. `None` = NULL past the edge.
    pub lag_default: Option<f64>,
    /// An explicit bounded `ROWS BETWEEN <start> AND <end>` frame with at
    /// least one finite row offset (a sliding window). `(start, end)` as
    /// signed row offsets from the current row: `None` = unbounded on that
    /// side, `Some(k)` = `k` rows away (negative = PRECEDING, 0 = CURRENT
    /// ROW, positive = FOLLOWING). When set, the executor aggregates each
    /// row over the clamped `[current+start, current+end]` window and the
    /// `rows_frame`/`frame_end_unbounded` flags are unused. Only aggregate
    /// windows support it (the binder rejects it for the navigation family).
    pub rows_bounds: Option<(Option<i64>, Option<i64>)>,
    /// An enclosing `WHERE <this window's output> <= K` proved only the
    /// top K rows per partition can survive (rank/row_number only —
    /// set by the binder). The executor prunes each partition to the rows
    /// ordering at-or-before its K-th best BEFORE sorting/projecting;
    /// rank values on that prefix are identical, and the still-applied
    /// outer filter trims threshold ties.
    pub top_k: Option<usize>,
}

/// A SQL set operation combining two query blocks' row sets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetOp {
    /// `UNION ALL` — concatenate.
    UnionAll,
    /// `UNION` — concatenate then deduplicate.
    Union,
    /// `INTERSECT` — distinct rows present in both sides.
    Intersect,
    /// `EXCEPT` — distinct left rows absent from the right side.
    Except,
    /// `INTERSECT ALL` — multiset: each row kept `min(count_left,
    /// count_right)` times.
    IntersectAll,
    /// `EXCEPT ALL` — multiset: each row kept `max(0, count_left −
    /// count_right)` times.
    ExceptAll,
}

/// The bound, typed query: the flat select-project-join block.
#[derive(Clone, Debug, PartialEq)]
pub struct BoundQuery {
    pub tables: Vec<TableInput>,
    pub edges: Vec<JoinEdge>,
    pub slots: Vec<Slot>,
    /// A slot-space predicate referencing **multiple** tables — evaluated at
    /// the join-tree root after payload attach (e.g. Q19's OR of
    /// part×lineitem conjunct groups, or a join-cycle's residual equality).
    pub post_filter: Option<Expr>,
    pub group: Vec<GroupExpr>,
    /// `GROUP BY ROLLUP(t₁, t₂, …)` — the column count of each ROLLUP term
    /// (`ROLLUP(a, b)` → `[1, 1]`; `ROLLUP((a,b), c)` → `[2, 1]`). Empty for
    /// a plain GROUP BY. The `group` list holds the flattened term columns;
    /// the executor emits one grouping set per term prefix (all terms, drop
    /// the last, …, drop all = grand total), NULL-filling dropped columns.
    pub rollup_terms: Vec<usize>,
    /// `GROUP BY CUBE(…)` / `GROUPING SETS(…)` — the general form ROLLUP
    /// cannot express. Each inner vec is one requested grouping set as the
    /// **active** column indices into `group` (kept in that set; the rest
    /// render as subtotals). Empty for a plain GROUP BY or ROLLUP (which use
    /// `rollup_terms`). The executor re-aggregates the base groups into each
    /// set. `CUBE(a,b)` → `[[0,1],[0],[1],[]]`; `GROUPING SETS((a,b),(c),())`
    /// → `[[0,1],[2],[]]`.
    pub grouping_sets: Vec<Vec<usize>>,
    pub aggs: Vec<AggExpr>,
    /// Row-space predicate over `[group keys…, agg values…]` (HAVING).
    pub having: Option<Expr>,
    /// `QUALIFY` — a predicate over the POST-WINDOW row space (may reference
    /// window outputs), applied as a row filter after windows materialize and
    /// before the output projection.
    pub qualify: Option<Expr>,
    pub output: Vec<OutputExpr>,
    /// Trailing outputs appended only to serve ORDER BY expressions —
    /// dropped from the final rows after sorting.
    pub hidden_outputs: usize,
    /// `GROUPING(col)` is used — the executor appends one 0/1 flag column
    /// per GROUP BY key (1 = that key is a ROLLUP subtotal), so row space is
    /// `[group keys…, agg values…, grouping flags…, window values…]`. The
    /// flags sit before windows so a window `PARTITION BY grouping(a)` reads
    /// them (q36/q70).
    pub has_grouping: bool,
    /// Window expressions — row space extends to `[group keys…, agg
    /// values…, grouping flags…, window values…]` after HAVING.
    pub windows: Vec<WindowExpr>,
    /// `SELECT DISTINCT` that grouping did not already absorb — dedup the
    /// final output rows before ORDER BY / LIMIT. Zero-cost when false, and
    /// the common no-GROUP-BY DISTINCT folds into grouping instead (so this
    /// stays false there).
    pub distinct: bool,
    pub order_by: Vec<OrderByKey>,
    pub limit: Option<usize>,
    /// `OFFSET n` — skip the first `n` rows (after ORDER BY, before LIMIT).
    pub offset: Option<usize>,
    /// Uncorrelated subqueries referenced by [`Expr::ScalarSub`] /
    /// [`Expr::InSub`] — executed first, then substituted as constants /
    /// membership sets.
    pub subqueries: Vec<BoundQuery>,
    /// Materialized derived queries (CTEs, aggregate FROM-subqueries,
    /// decorrelated scalars) referenced by [`TableSource::Derived`].
    pub derived: Vec<std::sync::Arc<BoundQuery>>,
    /// Further blocks combined into this one's rows, in order (`a UNION
    /// ALL b INTERSECT c` = left-deep). This block's ORDER BY / LIMIT
    /// apply to the COMBINED rows.
    pub set_ops: Vec<(SetOp, BoundQuery)>,
    /// `WITH RECURSIVE`: when `Some`, THIS query is the anchor (seed) and the
    /// box holds the recursive step. The executor runs a fixpoint — seed, then
    /// repeatedly the step (its [`TableSource::WorkingSet`] reading the last
    /// iteration's new rows) — accumulating until the step yields nothing.
    pub recursive: Option<Box<RecursiveCte>>,
}

/// The recursive branch of a `WITH RECURSIVE` CTE (see
/// [`BoundQuery::recursive`]).
#[derive(Clone, Debug, PartialEq)]
pub struct RecursiveCte {
    /// The recursive query, evaluated once per iteration with its
    /// [`TableSource::WorkingSet`] bound to the previous iteration's new rows.
    pub step: BoundQuery,
    /// `UNION` (dedup new rows against all seen) vs `UNION ALL` (keep every
    /// row); `true` = distinct.
    pub distinct: bool,
}
