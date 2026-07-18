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
}

/// A global column slot: which table, and which position in that table's
/// scan projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Slot {
    pub table: usize,
    pub col: usize,
}

/// One table in the query: its scan plus its own filters (conjuncts that
/// reference only this table's slots), pre-join.
#[derive(Clone, Debug, PartialEq)]
pub struct TableInput {
    /// Display name — the alias if one was given, else the table name.
    pub name: String,
    pub path: PathBuf,
    pub projection: Vec<ScanColumn>,
    /// Slot-space predicate over this table's slots only.
    pub filter: Option<Expr>,
}

/// An equi-join edge: the two global slots equated by `a = b`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JoinEdge {
    pub a: usize,
    pub b: usize,
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
}

/// Supported aggregate functions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggFunc {
    Sum,
    /// `COUNT(*)` / `COUNT(expr)` — no NULLs yet, so both count rows.
    Count,
    Min,
    Max,
    Avg,
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
    pub aggs: Vec<AggExpr>,
    /// Row-space predicate over `[group keys…, agg values…]` (HAVING).
    pub having: Option<Expr>,
    pub output: Vec<OutputExpr>,
    pub order_by: Vec<OrderByKey>,
    pub limit: Option<usize>,
}
