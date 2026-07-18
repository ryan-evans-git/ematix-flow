//! P3 owned logical IR: the typed relational plan the binder produces and
//! the physical planner consumes. Deliberately small — it grows one node at
//! a time as the SQL surface expands (`Join`, `Sort`, `Limit`, … are later
//! slices), and it is the tree the Σ optimizer rules will eventually rewrite
//! (their DataFusion `LogicalPlan` counterpart re-homes here).
//!
//! Everything below the binder is **bound**: expressions reference chunk
//! positions ([`Expr::Column`]) into the owning [`Scan`](LogicalPlan::Scan)'s
//! `projection`, never names. The scan's projection is the single source of
//! truth for what gets decoded and in what column order.

use std::path::PathBuf;

use crate::expr::Expr;
use crate::vector::LogicalType;

/// One column a scan decodes: resolved name (for display), parquet leaf
/// index (what the native scan reads), and type. Chunk column `i` is
/// `projection[i]`.
#[derive(Clone, Debug, PartialEq)]
pub struct ScanColumn {
    pub name: String,
    pub leaf: usize,
    pub ty: LogicalType,
}

/// A named group key, e.g. the `l_linenumber` in `SELECT l_linenumber,
/// sum(…) … GROUP BY l_linenumber`. The name is the output column label
/// (alias, or the column's own name).
#[derive(Clone, Debug, PartialEq)]
pub struct GroupExpr {
    pub expr: Expr,
    pub name: String,
}

/// An aggregate call, e.g. `sum(l_extendedprice * l_discount) as revenue`.
#[derive(Clone, Debug, PartialEq)]
pub struct AggExpr {
    pub func: AggFunc,
    /// The bound argument expression.
    pub arg: Expr,
    /// Output name (the SQL alias, or a generated one).
    pub alias: Option<String>,
}

/// Supported aggregate functions. Grows with P3 slice 5
/// (COUNT/MIN/MAX/AVG …).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggFunc {
    Sum,
}

/// The bound, typed relational plan.
#[derive(Clone, Debug, PartialEq)]
pub enum LogicalPlan {
    /// Decode `projection` (in order) from the parquet file at `path`.
    Scan {
        table: String,
        path: PathBuf,
        projection: Vec<ScanColumn>,
    },
    /// Keep the input rows satisfying `predicate` (a boolean [`Expr`]).
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    /// Inner equi-join where the **right side contributes no output
    /// columns** — its columns appear only in its own filters. Rows flow
    /// from `left` with true inner-join multiplicity: a left row is kept
    /// once per matching right row (selection-index duplication), which
    /// degenerates to a pure semijoin narrow when right keys are unique.
    /// `left_key` / `right_key` are chunk positions in each side's own
    /// scan projection. A payload-carrying join is the next slice.
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        left_key: usize,
        right_key: usize,
    },
    /// Group by `group` and compute `aggs` over each group. An empty
    /// `group` is the scalar-aggregate case (one output row). Output
    /// columns are the group keys (in order) then the aggregates.
    Aggregate {
        input: Box<LogicalPlan>,
        group: Vec<GroupExpr>,
        aggs: Vec<AggExpr>,
    },
}
