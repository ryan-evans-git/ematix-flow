//! v2 S0.1 — the seam where the `ematix.frame` DataFrame surface lowers to
//! a DataFusion [`LogicalPlan`] on the SHARED ematix session
//! ([`crate::preset::session_context`]).
//!
//! This is a **stub**: just enough surface to prove plan-identity with SQL
//! (the S0.1 gate), not the full pandas-shaped API — that lands in v2 S4.
//! Index-light by design (`docs/ADR_V2_DATAFRAME_API.md`): operations are
//! positional, there is no row-label index, and binary ops will align by
//! position rather than by label.
//!
//! Every op delegates to the DataFusion [`DataFrame`] builder, which holds
//! the `LogicalPlan`. Because a `Frame` is created from a
//! [`crate::preset::session_context`], the plan it builds flows through the
//! exact optimiser / physical / mesh path a `ctx.sql()` plan does — the two
//! surfaces merge at `SessionState::create_physical_plan` →
//! `FlowQueryPlanner`. See `docs/ADR_V2_SHARED_LOGICAL_PLAN.md`.

use datafusion::common::Result as DfResult;
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::prelude::SessionContext;

/// A minimal lazy frame over the shared ematix session.
///
/// Wraps a DataFusion [`DataFrame`] (which carries the unoptimised
/// `LogicalPlan`). Building ops are lazy; the plan is only optimised /
/// executed at a terminal method ([`Frame::into_optimized_plan`],
/// [`Frame::into_dataframe`] + `collect`). Lazy-by-default per
/// `docs/ADR_V2_DATAFRAME_API.md`.
#[derive(Clone)]
pub struct Frame {
    df: DataFrame,
}

impl Frame {
    /// Start a frame from a table already registered on the shared context.
    pub async fn table(ctx: &SessionContext, name: &str) -> DfResult<Self> {
        Ok(Self {
            df: ctx.table(name).await?,
        })
    }

    /// Wrap an existing DataFusion [`DataFrame`] (assumed to already sit on
    /// the shared session state).
    pub fn from_dataframe(df: DataFrame) -> Self {
        Self { df }
    }

    /// Positional row filter — no index alignment (index-light core).
    pub fn filter(self, predicate: Expr) -> DfResult<Self> {
        Ok(Self {
            df: self.df.filter(predicate)?,
        })
    }

    /// Column projection.
    pub fn select(self, exprs: Vec<Expr>) -> DfResult<Self> {
        Ok(Self {
            df: self.df.select(exprs)?,
        })
    }

    /// Group-by aggregate.
    pub fn aggregate(self, group_expr: Vec<Expr>, aggr_expr: Vec<Expr>) -> DfResult<Self> {
        Ok(Self {
            df: self.df.aggregate(group_expr, aggr_expr)?,
        })
    }

    /// The unoptimised `LogicalPlan`.
    pub fn logical_plan(&self) -> &LogicalPlan {
        self.df.logical_plan()
    }

    /// The optimised `LogicalPlan`. Comparing this against the SQL
    /// surface's optimised plan is the S0.1 plan-identity gate.
    pub fn into_optimized_plan(self) -> DfResult<LogicalPlan> {
        self.df.into_optimized_plan()
    }

    /// Escape hatch back to the DataFusion [`DataFrame`] for execution.
    pub fn into_dataframe(self) -> DataFrame {
        self.df
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::functions_aggregate::expr_fn::sum;
    use datafusion::logical_expr::{col, lit};

    /// A shared context (full ematix chain) with a small in-memory table
    /// `t(a: i64, b: i64)` registered.
    async fn ctx_with_t() -> SessionContext {
        let ctx = crate::preset::session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2, 3, 2])),
                Arc::new(Int64Array::from(vec![10_i64, 20, 30, 40])),
            ],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(table)).unwrap();
        ctx
    }

    /// **S0.1 gate.** A frame op and its SQL equivalent, built on the SAME
    /// shared session, must optimise to identical `LogicalPlan`s. If this
    /// diverges, the DataFrame surface is no longer getting the same engine
    /// treatment as SQL and the shared-plan guarantee is broken.
    #[tokio::test]
    async fn frame_and_sql_lower_to_identical_optimized_plan() -> DfResult<()> {
        let ctx = ctx_with_t().await;

        let sql_plan = ctx
            .sql("SELECT a, SUM(b) FROM t WHERE a > 1 GROUP BY a")
            .await?
            .into_optimized_plan()?;

        let frame_plan = Frame::table(&ctx, "t")
            .await?
            .filter(col("a").gt(lit(1_i64)))?
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .into_optimized_plan()?;

        assert_eq!(
            format!("{}", sql_plan.display_indent()),
            format!("{}", frame_plan.display_indent()),
            "frame lowering diverged from SQL — S0.1 plan-identity broken",
        );
        Ok(())
    }

    /// A second shape (filter + projection, no aggregate) to guard the gate
    /// against being an artefact of one query shape.
    #[tokio::test]
    async fn frame_and_sql_identical_filter_project() -> DfResult<()> {
        let ctx = ctx_with_t().await;

        let sql_plan = ctx
            .sql("SELECT a, b FROM t WHERE a > 1")
            .await?
            .into_optimized_plan()?;

        let frame_plan = Frame::table(&ctx, "t")
            .await?
            .filter(col("a").gt(lit(1_i64)))?
            .select(vec![col("a"), col("b")])?
            .into_optimized_plan()?;

        assert_eq!(
            format!("{}", sql_plan.display_indent()),
            format!("{}", frame_plan.display_indent()),
            "frame filter+project diverged from SQL",
        );
        Ok(())
    }
}
