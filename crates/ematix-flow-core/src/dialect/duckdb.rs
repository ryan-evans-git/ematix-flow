//! Σ.A2 PR 5: DuckDB→DataFusion translator.
//!
//! Pipeline mirrors the Spark translator (parse → walk → emit) but
//! the rewrite surface is much narrower. DuckDB and DataFusion both
//! lean on the Postgres dialect with arrow-rs types, so most queries
//! pass through unchanged. Known divergences captured in the
//! `DUCKDB_TO_DF` remap table below.
//!
//! Add a new entry only when the DuckDB function and its DataFusion
//! counterpart have identical signatures + semantics. Functions
//! that exist only in DuckDB (e.g., `list_filter` with a lambda)
//! pass through unmodified — DataFusion will error at execute time
//! with a clear "function not found" pointing at the rewrite path.

use sqlparser::ast::{Expr, Ident, ObjectName, ObjectNamePart, Statement, VisitMut, VisitorMut};
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;

use super::DialectError;

/// DuckDB → DataFusion function-name remap. Lowercase keys; the walker
/// matches case-insensitively and emits the new name in lowercase.
const DUCKDB_TO_DF: &[(&str, &str)] = &[
    // DuckDB's array literal constructor. DataFusion uses
    // `make_array(...)` for the same N-arg list constructor; both
    // return a List-typed Arrow scalar.
    ("list_value", "make_array"),
];

pub(super) fn translate(sql: &str) -> Result<String, DialectError> {
    if sql.trim().is_empty() {
        return Ok(String::new());
    }

    let dialect = DuckDbDialect;
    let mut statements: Vec<Statement> =
        Parser::parse_sql(&dialect, sql).map_err(|e| DialectError::ParseError(e.to_string()))?;

    let mut renamer = FunctionRenamer;
    for stmt in &mut statements {
        let _: std::ops::ControlFlow<()> = stmt.visit(&mut renamer);
    }

    Ok(statements
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join("; "))
}

/// AST walker — function-name remap only. DuckDB doesn't have the
/// Spark-style `LATERAL VIEW` syntax that needs structural rewriting,
/// so the visitor is simpler than `dialect/spark.rs`'s.
struct FunctionRenamer;

impl VisitorMut for FunctionRenamer {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> std::ops::ControlFlow<Self::Break> {
        if let Expr::Function(func) = expr
            && let Some(new_name) = remap_function_name(&func.name)
        {
            func.name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(new_name))]);
        }
        std::ops::ControlFlow::Continue(())
    }
}

fn remap_function_name(name: &ObjectName) -> Option<&'static str> {
    let single = match name.0.as_slice() {
        [ObjectNamePart::Identifier(id)] => id,
        _ => return None,
    };
    let lower = single.value.to_ascii_lowercase();
    DUCKDB_TO_DF
        .iter()
        .find(|(duck, _)| *duck == lower.as_str())
        .map(|(_, df)| *df)
}
