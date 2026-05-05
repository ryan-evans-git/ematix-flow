//! Σ.A2 PR 2: Spark→DataFusion translator.
//!
//! Pipeline:
//! 1. `sqlparser::parser::Parser::parse_sql(&DatabricksDialect, sql)`
//!    → `Vec<Statement>`. Databricks SQL is Spark SQL plus
//!    extensions, so anything the user writes that's valid Spark
//!    parses cleanly here. (sqlparser-rs 0.61 has no SparkDialect.)
//! 2. `VisitMut`-based AST walk that rewrites known Spark function
//!    names to their DataFusion equivalents (see `SPARK_TO_DF`).
//!    `expr(x)` is a special case — it's a no-op wrapper, so the
//!    walker replaces the entire `Expr::Function` node with the
//!    inner argument.
//! 3. Re-emit each `Statement` via `Display`. Statements joined
//!    with `; ` separator (matches sqlparser's input parsing).
//!
//! Scope of PR 2: pure function-name remap (and the `expr(...)`
//! strip since it falls out of the same walker). Argument-shape
//! rewrites (`from_unixtime` signature, `INTERVAL '90' DAY` literal),
//! structural rewrites (`LATERAL VIEW EXPLODE` → `UNNEST`, complex-
//! type literals), and TPC-DS audit run all land in PR 3 / PR 4.
//!
//! Function-name remap table is intentionally narrow — only known-
//! safe substitutions where the function semantics + signature are
//! identical between dialects. Functions that exist only in Spark
//! (e.g., `transform_keys`) are passed through; DataFusion will
//! error at execute time with a "function not found" message that
//! tells the user exactly what to rewrite.

use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident, ObjectName,
    ObjectNamePart, Statement, VisitMut, VisitorMut,
};
use sqlparser::dialect::DatabricksDialect;
use sqlparser::parser::Parser;

use super::DialectError;

/// Spark → DataFusion function-name remap. Both names are stored
/// lowercase; `FunctionRenamer` matches case-insensitively and emits
/// the new name in lowercase (DataFusion is case-insensitive on
/// function names, so this loses no information).
///
/// **Add a new entry only when the Spark function and its DataFusion
/// counterpart have identical signatures + semantics.** If they
/// differ in argument count, ordering, or null-handling, they're an
/// argument-shape rewrite (PR 3 territory) — not a name remap.
const SPARK_TO_DF: &[(&str, &str)] = &[
    // Null-handling aliases. Both take exactly two args; the first
    // returned if non-null, else the second. DataFusion's COALESCE
    // accepts >=2 args; Spark's IFNULL/NVL are 2-arg only. Within
    // the 2-arg case the semantics match.
    ("ifnull", "coalesce"),
    ("nvl", "coalesce"),
    // String case. Spark inherits the LCASE/UCASE names from Hive;
    // DataFusion uses ANSI LOWER/UPPER. Same single-string-arg
    // signature.
    ("lcase", "lower"),
    ("ucase", "upper"),
    // 1-based substring position. Both return 0 when not found.
    ("instr", "strpos"),
    // `current_timestamp()` works in both, but `now()` is shorter +
    // more idiomatic in DataFusion. Both 0-arg, both return the
    // same `Timestamp`.
    ("current_timestamp", "now"),
];

/// Parse `sql` as Spark/Databricks SQL, walk the AST applying the
/// `SPARK_TO_DF` remap + the `expr(x)` strip, re-emit as DataFusion-
/// compatible SQL.
pub(super) fn translate(sql: &str) -> Result<String, DialectError> {
    if sql.trim().is_empty() {
        // Matches the DataFusion arm: empty SQL is a valid input
        // (the windowed-transform path passes "" when there's no
        // pre-stage). Bail before sqlparser sees an empty token
        // stream and complains.
        return Ok(String::new());
    }

    let dialect = DatabricksDialect {};
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

/// `VisitMut`-driven AST walker. Two responsibilities:
///   - For every `Expr::Function`, look up the function name in
///     `SPARK_TO_DF`. If found, rewrite the `ObjectName` in place.
///   - For every `Expr::Function` whose name is `expr` and which
///     has exactly one positional arg, replace the whole `Expr` node
///     with the inner argument's expression.
///
/// Both rewrites preserve the surrounding AST; `Display` on the
/// outer expression then emits valid DataFusion SQL.
struct FunctionRenamer;

impl VisitorMut for FunctionRenamer {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> std::ops::ControlFlow<Self::Break> {
        // First: handle the `expr(x)` no-op wrapper. We need to
        // replace the *outer* node, so check + swap before
        // descending.
        if let Some(replacement) = strip_expr_wrapper(expr) {
            *expr = replacement;
            // Don't return Continue with no rewrite — descend into
            // the new expression, which may itself contain remaps
            // or another expr() wrapper. Falling through here lets
            // the next match arm see the just-swapped expr.
        }

        if let Expr::Function(func) = expr
            && let Some(new_name) = remap_function_name(&func.name)
        {
            func.name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new(new_name))]);
        }

        std::ops::ControlFlow::Continue(())
    }
}

/// If `expr` is a one-arg call to `expr(...)`, return the inner
/// expression so the caller can swap it in. Otherwise `None`.
///
/// Spark's `expr` is a passthrough used to embed arbitrary
/// expression text inside the DataFrame API; in raw SQL it's a
/// no-op + DataFusion has no equivalent function, so stripping is
/// the correct rewrite.
fn strip_expr_wrapper(expr: &Expr) -> Option<Expr> {
    let Expr::Function(func) = expr else {
        return None;
    };
    if !object_name_eq_ignore_case(&func.name, "expr") {
        return None;
    }
    let FunctionArguments::List(FunctionArgumentList { args, .. }) = &func.args else {
        return None;
    };
    if args.len() != 1 {
        return None;
    }
    let inner = match &args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => e.clone(),
        _ => return None,
    };
    Some(inner)
}

/// Look up `name` (case-insensitively) in `SPARK_TO_DF`. Returns
/// the DataFusion-side name if found, `None` if no remap applies.
/// `None` results in pass-through — DataFusion sees the original
/// function name + errors at execute if it's not a known function.
fn remap_function_name(name: &ObjectName) -> Option<&'static str> {
    let single = match name.0.as_slice() {
        [ObjectNamePart::Identifier(id)] => id,
        // Schema-qualified function names (`schema.fn(x)`) are out
        // of scope for the remap table — those are user-defined or
        // catalog-level functions, not idiomatic Spark builtins.
        _ => return None,
    };
    let lower = single.value.to_ascii_lowercase();
    SPARK_TO_DF
        .iter()
        .find(|(spark, _)| *spark == lower.as_str())
        .map(|(_, df)| *df)
}

/// `ObjectName.eq` is case-sensitive; this helper does the
/// case-insensitive single-segment comparison the function-rename
/// path needs.
fn object_name_eq_ignore_case(name: &ObjectName, target: &str) -> bool {
    matches!(
        name.0.as_slice(),
        [ObjectNamePart::Identifier(id)] if id.value.eq_ignore_ascii_case(target)
    )
}
