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
    BinaryOperator, CastKind, DataType, ExactNumberInfo, Expr, Function, FunctionArg,
    FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident, ObjectName, ObjectNamePart,
    Query, Select, SelectItem, SetExpr, Statement, TableAlias, TableFactor, TableWithJoins, Value,
    VisitMut, VisitorMut, WildcardAdditionalOptions,
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
    // Σ.A2 PR 3 audit finding: Spark's `array(1, 2, 3)` constructor
    // doesn't exist in DataFusion 53 (only `make_array(...)`). Pure
    // name remap — same N-arg signature, same return shape (List).
    ("array", "make_array"),
];

/// Parse `sql` as Spark/Databricks SQL, walk the AST applying the
/// `SPARK_TO_DF` remap + the `expr(x)` strip + the LATERAL VIEW
/// EXPLODE → CROSS JOIN UNNEST rewrite, re-emit as DataFusion-
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

    // Σ.A2 PR 3: rewrite LATERAL VIEW EXPLODE into CROSS JOIN UNNEST
    // *before* the FunctionRenamer descends. The renamer's only job
    // is per-Expr substitution; the lateral-view rewrite is a
    // FROM-clause restructuring better expressed as a separate pass.
    let mut lateral_rewriter = LateralViewRewriter { error: None };
    for stmt in &mut statements {
        let _: std::ops::ControlFlow<()> = stmt.visit(&mut lateral_rewriter);
    }
    if let Some(err) = lateral_rewriter.error {
        return Err(err);
    }

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

        // Spark `/` is TRUE division — `int / int` yields a DOUBLE (e.g.
        // `3 / 2` = 1.5). DataFusion does INTEGER division on integer
        // operands (`3 / 2` = 1), which silently truncates and changes
        // results: TPC-DS q34/q73's `hd_dep_count / hd_vehicle_count > 1.2`
        // filter dropped rows the Spark/DuckDB semantics keep (176 vs 223,
        // 0 vs 1). Force Spark semantics by casting the left operand of
        // every `/` to DOUBLE, so DataFusion does float division. The
        // DuckDB oracle runs the SAME translated SQL, so this is
        // parity-safe (both engines evaluate the identical double
        // division). Spark uses `div` for integer division — untouched.
        if let Expr::BinaryOp { left, op, .. } = expr
            && *op == BinaryOperator::Divide
            && !matches!(
                left.as_ref(),
                Expr::Cast {
                    data_type: DataType::Double(_),
                    ..
                }
            )
        {
            let taken =
                std::mem::replace(left.as_mut(), Expr::Value(Value::Null.with_empty_span()));
            **left = Expr::Cast {
                kind: CastKind::Cast,
                expr: Box::new(taken),
                data_type: DataType::Double(ExactNumberInfo::None),
                array: false,
                format: None,
            };
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

/// Σ.A2 PR 3: rewrites `Select.lateral_views` (Spark/Databricks/Hive
/// `LATERAL VIEW EXPLODE(arr) v AS x`) into a wrapped subquery
/// `FROM (SELECT *, unnest(arr) AS x FROM <orig>) sub`. DataFusion 53
/// supports the projection-form `unnest(...)` but rejects
/// `CROSS JOIN UNNEST(...)` with an `OuterReferenceColumn` physical-
/// plan error — the wrap-in-subquery shape works around that
/// limitation. See `examples/df_unnest_probe.rs` for the empirical
/// finding.
///
/// Multiple stacked lateral views chain by re-entering the visitor
/// on the wrapped query: each LATERAL VIEW wraps the previous result
/// in another subquery, matching Spark's apply-in-order semantics.
///
/// `LATERAL VIEW OUTER` (which preserves rows when the array is
/// empty) doesn't have a built-in DataFusion equivalent — DataFusion's
/// UNNEST drops empty-array rows. Rather than emit subtly-wrong SQL,
/// we capture an error in `self.error` and let the caller bail.
struct LateralViewRewriter {
    /// Set when an unsupported LATERAL VIEW shape is encountered.
    /// Captured here rather than returned via `ControlFlow::Break`
    /// because the visitor's `Break` type is `()` (matches the
    /// `FunctionRenamer` for symmetry); checking after the visit
    /// finishes is just as effective.
    error: Option<DialectError>,
}

impl VisitorMut for LateralViewRewriter {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> std::ops::ControlFlow<Self::Break> {
        let SetExpr::Select(select) = query.body.as_mut() else {
            return std::ops::ControlFlow::Continue(());
        };
        if select.lateral_views.is_empty() {
            return std::ops::ControlFlow::Continue(());
        }
        // Need a FROM relation to wrap. `FROM (VALUES ...) LATERAL
        // VIEW ...` is rare but legal in Spark; for the empty-FROM
        // edge case we'd need a synthetic single-row relation.
        // Defer until a user surfaces it.
        if select.from.is_empty() {
            self.error = Some(DialectError::ParseError(
                "LATERAL VIEW with no FROM relation is not supported by the \
                 Spark→DataFusion translator yet"
                    .into(),
            ));
            return std::ops::ControlFlow::Break(());
        }

        // Take ownership of the lateral views; clear the field on
        // `select` so re-emission doesn't include the original Spark
        // syntax. Process each LV in document order — each wraps the
        // previous result.
        let lateral_views = std::mem::take(&mut select.lateral_views);

        for lv in lateral_views {
            if lv.outer {
                self.error = Some(DialectError::ParseError(
                    "LATERAL VIEW OUTER has no direct DataFusion equivalent \
                     (DataFusion's UNNEST drops empty-array rows). \
                     Rewrite as CASE + UNION manually if outer-join semantics \
                     are required."
                        .into(),
                ));
                return std::ops::ControlFlow::Break(());
            }

            let array_expr = match extract_explode_arg(&lv.lateral_view) {
                Some(e) => e,
                None => {
                    self.error = Some(DialectError::ParseError(format!(
                        "LATERAL VIEW {} is not yet supported \
                         (only EXPLODE(arr) translates today)",
                        lv.lateral_view
                    )));
                    return std::ops::ControlFlow::Break(());
                }
            };

            // Spark allows multiple column aliases (`LATERAL VIEW
            // POSEXPLODE(arr) v AS pos, val` produces two columns).
            // Plain EXPLODE has exactly one alias; reject anything
            // else for now.
            if lv.lateral_col_alias.len() != 1 {
                self.error = Some(DialectError::ParseError(format!(
                    "LATERAL VIEW with {} column aliases is not yet supported \
                     (EXPLODE has one; POSEXPLODE has two and lands later)",
                    lv.lateral_col_alias.len()
                )));
                return std::ops::ControlFlow::Break(());
            }
            let col_alias = lv.lateral_col_alias.into_iter().next().unwrap();

            // Build the inner SELECT that wraps the existing FROM:
            //     SELECT *, unnest(<array_expr>) AS <col_alias>
            //     FROM <previous from>
            //
            // sqlparser's `Select::default()` would be cleaner if it
            // existed; building manually keeps the visible fields
            // explicit + makes the unused-default options auditable.
            let inner_select = Select {
                select_token: sqlparser::tokenizer::TokenWithSpan::wrap(
                    sqlparser::tokenizer::Token::SemiColon,
                )
                .into(),
                distinct: None,
                top: None,
                top_before_distinct: false,
                optimizer_hint: None,
                projection: vec![
                    SelectItem::Wildcard(WildcardAdditionalOptions::default()),
                    SelectItem::ExprWithAlias {
                        expr: Expr::Function(make_unnest(array_expr)),
                        alias: col_alias.clone(),
                    },
                ],
                exclude: None,
                into: None,
                from: std::mem::take(&mut select.from),
                lateral_views: Vec::new(),
                prewhere: None,
                selection: None,
                group_by: sqlparser::ast::GroupByExpr::Expressions(vec![], vec![]),
                cluster_by: Vec::new(),
                distribute_by: Vec::new(),
                sort_by: Vec::new(),
                having: None,
                named_window: Vec::new(),
                qualify: None,
                window_before_qualify: false,
                value_table_mode: None,
                connect_by: Vec::new(),
                flavor: sqlparser::ast::SelectFlavor::Standard,
                select_modifiers: Some(sqlparser::ast::SelectModifiers::default()),
            };

            let inner_query = Query {
                with: None,
                body: Box::new(SetExpr::Select(Box::new(inner_select))),
                order_by: None,
                limit_clause: None,
                fetch: None,
                locks: Vec::new(),
                for_clause: None,
                settings: None,
                format_clause: None,
                pipe_operators: Vec::new(),
            };

            // The wrapper alias is whatever the lateral view named
            // (`v` in `LATERAL VIEW EXPLODE(arr) v AS x`). Falling
            // back to a synthetic name if the lateral view didn't
            // declare one (rare; sqlparser parses an Ident for it).
            let wrapper_alias_name = lv
                .lateral_view_name
                .0
                .first()
                .and_then(|p| match p {
                    ObjectNamePart::Identifier(id) => Some(id.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| Ident::new("__sigma_lv_wrap"));

            let derived = TableFactor::Derived {
                lateral: false,
                subquery: Box::new(inner_query),
                alias: Some(TableAlias {
                    explicit: true,
                    name: wrapper_alias_name,
                    columns: Vec::new(),
                }),
                sample: None,
            };

            select.from = vec![TableWithJoins {
                relation: derived,
                joins: Vec::new(),
            }];
        }

        std::ops::ControlFlow::Continue(())
    }
}

/// Build `unnest(arr)` as a sqlparser `Function`.
fn make_unnest(arr: Expr) -> Function {
    Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("unnest"))]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(arr))],
            clauses: Vec::new(),
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: Vec::new(),
    }
}

/// If `expr` is `EXPLODE(arr)`, return `arr`. Otherwise `None`.
fn extract_explode_arg(expr: &Expr) -> Option<Expr> {
    let Expr::Function(func) = expr else {
        return None;
    };
    if !object_name_eq_ignore_case(&func.name, "explode") {
        return None;
    }
    let FunctionArguments::List(FunctionArgumentList { args, .. }) = &func.args else {
        return None;
    };
    if args.len() != 1 {
        return None;
    }
    match &args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e.clone()),
        _ => None,
    }
}
