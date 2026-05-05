//! Σ.A2 PR 2: Spark→DataFusion function-name remap.
//!
//! This is the tightest scope of the Spark dialect translator: pure
//! function-name substitutions where Spark and DataFusion expose the
//! same logical operation under different names. Argument-shape
//! rewrites (`from_unixtime`'s signature, `date_format`'s pattern
//! string, `INTERVAL` literals) land in PR 3. Structural rewrites
//! (`LATERAL VIEW EXPLODE` → `UNNEST`) also in PR 3.
//!
//! Why a "Databricks" parser stands in for Spark: sqlparser-rs 0.61
//! ships `DatabricksDialect` but no `SparkDialect`. Databricks SQL is
//! Spark SQL plus extensions, so anything valid in Spark parses
//! cleanly under Databricks. Documented in `dialect/spark.rs`.
//!
//! Test strategy: each case asserts the OUTPUT contains/lacks
//! specific tokens. We don't byte-compare the full output because
//! sqlparser normalizes whitespace + casing in ways that aren't
//! semantically meaningful. End-to-end execution against DataFusion
//! lands in PR 4 (TPC-DS audit run).

use ematix_flow_core::dialect::{Dialect, translate};

/// Quick helper: translate Spark → DataFusion, panic with a clear
/// message on failure (so the test output names which case broke).
fn spark_to_df(sql: &str) -> String {
    translate(sql, Dialect::Spark)
        .unwrap_or_else(|e| panic!("spark→df translate failed for {sql:?}: {e}"))
}

// --- function-name remaps ------------------------------------------

/// `expr(x)` is Spark's no-op wrapper used in DataFrame APIs that
/// embed expressions; in raw SQL it just returns its argument.
/// DataFusion has no `expr` function — strip the wrapper and emit
/// just the inner expression.
#[test]
fn spark_expr_wrapper_is_stripped() {
    let out = spark_to_df("SELECT expr(price) FROM orders");
    assert!(
        !out.to_lowercase().contains("expr("),
        "expr() wrapper must be stripped; got: {out}"
    );
    assert!(
        out.to_lowercase().contains("price"),
        "inner expression must be preserved; got: {out}"
    );
}

#[test]
fn spark_expr_wrapper_strip_handles_complex_inner_expression() {
    let out = spark_to_df("SELECT expr(price * 1.1 + tax) FROM orders");
    assert!(!out.to_lowercase().contains("expr("));
    assert!(out.contains('*') && out.contains('+'));
}

/// Spark's `IFNULL(a, b)` is the same as `COALESCE(a, b)` for two
/// arguments. DataFusion exposes `COALESCE` but not `IFNULL`.
#[test]
fn spark_ifnull_becomes_coalesce() {
    let out = spark_to_df("SELECT IFNULL(name, 'unknown') FROM users");
    let lo = out.to_lowercase();
    assert!(lo.contains("coalesce"), "IFNULL → COALESCE; got: {out}");
    assert!(!lo.contains("ifnull"), "IFNULL must be removed; got: {out}");
}

/// Spark's `NVL(a, b)` is also COALESCE-equivalent. Same family of
/// Oracle-inherited names that aren't in DataFusion.
#[test]
fn spark_nvl_becomes_coalesce() {
    let out = spark_to_df("SELECT NVL(price, 0) FROM orders");
    let lo = out.to_lowercase();
    assert!(lo.contains("coalesce"), "NVL → COALESCE; got: {out}");
    assert!(!lo.contains("nvl("), "NVL must be removed; got: {out}");
}

/// Spark's `LCASE` / `UCASE` are aliases for `LOWER` / `UPPER`.
/// DataFusion only has the latter.
#[test]
fn spark_lcase_becomes_lower() {
    let out = spark_to_df("SELECT LCASE(name) FROM users");
    let lo = out.to_lowercase();
    assert!(lo.contains("lower("), "LCASE → LOWER; got: {out}");
    assert!(!lo.contains("lcase("), "LCASE must be removed; got: {out}");
}

#[test]
fn spark_ucase_becomes_upper() {
    let out = spark_to_df("SELECT UCASE(name) FROM users");
    let lo = out.to_lowercase();
    assert!(lo.contains("upper("), "UCASE → UPPER; got: {out}");
    assert!(!lo.contains("ucase("), "UCASE must be removed; got: {out}");
}

/// Spark's `INSTR(haystack, needle)` is `STRPOS(haystack, needle)`
/// in DataFusion. Same return semantics (1-based position, 0 if
/// not found).
#[test]
fn spark_instr_becomes_strpos() {
    let out = spark_to_df("SELECT INSTR(email, '@') FROM users");
    let lo = out.to_lowercase();
    assert!(lo.contains("strpos("), "INSTR → STRPOS; got: {out}");
    assert!(!lo.contains("instr("), "INSTR must be removed; got: {out}");
}

/// Spark's `current_timestamp()` and `now()` are equivalent;
/// canonicalize on `now()` so the rewrite always emits the
/// shorter form (DataFusion supports both, but `now()` is more
/// commonly recommended).
#[test]
fn spark_current_timestamp_becomes_now() {
    let out = spark_to_df("SELECT current_timestamp() AS ts");
    let lo = out.to_lowercase();
    assert!(
        lo.contains("now("),
        "current_timestamp() → now(); got: {out}"
    );
}

// --- pass-through cases --------------------------------------------

/// Functions that exist with identical names + semantics in both
/// dialects must be passed through unchanged. This exercises the
/// translator's "don't touch what isn't broken" path.
#[test]
fn spark_pass_through_lower() {
    let out = spark_to_df("SELECT LOWER(name) FROM users");
    assert!(out.to_lowercase().contains("lower("));
}

#[test]
fn spark_pass_through_count_star() {
    let out = spark_to_df("SELECT COUNT(*) FROM users");
    assert!(out.to_lowercase().contains("count("));
}

#[test]
fn spark_pass_through_sum() {
    let out = spark_to_df("SELECT SUM(price) FROM orders");
    assert!(out.to_lowercase().contains("sum("));
}

#[test]
fn spark_pass_through_substr() {
    let out = spark_to_df("SELECT SUBSTR(name, 1, 5) FROM users");
    assert!(out.to_lowercase().contains("substr("));
}

#[test]
fn spark_pass_through_coalesce() {
    let out = spark_to_df("SELECT COALESCE(a, b, c) FROM t");
    assert!(out.to_lowercase().contains("coalesce("));
}

// --- multi-statement / complex shapes ------------------------------

/// Joins, GROUP BY, and HAVING: structural Spark SQL that's the same
/// shape as DataFusion's. Function remaps inside the projection list
/// must still apply.
#[test]
fn spark_join_with_remap_in_projection() {
    let sql = r#"
        SELECT u.id, IFNULL(u.name, 'anon') AS display, COUNT(*) AS n
        FROM users u
        JOIN orders o ON u.id = o.user_id
        GROUP BY u.id, u.name
        HAVING COUNT(*) > 1
    "#;
    let out = spark_to_df(sql);
    let lo = out.to_lowercase();
    assert!(
        lo.contains("coalesce("),
        "remap inside projection; got: {out}"
    );
    assert!(!lo.contains("ifnull("));
    assert!(lo.contains("join"), "join structure preserved");
    assert!(lo.contains("group by"), "group by preserved");
    assert!(lo.contains("having"), "having preserved");
}

/// Nested function calls: outer is a remap, inner is a pass-through.
#[test]
fn spark_nested_calls_remap_outer_only() {
    let out = spark_to_df("SELECT IFNULL(LOWER(name), 'unknown') FROM users");
    let lo = out.to_lowercase();
    assert!(lo.contains("coalesce(lower("));
}

/// Same as above but reversed — outer is pass-through, inner is
/// remap. Both must apply.
#[test]
fn spark_nested_calls_remap_inner() {
    let out = spark_to_df("SELECT LOWER(IFNULL(name, '')) FROM users");
    let lo = out.to_lowercase();
    assert!(lo.contains("lower(coalesce("));
}

/// Multiple statements separated by semicolons: rare in our use case
/// (one SQL per transform step) but the parser handles them and the
/// remap must apply to each.
#[test]
fn spark_multiple_statements() {
    let out = spark_to_df("SELECT IFNULL(a, 0) FROM t1; SELECT NVL(b, 0) FROM t2");
    let lo = out.to_lowercase();
    assert!(!lo.contains("ifnull("), "first stmt remapped; got: {out}");
    assert!(!lo.contains("nvl("), "second stmt remapped; got: {out}");
    let coalesce_count = lo.matches("coalesce").count();
    assert_eq!(coalesce_count, 2, "both stmts get COALESCE; got: {out}");
}

// --- error paths ---------------------------------------------------

/// Garbage SQL must produce a parse error, not a translation panic.
#[test]
fn spark_returns_parse_error_for_garbage() {
    let result = translate("THIS IS NOT SQL", Dialect::Spark);
    assert!(result.is_err(), "garbage must error; got: {result:?}");
    let msg = format!("{}", result.unwrap_err());
    assert!(
        msg.to_lowercase().contains("parse")
            || msg.to_lowercase().contains("expected")
            || msg.to_lowercase().contains("syntax"),
        "error must signal parse failure; got: {msg}"
    );
}

/// An empty string is technically a valid input — the windowed
/// transform path passes "" when no SQL pre-stage is configured.
/// Translator must not error on it (matches DataFusion arm).
#[test]
fn spark_empty_string_returns_empty() {
    let out = spark_to_df("");
    // sqlparser may emit "" or whitespace; either is fine — the
    // contract is "no panic, no error".
    assert!(
        out.trim().is_empty(),
        "empty input → empty output; got: {out:?}"
    );
}

// --- TPC-H representative queries (the four Σ.A1 ran) --------------
//
// These confirm the translator doesn't break TPC-H Spark SQL. We're
// not running them through DataFusion in PR 2 (that's PR 4); just
// confirming they survive the translator without panic.

#[test]
fn spark_tpch_q1_passes_through() {
    let q1 = include_str!("../../../examples/tpch/queries/q01.sql");
    // Strip trailing semicolon to avoid the multi-statement path's
    // surface area (the file has one).
    let q1 = q1.trim().trim_end_matches(';');
    let out = spark_to_df(q1);
    assert!(!out.is_empty());
    // Q1 has no Spark-specific functions, so it should round-trip
    // cleanly. If we ever break this, something in the walker is
    // touching things it shouldn't.
    assert!(out.to_lowercase().contains("lineitem"));
}

#[test]
fn spark_tpch_q6_passes_through() {
    let q6 = include_str!("../../../examples/tpch/queries/q06.sql");
    let q6 = q6.trim().trim_end_matches(';');
    let out = spark_to_df(q6);
    assert!(!out.is_empty());
    assert!(out.to_lowercase().contains("lineitem"));
}
