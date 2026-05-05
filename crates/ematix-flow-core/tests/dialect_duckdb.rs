//! Σ.A2 PR 5: DuckDB→DataFusion translator.
//!
//! DuckDB's SQL surface is closer to DataFusion's than Spark's — both
//! engines lean on the Postgres dialect with arrow-rs primitives. The
//! translator's expected scope is narrow: a handful of function-name
//! aliases where the engines diverge. Most queries should pass through
//! without modification.
//!
//! Plan acceptance: ≥90% pass rate on a hand-curated DuckDB test set
//! (no canonical equivalent of Spark's TPC-DS resource directory
//! exists; we audit against the same TPC-H reps Σ.A1 used).
//!
//! Test strategy mirrors `dialect_spark.rs`: emit-string contains/lacks
//! specific tokens. Round-trip queries through DataFusion in the e2e
//! harness (`dialect_spark_e2e.rs` and the duckdb counterpart).

use ematix_flow_core::dialect::{Dialect, translate};

fn duckdb_to_df(sql: &str) -> String {
    translate(sql, Dialect::DuckDb)
        .unwrap_or_else(|e| panic!("duckdb→df translate failed for {sql:?}: {e}"))
}

// --- pass-through cases (DuckDB ≈ DataFusion) ----------------------

/// A DuckDB SQL fragment with no engine-specific idioms must round-
/// trip through the translator unchanged in semantics. Most TPC-H
/// queries fall in this bucket.
#[test]
fn duckdb_pass_through_basic_select() {
    let out = duckdb_to_df("SELECT id, name FROM users WHERE age > 18");
    let lo = out.to_lowercase();
    assert!(lo.contains("select"));
    assert!(lo.contains("from users"));
    assert!(lo.contains("age > 18"));
}

#[test]
fn duckdb_pass_through_aggregates() {
    let out = duckdb_to_df("SELECT COUNT(*), SUM(price), AVG(qty) FROM orders");
    let lo = out.to_lowercase();
    assert!(lo.contains("count("));
    assert!(lo.contains("sum("));
    assert!(lo.contains("avg("));
}

#[test]
fn duckdb_pass_through_join() {
    let sql = "SELECT u.id, o.total FROM users u JOIN orders o ON u.id = o.user_id";
    let out = duckdb_to_df(sql);
    let lo = out.to_lowercase();
    assert!(lo.contains("join"));
    assert!(lo.contains("on u.id"));
}

#[test]
fn duckdb_pass_through_window_function() {
    let sql =
        "SELECT id, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM emp";
    let out = duckdb_to_df(sql);
    let lo = out.to_lowercase();
    assert!(lo.contains("row_number"));
    assert!(lo.contains("partition by"));
}

#[test]
fn duckdb_pass_through_cte() {
    let sql = "WITH t AS (SELECT id FROM users) SELECT * FROM t";
    let out = duckdb_to_df(sql);
    let lo = out.to_lowercase();
    assert!(lo.contains("with"));
    assert!(lo.contains("from t"));
}

#[test]
fn duckdb_pass_through_interval_literal() {
    // INTERVAL works identically in both engines.
    let out = duckdb_to_df("SELECT DATE '1998-12-01' - INTERVAL '90' DAY AS cutoff");
    let lo = out.to_lowercase();
    assert!(lo.contains("interval"));
}

// --- function-name remaps -----------------------------------------

/// DuckDB's `list_value(...)` is its array constructor (analogous to
/// Spark's `array(...)`). DataFusion uses `make_array(...)`.
#[test]
fn duckdb_list_value_becomes_make_array() {
    let out = duckdb_to_df("SELECT list_value(1, 2, 3) AS a");
    let lo = out.to_lowercase();
    assert!(
        lo.contains("make_array("),
        "list_value → make_array; got: {out}"
    );
    assert!(!lo.contains("list_value("));
}

// --- TPC-H representative queries ----------------------------------
//
// Σ.A1 PR 2's bench harness runs these through DataFusion's native
// dialect. Through `dialect = "duckdb"`, they should still translate
// (and PR 5's e2e file confirms they execute correctly).

#[test]
fn duckdb_tpch_q1_translates() {
    let q1 = include_str!("../../../examples/tpch/queries/q01.sql");
    let q1 = q1.trim().trim_end_matches(';');
    let out = duckdb_to_df(q1);
    assert!(out.to_lowercase().contains("lineitem"));
}

#[test]
fn duckdb_tpch_q3_translates() {
    let q3 = include_str!("../../../examples/tpch/queries/q03.sql");
    let q3 = q3.trim().trim_end_matches(';');
    let out = duckdb_to_df(q3);
    assert!(out.to_lowercase().contains("customer"));
}

#[test]
fn duckdb_tpch_q6_translates() {
    let q6 = include_str!("../../../examples/tpch/queries/q06.sql");
    let q6 = q6.trim().trim_end_matches(';');
    let out = duckdb_to_df(q6);
    assert!(out.to_lowercase().contains("lineitem"));
}

#[test]
fn duckdb_tpch_q19_translates() {
    let q19 = include_str!("../../../examples/tpch/queries/q19.sql");
    let q19 = q19.trim().trim_end_matches(';');
    let out = duckdb_to_df(q19);
    assert!(out.to_lowercase().contains("part"));
}

// --- error paths ---------------------------------------------------

#[test]
fn duckdb_returns_parse_error_for_garbage() {
    let result = translate("THIS IS NOT SQL", Dialect::DuckDb);
    assert!(result.is_err(), "garbage must error; got: {result:?}");
}

#[test]
fn duckdb_empty_string_returns_empty() {
    let out = duckdb_to_df("");
    assert!(out.trim().is_empty());
}
