//! Σ.A2 PR 1: dialect selector + translator scaffold.
//!
//! Locks the public API for `ematix_flow_core::dialect` before the
//! Spark + DuckDB translators land in PR 2 + PR 5. PR 1 ships:
//!
//! - `Dialect::{DataFusion, Spark, DuckDb}` enum with `FromStr`
//!   parsing of TOML config strings (`"datafusion" | "spark" |
//!   "duckdb"`).
//! - `translate(sql, from)` free function. `Dialect::DataFusion`
//!   passthrough; `Spark` + `DuckDb` return `DialectError::NotImplemented`
//!   with a pointer at the PR that fills them in.
//!
//! These tests anchor the contract — passthrough must be zero-cost
//! semantically, and unsupported dialects must error early with a
//! clear, actionable message instead of producing surprising SQL.

use ematix_flow_core::dialect::{Dialect, DialectError, translate};

#[test]
fn datafusion_passthrough_returns_the_input_unchanged() {
    let sql = "SELECT 1 AS one FROM lineitem WHERE l_quantity > 0";
    let out = translate(sql, Dialect::DataFusion).expect("datafusion is the native dialect");
    assert_eq!(out, sql, "DataFusion dialect must not modify the SQL");
}

#[test]
fn datafusion_passthrough_handles_empty_input() {
    // Empty string is technically valid input — the windowed transform
    // path passes "" when no SQL pre-stage is configured. Translator
    // must not error on it.
    let out = translate("", Dialect::DataFusion).expect("empty SQL is still valid input");
    assert_eq!(out, "");
}

/// Σ.A2 PR 2 landed Spark — the function-name-remap surface.
/// Pass-through cases (`SELECT 1`) succeed; complex Spark→DataFusion
/// rewrites are exercised separately in `tests/dialect_spark.rs`.
#[test]
fn spark_dialect_translates_pass_through() {
    let out = translate("SELECT 1", Dialect::Spark).expect("Spark passes through trivial SQL");
    // sqlparser may normalize whitespace; the literal `1` survives.
    assert!(
        out.contains('1'),
        "Spark must round-trip `SELECT 1`; got: {out}"
    );
}

#[test]
fn duckdb_dialect_errors_with_pr5_pointer() {
    let err = translate("SELECT 1", Dialect::DuckDb).expect_err("duckdb not yet implemented");
    assert!(matches!(err, DialectError::NotImplemented(Dialect::DuckDb)));
}

#[test]
fn dialect_from_str_accepts_canonical_lowercase() {
    assert_eq!(
        "datafusion".parse::<Dialect>().unwrap(),
        Dialect::DataFusion
    );
    assert_eq!("spark".parse::<Dialect>().unwrap(), Dialect::Spark);
    assert_eq!("duckdb".parse::<Dialect>().unwrap(), Dialect::DuckDb);
}

#[test]
fn dialect_from_str_is_case_insensitive() {
    // Users sometimes type "DataFusion" or "DuckDB" in TOML; accept
    // either capitalization. Anything else fails fast at config load.
    assert_eq!(
        "DataFusion".parse::<Dialect>().unwrap(),
        Dialect::DataFusion
    );
    assert_eq!("SPARK".parse::<Dialect>().unwrap(), Dialect::Spark);
    assert_eq!("DuckDB".parse::<Dialect>().unwrap(), Dialect::DuckDb);
}

#[test]
fn dialect_from_str_rejects_unknown_with_helpful_message() {
    let err = "trino"
        .parse::<Dialect>()
        .expect_err("trino not yet supported");
    let msg = format!("{err}");
    assert!(
        msg.contains("trino"),
        "error must echo the bad input; got: {msg}"
    );
    // Must list the supported dialects so the user knows what to try.
    for valid in &["datafusion", "spark", "duckdb"] {
        assert!(
            msg.contains(valid),
            "error must list valid dialect '{valid}'; got: {msg}"
        );
    }
}
