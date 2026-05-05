//! Σ.A2: SQL dialect selector + translation layer.
//!
//! Sits *above* the Backend dispatch (so it doesn't change the
//! `Backend` trait shape — see `docs/PHASE_SIGMA_B_TRAIT_SPIKE.md`):
//! a user's SQL string is parsed in the source dialect, rewritten
//! into DataFusion-compatible SQL, and only then handed to
//! `SessionContext::sql()`.
//!
//! Σ.A2 PR 1: config surface, enum, error type, pass-through for
//! `Dialect::DataFusion`. Σ.A2 PR 2: Spark function-name remap via
//! `sqlparser-rs` AST visit (this commit); structural rewrites
//! (`LATERAL VIEW EXPLODE` → `UNNEST`, complex-type literals,
//! window-frame syntax) are PR 3. DuckDB lands in PR 5.
//!
//! Public API:
//! ```ignore
//! use ematix_flow_core::dialect::{Dialect, translate};
//!
//! let sql_in: &str = "SELECT 1";
//! let dialect: Dialect = "datafusion".parse().unwrap();
//! let sql_out: String = translate(sql_in, dialect).unwrap();
//! assert_eq!(sql_out, sql_in);
//! ```
//!
//! Why a free function rather than a method on a `Translator` struct?
//! The translator is stateless (no caches, no sessions) and we want
//! the call site at `crates/ematix-flow-cli/src/lib.rs` to read as
//! "translate this string, then build the transform" without an
//! intermediate object. If a future dialect needs config (e.g.,
//! Spark version pinning), promote to a struct then.

use std::str::FromStr;

use thiserror::Error;

mod spark;

/// SQL dialect of the input passed to [`translate`]. Output is always
/// DataFusion's dialect (the workspace's native execution engine).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Dialect {
    /// DataFusion's own SQL surface (the engine we run on top of).
    /// Translator passes the string through unchanged — zero-cost
    /// semantically, modulo a single allocation for the returned
    /// `String`.
    DataFusion,

    /// Apache Spark SQL. Function-name remap, `LATERAL VIEW EXPLODE`
    /// → `UNNEST`, complex-type literal syntax. Σ.A2 PRs 2–4.
    Spark,

    /// DuckDB SQL. Closer to DataFusion's surface than Spark's;
    /// mostly function-name remap + a few date-literal differences.
    /// Σ.A2 PR 5.
    DuckDb,
}

/// Errors returned by the dialect parser + translator. Each variant
/// is meant to be actionable: the message tells the operator what to
/// fix in the TOML config or which PR fills the gap.
#[derive(Debug, Error)]
pub enum DialectError {
    /// Translator hasn't yet been implemented for this dialect.
    /// `DuckDb` returns this until Σ.A2 PR 5.
    #[error(
        "SQL translation for dialect {0:?} is not implemented yet. \
         Tracked under Σ.A2 — see docs/PHASE_SIGMA_PLAN.md. \
         Use dialect = \"datafusion\" until then or rewrite the SQL \
         to DataFusion's native dialect."
    )]
    NotImplemented(Dialect),

    /// `[transform] dialect = "..."` had a value the parser doesn't
    /// recognize. Listed valid options in the message so the user
    /// can fix it without leaving the error log.
    #[error(
        "unknown dialect {found:?}; valid options: {}",
        valid.join(", ")
    )]
    UnknownDialect {
        found: String,
        valid: &'static [&'static str],
    },

    /// `sqlparser` couldn't parse the input — invalid SQL syntax in
    /// the source dialect. Wraps the underlying message so the user
    /// sees the line + column from sqlparser's error.
    #[error("SQL parse error: {0}")]
    ParseError(String),
}

/// Stable list of dialect strings accepted by `Dialect::from_str`,
/// in the order we'd recommend a user discover them.
const VALID_DIALECTS: &[&str] = &["datafusion", "spark", "duckdb"];

impl FromStr for Dialect {
    type Err = DialectError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "datafusion" => Ok(Dialect::DataFusion),
            "spark" => Ok(Dialect::Spark),
            "duckdb" => Ok(Dialect::DuckDb),
            _ => Err(DialectError::UnknownDialect {
                found: s.to_string(),
                valid: VALID_DIALECTS,
            }),
        }
    }
}

/// Translate `sql` from the source dialect into DataFusion's dialect.
///
/// `Dialect::DataFusion` is a pass-through: the input is returned
/// unchanged (allocated as a fresh `String`). `Spark` and `DuckDb`
/// currently return [`DialectError::NotImplemented`]; PR 2 + PR 5
/// will fill them in via `sqlparser-rs` AST rewrites.
///
/// The function is stateless and inexpensive at the pass-through arm
/// (single allocation), so call sites may invoke it once per query
/// without caching.
pub fn translate(sql: &str, from: Dialect) -> Result<String, DialectError> {
    match from {
        Dialect::DataFusion => Ok(sql.to_string()),
        Dialect::Spark => spark::translate(sql),
        Dialect::DuckDb => Err(DialectError::NotImplemented(from)),
    }
}
