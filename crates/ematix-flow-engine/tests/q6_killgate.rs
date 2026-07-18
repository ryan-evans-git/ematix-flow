//! P0 kill-gate: the native push spine must reproduce DuckDB's TPC-H Q6.
//!
//! Oracle (DuckDB, verified 2026-07-18 against
//! `examples/tpch/data/sf1/lineitem.parquet`):
//!   revenue = 123141078.22829972, matched rows = 114160.
//! This is the canonical TPC-H SF-1 Q6 answer (123141078.2283).
//!
//! The engine crate carries no DuckDB/DataFusion dependency; the oracle
//! is pinned here as a verified constant. Full differential-vs-DuckDB
//! validation over the whole surface is a P4 concern.

use std::path::PathBuf;

use ematix_flow_engine::run_tpch_q6;

fn sf1_lineitem() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/tpch/data/sf1/lineitem.parquet")
}

#[test]
fn q6_matches_duckdb_oracle() {
    let path = sf1_lineitem();
    if !path.exists() {
        eprintln!("SKIP q6_matches_duckdb_oracle: {} absent", path.display());
        return;
    }

    let got = run_tpch_q6(&path).expect("Q6 spine failed");

    // Structural check first: the same rows passed the filter. Catches
    // any date-epoch / predicate-boundary bug regardless of the sum.
    assert_eq!(
        got.matched, 114160,
        "matched-row count {} != DuckDB oracle 114160",
        got.matched
    );

    // Aggregate check within a tight relative tolerance — f64 summation
    // order differs from DuckDB, so not bit-identical.
    let oracle = 123_141_078.229_299_72_f64;
    let rel = (got.revenue - oracle).abs() / oracle;
    assert!(
        rel < 1e-9,
        "revenue {} != DuckDB oracle {oracle} (rel err {rel:.3e})",
        got.revenue
    );
}
