//! P2 native-scan gate: the DF-free ematix-parquet decode path must
//! produce Q6 results identical to the P0 stock-parquet decoder AND
//! match the DuckDB SF-1 oracle. Cross-validating the two independent
//! decoders is a strong correctness check on the new native path.

use std::path::PathBuf;

use ematix_flow_engine::{run_tpch_q6, run_tpch_q6_native};

fn sf1_lineitem() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/tpch/data/sf1/lineitem.parquet")
}

#[test]
fn native_scan_matches_stock_and_oracle() {
    let path = sf1_lineitem();
    if !path.exists() {
        eprintln!(
            "SKIP native_scan_matches_stock_and_oracle: {} absent",
            path.display()
        );
        return;
    }

    let native = run_tpch_q6_native(&path).expect("native Q6 failed");
    let stock = run_tpch_q6(&path).expect("stock Q6 failed");

    // Differential: two independent decoders reading the same PLAIN
    // doubles in the same row order must agree bit-for-bit.
    assert_eq!(
        native.matched, stock.matched,
        "native vs stock matched-count"
    );
    assert_eq!(
        native.revenue, stock.revenue,
        "native vs stock revenue (identical decode + sum order)"
    );

    // And both hit the DuckDB SF-1 oracle.
    assert_eq!(native.matched, 114160, "native matched != oracle");
    let oracle = 123_141_078.229_299_72_f64;
    let rel = (native.revenue - oracle).abs() / oracle;
    assert!(
        rel < 1e-9,
        "native revenue {} != oracle {oracle} (rel {rel:.3e})",
        native.revenue
    );
}
