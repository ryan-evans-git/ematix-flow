//! Σ.L.3.c — bench gate for sequential masked-AND vs parallel-AND.
//!
//! Builds a Q06-shape BridgeFilter (3 predicates on lineitem: shipdate
//! range, discount range, quantity range) and compares wall-time of
//! `build_bitmap` (parallel) vs `build_bitmap_sequential` (Σ.L.3.c)
//! per row group.
//!
//! Pass criteria:
//!   sequential ≤ parallel × 0.85 → sequential wins ≥ 15% (per plan)
//!   sequential within ±5% of parallel → break-even, keep auto-pick
//!   sequential > parallel × 1.05 → sequential loses; revisit threshold
//!
//! Run:
//!   cargo run --release -p ematix-flow-core --example adaptive_filter_q06_gate

use std::time::Instant;

use datafusion::logical_expr::Operator;
use ematix_flow_core::ematix_fast_parquet::{
    BridgeFilter, ColumnPredicate, F64RangeClause, RangeClause,
};
use ematix_parquet_io::ParquetFile;

const REPS: usize = 5;

fn q06_filter() -> BridgeFilter {
    // TPC-H lineitem column layout:
    //   0:l_orderkey i64    4:l_quantity f64    10:l_shipdate Date32
    //   6:l_discount f64
    let shipdate = ColumnPredicate::I32Range {
        col_idx: 10,
        clauses: vec![
            RangeClause {
                op: Operator::GtEq,
                literal_i32: 8766, // 1994-01-01
            },
            RangeClause {
                op: Operator::Lt,
                literal_i32: 9131, // 1995-01-01
            },
        ],
    };
    let discount = ColumnPredicate::F64Range {
        col_idx: 6,
        clauses: vec![
            F64RangeClause {
                op: Operator::GtEq,
                literal_f64: 0.05,
            },
            F64RangeClause {
                op: Operator::LtEq,
                literal_f64: 0.07,
            },
        ],
    };
    let quantity = ColumnPredicate::F64Range {
        col_idx: 4,
        clauses: vec![F64RangeClause {
            op: Operator::Lt,
            literal_f64: 24.0,
        }],
    };
    // Build with `with_adaptive_reordering` so `applied_order` is
    // populated; the bench drives `build_bitmap_sequential` directly
    // (which reads applied_order).
    BridgeFilter::new(vec![shipdate, discount, quantity]).with_adaptive_reordering()
}

fn time_parallel(filter: &BridgeFilter, path: &std::path::Path, rgs: usize) -> (f64, usize) {
    let t = Instant::now();
    let mut total_set = 0usize;
    for rg in 0..rgs {
        let (bitmap, _total) = filter.build_bitmap(path, rg).unwrap();
        total_set += bitmap.iter().map(|b| b.count_ones() as usize).sum::<usize>();
    }
    (t.elapsed().as_secs_f64() * 1000.0, total_set)
}

fn time_sequential(filter: &BridgeFilter, path: &std::path::Path, rgs: usize) -> (f64, usize) {
    let t = Instant::now();
    let mut total_set = 0usize;
    for rg in 0..rgs {
        let (bitmap, _total) = filter
            .build_bitmap_sequential(path, rg)
            .unwrap()
            .expect("Q06 predicates all supported in sequential mode");
        total_set += bitmap.iter().map(|b| b.count_ones() as usize).sum::<usize>();
    }
    (t.elapsed().as_secs_f64() * 1000.0, total_set)
}

fn median(v: &[f64]) -> f64 {
    let mut s = v.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    s[s.len() / 2]
}

fn main() {
    let dir = std::env::var("TPCH_DATA_DIR")
        .unwrap_or_else(|_| "examples/tpch/data/sf1".to_string());
    let path = std::path::PathBuf::from(format!("{dir}/lineitem.parquet"));
    let file = ParquetFile::open(&path).unwrap();
    let n_rgs = file.metadata().unwrap().row_groups.len();

    println!("=== Σ.L.3.c bench gate — Q06 3-predicate sequential vs parallel ===");
    println!("File: {} ({n_rgs} row groups, REPS={REPS})\n", path.display());

    let filter = q06_filter();
    println!("Filter shape:");
    println!("  P0  I32Range l_shipdate ≥ 8766 (1994-01-01) AND < 9131 (1995-01-01)");
    println!("  P1  F64Range l_discount  ∈ [0.05, 0.07]");
    println!("  P2  F64Range l_quantity  < 24.0");
    println!(
        "  should_use_sequential() before predicted_pass_rate set: {}",
        filter.should_use_sequential()
    );
    let filter = filter.with_predicted_pass_rate(0.02);
    println!(
        "  should_use_sequential() after  predicted_pass_rate=0.02: {}",
        filter.should_use_sequential()
    );
    println!();

    // Warm-up: prime OS file cache so we measure CPU, not disk.
    let _ = time_parallel(&filter, &path, n_rgs);

    let mut parallel_ms = Vec::new();
    let mut sequential_ms = Vec::new();
    let mut last_par_set = 0usize;
    let mut last_seq_set = 0usize;
    for rep in 0..REPS {
        let (pms, pset) = time_parallel(&filter, &path, n_rgs);
        let (sms, sset) = time_sequential(&filter, &path, n_rgs);
        println!(
            "  rep {} : parallel {pms:>7.2} ms (set={pset}) | sequential {sms:>7.2} ms (set={sset})",
            rep + 1
        );
        parallel_ms.push(pms);
        sequential_ms.push(sms);
        last_par_set = pset;
        last_seq_set = sset;
    }

    let par_med = median(&parallel_ms);
    let seq_med = median(&sequential_ms);
    let ratio = seq_med / par_med;
    println!(
        "\n  median: parallel {par_med:>7.2} ms | sequential {seq_med:>7.2} ms | sequential/parallel = {ratio:.3}"
    );
    println!(
        "  bitmap equivalence: parallel set={last_par_set} | sequential set={last_seq_set} | {}",
        if last_par_set == last_seq_set {
            "MATCH ✓"
        } else {
            "MISMATCH ✗"
        }
    );
    println!();

    if last_par_set != last_seq_set {
        println!("FAIL: sequential bitmap differs from parallel. Correctness bug.");
        std::process::exit(2);
    }

    let verdict = if ratio <= 0.85 {
        "WIN — sequential ≥15% faster; keep auto-pick at predicted_pass_rate ≤ 0.30"
    } else if ratio <= 1.05 {
        "BREAK-EVEN — within ±5%; auto-pick is neutral, gate ON to harvest future wins"
    } else {
        "LOSS — sequential is slower; revisit threshold or kernel impl"
    };
    println!("Verdict: {verdict}");
}
