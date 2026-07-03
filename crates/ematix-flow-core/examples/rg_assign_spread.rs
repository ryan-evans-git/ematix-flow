//! LPT.RG diagnostic — per-partition predicted-cost spread of the
//! round-robin vs LPT row-group → partition assignment for one parquet
//! file. Quantifies the balance win the `EMAT_BALANCED_RG_ASSIGN`
//! lever buys (tail latency of a scan is set by its most expensive
//! partition; the spread IS the wasted tail).
//!
//! Usage:
//! ```text
//! cargo run --example rg_assign_spread -- <file.parquet> [num_partitions] [col_idx,col_idx,...]
//! ```
//! Defaults: 14 partitions (the M4 Max bench shape), all columns
//! projected. Costs are the per-RG sums of the projected columns'
//! column-chunk `total_compressed_size` — the exact cost model
//! `EmatixFastParquetTableProvider::scan()` uses.

use ematix_flow_core::emat_parquet_metadata::load_provider_metadata;
use ematix_flow_core::ematix_fast_parquet::{
    lpt_rg_assignments, rg_projected_costs, round_robin_rg_assignments,
};

fn spread_report(name: &str, assignments: &[Vec<usize>], costs: &[u64]) {
    let loads: Vec<u64> = assignments
        .iter()
        .map(|p| p.iter().map(|&rg| costs[rg]).sum())
        .collect();
    let max = *loads.iter().max().unwrap_or(&0);
    let min = *loads.iter().min().unwrap_or(&0);
    let total: u64 = loads.iter().sum();
    let mean = total as f64 / loads.len().max(1) as f64;
    println!("== {name} ==");
    for (p, (load, rgs)) in loads.iter().zip(assignments).enumerate() {
        println!(
            "  partition {p:>2}: {:>12} bytes  ({} RGs: {:?})",
            load,
            rgs.len(),
            rgs
        );
    }
    println!(
        "  max {max}  min {min}  spread {} ({:+.2}% over mean)  mean {mean:.0}",
        max - min,
        (max as f64 - mean) / mean * 100.0
    );
}

fn main() {
    let mut args = std::env::args().skip(1);
    let path = args.next().unwrap_or_else(|| {
        eprintln!("usage: rg_assign_spread <file.parquet> [num_partitions] [col,col,...]");
        std::process::exit(2);
    });
    let num_partitions: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(14);
    let meta = load_provider_metadata(&path).expect("load footer metadata");
    let num_rgs = meta.num_row_groups;
    let num_cols = meta.schema.fields().len();
    let projection: Vec<usize> = args
        .next()
        .map(|s| {
            s.split(',')
                .map(|c| c.trim().parse().expect("column index"))
                .collect()
        })
        .unwrap_or_else(|| (0..num_cols).collect());
    println!(
        "{path}: {num_rgs} row groups, {num_cols} columns, {num_partitions} partitions, projection {projection:?}"
    );
    let costs = rg_projected_costs(&meta.rg_column_compressed_sizes, num_rgs, &projection)
        .expect("footer metadata covers every row group");
    spread_report(
        "round-robin (legacy / EMAT_BALANCED_RG_ASSIGN=0)",
        &round_robin_rg_assignments(num_rgs, num_partitions),
        &costs,
    );
    spread_report(
        "LPT (default)",
        &lpt_rg_assignments(&costs, num_partitions),
        &costs,
    );
}
