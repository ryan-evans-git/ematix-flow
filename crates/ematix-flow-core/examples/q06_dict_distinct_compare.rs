//! Σ.Q06.SF10.5.b debug harness — compare distinct_count per column
//! between parquet-rs's `SerializedPageReader::get_next_page` path
//! and ematix-parquet's `read_page_header` path on a real TPC-H
//! lineitem file. Identifies the divergence that caused the 22q
//! regression in Σ.Q06.SF10.5.a's wired-up swap.
//!
//! Run:
//!   cargo run --release -p ematix-flow-core --example q06_dict_distinct_compare \
//!     --features triangulation -- examples/tpch/data/sf10/lineitem.parquet
//!
//! Prints one row per leaf column:
//!   col_idx  name  parquet_rs  ematix_parquet  diff_indicator
//!
//! `diff_indicator` is `OK` if equal, `MISMATCH` if both produced
//! a value but they differ, `ONLY_PARQUET_RS` / `ONLY_EMATIX` for
//! one-sided.

use std::env;
use std::fs::File;
use std::path::PathBuf;

use datafusion::parquet::column::page::Page;
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path: PathBuf = env::args()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("usage: q06_dict_distinct_compare <path/to/lineitem.parquet>")?;
    let path_str = path.to_string_lossy().to_string();

    // parquet-rs walk
    let file = File::open(&path)?;
    let reader = SerializedFileReader::new(file)?;
    let num_rgs = reader.metadata().num_row_groups();
    let num_cols = reader.metadata().row_group(0).num_columns();
    let leaf_names: Vec<String> = reader
        .metadata()
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .map(|c| c.name().to_string())
        .collect();

    let mut pq_rs_max: Vec<Option<usize>> = vec![None; num_cols];
    // Index loop: col_idx indexes reader column metadata, not just pq_rs_max.
    #[allow(clippy::needless_range_loop)]
    for col_idx in 0..num_cols {
        // Bail if any RG lacks a dict for this column.
        let mut all_have_dict = true;
        for rg in reader.metadata().row_groups() {
            if rg.column(col_idx).dictionary_page_offset().is_none() {
                all_have_dict = false;
                break;
            }
        }
        if !all_have_dict {
            continue;
        }
        let mut max_distinct: usize = 0;
        let mut any_dict = false;
        let mut ok = true;
        for rg_idx in 0..num_rgs {
            let Ok(rg) = reader.get_row_group(rg_idx) else {
                ok = false;
                break;
            };
            let Ok(mut pr) = rg.get_column_page_reader(col_idx) else {
                ok = false;
                break;
            };
            match pr.get_next_page() {
                Ok(Some(Page::DictionaryPage { num_values, .. })) => {
                    max_distinct = max_distinct.max(num_values as usize);
                    any_dict = true;
                }
                _ => {
                    ok = false;
                    break;
                }
            }
        }
        if ok && any_dict && max_distinct > 0 {
            pq_rs_max[col_idx] = Some(max_distinct);
        }
    }

    // ematix-parquet walk
    let emat_max =
        ematix_flow_core::emat_parquet_metadata::dict_distinct_max_per_column(&path_str, num_cols)?;

    println!(
        "{:>4}  {:<22}  {:>12}  {:>12}  verdict",
        "col", "name", "parquet_rs", "ematix_emat"
    );
    println!("{}", "-".repeat(74));
    let mut mismatches: usize = 0;
    let mut only_rs: usize = 0;
    let mut only_emat: usize = 0;
    let mut both_some_equal: usize = 0;
    let mut both_none: usize = 0;
    // Index loop: i indexes leaf_names (via .get) and pq_rs_max in parallel.
    #[allow(clippy::needless_range_loop)]
    for i in 0..num_cols {
        let name = leaf_names
            .get(i)
            .cloned()
            .unwrap_or_else(|| format!("?{i}"));
        let a = pq_rs_max[i];
        let b = emat_max.get(i).copied().flatten();
        let verdict = match (a, b) {
            (Some(x), Some(y)) if x == y => {
                both_some_equal += 1;
                "OK"
            }
            (Some(_), Some(_)) => {
                mismatches += 1;
                "MISMATCH"
            }
            (Some(_), None) => {
                only_rs += 1;
                "ONLY_PARQUET_RS"
            }
            (None, Some(_)) => {
                only_emat += 1;
                "ONLY_EMATIX"
            }
            (None, None) => {
                both_none += 1;
                "BOTH_NONE"
            }
        };
        println!(
            "{:>4}  {:<22}  {:>12}  {:>12}  {}",
            i,
            name,
            a.map(|v| v.to_string()).unwrap_or_else(|| "-".to_string()),
            b.map(|v| v.to_string()).unwrap_or_else(|| "-".to_string()),
            verdict,
        );
    }
    println!();
    println!(
        "Summary: {} OK, {} MISMATCH, {} ONLY_PARQUET_RS, {} ONLY_EMATIX, {} BOTH_NONE",
        both_some_equal, mismatches, only_rs, only_emat, both_none
    );
    Ok(())
}
