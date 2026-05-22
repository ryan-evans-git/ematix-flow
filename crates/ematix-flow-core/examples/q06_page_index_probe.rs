//! Q06 page-index pruning diagnostic at SF=10.
//!
//! Reads the parquet column index for `l_shipdate`, `l_quantity`,
//! `l_discount` and computes per-page overlap with the Q06 predicate
//! windows. Reports per-column and combined skippable %.
//!
//! Run:
//!   TPCH_DATA_DIR=examples/tpch/data/sf10 \
//!     cargo run --release -p ematix-flow-core --example q06_page_index_probe

use std::fs::File;
use std::path::PathBuf;

use datafusion::parquet::file::page_index::column_index::ColumnIndexMetaData;
use datafusion::parquet::file::reader::FileReader;
use datafusion::parquet::file::serialized_reader::{ReadOptionsBuilder, SerializedFileReader};

const SHIPDATE_LOW: i32 = 8766; // 1994-01-01
const SHIPDATE_HIGH: i32 = 9131; // 1995-01-01
const QUANTITY_MAX: f64 = 24.0;
const DISCOUNT_LOW: f64 = 0.05;
const DISCOUNT_HIGH: f64 = 0.07;

fn main() {
    let dir =
        std::env::var("TPCH_DATA_DIR").unwrap_or_else(|_| "examples/tpch/data/sf10".to_string());
    let path = PathBuf::from(&dir).join("lineitem.parquet");
    if !path.exists() {
        eprintln!("missing {}", path.display());
        std::process::exit(2);
    }

    let file = File::open(&path).expect("open");
    let reader = SerializedFileReader::new_with_options(
        file,
        ReadOptionsBuilder::new().with_page_index().build(),
    )
    .expect("reader");
    let md = reader.metadata();
    let schema = md.file_metadata().schema();
    let cols: Vec<String> = schema
        .get_fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    let shipdate = cols
        .iter()
        .position(|n| n == "l_shipdate")
        .expect("l_shipdate");
    let quantity = cols
        .iter()
        .position(|n| n == "l_quantity")
        .expect("l_quantity");
    let discount = cols
        .iter()
        .position(|n| n == "l_discount")
        .expect("l_discount");

    let n_rgs = md.num_row_groups();
    let ci = md.column_index().expect("column_index loaded");

    println!("=== Q06 page-index probe ({}) ===", path.display());
    println!("Row groups: {n_rgs}\n");

    // Per-column overlap counts.
    let probe = |col_idx: usize, label: &str, kind: &str| -> (usize, usize) {
        let mut total = 0usize;
        let mut skip = 0usize;
        #[allow(clippy::needless_range_loop)]
        for rg in 0..n_rgs {
            let meta = &ci[rg][col_idx];
            match (meta, kind) {
                (ColumnIndexMetaData::INT32(idx), "shipdate") => {
                    let n = idx.min_values().len();
                    for p in 0..n {
                        total += 1;
                        let mn = idx.min_value(p).copied().unwrap_or(i32::MIN);
                        let mx = idx.max_value(p).copied().unwrap_or(i32::MAX);
                        if mx < SHIPDATE_LOW || mn >= SHIPDATE_HIGH {
                            skip += 1;
                        }
                    }
                }
                (ColumnIndexMetaData::DOUBLE(idx), "quantity") => {
                    let n = idx.min_values().len();
                    for p in 0..n {
                        total += 1;
                        let mn = idx.min_value(p).copied().unwrap_or(f64::NEG_INFINITY);
                        if mn >= QUANTITY_MAX {
                            skip += 1;
                        }
                    }
                }
                (ColumnIndexMetaData::DOUBLE(idx), "discount") => {
                    let n = idx.min_values().len();
                    for p in 0..n {
                        total += 1;
                        let mn = idx.min_value(p).copied().unwrap_or(f64::NEG_INFINITY);
                        let mx = idx.max_value(p).copied().unwrap_or(f64::INFINITY);
                        if mx < DISCOUNT_LOW || mn > DISCOUNT_HIGH {
                            skip += 1;
                        }
                    }
                }
                (ColumnIndexMetaData::NONE, _) => {
                    eprintln!("WARN: {label} rg={rg} has no column index");
                }
                _ => {
                    eprintln!("unexpected {label} idx variant");
                }
            }
        }
        let pct = if total > 0 {
            skip as f64 / total as f64 * 100.0
        } else {
            0.0
        };
        println!("  {label:<14} pages: {total:>6}  skip: {skip:>6}  ({pct:>5.1}% skippable)");
        (total, skip)
    };

    let _ = probe(shipdate, "l_shipdate", "shipdate");
    let _ = probe(quantity, "l_quantity", "quantity");
    let _ = probe(discount, "l_discount", "discount");
    println!();

    // Combined: a page is skippable for Q06 iff ANY of its 3 filter
    // cols' stats prove no rows pass. Page index is per-row-range, so
    // page-i across the 3 columns refers to the same rows.
    let mut total = 0usize;
    let mut skip = 0usize;
    #[allow(clippy::needless_range_loop)]
    for rg in 0..n_rgs {
        let s_idx = match &ci[rg][shipdate] {
            ColumnIndexMetaData::INT32(i) => i,
            _ => continue,
        };
        let q_idx = match &ci[rg][quantity] {
            ColumnIndexMetaData::DOUBLE(i) => i,
            _ => continue,
        };
        let d_idx = match &ci[rg][discount] {
            ColumnIndexMetaData::DOUBLE(i) => i,
            _ => continue,
        };
        let n_pages = s_idx.min_values().len();
        for p in 0..n_pages {
            total += 1;
            let s_mn = s_idx.min_value(p).copied().unwrap_or(i32::MIN);
            let s_mx = s_idx.max_value(p).copied().unwrap_or(i32::MAX);
            let q_mn = q_idx.min_value(p).copied().unwrap_or(f64::NEG_INFINITY);
            let d_mn = d_idx.min_value(p).copied().unwrap_or(f64::NEG_INFINITY);
            let d_mx = d_idx.max_value(p).copied().unwrap_or(f64::INFINITY);
            let s_skip = s_mx < SHIPDATE_LOW || s_mn >= SHIPDATE_HIGH;
            let q_skip = q_mn >= QUANTITY_MAX;
            let d_skip = d_mx < DISCOUNT_LOW || d_mn > DISCOUNT_HIGH;
            if s_skip || q_skip || d_skip {
                skip += 1;
            }
        }
    }
    let pct = if total > 0 {
        skip as f64 / total as f64 * 100.0
    } else {
        0.0
    };
    println!("Combined Q06 (AND of 3 predicate cols):");
    println!(
        "  pages: {total}  keep: {}  skip: {skip}  ({pct:.1}% skippable)",
        total - skip
    );
    println!();
    if skip == 0 {
        println!(
            "Verdict: DEAD LEVER — data uniform within every page. Same as Σ.E5 SF=1 finding."
        );
    } else if pct < 5.0 {
        println!("Verdict: marginal ({pct:.1}%). Probably not worth the wire-up cost.");
    } else if pct < 30.0 {
        println!("Verdict: real lever ({pct:.1}%). Worth wiring up.");
    } else {
        println!("Verdict: BIG lever ({pct:.1}%). Investigate immediately.");
    }
}
