//! Σ.E2 follow-up: probe whether SF=1 lineitem.parquet carries
//! page-level column/offset indexes, and if so what the per-page
//! `l_shipdate` min/max distribution looks like for Q14's filter
//! window `[1995-09-01, 1995-10-01)` (Date32 days = [9374, 9404)).
//!
//! Background: row-group-level stats are uniform across all 6 RGs
//! (commit 2061db8's probe showed every RG carries min=8036 max=10561
//! ≈ [1992-01-02, 1998-12-01]). RG pruning therefore doesn't help Q14.
//! But TPC-H lineitem is dbgen-ordered by (orderkey, linenumber) —
//! orderkey correlates with orderdate, and orderdate ≈ shipdate − k
//! days where k ∈ [1, 121]. So *within* a row group, individual pages
//! cover a narrow orderkey range, which back-derives to a narrow
//! shipdate range. If that holds in practice, Q14's 30-day window
//! filters out the bulk of pages within each RG and page-index pruning
//! closes the gap to Polars.
//!
//! Output: per row group, per data column (just l_shipdate),
//! list each page's (row count, min, max) so we can eyeball
//! how many pages overlap the Q14 window.

use std::fs::File;
use std::path::PathBuf;

use datafusion::parquet::arrow::arrow_reader::{
    ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use datafusion::parquet::file::metadata::PageIndexPolicy;

fn data_path() -> String {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    match std::env::var("TPCH_DATA_DIR") {
        Ok(s) => format!("{s}/lineitem.parquet"),
        Err(_) => manifest
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("examples/tpch/data/sf1/lineitem.parquet")
            .to_string_lossy()
            .into_owned(),
    }
}

/// Q14 window: 1995-09-01 = 9374, 1995-10-01 = 9404 (half-open).
const LO: i32 = 9374;
const HI: i32 = 9404;

fn main() {
    let path = data_path();
    let file = File::open(&path).unwrap();
    let opts = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Optional);
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, opts).unwrap();
    let meta = builder.metadata();
    let parquet_schema = builder.parquet_schema();

    println!("==> page-index probe for {path}");
    println!("==> Q14 shipdate window: [{LO}, {HI}) (Date32 days)");
    println!();

    // Find l_shipdate column index by name (parquet column descriptors).
    let mut shipdate_col: Option<usize> = None;
    for i in 0..parquet_schema.num_columns() {
        let col = parquet_schema.column(i);
        if col.name() == "l_shipdate" {
            shipdate_col = Some(i);
            break;
        }
    }
    let Some(col_idx) = shipdate_col else {
        eprintln!("l_shipdate column not found in parquet schema");
        return;
    };
    println!("l_shipdate is parquet column {col_idx}");

    // Check whether page indexes are present.
    let has_col_idx = meta.column_index().is_some();
    let has_off_idx = meta.offset_index().is_some();
    println!("page-index presence: column_index={has_col_idx}, offset_index={has_off_idx}");
    println!();

    if !has_col_idx {
        eprintln!("no column index → page-level stats unavailable, can't probe");
        return;
    }

    let col_indexes = meta.column_index().unwrap();
    let off_indexes = meta.offset_index();

    let mut total_pages: usize = 0;
    let mut overlap_pages: usize = 0;
    let mut total_rows: i64 = 0;
    let mut overlap_rows: i64 = 0;

    use datafusion::parquet::file::page_index::column_index::ColumnIndexMetaData;
    for rg_idx in 0..meta.num_row_groups() {
        let rg = meta.row_group(rg_idx);
        let col_pages = &col_indexes[rg_idx][col_idx];
        let off_pages = off_indexes.map(|o| &o[rg_idx][col_idx]);
        let (mins, maxs, null_pages): (&[i32], &[i32], Vec<bool>) = match col_pages {
            ColumnIndexMetaData::INT32(inner) => {
                let nulls: Vec<bool> = (0..inner.min_values().len())
                    .map(|i| inner.is_null_page(i))
                    .collect();
                (inner.min_values(), inner.max_values(), nulls)
            }
            other => {
                println!("RG {rg_idx}: column index variant {other:?}, skipping");
                continue;
            }
        };
        let row_counts: Vec<i64> = if let Some(op) = off_pages {
            let locs = op.page_locations();
            let rg_total = rg.num_rows();
            (0..locs.len())
                .map(|i| {
                    let start = locs[i].first_row_index;
                    let end = if i + 1 < locs.len() {
                        locs[i + 1].first_row_index
                    } else {
                        rg_total
                    };
                    end - start
                })
                .collect()
        } else {
            vec![]
        };
        let n_pages = mins.len();
        let mut rg_overlap_pages = 0;
        let mut rg_overlap_rows: i64 = 0;
        for i in 0..n_pages {
            let rows = row_counts.get(i).copied().unwrap_or(0);
            total_pages += 1;
            total_rows += rows;
            let overlaps = if null_pages.get(i).copied().unwrap_or(false) {
                true
            } else {
                let lo = mins[i];
                let hi = maxs[i];
                hi >= LO && lo < HI
            };
            if overlaps {
                overlap_pages += 1;
                overlap_rows += rows;
                rg_overlap_pages += 1;
                rg_overlap_rows += rows;
            }
        }
        println!(
            "RG {rg_idx}: {n_pages} pages, {} rows → {rg_overlap_pages} pages / {rg_overlap_rows} rows overlap Q14 window",
            rg.num_rows()
        );
        for i in 0..n_pages.min(5) {
            println!(
                "  page {i:>3}: rows={}  min={}  max={}",
                row_counts.get(i).copied().unwrap_or(0),
                mins[i],
                maxs[i]
            );
        }
        if n_pages > 5 {
            println!("  ... (showing first 5 of {n_pages} pages)");
        }
    }
    println!();
    println!("==> Summary");
    println!("  Total pages:     {total_pages}  ({total_rows} rows)");
    println!(
        "  Overlap pages:   {overlap_pages} ({:.1}% of pages, {overlap_rows} rows = {:.2}% of total)",
        100.0 * overlap_pages as f64 / total_pages.max(1) as f64,
        100.0 * overlap_rows as f64 / total_rows.max(1) as f64,
    );
    let row_select = overlap_rows as f64 / total_rows.max(1) as f64;
    println!();
    if row_select < 0.5 {
        println!(
            "  ✓ Page-index pruning could skip ~{:.0}% of l_shipdate decode work for Q14.",
            100.0 * (1.0 - row_select)
        );
        println!(
            "    Combined with sparse-decode for the other 3 cols (partkey/extprice/discount),"
        );
        println!("    this is the lever to close the gap to Polars.");
    } else {
        println!(
            "  ✗ Pages overlap the Q14 window too broadly ({:.0}% of rows still need decode);",
            100.0 * row_select
        );
        println!("    page-index pruning won't help here. The gap to Polars must live elsewhere.");
    }
}
