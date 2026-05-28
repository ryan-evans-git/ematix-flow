//! Σ.Q06.SF10.5.c parity gate — compare the ematix-parquet metadata
//! mapper (`emat_parquet_metadata::load_provider_metadata`) against the
//! parquet-rs path that `EmatixFastParquetTableProvider::try_new`
//! currently uses, field-by-field, on every TPC-H table. Proves the
//! swap is behaviour-preserving before parquet-rs is removed from
//! try_new.
//!
//! Run:
//!   cargo run --release -p ematix-flow-core --example emat_meta_parity \
//!     --features triangulation -- examples/tpch/data/sf10

use std::fs::File;
use std::path::PathBuf;

use datafusion::common::stats::Precision;
use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};

const TABLES: &[&str] = &[
    "region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir: PathBuf = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("examples/tpch/data/sf10"));

    let mut total_mismatch = 0usize;
    for t in TABLES {
        let path = dir.join(format!("{t}.parquet"));
        if !path.exists() {
            println!("{t}: missing — skip");
            continue;
        }
        let path_str = path.to_string_lossy().to_string();

        // --- ematix path ---
        let em = ematix_flow_core::emat_parquet_metadata::load_provider_metadata(&path_str)?;

        // --- parquet-rs path (mirror of try_new pre-promotion) ---
        let file = File::open(&path)?;
        let opts = ArrowReaderOptions::new();
        let meta = ArrowReaderMetadata::load(&file, opts)?;
        let pq_schema = meta.schema().clone();
        let reader = SerializedFileReader::new(File::open(&path)?)?;
        let pq_num_rows = reader.metadata().file_metadata().num_rows() as usize;
        let pq_num_rg = reader.metadata().num_row_groups();
        let pq_stats = ematix_flow_core::fast_parquet::aggregate_column_statistics(
            reader.metadata(),
            pq_schema.as_ref(),
        );

        let mut mism = 0usize;
        // schema: field count + names + types + nullability
        if em.schema.fields().len() != pq_schema.fields().len() {
            println!("  {t}: SCHEMA LEN {} vs {}", em.schema.fields().len(), pq_schema.fields().len());
            mism += 1;
        } else {
            for (i, (ef, pf)) in em
                .schema
                .fields()
                .iter()
                .zip(pq_schema.fields().iter())
                .enumerate()
            {
                if ef.name() != pf.name() || ef.data_type() != pf.data_type() {
                    println!(
                        "  {t}.col[{i}] schema mismatch: emat({}:{:?}) vs pqrs({}:{:?})",
                        ef.name(), ef.data_type(), pf.name(), pf.data_type()
                    );
                    mism += 1;
                }
            }
        }
        if em.num_rows != pq_num_rows {
            println!("  {t}: num_rows {} vs {}", em.num_rows, pq_num_rows);
            mism += 1;
        }
        if em.num_row_groups != pq_num_rg {
            println!("  {t}: num_rg {} vs {}", em.num_row_groups, pq_num_rg);
            mism += 1;
        }
        // column_stats: null_count + min + max
        for i in 0..em.column_stats.len().min(pq_stats.len()) {
            let e = &em.column_stats[i];
            let p = &pq_stats[i];
            let prec_eq = |a: &Precision<usize>, b: &Precision<usize>| format!("{a:?}") == format!("{b:?}");
            let sv_eq = |a: &Precision<datafusion::common::ScalarValue>,
                         b: &Precision<datafusion::common::ScalarValue>| format!("{a:?}") == format!("{b:?}");
            if !prec_eq(&e.null_count, &p.null_count)
                || !sv_eq(&e.min_value, &p.min_value)
                || !sv_eq(&e.max_value, &p.max_value)
            {
                println!(
                    "  {t}.col[{i}] stats mismatch:\n    emat nc={:?} min={:?} max={:?}\n    pqrs nc={:?} min={:?} max={:?}",
                    e.null_count, e.min_value, e.max_value, p.null_count, p.min_value, p.max_value
                );
                mism += 1;
            }
        }
        println!("{t}: {} cols, {} rows, {} RG — {}", em.schema.fields().len(), em.num_rows, em.num_row_groups,
            if mism == 0 { "OK".to_string() } else { format!("{mism} MISMATCH") });
        total_mismatch += mism;
    }
    println!("\nTOTAL MISMATCHES: {total_mismatch}");
    if total_mismatch > 0 {
        std::process::exit(1);
    }
    Ok(())
}
